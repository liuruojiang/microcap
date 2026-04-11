from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from analyze_top100_mom16_v1_1_nav_throttle_practical import (
    PracticalThrottleConfig,
    apply_practical_throttle,
    load_base_returns,
    summarize_returns,
)


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"

SUMMARY_CSV = OUTPUT_DIR / "microcap_top100_mom16_0p8_nav_throttle_4_8_vs_5_6_windows.csv"
NAV_CSV = OUTPUT_DIR / "microcap_top100_mom16_0p8_nav_throttle_4_8_vs_5_6_rebased_nav.csv"
PLOT_PNG = OUTPUT_DIR / "microcap_top100_mom16_0p8_nav_throttle_4_8_vs_5_6_compare.png"

CONFIGS = {
    "0.8x + nav 4/8": PracticalThrottleConfig(
        dd_moderate=0.04,
        dd_severe=0.08,
        scale_moderate=0.85,
        scale_severe=0.70,
        recover_dd=0.03,
        rebal_cost_bps=2.0,
    ),
    "0.8x + nav 5/6": PracticalThrottleConfig(
        dd_moderate=0.05,
        dd_severe=0.06,
        scale_moderate=0.90,
        scale_severe=0.65,
        recover_dd=0.03,
        rebal_cost_bps=2.0,
    ),
}

WINDOWS = [
    ("ytd", "今年"),
    ("1y", "最近1年"),
    ("3y", "最近3年"),
    ("5y", "最近5年"),
]


def build_window_start(latest: pd.Timestamp, key: str) -> pd.Timestamp:
    if key == "ytd":
        return pd.Timestamp(year=latest.year, month=1, day=1)
    if key == "1y":
        return latest - pd.DateOffset(years=1)
    if key == "3y":
        return latest - pd.DateOffset(years=3)
    if key == "5y":
        return latest - pd.DateOffset(years=5)
    raise ValueError(key)


def main() -> None:
    base_ret, _ = load_base_returns()
    latest = base_ret.index[-1]

    ret_map = {}
    for label, cfg in CONFIGS.items():
        run_df = apply_practical_throttle(base_ret, cfg)
        ret_map[label] = run_df["return"].fillna(0.0)

    summary_rows = []
    nav_rows = []

    fig, axes = plt.subplots(2, 2, figsize=(15, 9))
    axes = axes.flatten()

    for idx, (window_key, title) in enumerate(WINDOWS):
        ax = axes[idx]
        start = build_window_start(latest, window_key)
        for strategy, ret in ret_map.items():
            seg = ret.loc[ret.index >= start].dropna()
            nav = (1.0 + seg).cumprod()
            nav = nav / nav.iloc[0]
            ax.plot(nav.index, nav.values, linewidth=2.0, label=strategy)

            perf = summarize_returns(seg)
            summary_rows.append(
                {
                    "strategy": strategy,
                    "window": window_key,
                    **perf,
                }
            )
            nav_rows.append(
                pd.DataFrame(
                    {
                        "date": nav.index,
                        "window": window_key,
                        "strategy": strategy,
                        "rebased_nav": nav.values,
                    }
                )
            )

        ax.set_title(title)
        ax.grid(alpha=0.25)
        ax.legend()

    fig.suptitle("Top100 Mom16 0.8x: NAV Throttle 4/8 vs 5/6", fontsize=14)
    fig.tight_layout(rect=[0, 0, 1, 0.97])
    fig.savefig(PLOT_PNG, dpi=160)
    plt.close(fig)

    pd.concat(nav_rows, ignore_index=True).to_csv(NAV_CSV, index=False, encoding="utf-8-sig")
    pd.DataFrame(summary_rows).to_csv(SUMMARY_CSV, index=False, encoding="utf-8-sig")

    print(str(SUMMARY_CSV))
    print(str(NAV_CSV))
    print(str(PLOT_PNG))


if __name__ == "__main__":
    main()
