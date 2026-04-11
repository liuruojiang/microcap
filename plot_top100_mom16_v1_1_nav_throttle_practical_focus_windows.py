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

PLOT_PNG = OUTPUT_DIR / "microcap_top100_mom16_v1_1_nav_throttle_practical_focus_windows_compare.png"
NAV_CSV = OUTPUT_DIR / "microcap_top100_mom16_v1_1_nav_throttle_practical_focus_windows_rebased_nav.csv"
SUMMARY_CSV = OUTPUT_DIR / "microcap_top100_mom16_v1_1_nav_throttle_practical_focus_windows_summary.csv"


CONFIGS = {
    "3/6": PracticalThrottleConfig(0.03, 0.06, 0.85, 0.70, 0.03),
    "3/8": PracticalThrottleConfig(0.03, 0.08, 0.85, 0.65, 0.03),
    "4/8": PracticalThrottleConfig(0.04, 0.08, 0.85, 0.70, 0.03),
}

WINDOWS = [
    ("今年", "ytd"),
    ("最近1年", "1y"),
    ("最近3年", "3y"),
    ("最近5年", "5y"),
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
        ret_map[label] = run_df["return"]

    summary_rows = []
    nav_frames = []

    fig, axes = plt.subplots(2, 2, figsize=(15, 9))
    axes = axes.flatten()

    for idx, (title, key) in enumerate(WINDOWS):
        ax = axes[idx]
        start = build_window_start(latest, key)
        for strategy, ret in ret_map.items():
            seg = ret.loc[ret.index >= start].dropna()
            nav = (1.0 + seg).cumprod()
            nav = nav / nav.iloc[0]
            ax.plot(nav.index, nav.values, linewidth=2.0, label=strategy)

            perf = summarize_returns(seg)
            summary_rows.append(
                {
                    "strategy": strategy,
                    "window": key,
                    **perf,
                }
            )

            nav_frames.append(
                pd.DataFrame(
                    {
                        "date": nav.index,
                        "window": key,
                        "strategy": strategy,
                        "rebased_nav": nav.values,
                    }
                )
            )

        ax.set_title(title)
        ax.grid(alpha=0.25)
        ax.legend()

    fig.suptitle("Top100 Mom16 V1.1 Practical NAV Throttle: 3/6 vs 3/8 vs 4/8", fontsize=14)
    fig.tight_layout(rect=[0, 0, 1, 0.97])
    fig.savefig(PLOT_PNG, dpi=160)
    plt.close(fig)

    pd.concat(nav_frames, ignore_index=True).to_csv(NAV_CSV, index=False, encoding="utf-8-sig")
    pd.DataFrame(summary_rows).to_csv(SUMMARY_CSV, index=False, encoding="utf-8-sig")

    print(str(PLOT_PNG))
    print(str(NAV_CSV))
    print(str(SUMMARY_CSV))


if __name__ == "__main__":
    main()
