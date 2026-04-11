import json
from dataclasses import asdict
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from analyze_top100_mom16_hedge0p5_nav_throttle_practical import (
    apply_fixed_hedge_ratio,
    load_gross_result,
    load_turnover,
    summarize_returns,
)
from analyze_top100_mom16_v1_1_nav_throttle_practical import (
    PracticalThrottleConfig,
    apply_practical_throttle,
    iter_scan_configs,
)
import analyze_top100_rebalance_frequency as freq_mod


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"

SCAN_CSV = OUTPUT_DIR / "microcap_top100_mom16_nav_throttle_practical_scan_0p5_vs_0p8.csv"
BEST_CSV = OUTPUT_DIR / "microcap_top100_mom16_nav_throttle_practical_best_configs_0p5_vs_0p8.csv"
WINDOWS_CSV = OUTPUT_DIR / "microcap_top100_mom16_nav_throttle_practical_windows_0p5_vs_0p8.csv"
OPT_WINDOWS_CSV = OUTPUT_DIR / "microcap_top100_mom16_nav_throttle_practical_optimized_windows_0p5_vs_0p8.csv"
SUMMARY_JSON = OUTPUT_DIR / "microcap_top100_mom16_nav_throttle_practical_compare_0p5_vs_0p8.json"
PLOT_PNG = OUTPUT_DIR / "microcap_top100_mom16_nav_throttle_practical_compare_0p5_vs_0p8_recent10y.png"
REBASed_CSV = OUTPUT_DIR / "microcap_top100_mom16_nav_throttle_practical_compare_0p5_vs_0p8_recent10y_rebased_nav.csv"

HEDGE_RATIOS = [0.5, 0.8]


def build_baseline_net(hedge_ratio: float) -> pd.DataFrame:
    turnover = load_turnover()
    gross = load_gross_result()
    gross_ratio = apply_fixed_hedge_ratio(gross, hedge_ratio)
    net = freq_mod.cost_mod.apply_cost_model(gross_ratio, turnover)
    net.index = pd.to_datetime(net.index)
    return net


def summarize_window(ret: pd.Series, start: pd.Timestamp, label: str) -> dict | None:
    seg = ret.loc[ret.index >= start].dropna()
    if len(seg) < 20:
        return None
    out = summarize_returns(seg)
    out["window"] = label
    return out


def build_window_table(series_map: dict[str, pd.Series]) -> pd.DataFrame:
    latest = max(series.index[-1] for series in series_map.values())
    windows = [
        ("ytd", pd.Timestamp(year=latest.year, month=1, day=1)),
        ("1y", latest - pd.DateOffset(years=1)),
        ("3y", latest - pd.DateOffset(years=3)),
        ("5y", latest - pd.DateOffset(years=5)),
        ("10y", latest - pd.DateOffset(years=10)),
        ("15y", latest - pd.DateOffset(years=15)),
    ]
    rows = []
    for name, series in series_map.items():
        for label, start in windows:
            item = summarize_window(series, start, label)
            if item is None:
                continue
            item["strategy"] = name
            rows.append(item)
    return pd.DataFrame(rows)


def build_plot(nav_map: dict[str, pd.Series]) -> None:
    latest = max(series.index[-1] for series in nav_map.values())
    start = latest - pd.DateOffset(years=10)
    rebased = {}
    for name, nav in nav_map.items():
        seg = nav.loc[nav.index >= start].copy()
        seg = seg / seg.iloc[0]
        rebased[name] = seg

    pd.DataFrame(rebased).to_csv(REBASed_CSV, index_label="date")

    plt.figure(figsize=(13, 7))
    for name, series in rebased.items():
        plt.plot(series.index, series.values, linewidth=2.0, label=name)
    plt.title("Top100 Mom16 Practical NAV Throttle Compare: 0.5x vs 0.8x - Recent 10Y")
    plt.ylabel("Rebased NAV")
    plt.grid(alpha=0.25)
    plt.legend()
    plt.tight_layout()
    plt.savefig(PLOT_PNG, dpi=160)
    plt.close()


def scan_ratio(hedge_ratio: float) -> tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
    baseline = build_baseline_net(hedge_ratio)
    baseline_ret = baseline["return_net"].fillna(0.0)
    rows = []
    best_run = None
    best_cfg = None

    for cfg in iter_scan_configs():
        run_df = apply_practical_throttle(baseline_ret, cfg)
        perf = summarize_returns(run_df["return"])
        rows.append(
            {
                "hedge_ratio": hedge_ratio,
                "label": cfg.label,
                **asdict(cfg),
                **perf,
                "avg_scale": float(run_df["scale"].mean()),
                "min_scale": float(run_df["scale"].min()),
                "share_below_1": float((run_df["scale"] < 0.999999).mean()),
                "severe_share": float((run_df["state"] == "severe").mean()),
                "n_scale_changes": int((run_df["turnover"] > 0).sum()),
                "annualized_turnover": float(run_df["turnover"].sum() / len(run_df) * 252.0),
            }
        )

    scan_df = pd.DataFrame(rows).sort_values(
        ["sharpe", "max_drawdown", "annual_return"],
        ascending=[False, False, False],
    ).reset_index(drop=True)

    best_row = scan_df.iloc[0]
    best_cfg = PracticalThrottleConfig(
        dd_moderate=float(best_row["dd_moderate"]),
        dd_severe=float(best_row["dd_severe"]),
        scale_moderate=float(best_row["scale_moderate"]),
        scale_severe=float(best_row["scale_severe"]),
        recover_dd=float(best_row["recover_dd"]),
        rebal_cost_bps=float(best_row["rebal_cost_bps"]),
    )
    best_run = apply_practical_throttle(baseline_ret, best_cfg)

    best_summary = pd.DataFrame(
        [
            {
                "hedge_ratio": hedge_ratio,
                "label": best_cfg.label,
                **asdict(best_cfg),
                **summarize_returns(best_run["return"]),
                "avg_scale": float(best_run["scale"].mean()),
                "min_scale": float(best_run["scale"].min()),
                "share_below_1": float((best_run["scale"] < 0.999999).mean()),
                "severe_share": float((best_run["state"] == "severe").mean()),
                "n_scale_changes": int((best_run["turnover"] > 0).sum()),
                "annualized_turnover": float(best_run["turnover"].sum() / len(best_run) * 252.0),
            }
        ]
    )
    return scan_df, best_summary, baseline_ret, best_run["return"].fillna(0.0)


def main() -> None:
    scan_frames = []
    best_frames = []
    compare_series = {}
    optimized_series = {}
    summary_payload = {"as_of_date": None, "scan_grid": {
        "dd_moderate": [0.03, 0.04, 0.05],
        "dd_severe": [0.06, 0.08, 0.10],
        "scale_moderate": [0.85, 0.90, 0.95],
        "scale_severe": [0.65, 0.70, 0.75, 0.80],
        "recover_dd": [0.02, 0.03],
        "n_configs_per_ratio": len(iter_scan_configs()),
    }, "ratios": {}}

    for hedge_ratio in HEDGE_RATIOS:
        scan_df, best_df, baseline_ret, best_ret = scan_ratio(hedge_ratio)
        scan_frames.append(scan_df)
        best_frames.append(best_df)
        compare_series[f"{hedge_ratio:.1f}x baseline"] = baseline_ret
        compare_series[f"{hedge_ratio:.1f}x best nav"] = best_ret
        optimized_series[f"{hedge_ratio:.1f}x best nav"] = best_ret

        best_row = best_df.iloc[0].to_dict()
        baseline_summary = summarize_returns(baseline_ret)
        summary_payload["ratios"][str(hedge_ratio)] = {
            "baseline": baseline_summary,
            "best_nav_throttle": best_row,
        }
        if summary_payload["as_of_date"] is None:
            summary_payload["as_of_date"] = baseline_summary["end"]

    scan_all = pd.concat(scan_frames, ignore_index=True)
    best_all = pd.concat(best_frames, ignore_index=True).sort_values("hedge_ratio").reset_index(drop=True)
    scan_all.to_csv(SCAN_CSV, index=False, encoding="utf-8-sig")
    best_all.to_csv(BEST_CSV, index=False, encoding="utf-8-sig")

    windows_df = build_window_table(compare_series)
    windows_df.to_csv(WINDOWS_CSV, index=False, encoding="utf-8-sig")
    opt_windows_df = build_window_table(optimized_series)
    opt_windows_df.to_csv(OPT_WINDOWS_CSV, index=False, encoding="utf-8-sig")

    build_plot({name: (1.0 + ret).cumprod() for name, ret in compare_series.items()})

    summary_payload["artifacts"] = {
        "scan_csv": str(SCAN_CSV),
        "best_csv": str(BEST_CSV),
        "windows_csv": str(WINDOWS_CSV),
        "optimized_windows_csv": str(OPT_WINDOWS_CSV),
        "plot_png": str(PLOT_PNG),
        "rebased_nav_csv": str(REBASed_CSV),
    }
    SUMMARY_JSON.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary_payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
