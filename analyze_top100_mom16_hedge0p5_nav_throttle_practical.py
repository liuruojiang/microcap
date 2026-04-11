import json
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

import analyze_microcap_zz1000_hedge as hedge_mod
import analyze_top100_rebalance_frequency as freq_mod
import microcap_top100_mom16_biweekly_live as mom_mod
from analyze_top100_mom16_v1_1_nav_throttle_practical import (
    PracticalThrottleConfig,
    apply_practical_throttle,
)


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"
HEDGE_RATIO = 0.5
THROTTLE_CFG = PracticalThrottleConfig(
    dd_moderate=0.04,
    dd_severe=0.08,
    scale_moderate=0.85,
    scale_severe=0.70,
    recover_dd=0.03,
    rebal_cost_bps=2.0,
)

BASELINE_NAV_CSV = OUTPUT_DIR / "microcap_top100_mom16_hedge_zz1000_0p5x_biweekly_thursday_16y_costed_nav.csv"
THROTTLED_NAV_CSV = OUTPUT_DIR / "microcap_top100_mom16_hedge_zz1000_0p5x_nav_throttle_4_8_biweekly_thursday_16y_costed_nav.csv"
WINDOWS_CSV = OUTPUT_DIR / "microcap_top100_mom16_hedge0p5_nav_throttle_practical_windows.csv"
SUMMARY_JSON = OUTPUT_DIR / "microcap_top100_mom16_hedge0p5_nav_throttle_practical_summary.json"
PLOT_PNG = OUTPUT_DIR / "microcap_top100_mom16_hedge0p5_nav_throttle_practical_recent10y_compare.png"
REBASed_CSV = OUTPUT_DIR / "microcap_top100_mom16_hedge0p5_nav_throttle_practical_recent10y_rebased_nav.csv"


def load_turnover() -> pd.DataFrame:
    preferred = OUTPUT_DIR / "microcap_top100_biweekly_thursday_turnover_stats_live_20260409.csv"
    path = preferred if preferred.exists() else OUTPUT_DIR / "microcap_top100_biweekly_thursday_turnover_stats.csv"
    turnover = pd.read_csv(path)
    turnover["rebalance_date"] = pd.to_datetime(turnover["rebalance_date"])
    return turnover.sort_values("rebalance_date").reset_index(drop=True)


def load_gross_result() -> pd.DataFrame:
    panel_path = mom_mod.OUTPUT_DIR / "microcap_top100_mom16_biweekly_live_panel_refreshed.csv"
    close_df = mom_mod.load_close_df(panel_path, mom_mod.DEFAULT_INDEX_CSV)
    result = mom_mod.run_signal(close_df).copy()
    result.index = pd.to_datetime(result.index)
    return result


def apply_fixed_hedge_ratio(gross: pd.DataFrame, hedge_ratio: float) -> pd.DataFrame:
    out = gross.copy()
    active = out["holding"].ne("cash")
    out["hedge_ratio"] = hedge_ratio
    out["futures_drag"] = active.astype(float) * mom_mod.FUTURES_DRAG * hedge_ratio
    out["active_spread_ret"] = 0.0
    valid = active & out["microcap_ret"].notna() & out["hedge_ret"].notna()
    out.loc[valid, "active_spread_ret"] = out.loc[valid, "microcap_ret"] - hedge_ratio * out.loc[valid, "hedge_ret"]
    out["return_raw"] = out["active_spread_ret"] - out["futures_drag"]
    out["return"] = out["return_raw"]
    out["nav"] = (1.0 + out["return"]).cumprod()
    return out


def summarize_returns(ret: pd.Series) -> dict:
    metrics = hedge_mod.calc_metrics(ret.fillna(0.0))
    nav = (1.0 + ret.fillna(0.0)).cumprod()
    return {
        "annual_return": float(metrics.annual),
        "annual_vol": float(metrics.vol),
        "sharpe": float(metrics.sharpe),
        "max_drawdown": float(metrics.max_dd),
        "total_return": float(metrics.total_return),
        "start": str(pd.Timestamp(ret.index.min()).date()),
        "end": str(pd.Timestamp(ret.index.max()).date()),
        "days": int(len(ret)),
        "latest_nav": float(nav.iloc[-1]),
    }


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
    plt.title("Top100 Mom16 0.5x Hedge With Practical NAV Throttle - Recent 10Y")
    plt.ylabel("Rebased NAV")
    plt.grid(alpha=0.25)
    plt.legend()
    plt.tight_layout()
    plt.savefig(PLOT_PNG, dpi=160)
    plt.close()


def main() -> None:
    turnover = load_turnover()
    gross = load_gross_result()
    gross_ratio = apply_fixed_hedge_ratio(gross, HEDGE_RATIO)
    baseline = freq_mod.cost_mod.apply_cost_model(gross_ratio, turnover)
    baseline.index = pd.to_datetime(baseline.index)
    baseline.to_csv(BASELINE_NAV_CSV, encoding="utf-8-sig")

    throttled = baseline.copy()
    throttle_run = apply_practical_throttle(baseline["return_net"].fillna(0.0), THROTTLE_CFG)
    throttled["nav_scale"] = throttle_run["scale"]
    throttled["nav_state"] = throttle_run["state"]
    throttled["nav_prev_drawdown"] = throttle_run["prev_drawdown"]
    throttled["nav_turnover"] = throttle_run["turnover"]
    throttled["nav_control_cost"] = throttle_run["cost"]
    throttled["return_nav_throttled"] = throttle_run["return"]
    throttled["nav_nav_throttled"] = (1.0 + throttled["return_nav_throttled"].fillna(0.0)).cumprod()
    throttled.to_csv(THROTTLED_NAV_CSV, encoding="utf-8-sig")

    baseline_ret = baseline["return_net"].fillna(0.0)
    throttled_ret = throttled["return_nav_throttled"].fillna(0.0)
    windows_df = build_window_table(
        {
            "hedge0p5_baseline": baseline_ret,
            "hedge0p5_nav4_8": throttled_ret,
        }
    )
    windows_df.to_csv(WINDOWS_CSV, index=False, encoding="utf-8-sig")

    baseline_nav = (1.0 + baseline_ret).cumprod()
    throttled_nav = (1.0 + throttled_ret).cumprod()
    build_plot(
        {
            "0.5x baseline": baseline_nav,
            "0.5x + nav 4/8": throttled_nav,
        }
    )

    summary = {
        "as_of_date": str(pd.Timestamp(baseline.index.max()).date()),
        "hedge_ratio": HEDGE_RATIO,
        "nav_throttle_config": {
            "dd_moderate": THROTTLE_CFG.dd_moderate,
            "dd_severe": THROTTLE_CFG.dd_severe,
            "scale_moderate": THROTTLE_CFG.scale_moderate,
            "scale_severe": THROTTLE_CFG.scale_severe,
            "recover_dd": THROTTLE_CFG.recover_dd,
            "rebal_cost_bps": THROTTLE_CFG.rebal_cost_bps,
            "timing_rule": "T close drawdown observed, T+1 scale applied",
            "rerisk_rule": "recover to full immediately once drawdown is back inside recover_dd",
        },
        "baseline_0p5": summarize_returns(baseline_ret),
        "baseline_0p5_nav_controlled": summarize_returns(throttled_ret),
        "nav_control_stats": {
            "avg_scale": float(throttle_run["scale"].mean()),
            "min_scale": float(throttle_run["scale"].min()),
            "share_below_1": float((throttle_run["scale"] < 0.999999).mean()),
            "severe_share": float((throttle_run["state"] == "severe").mean()),
            "n_scale_changes": int((throttle_run["turnover"] > 0).sum()),
            "annualized_turnover": float(throttle_run["turnover"].sum() / len(throttle_run) * 252.0),
        },
        "artifacts": {
            "baseline_nav_csv": str(BASELINE_NAV_CSV),
            "throttled_nav_csv": str(THROTTLED_NAV_CSV),
            "windows_csv": str(WINDOWS_CSV),
            "plot_png": str(PLOT_PNG),
            "rebased_nav_csv": str(REBASed_CSV),
        },
    }
    SUMMARY_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
