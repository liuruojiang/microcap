from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

import analyze_microcap_zz1000_hedge as hedge_mod
import analyze_top100_rebalance_frequency as freq_mod
import microcap_top100_mom16_biweekly_live as mom_mod


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"
RATIO_GRID = [round(x, 1) for x in [0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5]]
WINDOWS = [1, 3, 5, 10, 15]


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
    out.loc[valid, "active_spread_ret"] = (
        out.loc[valid, "microcap_ret"] - hedge_ratio * out.loc[valid, "hedge_ret"]
    )
    out["return_raw"] = out["active_spread_ret"] - out["futures_drag"]
    out["return"] = out["return_raw"]
    out["nav"] = (1.0 + out["return"]).cumprod()
    return out


def calc_drawdown_dates(ret: pd.Series) -> dict[str, object]:
    nav = (1.0 + ret.fillna(0.0)).cumprod()
    dd = nav / nav.cummax() - 1.0
    trough = dd.idxmin()
    peak = nav.loc[:trough].idxmax()
    recovery = nav.loc[trough:]
    recovery = recovery[recovery >= nav.loc[peak]]
    return {
        "peak_date": str(pd.Timestamp(peak).date()),
        "trough_date": str(pd.Timestamp(trough).date()),
        "recovery_date": None if recovery.empty else str(pd.Timestamp(recovery.index[0]).date()),
    }


def summarize_ratio(net: pd.DataFrame, hedge_ratio: float) -> dict[str, object]:
    ret = net["return_net"].fillna(0.0)
    metrics = hedge_mod.calc_metrics(ret)
    dd_dates = calc_drawdown_dates(ret)
    active_days = net["holding"].ne("cash")
    return {
        "hedge_ratio": hedge_ratio,
        "annual": metrics.annual,
        "max_dd": metrics.max_dd,
        "sharpe": metrics.sharpe,
        "vol": metrics.vol,
        "total_return": metrics.total_return,
        "entry_exit_cost_sum": float(net["entry_exit_cost"].sum()),
        "rebalance_cost_sum": float(net["rebalance_cost"].sum()),
        "total_cost_sum": float(net["total_cost"].sum()),
        "active_days_pct": float(active_days.mean()),
        **dd_dates,
    }


def summarize_windows(net: pd.DataFrame, hedge_ratio: float) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    last_date = pd.Timestamp(net.index.max())
    for yrs in WINDOWS:
        part = net.loc[net.index >= last_date - pd.DateOffset(years=yrs)].copy()
        ret = part["return_net"].fillna(0.0)
        metrics = hedge_mod.calc_metrics(ret)
        rows.append(
            {
                "hedge_ratio": hedge_ratio,
                "window_years": yrs,
                "start_date": str(pd.Timestamp(part.index.min()).date()),
                "end_date": str(pd.Timestamp(part.index.max()).date()),
                "annual": metrics.annual,
                "max_dd": metrics.max_dd,
                "sharpe": metrics.sharpe,
                "vol": metrics.vol,
                "total_return": float((1.0 + ret).prod() - 1.0),
            }
        )
    return rows


def main() -> None:
    turnover = load_turnover()
    gross = load_gross_result()
    summary_rows: list[dict[str, object]] = []
    window_rows: list[dict[str, object]] = []

    for hedge_ratio in RATIO_GRID:
        gross_ratio = apply_fixed_hedge_ratio(gross, hedge_ratio)
        net = freq_mod.cost_mod.apply_cost_model(gross_ratio, turnover)
        net.index = pd.to_datetime(net.index)
        summary_rows.append(summarize_ratio(net, hedge_ratio))
        window_rows.extend(summarize_windows(net, hedge_ratio))

    summary_df = pd.DataFrame(summary_rows).sort_values("hedge_ratio")
    windows_df = pd.DataFrame(window_rows).sort_values(["window_years", "hedge_ratio"])

    summary_path = OUTPUT_DIR / "microcap_top100_mom16_biweekly_live_hedge_ratio_scan_0p5_1p5.csv"
    windows_path = OUTPUT_DIR / "microcap_top100_mom16_biweekly_live_hedge_ratio_recent_windows_0p5_1p5.csv"
    json_path = OUTPUT_DIR / "microcap_top100_mom16_biweekly_live_hedge_ratio_summary_0p5_1p5.json"

    summary_df.to_csv(summary_path, index=False, encoding="utf-8")
    windows_df.to_csv(windows_path, index=False, encoding="utf-8")

    payload = {
        "strategy": "top100_mom16_biweekly_live_fixed_hedge_ratio_scan",
        "as_of_date": str(pd.Timestamp(gross.index.max()).date()),
        "ratio_grid": RATIO_GRID,
        "summary": summary_df.to_dict(orient="records"),
        "recent_windows": windows_df.to_dict(orient="records"),
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(summary_df.to_string(index=False))
    print(f"saved {summary_path.name}")
    print(f"saved {windows_path.name}")
    print(f"saved {json_path.name}")


if __name__ == "__main__":
    main()
