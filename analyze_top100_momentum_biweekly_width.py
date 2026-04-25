from __future__ import annotations

import argparse
import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd

import analyze_microcap_zz1000_hedge as hedge_mod
import microcap_top100_mom16_biweekly_live as live_mod
import scan_top100_momentum_costs as cost_mod


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "docs" / "top100_momentum_biweekly_width_20260424"
DEFAULT_MICROCAP_CSV = ROOT / "outputs" / "wind_microcap_top_100_biweekly_thursday_16y_cached.csv"
DEFAULT_TURNOVER_CSV = ROOT / "outputs" / "microcap_top100_mom16_biweekly_live_proxy_turnover.csv"
DEFAULT_BASELINE_COSTED_NAV = ROOT / "outputs" / "microcap_top100_mom16_hedge_zz1000_biweekly_thursday_16y_costed_nav.csv"
DEFAULT_OUTPUT_PREFIX = "top100_momentum_biweekly_width"
BASE_LOOKBACK = 16


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scan Top100 biweekly momentum lookback width using the current practical proxy and turnover table."
    )
    parser.add_argument("--panel-path", type=Path, default=hedge_mod.DEFAULT_PANEL)
    parser.add_argument("--microcap-csv", type=Path, default=DEFAULT_MICROCAP_CSV)
    parser.add_argument("--turnover-csv", type=Path, default=DEFAULT_TURNOVER_CSV)
    parser.add_argument("--baseline-costed-nav", type=Path, default=DEFAULT_BASELINE_COSTED_NAV)
    parser.add_argument("--hedge-column", default=hedge_mod.DEFAULT_HEDGE_COLUMN)
    parser.add_argument("--lookback-start", type=int, default=4)
    parser.add_argument("--lookback-end", type=int, default=40)
    parser.add_argument("--base-lookback", type=int, default=BASE_LOOKBACK)
    parser.add_argument("--futures-drag", type=float, default=hedge_mod.DEFAULT_FUTURES_DRAG)
    parser.add_argument("--output-prefix", default=DEFAULT_OUTPUT_PREFIX)
    return parser.parse_args()


def build_close_df(args: argparse.Namespace) -> pd.DataFrame:
    close_df = live_mod.load_close_df(args.panel_path, args.microcap_csv)
    if len(close_df) < args.lookback_end + 3:
        raise ValueError(f"Not enough aligned rows for lookback_end={args.lookback_end}: got {len(close_df)}.")
    return close_df


def run_one(close_df: pd.DataFrame, turnover: pd.DataFrame, lookback: int, futures_drag: float) -> pd.DataFrame:
    gross = hedge_mod.run_backtest(
        close_df=close_df,
        signal_model="momentum",
        lookback=lookback,
        bias_n=hedge_mod.DEFAULT_BIAS_N,
        bias_mom_day=hedge_mod.DEFAULT_BIAS_MOM_DAY,
        futures_drag=futures_drag,
        require_positive_microcap_mom=False,
        r2_window=hedge_mod.DEFAULT_R2_WINDOW,
        r2_threshold=0.0,
        vol_scale_enabled=False,
        target_vol=hedge_mod.DEFAULT_TARGET_VOL,
        vol_window=hedge_mod.DEFAULT_VOL_WINDOW,
        max_lev=hedge_mod.DEFAULT_MAX_LEV,
        min_lev=hedge_mod.DEFAULT_MIN_LEV,
        scale_threshold=hedge_mod.DEFAULT_SCALE_THRESHOLD,
    )
    net = cost_mod.apply_cost_model(gross, turnover)
    return net


def calc_window_metrics(ret: pd.Series) -> dict[str, float]:
    metrics = hedge_mod.calc_metrics(ret)
    return {
        "annual": float(metrics.annual),
        "max_dd": float(metrics.max_dd),
        "sharpe": float(metrics.sharpe),
        "vol": float(metrics.vol),
        "total_return": float(metrics.total_return),
    }


def slice_returns(ret: pd.Series, years: int | None) -> pd.Series:
    clean = ret.dropna()
    if years is None:
        return clean
    start = clean.index.max() - pd.DateOffset(years=years)
    return clean.loc[clean.index >= start]


def summarize_windows(result_map: dict[int, pd.DataFrame], base_lookback: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    window_defs: list[tuple[str, int | None]] = [
        ("last_3y", 3),
        ("last_5y", 5),
        ("last_10y", 10),
        ("full_common", None),
    ]

    rows: list[dict[str, object]] = []
    band_rows: list[dict[str, object]] = []

    for window_name, years in window_defs:
        base_ret = slice_returns(result_map[base_lookback]["return_net"], years)
        base_metrics = calc_window_metrics(base_ret)
        strict_members: list[int] = []
        loose_members: list[int] = []

        for lookback, result in sorted(result_map.items()):
            ret = slice_returns(result["return_net"], years)
            metrics = calc_window_metrics(ret)
            rows.append(
                {
                    "window": window_name,
                    "lookback": lookback,
                    "annual": metrics["annual"],
                    "max_dd": metrics["max_dd"],
                    "sharpe": metrics["sharpe"],
                    "vol": metrics["vol"],
                    "total_return": metrics["total_return"],
                    "base_annual": base_metrics["annual"],
                    "base_max_dd": base_metrics["max_dd"],
                    "base_sharpe": base_metrics["sharpe"],
                    "annual_delta": metrics["annual"] - base_metrics["annual"],
                    "max_dd_delta": metrics["max_dd"] - base_metrics["max_dd"],
                    "sharpe_delta": metrics["sharpe"] - base_metrics["sharpe"],
                }
            )

            strict_ok = (
                metrics["sharpe"] >= base_metrics["sharpe"] * 0.90
                and metrics["max_dd"] >= base_metrics["max_dd"] - 0.03
            )
            loose_ok = (
                metrics["sharpe"] >= base_metrics["sharpe"] * 0.85
                and metrics["max_dd"] >= base_metrics["max_dd"] - 0.05
            )
            if strict_ok:
                strict_members.append(lookback)
            if loose_ok:
                loose_members.append(lookback)

        band_rows.append(
            {
                "window": window_name,
                "base_lookback": base_lookback,
                "base_annual": base_metrics["annual"],
                "base_max_dd": base_metrics["max_dd"],
                "base_sharpe": base_metrics["sharpe"],
                "strict_members": ",".join(str(v) for v in strict_members),
                "loose_members": ",".join(str(v) for v in loose_members),
            }
        )

    scan_df = pd.DataFrame(rows).sort_values(["window", "sharpe", "annual"], ascending=[True, False, False])
    band_df = pd.DataFrame(band_rows)
    return scan_df, band_df


def validate_baseline(result_map: dict[int, pd.DataFrame], baseline_costed_nav: Path, base_lookback: int) -> dict[str, object]:
    payload: dict[str, object] = {"baseline_costed_nav_exists": baseline_costed_nav.exists()}
    if not baseline_costed_nav.exists():
        return payload

    saved = pd.read_csv(baseline_costed_nav, parse_dates=["date"]).set_index("date").sort_index()
    fresh = result_map[base_lookback].copy().sort_index()
    common = saved.index.intersection(fresh.index)
    payload["common_rows"] = int(len(common))
    if len(common) == 0:
        return payload

    nav_col = "nav_net" if "nav_net" in saved.columns else None
    if nav_col is None:
        return payload

    diff = (saved.loc[common, nav_col] - fresh.loc[common, "nav_net"]).abs()
    payload["max_abs_nav_diff"] = float(diff.max())
    payload["end_saved_nav"] = float(saved.loc[common[-1], nav_col])
    payload["end_fresh_nav"] = float(fresh.loc[common[-1], "nav_net"])
    payload["validation_pass"] = bool(diff.max() < 1e-9)
    return payload


def main() -> None:
    args = parse_args()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    close_df = build_close_df(args)
    turnover = cost_mod.load_turnover_table(args.turnover_csv)
    result_map: dict[int, pd.DataFrame] = {}
    summary_rows: list[dict[str, object]] = []

    for lookback in range(args.lookback_start, args.lookback_end + 1):
        net = run_one(close_df=close_df, turnover=turnover, lookback=lookback, futures_drag=args.futures_drag)
        result_map[lookback] = net
        metrics = calc_window_metrics(net["return_net"])
        summary_rows.append(
            {
                "lookback": lookback,
                "annual": metrics["annual"],
                "max_dd": metrics["max_dd"],
                "sharpe": metrics["sharpe"],
                "total_return": metrics["total_return"],
                "active_days_pct": float(net["holding"].ne("cash").mean()),
                "entry_exit_cost_sum": float(net["entry_exit_cost"].sum()),
                "rebalance_cost_sum": float(net["rebalance_cost"].sum()),
                "total_cost_sum": float(net["total_cost"].sum()),
            }
        )

    summary_df = pd.DataFrame(summary_rows).sort_values(["sharpe", "annual"], ascending=[False, False])
    scan_df, band_df = summarize_windows(result_map=result_map, base_lookback=args.base_lookback)
    validation = validate_baseline(
        result_map=result_map,
        baseline_costed_nav=args.baseline_costed_nav,
        base_lookback=args.base_lookback,
    )

    summary_path = OUTPUT_DIR / f"{args.output_prefix}_fullscan.csv"
    windows_path = OUTPUT_DIR / f"{args.output_prefix}_windows.csv"
    bands_path = OUTPUT_DIR / f"{args.output_prefix}_bands.csv"
    validation_path = OUTPUT_DIR / f"{args.output_prefix}_validation.json"

    summary_df.to_csv(summary_path, index=False, encoding="utf-8")
    scan_df.to_csv(windows_path, index=False, encoding="utf-8")
    band_df.to_csv(bands_path, index=False, encoding="utf-8")
    validation_path.write_text(json.dumps(validation, ensure_ascii=False, indent=2), encoding="utf-8")

    top_rows = summary_df.head(10)[["lookback", "annual", "sharpe", "max_dd"]]
    print("Top full-sample rows:")
    print(top_rows.to_string(index=False))
    print("Bands:")
    print(band_df.to_string(index=False))
    print("Validation:")
    print(json.dumps(validation, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
