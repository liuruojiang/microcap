from __future__ import annotations

import argparse
import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd

import analyze_microcap_zz1000_hedge as hedge_mod
import scan_top100_momentum_costs as cost_mod


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"
DEFAULT_MICROCAP_CSV = ROOT / "wind_microcap_top_50_biweekly_thursday_16y_cached.csv"
DEFAULT_TURNOVER_CSV = ROOT / "microcap_top50_biweekly_thursday_turnover_stats.csv"
DEFAULT_OUTPUT_PREFIX = "microcap_top50_momentum_biweekly_cost_scan"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scan Top 50 biweekly relative-momentum lookbacks under the stock-basket cost model."
    )
    parser.add_argument("--panel-path", type=Path, default=hedge_mod.DEFAULT_PANEL)
    parser.add_argument("--microcap-csv", type=Path, default=DEFAULT_MICROCAP_CSV)
    parser.add_argument("--turnover-csv", type=Path, default=DEFAULT_TURNOVER_CSV)
    parser.add_argument("--hedge-column", default=hedge_mod.DEFAULT_HEDGE_COLUMN)
    parser.add_argument("--lookback-start", type=int, default=1)
    parser.add_argument("--lookback-end", type=int, default=20)
    parser.add_argument("--futures-drag", type=float, default=hedge_mod.DEFAULT_FUTURES_DRAG)
    parser.add_argument("--output-prefix", default=DEFAULT_OUTPUT_PREFIX)
    return parser.parse_args()


def build_close_df(args: argparse.Namespace) -> pd.DataFrame:
    ns = SimpleNamespace(
        panel_path=args.panel_path,
        microcap_column=hedge_mod.DEFAULT_MICROCAP_COLUMN,
        hedge_column=args.hedge_column,
        lookback=args.lookback_end,
        signal_model="momentum",
        bias_n=hedge_mod.DEFAULT_BIAS_N,
        bias_mom_day=hedge_mod.DEFAULT_BIAS_MOM_DAY,
        futures_drag=args.futures_drag,
        r2_window=hedge_mod.DEFAULT_R2_WINDOW,
        r2_threshold=0.0,
        vol_scale_enabled=False,
        target_vol=hedge_mod.DEFAULT_TARGET_VOL,
        vol_window=hedge_mod.DEFAULT_VOL_WINDOW,
        max_lev=hedge_mod.DEFAULT_MAX_LEV,
        min_lev=hedge_mod.DEFAULT_MIN_LEV,
        scale_threshold=hedge_mod.DEFAULT_SCALE_THRESHOLD,
        microcap_csv=args.microcap_csv,
        microcap_date_col="date",
        microcap_close_col="close",
    )
    return hedge_mod.build_close_df(ns)


def summarize_scan_row(lookback: int, gross: pd.DataFrame, net: pd.DataFrame) -> dict[str, object]:
    gross_metrics = hedge_mod.calc_metrics(gross["return"])
    net_metrics = hedge_mod.calc_metrics(net["return_net"])
    active = gross["holding"].ne("cash")
    active_prev = active.shift(1, fill_value=False)
    dd_info = cost_mod.calc_drawdown_info(net["return_net"])
    return {
        "lookback": lookback,
        "gross_annual": gross_metrics.annual,
        "gross_max_dd": gross_metrics.max_dd,
        "gross_sharpe": gross_metrics.sharpe,
        "gross_vol": gross_metrics.vol,
        "gross_total_return": gross_metrics.total_return,
        "net_annual": net_metrics.annual,
        "net_max_dd": net_metrics.max_dd,
        "net_sharpe": net_metrics.sharpe,
        "net_vol": net_metrics.vol,
        "net_total_return": net_metrics.total_return,
        "active_days_pct": float(active.mean()),
        "signal_changes": int(gross["signal_on"].ne(gross["signal_on"].shift()).sum() - 1),
        "entry_days": int((active & ~active_prev).sum()),
        "exit_days": int((~active & active_prev).sum()),
        "rebalance_cost_days": int(net["rebalance_cost"].gt(0).sum()),
        "entry_exit_cost_sum": float(net["entry_exit_cost"].sum()),
        "rebalance_cost_sum": float(net["rebalance_cost"].sum()),
        "total_cost_sum": float(net["total_cost"].sum()),
        "peak_date": dd_info["peak_date"],
        "trough_date": dd_info["trough_date"],
        "recovery_date": dd_info["recovery_date"],
    }


def build_position_payload(scan_df: pd.DataFrame, lookbacks: list[int]) -> dict[str, object]:
    ordered = scan_df.sort_values(["net_sharpe", "net_annual"], ascending=[False, False]).reset_index(drop=True)
    payload: dict[str, object] = {
        "ranking_rule": "sort by net_sharpe desc, then net_annual desc",
        "universe": {
            "top_n": 50,
            "rebalance": "biweekly_thursday",
        },
        "cost_model": {
            "entry_buy_one_side": cost_mod.ENTRY_COST,
            "exit_sell_one_side": cost_mod.EXIT_COST,
            "rebalance_one_side": cost_mod.MONTHLY_REBALANCE_ONE_SIDE,
            "rebalance_cost_formula": "2 * 0.003 * replaced_fraction",
            "note": "Only microcap stock basket cost is added. Futures leg keeps daily drag 3/10000.",
        },
    }
    for lb in lookbacks:
        subset = ordered.loc[ordered["lookback"] == lb]
        if subset.empty:
            continue
        payload[f"lookback_{lb}_rank"] = {
            "rank": int(subset.index[0] + 1),
            "total": int(len(ordered)),
            "target_row": subset.iloc[0].to_dict(),
            "top10": ordered.head(10).to_dict(orient="records"),
        }
    return payload


def main() -> None:
    args = parse_args()
    close_df = build_close_df(args)
    turnover = cost_mod.load_turnover_table(args.turnover_csv)

    rows: list[dict[str, object]] = []
    best_nav: pd.DataFrame | None = None
    best_lb: int | None = None
    best_sharpe = -np.inf

    for lookback in range(args.lookback_start, args.lookback_end + 1):
        gross = hedge_mod.run_backtest(
            close_df=close_df,
            signal_model="momentum",
            lookback=lookback,
            bias_n=hedge_mod.DEFAULT_BIAS_N,
            bias_mom_day=hedge_mod.DEFAULT_BIAS_MOM_DAY,
            futures_drag=args.futures_drag,
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
        row = summarize_scan_row(lookback=lookback, gross=gross, net=net)
        rows.append(row)
        if row["net_sharpe"] > best_sharpe:
            best_sharpe = row["net_sharpe"]
            best_lb = lookback
            best_nav = net.copy()

    scan_df = pd.DataFrame(rows)
    scan_df = scan_df.sort_values(["net_sharpe", "net_annual"], ascending=[False, False]).reset_index(drop=True)
    scan_df["net_rank"] = np.arange(1, len(scan_df) + 1)
    scan_df["gross_rank"] = scan_df["gross_sharpe"].rank(ascending=False, method="min").astype(int)
    scan_df = scan_df[
        [
            "lookback",
            "gross_rank",
            "net_rank",
            "gross_annual",
            "gross_max_dd",
            "gross_sharpe",
            "net_annual",
            "net_max_dd",
            "net_sharpe",
            "entry_days",
            "exit_days",
            "rebalance_cost_days",
            "entry_exit_cost_sum",
            "rebalance_cost_sum",
            "total_cost_sum",
            "active_days_pct",
            "signal_changes",
            "peak_date",
            "trough_date",
            "recovery_date",
        ]
    ]

    scan_path = ROOT / f"{args.output_prefix}.csv"
    position_path = ROOT / f"{args.output_prefix}_position.json"
    scan_df.to_csv(scan_path, index=False, encoding="utf-8")

    payload = build_position_payload(scan_df=scan_df, lookbacks=[5, 8, 16, 20])
    payload["best_lookback"] = best_lb
    position_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    if best_nav is not None and best_lb is not None:
        nav_path = ROOT / f"microcap_top50_mom{best_lb}_hedge_zz1000_biweekly_costed_nav.csv"
        best_nav.to_csv(nav_path, index_label="date", encoding="utf-8")

    print(scan_df.head(10).to_string(index=False))
    print(f"saved {scan_path.name}")
    print(f"saved {position_path.name}")


if __name__ == "__main__":
    main()
