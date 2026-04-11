from __future__ import annotations

import argparse
import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd

import analyze_microcap_zz1000_hedge as hedge_mod


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"
DEFAULT_MICROCAP_CSV = OUTPUT_DIR / "wind_microcap_top_100_monthly_16y.csv"
DEFAULT_TURNOVER_CSV = OUTPUT_DIR / "microcap_top100_monthly_turnover_stats.csv"
DEFAULT_OUTPUT_PREFIX = "microcap_top100_momentum_16y_cost_scan"
ENTRY_COST = 0.003
EXIT_COST = 0.003
MONTHLY_REBALANCE_ONE_SIDE = 0.003


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scan Top 100 relative momentum lookbacks under the microcap trading cost model."
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


def load_turnover_table(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Turnover table not found: {path}")
    turnover = pd.read_csv(path)
    required = {"rebalance_date", "two_side_cost_rate"}
    missing = required.difference(turnover.columns)
    if missing:
        raise ValueError(f"Turnover table missing columns: {sorted(missing)}")
    turnover["rebalance_date"] = pd.to_datetime(turnover["rebalance_date"])
    return turnover.sort_values("rebalance_date").reset_index(drop=True)


def map_rebalance_apply_costs(index: pd.Index, turnover: pd.DataFrame) -> pd.Series:
    cost_series = pd.Series(0.0, index=index, dtype=float)
    date_array = index.to_numpy()
    for row in turnover.itertuples(index=False):
        pos = date_array.searchsorted(np.datetime64(row.rebalance_date), side="right")
        if pos < len(date_array):
            cost_series.iloc[pos] += float(row.two_side_cost_rate)
    return cost_series


def apply_cost_model(result: pd.DataFrame, turnover: pd.DataFrame) -> pd.DataFrame:
    out = result.copy()
    active = out["holding"].ne("cash")
    prev_active = active.shift(1, fill_value=False)

    entry_cost = pd.Series(0.0, index=out.index, dtype=float)
    entry_cost.loc[active & ~prev_active] = ENTRY_COST

    exit_cost = pd.Series(0.0, index=out.index, dtype=float)
    exit_cost.loc[~active & prev_active] = EXIT_COST

    rebalance_base = map_rebalance_apply_costs(out.index, turnover)
    rebalance_cost = rebalance_base.where(active & prev_active, 0.0)

    out["entry_exit_cost"] = entry_cost + exit_cost
    out["rebalance_cost"] = rebalance_cost
    out["total_cost"] = out["entry_exit_cost"] + out["rebalance_cost"]
    out["return_net"] = (1.0 + out["return"]) * (1.0 - out["total_cost"]) - 1.0
    out["nav_net"] = (1.0 + out["return_net"]).cumprod()
    return out


def calc_drawdown_info(ret: pd.Series) -> dict[str, object]:
    nav = (1.0 + ret).cumprod()
    dd = nav.div(nav.cummax()).sub(1.0)
    trough_date = dd.idxmin()
    peak_date = nav.loc[:trough_date].idxmax()
    post = nav.loc[trough_date:]
    recovery = post[post >= nav.loc[peak_date]]
    recovery_date = recovery.index[0] if len(recovery) else pd.NaT
    return {
        "peak_date": str(peak_date.date()),
        "trough_date": str(trough_date.date()),
        "recovery_date": None if pd.isna(recovery_date) else str(recovery_date.date()),
    }


def summarize_scan_row(lookback: int, gross: pd.DataFrame, net: pd.DataFrame) -> dict[str, object]:
    gross_metrics = hedge_mod.calc_metrics(gross["return"])
    net_metrics = hedge_mod.calc_metrics(net["return_net"])
    active = gross["holding"].ne("cash")
    active_prev = active.shift(1, fill_value=False)
    dd_info = calc_drawdown_info(net["return_net"])
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
        "cost_model": {
            "entry_buy_one_side": ENTRY_COST,
            "exit_sell_one_side": EXIT_COST,
            "monthly_rebalance_one_side": MONTHLY_REBALANCE_ONE_SIDE,
            "rebalance_cost_formula": "2 * 0.003 * replaced_fraction",
            "note": "Only microcap stock basket cost is added. Futures leg keeps daily drag 3/10000.",
        },
    }
    for lb in lookbacks:
        subset = ordered.loc[ordered["lookback"] == lb]
        if subset.empty:
            continue
        target = subset.iloc[0].to_dict()
        payload[f"lookback_{lb}_rank"] = {
            "rank": int(subset.index[0] + 1),
            "total": int(len(ordered)),
            "target_row": target,
            "top10": ordered.head(10).to_dict(orient="records"),
        }
    return payload


def main() -> None:
    args = parse_args()
    close_df = build_close_df(args)
    turnover = load_turnover_table(args.turnover_csv)

    rows: list[dict[str, object]] = []
    costed_nav_8: pd.DataFrame | None = None

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
        net = apply_cost_model(gross, turnover)
        if lookback == 8:
            costed_nav_8 = net.copy()
        rows.append(summarize_scan_row(lookback=lookback, gross=gross, net=net))

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

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    scan_path = OUTPUT_DIR / f"{args.output_prefix}.csv"
    position_path = OUTPUT_DIR / f"{args.output_prefix}_position.json"
    scan_df.to_csv(scan_path, index=False, encoding="utf-8")

    payload = build_position_payload(scan_df=scan_df, lookbacks=[8, 5])
    position_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    if costed_nav_8 is not None:
        nav8_path = OUTPUT_DIR / "microcap_top100_mom8_hedge_zz1000_16y_costed_nav.csv"
        costed_nav_8.to_csv(nav8_path, index_label="date", encoding="utf-8")

    print(scan_df.head(10).to_string(index=False))
    print(f"saved {scan_path.name}")
    print(f"saved {position_path.name}")


if __name__ == "__main__":
    main()
