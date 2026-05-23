from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import microcap_top100_mom16_biweekly_live_v2_3 as v23  # noqa: E402


TRADING_DAYS = int(v23.TRADING_DAYS)
DEFAULT_RUN_FOLDER = ROOT / "quant_param_scan_runs" / "20260523_microcap_v2_3_microcap_only_original_momentum_cost_only"


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        value = float(value)
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, np.bool_):
        return bool(value)
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(_json_safe(payload), ensure_ascii=False, indent=2), encoding="utf-8")


def _git(args: list[str]) -> str:
    try:
        return subprocess.check_output(["git", *args], cwd=ROOT, text=True, stderr=subprocess.STDOUT).strip()
    except Exception as exc:  # pragma: no cover
        return f"unavailable: {exc}"


def _metrics(ret: pd.Series) -> dict[str, float | int]:
    r = pd.to_numeric(ret, errors="coerce").fillna(0.0).astype(float)
    rows = int(len(r))
    if rows <= 0:
        return {
            "rows": 0,
            "ann_return": np.nan,
            "ann_vol": np.nan,
            "sharpe_repo": np.nan,
            "max_dd": np.nan,
            "final_nav": np.nan,
        }
    nav = (1.0 + r).cumprod()
    final_nav = float(nav.iloc[-1])
    ann_return = final_nav ** (TRADING_DAYS / rows) - 1.0 if final_nav > 0 else np.nan
    ann_vol = float(r.std(ddof=1) * math.sqrt(TRADING_DAYS)) if rows > 1 else 0.0
    sharpe = ann_return / ann_vol if ann_vol and math.isfinite(ann_vol) else np.nan
    dd = nav / nav.cummax() - 1.0
    return {
        "rows": rows,
        "ann_return": float(ann_return),
        "ann_vol": float(ann_vol),
        "sharpe_repo": float(sharpe),
        "max_dd": float(dd.min()),
        "final_nav": final_nav,
    }


def _window_slices(index: pd.DatetimeIndex) -> dict[str, tuple[pd.Timestamp, pd.Timestamp]]:
    end = pd.Timestamp(index.max())
    first = pd.Timestamp(index.min())
    windows = {"full": (first, end)}
    for years in (10, 5, 3, 1):
        windows[f"last_{years}y"] = (max(first, end - pd.DateOffset(years=years)), end)
    return windows


def _build_microcap_only_gross(close_df: pd.DataFrame, official_index: pd.DatetimeIndex) -> pd.DataFrame:
    close_df = close_df.sort_index()
    micro_ret = close_df["microcap"].pct_change(fill_method=None)
    hedge_ret = close_df["hedge"].pct_change(fill_method=None)
    micro_nav = (1.0 + micro_ret.fillna(0.0)).cumprod()
    micro_nav.name = "microcap_nav"
    score_frame = v23.log_wls_score_and_r2(micro_nav, lookback=v23.LOOKBACK, halflife=v23.HALFLIFE)
    valid = score_frame["annualized_log_wls_score"].notna()
    common_index = pd.DatetimeIndex(score_frame.index[valid])
    common_index = pd.DatetimeIndex(common_index.intersection(pd.DatetimeIndex(official_index)))
    common_index = common_index[common_index >= v23.FORMAL_START_DATE].sort_values()

    score = pd.to_numeric(score_frame["annualized_log_wls_score"].loc[common_index], errors="coerce")
    r2 = pd.to_numeric(score_frame["log_wls_r2"].loc[common_index], errors="coerce")
    signal_on = score.gt(0.0)
    current_active = signal_on.shift(1, fill_value=False)
    ret = micro_ret.loc[common_index].fillna(0.0)
    gross_ret = pd.Series(np.where(current_active, ret, 0.0), index=common_index, dtype=float)
    holding = np.where(current_active, "long_microcap_top100", "cash")
    next_holding = np.where(signal_on, "long_microcap_top100", "cash")

    out = pd.DataFrame(
        {
            "return_raw": gross_ret,
            "return": gross_ret,
            "holding": holding,
            "next_holding": next_holding,
            "signal_on": signal_on.astype(bool),
            "microcap_close": close_df["microcap"].loc[common_index],
            "hedge_close": close_df["hedge"].loc[common_index],
            "microcap_ret": micro_ret.loc[common_index],
            "hedge_ret": hedge_ret.loc[common_index],
            "microcap_mom": score,
            "hedge_mom": 0.0,
            "momentum_gap": score,
            "annualized_log_wls_score": score,
            "log_wls_r2": r2,
            "microcap_nav": micro_nav.loc[common_index],
            "signal_score_label": "microcap_only_annualized_log_wls_score",
            "futures_drag": 0.0,
            "active_spread_ret": pd.Series(np.where(current_active, ret, 0.0), index=common_index, dtype=float),
            "weight": 1.0,
            "target_vol_execution_scale": 1.0,
        },
        index=common_index,
    )
    out["nav_gross"] = (1.0 + out["return"].fillna(0.0)).cumprod()
    return out


def _build_window_tables(net: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows: list[dict[str, Any]] = []
    wide: dict[str, Any] = {
        "candidate": "microcap_only_original_momentum_cost_only",
        "lookback": int(v23.LOOKBACK),
        "halflife": float(v23.HALFLIFE),
        "signal_threshold": 0.0,
        "hedge_removed": True,
        "target_vol_enabled": False,
        "cash_yield_enabled": False,
    }
    for segment, (start, end) in _window_slices(pd.DatetimeIndex(net.index)).items():
        part = net.loc[(net.index >= start) & (net.index <= end)]
        m = _metrics(part["return_net"])
        active = part["holding"].astype(str).ne("cash")
        prev_active = active.shift(1, fill_value=False)
        row = {
            "candidate": "microcap_only_original_momentum_cost_only",
            "segment": segment,
            "start": str(pd.Timestamp(start).date()),
            "end": str(pd.Timestamp(end).date()),
            "rows": int(m["rows"]),
            "ann_return": m["ann_return"],
            "ann_vol": m["ann_vol"],
            "sharpe_repo": m["sharpe_repo"],
            "max_dd": m["max_dd"],
            "final_nav": m["final_nav"],
            "holding_days": int(active.sum()),
            "holding_day_ratio": float(active.mean()) if len(part) else np.nan,
            "entry_days": int((active & ~prev_active).sum()),
            "exit_days": int((~active & prev_active).sum()),
            "entry_exit_cost_sum": float(pd.to_numeric(part.get("entry_exit_cost", 0.0), errors="coerce").fillna(0.0).sum()),
            "rebalance_cost_sum": float(pd.to_numeric(part.get("rebalance_cost", 0.0), errors="coerce").fillna(0.0).sum()),
            "total_cost_sum": float(pd.to_numeric(part.get("total_cost", 0.0), errors="coerce").fillna(0.0).sum()),
        }
        rows.append(row)
        for metric in ("ann_return", "ann_vol", "sharpe_repo", "max_dd", "final_nav", "holding_day_ratio", "total_cost_sum"):
            wide[f"{metric}_{segment}"] = row[metric]
    return pd.DataFrame(rows), pd.DataFrame([wide])


def _write_record(run_folder: Path, wide: pd.DataFrame, meta: dict[str, Any]) -> None:
    row = wide.iloc[0]
    lines = [
        "# Microcap Top100 v2.3 Microcap-Only Original Momentum",
        "",
        "## Scope",
        "",
        "- Baseline source: `microcap_top100_mom16_biweekly_live_v2_3.py`.",
        "- Signal: annualized log-WLS score on microcap Top100 NAV only.",
        "- Position rule: hold microcap Top100 when previous close-confirmed score is positive; otherwise cash.",
        "- Removed: ZZ1000 hedge leg, futures drag, spread NAV, exit buffer, target-vol scaling, financing, cash-day yield, R2 gate, peak decay, and broad-volume overlays.",
        "- Retained: Top100 basket transaction-cost model from the existing turnover table.",
        "",
        "## Data",
        "",
        f"- Start: {meta['data_snapshot']['metrics_start']}",
        f"- End: {meta['data_snapshot']['metrics_end']}",
        f"- Rows: {meta['data_snapshot']['rows']}",
        f"- Turnover rows: {meta['data_snapshot']['turnover_rows']}",
        "- Annualization: 244 trading days.",
        "",
        "## Outputs",
        "",
        "- `daily_microcap_only_original_momentum_cost_only.csv`",
        "- `scan_summary.csv`",
        "- `window_metrics.csv`",
        "- `scan_meta.json`",
        "",
        "## Results",
        "",
        f"- Full: annual return {row['ann_return_full']:.4%}, max drawdown {row['max_dd_full']:.4%}, Sharpe {row['sharpe_repo_full']:.3f}, final NAV {row['final_nav_full']:.4f}.",
        f"- 10Y: annual return {row['ann_return_last_10y']:.4%}, max drawdown {row['max_dd_last_10y']:.4%}.",
        f"- 5Y: annual return {row['ann_return_last_5y']:.4%}, max drawdown {row['max_dd_last_5y']:.4%}.",
        f"- 3Y: annual return {row['ann_return_last_3y']:.4%}, max drawdown {row['max_dd_last_3y']:.4%}.",
        f"- 1Y: annual return {row['ann_return_last_1y']:.4%}, max drawdown {row['max_dd_last_1y']:.4%}.",
        "",
        "## Verification",
        "",
        f"- Costed NAV <= gross NAV on same stream: {meta['verification']['costed_final_nav_lte_gross_final_nav']}.",
        f"- Gross final NAV: {meta['verification']['gross_final_nav']:.6f}.",
        f"- Costed final NAV: {meta['verification']['costed_final_nav']:.6f}.",
    ]
    (run_folder / "record.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(run_folder: Path) -> None:
    started = time.time()
    run_folder.mkdir(parents=True, exist_ok=True)
    command_log = run_folder / "command_log.txt"
    with command_log.open("a", encoding="utf-8") as fh:
        fh.write(f"[{pd.Timestamp.now().isoformat()}] python scripts/run_microcap_v2_3_microcap_only_original_momentum.py --run-folder {run_folder}\n")

    reference_summary, _signal_df, official_out = v23.generate_v2_3_outputs()
    _reference_summary2, base_gross_cached, turnover_df = v23.v2_0.embedded_context._load_embedded_base_context()
    close_df = v23._close_df_from_base(base_gross_cached)
    gross = _build_microcap_only_gross(close_df, pd.DatetimeIndex(official_out.index))
    net = v23.v2_0.base_mod.freq_mod.cost_mod.apply_cost_model(gross, turnover_df)
    net["nav_gross"] = gross["nav_gross"]
    net["strategy_variant"] = "v2_3_microcap_only_original_momentum_cost_only"
    net["lookback"] = int(v23.LOOKBACK)
    net["halflife"] = float(v23.HALFLIFE)
    net["hedge_removed"] = True
    net["target_vol_enabled"] = False
    net["cash_yield_enabled"] = False

    summary, wide = _build_window_tables(net)
    daily_path = run_folder / "daily_microcap_only_original_momentum_cost_only.csv"
    net.rename_axis("date").reset_index().to_csv(daily_path, index=False, encoding="utf-8")
    summary.to_csv(run_folder / "scan_summary.csv", index=False, encoding="utf-8")
    wide.to_csv(run_folder / "window_metrics.csv", index=False, encoding="utf-8")

    gross_final_nav = float(gross["nav_gross"].iloc[-1])
    costed_final_nav = float(net["nav_net"].iloc[-1])
    meta = {
        "run_id": run_folder.name,
        "created_at": pd.Timestamp.now().isoformat(),
        "phase": "analysis_written",
        "project": "microcap Top100",
        "strategy": "v2.3 derived",
        "subsystem": "microcap-only original momentum",
        "repo_root": str(ROOT),
        "entrypoint": "scripts/run_microcap_v2_3_microcap_only_original_momentum.py",
        "base_entrypoint": "microcap_top100_mom16_biweekly_live_v2_3.py",
        "git_branch": _git(["branch", "--show-current"]),
        "git_commit": _git(["rev-parse", "HEAD"]),
        "git_status": _git(["status", "--short"]),
        "data_snapshot": {
            "metrics_start": str(pd.Timestamp(net.index.min()).date()),
            "metrics_end": str(pd.Timestamp(net.index.max()).date()),
            "rows": int(len(net)),
            "turnover_rows": int(len(turnover_df)),
            "reference_summary_latest_nav_date": reference_summary.get("latest_nav_date"),
            "close_df_start": str(pd.Timestamp(close_df.index.min()).date()),
            "close_df_end": str(pd.Timestamp(close_df.index.max()).date()),
        },
        "signal_model": {
            "type": "microcap_nav_log_wls_exp",
            "lookback": int(v23.LOOKBACK),
            "halflife": float(v23.HALFLIFE),
            "threshold": 0.0,
            "execution_timing": "next_session_after_close_confirmed_signal",
        },
        "removed_conditions": [
            "zz1000_hedge_leg",
            "futures_drag",
            "spread_nav_signal",
            "momentum_gap_exit_buffer",
            "target_vol_scaling",
            "financing_cost",
            "cash_day_yield",
            "r2_gate",
            "peak_decay",
            "broad_volume_overlay",
        ],
        "cost_model": {
            "retained": "top100_basket_transaction_cost_model",
            "entry_buy_one_side": float(v23.v2_0.base_mod.freq_mod.cost_mod.ENTRY_COST),
            "exit_sell_one_side": float(v23.v2_0.base_mod.freq_mod.cost_mod.EXIT_COST),
            "rebalance_cost_source": "turnover table via map_rebalance_apply_costs",
        },
        "verification": {
            "gross_final_nav": gross_final_nav,
            "costed_final_nav": costed_final_nav,
            "costed_final_nav_lte_gross_final_nav": bool(costed_final_nav <= gross_final_nav + 1e-12),
            "costed_rows_match_gross_rows": bool(len(net) == len(gross)),
        },
        "outputs": {
            "record": str(run_folder / "record.md"),
            "daily": str(daily_path),
            "scan_summary": str(run_folder / "scan_summary.csv"),
            "window_metrics": str(run_folder / "window_metrics.csv"),
            "scan_meta": str(run_folder / "scan_meta.json"),
            "command_log": str(command_log),
        },
        "elapsed_sec": round(time.time() - started, 3),
    }
    _write_json(run_folder / "scan_meta.json", meta)
    _write_record(run_folder, wide, meta)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-folder", type=Path, default=DEFAULT_RUN_FOLDER)
    args = parser.parse_args()
    run(args.run_folder)


if __name__ == "__main__":
    main()
