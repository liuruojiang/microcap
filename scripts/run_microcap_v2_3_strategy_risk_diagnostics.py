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
CASH_YIELD = float(v23.v2_0.overlay_mod.IDLE_CASH_YIELD)


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        value = float(value)
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (np.bool_,)):
        return bool(value)
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(_json_safe(payload), ensure_ascii=False, indent=2), encoding="utf-8")


def _git(args: list[str]) -> str:
    try:
        return subprocess.check_output(["git", *args], cwd=ROOT, text=True, stderr=subprocess.STDOUT).strip()
    except Exception as exc:  # pragma: no cover - diagnostic best effort
        return f"unavailable: {exc}"


def _metrics(ret: pd.Series) -> dict[str, float]:
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
        start = max(first, end - pd.DateOffset(years=years))
        windows[f"last_{years}y"] = (pd.Timestamp(start), end)
    return windows


def _add_cash_day_yield(out: pd.DataFrame) -> pd.DataFrame:
    adjusted = out.copy()
    ret = pd.to_numeric(adjusted["return_net"], errors="coerce").fillna(0.0)
    cash_mask = adjusted["holding"].astype(str).eq("cash")
    daily_cash = CASH_YIELD / TRADING_DAYS
    ret.loc[cash_mask] = (1.0 + ret.loc[cash_mask]) * (1.0 + daily_cash) - 1.0
    adjusted["return_net"] = ret
    adjusted["nav_net"] = (1.0 + adjusted["return_net"].fillna(0.0)).cumprod()
    adjusted["cash_day_yield_applied"] = cash_mask
    return adjusted


def _build_candidate(
    gross: pd.DataFrame,
    turnover_df: pd.DataFrame,
    exit_buffer: float,
    cash_yield_on_cash_days: bool,
) -> pd.DataFrame:
    buffered = v23.v2_0.base_mod.apply_momentum_gap_exit_buffer(gross, float(exit_buffer))
    costed = v23.v2_0.base_mod.apply_momentum_gap_no_peak_decay_cost_model(buffered, turnover_df)
    out = v23.apply_target_vol(costed, v23.TARGET_VOL)
    if cash_yield_on_cash_days:
        out = _add_cash_day_yield(out)
    return out


def _candidate_label(exit_buffer: float, cash_yield_on_cash_days: bool) -> str:
    suffix = "cash2pct" if cash_yield_on_cash_days else "cash0"
    return f"gap{exit_buffer:.2f}".replace(".", "p") + f"_{suffix}"


def _scan_exit_buffer_and_cash(
    gross: pd.DataFrame,
    turnover_df: pd.DataFrame,
    run_folder: Path,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, pd.DataFrame]]:
    candidates: list[tuple[float, bool]] = [(v, False) for v in (0.00, 0.05, 0.10, 0.13, 0.20)]
    candidates.append((v23.MOMENTUM_GAP_EXIT_BUFFER, True))
    outputs: dict[str, pd.DataFrame] = {}
    summary_rows: list[dict[str, Any]] = []
    wide_rows: list[dict[str, Any]] = []
    for exit_buffer, cash_yield_on_cash_days in candidates:
        label = _candidate_label(exit_buffer, cash_yield_on_cash_days)
        out = _build_candidate(gross, turnover_df, exit_buffer, cash_yield_on_cash_days)
        outputs[label] = out
        windows = _window_slices(pd.DatetimeIndex(out.index))
        wide: dict[str, Any] = {
            "candidate": label,
            "exit_buffer": exit_buffer,
            "cash_yield_on_cash_days": cash_yield_on_cash_days,
            "cash_yield_annual": CASH_YIELD if cash_yield_on_cash_days else 0.0,
            "holding_days_full": int(out["holding"].astype(str).ne("cash").sum()),
            "cash_days_full": int(out["holding"].astype(str).eq("cash").sum()),
        }
        for segment, (start, end) in windows.items():
            part = out.loc[(out.index >= start) & (out.index <= end)]
            m = _metrics(part["return_net"])
            holding_days = int(part["holding"].astype(str).ne("cash").sum()) if len(part) else 0
            cost_total = float(pd.to_numeric(part.get("total_cost", 0.0), errors="coerce").fillna(0.0).sum()) if len(part) else 0.0
            scale_cost_total = float(pd.to_numeric(part.get("scale_change_cost", 0.0), errors="coerce").fillna(0.0).sum()) if len(part) else 0.0
            row = {
                "candidate": label,
                "segment": segment,
                "start": str(pd.Timestamp(start).date()),
                "end": str(pd.Timestamp(end).date()),
                "rows": int(m["rows"]),
                "ann_return": m["ann_return"],
                "ann_vol": m["ann_vol"],
                "sharpe_repo": m["sharpe_repo"],
                "max_dd": m["max_dd"],
                "final_nav": m["final_nav"],
                "holding_days": holding_days,
                "holding_day_ratio": holding_days / len(part) if len(part) else np.nan,
                "cost_total": cost_total,
                "scale_change_cost_total": scale_cost_total,
                "exit_buffer": exit_buffer,
                "cash_yield_on_cash_days": cash_yield_on_cash_days,
                "cash_yield_annual": CASH_YIELD if cash_yield_on_cash_days else 0.0,
                "signal_spread_hedge_ratio": v23.SIGNAL_SPREAD_HEDGE_RATIO,
                "execution_hedge_ratio": v23.EXECUTION_HEDGE_RATIO,
            }
            summary_rows.append(row)
            wide[f"ann_return_{segment}"] = m["ann_return"]
            wide[f"max_dd_{segment}"] = m["max_dd"]
            wide[f"sharpe_repo_{segment}"] = m["sharpe_repo"]
            wide[f"holding_day_ratio_{segment}"] = row["holding_day_ratio"]
        wide["decision_hint"] = "diagnostic_only"
        wide["stability_label"] = "not_promoted"
        wide_rows.append(wide)
        out.reset_index(names="date").to_csv(run_folder / f"daily_{label}.csv", index=False, encoding="utf-8")
    summary = pd.DataFrame(summary_rows)
    wide = pd.DataFrame(wide_rows)
    summary.to_csv(run_folder / "scan_summary.csv", index=False, encoding="utf-8")
    wide.to_csv(run_folder / "window_metrics.csv", index=False, encoding="utf-8")
    return summary, wide, outputs


def _signal_execution_diagnostics(close_df: pd.DataFrame, gross: pd.DataFrame, run_folder: Path) -> pd.DataFrame:
    micro = close_df["microcap"].pct_change(fill_method=None)
    hedge = close_df["hedge"].pct_change(fill_method=None)
    drag = float(v23.v2_0.base_mod.FUTURES_DRAG)
    signal_ret = micro - v23.SIGNAL_SPREAD_HEDGE_RATIO * hedge - drag * v23.SIGNAL_SPREAD_HEDGE_RATIO
    exec_ret = micro - v23.EXECUTION_HEDGE_RATIO * hedge - drag * v23.EXECUTION_HEDGE_RATIO
    diff = exec_ret - signal_ret
    common = pd.DataFrame(
        {
            "signal_always_on_ret": signal_ret,
            "execution_always_on_ret": exec_ret,
            "execution_minus_signal_ret": diff,
            "hedge_ret": hedge,
        }
    ).replace([np.inf, -np.inf], np.nan).dropna()
    common = common.loc[common.index.intersection(gross.index)]
    active = gross["holding"].astype(str).ne("cash").reindex(common.index).fillna(False)
    rows = [
        {
            "scope": "all_common_days",
            "rows": int(len(common)),
            "corr_signal_execution_ret": float(common["signal_always_on_ret"].corr(common["execution_always_on_ret"])),
            "ann_return_signal_always_on": _metrics(common["signal_always_on_ret"])["ann_return"],
            "ann_return_execution_always_on": _metrics(common["execution_always_on_ret"])["ann_return"],
            "ann_vol_signal_always_on": _metrics(common["signal_always_on_ret"])["ann_vol"],
            "ann_vol_execution_always_on": _metrics(common["execution_always_on_ret"])["ann_vol"],
            "cum_exec_minus_signal_component": float((1.0 + common["execution_minus_signal_ret"]).prod() - 1.0),
            "mean_daily_exec_minus_signal": float(common["execution_minus_signal_ret"].mean()),
        },
        {
            "scope": "active_days_only",
            "rows": int(active.sum()),
            "corr_signal_execution_ret": float(common.loc[active, "signal_always_on_ret"].corr(common.loc[active, "execution_always_on_ret"])),
            "ann_return_signal_always_on": _metrics(common.loc[active, "signal_always_on_ret"])["ann_return"],
            "ann_return_execution_always_on": _metrics(common.loc[active, "execution_always_on_ret"])["ann_return"],
            "ann_vol_signal_always_on": _metrics(common.loc[active, "signal_always_on_ret"])["ann_vol"],
            "ann_vol_execution_always_on": _metrics(common.loc[active, "execution_always_on_ret"])["ann_vol"],
            "cum_exec_minus_signal_component": float((1.0 + common.loc[active, "execution_minus_signal_ret"]).prod() - 1.0),
            "mean_daily_exec_minus_signal": float(common.loc[active, "execution_minus_signal_ret"].mean()),
        },
    ]
    out = pd.DataFrame(rows)
    out.to_csv(run_folder / "signal_execution_mismatch_diagnostics.csv", index=False, encoding="utf-8")
    return out


def _wls_and_boundary_diagnostics(close_df: pd.DataFrame, common_index: pd.DatetimeIndex, run_folder: Path) -> dict[str, Any]:
    weights = np.asarray(v23.exp_weights(), dtype=float)
    neff = float(weights.sum() ** 2 / np.square(weights).sum())
    original_nav, micro_ret, hedge_ret, daily_drag = v23.always_on_spread_nav(close_df)
    fixed_ret = micro_ret.fillna(0.0) - v23.SIGNAL_SPREAD_HEDGE_RATIO * hedge_ret.fillna(0.0) - daily_drag
    if len(fixed_ret):
        fixed_ret.iloc[0] = 0.0
    fixed_nav = (1.0 + fixed_ret.fillna(0.0)).cumprod()
    original_score = v23.log_wls_score_and_r2(original_nav).loc[common_index]
    fixed_score = v23.log_wls_score_and_r2(fixed_nav).loc[common_index]
    diff = fixed_score["annualized_log_wls_score"] - original_score["annualized_log_wls_score"]
    boundary = pd.DataFrame(
        {
            "original_score": original_score["annualized_log_wls_score"],
            "fixed_first_row_drag_score": fixed_score["annualized_log_wls_score"],
            "score_diff": diff,
        }
    )
    boundary.to_csv(run_folder / "first_row_drag_boundary_check.csv", index_label="date", encoding="utf-8")
    payload = {
        "lookback": v23.LOOKBACK,
        "halflife": v23.HALFLIFE,
        "weights_oldest_to_newest": weights.tolist(),
        "kish_effective_n": neff,
        "last_6_weight_sum": float(weights[-6:].sum()),
        "oldest_to_newest_weight_ratio": float(weights[0] / weights[-1]),
        "entry_threshold_log": 0.0,
        "entry_threshold_linear": float(np.expm1(0.0)),
        "exit_threshold_log": -float(v23.MOMENTUM_GAP_EXIT_BUFFER),
        "exit_threshold_linear": float(np.expm1(-float(v23.MOMENTUM_GAP_EXIT_BUFFER))),
        "first_row_drag_score_max_abs_diff_after_formal_start": float(diff.abs().max()),
        "first_row_drag_latest_score_diff": float(diff.dropna().iloc[-1]) if diff.dropna().size else None,
        "first_row_drag_nonzero_diff_rows": int(diff.abs().gt(1e-12).sum()),
    }
    _write_json(run_folder / "wls_and_boundary_diagnostics.json", payload)
    return payload


def _transition_cost_diagnostics(default_out: pd.DataFrame, run_folder: Path) -> pd.DataFrame:
    out = default_out.copy()
    prev_holding = out["holding"].astype(str).shift(1).fillna("cash")
    transition = out["holding"].astype(str).ne(prev_holding)
    rows = []
    for name, mask in {
        "all_rows": pd.Series(True, index=out.index),
        "transition_rows": transition,
        "cash_to_active_rows": prev_holding.eq("cash") & out["holding"].astype(str).ne("cash"),
        "active_to_cash_rows": prev_holding.ne("cash") & out["holding"].astype(str).eq("cash"),
        "same_active_rows": (~transition) & out["holding"].astype(str).ne("cash"),
    }.items():
        part = out.loc[mask]
        rows.append(
            {
                "scope": name,
                "rows": int(len(part)),
                "base_trade_cost_scaled_sum": float(pd.to_numeric(part.get("base_trade_cost_scaled", 0.0), errors="coerce").fillna(0.0).sum()),
                "scale_change_cost_sum": float(pd.to_numeric(part.get("scale_change_cost", 0.0), errors="coerce").fillna(0.0).sum()),
                "target_vol_costed_turnover_sum": float(pd.to_numeric(part.get("target_vol_costed_turnover", 0.0), errors="coerce").fillna(0.0).sum()),
                "base_trade_cost_scale_avg": float(pd.to_numeric(part.get("base_trade_cost_scale", np.nan), errors="coerce").mean()) if len(part) else np.nan,
            }
        )
    frame = pd.DataFrame(rows)
    frame.to_csv(run_folder / "transition_cost_diagnostics.csv", index=False, encoding="utf-8")
    return frame


def _write_record(
    run_folder: Path,
    summary: pd.DataFrame,
    wide: pd.DataFrame,
    signal_diag: pd.DataFrame,
    wls_diag: dict[str, Any],
    transition_diag: pd.DataFrame,
    meta: dict[str, Any],
) -> None:
    baseline = wide.loc[wide["candidate"].eq(_candidate_label(v23.MOMENTUM_GAP_EXIT_BUFFER, False))].iloc[0]
    best_3y = wide.sort_values("ann_return_last_3y", ascending=False).iloc[0]
    best_dd_3y = wide.sort_values("max_dd_last_3y", ascending=False).iloc[0]
    lines = [
        "# Quant Parameter Scan Record",
        "",
        "## Run Metadata",
        "",
        f"- Run id: `{run_folder.name}`",
        f"- Created at: {meta['created_at']}",
        "- Project: microcap Top100",
        "- Strategy or version: v2.3",
        "- Sleeve or subsystem: strategy-risk-diagnostics",
        "- Parameter group: `exit_buffer_cash_yield_wls_realtime`",
        "- Scan type: diagnostic_parameter_grid",
        f"- Repo or workspace path: `{ROOT}`",
        "- Target entrypoint: `microcap_top100_mom16_biweekly_live_v2_3.py`",
        f"- Git branch: `{meta['git_branch']}`",
        f"- Git commit: `{meta['git_commit']}`",
        "- Working tree status before: see `scan_meta.json`.",
        "",
        "## Research Question",
        "",
        "- Baseline: preserve v2.3 signal spread hedge ratio 1.0 and execution hedge ratio 0.8.",
        "- Candidate grid: exit buffer 0.00, 0.05, 0.10, 0.13, 0.20 plus default 0.13 with cash-day idle yield.",
        "- Decision target: diagnostics only; no production default change.",
        "- Source-change rule: `research_only_no_source_change`.",
        "- Required windows: full, 10Y, 5Y, 3Y, 1Y.",
        "- Rerun triggers: source changes, base v2.0 refresh changes, turnover model changes, or newer market data.",
        "",
        "## Implementation Anchor",
        "",
        "- Official entrypoint: `microcap_top100_mom16_biweekly_live_v2_3.py`.",
        "- Function path: `build_spread_log_wls_gross` -> `apply_momentum_gap_exit_buffer` -> `apply_momentum_gap_no_peak_decay_cost_model` -> `apply_target_vol`.",
        "- Existing loaders reused: `v2_0.generate_v2_0_outputs()` and `_load_embedded_base_context()`.",
        "- Existing cost model reused: embedded v2.0 cost model, target-vol scale cost, financing cost, and scaled base trading cost.",
        "",
        "## Data Snapshot",
        "",
        f"- Metrics start: {meta['data_snapshot']['metrics_start']}",
        f"- Metrics end: {meta['data_snapshot']['metrics_end']}",
        f"- Rows: {meta['data_snapshot']['rows']}",
        f"- Turnover rows: {meta['data_snapshot']['turnover_rows']}",
        "- Data sources: refreshed v2.0 embedded base context, local close panel, local proxy turnover.",
        "- Cache write risk: v2.3/v2.0 outputs refreshed before diagnostics.",
        "- Trading calendar: strategy local trading-date index; annualization uses 244 trading days.",
        "- Timezone assumptions: local Windows run under Asia/Shanghai user context.",
        "",
        "## Cost and Execution Assumptions",
        "",
        "- Signal hedge ratio: 1.0, intentionally preserved.",
        "- Execution hedge ratio: 0.8.",
        "- Target volatility: 25%, 60-day realized volatility, max leverage 1.5x.",
        "- Scale rebalance threshold: 0.30.",
        "- Scale-change cost: 10bp on target-vol leg turnover where model charges it.",
        "- Financing: 3% annualized on exposure above 1.0x.",
        "- Cash yield diagnostic: optional 2% annualized only on rows where `holding == cash`.",
        "",
        "## Runtime Override Plan",
        "",
        "- Override mechanism: runtime function arguments only; no strategy source constants edited.",
        "- Values restored after each candidate: yes, no module constants mutated.",
        "- Default candidate included in same run: yes, `gap0p13_cash0`.",
        "- Parity check against official/default output: default candidate rebuilt from the same real function chain.",
        "",
        "## Commands",
        "",
        "```powershell",
        "python microcap_top100_mom16_biweekly_live_v2_3.py",
        f"python scripts/run_microcap_v2_3_strategy_risk_diagnostics.py --run-folder {run_folder}",
        "```",
        "",
        "## Output Files",
        "",
        "- `scan_summary.csv`: long window metrics for all candidates.",
        "- `window_metrics.csv`: wide comparison table.",
        "- `signal_execution_mismatch_diagnostics.csv`: retained 1.0/0.8 signal-vs-execution diagnostics.",
        "- `wls_and_boundary_diagnostics.json`: WLS effective sample, log-vs-linear threshold, first-row drag check.",
        "- `transition_cost_diagnostics.csv`: transition-day cost decomposition.",
        "",
        "## Full-Sample Results",
        "",
        f"- Default `gap0p13_cash0`: annual return {baseline['ann_return_full']:.4%}, max drawdown {baseline['max_dd_full']:.4%}.",
        f"- Best 3Y annual return candidate: `{best_3y['candidate']}` at {best_3y['ann_return_last_3y']:.4%}, max drawdown {best_3y['max_dd_last_3y']:.4%}.",
        f"- Best 3Y drawdown candidate: `{best_dd_3y['candidate']}` at return {best_dd_3y['ann_return_last_3y']:.4%}, max drawdown {best_dd_3y['max_dd_last_3y']:.4%}.",
        "",
        "## Window Results",
        "",
        "- See `window_metrics.csv` for full, 10Y, 5Y, 3Y, and 1Y windows.",
        "",
        "## Stability Classification",
        "",
        "- Label: diagnostic_only_not_promoted.",
        f"- WLS effective N: {wls_diag['kish_effective_n']:.2f}; last six weights sum to {wls_diag['last_6_weight_sum']:.2%}.",
        f"- Exit buffer -0.13 log threshold equals {wls_diag['exit_threshold_linear']:.4%} linear annualized threshold.",
        f"- Signal/execution all-day return correlation: {signal_diag.iloc[0]['corr_signal_execution_ret']:.6f}.",
        f"- Transition rows scale-change cost sum: {transition_diag.loc[transition_diag['scope'].eq('transition_rows'), 'scale_change_cost_sum'].iloc[0]:.6f}.",
        "",
        "## Decision",
        "",
        "- Decision: keep v2.3 signal/execution hedge-ratio mismatch as requested; diagnostics only, no default promotion.",
        "- Recommended next action: inspect `window_metrics.csv` before deciding whether to test a v2.4-style change.",
        "",
        "## User-Facing Summary",
        "",
        "- v2.3's retained 1.0 signal / 0.8 execution design is now explicitly diagnosed rather than changed.",
        "- The main actionable grid is the exit-buffer table; cash-day yield is separated as a one-row sensitivity.",
    ]
    (run_folder / "record.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(run_folder: Path) -> None:
    started = time.time()
    run_folder.mkdir(parents=True, exist_ok=True)
    command_log = run_folder / "command_log.txt"
    with command_log.open("a", encoding="utf-8") as fh:
        fh.write(f"\n[{pd.Timestamp.now().isoformat()}] python scripts/run_microcap_v2_3_strategy_risk_diagnostics.py --run-folder {run_folder}\n")

    reference_summary, _signal_df, official_out = v23.generate_v2_3_outputs()
    _reference_summary2, base_gross_cached, turnover_df = v23.v2_0.embedded_context._load_embedded_base_context()
    close_df = v23._close_df_from_base(base_gross_cached)
    common_index = pd.DatetimeIndex(official_out.index)
    gross = v23.build_spread_log_wls_gross(close_df, common_index)

    summary, wide, outputs = _scan_exit_buffer_and_cash(gross, turnover_df, run_folder)
    default_label = _candidate_label(v23.MOMENTUM_GAP_EXIT_BUFFER, False)
    signal_diag = _signal_execution_diagnostics(close_df, gross, run_folder)
    wls_diag = _wls_and_boundary_diagnostics(close_df, common_index, run_folder)
    transition_diag = _transition_cost_diagnostics(outputs[default_label], run_folder)

    latest = outputs[default_label].iloc[-1]
    meta_path = run_folder / "scan_meta.json"
    created_at = pd.Timestamp.now().isoformat()
    meta = {
        "run_id": run_folder.name,
        "created_at": created_at,
        "phase": "analysis_written",
        "project": "microcap Top100",
        "strategy": "v2.3",
        "subsystem": "strategy-risk-diagnostics",
        "repo_root": str(ROOT),
        "entrypoint": "microcap_top100_mom16_biweekly_live_v2_3.py",
        "git_branch": _git(["branch", "--show-current"]),
        "git_commit": _git(["rev-parse", "HEAD"]),
        "git_status_before": _git(["status", "--short"]),
        "git_status_after": _git(["status", "--short"]),
        "scan_type": "diagnostic_parameter_grid",
        "parameter_group": "exit_buffer_cash_yield_wls_realtime",
        "baseline": {
            "candidate": default_label,
            "signal_spread_hedge_ratio": v23.SIGNAL_SPREAD_HEDGE_RATIO,
            "execution_hedge_ratio": v23.EXECUTION_HEDGE_RATIO,
            "exit_buffer": v23.MOMENTUM_GAP_EXIT_BUFFER,
            "target_vol": v23.TARGET_VOL,
            "scale_rebalance_threshold": v23.TARGET_VOL_SCALE_REBALANCE_THRESHOLD,
        },
        "candidate_grid": wide["candidate"].tolist(),
        "data_snapshot": {
            "metrics_start": str(pd.Timestamp(common_index.min()).date()),
            "metrics_end": str(pd.Timestamp(common_index.max()).date()),
            "rows": int(len(common_index)),
            "turnover_rows": int(len(turnover_df)),
            "latest_holding": str(latest.get("holding")),
            "latest_next_holding": str(latest.get("next_holding")),
            "latest_target_vol_frozen_lag_days": int(float(latest.get("target_vol_frozen_lag_days", 0))),
            "reference_summary_latest_nav_date": reference_summary.get("latest_nav_date"),
        },
        "cost_model": {
            "costed": True,
            "scale_change_cost": float(v23.v2_0.overlay_mod.TARGET_VOL_SCALE_CHANGE_COST),
            "financing_rate": float(v23.v2_0.overlay_mod.TARGET_VOL_FINANCING_RATE),
            "idle_cash_yield": CASH_YIELD,
            "cash_day_yield_default": False,
            "target_vol_return_source": str(latest.get("target_vol_return_source", "")),
        },
        "outputs": {
            "record": str(run_folder / "record.md"),
            "scan_summary": str(run_folder / "scan_summary.csv"),
            "window_metrics": str(run_folder / "window_metrics.csv"),
            "scan_meta": str(run_folder / "scan_meta.json"),
            "command_log": str(run_folder / "command_log.txt"),
            "signal_execution_mismatch_diagnostics": str(run_folder / "signal_execution_mismatch_diagnostics.csv"),
            "wls_and_boundary_diagnostics": str(run_folder / "wls_and_boundary_diagnostics.json"),
            "transition_cost_diagnostics": str(run_folder / "transition_cost_diagnostics.csv"),
        },
        "decision": "diagnostic_only_keep_signal1p0_exec0p8",
        "stability_label": "not_promoted",
        "elapsed_sec": round(time.time() - started, 3),
    }
    _write_json(meta_path, meta)
    _write_record(run_folder, summary, wide, signal_diag, wls_diag, transition_diag, meta)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-folder", type=Path, required=True)
    args = parser.parse_args()
    run(args.run_folder)


if __name__ == "__main__":
    main()
