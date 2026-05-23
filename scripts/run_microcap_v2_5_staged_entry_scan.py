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

import microcap_top100_mom16_biweekly_live_v2_5 as v25  # noqa: E402


RUN_FOLDER = ROOT / "quant_param_scan_runs" / "20260523_microcap_top100_v2_5_staged_entry_half_then_down_close"
TRADING_DAYS = int(v25.TRADING_DAYS)
ENTRY_COST = float(v25.v2_0.base_mod.freq_mod.cost_mod.ENTRY_COST)
SCALE_CHANGE_COST = float(v25.TARGET_VOL_SCALE_CHANGE_COST)
FINANCING_RATE = float(v25.TARGET_VOL_FINANCING_RATE)
IDLE_CASH_YIELD = float(v25.IDLE_CASH_YIELD)
WINDOW_YEARS = (10, 5, 3, 1)


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, Path):
        return str(value)
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
    except Exception as exc:  # noqa: BLE001
        return f"git_error:{exc}"


def _load_v2_5_shadow() -> tuple[dict[str, Any], pd.DataFrame]:
    if v25.SUMMARY_JSON.exists() and v25.COSTED_NAV_CSV.exists():
        try:
            summary = json.loads(v25.SUMMARY_JSON.read_text(encoding="utf-8"))
            if v25.summary_matches_current_v2_5_base(summary):
                shadow = pd.read_csv(v25.COSTED_NAV_CSV, parse_dates=["date"]).sort_values("date").set_index("date")
                return summary, shadow
        except Exception:
            pass
    summary, _signal_df, shadow = v25.generate_v2_5_outputs()
    return summary, shadow


def apply_staged_entry_overlay(
    shadow: pd.DataFrame,
    *,
    trigger_scope: str,
    initial_fraction: float = 0.5,
    fill_on_down_close: bool = True,
    entry_cost: float = ENTRY_COST,
    scale_change_cost: float = SCALE_CHANGE_COST,
    financing_rate: float = FINANCING_RATE,
    idle_cash_yield_annual: float = IDLE_CASH_YIELD,
) -> pd.DataFrame:
    """Apply a close-confirmed staged-entry execution overlay to a v2.5 daily stream."""
    if trigger_scope not in {"none", "cash_only", "any_scaleup"}:
        raise ValueError("trigger_scope must be one of: none, cash_only, any_scaleup")
    if not (0.0 < float(initial_fraction) <= 1.0):
        raise ValueError("initial_fraction must be in (0, 1]")

    out = shadow.copy().sort_index()
    holding = out["holding"].fillna("cash").astype(str)
    next_holding = out["next_holding"].fillna(holding).astype(str)
    base_pre_cost_return = pd.to_numeric(out["base_pre_cost_return"], errors="coerce").fillna(0.0)
    base_trade_cost = pd.to_numeric(out.get("base_trade_cost", out.get("total_cost", 0.0)), errors="coerce").fillna(0.0)
    base_current_scale = pd.to_numeric(out["current_execution_scale"], errors="coerce").fillna(0.0).clip(lower=0.0)
    base_next_scale = pd.to_numeric(out["next_session_actionable_scale"], errors="coerce").fillna(base_current_scale).clip(lower=0.0)
    close = pd.to_numeric(out["microcap_close"], errors="coerce")

    actual_current_values: list[float] = []
    actual_next_values: list[float] = []
    return_values: list[float] = []
    base_trade_cost_values: list[float] = []
    scale_change_cost_values: list[float] = []
    staged_entry_cost_values: list[float] = []
    financing_cost_values: list[float] = []
    idle_cash_yield_values: list[float] = []
    open_flags: list[bool] = []
    fill_flags: list[bool] = []
    pending_flags: list[bool] = []
    pending_target_values: list[float] = []

    prev_actual_next = 0.0
    prev_next_active = False
    prev_actual_current = 0.0
    prev_current_active = False
    pending_full_target: float | None = None
    eps = 1e-12

    for pos, dt in enumerate(out.index):
        current_active = bool(holding.loc[dt] != "cash")
        next_active = bool(next_holding.loc[dt] != "cash")
        base_current = float(base_current_scale.loc[dt]) if current_active else 0.0
        target_next = float(base_next_scale.loc[dt]) if next_active else 0.0
        if current_active:
            actual_current = float(prev_actual_next) if prev_next_active else base_current
        else:
            actual_current = 0.0
            if not next_active:
                pending_full_target = None

        current_close = close.loc[dt]
        previous_close = close.iloc[pos - 1] if pos > 0 else np.nan
        is_down_close = bool(pd.notna(current_close) and pd.notna(previous_close) and float(current_close) < float(previous_close))

        open_trigger = False
        fill_trigger = False
        if trigger_scope == "none" or not next_active or target_next <= eps:
            actual_next = target_next
            if not next_active:
                pending_full_target = None
        elif pending_full_target is not None:
            pending_full_target = max(float(pending_full_target), target_next)
            if fill_on_down_close and is_down_close:
                actual_next = pending_full_target
                pending_full_target = None
                fill_trigger = True
            else:
                actual_next = min(actual_current, pending_full_target)
        else:
            if trigger_scope == "cash_only":
                should_stage = (not current_active) and next_active and target_next > eps
            else:
                should_stage = next_active and target_next > actual_current + eps
            if should_stage:
                increment = target_next - actual_current
                actual_next = actual_current + float(initial_fraction) * increment
                pending_full_target = target_next if actual_next < target_next - eps else None
                open_trigger = True
            else:
                actual_next = target_next

        if pending_full_target is not None and actual_next >= pending_full_target - eps:
            pending_full_target = None

        base_cost_scale = actual_current if current_active else actual_next if next_active else 0.0
        base_cost_scaled = float(base_trade_cost.loc[dt]) * max(base_cost_scale, 0.0)
        same_holding_active = current_active and prev_current_active
        target_vol_scale_cost = abs(actual_current - prev_actual_current) * float(scale_change_cost) if same_holding_active else 0.0
        staged_entry_cost = 0.0
        if current_active and next_active and (open_trigger or fill_trigger):
            staged_entry_cost = max(actual_next - actual_current, 0.0) * float(entry_cost)
        financing_cost = max(actual_current - 1.0, 0.0) * float(financing_rate) / TRADING_DAYS
        idle_cash_yield = (
            max(1.0 - actual_current, 0.0) * float(idle_cash_yield_annual) / TRADING_DAYS if current_active else 0.0
        )
        gross_part = float(base_pre_cost_return.loc[dt]) * actual_current + idle_cash_yield
        ret = (
            (1.0 + gross_part)
            * (1.0 - min(max(base_cost_scaled, 0.0), 0.99))
            * (1.0 - min(max(target_vol_scale_cost, 0.0), 0.99))
            * (1.0 - min(max(staged_entry_cost, 0.0), 0.99))
            * (1.0 - min(max(financing_cost, 0.0), 0.99))
            - 1.0
        )

        actual_current_values.append(float(actual_current))
        actual_next_values.append(float(actual_next))
        return_values.append(float(ret))
        base_trade_cost_values.append(float(base_cost_scaled))
        scale_change_cost_values.append(float(target_vol_scale_cost))
        staged_entry_cost_values.append(float(staged_entry_cost))
        financing_cost_values.append(float(financing_cost))
        idle_cash_yield_values.append(float(idle_cash_yield))
        open_flags.append(bool(open_trigger))
        fill_flags.append(bool(fill_trigger))
        pending_flags.append(bool(pending_full_target is not None))
        pending_target_values.append(float(pending_full_target) if pending_full_target is not None else np.nan)

        prev_actual_next = float(actual_next)
        prev_next_active = bool(next_active)
        prev_actual_current = float(actual_current)
        prev_current_active = bool(current_active)

    ret = pd.Series(return_values, index=out.index, dtype=float)
    out["actual_execution_scale"] = pd.Series(actual_current_values, index=out.index, dtype=float)
    out["actual_next_session_scale"] = pd.Series(actual_next_values, index=out.index, dtype=float)
    out["staged_entry_open_triggered"] = pd.Series(open_flags, index=out.index, dtype=bool)
    out["staged_entry_fill_triggered"] = pd.Series(fill_flags, index=out.index, dtype=bool)
    out["staged_entry_pending"] = pd.Series(pending_flags, index=out.index, dtype=bool)
    out["staged_entry_pending_target_scale"] = pd.Series(pending_target_values, index=out.index, dtype=float)
    out["base_trade_cost_scaled_actual"] = pd.Series(base_trade_cost_values, index=out.index, dtype=float)
    out["scale_change_cost_actual"] = pd.Series(scale_change_cost_values, index=out.index, dtype=float)
    out["staged_entry_trade_cost"] = pd.Series(staged_entry_cost_values, index=out.index, dtype=float)
    out["financing_cost_actual"] = pd.Series(financing_cost_values, index=out.index, dtype=float)
    out["idle_cash_yield_actual"] = pd.Series(idle_cash_yield_values, index=out.index, dtype=float)
    out["trigger_scope"] = trigger_scope
    out["staged_entry_initial_fraction"] = float(initial_fraction)
    out["staged_entry_fill_rule"] = "next_down_close" if fill_on_down_close else "disabled"
    out["return_net"] = ret
    out["return"] = ret
    out["nav_net"] = (1.0 + ret.fillna(0.0)).cumprod()
    out["nav"] = out["nav_net"]
    return out


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
    dd = nav.div(nav.cummax()).sub(1.0)
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
    for years in WINDOW_YEARS:
        windows[f"last_{years}y"] = (max(first, end - pd.DateOffset(years=years)), end)
    return windows


def _transition_counts(out: pd.DataFrame) -> dict[str, int]:
    holding = out["holding"].astype(str)
    prev = holding.shift(1).fillna("cash")
    return {
        "holding_days": int(holding.ne("cash").sum()),
        "cash_days": int(holding.eq("cash").sum()),
        "entry_days": int((holding.ne("cash") & prev.eq("cash")).sum()),
        "exit_days": int((holding.eq("cash") & prev.ne("cash")).sum()),
    }


def _candidate_label(scope: str) -> str:
    return {
        "none": "v2_5_baseline",
        "cash_only": "staged_cash_entry_only",
        "any_scaleup": "staged_any_scaleup",
    }[scope]


def _scan(run_folder: Path, initial_fraction: float) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    reference_summary, shadow = _load_v2_5_shadow()
    shadow = shadow.copy().sort_index()
    candidates = ("none", "cash_only", "any_scaleup")
    summary_rows: list[dict[str, Any]] = []
    wide_rows: list[dict[str, Any]] = []
    verification_rows: list[dict[str, Any]] = []

    for scope in candidates:
        label = _candidate_label(scope)
        out = apply_staged_entry_overlay(shadow, trigger_scope=scope, initial_fraction=initial_fraction)
        out["candidate"] = label
        out.rename_axis("date").reset_index().to_csv(run_folder / f"daily_{label}.csv", index=False, encoding="utf-8")
        counts = _transition_counts(out)
        actual = pd.to_numeric(out["actual_execution_scale"], errors="coerce").fillna(0.0)
        base = pd.to_numeric(out["current_execution_scale"], errors="coerce").fillna(0.0)
        verification_rows.append(
            {
                "candidate": label,
                "rows_match": bool(len(out) == len(shadow)),
                "finite_return_net": bool(np.isfinite(pd.to_numeric(out["return_net"], errors="coerce").fillna(0.0)).all()),
                "baseline_return_match": (
                    bool(
                        np.allclose(
                            pd.to_numeric(out["return_net"], errors="coerce").fillna(0.0).to_numpy(),
                            pd.to_numeric(shadow["return_net"], errors="coerce").fillna(0.0).to_numpy(),
                            atol=1e-12,
                            rtol=0.0,
                        )
                    )
                    if scope == "none"
                    else np.nan
                ),
                "same_as_baseline": bool(
                    np.allclose(
                        pd.to_numeric(out["return_net"], errors="coerce").fillna(0.0).to_numpy(),
                        pd.to_numeric(shadow["return_net"], errors="coerce").fillna(0.0).to_numpy(),
                        atol=1e-12,
                        rtol=0.0,
                    )
                ),
                "open_trigger_count": int(out["staged_entry_open_triggered"].sum()),
                "fill_trigger_count": int(out["staged_entry_fill_triggered"].sum()),
                "pending_days": int(out["staged_entry_pending"].sum()),
                "reduced_exposure_days": int((actual < base - 1e-12).sum()),
                "final_nav": float(out["nav_net"].iloc[-1]),
            }
        )
        wide: dict[str, Any] = {
            "candidate": label,
            "trigger_scope": scope,
            "initial_fraction": float(initial_fraction),
            "target_vol": v25.TARGET_VOL,
            "max_leverage": v25.TARGET_VOL_MAX_LEVERAGE,
            "scale_rebalance_threshold": v25.TARGET_VOL_SCALE_REBALANCE_THRESHOLD,
            "holding_days_full": counts["holding_days"],
            "cash_days_full": counts["cash_days"],
            "entry_days_full": counts["entry_days"],
            "exit_days_full": counts["exit_days"],
            "open_trigger_count_full": int(out["staged_entry_open_triggered"].sum()),
            "fill_trigger_count_full": int(out["staged_entry_fill_triggered"].sum()),
            "pending_days_full": int(out["staged_entry_pending"].sum()),
            "reduced_exposure_days_full": int((actual < base - 1e-12).sum()),
            "staged_entry_trade_cost_sum_full": float(out["staged_entry_trade_cost"].sum()),
        }
        for segment, (start, end) in _window_slices(pd.DatetimeIndex(out.index)).items():
            part = out.loc[(out.index >= start) & (out.index <= end)]
            part_actual = pd.to_numeric(part["actual_execution_scale"], errors="coerce").fillna(0.0)
            part_base = pd.to_numeric(part["current_execution_scale"], errors="coerce").fillna(0.0)
            m = _metrics(part["return_net"])
            part_counts = _transition_counts(part)
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
                "holding_days": part_counts["holding_days"],
                "cash_days": part_counts["cash_days"],
                "holding_day_ratio": part_counts["holding_days"] / len(part) if len(part) else np.nan,
                "avg_execution_scale": float(part_actual.mean()) if len(part) else np.nan,
                "max_execution_scale": float(part_actual.max()) if len(part) else np.nan,
                "open_trigger_count": int(part["staged_entry_open_triggered"].sum()),
                "fill_trigger_count": int(part["staged_entry_fill_triggered"].sum()),
                "pending_days": int(part["staged_entry_pending"].sum()),
                "reduced_exposure_days": int((part_actual < part_base - 1e-12).sum()),
                "staged_entry_trade_cost_sum": float(part["staged_entry_trade_cost"].sum()),
            }
            summary_rows.append(row)
            for metric in (
                "ann_return",
                "ann_vol",
                "sharpe_repo",
                "max_dd",
                "final_nav",
                "holding_day_ratio",
                "avg_execution_scale",
                "max_execution_scale",
                "open_trigger_count",
                "fill_trigger_count",
                "pending_days",
                "reduced_exposure_days",
                "staged_entry_trade_cost_sum",
            ):
                wide[f"{metric}_{segment}"] = row[metric]
        wide["decision_hint"] = "baseline" if scope == "none" else "compare_only"
        wide_rows.append(wide)

    summary = pd.DataFrame(summary_rows)
    wide = pd.DataFrame(wide_rows)
    verification = pd.DataFrame(verification_rows)
    summary.to_csv(run_folder / "scan_summary.csv", index=False, encoding="utf-8")
    wide.to_csv(run_folder / "window_metrics.csv", index=False, encoding="utf-8")
    verification.to_csv(run_folder / "staged_entry_sanity_checks.csv", index=False, encoding="utf-8")
    return summary, wide, {
        "reference_summary": reference_summary,
        "metrics_start": str(pd.Timestamp(shadow.index.min()).date()),
        "metrics_end": str(pd.Timestamp(shadow.index.max()).date()),
        "rows": int(len(shadow)),
        "candidate_count": int(len(wide)),
        "all_rows_match": bool(verification["rows_match"].all()),
        "all_finite_return_net": bool(verification["finite_return_net"].all()),
        "baseline_return_match": bool(
            verification.loc[verification["candidate"].eq("v2_5_baseline"), "baseline_return_match"].iloc[0]
        ),
    }


def _write_record(run_folder: Path, wide: pd.DataFrame, context: dict[str, Any], meta: dict[str, Any]) -> None:
    cols = [
        "candidate",
        "ann_return_full",
        "max_dd_full",
        "ann_return_last_10y",
        "max_dd_last_10y",
        "ann_return_last_5y",
        "max_dd_last_5y",
        "ann_return_last_3y",
        "max_dd_last_3y",
        "ann_return_last_1y",
        "max_dd_last_1y",
        "open_trigger_count_full",
        "fill_trigger_count_full",
        "reduced_exposure_days_full",
        "staged_entry_trade_cost_sum_full",
    ]
    view = wide[cols]
    lines = [
        "# Quant Parameter Scan Record",
        "",
        "## Run Metadata",
        "",
        f"- Run id: `{run_folder.name}`",
        f"- Created at: {meta['created_at']}",
        "- Project: microcap Top100",
        "- Strategy or version: v2.5",
        "- Sleeve or subsystem: staged entry execution overlay",
        "- Parameter group: `half_entry_then_down_close_fill`",
        "- Scan type: staged_entry_scope_compare",
        f"- Repo or workspace path: `{ROOT}`",
        "- Target entrypoint: `scripts/run_microcap_v2_5_staged_entry_scan.py`",
        f"- Git branch: `{meta['git_branch']}`",
        f"- Git commit: `{meta['git_commit']}`",
        "- Working tree status before: see `scan_meta.json`.",
        "",
        "## Research Question",
        "",
        "- Baseline: formal v2.5 costed daily stream.",
        "- Candidate 1: when v2.5 moves from cash to active, open half the target exposure first, then fill the other half after the next down close.",
        "- Candidate 2: apply the same half-then-down-close rule to any positive scale-up, including target-vol scale increases while already active.",
        "- Source-change rule: `research_only_no_source_change`.",
        "- Required windows: full, last_10y, last_5y, last_3y, last_1y.",
        "",
        "## Implementation Anchor",
        "",
        "- Shadow path: official v2.5 costed NAV generated by `microcap_top100_mom16_biweekly_live_v2_5.py`.",
        "- The signal remains close-confirmed: a down close on T fills the remaining half for T+1, so T itself still earns the staged exposure.",
        "- `cash_only` stages only cash-to-active entries.",
        "- `any_scaleup` stages cash-to-active entries and later positive target-scale increases.",
        "",
        "## Data Snapshot",
        "",
        f"- Metrics start: {context['metrics_start']}",
        f"- Metrics end: {context['metrics_end']}",
        f"- Rows: {context['rows']}",
        f"- Reference v2.5 latest NAV date: {context['reference_summary'].get('latest_nav_date')}",
        "- Trading calendar: strategy local trading-date index; annualization uses 244 trading days.",
        "",
        "## Cost and Execution Assumptions",
        "",
        "- Retained: v2.5 embedded base trading cost, target-vol scale-change cost, financing cost, and idle-cash yield mechanics.",
        f"- Added: staged fill/add trades charge one-side entry cost `{ENTRY_COST:.4f}` on incremental exposure.",
        "- The test is costed and close-to-close; no hedge leg, stop-loss, drawdown stop, or overheat layer is introduced.",
        "",
        "## Commands",
        "",
        "```powershell",
        f"python scripts/run_microcap_v2_5_staged_entry_scan.py --run-folder {run_folder}",
        f"python C:\\Users\\Administrator.DESKTOP-95I7VVU\\.codex\\skills\\quant-param-scan\\scripts\\finalize_quant_param_scan_run.py {run_folder} --decision research_only_do_not_promote_staged_entry --stability-label first_pass_scope_compare",
        f"python C:\\Users\\Administrator.DESKTOP-95I7VVU\\.codex\\skills\\quant-param-scan\\scripts\\check_quant_param_scan_artifacts.py --phase complete --strict {run_folder}",
        "```",
        "",
        "## Output Files",
        "",
        "- `scan_summary.csv`: long-form metrics by candidate and window.",
        "- `window_metrics.csv`: wide candidate comparison table.",
        "- `staged_entry_sanity_checks.csv`: row, finite-return, baseline-parity, trigger, and final NAV checks.",
        "",
        "## Results",
        "",
        view.to_markdown(index=False, floatfmt=".6f"),
        "",
        "## Stability Classification",
        "",
        "- Label: first_pass_scope_compare.",
        f"- Sanity: all candidates row-match baseline: {context['all_rows_match']}; finite return_net: {context['all_finite_return_net']}; baseline parity: {context['baseline_return_match']}.",
        "",
        "## Decision",
        "",
        "- Decision: research_only_do_not_promote_staged_entry.",
        "- Recommended next action: only revisit if the priority is explicitly lowering entry-day exposure risk rather than improving return/drawdown.",
        "",
    ]
    (run_folder / "record.md").write_text("\n".join(lines), encoding="utf-8")


def run(run_folder: Path, initial_fraction: float = 0.5) -> None:
    started = time.time()
    run_folder.mkdir(parents=True, exist_ok=True)
    command_log = run_folder / "command_log.txt"
    with command_log.open("a", encoding="utf-8") as fh:
        fh.write(
            f"\n[{pd.Timestamp.now().isoformat()}] "
            f"python scripts/run_microcap_v2_5_staged_entry_scan.py --run-folder {run_folder}\n"
        )
    _summary, wide, context = _scan(run_folder, initial_fraction=initial_fraction)
    meta = {
        "run_id": run_folder.name,
        "created_at": pd.Timestamp.now().isoformat(),
        "phase": "analysis_written",
        "project": "microcap Top100",
        "strategy": "v2.5",
        "subsystem": "staged entry execution overlay",
        "repo_root": str(ROOT),
        "entrypoint": "scripts/run_microcap_v2_5_staged_entry_scan.py",
        "git_branch": _git(["branch", "--show-current"]),
        "git_commit": _git(["rev-parse", "HEAD"]),
        "git_status_before": _git(["status", "--short"]),
        "git_status_after": _git(["status", "--short"]),
        "scan_type": "staged_entry_scope_compare",
        "parameter_group": "half_entry_then_down_close_fill",
        "baseline": {
            "candidate": "v2_5_baseline",
            "strategy_version": "v2.5",
            "entry_threshold": v25.ENTRY_THRESHOLD,
            "exit_threshold": v25.EXIT_THRESHOLD,
            "target_vol": v25.TARGET_VOL,
            "max_leverage": v25.TARGET_VOL_MAX_LEVERAGE,
            "scale_rebalance_threshold": v25.TARGET_VOL_SCALE_REBALANCE_THRESHOLD,
        },
        "candidate_grid": wide["candidate"].tolist(),
        "data_snapshot": {
            "metrics_start": context["metrics_start"],
            "metrics_end": context["metrics_end"],
            "rows": context["rows"],
            "reference_summary_latest_nav_date": context["reference_summary"].get("latest_nav_date"),
        },
        "cost_model": {
            "base": "formal_v2_5_costed",
            "staged_entry_incremental_trade_cost": ENTRY_COST,
            "target_vol_scale_change_cost": SCALE_CHANGE_COST,
            "financing_rate": FINANCING_RATE,
            "idle_cash_yield": IDLE_CASH_YIELD,
            "execution_timing": "close_confirmed_t_signal_next_session_execution",
        },
        "verification": {
            "all_rows_match": context["all_rows_match"],
            "all_finite_return_net": context["all_finite_return_net"],
            "baseline_return_match": context["baseline_return_match"],
        },
        "outputs": {
            "record": str(run_folder / "record.md"),
            "scan_summary": str(run_folder / "scan_summary.csv"),
            "window_metrics": str(run_folder / "window_metrics.csv"),
            "scan_meta": str(run_folder / "scan_meta.json"),
            "command_log": str(command_log),
            "staged_entry_sanity_checks": str(run_folder / "staged_entry_sanity_checks.csv"),
        },
        "decision": "research_only_do_not_promote_staged_entry",
        "stability_label": "first_pass_scope_compare",
        "elapsed_sec": round(time.time() - started, 3),
    }
    _write_json(run_folder / "scan_meta.json", meta)
    _write_record(run_folder, wide, context, meta)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-folder", type=Path, default=RUN_FOLDER)
    parser.add_argument("--initial-fraction", type=float, default=0.5)
    args = parser.parse_args()
    run(args.run_folder, initial_fraction=args.initial_fraction)


if __name__ == "__main__":
    main()
