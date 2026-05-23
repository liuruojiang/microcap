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
SCRIPTS = ROOT / "scripts"
for path in (ROOT, SCRIPTS):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

import microcap_top100_mom16_biweekly_live_v2_3 as v23  # noqa: E402
import run_microcap_v2_3_microcap_only_momentum_decay_scan as common_scan  # noqa: E402
import run_microcap_v2_3_microcap_only_target_vol_scan as tv_base  # noqa: E402


RUN_FOLDER = ROOT / "quant_param_scan_runs" / "20260523_microcap_top100_v2_3_derived_microcap_only_overheat_momentum_overheat_exit_reentry"
TRADING_DAYS = int(v23.TRADING_DAYS)
TARGET_VOL = 0.30
MAX_LEVERAGE = 1.3
SCALE_THRESHOLD = 0.30
ONE_SIDE_TRADE_COST = float(v23.v2_0.base_mod.freq_mod.cost_mod.ENTRY_COST)
OVERHEAT_THRESHOLDS = (1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 5.0)
COOLING_THRESHOLDS = (0.4, 0.6, 0.8, 1.0, 1.2, 1.5, 2.0, 2.5, 3.0)


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


def _label(overheat_threshold: float | None, cooling_threshold: float | None) -> str:
    if overheat_threshold is None:
        return "no_overheat"
    return f"hot{int(round(overheat_threshold * 100)):03d}_cool{int(round(float(cooling_threshold) * 100)):03d}"


def _set_tv_globals() -> None:
    tv_base.MAX_LEVERAGE = MAX_LEVERAGE
    tv_base.SCALE_REBALANCE_THRESHOLD = SCALE_THRESHOLD


def _build_shadow_path() -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    _set_tv_globals()
    return common_scan._build_shadow_path()


def _apply_overheat_overlay(
    shadow: pd.DataFrame,
    overheat_threshold: float | None,
    cooling_threshold: float | None,
) -> pd.DataFrame:
    out = shadow.copy().sort_index()
    score = pd.to_numeric(out["annualized_log_wls_score"], errors="coerce")
    shadow_scale = pd.to_numeric(out["current_execution_scale"], errors="coerce").fillna(0.0)
    shadow_ret = pd.to_numeric(out["return_net"], errors="coerce").fillna(0.0)
    base_next_active = out["next_holding"].astype(str).ne("cash")

    if overheat_threshold is None:
        out["overheat_threshold"] = np.nan
        out["cooling_threshold"] = np.nan
        out["overheat_risk_off"] = False
        out["overheat_exit_triggered"] = False
        out["overheat_reentry_triggered"] = False
        out["actual_execution_scale"] = shadow_scale
        out["overlay_trade_cost"] = 0.0
        return out

    hot_thr = float(overheat_threshold)
    cool_thr = float(cooling_threshold)
    if hot_thr <= tv_base.ENTRY_THRESHOLD:
        raise ValueError("overheat_threshold must be above entry threshold")
    if cool_thr < tv_base.EXIT_THRESHOLD or cool_thr >= hot_thr:
        raise ValueError("cooling_threshold must be in [base exit threshold, overheat_threshold)")

    risk_off = False
    prev_actual_scale = 0.0
    prev_risk_off = False
    actual_returns: list[float] = []
    actual_scales: list[float] = []
    overlay_costs: list[float] = []
    exit_flags: list[bool] = []
    reentry_flags: list[bool] = []
    risk_off_flags: list[bool] = []

    for dt in out.index:
        current_risk_off = risk_off
        target_scale = 0.0 if current_risk_off else float(shadow_scale.loc[dt])
        overlay_turnover = abs(target_scale - prev_actual_scale) if current_risk_off != prev_risk_off else 0.0
        overlay_cost = overlay_turnover * ONE_SIDE_TRADE_COST
        day_ret = 0.0 if current_risk_off else float(shadow_ret.loc[dt])
        actual_ret = (1.0 + day_ret) * (1.0 - min(max(overlay_cost, 0.0), 0.99)) - 1.0

        current_score = score.loc[dt]
        next_active = bool(base_next_active.loc[dt])
        exit_trigger = False
        reentry_trigger = False
        if not next_active or pd.isna(current_score):
            risk_off = False
        elif risk_off:
            if float(current_score) <= cool_thr:
                risk_off = False
                reentry_trigger = True
        elif float(current_score) >= hot_thr:
            risk_off = True
            exit_trigger = True

        actual_returns.append(actual_ret)
        actual_scales.append(target_scale)
        overlay_costs.append(overlay_cost)
        exit_flags.append(exit_trigger)
        reentry_flags.append(reentry_trigger)
        risk_off_flags.append(current_risk_off)
        prev_actual_scale = target_scale
        prev_risk_off = current_risk_off

    ret = pd.Series(actual_returns, index=out.index, dtype=float)
    out["overheat_threshold"] = hot_thr
    out["cooling_threshold"] = cool_thr
    out["overheat_risk_off"] = pd.Series(risk_off_flags, index=out.index, dtype=bool)
    out["overheat_exit_triggered"] = pd.Series(exit_flags, index=out.index, dtype=bool)
    out["overheat_reentry_triggered"] = pd.Series(reentry_flags, index=out.index, dtype=bool)
    out["actual_execution_scale"] = pd.Series(actual_scales, index=out.index, dtype=float)
    out["overlay_trade_cost"] = pd.Series(overlay_costs, index=out.index, dtype=float)
    out["return_net"] = ret
    out["nav_net"] = (1.0 + ret.fillna(0.0)).cumprod()
    out["return"] = out["return_net"]
    out["nav"] = out["nav_net"]
    return out


def _candidate_grid() -> list[tuple[float | None, float | None]]:
    pairs: list[tuple[float | None, float | None]] = [(None, None)]
    for hot_thr in OVERHEAT_THRESHOLDS:
        for cool_thr in COOLING_THRESHOLDS:
            if tv_base.EXIT_THRESHOLD <= cool_thr < hot_thr:
                pairs.append((float(hot_thr), float(cool_thr)))
    return pairs


def _scan(run_folder: Path) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    reference_summary, base, shadow = _build_shadow_path()
    summary_rows: list[dict[str, Any]] = []
    wide_rows: list[dict[str, Any]] = []
    verification_rows: list[dict[str, Any]] = []

    for hot_thr, cool_thr in _candidate_grid():
        label = _label(hot_thr, cool_thr)
        out = _apply_overheat_overlay(shadow, hot_thr, cool_thr)
        out.rename_axis("date").reset_index().to_csv(run_folder / f"daily_{label}.csv", index=False, encoding="utf-8")
        counts = common_scan._transition_counts(out)
        verification_rows.append(
            {
                "candidate": label,
                "rows_match": bool(len(out) == len(shadow)),
                "finite_return_net": bool(np.isfinite(pd.to_numeric(out["return_net"], errors="coerce").fillna(0.0)).all()),
                "exit_count": int(out["overheat_exit_triggered"].fillna(False).sum()),
                "reentry_count": int(out["overheat_reentry_triggered"].fillna(False).sum()),
                "risk_off_days": int(out["overheat_risk_off"].fillna(False).sum()),
                "final_nav": float(out["nav_net"].iloc[-1]),
            }
        )
        wide: dict[str, Any] = {
            "candidate": label,
            "overheat_threshold": np.nan if hot_thr is None else float(hot_thr),
            "cooling_threshold": np.nan if cool_thr is None else float(cool_thr),
            "target_vol": TARGET_VOL,
            "max_leverage": MAX_LEVERAGE,
            "scale_rebalance_threshold": SCALE_THRESHOLD,
            "holding_days_full": counts["holding_days"],
            "cash_days_full": counts["cash_days"],
            "entry_days_full": counts["entry_days"],
            "exit_days_full": counts["exit_days"],
            "overheat_exit_count_full": int(out["overheat_exit_triggered"].fillna(False).sum()),
            "overheat_reentry_count_full": int(out["overheat_reentry_triggered"].fillna(False).sum()),
            "overheat_risk_off_days_full": int(out["overheat_risk_off"].fillna(False).sum()),
            "overlay_trade_cost_sum_full": float(pd.to_numeric(out["overlay_trade_cost"], errors="coerce").fillna(0.0).sum()),
        }
        for segment, (start, end) in common_scan._window_slices(pd.DatetimeIndex(out.index)).items():
            part = out.loc[(out.index >= start) & (out.index <= end)]
            m = common_scan._metrics(part["return_net"])
            part_counts = common_scan._transition_counts(part)
            actual_scale = pd.to_numeric(part.get("actual_execution_scale", part.get("current_execution_scale", 0.0)), errors="coerce")
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
                "avg_execution_scale": float(actual_scale.mean()) if len(part) else np.nan,
                "max_execution_scale": float(actual_scale.max()) if len(part) else np.nan,
                "overheat_exit_count": int(part["overheat_exit_triggered"].fillna(False).sum()),
                "overheat_reentry_count": int(part["overheat_reentry_triggered"].fillna(False).sum()),
                "overheat_risk_off_days": int(part["overheat_risk_off"].fillna(False).sum()),
                "overlay_trade_cost_sum": float(pd.to_numeric(part["overlay_trade_cost"], errors="coerce").fillna(0.0).sum()),
                "overheat_threshold": np.nan if hot_thr is None else float(hot_thr),
                "cooling_threshold": np.nan if cool_thr is None else float(cool_thr),
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
                "overheat_exit_count",
                "overheat_reentry_count",
                "overheat_risk_off_days",
                "overlay_trade_cost_sum",
            ):
                wide[f"{metric}_{segment}"] = row[metric]
        wide["decision_score"] = np.nan
        wide["decision_hint"] = "compare_only"
        wide["stability_label"] = "candidate"
        wide_rows.append(wide)

    summary = pd.DataFrame(summary_rows)
    wide = pd.DataFrame(wide_rows)
    wide["decision_score"] = wide.apply(common_scan._decision_score, axis=1)
    best_label = str(wide.sort_values("decision_score", ascending=False).iloc[0]["candidate"])
    wide.loc[wide["candidate"].eq("no_overheat"), "decision_hint"] = "tv30_max1p3_no_overheat"
    wide.loc[wide["candidate"].eq(best_label), "decision_hint"] = "best_10y_5y_3y_balanced_score"
    wide.loc[wide["candidate"].eq(best_label), "stability_label"] = "watchlist_best"

    verification = pd.DataFrame(verification_rows)
    summary.to_csv(run_folder / "scan_summary.csv", index=False, encoding="utf-8")
    wide.to_csv(run_folder / "window_metrics.csv", index=False, encoding="utf-8")
    verification.to_csv(run_folder / "overheat_sanity_checks.csv", index=False, encoding="utf-8")
    return summary, wide, {
        "reference_summary": reference_summary,
        "metrics_start": str(pd.Timestamp(base.index.min()).date()),
        "metrics_end": str(pd.Timestamp(base.index.max()).date()),
        "rows": int(len(base)),
        "baseline_label": "no_overheat",
        "best_label": best_label,
        "candidate_count": int(len(wide)),
        "all_rows_match": bool(verification["rows_match"].all()),
        "all_finite_return_net": bool(verification["finite_return_net"].all()),
    }


def _write_record(run_folder: Path, wide: pd.DataFrame, context: dict[str, Any], meta: dict[str, Any]) -> None:
    ordered = wide.sort_values("decision_score", ascending=False)
    best = ordered.iloc[0]
    baseline = wide.loc[wide["candidate"].eq(context["baseline_label"])].iloc[0]
    top = ordered.head(15)[
        [
            "candidate",
            "decision_score",
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
            "overheat_exit_count_full",
            "overheat_reentry_count_full",
            "overheat_risk_off_days_full",
            "overlay_trade_cost_sum_full",
        ]
    ]
    lines = [
        "# Quant Parameter Scan Record",
        "",
        "## Run Metadata",
        "",
        f"- Run id: `{run_folder.name}`",
        f"- Created at: {meta['created_at']}",
        "- Project: microcap Top100",
        "- Strategy or version: v2.3 derived",
        "- Sleeve or subsystem: microcap-only overheat",
        "- Parameter group: `momentum_overheat_exit_reentry`",
        "- Scan type: momentum_overheat_overlay_grid",
        f"- Repo or workspace path: `{ROOT}`",
        "- Target entrypoint: `scripts/run_microcap_v2_3_microcap_only_overheat_scan.py`",
        f"- Git branch: `{meta['git_branch']}`",
        f"- Git commit: `{meta['git_commit']}`",
        "- Working tree status before: see `scan_meta.json`.",
        "",
        "## Research Question",
        "",
        "- Baseline: microcap-only signal `entry=40%`, `exit=40%`, target-vol `30%`, max leverage `1.3`, scale threshold `0.30`.",
        "- Candidate grid: exit when the annualized log-WLS momentum score is above an overheat threshold; recover when it cools below a lower threshold and the base signal remains active.",
        f"- Overheat thresholds: `{list(OVERHEAT_THRESHOLDS)}`.",
        f"- Cooling thresholds: `{list(COOLING_THRESHOLDS)}` where base exit threshold <= cooling < overheat.",
        "- Source-change rule: `research_only_no_source_change`.",
        "- Required windows: full, last_10y, last_5y, last_3y, last_1y.",
        "",
        "## Implementation Anchor",
        "",
        "- Shadow path: baseline TV30/max1.3 strategy without overheat overlay.",
        "- Exit trigger: after close on T, if score >= overheat_threshold, set overlay risk-off for T+1.",
        "- Recovery trigger: after close on T, if score <= cooling_threshold and the base signal is active, resume following the shadow strategy from T+1.",
        "- Actual path: returns are set to cash while overlay risk-off; overlay exit/reentry charges 30bp one-side microcap turnover only when this overlay changes risk state.",
        "",
        "## Data Snapshot",
        "",
        f"- Metrics start: {context['metrics_start']}",
        f"- Metrics end: {context['metrics_end']}",
        f"- Rows: {context['rows']}",
        f"- Reference v2.3 latest NAV date: {context['reference_summary'].get('latest_nav_date')}",
        "- Trading calendar: strategy local trading-date index; annualization uses 244 trading days.",
        "",
        "## Cost and Execution Assumptions",
        "",
        "- Retained: base TV30/max1.3 costs and financing from the microcap-only target-vol layer.",
        "- Added: overlay microcap one-side trading cost on overheat risk-off/risk-on scale changes.",
        "- No hedge leg, futures drag, or hedge turnover.",
        "",
        "## Commands",
        "",
        "```powershell",
        f"python scripts/run_microcap_v2_3_microcap_only_overheat_scan.py --run-folder {run_folder}",
        f"python C:\\Users\\Administrator.DESKTOP-95I7VVU\\.codex\\skills\\quant-param-scan\\scripts\\finalize_quant_param_scan_run.py {run_folder} --decision research_only_watchlist_overheat --stability-label overheat_first_pass",
        f"python C:\\Users\\Administrator.DESKTOP-95I7VVU\\.codex\\skills\\quant-param-scan\\scripts\\check_quant_param_scan_artifacts.py --phase complete --strict {run_folder}",
        "```",
        "",
        "## Full-Sample Results",
        "",
        f"- Baseline `{context['baseline_label']}`: annual return {baseline['ann_return_full']:.4%}, max drawdown {baseline['max_dd_full']:.4%}, Sharpe {baseline['sharpe_repo_full']:.3f}.",
        f"- Best balanced candidate `{best['candidate']}`: annual return {best['ann_return_full']:.4%}, max drawdown {best['max_dd_full']:.4%}, Sharpe {best['sharpe_repo_full']:.3f}.",
        "",
        "## Top Candidates",
        "",
        top.to_markdown(index=False, floatfmt=".6f"),
        "",
        "## Stability Classification",
        "",
        "- Label: overheat_first_pass.",
        "- Evidence: compare `window_metrics.csv`; this is not a production promotion.",
        f"- Candidate count: {context['candidate_count']}.",
        f"- Sanity: all candidates row-match baseline: {context['all_rows_match']}; finite return_net: {context['all_finite_return_net']}.",
        "",
        "## Decision",
        "",
        "- Decision: research_only_watchlist_overheat.",
        "- Recommended next action: compare overheat candidates against the no-overlay baseline across all required windows.",
    ]
    (run_folder / "record.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(run_folder: Path) -> None:
    started = time.time()
    run_folder.mkdir(parents=True, exist_ok=True)
    command_log = run_folder / "command_log.txt"
    with command_log.open("a", encoding="utf-8") as fh:
        fh.write(f"\n[{pd.Timestamp.now().isoformat()}] python scripts/run_microcap_v2_3_microcap_only_overheat_scan.py --run-folder {run_folder}\n")

    _summary, wide, context = _scan(run_folder)
    meta = {
        "run_id": run_folder.name,
        "created_at": pd.Timestamp.now().isoformat(),
        "phase": "analysis_written",
        "project": "microcap Top100",
        "strategy": "v2.3 derived",
        "subsystem": "microcap-only overheat",
        "repo_root": str(ROOT),
        "entrypoint": "scripts/run_microcap_v2_3_microcap_only_overheat_scan.py",
        "git_branch": _git(["branch", "--show-current"]),
        "git_commit": _git(["rev-parse", "HEAD"]),
        "git_status_before": _git(["status", "--short"]),
        "git_status_after": _git(["status", "--short"]),
        "scan_type": "momentum_overheat_overlay_grid",
        "parameter_group": "momentum_overheat_exit_reentry",
        "baseline": {
            "candidate": context["baseline_label"],
            "entry_threshold": tv_base.ENTRY_THRESHOLD,
            "exit_threshold": tv_base.EXIT_THRESHOLD,
            "target_vol": TARGET_VOL,
            "max_leverage": MAX_LEVERAGE,
            "scale_rebalance_threshold": SCALE_THRESHOLD,
        },
        "candidate_grid": wide["candidate"].tolist(),
        "data_snapshot": {
            "metrics_start": context["metrics_start"],
            "metrics_end": context["metrics_end"],
            "rows": context["rows"],
            "reference_summary_latest_nav_date": context["reference_summary"].get("latest_nav_date"),
        },
        "cost_model": {
            "base": "microcap_only_tv30_max1p3_scale030",
            "overlay_trade_cost": ONE_SIDE_TRADE_COST,
            "overlay_trade_cost_model": "one_side_microcap_turnover_only_when_overheat_overlay_changes_risk_state",
            "execution_timing": "close_confirmed_t_signal_next_session_execution",
            "target_vol_return_source": "microcap_pct_change_unhedged",
            "hedge_removed": True,
        },
        "verification": {
            "all_rows_match": context["all_rows_match"],
            "all_finite_return_net": context["all_finite_return_net"],
        },
        "outputs": {
            "record": str(run_folder / "record.md"),
            "scan_summary": str(run_folder / "scan_summary.csv"),
            "window_metrics": str(run_folder / "window_metrics.csv"),
            "scan_meta": str(run_folder / "scan_meta.json"),
            "command_log": str(command_log),
            "overheat_sanity_checks": str(run_folder / "overheat_sanity_checks.csv"),
        },
        "decision": "research_only_watchlist_overheat",
        "stability_label": "overheat_first_pass",
        "elapsed_sec": round(time.time() - started, 3),
    }
    _write_json(run_folder / "scan_meta.json", meta)
    _write_record(run_folder, wide, context, meta)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-folder", type=Path, default=RUN_FOLDER)
    args = parser.parse_args()
    run(args.run_folder)


if __name__ == "__main__":
    main()
