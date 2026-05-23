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
import run_microcap_v2_3_microcap_only_target_vol_scan as tv_base  # noqa: E402


RUN_FOLDER = ROOT / "quant_param_scan_runs" / "20260523_microcap_v2_3_microcap_only_tv30_max1p3_equity_dd"
TRADING_DAYS = int(v23.TRADING_DAYS)
TARGET_VOL = 0.30
MAX_LEVERAGE = 1.3
SCALE_THRESHOLD = 0.30
ONE_SIDE_TRADE_COST = float(v23.v2_0.base_mod.freq_mod.cost_mod.ENTRY_COST)
EXIT_THRESHOLDS = (0.05, 0.08, 0.10, 0.12, 0.15, 0.18, 0.20)
RECOVERY_THRESHOLDS = (0.00, 0.02, 0.04, 0.06, 0.08, 0.10)


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


def _label(exit_threshold: float | None, recovery_threshold: float | None) -> str:
    if exit_threshold is None:
        return "no_equity_dd"
    return f"exit{int(round(exit_threshold * 100)):02d}_rec{int(round(float(recovery_threshold) * 100)):02d}"


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


def _transition_counts(out: pd.DataFrame) -> dict[str, int]:
    active = pd.to_numeric(out.get("actual_execution_scale", out.get("current_execution_scale", 0.0)), errors="coerce").fillna(0.0).gt(1e-12)
    prev = active.shift(1, fill_value=False)
    return {
        "holding_days": int(active.sum()),
        "cash_days": int((~active).sum()),
        "entry_days": int((active & ~prev).sum()),
        "exit_days": int((~active & prev).sum()),
    }


def _decision_score(row: pd.Series) -> float:
    return (
        float(row["ann_return_last_10y"])
        + 0.75 * float(row["max_dd_last_10y"])
        + 0.35 * float(row["ann_return_last_5y"])
        + 0.35 * float(row["max_dd_last_5y"])
        + 0.20 * float(row["ann_return_last_3y"])
        + 0.20 * float(row["max_dd_last_3y"])
    )


def _set_tv_globals() -> None:
    tv_base.MAX_LEVERAGE = MAX_LEVERAGE
    tv_base.SCALE_REBALANCE_THRESHOLD = SCALE_THRESHOLD


def _build_shadow_path() -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    _set_tv_globals()
    reference_summary, _signal_df, official_out = v23.generate_v2_3_outputs()
    _reference_summary2, base_gross_cached, turnover_df = v23.v2_0.embedded_context._load_embedded_base_context()
    close_df = v23._close_df_from_base(base_gross_cached)
    base = tv_base._base_signal_frame(close_df, pd.DatetimeIndex(official_out.index))
    gross = tv_base._build_entry40_exit40_gross(base)
    base_costed = tv_base._apply_base_cost(gross, turnover_df)
    shadow = tv_base._apply_microcap_only_target_vol(base_costed, TARGET_VOL)
    return reference_summary, base, shadow


def _apply_equity_dd_overlay(
    shadow: pd.DataFrame,
    exit_threshold: float | None,
    recovery_threshold: float | None,
) -> pd.DataFrame:
    out = shadow.copy().sort_index()
    if exit_threshold is None:
        out["equity_dd_exit_threshold"] = np.nan
        out["equity_dd_recovery_threshold"] = np.nan
        out["shadow_nav_net"] = out["nav_net"]
        out["shadow_drawdown"] = out["nav_net"].div(out["nav_net"].cummax()).sub(1.0)
        out["equity_dd_risk_off"] = False
        out["equity_dd_exit_triggered"] = False
        out["equity_dd_reentry_triggered"] = False
        out["actual_execution_scale"] = out["current_execution_scale"]
        out["overlay_trade_cost"] = 0.0
        return out

    exit_thr = float(exit_threshold)
    rec_thr = float(recovery_threshold)
    if exit_thr <= 0:
        raise ValueError("exit_threshold must be positive")
    if rec_thr < 0 or rec_thr > exit_thr:
        raise ValueError("recovery_threshold must be in [0, exit_threshold]")

    shadow_ret = pd.to_numeric(out["return_net"], errors="coerce").fillna(0.0)
    shadow_nav = (1.0 + shadow_ret).cumprod()
    shadow_dd = shadow_nav.div(shadow_nav.cummax()).sub(1.0)
    shadow_scale = pd.to_numeric(out["current_execution_scale"], errors="coerce").fillna(0.0)
    base_signal_active = out["next_holding"].astype(str).ne("cash")

    risk_off = False
    prev_actual_scale = 0.0
    prev_risk_off = False
    actual_returns: list[float] = []
    actual_scales: list[float] = []
    exit_flags: list[bool] = []
    reentry_flags: list[bool] = []
    risk_off_flags: list[bool] = []
    overlay_costs: list[float] = []

    for dt in out.index:
        dd_value = float(shadow_dd.loc[dt])
        exit_trigger = False
        reentry_trigger = False

        current_risk_off = risk_off
        target_scale = 0.0 if current_risk_off else float(shadow_scale.loc[dt])
        overlay_turnover = abs(target_scale - prev_actual_scale) if current_risk_off != prev_risk_off else 0.0
        overlay_cost = overlay_turnover * ONE_SIDE_TRADE_COST
        shadow_day_ret = float(shadow_ret.loc[dt])
        actual_ret = 0.0 if current_risk_off else shadow_day_ret
        actual_ret = (1.0 + actual_ret) * (1.0 - min(max(overlay_cost, 0.0), 0.99)) - 1.0

        if risk_off:
            if dd_value >= -rec_thr and bool(base_signal_active.loc[dt]):
                risk_off = False
                reentry_trigger = True
        else:
            if dd_value <= -exit_thr:
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
    out["shadow_nav_net"] = shadow_nav
    out["shadow_drawdown"] = shadow_dd
    out["equity_dd_exit_threshold"] = exit_thr
    out["equity_dd_recovery_threshold"] = rec_thr
    out["equity_dd_risk_off"] = pd.Series(risk_off_flags, index=out.index, dtype=bool)
    out["equity_dd_exit_triggered"] = pd.Series(exit_flags, index=out.index, dtype=bool)
    out["equity_dd_reentry_triggered"] = pd.Series(reentry_flags, index=out.index, dtype=bool)
    out["actual_execution_scale"] = pd.Series(actual_scales, index=out.index, dtype=float)
    out["overlay_trade_cost"] = pd.Series(overlay_costs, index=out.index, dtype=float)
    out["return_net"] = ret
    out["nav_net"] = (1.0 + ret.fillna(0.0)).cumprod()
    out["return"] = out["return_net"]
    out["nav"] = out["nav_net"]
    return out


def _candidate_grid() -> list[tuple[float | None, float | None]]:
    pairs: list[tuple[float | None, float | None]] = [(None, None)]
    for exit_thr in EXIT_THRESHOLDS:
        for rec_thr in RECOVERY_THRESHOLDS:
            if rec_thr <= exit_thr:
                pairs.append((float(exit_thr), float(rec_thr)))
    return pairs


def _scan(run_folder: Path) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    reference_summary, base, shadow = _build_shadow_path()
    summary_rows: list[dict[str, Any]] = []
    wide_rows: list[dict[str, Any]] = []
    verification_rows: list[dict[str, Any]] = []

    for exit_thr, rec_thr in _candidate_grid():
        label = _label(exit_thr, rec_thr)
        out = _apply_equity_dd_overlay(shadow, exit_thr, rec_thr)
        out.rename_axis("date").reset_index().to_csv(run_folder / f"daily_{label}.csv", index=False, encoding="utf-8")
        counts = _transition_counts(out)
        verification_rows.append(
            {
                "candidate": label,
                "rows_match": bool(len(out) == len(shadow)),
                "finite_return_net": bool(np.isfinite(pd.to_numeric(out["return_net"], errors="coerce").fillna(0.0)).all()),
                "exit_count": int(out["equity_dd_exit_triggered"].fillna(False).sum()),
                "reentry_count": int(out["equity_dd_reentry_triggered"].fillna(False).sum()),
                "risk_off_days": int(out["equity_dd_risk_off"].fillna(False).sum()),
                "final_nav": float(out["nav_net"].iloc[-1]),
            }
        )
        wide: dict[str, Any] = {
            "candidate": label,
            "equity_dd_exit_threshold": np.nan if exit_thr is None else float(exit_thr),
            "equity_dd_recovery_threshold": np.nan if rec_thr is None else float(rec_thr),
            "target_vol": TARGET_VOL,
            "max_leverage": MAX_LEVERAGE,
            "scale_rebalance_threshold": SCALE_THRESHOLD,
            "holding_days_full": counts["holding_days"],
            "cash_days_full": counts["cash_days"],
            "entry_days_full": counts["entry_days"],
            "exit_days_full": counts["exit_days"],
            "equity_dd_exit_count_full": int(out["equity_dd_exit_triggered"].fillna(False).sum()),
            "equity_dd_reentry_count_full": int(out["equity_dd_reentry_triggered"].fillna(False).sum()),
            "equity_dd_risk_off_days_full": int(out["equity_dd_risk_off"].fillna(False).sum()),
            "overlay_trade_cost_sum_full": float(pd.to_numeric(out["overlay_trade_cost"], errors="coerce").fillna(0.0).sum()),
        }
        for segment, (start, end) in _window_slices(pd.DatetimeIndex(out.index)).items():
            part = out.loc[(out.index >= start) & (out.index <= end)]
            m = _metrics(part["return_net"])
            part_counts = _transition_counts(part)
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
                "equity_dd_exit_count": int(part["equity_dd_exit_triggered"].fillna(False).sum()),
                "equity_dd_reentry_count": int(part["equity_dd_reentry_triggered"].fillna(False).sum()),
                "equity_dd_risk_off_days": int(part["equity_dd_risk_off"].fillna(False).sum()),
                "overlay_trade_cost_sum": float(pd.to_numeric(part["overlay_trade_cost"], errors="coerce").fillna(0.0).sum()),
                "equity_dd_exit_threshold": np.nan if exit_thr is None else float(exit_thr),
                "equity_dd_recovery_threshold": np.nan if rec_thr is None else float(rec_thr),
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
                "equity_dd_exit_count",
                "equity_dd_reentry_count",
                "equity_dd_risk_off_days",
                "overlay_trade_cost_sum",
            ):
                wide[f"{metric}_{segment}"] = row[metric]
        wide["decision_score"] = np.nan
        wide["decision_hint"] = "compare_only"
        wide["stability_label"] = "candidate"
        wide_rows.append(wide)

    summary = pd.DataFrame(summary_rows)
    wide = pd.DataFrame(wide_rows)
    wide["decision_score"] = wide.apply(_decision_score, axis=1)
    best_label = str(wide.sort_values("decision_score", ascending=False).iloc[0]["candidate"])
    wide.loc[wide["candidate"].eq("no_equity_dd"), "decision_hint"] = "tv30_max1p3_no_equity_dd"
    wide.loc[wide["candidate"].eq(best_label), "decision_hint"] = "best_10y_5y_3y_balanced_score"
    wide.loc[wide["candidate"].eq(best_label), "stability_label"] = "watchlist_best"

    verification = pd.DataFrame(verification_rows)
    summary.to_csv(run_folder / "scan_summary.csv", index=False, encoding="utf-8")
    wide.to_csv(run_folder / "window_metrics.csv", index=False, encoding="utf-8")
    verification.to_csv(run_folder / "equity_dd_sanity_checks.csv", index=False, encoding="utf-8")
    return summary, wide, {
        "reference_summary": reference_summary,
        "metrics_start": str(pd.Timestamp(base.index.min()).date()),
        "metrics_end": str(pd.Timestamp(base.index.max()).date()),
        "rows": int(len(base)),
        "baseline_label": "no_equity_dd",
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
            "equity_dd_exit_count_full",
            "equity_dd_reentry_count_full",
            "equity_dd_risk_off_days_full",
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
        "- Sleeve or subsystem: microcap-only equity-dd",
        "- Parameter group: `equity_drawdown_exit_reentry`",
        "- Scan type: equity_drawdown_overlay_grid",
        f"- Repo or workspace path: `{ROOT}`",
        "- Target entrypoint: `scripts/run_microcap_v2_3_microcap_only_equity_dd_scan.py`",
        f"- Git branch: `{meta['git_branch']}`",
        f"- Git commit: `{meta['git_commit']}`",
        "- Working tree status before: see `scan_meta.json`.",
        "",
        "## Research Question",
        "",
        "- Baseline: microcap-only signal `entry=40%`, `exit=40%`, target-vol `30%`, max leverage `1.3`, scale threshold `0.30`.",
        f"- Candidate grid: shadow equity drawdown exits `{list(EXIT_THRESHOLDS)}` crossed with recovery thresholds `{list(RECOVERY_THRESHOLDS)}` where recovery <= exit.",
        "- Decision target: test whether portfolio-level equity drawdown exit/reentry improves risk after single-trade stop-loss failed.",
        "- Source-change rule: `research_only_no_source_change`.",
        "- Required windows: full, last_10y, last_5y, last_3y, last_1y.",
        "",
        "## Implementation Anchor",
        "",
        "- Shadow path: baseline TV30/max1.3 strategy without equity drawdown overlay.",
        "- Exit trigger: shadow NAV drawdown falls below `-exit_threshold`.",
        "- Recovery trigger: shadow NAV drawdown recovers to at least `-recovery_threshold` and base signal is active.",
        "- Execution timing: shadow NAV drawdown is observed at close on T; exit or recovery is applied from T+1, so the trigger-day return is retained.",
        "- Actual path: returns are set to cash while risk-off; overlay exit/reentry charges 30bp one-side microcap turnover only when the drawdown overlay changes risk state.",
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
        "- Added: overlay microcap one-side trading cost on equity-DD risk-off/risk-on scale changes.",
        "- No hedge leg, futures drag, or hedge turnover.",
        "",
        "## Commands",
        "",
        "```powershell",
        f"python scripts/run_microcap_v2_3_microcap_only_equity_dd_scan.py --run-folder {run_folder}",
        f"python C:\\Users\\Administrator.DESKTOP-95I7VVU\\.codex\\skills\\quant-param-scan\\scripts\\finalize_quant_param_scan_run.py {run_folder} --decision research_only_watchlist_equity_dd --stability-label equity_dd_first_pass",
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
        "- Label: equity_dd_first_pass.",
        "- Evidence: compare `window_metrics.csv`; this is not a production promotion.",
        f"- Candidate count: {context['candidate_count']}.",
        f"- Sanity: all candidates row-match baseline: {context['all_rows_match']}; finite return_net: {context['all_finite_return_net']}.",
        "",
        "## Decision",
        "",
        "- Decision: research_only_watchlist_equity_dd.",
        "- Recommended next action: inspect whether top rows form a stable threshold plateau.",
    ]
    (run_folder / "record.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(run_folder: Path) -> None:
    started = time.time()
    run_folder.mkdir(parents=True, exist_ok=True)
    command_log = run_folder / "command_log.txt"
    with command_log.open("a", encoding="utf-8") as fh:
        fh.write(f"\n[{pd.Timestamp.now().isoformat()}] python scripts/run_microcap_v2_3_microcap_only_equity_dd_scan.py --run-folder {run_folder}\n")

    _summary, wide, context = _scan(run_folder)
    meta = {
        "run_id": run_folder.name,
        "created_at": pd.Timestamp.now().isoformat(),
        "phase": "analysis_written",
        "project": "microcap Top100",
        "strategy": "v2.3 derived",
        "subsystem": "microcap-only equity-dd",
        "repo_root": str(ROOT),
        "entrypoint": "scripts/run_microcap_v2_3_microcap_only_equity_dd_scan.py",
        "git_branch": _git(["branch", "--show-current"]),
        "git_commit": _git(["rev-parse", "HEAD"]),
        "git_status_before": _git(["status", "--short"]),
        "git_status_after": _git(["status", "--short"]),
        "scan_type": "equity_drawdown_overlay_grid",
        "parameter_group": "equity_drawdown_exit_reentry",
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
            "overlay_trade_cost_model": "one_side_microcap_turnover_only_when_equity_dd_overlay_changes_risk_state",
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
            "equity_dd_sanity_checks": str(run_folder / "equity_dd_sanity_checks.csv"),
        },
        "decision": "research_only_watchlist_equity_dd",
        "stability_label": "equity_dd_first_pass",
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
