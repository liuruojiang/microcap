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


RUN_FOLDER = ROOT / "quant_param_scan_runs" / "20260523_microcap_v2_3_microcap_only_tv30_max1p3_stop_loss"
TRADING_DAYS = int(v23.TRADING_DAYS)
TARGET_VOL = 0.30
MAX_LEVERAGE = 1.3
SCALE_THRESHOLD = 0.30
STOP_THRESHOLDS: tuple[float | None, ...] = (None, 0.03, 0.05, 0.08, 0.10, 0.12, 0.15, 0.20)


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


def _label(stop_threshold: float | None) -> str:
    if stop_threshold is None:
        return "no_stop"
    return f"stop{int(round(float(stop_threshold) * 100)):02d}"


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


def _apply_single_trade_drawdown_stop(gross: pd.DataFrame, stop_threshold: float | None) -> pd.DataFrame:
    if stop_threshold is None:
        out = gross.copy()
        out["stop_loss_threshold"] = np.nan
        out["stop_loss_triggered"] = False
        out["stop_loss_blocked"] = False
        out["trade_nav"] = np.where(out["holding"].astype(str).ne("cash"), 1.0, np.nan)
        out["trade_drawdown"] = 0.0
        return out
    threshold = float(stop_threshold)
    if threshold <= 0:
        raise ValueError("stop_threshold must be positive")

    base = gross.copy().sort_index()
    current_active = False
    blocked_until_signal_reset = False
    trade_nav = 1.0
    trade_peak = 1.0
    rows: list[dict[str, Any]] = []
    for dt, row in base.iterrows():
        base_next_active = str(row.get("next_holding", "cash")) != "cash"
        micro_ret = row.get("microcap_ret", np.nan)
        day_ret = float(micro_ret) if current_active and pd.notna(micro_ret) else 0.0
        if current_active:
            trade_nav *= 1.0 + day_ret
            trade_peak = max(trade_peak, trade_nav)
            trade_drawdown = trade_nav / trade_peak - 1.0
        else:
            trade_nav = 1.0
            trade_peak = 1.0
            trade_drawdown = 0.0

        stop_triggered = bool(current_active and trade_drawdown <= -threshold)
        if stop_triggered:
            next_active = False
            blocked_until_signal_reset = True
        elif blocked_until_signal_reset:
            if not base_next_active:
                blocked_until_signal_reset = False
            next_active = False
        else:
            next_active = bool(base_next_active)

        rows.append(
            {
                "return_raw": day_ret,
                "return": day_ret,
                "holding": "long_microcap_top100" if current_active else "cash",
                "next_holding": "long_microcap_top100" if next_active else "cash",
                "signal_on": bool(next_active),
                "base_next_holding_before_stop": row.get("next_holding", "cash"),
                "base_signal_on_before_stop": bool(base_next_active),
                "stop_loss_threshold": threshold,
                "stop_loss_triggered": stop_triggered,
                "stop_loss_blocked": bool(blocked_until_signal_reset and not stop_triggered),
                "trade_nav": trade_nav if current_active else np.nan,
                "trade_peak": trade_peak if current_active else np.nan,
                "trade_drawdown": trade_drawdown if current_active else 0.0,
            }
        )
        current_active = bool(next_active)
        if not current_active and not blocked_until_signal_reset:
            trade_nav = 1.0
            trade_peak = 1.0

    adjusted = pd.DataFrame(rows, index=base.index)
    out = base.copy()
    for col in adjusted.columns:
        out[col] = adjusted[col]
    out["nav_gross"] = (1.0 + out["return"].fillna(0.0)).cumprod()
    return out


def _transition_counts(out: pd.DataFrame) -> dict[str, int]:
    holding = out["holding"].astype(str)
    prev = holding.shift(1).fillna("cash")
    return {
        "holding_days": int(holding.ne("cash").sum()),
        "cash_days": int(holding.eq("cash").sum()),
        "entry_days": int((holding.ne("cash") & prev.eq("cash")).sum()),
        "exit_days": int((holding.eq("cash") & prev.ne("cash")).sum()),
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


def _scan(run_folder: Path) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    _set_tv_globals()
    reference_summary, _signal_df, official_out = v23.generate_v2_3_outputs()
    _reference_summary2, base_gross_cached, turnover_df = v23.v2_0.embedded_context._load_embedded_base_context()
    close_df = v23._close_df_from_base(base_gross_cached)
    base = tv_base._base_signal_frame(close_df, pd.DatetimeIndex(official_out.index))
    gross_signal = tv_base._build_entry40_exit40_gross(base)

    summary_rows: list[dict[str, Any]] = []
    wide_rows: list[dict[str, Any]] = []
    verification_rows: list[dict[str, Any]] = []

    for stop_threshold in STOP_THRESHOLDS:
        label = _label(stop_threshold)
        stopped_gross = _apply_single_trade_drawdown_stop(gross_signal, stop_threshold)
        base_costed = tv_base._apply_base_cost(stopped_gross, turnover_df)
        out = tv_base._apply_microcap_only_target_vol(base_costed, TARGET_VOL)
        out["stop_loss_threshold"] = np.nan if stop_threshold is None else float(stop_threshold)
        out["stop_loss_triggered"] = stopped_gross["stop_loss_triggered"]
        out["stop_loss_blocked"] = stopped_gross["stop_loss_blocked"]
        out["trade_drawdown_before_tv"] = stopped_gross["trade_drawdown"]
        out.rename_axis("date").reset_index().to_csv(run_folder / f"daily_{label}.csv", index=False, encoding="utf-8")
        counts = _transition_counts(out)
        verification_rows.append(
            {
                "candidate": label,
                "rows_match": bool(len(out) == len(gross_signal)),
                "finite_return_net": bool(np.isfinite(pd.to_numeric(out["return_net"], errors="coerce").fillna(0.0)).all()),
                "stop_trigger_count": int(out["stop_loss_triggered"].fillna(False).sum()),
                "final_nav": float(out["nav_net"].iloc[-1]),
            }
        )
        wide: dict[str, Any] = {
            "candidate": label,
            "stop_threshold": np.nan if stop_threshold is None else float(stop_threshold),
            "target_vol": TARGET_VOL,
            "max_leverage": MAX_LEVERAGE,
            "scale_rebalance_threshold": SCALE_THRESHOLD,
            "entry_threshold": tv_base.ENTRY_THRESHOLD,
            "exit_threshold": tv_base.EXIT_THRESHOLD,
            "holding_days_full": counts["holding_days"],
            "cash_days_full": counts["cash_days"],
            "entry_days_full": counts["entry_days"],
            "exit_days_full": counts["exit_days"],
            "stop_trigger_count_full": int(out["stop_loss_triggered"].fillna(False).sum()),
            "blocked_days_full": int(out["stop_loss_blocked"].fillna(False).sum()),
        }
        for segment, (start, end) in _window_slices(pd.DatetimeIndex(out.index)).items():
            part = out.loc[(out.index >= start) & (out.index <= end)]
            m = _metrics(part["return_net"])
            part_counts = _transition_counts(part)
            scale = pd.to_numeric(part.get("current_execution_scale", np.nan), errors="coerce")
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
                "avg_execution_scale": float(scale.mean()) if len(part) else np.nan,
                "max_execution_scale": float(scale.max()) if len(part) else np.nan,
                "leverage_gt_1_days": int(scale.fillna(0.0).gt(1.0).sum()) if len(part) else 0,
                "base_trade_cost_sum": float(pd.to_numeric(part.get("base_trade_cost_scaled", part.get("total_cost", 0.0)), errors="coerce").fillna(0.0).sum()) if len(part) else 0.0,
                "scale_change_cost_sum": float(pd.to_numeric(part.get("scale_change_cost", 0.0), errors="coerce").fillna(0.0).sum()) if len(part) else 0.0,
                "financing_cost_sum": float(pd.to_numeric(part.get("financing_cost", 0.0), errors="coerce").fillna(0.0).sum()) if len(part) else 0.0,
                "stop_threshold": np.nan if stop_threshold is None else float(stop_threshold),
                "stop_trigger_count": int(part["stop_loss_triggered"].fillna(False).sum()),
                "blocked_days": int(part["stop_loss_blocked"].fillna(False).sum()),
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
                "leverage_gt_1_days",
                "base_trade_cost_sum",
                "scale_change_cost_sum",
                "financing_cost_sum",
                "stop_trigger_count",
                "blocked_days",
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
    wide.loc[wide["candidate"].eq("no_stop"), "decision_hint"] = "tv30_max1p3_no_stop"
    wide.loc[wide["candidate"].eq(best_label), "decision_hint"] = "best_10y_5y_3y_balanced_score"
    wide.loc[wide["candidate"].eq(best_label), "stability_label"] = "watchlist_best"

    verification = pd.DataFrame(verification_rows)
    summary.to_csv(run_folder / "scan_summary.csv", index=False, encoding="utf-8")
    wide.to_csv(run_folder / "window_metrics.csv", index=False, encoding="utf-8")
    verification.to_csv(run_folder / "stop_loss_sanity_checks.csv", index=False, encoding="utf-8")
    return summary, wide, {
        "reference_summary": reference_summary,
        "turnover_rows": int(len(turnover_df)),
        "metrics_start": str(pd.Timestamp(base.index.min()).date()),
        "metrics_end": str(pd.Timestamp(base.index.max()).date()),
        "rows": int(len(base)),
        "close_df_start": str(pd.Timestamp(close_df.index.min()).date()),
        "close_df_end": str(pd.Timestamp(close_df.index.max()).date()),
        "baseline_label": "no_stop",
        "best_label": best_label,
        "candidate_count": int(len(wide)),
        "all_rows_match": bool(verification["rows_match"].all()),
        "all_finite_return_net": bool(verification["finite_return_net"].all()),
    }


def _write_record(run_folder: Path, wide: pd.DataFrame, context: dict[str, Any], meta: dict[str, Any]) -> None:
    ordered = wide.sort_values("decision_score", ascending=False)
    best = ordered.iloc[0]
    baseline = wide.loc[wide["candidate"].eq(context["baseline_label"])].iloc[0]
    top = ordered[
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
            "stop_trigger_count_full",
            "blocked_days_full",
            "avg_execution_scale_full",
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
        "- Sleeve or subsystem: microcap-only stop-loss",
        "- Parameter group: `single_trade_drawdown_stop`",
        "- Scan type: stop_loss_grid",
        f"- Repo or workspace path: `{ROOT}`",
        "- Target entrypoint: `scripts/run_microcap_v2_3_microcap_only_stop_loss_scan.py`",
        f"- Git branch: `{meta['git_branch']}`",
        f"- Git commit: `{meta['git_commit']}`",
        "- Working tree status before: see `scan_meta.json`.",
        "",
        "## Research Question",
        "",
        "- Baseline: microcap-only signal `entry=40%`, `exit=40%`, target-vol `30%`, max leverage `1.3`, scale threshold `0.30`.",
        f"- Candidate grid: single-trade drawdown stop thresholds `{list(STOP_THRESHOLDS)}`.",
        "- Decision target: test whether a close-confirmed per-trade stop-loss improves risk before adding take-profit or signal-decay layers.",
        "- Source-change rule: `research_only_no_source_change`.",
        "- Required windows: full, last_10y, last_5y, last_3y, last_1y.",
        "",
        "## Implementation Anchor",
        "",
        "- Stop-loss layer is applied after the 40/40 signal state and before costs/target-vol.",
        "- Trigger: close-confirmed drawdown from the current trade high-water mark is below the threshold.",
        "- Execution timing: stop affects next trading day; the trigger day keeps the current position return.",
        "- Reentry: after a stop, the strategy stays cash until the base signal first resets to cash, then it may reenter on a new entry signal.",
        "- Target-vol is recomputed on the stopped holding path, so stopped cash days have zero strategy return before later overlays.",
        "",
        "## Data Snapshot",
        "",
        f"- Metrics start: {context['metrics_start']}",
        f"- Metrics end: {context['metrics_end']}",
        f"- Rows: {context['rows']}",
        f"- Close data start: {context['close_df_start']}",
        f"- Close data end: {context['close_df_end']}",
        f"- Turnover rows: {context['turnover_rows']}",
        f"- Reference v2.3 latest NAV date: {context['reference_summary'].get('latest_nav_date')}",
        "- Trading calendar: strategy local trading-date index; annualization uses 244 trading days.",
        "",
        "## Cost and Execution Assumptions",
        "",
        "- Retained: Top100 basket entry/exit/rebalance transaction costs, scaled by target-vol exposure.",
        "- Retained: microcap-only target-vol scale-change cost and financing cost.",
        "- Removed: ZZ1000 hedge, futures drag, and hedge-leg turnover.",
        "",
        "## Commands",
        "",
        "```powershell",
        f"python scripts/run_microcap_v2_3_microcap_only_stop_loss_scan.py --run-folder {run_folder}",
        f"python C:\\Users\\Administrator.DESKTOP-95I7VVU\\.codex\\skills\\quant-param-scan\\scripts\\finalize_quant_param_scan_run.py {run_folder} --decision research_only_watchlist_stop_loss --stability-label stop_loss_first_pass",
        f"python C:\\Users\\Administrator.DESKTOP-95I7VVU\\.codex\\skills\\quant-param-scan\\scripts\\check_quant_param_scan_artifacts.py --phase complete --strict {run_folder}",
        "```",
        "",
        "## Full-Sample Results",
        "",
        f"- Baseline `{context['baseline_label']}`: annual return {baseline['ann_return_full']:.4%}, max drawdown {baseline['max_dd_full']:.4%}, Sharpe {baseline['sharpe_repo_full']:.3f}.",
        f"- Best balanced candidate `{best['candidate']}`: annual return {best['ann_return_full']:.4%}, max drawdown {best['max_dd_full']:.4%}, Sharpe {best['sharpe_repo_full']:.3f}.",
        "",
        "## Window Results",
        "",
        f"- Best 10Y: annual return {best['ann_return_last_10y']:.4%}, max drawdown {best['max_dd_last_10y']:.4%}.",
        f"- Best 5Y: annual return {best['ann_return_last_5y']:.4%}, max drawdown {best['max_dd_last_5y']:.4%}.",
        f"- Best 3Y: annual return {best['ann_return_last_3y']:.4%}, max drawdown {best['max_dd_last_3y']:.4%}.",
        f"- Best 1Y: annual return {best['ann_return_last_1y']:.4%}, max drawdown {best['max_dd_last_1y']:.4%}.",
        "",
        "## Candidates",
        "",
        top.to_markdown(index=False, floatfmt=".6f"),
        "",
        "## Stability Classification",
        "",
        "- Label: stop_loss_first_pass.",
        "- Evidence: compare `window_metrics.csv`; this is not a production promotion.",
        f"- Candidate count: {context['candidate_count']}.",
        f"- Sanity: all candidates row-match baseline: {context['all_rows_match']}; finite return_net: {context['all_finite_return_net']}.",
        "",
        "## Decision",
        "",
        "- Decision: research_only_watchlist_stop_loss.",
        "- Recommended next action: choose whether to keep no-stop or inspect the best threshold with local robustness before adding take-profit.",
    ]
    (run_folder / "record.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(run_folder: Path) -> None:
    started = time.time()
    run_folder.mkdir(parents=True, exist_ok=True)
    command_log = run_folder / "command_log.txt"
    with command_log.open("a", encoding="utf-8") as fh:
        fh.write(f"\n[{pd.Timestamp.now().isoformat()}] python scripts/run_microcap_v2_3_microcap_only_stop_loss_scan.py --run-folder {run_folder}\n")

    _summary, wide, context = _scan(run_folder)
    meta = {
        "run_id": run_folder.name,
        "created_at": pd.Timestamp.now().isoformat(),
        "phase": "analysis_written",
        "project": "microcap Top100",
        "strategy": "v2.3 derived",
        "subsystem": "microcap-only stop-loss",
        "repo_root": str(ROOT),
        "entrypoint": "scripts/run_microcap_v2_3_microcap_only_stop_loss_scan.py",
        "git_branch": _git(["branch", "--show-current"]),
        "git_commit": _git(["rev-parse", "HEAD"]),
        "git_status_before": _git(["status", "--short"]),
        "git_status_after": _git(["status", "--short"]),
        "scan_type": "stop_loss_grid",
        "parameter_group": "single_trade_drawdown_stop",
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
            "turnover_rows": context["turnover_rows"],
            "reference_summary_latest_nav_date": context["reference_summary"].get("latest_nav_date"),
            "close_df_start": context["close_df_start"],
            "close_df_end": context["close_df_end"],
        },
        "cost_model": {
            "retained": "top100_basket_transaction_cost_model_scaled_by_exposure",
            "target_vol_return_source": "microcap_pct_change_unhedged",
            "target_vol_turnover_model": "microcap_single_leg_only",
            "scale_change_cost": tv_base.SCALE_CHANGE_COST,
            "financing_rate": tv_base.FINANCING_RATE,
            "idle_cash_yield": tv_base.IDLE_CASH_YIELD,
            "max_leverage": MAX_LEVERAGE,
            "scale_rebalance_threshold": SCALE_THRESHOLD,
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
            "stop_loss_sanity_checks": str(run_folder / "stop_loss_sanity_checks.csv"),
        },
        "decision": "research_only_watchlist_stop_loss",
        "stability_label": "stop_loss_first_pass",
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
