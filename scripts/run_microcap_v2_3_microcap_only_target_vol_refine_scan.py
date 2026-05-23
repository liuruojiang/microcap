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


RUN_FOLDER = ROOT / "quant_param_scan_runs" / "20260523_microcap_v2_3_microcap_only_entry40_exit40_target_vol_refine"
TRADING_DAYS = int(v23.TRADING_DAYS)
TARGET_VOL_VALUES = (0.24, 0.27, 0.30, 0.33, 0.36)
MAX_LEVERAGE_VALUES = (1.2, 1.3, 1.5)
SCALE_THRESHOLDS = (0.10, 0.20, 0.30)


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


def _label(target_vol: float | None, max_lev: float | None = None, scale_threshold: float | None = None) -> str:
    if target_vol is None:
        return "no_target_vol"
    return (
        f"tv{int(round(target_vol * 100)):02d}_"
        f"max{str(max_lev).replace('.', 'p')}_"
        f"thr{str(scale_threshold).replace('.', 'p')}"
    )


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


def _decision_score(row: pd.Series) -> float:
    return (
        float(row["ann_return_last_10y"])
        + 0.75 * float(row["max_dd_last_10y"])
        + 0.35 * float(row["ann_return_last_5y"])
        + 0.35 * float(row["max_dd_last_5y"])
        + 0.20 * float(row["ann_return_last_3y"])
        + 0.20 * float(row["max_dd_last_3y"])
    )


def _set_tv_globals(max_lev: float, scale_threshold: float) -> None:
    tv_base.MAX_LEVERAGE = float(max_lev)
    tv_base.SCALE_REBALANCE_THRESHOLD = float(scale_threshold)


def _scan(run_folder: Path) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    reference_summary, _signal_df, official_out = v23.generate_v2_3_outputs()
    _reference_summary2, base_gross_cached, turnover_df = v23.v2_0.embedded_context._load_embedded_base_context()
    close_df = v23._close_df_from_base(base_gross_cached)
    base = tv_base._base_signal_frame(close_df, pd.DatetimeIndex(official_out.index))
    gross = tv_base._build_entry40_exit40_gross(base)
    base_costed = tv_base._apply_base_cost(gross, turnover_df)

    summary_rows: list[dict[str, Any]] = []
    wide_rows: list[dict[str, Any]] = []
    verification_rows: list[dict[str, Any]] = []

    candidates: list[tuple[str, float | None, float | None, float | None, pd.DataFrame]] = [
        ("no_target_vol", None, None, None, tv_base._apply_microcap_only_target_vol(base_costed, None))
    ]
    for target_vol in TARGET_VOL_VALUES:
        for max_lev in MAX_LEVERAGE_VALUES:
            for scale_threshold in SCALE_THRESHOLDS:
                _set_tv_globals(max_lev, scale_threshold)
                label = _label(target_vol, max_lev, scale_threshold)
                out = tv_base._apply_microcap_only_target_vol(base_costed, target_vol)
                candidates.append((label, float(target_vol), float(max_lev), float(scale_threshold), out))

    for label, target_vol, max_lev, scale_threshold, out in candidates:
        out.rename_axis("date").reset_index().to_csv(run_folder / f"daily_{label}.csv", index=False, encoding="utf-8")
        counts = tv_base._transition_counts(out)
        verification_rows.append(
            {
                "candidate": label,
                "rows_match": bool(len(out) == len(base_costed)),
                "finite_return_net": bool(np.isfinite(pd.to_numeric(out["return_net"], errors="coerce").fillna(0.0)).all()),
                "final_nav": float(out["nav_net"].iloc[-1]),
            }
        )
        wide: dict[str, Any] = {
            "candidate": label,
            "target_vol": np.nan if target_vol is None else float(target_vol),
            "max_leverage": np.nan if max_lev is None else float(max_lev),
            "scale_rebalance_threshold": np.nan if scale_threshold is None else float(scale_threshold),
            "entry_threshold": tv_base.ENTRY_THRESHOLD,
            "exit_threshold": tv_base.EXIT_THRESHOLD,
            "vol_window": tv_base.VOL_WINDOW,
            "holding_days_full": counts["holding_days"],
            "cash_days_full": counts["cash_days"],
            "entry_days_full": counts["entry_days"],
            "exit_days_full": counts["exit_days"],
        }
        for segment, (start, end) in _window_slices(pd.DatetimeIndex(out.index)).items():
            part = out.loc[(out.index >= start) & (out.index <= end)]
            m = _metrics(part["return_net"])
            part_counts = tv_base._transition_counts(part)
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
                "target_vol": np.nan if target_vol is None else float(target_vol),
                "max_leverage": np.nan if max_lev is None else float(max_lev),
                "scale_rebalance_threshold": np.nan if scale_threshold is None else float(scale_threshold),
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
    wide.loc[wide["candidate"].eq("no_target_vol"), "decision_hint"] = "entry40_exit40_no_target_vol"
    wide.loc[wide["candidate"].eq(best_label), "decision_hint"] = "best_10y_5y_3y_balanced_score"
    wide.loc[wide["candidate"].eq(best_label), "stability_label"] = "watchlist_best"

    verification = pd.DataFrame(verification_rows)
    summary.to_csv(run_folder / "scan_summary.csv", index=False, encoding="utf-8")
    wide.to_csv(run_folder / "window_metrics.csv", index=False, encoding="utf-8")
    verification.to_csv(run_folder / "target_vol_refine_sanity_checks.csv", index=False, encoding="utf-8")
    return summary, wide, {
        "reference_summary": reference_summary,
        "turnover_rows": int(len(turnover_df)),
        "metrics_start": str(pd.Timestamp(base.index.min()).date()),
        "metrics_end": str(pd.Timestamp(base.index.max()).date()),
        "rows": int(len(base)),
        "close_df_start": str(pd.Timestamp(close_df.index.min()).date()),
        "close_df_end": str(pd.Timestamp(close_df.index.max()).date()),
        "baseline_label": "no_target_vol",
        "best_label": best_label,
        "candidate_count": int(len(wide)),
        "all_rows_match": bool(verification["rows_match"].all()),
        "all_finite_return_net": bool(verification["finite_return_net"].all()),
    }


def _write_record(run_folder: Path, wide: pd.DataFrame, context: dict[str, Any], meta: dict[str, Any]) -> None:
    ordered = wide.sort_values("decision_score", ascending=False)
    best = ordered.iloc[0]
    baseline = wide.loc[wide["candidate"].eq(context["baseline_label"])].iloc[0]
    top15 = ordered.head(15)[
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
            "avg_execution_scale_full",
            "max_execution_scale_full",
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
        "- Sleeve or subsystem: microcap-only target-vol",
        "- Parameter group: `target_vol_maxlev_scale_threshold`",
        "- Scan type: target_volatility_grid_refine",
        f"- Repo or workspace path: `{ROOT}`",
        "- Target entrypoint: `scripts/run_microcap_v2_3_microcap_only_target_vol_refine_scan.py`",
        f"- Git branch: `{meta['git_branch']}`",
        f"- Git commit: `{meta['git_commit']}`",
        "- Working tree status before: see `scan_meta.json`.",
        "",
        "## Research Question",
        "",
        "- Baseline signal: microcap-only annualized log-WLS, `entry=40%`, `exit=40%`.",
        f"- Candidate grid: target volatility `{list(TARGET_VOL_VALUES)}`, max leverage `{list(MAX_LEVERAGE_VALUES)}`, scale threshold `{list(SCALE_THRESHOLDS)}`.",
        "- Decision target: refine the target-vol layer after the first pass selected around TV30.",
        "- Source-change rule: `research_only_no_source_change`.",
        "- Required windows: full, last_10y, last_5y, last_3y, last_1y.",
        "",
        "## Implementation Anchor",
        "",
        "- Reuses microcap-only target-vol implementation from `run_microcap_v2_3_microcap_only_target_vol_scan.py`.",
        "- Target-vol return source: unhedged microcap Top100 pct-change.",
        "- Target-vol turnover model: microcap single leg only.",
        "- Existing Top100 base transaction-cost model retained and scaled by execution exposure.",
        "- No production constants changed.",
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
        "- Added: same-holding target-vol scale-change cost at 10bp of microcap single-leg turnover.",
        "- Added: 3% annual financing cost on exposure above 1.0x.",
        "- Added: 2% annual idle-cash credit only on active days with exposure below 1.0x; full cash days remain 0 return.",
        "- Removed: ZZ1000 hedge, futures drag, and hedge-leg turnover.",
        "",
        "## Commands",
        "",
        "```powershell",
        f"python scripts/run_microcap_v2_3_microcap_only_target_vol_refine_scan.py --run-folder {run_folder}",
        f"python C:\\Users\\Administrator.DESKTOP-95I7VVU\\.codex\\skills\\quant-param-scan\\scripts\\finalize_quant_param_scan_run.py {run_folder} --decision research_only_watchlist_target_vol_refine --stability-label target_vol_refine",
        f"python C:\\Users\\Administrator.DESKTOP-95I7VVU\\.codex\\skills\\quant-param-scan\\scripts\\check_quant_param_scan_artifacts.py --phase complete --strict {run_folder}",
        "```",
        "",
        "## Output Files",
        "",
        "- `scan_summary.csv`: long-form window metrics.",
        "- `window_metrics.csv`: wide candidate table.",
        "- `target_vol_refine_sanity_checks.csv`: finite-return and row-count checks.",
        "- `daily_*.csv`: candidate daily paths.",
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
        "## Top Candidates",
        "",
        top15.to_markdown(index=False, floatfmt=".6f"),
        "",
        "## Stability Classification",
        "",
        "- Label: target_vol_refine.",
        "- Evidence: compare `window_metrics.csv`; this is not a production promotion.",
        f"- Candidate count: {context['candidate_count']}.",
        f"- Sanity: all candidates row-match baseline: {context['all_rows_match']}; finite return_net: {context['all_finite_return_net']}.",
        "",
        "## Decision",
        "",
        "- Decision: research_only_watchlist_target_vol_refine.",
        "- Recommended next action: choose a main/side target-vol branch before testing stop-loss or take-profit layers.",
    ]
    (run_folder / "record.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(run_folder: Path) -> None:
    started = time.time()
    run_folder.mkdir(parents=True, exist_ok=True)
    command_log = run_folder / "command_log.txt"
    with command_log.open("a", encoding="utf-8") as fh:
        fh.write(f"\n[{pd.Timestamp.now().isoformat()}] python scripts/run_microcap_v2_3_microcap_only_target_vol_refine_scan.py --run-folder {run_folder}\n")

    _summary, wide, context = _scan(run_folder)
    meta = {
        "run_id": run_folder.name,
        "created_at": pd.Timestamp.now().isoformat(),
        "phase": "analysis_written",
        "project": "microcap Top100",
        "strategy": "v2.3 derived",
        "subsystem": "microcap-only target-vol",
        "repo_root": str(ROOT),
        "entrypoint": "scripts/run_microcap_v2_3_microcap_only_target_vol_refine_scan.py",
        "git_branch": _git(["branch", "--show-current"]),
        "git_commit": _git(["rev-parse", "HEAD"]),
        "git_status_before": _git(["status", "--short"]),
        "git_status_after": _git(["status", "--short"]),
        "scan_type": "target_volatility_grid_refine",
        "parameter_group": "target_vol_maxlev_scale_threshold",
        "baseline": {
            "candidate": context["baseline_label"],
            "entry_threshold": tv_base.ENTRY_THRESHOLD,
            "exit_threshold": tv_base.EXIT_THRESHOLD,
            "lookback": int(v23.LOOKBACK),
            "halflife": float(v23.HALFLIFE),
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
            "vol_window": tv_base.VOL_WINDOW,
            "hedge_removed": True,
            "cash_day_full_yield_enabled": False,
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
            "target_vol_refine_sanity_checks": str(run_folder / "target_vol_refine_sanity_checks.csv"),
        },
        "decision": "research_only_watchlist_target_vol_refine",
        "stability_label": "target_vol_refine",
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
