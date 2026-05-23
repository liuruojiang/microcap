from __future__ import annotations

import argparse
import json
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
from run_microcap_v2_3_strategy_risk_diagnostics import (  # noqa: E402
    CASH_YIELD,
    _build_candidate,
    _candidate_label,
    _git,
    _json_safe,
    _metrics,
    _window_slices,
)


EXIT_BUFFERS = (0.08, 0.10, 0.13, 0.15, 0.18, 0.20)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(_json_safe(payload), ensure_ascii=False, indent=2), encoding="utf-8")


def _transition_counts(out: pd.DataFrame) -> dict[str, int]:
    holding = out["holding"].astype(str)
    next_holding = out["next_holding"].astype(str)
    return {
        "entry_signals": int(holding.eq("cash").mul(next_holding.ne("cash")).sum()),
        "exit_signals": int(holding.ne("cash").mul(next_holding.eq("cash")).sum()),
        "holding_days": int(holding.ne("cash").sum()),
        "cash_days": int(holding.eq("cash").sum()),
    }


def _score_candidate(row: pd.Series) -> float:
    # Prefer robust recent behavior, then drawdown control, then full-sample return.
    return (
        float(row["ann_return_last_3y"])
        + 0.50 * float(row["ann_return_last_5y"])
        + 0.25 * float(row["ann_return_last_1y"])
        + 0.75 * float(row["max_dd_last_3y"])
        + 0.50 * float(row["max_dd_last_5y"])
    )


def _scan(run_folder: Path) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, pd.DataFrame], dict[str, Any]]:
    reference_summary, _signal_df, official_out = v23.generate_v2_3_outputs()
    _reference_summary2, base_gross_cached, turnover_df = v23.v2_0.embedded_context._load_embedded_base_context()
    close_df = v23._close_df_from_base(base_gross_cached)
    common_index = pd.DatetimeIndex(official_out.index)
    gross = v23.build_spread_log_wls_gross(close_df, common_index)

    summary_rows: list[dict[str, Any]] = []
    wide_rows: list[dict[str, Any]] = []
    outputs: dict[str, pd.DataFrame] = {}
    for exit_buffer in EXIT_BUFFERS:
        label = _candidate_label(exit_buffer, True)
        out = _build_candidate(gross, turnover_df, exit_buffer, True)
        outputs[label] = out
        counts = _transition_counts(out)
        wide: dict[str, Any] = {
            "candidate": label,
            "exit_buffer": exit_buffer,
            "cash_yield_on_cash_days": True,
            "cash_yield_annual": CASH_YIELD,
            **{f"{key}_full": value for key, value in counts.items()},
        }
        for segment, (start, end) in _window_slices(pd.DatetimeIndex(out.index)).items():
            part = out.loc[(out.index >= start) & (out.index <= end)]
            m = _metrics(part["return_net"])
            part_counts = _transition_counts(part)
            cost_total = float(pd.to_numeric(part.get("total_cost", 0.0), errors="coerce").fillna(0.0).sum()) if len(part) else 0.0
            scale_cost_total = float(pd.to_numeric(part.get("scale_change_cost", 0.0), errors="coerce").fillna(0.0).sum()) if len(part) else 0.0
            base_cost_scaled = float(pd.to_numeric(part.get("base_trade_cost_scaled", 0.0), errors="coerce").fillna(0.0).sum()) if len(part) else 0.0
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
                "entry_signals": part_counts["entry_signals"],
                "exit_signals": part_counts["exit_signals"],
                "cost_total": cost_total,
                "base_trade_cost_scaled_total": base_cost_scaled,
                "scale_change_cost_total": scale_cost_total,
                "exit_buffer": exit_buffer,
                "cash_yield_on_cash_days": True,
                "cash_yield_annual": CASH_YIELD,
                "signal_spread_hedge_ratio": v23.SIGNAL_SPREAD_HEDGE_RATIO,
                "execution_hedge_ratio": v23.EXECUTION_HEDGE_RATIO,
            }
            summary_rows.append(row)
            for metric in (
                "ann_return",
                "max_dd",
                "sharpe_repo",
                "holding_day_ratio",
                "entry_signals",
                "exit_signals",
                "cost_total",
                "base_trade_cost_scaled_total",
                "scale_change_cost_total",
            ):
                wide[f"{metric}_{segment}"] = row[metric]
        wide["decision_score"] = np.nan
        wide["decision_hint"] = "pending"
        wide["stability_label"] = "candidate"
        wide_rows.append(wide)
        out.reset_index(names="date").to_csv(run_folder / f"daily_{label}.csv", index=False, encoding="utf-8")

    summary = pd.DataFrame(summary_rows)
    wide = pd.DataFrame(wide_rows)
    wide["decision_score"] = wide.apply(_score_candidate, axis=1)
    best_label = str(wide.sort_values("decision_score", ascending=False).iloc[0]["candidate"])
    wide.loc[wide["candidate"].eq(best_label), "decision_hint"] = "best_recent_balanced_score"
    wide.loc[~wide["candidate"].eq(best_label), "decision_hint"] = "compare_only"
    summary.to_csv(run_folder / "scan_summary.csv", index=False, encoding="utf-8")
    wide.to_csv(run_folder / "window_metrics.csv", index=False, encoding="utf-8")
    context = {
        "reference_summary": reference_summary,
        "turnover_rows": int(len(turnover_df)),
        "common_start": str(pd.Timestamp(common_index.min()).date()),
        "common_end": str(pd.Timestamp(common_index.max()).date()),
        "common_rows": int(len(common_index)),
        "best_label": best_label,
    }
    return summary, wide, outputs, context


def _write_record(run_folder: Path, wide: pd.DataFrame, context: dict[str, Any], meta: dict[str, Any]) -> None:
    ordered = wide.sort_values("decision_score", ascending=False)
    best = ordered.iloc[0]
    default = wide.loc[wide["candidate"].eq(_candidate_label(v23.MOMENTUM_GAP_EXIT_BUFFER, True))].iloc[0]
    lines = [
        "# Quant Parameter Scan Record",
        "",
        "## Run Metadata",
        "",
        f"- Run id: `{run_folder.name}`",
        f"- Created at: {meta['created_at']}",
        "- Project: microcap Top100",
        "- Strategy or version: v2.3",
        "- Sleeve or subsystem: cash-yield-exit-buffer",
        "- Parameter group: `cash_day_yield_exit_buffer_fine`",
        "- Scan type: fine_parameter_grid",
        f"- Repo or workspace path: `{ROOT}`",
        "- Target entrypoint: `microcap_top100_mom16_biweekly_live_v2_3.py`",
        f"- Git branch: `{meta['git_branch']}`",
        f"- Git commit: `{meta['git_commit']}`",
        "- Working tree status before: see `scan_meta.json`.",
        "",
        "## Research Question",
        "",
        "- Baseline: preserve v2.3 signal hedge 1.0 and execution hedge 0.8.",
        "- Candidate grid: cash-day yield on, exit buffer 0.08, 0.10, 0.13, 0.15, 0.18, 0.20.",
        "- Decision target: identify whether default 0.13 remains robust once cash-day yield is included.",
        "- Source-change rule: `research_only_no_source_change`.",
        "- Required windows: full, 10Y, 5Y, 3Y, 1Y.",
        "",
        "## Implementation Anchor",
        "",
        "- Official entrypoint: `microcap_top100_mom16_biweekly_live_v2_3.py`.",
        "- Function path: spread log-WLS gross -> exit-buffer overlay -> no-peak-decay cost model -> target-vol scaling -> cash-day yield.",
        "- Existing loaders reused: `v2_0.generate_v2_0_outputs()` and `_load_embedded_base_context()`.",
        "",
        "## Data Snapshot",
        "",
        f"- Metrics start: {context['common_start']}",
        f"- Metrics end: {context['common_end']}",
        f"- Rows: {context['common_rows']}",
        f"- Turnover rows: {context['turnover_rows']}",
        "- Data sources: refreshed local v2.0 embedded base context and local proxy turnover.",
        "- Trading calendar: strategy local trading-date index; annualization uses 244 trading days.",
        "",
        "## Cost and Execution Assumptions",
        "",
        "- Costed: yes.",
        "- Cash-day yield: 2% annualized on rows where `holding == cash`.",
        "- Target volatility: 25%, 60-day realized volatility, max leverage 1.5x.",
        "- Scale-change cost: 10bp on model-charged target-vol leg turnover.",
        "- Financing: 3% annualized on exposure above 1.0x.",
        "",
        "## Runtime Override Plan",
        "",
        "- Override mechanism: runtime function arguments only; no module constants mutated.",
        "- Default candidate included in same run: yes, `gap0p13_cash2pct`.",
        "- Parity check: uses same source function chain as the prior diagnostic run.",
        "",
        "## Commands",
        "",
        "```powershell",
        "python microcap_top100_mom16_biweekly_live_v2_3.py",
        f"python scripts/run_microcap_v2_3_cash_yield_exit_buffer_fine_scan.py --run-folder {run_folder}",
        "```",
        "",
        "## Output Files",
        "",
        "- `scan_summary.csv`: long-form metrics.",
        "- `window_metrics.csv`: wide comparison table.",
        "- `daily_gap*_cash2pct.csv`: daily candidate outputs.",
        "- `scan_meta.json`: machine-readable metadata.",
        "- `command_log.txt`: command log.",
        "",
        "## Full-Sample Results",
        "",
        f"- Default `gap0p13_cash2pct`: annual return {default['ann_return_full']:.4%}, max drawdown {default['max_dd_full']:.4%}.",
        f"- Best balanced-score candidate: `{best['candidate']}`; full annual return {best['ann_return_full']:.4%}, full max drawdown {best['max_dd_full']:.4%}.",
        "",
        "## Window Results",
        "",
        f"- Best candidate 5Y: annual return {best['ann_return_last_5y']:.4%}, max drawdown {best['max_dd_last_5y']:.4%}.",
        f"- Best candidate 3Y: annual return {best['ann_return_last_3y']:.4%}, max drawdown {best['max_dd_last_3y']:.4%}.",
        f"- Best candidate 1Y: annual return {best['ann_return_last_1y']:.4%}, max drawdown {best['max_dd_last_1y']:.4%}.",
        "",
        "## Stability Classification",
        "",
        "- Label: fine_scan_candidate_not_promoted.",
        "- Evidence: see `window_metrics.csv`; decision score weighs 3Y/5Y/1Y return and recent drawdown.",
        "- Cost sensitivity: cost totals and entry/exit counts included in both CSVs.",
        "",
        "## Decision",
        "",
        "- Decision: research-only fine scan complete; do not promote automatically.",
        "- Recommended next action: if `0.13` remains close to top, promote cash-day yield only and keep exit buffer unchanged.",
        "",
        "## User-Facing Summary",
        "",
        "- This scan isolates the cash-yield-on regime and checks whether v2.3's 0.13 exit buffer still makes sense.",
    ]
    (run_folder / "record.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(run_folder: Path) -> None:
    started = time.time()
    run_folder.mkdir(parents=True, exist_ok=True)
    with (run_folder / "command_log.txt").open("a", encoding="utf-8") as fh:
        fh.write(f"\n[{pd.Timestamp.now().isoformat()}] python scripts/run_microcap_v2_3_cash_yield_exit_buffer_fine_scan.py --run-folder {run_folder}\n")
    summary, wide, outputs, context = _scan(run_folder)
    latest = outputs[_candidate_label(v23.MOMENTUM_GAP_EXIT_BUFFER, True)].iloc[-1]
    created_at = pd.Timestamp.now().isoformat()
    meta = {
        "run_id": run_folder.name,
        "created_at": created_at,
        "phase": "analysis_written",
        "project": "microcap Top100",
        "strategy": "v2.3",
        "subsystem": "cash-yield-exit-buffer",
        "repo_root": str(ROOT),
        "entrypoint": "microcap_top100_mom16_biweekly_live_v2_3.py",
        "git_branch": _git(["branch", "--show-current"]),
        "git_commit": _git(["rev-parse", "HEAD"]),
        "git_status_before": _git(["status", "--short"]),
        "git_status_after": _git(["status", "--short"]),
        "scan_type": "fine_parameter_grid",
        "parameter_group": "cash_day_yield_exit_buffer_fine",
        "baseline": {
            "candidate": _candidate_label(v23.MOMENTUM_GAP_EXIT_BUFFER, True),
            "signal_spread_hedge_ratio": v23.SIGNAL_SPREAD_HEDGE_RATIO,
            "execution_hedge_ratio": v23.EXECUTION_HEDGE_RATIO,
            "exit_buffer": v23.MOMENTUM_GAP_EXIT_BUFFER,
            "cash_yield_on_cash_days": True,
            "cash_yield_annual": CASH_YIELD,
            "target_vol": v23.TARGET_VOL,
            "scale_rebalance_threshold": v23.TARGET_VOL_SCALE_REBALANCE_THRESHOLD,
        },
        "candidate_grid": wide["candidate"].tolist(),
        "data_snapshot": {
            "metrics_start": context["common_start"],
            "metrics_end": context["common_end"],
            "rows": context["common_rows"],
            "turnover_rows": context["turnover_rows"],
            "latest_holding": str(latest.get("holding")),
            "latest_next_holding": str(latest.get("next_holding")),
            "latest_target_vol_frozen_lag_days": int(float(latest.get("target_vol_frozen_lag_days", 0))),
            "reference_summary_latest_nav_date": context["reference_summary"].get("latest_nav_date"),
        },
        "cost_model": {
            "costed": True,
            "cash_day_yield": CASH_YIELD,
            "scale_change_cost": float(v23.v2_0.overlay_mod.TARGET_VOL_SCALE_CHANGE_COST),
            "financing_rate": float(v23.v2_0.overlay_mod.TARGET_VOL_FINANCING_RATE),
            "target_vol_return_source": str(latest.get("target_vol_return_source", "")),
        },
        "outputs": {
            "record": str(run_folder / "record.md"),
            "scan_summary": str(run_folder / "scan_summary.csv"),
            "window_metrics": str(run_folder / "window_metrics.csv"),
            "scan_meta": str(run_folder / "scan_meta.json"),
            "command_log": str(run_folder / "command_log.txt"),
        },
        "decision": "fine_scan_complete_no_promotion_yet",
        "stability_label": "fine_scan_candidate_not_promoted",
        "elapsed_sec": round(time.time() - started, 3),
    }
    _write_json(run_folder / "scan_meta.json", meta)
    _write_record(run_folder, wide, context, meta)
    summary.to_csv(run_folder / "scan_summary.csv", index=False, encoding="utf-8")
    wide.to_csv(run_folder / "window_metrics.csv", index=False, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-folder", type=Path, required=True)
    args = parser.parse_args()
    run(args.run_folder)


if __name__ == "__main__":
    main()
