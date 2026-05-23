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
from run_microcap_v2_3_lb_hl_r2_gate_scan import _build_candidate  # noqa: E402
from run_microcap_v2_3_strategy_risk_diagnostics import _git, _json_safe, _metrics  # noqa: E402
from run_microcap_v2_3_strict_cost_sensitivity import _apply_extra_cost  # noqa: E402


CANDIDATES = (
    ("current_hl4", 17, 4.0, None),
    ("watch_hl3", 17, 3.0, None),
)
RECENT_WINDOWS = (
    ("full", None),
    ("last_10y", pd.DateOffset(years=10)),
    ("last_5y", pd.DateOffset(years=5)),
    ("last_3y", pd.DateOffset(years=3)),
    ("last_2y", pd.DateOffset(years=2)),
    ("last_1y", pd.DateOffset(years=1)),
    ("ytd", "ytd"),
)
COST_STRESSES = (
    ("official", False, False),
    ("transition_10bp", True, False),
    ("slippage_5bp", False, True),
    ("combined_strict", True, True),
)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(_json_safe(payload), ensure_ascii=False, indent=2), encoding="utf-8")


def _window_bounds(index: pd.DatetimeIndex, spec: pd.DateOffset | str | None) -> tuple[pd.Timestamp, pd.Timestamp]:
    end = pd.Timestamp(index.max())
    first = pd.Timestamp(index.min())
    if spec is None:
        return first, end
    if spec == "ytd":
        return max(first, pd.Timestamp(year=end.year, month=1, day=1)), end
    return max(first, end - spec), end


def _transition_counts(out: pd.DataFrame) -> dict[str, int | float]:
    holding = out["holding"].astype(str)
    next_holding = out["next_holding"].astype(str)
    active = holding.ne("cash")
    spell_id = active.ne(active.shift(fill_value=False)).cumsum()
    active_spells = pd.Series(dtype=int)
    if active.any():
        active_spells = active.groupby(spell_id).sum()
        active_spells = active_spells.loc[active_spells.gt(0)]
    return {
        "entry_signals": int(holding.eq("cash").mul(next_holding.ne("cash")).sum()),
        "exit_signals": int(holding.ne("cash").mul(next_holding.eq("cash")).sum()),
        "holding_days": int(active.sum()),
        "cash_days": int(holding.eq("cash").sum()),
        "active_spell_count": int(len(active_spells)),
        "active_spell_avg_days": float(active_spells.mean()) if len(active_spells) else 0.0,
        "active_spell_median_days": float(active_spells.median()) if len(active_spells) else 0.0,
    }


def _score_stability(out: pd.DataFrame) -> dict[str, float | int]:
    score = pd.to_numeric(out.get("annualized_log_wls_score", pd.Series(index=out.index, dtype=float)), errors="coerce")
    r2 = pd.to_numeric(out.get("log_wls_r2", pd.Series(index=out.index, dtype=float)), errors="coerce")
    holding = out["holding"].astype(str)
    next_holding = out["next_holding"].astype(str)
    return {
        "score_latest": float(score.dropna().iloc[-1]) if score.dropna().size else np.nan,
        "score_mean": float(score.mean()),
        "score_std": float(score.std(ddof=1)),
        "score_positive_ratio": float(score.gt(0.0).mean()),
        "score_below_exit_buffer_ratio": float(score.lt(-float(v23.MOMENTUM_GAP_EXIT_BUFFER)).mean()),
        "r2_latest": float(r2.dropna().iloc[-1]) if r2.dropna().size else np.nan,
        "r2_mean": float(r2.mean()),
        "r2_below_0p05_ratio": float(r2.lt(0.05).mean()),
        "r2_below_0p10_ratio": float(r2.lt(0.10).mean()),
        "same_day_transition_count": int(holding.ne(next_holding).sum()),
    }


def _candidate_daily_outputs(run_folder: Path) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
    v23.generate_v2_3_outputs()
    _reference_summary, _signal_df, official_out = v23.generate_v2_3_outputs()
    _reference_summary2, base_gross_cached, turnover_df = v23.v2_0.embedded_context._load_embedded_base_context()
    close_df = v23._close_df_from_base(base_gross_cached)
    official_index = pd.DatetimeIndex(official_out.index)
    outputs: dict[str, pd.DataFrame] = {}
    for label, lookback, halflife, r2_gate in CANDIDATES:
        out = _build_candidate(close_df, turnover_df, official_index, lookback, halflife, r2_gate)
        outputs[label] = out
        out.reset_index(names="date").to_csv(run_folder / f"daily_{label}.csv", index=False, encoding="utf-8")
    context = {
        "metrics_start": str(pd.Timestamp(official_index.min()).date()),
        "metrics_end": str(pd.Timestamp(official_index.max()).date()),
        "rows": int(len(official_index)),
        "turnover_rows": int(len(turnover_df)),
    }
    return outputs, context


def _build_metrics(run_folder: Path, outputs: dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    summary_rows: list[dict[str, Any]] = []
    wide_rows: list[dict[str, Any]] = []
    cost_rows: list[dict[str, Any]] = []
    stability_rows: list[dict[str, Any]] = []
    for label, out in outputs.items():
        wide: dict[str, Any] = {"candidate": label}
        counts_full = _transition_counts(out)
        stability_rows.append({"candidate": label, "window": "full", **counts_full, **_score_stability(out)})
        for window, spec in RECENT_WINDOWS:
            start, end = _window_bounds(pd.DatetimeIndex(out.index), spec)
            part = out.loc[(out.index >= start) & (out.index <= end)]
            metrics = _metrics(part["return_net"])
            counts = _transition_counts(part)
            row = {
                "candidate": label,
                "segment": window,
                "start": str(pd.Timestamp(start).date()),
                "end": str(pd.Timestamp(end).date()),
                "rows": int(metrics["rows"]),
                "ann_return": metrics["ann_return"],
                "ann_vol": metrics["ann_vol"],
                "sharpe_repo": metrics["sharpe_repo"],
                "max_dd": metrics["max_dd"],
                "final_nav": metrics["final_nav"],
                "holding_day_ratio": counts["holding_days"] / len(part) if len(part) else np.nan,
                **counts,
            }
            summary_rows.append(row)
            for metric in ("ann_return", "max_dd", "sharpe_repo", "holding_day_ratio", "entry_signals", "exit_signals", "active_spell_avg_days"):
                wide[f"{metric}_{window}"] = row[metric]
            stability_rows.append({"candidate": label, "window": window, **counts, **_score_stability(part)})
        for stress_label, transition_cost, slippage in COST_STRESSES:
            stressed = _apply_extra_cost(out, transition_cost=transition_cost, slippage_5bp=slippage)
            stressed.reset_index(names="date").to_csv(run_folder / f"daily_{label}_{stress_label}.csv", index=False, encoding="utf-8")
            for window, spec in RECENT_WINDOWS:
                start, end = _window_bounds(pd.DatetimeIndex(stressed.index), spec)
                part = stressed.loc[(stressed.index >= start) & (stressed.index <= end)]
                metrics = _metrics(part["return_net"])
                cost_rows.append(
                    {
                        "candidate": label,
                        "stress": stress_label,
                        "segment": window,
                        "start": str(pd.Timestamp(start).date()),
                        "end": str(pd.Timestamp(end).date()),
                        "rows": int(metrics["rows"]),
                        "ann_return": metrics["ann_return"],
                        "ann_vol": metrics["ann_vol"],
                        "sharpe_repo": metrics["sharpe_repo"],
                        "max_dd": metrics["max_dd"],
                        "extra_cost_total": float(pd.to_numeric(part.get("strict_extra_cost", 0.0), errors="coerce").fillna(0.0).sum()) if len(part) else 0.0,
                    }
                )
        wide_rows.append(wide)

    summary = pd.DataFrame(summary_rows)
    wide = pd.DataFrame(wide_rows)
    cost = pd.DataFrame(cost_rows)
    stability = pd.DataFrame(stability_rows)

    # Pairwise differences: watch_hl3 minus current_hl4 for every window and cost stress.
    diff_rows: list[dict[str, Any]] = []
    for window in [name for name, _ in RECENT_WINDOWS]:
        base = summary.loc[(summary["candidate"].eq("current_hl4")) & (summary["segment"].eq(window))].iloc[0]
        watch = summary.loc[(summary["candidate"].eq("watch_hl3")) & (summary["segment"].eq(window))].iloc[0]
        diff_rows.append(
            {
                "comparison": "watch_hl3_minus_current_hl4",
                "segment": window,
                "ann_return_diff": float(watch["ann_return"] - base["ann_return"]),
                "max_dd_diff": float(watch["max_dd"] - base["max_dd"]),
                "sharpe_diff": float(watch["sharpe_repo"] - base["sharpe_repo"]),
                "holding_day_ratio_diff": float(watch["holding_day_ratio"] - base["holding_day_ratio"]),
                "entry_signals_diff": int(watch["entry_signals"] - base["entry_signals"]),
            }
        )
    for stress in [name for name, _, _ in COST_STRESSES]:
        for window in [name for name, _ in RECENT_WINDOWS]:
            base = cost.loc[(cost["candidate"].eq("current_hl4")) & (cost["stress"].eq(stress)) & (cost["segment"].eq(window))].iloc[0]
            watch = cost.loc[(cost["candidate"].eq("watch_hl3")) & (cost["stress"].eq(stress)) & (cost["segment"].eq(window))].iloc[0]
            diff_rows.append(
                {
                    "comparison": f"watch_hl3_minus_current_hl4_{stress}",
                    "segment": window,
                    "ann_return_diff": float(watch["ann_return"] - base["ann_return"]),
                    "max_dd_diff": float(watch["max_dd"] - base["max_dd"]),
                    "sharpe_diff": float(watch["sharpe_repo"] - base["sharpe_repo"]),
                    "extra_cost_total_diff": float(watch["extra_cost_total"] - base["extra_cost_total"]),
                }
            )
    diffs = pd.DataFrame(diff_rows)

    summary.to_csv(run_folder / "scan_summary.csv", index=False, encoding="utf-8")
    wide.to_csv(run_folder / "window_metrics.csv", index=False, encoding="utf-8")
    cost.to_csv(run_folder / "cost_stress_metrics.csv", index=False, encoding="utf-8")
    stability.to_csv(run_folder / "signal_stability_metrics.csv", index=False, encoding="utf-8")
    diffs.to_csv(run_folder / "candidate_differences.csv", index=False, encoding="utf-8")
    return summary, wide, cost, stability


def _write_record(run_folder: Path, summary: pd.DataFrame, cost: pd.DataFrame, stability: pd.DataFrame, meta: dict[str, Any]) -> None:
    def row(candidate: str, segment: str) -> pd.Series:
        return summary.loc[(summary["candidate"].eq(candidate)) & (summary["segment"].eq(segment))].iloc[0]

    current_3y = row("current_hl4", "last_3y")
    watch_3y = row("watch_hl3", "last_3y")
    current_1y = row("current_hl4", "last_1y")
    watch_1y = row("watch_hl3", "last_1y")
    harsh = cost.loc[(cost["stress"].eq("combined_strict")) & (cost["segment"].eq("last_3y"))]
    current_harsh = harsh.loc[harsh["candidate"].eq("current_hl4")].iloc[0]
    watch_harsh = harsh.loc[harsh["candidate"].eq("watch_hl3")].iloc[0]
    stable_full = stability.loc[stability["window"].eq("full")]
    current_stable = stable_full.loc[stable_full["candidate"].eq("current_hl4")].iloc[0]
    watch_stable = stable_full.loc[stable_full["candidate"].eq("watch_hl3")].iloc[0]
    lines = [
        "# Quant Parameter Scan Record",
        "",
        "## Run Metadata",
        "",
        f"- Run id: `{run_folder.name}`",
        f"- Created at: {meta['created_at']}",
        "- Project: microcap Top100",
        "- Strategy or version: v2.3",
        "- Sleeve or subsystem: signal-model-validation",
        "- Parameter group: `hl3_watchlist_recent_cost_signal_stability`",
        "- Scan type: focused_validation",
        f"- Repo or workspace path: `{ROOT}`",
        "- Target entrypoint: `microcap_top100_mom16_biweekly_live_v2_3.py`",
        f"- Git branch: `{meta['git_branch']}`",
        f"- Git commit: `{meta['git_commit']}`",
        "- Working tree status before: see `scan_meta.json`.",
        "",
        "## Research Question",
        "",
        "- Baseline: current `LOOKBACK=17, HALFLIFE=4, no R2 gate`.",
        "- Watchlist: `LOOKBACK=17, HALFLIFE=3, no R2 gate`.",
        "- Decision target: validate recent-window performance, stricter cost sensitivity, and signal stability before any production change.",
        "- Source-change rule: `research_only_no_source_change`.",
        "- Required windows: full, 10Y, 5Y, 3Y, 2Y, 1Y, YTD.",
        "",
        "## Implementation Anchor",
        "",
        "- Reused parameterized candidate builder from `run_microcap_v2_3_lb_hl_r2_gate_scan.py`.",
        "- Cost stress reuses the strict-cost overlay from `run_microcap_v2_3_strict_cost_sensitivity.py`.",
        "- Both candidates use current v2.3 target-vol, cash-day yield, and execution hedge settings.",
        "",
        "## Data Snapshot",
        "",
        f"- Metrics start: {meta['data_snapshot']['metrics_start']}",
        f"- Metrics end: {meta['data_snapshot']['metrics_end']}",
        f"- Rows: {meta['data_snapshot']['rows']}",
        f"- Turnover rows: {meta['data_snapshot']['turnover_rows']}",
        "- Trading calendar: strategy local trading-date index; annualization uses 244 trading days.",
        "",
        "## Cost and Execution Assumptions",
        "",
        "- Official costed path: embedded base cost, target-vol costs where charged, financing, cash-day yield.",
        "- Cost stress: transition 10bp, turnover slippage 5bp, and combined strict scenario.",
        "",
        "## Runtime Override Plan",
        "",
        "- Runtime parameterized rebuild only; no production constants changed.",
        "- Default candidate included: `current_hl4`.",
        "- Watchlist candidate included: `watch_hl3`.",
        "",
        "## Commands",
        "",
        "```powershell",
        "python microcap_top100_mom16_biweekly_live_v2_3.py",
        f"python scripts/run_microcap_v2_3_hl3_watchlist_validation.py --run-folder {run_folder}",
        "```",
        "",
        "## Output Files",
        "",
        "- `scan_summary.csv`: official-cost metrics by recent window.",
        "- `window_metrics.csv`: wide official-cost metrics.",
        "- `cost_stress_metrics.csv`: official and stressed cost metrics.",
        "- `signal_stability_metrics.csv`: score, R2, entries/exits, spell metrics.",
        "- `candidate_differences.csv`: watchlist minus current differences.",
        "",
        "## Full-Sample Results",
        "",
        f"- Current hl4 full: annual return {row('current_hl4', 'full')['ann_return']:.4%}, max drawdown {row('current_hl4', 'full')['max_dd']:.4%}.",
        f"- Watch hl3 full: annual return {row('watch_hl3', 'full')['ann_return']:.4%}, max drawdown {row('watch_hl3', 'full')['max_dd']:.4%}.",
        "",
        "## Window Results",
        "",
        f"- 3Y current hl4: annual return {current_3y['ann_return']:.4%}, max drawdown {current_3y['max_dd']:.4%}.",
        f"- 3Y watch hl3: annual return {watch_3y['ann_return']:.4%}, max drawdown {watch_3y['max_dd']:.4%}.",
        f"- 1Y current hl4: annual return {current_1y['ann_return']:.4%}, max drawdown {current_1y['max_dd']:.4%}.",
        f"- 1Y watch hl3: annual return {watch_1y['ann_return']:.4%}, max drawdown {watch_1y['max_dd']:.4%}.",
        "",
        "## Stability Classification",
        "",
        f"- Current hl4 full entry signals: {int(current_stable['entry_signals'])}; average active spell {current_stable['active_spell_avg_days']:.2f} days.",
        f"- Watch hl3 full entry signals: {int(watch_stable['entry_signals'])}; average active spell {watch_stable['active_spell_avg_days']:.2f} days.",
        f"- Combined strict 3Y current hl4: annual return {current_harsh['ann_return']:.4%}, max drawdown {current_harsh['max_dd']:.4%}.",
        f"- Combined strict 3Y watch hl3: annual return {watch_harsh['ann_return']:.4%}, max drawdown {watch_harsh['max_dd']:.4%}.",
        "",
        "## Decision",
        "",
        "- Decision: focused validation complete; do not promote automatically.",
        "- Recommended next action: promote hl3 only if the user accepts slightly higher turnover and full-sample drawdown for better recent drawdown/1Y behavior.",
        "",
        "## User-Facing Summary",
        "",
        "- This run checks whether the hl3 watchlist candidate remains compelling after recent-window, stricter-cost, and signal-stability checks.",
    ]
    (run_folder / "record.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(run_folder: Path) -> None:
    started = time.time()
    run_folder.mkdir(parents=True, exist_ok=True)
    with (run_folder / "command_log.txt").open("a", encoding="utf-8") as fh:
        fh.write(f"\n[{pd.Timestamp.now().isoformat()}] python scripts/run_microcap_v2_3_hl3_watchlist_validation.py --run-folder {run_folder}\n")
    outputs, context = _candidate_daily_outputs(run_folder)
    summary, wide, cost, stability = _build_metrics(run_folder, outputs)
    created_at = pd.Timestamp.now().isoformat()
    meta = {
        "run_id": run_folder.name,
        "created_at": created_at,
        "phase": "analysis_written",
        "project": "microcap Top100",
        "strategy": "v2.3",
        "subsystem": "signal-model-validation",
        "repo_root": str(ROOT),
        "entrypoint": "microcap_top100_mom16_biweekly_live_v2_3.py",
        "git_branch": _git(["branch", "--show-current"]),
        "git_commit": _git(["rev-parse", "HEAD"]),
        "git_status_before": _git(["status", "--short"]),
        "git_status_after": _git(["status", "--short"]),
        "scan_type": "focused_validation",
        "parameter_group": "hl3_watchlist_recent_cost_signal_stability",
        "baseline": {
            "candidate": "current_hl4",
            "lookback": 17,
            "halflife": 4.0,
            "r2_entry_gate": None,
        },
        "candidate_grid": ["current_hl4", "watch_hl3"],
        "data_snapshot": context,
        "cost_model": {
            "official_costed": True,
            "cost_stress_scenarios": [name for name, _, _ in COST_STRESSES],
            "uses_current_v23_cash_day_yield": True,
        },
        "outputs": {
            "record": str(run_folder / "record.md"),
            "scan_summary": str(run_folder / "scan_summary.csv"),
            "window_metrics": str(run_folder / "window_metrics.csv"),
            "scan_meta": str(run_folder / "scan_meta.json"),
            "command_log": str(run_folder / "command_log.txt"),
            "cost_stress_metrics": str(run_folder / "cost_stress_metrics.csv"),
            "signal_stability_metrics": str(run_folder / "signal_stability_metrics.csv"),
            "candidate_differences": str(run_folder / "candidate_differences.csv"),
        },
        "decision": "focused_validation_complete_no_promotion_yet",
        "stability_label": "hl3_watchlist_tradeoff",
        "elapsed_sec": round(time.time() - started, 3),
    }
    _write_json(run_folder / "scan_meta.json", meta)
    _write_record(run_folder, summary, cost, stability, meta)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-folder", type=Path, required=True)
    args = parser.parse_args()
    run(args.run_folder)


if __name__ == "__main__":
    main()
