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
from run_microcap_v2_3_strategy_risk_diagnostics import _git, _json_safe, _metrics, _window_slices  # noqa: E402


LOOKBACKS = (12, 17, 26, 40)
HALFLIFES = (3.0, 4.0, 6.0, 10.0)
R2_GATES: tuple[float | None, ...] = (None, 0.05, 0.10)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(_json_safe(payload), ensure_ascii=False, indent=2), encoding="utf-8")


def _candidate_label(lookback: int, halflife: float, r2_gate: float | None) -> str:
    gate = "none" if r2_gate is None else f"{r2_gate:.2f}".replace(".", "p")
    return f"lb{int(lookback)}_hl{halflife:g}".replace(".", "p") + f"_r2{gate}"


def _build_parameterized_gross(
    close_df: pd.DataFrame,
    index: pd.DatetimeIndex,
    lookback: int,
    halflife: float,
    r2_gate: float | None,
) -> pd.DataFrame:
    close_df = close_df.sort_index()
    spread_nav, micro_ret, hedge_ret, _signal_daily_drag = v23.always_on_spread_nav(close_df)
    log_wls = v23.log_wls_score_and_r2(spread_nav, lookback=lookback, halflife=halflife)
    common_index = pd.DatetimeIndex(index).intersection(pd.DatetimeIndex(log_wls.index)).sort_values()
    score = pd.to_numeric(log_wls["annualized_log_wls_score"].loc[common_index], errors="coerce")
    r2 = pd.to_numeric(log_wls["log_wls_r2"].loc[common_index], errors="coerce")

    holding = False
    holdings: list[str] = []
    next_holdings: list[str] = []
    signal_on_values: list[bool] = []
    returns: list[float] = []
    futures_drag_values: list[float] = []
    active_spread_values: list[float] = []
    execution_daily_drag = float(v23.v2_0.base_mod.FUTURES_DRAG) * v23.EXECUTION_HEDGE_RATIO
    micro = micro_ret.loc[common_index]
    hedge = hedge_ret.loc[common_index]
    for dt in common_index:
        current_active = bool(holding)
        active_spread_ret = float(micro.loc[dt] - v23.EXECUTION_HEDGE_RATIO * hedge.loc[dt]) if pd.notna(micro.loc[dt]) and pd.notna(hedge.loc[dt]) else 0.0
        futures_drag = execution_daily_drag if current_active else 0.0
        returns.append(active_spread_ret - futures_drag if current_active else 0.0)
        futures_drag_values.append(futures_drag)
        active_spread_values.append(active_spread_ret if current_active else 0.0)
        current_score = score.loc[dt]
        current_r2 = r2.loc[dt]
        score_valid = pd.notna(current_score)
        r2_valid = r2_gate is None or (pd.notna(current_r2) and float(current_r2) >= float(r2_gate))
        if not score_valid:
            next_active = False
        elif current_active:
            next_active = float(current_score) >= -float(v23.MOMENTUM_GAP_EXIT_BUFFER)
        else:
            next_active = float(current_score) > 0.0 and bool(r2_valid)
        holdings.append("long_microcap_short_zz1000" if current_active else "cash")
        next_holdings.append("long_microcap_short_zz1000" if next_active else "cash")
        signal_on_values.append(bool(next_active))
        holding = bool(next_active)

    gross = pd.DataFrame(
        {
            "return_raw": returns,
            "return": returns,
            "holding": holdings,
            "next_holding": next_holdings,
            "signal_on": signal_on_values,
            "microcap_close": close_df["microcap"].loc[common_index],
            "hedge_close": close_df["hedge"].loc[common_index],
            "microcap_ret": micro,
            "hedge_ret": hedge,
            "microcap_mom": score,
            "hedge_mom": 0.0,
            "momentum_gap": score,
            "annualized_log_wls_score": score,
            "log_wls_r2": r2,
            "spread_nav": spread_nav.loc[common_index],
            "halflife": float(halflife),
            "lookback": int(lookback),
            "r2_entry_gate": np.nan if r2_gate is None else float(r2_gate),
            "signal_score_label": "annualized_log_wls_score",
            "momentum_gap_legacy_note": "legacy field contains annualized spread-NAV log-WLS score, not plain microcap-minus-hedge momentum gap",
            "futures_drag": futures_drag_values,
            "active_spread_ret": active_spread_values,
            "weight": 1.0,
            "target_vol_execution_scale": 1.0,
        },
        index=common_index,
    )
    return gross


def _build_candidate(
    close_df: pd.DataFrame,
    turnover_df: pd.DataFrame,
    official_index: pd.DatetimeIndex,
    lookback: int,
    halflife: float,
    r2_gate: float | None,
) -> pd.DataFrame:
    base_index = v23.build_v2_3_common_index(close_df, official_index)
    gross = _build_parameterized_gross(close_df, base_index, lookback, halflife, r2_gate)
    costed = v23.v2_0.base_mod.apply_momentum_gap_no_peak_decay_cost_model(gross, turnover_df)
    out = v23.apply_target_vol(costed, v23.TARGET_VOL)
    out["scan_lookback"] = int(lookback)
    out["scan_halflife"] = float(halflife)
    out["scan_r2_entry_gate"] = np.nan if r2_gate is None else float(r2_gate)
    return out


def _transition_counts(out: pd.DataFrame) -> dict[str, int]:
    holding = out["holding"].astype(str)
    next_holding = out["next_holding"].astype(str)
    return {
        "entry_signals": int(holding.eq("cash").mul(next_holding.ne("cash")).sum()),
        "exit_signals": int(holding.ne("cash").mul(next_holding.eq("cash")).sum()),
        "holding_days": int(holding.ne("cash").sum()),
        "cash_days": int(holding.eq("cash").sum()),
    }


def _decision_score(row: pd.Series) -> float:
    # Recent-window weighted score. Drawdowns are negative, so adding them penalizes larger drawdowns.
    return (
        float(row["ann_return_last_3y"])
        + 0.50 * float(row["ann_return_last_5y"])
        + 0.25 * float(row["ann_return_last_1y"])
        + 0.75 * float(row["max_dd_last_3y"])
        + 0.50 * float(row["max_dd_last_5y"])
    )


def _scan(run_folder: Path) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    reference_summary, _signal_df, official_out = v23.generate_v2_3_outputs()
    _reference_summary2, base_gross_cached, turnover_df = v23.v2_0.embedded_context._load_embedded_base_context()
    close_df = v23._close_df_from_base(base_gross_cached)
    official_index = pd.DatetimeIndex(official_out.index)
    summary_rows: list[dict[str, Any]] = []
    wide_rows: list[dict[str, Any]] = []

    for lookback in LOOKBACKS:
        for halflife in HALFLIFES:
            for r2_gate in R2_GATES:
                label = _candidate_label(lookback, halflife, r2_gate)
                out = _build_candidate(close_df, turnover_df, official_index, lookback, halflife, r2_gate)
                out.reset_index(names="date").to_csv(run_folder / f"daily_{label}.csv", index=False, encoding="utf-8")
                counts = _transition_counts(out)
                wide: dict[str, Any] = {
                    "candidate": label,
                    "lookback": int(lookback),
                    "halflife": float(halflife),
                    "r2_entry_gate": np.nan if r2_gate is None else float(r2_gate),
                    "holding_days_full": counts["holding_days"],
                    "cash_days_full": counts["cash_days"],
                    "entry_signals_full": counts["entry_signals"],
                    "exit_signals_full": counts["exit_signals"],
                }
                for segment, (start, end) in _window_slices(pd.DatetimeIndex(out.index)).items():
                    part = out.loc[(out.index >= start) & (out.index <= end)]
                    m = _metrics(part["return_net"])
                    part_counts = _transition_counts(part)
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
                        "holding_days": part_counts["holding_days"],
                        "cash_days": part_counts["cash_days"],
                        "holding_day_ratio": part_counts["holding_days"] / len(part) if len(part) else np.nan,
                        "entry_signals": part_counts["entry_signals"],
                        "exit_signals": part_counts["exit_signals"],
                        "cost_total": cost_total,
                        "scale_change_cost_total": scale_cost_total,
                        "lookback": int(lookback),
                        "halflife": float(halflife),
                        "r2_entry_gate": np.nan if r2_gate is None else float(r2_gate),
                        "cash_day_yield": v23.CASH_DAY_YIELD,
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
                        "scale_change_cost_total",
                    ):
                        wide[f"{metric}_{segment}"] = row[metric]
                wide["decision_score"] = np.nan
                wide["decision_hint"] = "compare_only"
                wide["stability_label"] = "candidate"
                wide_rows.append(wide)

    summary = pd.DataFrame(summary_rows)
    wide = pd.DataFrame(wide_rows)
    wide["decision_score"] = wide.apply(_decision_score, axis=1)
    baseline_label = _candidate_label(v23.LOOKBACK, v23.HALFLIFE, None)
    wide.loc[wide["candidate"].eq(baseline_label), "decision_hint"] = "current_v2_3_baseline"
    best_label = str(wide.sort_values("decision_score", ascending=False).iloc[0]["candidate"])
    wide.loc[wide["candidate"].eq(best_label), "decision_hint"] = "best_recent_balanced_score"
    summary.to_csv(run_folder / "scan_summary.csv", index=False, encoding="utf-8")
    wide.to_csv(run_folder / "window_metrics.csv", index=False, encoding="utf-8")
    context = {
        "reference_summary": reference_summary,
        "turnover_rows": int(len(turnover_df)),
        "common_start": str(pd.Timestamp(official_index.min()).date()),
        "common_end": str(pd.Timestamp(official_index.max()).date()),
        "common_rows": int(len(official_index)),
        "baseline_label": baseline_label,
        "best_label": best_label,
    }
    return summary, wide, context


def _write_record(run_folder: Path, wide: pd.DataFrame, context: dict[str, Any], meta: dict[str, Any]) -> None:
    ordered = wide.sort_values("decision_score", ascending=False)
    best = ordered.iloc[0]
    baseline = wide.loc[wide["candidate"].eq(context["baseline_label"])].iloc[0]
    top10 = ordered.head(10)[
        [
            "candidate",
            "decision_score",
            "ann_return_full",
            "max_dd_full",
            "ann_return_last_5y",
            "max_dd_last_5y",
            "ann_return_last_3y",
            "max_dd_last_3y",
            "ann_return_last_1y",
            "max_dd_last_1y",
            "holding_day_ratio_full",
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
        "- Strategy or version: v2.3",
        "- Sleeve or subsystem: signal-model",
        "- Parameter group: `lookback_halflife_r2_entry_gate`",
        "- Scan type: signal_parameter_grid",
        f"- Repo or workspace path: `{ROOT}`",
        "- Target entrypoint: `microcap_top100_mom16_biweekly_live_v2_3.py`",
        f"- Git branch: `{meta['git_branch']}`",
        f"- Git commit: `{meta['git_commit']}`",
        "- Working tree status before: see `scan_meta.json`.",
        "",
        "## Research Question",
        "",
        "- Baseline: current v2.3, cash-day yield on, signal hedge 1.0, execution hedge 0.8, exit buffer 0.13.",
        "- Candidate grid: LOOKBACK 12/17/26/40, HALFLIFE 3/4/6/10, R2 entry gate none/0.05/0.10.",
        "- R2 gate semantics: entry-only filter; held positions still exit by the existing score buffer.",
        "- Decision target: find whether v2.3's current 17/4/no-gate signal remains robust.",
        "- Source-change rule: `research_only_no_source_change`.",
        "- Required windows: full, 10Y, 5Y, 3Y, 1Y.",
        "",
        "## Implementation Anchor",
        "",
        "- Data loaded through `v2_0.generate_v2_0_outputs()` and `_load_embedded_base_context()`.",
        "- Parameterized score uses `v23.log_wls_score_and_r2(spread_nav, lookback, halflife)`.",
        "- Candidate path: parameterized gross signal -> v2 no-peak-decay cost model -> v2.3 target-vol/cash-day-yield overlay.",
        "",
        "## Data Snapshot",
        "",
        f"- Metrics start: {context['common_start']}",
        f"- Metrics end: {context['common_end']}",
        f"- Rows: {context['common_rows']}",
        f"- Turnover rows: {context['turnover_rows']}",
        "- Trading calendar: strategy local trading-date index; annualization uses 244 trading days.",
        "",
        "## Cost and Execution Assumptions",
        "",
        "- Costed: yes.",
        "- Cash-day yield: current v2.3 production setting.",
        "- Target volatility: current v2.3 production setting.",
        "- Signal/execution hedge mismatch: retained at signal 1.0 and execution 0.8.",
        "",
        "## Runtime Override Plan",
        "",
        "- Override mechanism: parameterized rebuild in a research script; production constants are not mutated.",
        "- Default candidate included in same run: yes, `lb17_hl4_r2none`.",
        "- No production source default change in this scan.",
        "",
        "## Commands",
        "",
        "```powershell",
        "python microcap_top100_mom16_biweekly_live_v2_3.py",
        f"python scripts/run_microcap_v2_3_lb_hl_r2_gate_scan.py --run-folder {run_folder}",
        "```",
        "",
        "## Output Files",
        "",
        "- `scan_summary.csv`: long-form window metrics for all candidates.",
        "- `window_metrics.csv`: wide comparison table.",
        "- `daily_*.csv`: daily candidate outputs.",
        "",
        "## Full-Sample Results",
        "",
        f"- Baseline `{baseline['candidate']}`: annual return {baseline['ann_return_full']:.4%}, max drawdown {baseline['max_dd_full']:.4%}.",
        f"- Best balanced-score `{best['candidate']}`: annual return {best['ann_return_full']:.4%}, max drawdown {best['max_dd_full']:.4%}.",
        "",
        "## Window Results",
        "",
        f"- Best candidate 5Y: annual return {best['ann_return_last_5y']:.4%}, max drawdown {best['max_dd_last_5y']:.4%}.",
        f"- Best candidate 3Y: annual return {best['ann_return_last_3y']:.4%}, max drawdown {best['max_dd_last_3y']:.4%}.",
        f"- Best candidate 1Y: annual return {best['ann_return_last_1y']:.4%}, max drawdown {best['max_dd_last_1y']:.4%}.",
        "",
        "## Stability Classification",
        "",
        "- Label: signal_grid_candidate_not_promoted.",
        "- Evidence: decision score weighs 3Y/5Y/1Y return and recent drawdown; see `window_metrics.csv`.",
        "- Top 10 by decision score:",
        "",
        top10.to_markdown(index=False),
        "",
        "## Decision",
        "",
        "- Decision: research-only signal grid complete; do not promote automatically.",
        "- Recommended next action: inspect top candidates for turnover and window stability before changing v2.3 defaults.",
        "",
        "## User-Facing Summary",
        "",
        "- This scan tests whether R2 entry filtering and longer/shorter WLS memory improve v2.3 under the current production cost and cash-yield assumptions.",
    ]
    (run_folder / "record.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(run_folder: Path) -> None:
    started = time.time()
    run_folder.mkdir(parents=True, exist_ok=True)
    with (run_folder / "command_log.txt").open("a", encoding="utf-8") as fh:
        fh.write(f"\n[{pd.Timestamp.now().isoformat()}] python scripts/run_microcap_v2_3_lb_hl_r2_gate_scan.py --run-folder {run_folder}\n")
    _summary, wide, context = _scan(run_folder)
    created_at = pd.Timestamp.now().isoformat()
    meta = {
        "run_id": run_folder.name,
        "created_at": created_at,
        "phase": "analysis_written",
        "project": "microcap Top100",
        "strategy": "v2.3",
        "subsystem": "signal-model",
        "repo_root": str(ROOT),
        "entrypoint": "microcap_top100_mom16_biweekly_live_v2_3.py",
        "git_branch": _git(["branch", "--show-current"]),
        "git_commit": _git(["rev-parse", "HEAD"]),
        "git_status_before": _git(["status", "--short"]),
        "git_status_after": _git(["status", "--short"]),
        "scan_type": "signal_parameter_grid",
        "parameter_group": "lookback_halflife_r2_entry_gate",
        "baseline": {
            "candidate": context["baseline_label"],
            "lookback": v23.LOOKBACK,
            "halflife": v23.HALFLIFE,
            "r2_entry_gate": None,
            "cash_day_yield": v23.CASH_DAY_YIELD,
            "exit_buffer": v23.MOMENTUM_GAP_EXIT_BUFFER,
            "signal_spread_hedge_ratio": v23.SIGNAL_SPREAD_HEDGE_RATIO,
            "execution_hedge_ratio": v23.EXECUTION_HEDGE_RATIO,
        },
        "candidate_grid": wide["candidate"].tolist(),
        "data_snapshot": {
            "metrics_start": context["common_start"],
            "metrics_end": context["common_end"],
            "rows": context["common_rows"],
            "turnover_rows": context["turnover_rows"],
            "reference_summary_latest_nav_date": context["reference_summary"].get("latest_nav_date"),
        },
        "cost_model": {
            "costed": True,
            "uses_current_v23_target_vol_and_cash_day_yield": True,
            "r2_gate_semantics": "entry_only",
        },
        "outputs": {
            "record": str(run_folder / "record.md"),
            "scan_summary": str(run_folder / "scan_summary.csv"),
            "window_metrics": str(run_folder / "window_metrics.csv"),
            "scan_meta": str(run_folder / "scan_meta.json"),
            "command_log": str(run_folder / "command_log.txt"),
        },
        "decision": "signal_grid_complete_no_promotion_yet",
        "stability_label": "signal_grid_candidate_not_promoted",
        "elapsed_sec": round(time.time() - started, 3),
    }
    _write_json(run_folder / "scan_meta.json", meta)
    _write_record(run_folder, wide, context, meta)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-folder", type=Path, required=True)
    args = parser.parse_args()
    run(args.run_folder)


if __name__ == "__main__":
    main()
