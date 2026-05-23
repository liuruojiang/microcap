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


SCALE_CHANGE_COST = float(v23.v2_0.overlay_mod.TARGET_VOL_SCALE_CHANGE_COST)
EXTRA_SLIPPAGE_RATE = 0.0005


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(_json_safe(payload), ensure_ascii=False, indent=2), encoding="utf-8")


def _apply_extra_cost(nav: pd.DataFrame, transition_cost: bool, slippage_5bp: bool) -> pd.DataFrame:
    out = nav.copy()
    holding = out["holding"].astype(str)
    transition = holding.ne(holding.shift(1).fillna("cash"))
    turnover = pd.to_numeric(out.get("target_vol_turnover", 0.0), errors="coerce").fillna(0.0)
    extra = pd.Series(0.0, index=out.index, dtype=float)
    if transition_cost:
        extra = extra.add(turnover.where(transition, 0.0) * SCALE_CHANGE_COST, fill_value=0.0)
    if slippage_5bp:
        extra = extra.add(turnover * EXTRA_SLIPPAGE_RATE, fill_value=0.0)
    ret = pd.to_numeric(out["return_net"], errors="coerce").fillna(0.0)
    out["strict_extra_cost"] = extra
    out["strict_transition_cost_enabled"] = bool(transition_cost)
    out["strict_slippage_5bp_enabled"] = bool(slippage_5bp)
    out["return_net"] = (1.0 + ret) * (1.0 - extra.clip(lower=0.0, upper=0.99)) - 1.0
    out["nav_net"] = (1.0 + out["return_net"].fillna(0.0)).cumprod()
    return out


def _candidate_outputs(nav: pd.DataFrame) -> dict[str, pd.DataFrame]:
    return {
        "baseline_official": _apply_extra_cost(nav, transition_cost=False, slippage_5bp=False),
        "transition_tv_cost_10bp": _apply_extra_cost(nav, transition_cost=True, slippage_5bp=False),
        "slippage_5bp_turnover": _apply_extra_cost(nav, transition_cost=False, slippage_5bp=True),
        "transition_10bp_plus_slippage_5bp": _apply_extra_cost(nav, transition_cost=True, slippage_5bp=True),
    }


def _scan(run_folder: Path) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, pd.DataFrame]]:
    v23.generate_v2_3_outputs()
    nav = pd.read_csv(v23.NAV_CSV, parse_dates=["date"]).sort_values("date").set_index("date")
    outputs = _candidate_outputs(nav)
    summary_rows: list[dict[str, Any]] = []
    wide_rows: list[dict[str, Any]] = []
    for label, out in outputs.items():
        out.reset_index(names="date").to_csv(run_folder / f"daily_{label}.csv", index=False, encoding="utf-8")
        wide: dict[str, Any] = {
            "candidate": label,
            "transition_tv_cost_10bp": bool(out["strict_transition_cost_enabled"].iloc[-1]),
            "slippage_5bp_turnover": bool(out["strict_slippage_5bp_enabled"].iloc[-1]),
            "extra_cost_total_full": float(out["strict_extra_cost"].sum()),
            "extra_cost_days_full": int(out["strict_extra_cost"].gt(0).sum()),
        }
        for segment, (start, end) in _window_slices(pd.DatetimeIndex(out.index)).items():
            part = out.loc[(out.index >= start) & (out.index <= end)]
            m = _metrics(part["return_net"])
            extra_cost_total = float(pd.to_numeric(part["strict_extra_cost"], errors="coerce").fillna(0.0).sum()) if len(part) else 0.0
            holding_days = int(part["holding"].astype(str).ne("cash").sum()) if len(part) else 0
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
                "extra_cost_total": extra_cost_total,
                "extra_cost_days": int(part["strict_extra_cost"].gt(0).sum()) if len(part) else 0,
                "transition_tv_cost_10bp": bool(out["strict_transition_cost_enabled"].iloc[-1]),
                "slippage_5bp_turnover": bool(out["strict_slippage_5bp_enabled"].iloc[-1]),
            }
            summary_rows.append(row)
            for metric in ("ann_return", "max_dd", "sharpe_repo", "holding_day_ratio", "extra_cost_total"):
                wide[f"{metric}_{segment}"] = row[metric]
        wide["decision_hint"] = "stress_test_only" if label != "baseline_official" else "official_current"
        wide["stability_label"] = "cost_sensitivity"
        wide_rows.append(wide)
    summary = pd.DataFrame(summary_rows)
    wide = pd.DataFrame(wide_rows)
    summary.to_csv(run_folder / "scan_summary.csv", index=False, encoding="utf-8")
    wide.to_csv(run_folder / "window_metrics.csv", index=False, encoding="utf-8")
    return summary, wide, outputs


def _write_record(run_folder: Path, wide: pd.DataFrame, meta: dict[str, Any]) -> None:
    baseline = wide.loc[wide["candidate"].eq("baseline_official")].iloc[0]
    harsh = wide.loc[wide["candidate"].eq("transition_10bp_plus_slippage_5bp")].iloc[0]
    lines = [
        "# Quant Parameter Scan Record",
        "",
        "## Run Metadata",
        "",
        f"- Run id: `{run_folder.name}`",
        f"- Created at: {meta['created_at']}",
        "- Project: microcap Top100",
        "- Strategy or version: v2.3",
        "- Sleeve or subsystem: cost-sensitivity",
        "- Parameter group: `strict_transition_and_slippage_cost`",
        "- Scan type: cost_sensitivity",
        f"- Repo or workspace path: `{ROOT}`",
        "- Target entrypoint: `microcap_top100_mom16_biweekly_live_v2_3.py`",
        f"- Git branch: `{meta['git_branch']}`",
        f"- Git commit: `{meta['git_commit']}`",
        "- Working tree status before: see `scan_meta.json`.",
        "",
        "## Research Question",
        "",
        "- Baseline: current v2.3 including cash-day yield.",
        "- Candidate grid: official cost, transition target-vol turnover cost, 5bp turnover slippage, and both combined.",
        "- Decision target: check whether v2.3 remains attractive under harsher transaction-cost assumptions.",
        "- Source-change rule: `research_only_no_source_change`.",
        "- Required windows: full, 10Y, 5Y, 3Y, 1Y.",
        "",
        "## Implementation Anchor",
        "",
        "- Official NAV rebuilt through `microcap_top100_mom16_biweekly_live_v2_3.py`.",
        "- Stress costs are applied multiplicatively to current `return_net`; strategy signals and sizing are not reoptimized.",
        "",
        "## Data Snapshot",
        "",
        f"- Metrics start: {meta['data_snapshot']['metrics_start']}",
        f"- Metrics end: {meta['data_snapshot']['metrics_end']}",
        f"- Rows: {meta['data_snapshot']['rows']}",
        "- Trading calendar: strategy local trading-date index; annualization uses 244 trading days.",
        "",
        "## Cost and Execution Assumptions",
        "",
        "- Baseline already includes embedded trading cost, target-vol scale-change cost where charged, financing, and cash-day yield.",
        "- Transition target-vol stress: charge 10bp times target-vol leg turnover on holding transition rows.",
        "- Slippage stress: charge 5bp times target-vol leg turnover on all rows.",
        "",
        "## Runtime Override Plan",
        "",
        "- Override mechanism: post-run cost stress on official v2.3 NAV only.",
        "- Default candidate included in same run: yes, `baseline_official`.",
        "- No production constants changed.",
        "",
        "## Commands",
        "",
        "```powershell",
        "python microcap_top100_mom16_biweekly_live_v2_3.py",
        f"python scripts/run_microcap_v2_3_strict_cost_sensitivity.py --run-folder {run_folder}",
        "```",
        "",
        "## Output Files",
        "",
        "- `scan_summary.csv`: long-form metrics.",
        "- `window_metrics.csv`: wide comparison table.",
        "- `daily_*.csv`: stressed daily paths.",
        "",
        "## Full-Sample Results",
        "",
        f"- Baseline: annual return {baseline['ann_return_full']:.4%}, max drawdown {baseline['max_dd_full']:.4%}.",
        f"- Harsh combined cost: annual return {harsh['ann_return_full']:.4%}, max drawdown {harsh['max_dd_full']:.4%}, extra cost sum {harsh['extra_cost_total_full']:.4f}.",
        "",
        "## Window Results",
        "",
        f"- Harsh combined 5Y: annual return {harsh['ann_return_last_5y']:.4%}, max drawdown {harsh['max_dd_last_5y']:.4%}.",
        f"- Harsh combined 3Y: annual return {harsh['ann_return_last_3y']:.4%}, max drawdown {harsh['max_dd_last_3y']:.4%}.",
        f"- Harsh combined 1Y: annual return {harsh['ann_return_last_1y']:.4%}, max drawdown {harsh['max_dd_last_1y']:.4%}.",
        "",
        "## Stability Classification",
        "",
        "- Label: cost_sensitivity_pass_watch.",
        "- Evidence: compare `baseline_official` with `transition_10bp_plus_slippage_5bp` in `window_metrics.csv`.",
        "- Caveat: stress is a post-run cost overlay; it does not feed back into future target-vol sizing.",
        "",
        "## Decision",
        "",
        "- Decision: research-only cost sensitivity complete; no default cost-model change.",
        "- Recommended next action: keep current production cost model but monitor live turnover/slippage evidence.",
        "",
        "## User-Facing Summary",
        "",
        "- This run tests whether the accepted v2.3 cash-yield version is fragile to stricter execution costs.",
    ]
    (run_folder / "record.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(run_folder: Path) -> None:
    started = time.time()
    run_folder.mkdir(parents=True, exist_ok=True)
    with (run_folder / "command_log.txt").open("a", encoding="utf-8") as fh:
        fh.write(f"\n[{pd.Timestamp.now().isoformat()}] python scripts/run_microcap_v2_3_strict_cost_sensitivity.py --run-folder {run_folder}\n")
    _summary, wide, outputs = _scan(run_folder)
    baseline = outputs["baseline_official"]
    created_at = pd.Timestamp.now().isoformat()
    meta = {
        "run_id": run_folder.name,
        "created_at": created_at,
        "phase": "analysis_written",
        "project": "microcap Top100",
        "strategy": "v2.3",
        "subsystem": "cost-sensitivity",
        "repo_root": str(ROOT),
        "entrypoint": "microcap_top100_mom16_biweekly_live_v2_3.py",
        "git_branch": _git(["branch", "--show-current"]),
        "git_commit": _git(["rev-parse", "HEAD"]),
        "git_status_before": _git(["status", "--short"]),
        "git_status_after": _git(["status", "--short"]),
        "scan_type": "cost_sensitivity",
        "parameter_group": "strict_transition_and_slippage_cost",
        "baseline": {
            "candidate": "baseline_official",
            "cash_day_yield": v23.CASH_DAY_YIELD,
            "exit_buffer": v23.MOMENTUM_GAP_EXIT_BUFFER,
            "signal_spread_hedge_ratio": v23.SIGNAL_SPREAD_HEDGE_RATIO,
            "execution_hedge_ratio": v23.EXECUTION_HEDGE_RATIO,
        },
        "candidate_grid": wide["candidate"].tolist(),
        "data_snapshot": {
            "metrics_start": str(pd.Timestamp(baseline.index.min()).date()),
            "metrics_end": str(pd.Timestamp(baseline.index.max()).date()),
            "rows": int(len(baseline)),
        },
        "cost_model": {
            "baseline_costed": True,
            "transition_tv_cost_rate": SCALE_CHANGE_COST,
            "extra_slippage_rate": EXTRA_SLIPPAGE_RATE,
            "post_run_overlay": True,
        },
        "outputs": {
            "record": str(run_folder / "record.md"),
            "scan_summary": str(run_folder / "scan_summary.csv"),
            "window_metrics": str(run_folder / "window_metrics.csv"),
            "scan_meta": str(run_folder / "scan_meta.json"),
            "command_log": str(run_folder / "command_log.txt"),
        },
        "decision": "cost_sensitivity_complete_no_default_cost_model_change",
        "stability_label": "cost_sensitivity_pass_watch",
        "elapsed_sec": round(time.time() - started, 3),
    }
    _write_json(run_folder / "scan_meta.json", meta)
    _write_record(run_folder, wide, meta)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-folder", type=Path, required=True)
    args = parser.parse_args()
    run(args.run_folder)


if __name__ == "__main__":
    main()
