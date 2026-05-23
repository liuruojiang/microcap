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


LOOKBACKS = tuple(range(14, 21))
HALFLIFES = (2.0, 2.5, 3.0, 3.5, 4.0)
CENTER_LOOKBACK = 17
CENTER_HALFLIFE = 3.0
WINDOWS = (
    ("full", None),
    ("last_10y", pd.DateOffset(years=10)),
    ("last_5y", pd.DateOffset(years=5)),
    ("last_3y", pd.DateOffset(years=3)),
    ("last_2y", pd.DateOffset(years=2)),
    ("last_1y", pd.DateOffset(years=1)),
    ("ytd", "ytd"),
)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(_json_safe(payload), ensure_ascii=False, indent=2), encoding="utf-8")


def _label(lookback: int, halflife: float) -> str:
    return f"lb{lookback}_hl{halflife:g}".replace(".", "p")


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
    active_spells = active.groupby(spell_id).sum()
    active_spells = active_spells.loc[active_spells.gt(0)]
    return {
        "entry_signals": int(holding.eq("cash").mul(next_holding.ne("cash")).sum()),
        "exit_signals": int(holding.ne("cash").mul(next_holding.eq("cash")).sum()),
        "holding_days": int(active.sum()),
        "cash_days": int(holding.eq("cash").sum()),
        "active_spell_count": int(len(active_spells)),
        "active_spell_avg_days": float(active_spells.mean()) if len(active_spells) else 0.0,
    }


def _score(row: pd.Series) -> float:
    return (
        float(row["ann_return_last_3y"])
        + 0.50 * float(row["ann_return_last_2y"])
        + 0.35 * float(row["ann_return_last_1y"])
        + 0.75 * float(row["max_dd_last_3y"])
        + 0.50 * float(row["max_dd_last_2y"])
    )


def _scan(run_folder: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    v23.generate_v2_3_outputs()
    _reference_summary, _signal_df, official_out = v23.generate_v2_3_outputs()
    _reference_summary2, base_gross_cached, turnover_df = v23.v2_0.embedded_context._load_embedded_base_context()
    close_df = v23._close_df_from_base(base_gross_cached)
    official_index = pd.DatetimeIndex(official_out.index)

    summary_rows: list[dict[str, Any]] = []
    wide_rows: list[dict[str, Any]] = []
    strict_rows: list[dict[str, Any]] = []
    for lookback in LOOKBACKS:
        for halflife in HALFLIFES:
            label = _label(lookback, halflife)
            out = _build_candidate(close_df, turnover_df, official_index, lookback, halflife, None)
            out.reset_index(names="date").to_csv(run_folder / f"daily_{label}.csv", index=False, encoding="utf-8")
            strict = _apply_extra_cost(out, transition_cost=True, slippage_5bp=True)
            wide: dict[str, Any] = {
                "candidate": label,
                "lookback": lookback,
                "halflife": halflife,
                "is_center": lookback == CENTER_LOOKBACK and abs(halflife - CENTER_HALFLIFE) < 1e-12,
            }
            for window, spec in WINDOWS:
                start, end = _window_bounds(pd.DatetimeIndex(out.index), spec)
                part = out.loc[(out.index >= start) & (out.index <= end)]
                strict_part = strict.loc[(strict.index >= start) & (strict.index <= end)]
                metrics = _metrics(part["return_net"])
                strict_metrics = _metrics(strict_part["return_net"])
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
                    "lookback": lookback,
                    "halflife": halflife,
                }
                summary_rows.append(row)
                strict_rows.append(
                    {
                        "candidate": label,
                        "segment": window,
                        "lookback": lookback,
                        "halflife": halflife,
                        "ann_return": strict_metrics["ann_return"],
                        "ann_vol": strict_metrics["ann_vol"],
                        "sharpe_repo": strict_metrics["sharpe_repo"],
                        "max_dd": strict_metrics["max_dd"],
                        "extra_cost_total": float(pd.to_numeric(strict_part.get("strict_extra_cost", 0.0), errors="coerce").fillna(0.0).sum()) if len(strict_part) else 0.0,
                    }
                )
                for metric in ("ann_return", "max_dd", "sharpe_repo", "holding_day_ratio", "entry_signals", "exit_signals", "active_spell_avg_days"):
                    wide[f"{metric}_{window}"] = row[metric]
                wide[f"strict_ann_return_{window}"] = strict_metrics["ann_return"]
                wide[f"strict_max_dd_{window}"] = strict_metrics["max_dd"]
            wide_rows.append(wide)

    summary = pd.DataFrame(summary_rows)
    wide = pd.DataFrame(wide_rows)
    strict_df = pd.DataFrame(strict_rows)
    wide["ridge_score"] = wide.apply(_score, axis=1)
    center = wide.loc[wide["is_center"]].iloc[0]
    wide["score_vs_center"] = wide["ridge_score"] - float(center["ridge_score"])
    wide["near_center"] = wide["lookback"].between(CENTER_LOOKBACK - 1, CENTER_LOOKBACK + 1) & wide["halflife"].between(CENTER_HALFLIFE - 0.5, CENTER_HALFLIFE + 0.5)
    wide["within_95pct_center_score"] = wide["ridge_score"] >= float(center["ridge_score"]) * 0.95
    wide["within_90pct_center_score"] = wide["ridge_score"] >= float(center["ridge_score"]) * 0.90
    wide["decision_hint"] = "center" 
    wide.loc[~wide["is_center"], "decision_hint"] = "compare_only"
    wide.loc[wide["ridge_score"].eq(wide["ridge_score"].max()), "decision_hint"] = "best_ridge_score"

    ridge_rows: list[dict[str, Any]] = []
    for axis_name, mask in {
        "halflife_axis_lb17": wide["lookback"].eq(CENTER_LOOKBACK),
        "lookback_axis_hl3": wide["halflife"].eq(CENTER_HALFLIFE),
        "near_center_3x3": wide["near_center"],
        "all_grid": pd.Series(True, index=wide.index),
    }.items():
        part = wide.loc[mask].copy()
        ridge_rows.append(
            {
                "axis": axis_name,
                "candidate_count": int(len(part)),
                "within_95pct_center_count": int(part["within_95pct_center_score"].sum()),
                "within_90pct_center_count": int(part["within_90pct_center_score"].sum()),
                "best_candidate": str(part.sort_values("ridge_score", ascending=False).iloc[0]["candidate"]),
                "best_ridge_score": float(part["ridge_score"].max()),
                "worst_ridge_score": float(part["ridge_score"].min()),
                "center_score": float(center["ridge_score"]),
            }
        )
    ridge = pd.DataFrame(ridge_rows)

    summary.to_csv(run_folder / "scan_summary.csv", index=False, encoding="utf-8")
    wide.to_csv(run_folder / "window_metrics.csv", index=False, encoding="utf-8")
    strict_df.to_csv(run_folder / "strict_cost_metrics.csv", index=False, encoding="utf-8")
    ridge.to_csv(run_folder / "ridge_width_summary.csv", index=False, encoding="utf-8")
    context = {
        "metrics_start": str(pd.Timestamp(official_index.min()).date()),
        "metrics_end": str(pd.Timestamp(official_index.max()).date()),
        "rows": int(len(official_index)),
        "turnover_rows": int(len(turnover_df)),
    }
    return summary, wide, ridge, context


def _write_record(run_folder: Path, wide: pd.DataFrame, ridge: pd.DataFrame, meta: dict[str, Any]) -> None:
    center = wide.loc[wide["is_center"]].iloc[0]
    top = wide.sort_values("ridge_score", ascending=False).head(12)
    near = wide.loc[wide["near_center"]].sort_values(["lookback", "halflife"])
    lines = [
        "# Quant Parameter Scan Record",
        "",
        "## Run Metadata",
        "",
        f"- Run id: `{run_folder.name}`",
        f"- Created at: {meta['created_at']}",
        "- Project: microcap Top100",
        "- Strategy or version: v2.3",
        "- Sleeve or subsystem: signal-model-ridge",
        "- Parameter group: `hl3_local_ridge_width`",
        "- Scan type: local_ridge_width",
        f"- Repo or workspace path: `{ROOT}`",
        "- Target entrypoint: `microcap_top100_mom16_biweekly_live_v2_3.py`",
        f"- Git branch: `{meta['git_branch']}`",
        f"- Git commit: `{meta['git_commit']}`",
        "",
        "## Research Question",
        "",
        "- Test whether `LOOKBACK=17, HALFLIFE=3` has enough ridge width on both parameter axes to qualify as a formal signal candidate.",
        "- Grid: LOOKBACK 14..20, HALFLIFE 2.0/2.5/3.0/3.5/4.0, no R2 gate.",
        "- Ridge score weights 3Y/2Y/1Y return and 3Y/2Y drawdown.",
        "- Source-change rule: `research_only_no_source_change`.",
        "- Required windows: full, 10Y, 5Y, 3Y, 2Y, 1Y, YTD.",
        "",
        "## Implementation Anchor",
        "",
        "- Reused parameterized candidate builder from `run_microcap_v2_3_lb_hl_r2_gate_scan.py`.",
        "- Each candidate uses current v2.3 costed target-vol and cash-day-yield path.",
        "- Strict cost metrics use transition 10bp plus turnover slippage 5bp overlay.",
        "",
        "## Data Snapshot",
        "",
        f"- Metrics start: {meta['data_snapshot']['metrics_start']}",
        f"- Metrics end: {meta['data_snapshot']['metrics_end']}",
        f"- Rows: {meta['data_snapshot']['rows']}",
        f"- Turnover rows: {meta['data_snapshot']['turnover_rows']}",
        "",
        "## Cost and Execution Assumptions",
        "",
        "- Official costed path: current v2.3.",
        "- Strict cost path: post-run transition 10bp plus turnover slippage 5bp.",
        "- Signal/execution hedge mismatch retained at 1.0/0.8.",
        "",
        "## Runtime Override Plan",
        "",
        "- Runtime parameterized rebuild only; no production defaults changed.",
        "- Center candidate: `lb17_hl3`.",
        "",
        "## Commands",
        "",
        "```powershell",
        "python microcap_top100_mom16_biweekly_live_v2_3.py",
        f"python scripts/run_microcap_v2_3_hl3_local_ridge_width.py --run-folder {run_folder}",
        "```",
        "",
        "## Output Files",
        "",
        "- `scan_summary.csv`: long-form metrics.",
        "- `window_metrics.csv`: wide metrics and ridge-score flags.",
        "- `strict_cost_metrics.csv`: strict-cost metrics.",
        "- `ridge_width_summary.csv`: ridge-width counts by axis.",
        "",
        "## Full-Sample Results",
        "",
        f"- Center `lb17_hl3`: full annual return {center['ann_return_full']:.4%}, full max drawdown {center['max_dd_full']:.4%}.",
        f"- Center 3Y: annual return {center['ann_return_last_3y']:.4%}, max drawdown {center['max_dd_last_3y']:.4%}.",
        f"- Center 1Y: annual return {center['ann_return_last_1y']:.4%}, max drawdown {center['max_dd_last_1y']:.4%}.",
        "",
        "## Window Results",
        "",
        "- Top 12 by ridge score:",
        "",
        top[["candidate", "ridge_score", "ann_return_last_3y", "max_dd_last_3y", "ann_return_last_2y", "max_dd_last_2y", "ann_return_last_1y", "max_dd_last_1y", "strict_ann_return_last_3y", "strict_max_dd_last_3y"]].to_markdown(index=False),
        "",
        "## Stability Classification",
        "",
        "- Local 3x3 neighborhood around center:",
        "",
        near[["candidate", "ridge_score", "score_vs_center", "within_95pct_center_score", "ann_return_last_3y", "max_dd_last_3y", "ann_return_last_1y", "max_dd_last_1y"]].to_markdown(index=False),
        "",
        "- Ridge-width summary:",
        "",
        ridge.to_markdown(index=False),
        "",
        "## Decision",
        "",
        "- Decision: local ridge scan complete; do not change production defaults automatically.",
        "- Recommended next action: if center and adjacent points remain in the high-score ridge, `HL=3` can be promoted as a formal signal variant rather than replacing current v2.3 immediately.",
        "",
        "## User-Facing Summary",
        "",
        "- This run checks whether the apparent `HL=3` improvement is a robust ridge or a narrow optimum.",
    ]
    (run_folder / "record.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(run_folder: Path) -> None:
    started = time.time()
    run_folder.mkdir(parents=True, exist_ok=True)
    with (run_folder / "command_log.txt").open("a", encoding="utf-8") as fh:
        fh.write(f"\n[{pd.Timestamp.now().isoformat()}] python scripts/run_microcap_v2_3_hl3_local_ridge_width.py --run-folder {run_folder}\n")
    _summary, wide, ridge, context = _scan(run_folder)
    created_at = pd.Timestamp.now().isoformat()
    meta = {
        "run_id": run_folder.name,
        "created_at": created_at,
        "phase": "analysis_written",
        "project": "microcap Top100",
        "strategy": "v2.3",
        "subsystem": "signal-model-ridge",
        "repo_root": str(ROOT),
        "entrypoint": "microcap_top100_mom16_biweekly_live_v2_3.py",
        "git_branch": _git(["branch", "--show-current"]),
        "git_commit": _git(["rev-parse", "HEAD"]),
        "git_status_before": _git(["status", "--short"]),
        "git_status_after": _git(["status", "--short"]),
        "scan_type": "local_ridge_width",
        "parameter_group": "hl3_local_ridge_width",
        "baseline": {
            "center_candidate": _label(CENTER_LOOKBACK, CENTER_HALFLIFE),
            "lookback": CENTER_LOOKBACK,
            "halflife": CENTER_HALFLIFE,
            "r2_entry_gate": None,
        },
        "candidate_grid": wide["candidate"].tolist(),
        "data_snapshot": context,
        "cost_model": {
            "official_costed": True,
            "strict_cost_overlay": "transition_10bp_plus_slippage_5bp",
            "uses_current_v23_cash_day_yield": True,
        },
        "outputs": {
            "record": str(run_folder / "record.md"),
            "scan_summary": str(run_folder / "scan_summary.csv"),
            "window_metrics": str(run_folder / "window_metrics.csv"),
            "scan_meta": str(run_folder / "scan_meta.json"),
            "command_log": str(run_folder / "command_log.txt"),
            "strict_cost_metrics": str(run_folder / "strict_cost_metrics.csv"),
            "ridge_width_summary": str(run_folder / "ridge_width_summary.csv"),
        },
        "decision": "local_ridge_scan_complete_no_production_change",
        "stability_label": "pending_ridge_interpretation",
        "elapsed_sec": round(time.time() - started, 3),
    }
    _write_json(run_folder / "scan_meta.json", meta)
    _write_record(run_folder, wide, ridge, meta)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-folder", type=Path, required=True)
    args = parser.parse_args()
    run(args.run_folder)


if __name__ == "__main__":
    main()
