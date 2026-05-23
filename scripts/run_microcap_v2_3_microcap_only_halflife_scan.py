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

import microcap_top100_mom16_biweekly_live_v2_3 as v23  # noqa: E402


TRADING_DAYS = int(v23.TRADING_DAYS)
RUN_FOLDER = ROOT / "quant_param_scan_runs" / "20260523_microcap_v2_3_microcap_only_halflife_lb17_thr40"
LOOKBACK = int(v23.LOOKBACK)
MOMENTUM_THRESHOLD = 0.40
HALFLIFES = (1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 5.0, 6.0, 8.0, 10.0, 12.0, 16.0, 20.0)


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


def _label(halflife: float) -> str:
    return f"lb{LOOKBACK}_hl{halflife:g}_thr0p40".replace(".", "p")


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


def _base_and_scores(close_df: pd.DataFrame, official_index: pd.DatetimeIndex) -> tuple[pd.DataFrame, dict[float, pd.DataFrame]]:
    close_df = close_df.sort_index()
    micro_ret = close_df["microcap"].pct_change(fill_method=None)
    hedge_ret = close_df["hedge"].pct_change(fill_method=None)
    micro_nav = (1.0 + micro_ret.fillna(0.0)).cumprod()
    micro_nav.name = "microcap_nav"

    frames: dict[float, pd.DataFrame] = {}
    common_index = pd.DatetimeIndex(official_index)
    common_index = common_index[common_index >= v23.FORMAL_START_DATE]
    for halflife in HALFLIFES:
        score = v23.log_wls_score_and_r2(micro_nav, lookback=LOOKBACK, halflife=float(halflife))
        valid = score["annualized_log_wls_score"].notna() & score["log_wls_r2"].notna()
        common_index = pd.DatetimeIndex(common_index.intersection(pd.DatetimeIndex(score.index[valid])))
        frames[float(halflife)] = score
    common_index = pd.DatetimeIndex(common_index).sort_values()

    base = pd.DataFrame(
        {
            "microcap_close": close_df["microcap"].loc[common_index],
            "hedge_close": close_df["hedge"].loc[common_index],
            "microcap_ret": micro_ret.loc[common_index],
            "hedge_ret": hedge_ret.loc[common_index],
            "microcap_nav": micro_nav.loc[common_index],
        },
        index=common_index,
    )
    return base, frames


def _build_candidate_gross(base: pd.DataFrame, score_frame: pd.DataFrame, halflife: float) -> pd.DataFrame:
    score = pd.to_numeric(score_frame["annualized_log_wls_score"].loc[base.index], errors="coerce")
    r2 = pd.to_numeric(score_frame["log_wls_r2"].loc[base.index], errors="coerce")
    signal_on = score.gt(MOMENTUM_THRESHOLD)
    current_active = signal_on.shift(1, fill_value=False)
    ret = pd.to_numeric(base["microcap_ret"], errors="coerce").fillna(0.0)
    gross_ret = pd.Series(np.where(current_active, ret, 0.0), index=base.index, dtype=float)
    out = pd.DataFrame(
        {
            "return_raw": gross_ret,
            "return": gross_ret,
            "holding": np.where(current_active, "long_microcap_top100", "cash"),
            "next_holding": np.where(signal_on, "long_microcap_top100", "cash"),
            "signal_on": signal_on.astype(bool),
            "microcap_close": base["microcap_close"],
            "hedge_close": base["hedge_close"],
            "microcap_ret": base["microcap_ret"],
            "hedge_ret": base["hedge_ret"],
            "microcap_mom": score,
            "hedge_mom": 0.0,
            "momentum_gap": score,
            "annualized_log_wls_score": score,
            "log_wls_r2": r2,
            "microcap_nav": base["microcap_nav"],
            "signal_score_label": "microcap_only_annualized_log_wls_score",
            "momentum_threshold": MOMENTUM_THRESHOLD,
            "lookback": LOOKBACK,
            "halflife": float(halflife),
            "r2_gate": np.nan,
            "futures_drag": 0.0,
            "active_spread_ret": gross_ret,
            "weight": 1.0,
            "target_vol_execution_scale": 1.0,
        },
        index=base.index,
    )
    out["nav_gross"] = (1.0 + out["return"].fillna(0.0)).cumprod()
    return out


def _apply_cost(gross: pd.DataFrame, turnover_df: pd.DataFrame) -> pd.DataFrame:
    out = v23.v2_0.base_mod.freq_mod.cost_mod.apply_cost_model(gross, turnover_df)
    out["nav_gross"] = gross["nav_gross"]
    out["strategy_variant"] = "v2_3_microcap_only_halflife_lb17_thr40_cost_only"
    out["hedge_removed"] = True
    out["target_vol_enabled"] = False
    out["cash_yield_enabled"] = False
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
    base, score_frames = _base_and_scores(close_df, pd.DatetimeIndex(official_out.index))
    summary_rows: list[dict[str, Any]] = []
    wide_rows: list[dict[str, Any]] = []
    verification_rows: list[dict[str, Any]] = []

    for halflife in HALFLIFES:
        label = _label(float(halflife))
        gross = _build_candidate_gross(base, score_frames[float(halflife)], float(halflife))
        out = _apply_cost(gross, turnover_df)
        out.rename_axis("date").reset_index().to_csv(run_folder / f"daily_{label}.csv", index=False, encoding="utf-8")
        counts = _transition_counts(out)
        gross_final_nav = float(gross["nav_gross"].iloc[-1])
        costed_final_nav = float(out["nav_net"].iloc[-1])
        verification_rows.append(
            {
                "candidate": label,
                "gross_final_nav": gross_final_nav,
                "costed_final_nav": costed_final_nav,
                "costed_lte_gross": bool(costed_final_nav <= gross_final_nav + 1e-12),
                "rows_match": bool(len(out) == len(gross)),
            }
        )
        wide: dict[str, Any] = {
            "candidate": label,
            "lookback": LOOKBACK,
            "halflife": float(halflife),
            "momentum_threshold": MOMENTUM_THRESHOLD,
            "r2_gate": np.nan,
            "holding_days_full": counts["holding_days"],
            "cash_days_full": counts["cash_days"],
            "entry_days_full": counts["entry_days"],
            "exit_days_full": counts["exit_days"],
            "gross_final_nav_full": gross_final_nav,
        }
        for segment, (start, end) in _window_slices(pd.DatetimeIndex(out.index)).items():
            part = out.loc[(out.index >= start) & (out.index <= end)]
            m = _metrics(part["return_net"])
            part_counts = _transition_counts(part)
            cost_total = float(pd.to_numeric(part.get("total_cost", 0.0), errors="coerce").fillna(0.0).sum()) if len(part) else 0.0
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
                "entry_days": part_counts["entry_days"],
                "exit_days": part_counts["exit_days"],
                "total_cost_sum": cost_total,
                "lookback": LOOKBACK,
                "halflife": float(halflife),
                "momentum_threshold": MOMENTUM_THRESHOLD,
                "r2_gate": np.nan,
            }
            summary_rows.append(row)
            for metric in (
                "ann_return",
                "ann_vol",
                "sharpe_repo",
                "max_dd",
                "final_nav",
                "holding_day_ratio",
                "entry_days",
                "exit_days",
                "total_cost_sum",
            ):
                wide[f"{metric}_{segment}"] = row[metric]
        wide["decision_score"] = np.nan
        wide["decision_hint"] = "compare_only"
        wide["stability_label"] = "candidate"
        wide_rows.append(wide)

    summary = pd.DataFrame(summary_rows)
    wide = pd.DataFrame(wide_rows)
    wide["decision_score"] = wide.apply(_decision_score, axis=1)
    baseline_label = _label(float(v23.HALFLIFE))
    best_label = str(wide.sort_values("decision_score", ascending=False).iloc[0]["candidate"])
    wide.loc[wide["candidate"].eq(baseline_label), "decision_hint"] = "current_halflife_baseline"
    wide.loc[wide["candidate"].eq(best_label), "decision_hint"] = "best_recent_balanced_score"
    wide.loc[wide["candidate"].eq(best_label), "stability_label"] = "watchlist_best"

    verification = pd.DataFrame(verification_rows)
    summary.to_csv(run_folder / "scan_summary.csv", index=False, encoding="utf-8")
    wide.to_csv(run_folder / "window_metrics.csv", index=False, encoding="utf-8")
    verification.to_csv(run_folder / "cost_parity_checks.csv", index=False, encoding="utf-8")
    context = {
        "reference_summary": reference_summary,
        "turnover_rows": int(len(turnover_df)),
        "metrics_start": str(pd.Timestamp(base.index.min()).date()),
        "metrics_end": str(pd.Timestamp(base.index.max()).date()),
        "rows": int(len(base)),
        "close_df_start": str(pd.Timestamp(close_df.index.min()).date()),
        "close_df_end": str(pd.Timestamp(close_df.index.max()).date()),
        "baseline_label": baseline_label,
        "best_label": best_label,
        "all_costed_lte_gross": bool(verification["costed_lte_gross"].all()),
        "all_rows_match": bool(verification["rows_match"].all()),
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
        "- Strategy or version: v2.3 derived",
        "- Sleeve or subsystem: microcap-only signal",
        "- Parameter group: `halflife`",
        "- Scan type: signal_parameter_grid",
        f"- Repo or workspace path: `{ROOT}`",
        "- Target entrypoint: `scripts/run_microcap_v2_3_microcap_only_halflife_scan.py`",
        f"- Git branch: `{meta['git_branch']}`",
        f"- Git commit: `{meta['git_commit']}`",
        "- Working tree status before: see `scan_meta.json`.",
        "",
        "## Research Question",
        "",
        f"- Baseline: microcap-only log-WLS momentum with lookback `{LOOKBACK}`, halflife `{v23.HALFLIFE:g}`, threshold `{MOMENTUM_THRESHOLD:.2f}`, no R2 gate.",
        f"- Candidate grid: halflifes `{list(HALFLIFES)}`.",
        "- Decision target: identify whether the WLS decay rate improves after threshold and lookback layers.",
        "- Source-change rule: `research_only_no_source_change`.",
        "- Required windows: full, last_10y, last_5y, last_3y, last_1y.",
        "",
        "## Implementation Anchor",
        "",
        "- Base source: `microcap_top100_mom16_biweekly_live_v2_3.py` for refreshed v2.3 index and local Top100 data.",
        "- Signal object: log-WLS score on microcap Top100 NAV only.",
        "- Signal timing: close-confirmed score at T controls holding on T+1.",
        "- Existing cost model reused: `v2_0.base_mod.freq_mod.cost_mod.apply_cost_model`.",
        "- Date alignment: all candidates share the same valid date index.",
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
        "- Retained: Top100 basket entry/exit/rebalance transaction costs.",
        "- Removed: ZZ1000 hedge, futures drag, target-vol scaling, financing, cash-day yield, spread NAV, exit buffer, peak decay, R2 gate, and broad-volume overlays.",
        "- Entry/exit cost: 30bp one-side buy and 30bp one-side sell from the repo cost model.",
        "- Rebalance cost: existing turnover table mapped by the repo cost model.",
        "",
        "## Commands",
        "",
        "```powershell",
        f"python scripts/run_microcap_v2_3_microcap_only_halflife_scan.py --run-folder {run_folder}",
        f"python C:\\Users\\Administrator.DESKTOP-95I7VVU\\.codex\\skills\\quant-param-scan\\scripts\\finalize_quant_param_scan_run.py {run_folder} --decision research_only_watchlist_halflife --stability-label third_layer_halflife_scan",
        f"python C:\\Users\\Administrator.DESKTOP-95I7VVU\\.codex\\skills\\quant-param-scan\\scripts\\check_quant_param_scan_artifacts.py --phase complete --strict {run_folder}",
        "```",
        "",
        "## Output Files",
        "",
        "- `scan_summary.csv`: long-form window metrics.",
        "- `window_metrics.csv`: wide candidate table.",
        "- `cost_parity_checks.csv`: costed-vs-gross sanity check.",
        "- `daily_*.csv`: candidate daily paths.",
        "",
        "## Full-Sample Results",
        "",
        f"- Baseline `{context['baseline_label']}`: annual return {baseline['ann_return_full']:.4%}, max drawdown {baseline['max_dd_full']:.4%}, Sharpe {baseline['sharpe_repo_full']:.3f}.",
        f"- Best balanced candidate `{best['candidate']}`: annual return {best['ann_return_full']:.4%}, max drawdown {best['max_dd_full']:.4%}, Sharpe {best['sharpe_repo_full']:.3f}.",
        "",
        "## Window Results",
        "",
        f"- Best 5Y: annual return {best['ann_return_last_5y']:.4%}, max drawdown {best['max_dd_last_5y']:.4%}.",
        f"- Best 3Y: annual return {best['ann_return_last_3y']:.4%}, max drawdown {best['max_dd_last_3y']:.4%}.",
        f"- Best 1Y: annual return {best['ann_return_last_1y']:.4%}, max drawdown {best['max_dd_last_1y']:.4%}.",
        "",
        "## Top Candidates",
        "",
        top10.to_markdown(index=False, floatfmt=".6f"),
        "",
        "## Stability Classification",
        "",
        "- Label: third_layer_halflife_scan.",
        "- Evidence: compare `window_metrics.csv`; this is not a promotion decision.",
        f"- Cost sanity: all candidates costed NAV <= gross NAV: {context['all_costed_lte_gross']}.",
        "",
        "## Decision",
        "",
        "- Decision: research_only_watchlist_halflife.",
        "- Recommended next action: use this as the decay-rate baseline for the next condition layer.",
    ]
    (run_folder / "record.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(run_folder: Path) -> None:
    started = time.time()
    run_folder.mkdir(parents=True, exist_ok=True)
    command_log = run_folder / "command_log.txt"
    with command_log.open("a", encoding="utf-8") as fh:
        fh.write(f"\n[{pd.Timestamp.now().isoformat()}] python scripts/run_microcap_v2_3_microcap_only_halflife_scan.py --run-folder {run_folder}\n")

    _summary, wide, context = _scan(run_folder)
    meta = {
        "run_id": run_folder.name,
        "created_at": pd.Timestamp.now().isoformat(),
        "phase": "analysis_written",
        "project": "microcap Top100",
        "strategy": "v2.3 derived",
        "subsystem": "microcap-only signal",
        "repo_root": str(ROOT),
        "entrypoint": "scripts/run_microcap_v2_3_microcap_only_halflife_scan.py",
        "git_branch": _git(["branch", "--show-current"]),
        "git_commit": _git(["rev-parse", "HEAD"]),
        "git_status_before": _git(["status", "--short"]),
        "git_status_after": _git(["status", "--short"]),
        "scan_type": "signal_parameter_grid",
        "parameter_group": "halflife",
        "baseline": {
            "candidate": context["baseline_label"],
            "lookback": LOOKBACK,
            "halflife": float(v23.HALFLIFE),
            "momentum_threshold": MOMENTUM_THRESHOLD,
            "r2_gate": None,
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
            "same_index_all_candidates": True,
        },
        "cost_model": {
            "retained": "top100_basket_transaction_cost_model",
            "entry_buy_one_side": float(v23.v2_0.base_mod.freq_mod.cost_mod.ENTRY_COST),
            "exit_sell_one_side": float(v23.v2_0.base_mod.freq_mod.cost_mod.EXIT_COST),
            "rebalance_cost_source": "turnover table via map_rebalance_apply_costs",
            "target_vol_enabled": False,
            "hedge_removed": True,
            "cash_yield_enabled": False,
        },
        "verification": {
            "all_costed_lte_gross": context["all_costed_lte_gross"],
            "all_rows_match": context["all_rows_match"],
        },
        "outputs": {
            "record": str(run_folder / "record.md"),
            "scan_summary": str(run_folder / "scan_summary.csv"),
            "window_metrics": str(run_folder / "window_metrics.csv"),
            "scan_meta": str(run_folder / "scan_meta.json"),
            "command_log": str(command_log),
            "cost_parity_checks": str(run_folder / "cost_parity_checks.csv"),
        },
        "decision": "research_only_watchlist_halflife",
        "stability_label": "third_layer_halflife_scan",
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
