from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import akshare as ak
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import microcap_top100_mom16_biweekly_live_v2_5 as v25  # noqa: E402


RUN_FOLDER = (
    ROOT
    / "quant_param_scan_runs"
    / "20260523_microcap_top100_v2_5_broad_volume_warning_overlay_zz2000_cyb_amount_ma_days_scale"
)
TRADING_DAYS = int(v25.TRADING_DAYS)
SCALE_CHANGE_COST = 0.003
CSI2000_CODE = "932000"
CSI2000_SYMBOL = "csindex932000"
CSI2000_NAME = "CSI2000 index"
CYB_SYMBOL_TX = "sz399006"
MA_GRID = list(range(45, 66, 5))
DAYS_GRID = list(range(10, 21))
SCALE_GRID = [0.0, 0.25, 0.5, 0.75]
WINDOWS = {
    "full": None,
    "last_10y": pd.DateOffset(years=10),
    "last_5y": pd.DateOffset(years=5),
    "last_3y": pd.DateOffset(years=3),
    "last_1y": pd.DateOffset(years=1),
}


def _git(args: list[str]) -> str:
    try:
        return subprocess.check_output(["git", *args], cwd=ROOT, text=True, stderr=subprocess.STDOUT).strip()
    except Exception as exc:  # noqa: BLE001
        return f"git_error:{exc}"


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, Path):
        return str(value)
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


def _metrics(ret: pd.Series) -> dict[str, float | int]:
    ret = pd.to_numeric(ret, errors="coerce").fillna(0.0)
    rows = int(len(ret))
    if rows == 0:
        return {
            "rows": 0,
            "ann_return": np.nan,
            "ann_vol": np.nan,
            "sharpe_repo": np.nan,
            "max_dd": np.nan,
            "final_nav": np.nan,
        }
    nav = (1.0 + ret).cumprod()
    ann_return = float(nav.iloc[-1] ** (TRADING_DAYS / rows) - 1.0) if nav.iloc[-1] > 0 else np.nan
    ann_vol = float(ret.std(ddof=1) * math.sqrt(TRADING_DAYS)) if rows > 1 else 0.0
    sharpe = ann_return / ann_vol if ann_vol > 0 and math.isfinite(ann_vol) else np.nan
    max_dd = float(nav.div(nav.cummax()).sub(1.0).min())
    return {
        "rows": rows,
        "ann_return": ann_return,
        "ann_vol": ann_vol,
        "sharpe_repo": float(sharpe) if math.isfinite(sharpe) else np.nan,
        "max_dd": max_dd,
        "final_nav": float(nav.iloc[-1]),
    }


def _window_index(index: pd.DatetimeIndex, offset: pd.DateOffset | None) -> pd.DatetimeIndex:
    if offset is None:
        return index
    cutoff = index.max() - offset
    return index[index >= cutoff]


def _find_column(columns: list[object], *patterns: str) -> object:
    for col in columns:
        name = str(col).lower()
        if all(pattern.lower() in name for pattern in patterns):
            return col
    raise RuntimeError(f"missing expected column matching {patterns}: {[str(c) for c in columns]}")


def _load_csi2000_amount() -> pd.Series:
    df = ak.stock_zh_index_hist_csindex(symbol=CSI2000_CODE, start_date="20100101", end_date="20991231")
    date_col = _find_column(df.columns.tolist(), "\u65e5\u671f")
    try:
        amount_col = _find_column(df.columns.tolist(), "\u91d1\u989d")
    except RuntimeError:
        amount_col = _find_column(df.columns.tolist(), "amount")
    out = (
        df.loc[:, [date_col, amount_col]]
        .rename(columns={date_col: "date", amount_col: "amount"})
        .assign(date=lambda x: pd.to_datetime(x["date"], errors="coerce"))
        .dropna(subset=["date", "amount"])
        .drop_duplicates("date", keep="last")
        .sort_values("date")
        .set_index("date")["amount"]
    )
    out = pd.to_numeric(out, errors="coerce").dropna()
    if out.empty:
        raise RuntimeError("CSI2000 amount series is empty")
    return out.rename("csi2000_amount")


def _load_cyb_amount() -> pd.Series:
    df = ak.stock_zh_index_daily_tx(symbol=CYB_SYMBOL_TX)
    if "date" not in df.columns or "amount" not in df.columns:
        raise RuntimeError(f"unexpected CYB columns: {df.columns.tolist()}")
    out = (
        df.loc[:, ["date", "amount"]]
        .assign(date=lambda x: pd.to_datetime(x["date"], errors="coerce"))
        .dropna(subset=["date", "amount"])
        .drop_duplicates("date", keep="last")
        .sort_values("date")
        .set_index("date")["amount"]
    )
    out = pd.to_numeric(out, errors="coerce").dropna()
    if out.empty:
        raise RuntimeError("CYB amount series is empty")
    return out.rename("cyb_amount")


def _load_v2_5_nav() -> tuple[dict[str, object], pd.DataFrame]:
    summary, _signal, nav = v25.generate_v2_5_outputs()
    nav = nav.copy().sort_index()
    if not isinstance(nav.index, pd.DatetimeIndex):
        nav.index = pd.DatetimeIndex(nav.index)
    return summary, nav


def _build_execution_signal(amount: pd.DataFrame, ma: int, days: int, nav_index: pd.DatetimeIndex) -> pd.Series:
    amount = amount.sort_index()
    csi_below = amount["csi2000_amount"].lt(amount["csi2000_amount"].rolling(ma, min_periods=ma).mean())
    cyb_below = amount["cyb_amount"].lt(amount["cyb_amount"].rolling(ma, min_periods=ma).mean())
    condition = csi_below.fillna(False) & cyb_below.fillna(False)
    run_id = condition.ne(condition.shift(fill_value=False)).cumsum()
    consecutive = condition.groupby(run_id).cumcount() + 1
    trigger = (condition & consecutive.ge(days)).rename("volume_trigger")
    trigger_on_nav = trigger.reindex(nav_index).fillna(False).astype(bool)
    return trigger_on_nav.shift(1, fill_value=False).rename("volume_execution_day")


def _apply_volume_scale(nav: pd.DataFrame, execution_day: pd.Series, scale: float) -> tuple[pd.Series, pd.Series, pd.Series]:
    base_ret = pd.to_numeric(nav["return_net"], errors="coerce").fillna(0.0)
    execution_day = execution_day.reindex(nav.index).fillna(False).astype(bool)
    active_exposure = (
        pd.to_numeric(nav.get("current_execution_scale", pd.Series(1.0, index=nav.index)), errors="coerce")
        .fillna(0.0)
        .gt(1e-12)
    )
    scale_series = pd.Series(1.0, index=nav.index, dtype=float)
    scale_series.loc[execution_day & active_exposure] = float(scale)
    overlay_cost = scale_series.diff().abs().fillna(0.0) * active_exposure.astype(float) * SCALE_CHANGE_COST
    ret = base_ret * scale_series - overlay_cost
    return ret.rename("return_net"), scale_series.rename("volume_execution_scale"), overlay_cost.rename("volume_overlay_cost")


def _score_candidate(row: dict[str, Any]) -> float:
    return float(
        row["ann_return_delta_last_5y"]
        + row["ann_return_delta_last_3y"]
        + 0.5 * row["ann_return_delta_last_10y"]
        + 0.5 * row["max_dd_delta_last_10y"]
        + 0.5 * row["max_dd_delta_last_5y"]
        - max(0.0, -row["ann_return_delta_full"]) * 0.5
    )


def _scan(run_folder: Path) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    reference_summary, nav = _load_v2_5_nav()
    csi_amount = _load_csi2000_amount()
    cyb_amount = _load_cyb_amount()
    amount = pd.concat([csi_amount, cyb_amount], axis=1).dropna().sort_index()
    if amount.empty:
        raise RuntimeError("combined CSI2000 + CYB amount frame is empty")

    common_start = max(pd.Timestamp(nav.index.min()), pd.Timestamp(amount.index.min()))
    common_end = min(pd.Timestamp(nav.index.max()), pd.Timestamp(amount.index.max()))
    nav = nav.loc[(nav.index >= common_start) & (nav.index <= common_end)].copy()
    amount = amount.loc[(amount.index >= common_start) & (amount.index <= common_end)].copy()
    if nav.empty:
        raise RuntimeError("v2.5 NAV is empty after amount alignment")

    base_ret = pd.to_numeric(nav["return_net"], errors="coerce").fillna(0.0)
    baseline_by_window: dict[str, dict[str, float | int]] = {}
    summary_rows: list[dict[str, Any]] = []
    wide_rows: list[dict[str, Any]] = []

    baseline_wide: dict[str, Any] = {
        "candidate": "v2_5_baseline",
        "version": "v2.5",
        "family": "baseline",
        "ma": 0,
        "days": 0,
        "scale": 1.0,
        "trigger_days": 0,
        "execution_days": 0,
        "cost_total_full": 0.0,
        "score": 0.0,
        "robust_pass": True,
        "decision_hint": "baseline",
        "stability_label": "formal_v2_5_baseline",
    }
    for segment, offset in WINDOWS.items():
        idx = _window_index(nav.index, offset)
        metrics = _metrics(base_ret.loc[idx])
        baseline_by_window[segment] = metrics
        summary_rows.append(
            {
                "candidate": "v2_5_baseline",
                "version": "v2.5",
                "family": "baseline",
                "ma": 0,
                "days": 0,
                "scale": 1.0,
                "segment": segment,
                "start": str(idx.min().date()),
                "end": str(idx.max().date()),
                **metrics,
                "trigger_days": 0,
                "execution_days": 0,
                "cost_total": 0.0,
                "ann_return_delta": 0.0,
                "max_dd_delta": 0.0,
                "sharpe_delta": 0.0,
            }
        )
        for key in ("ann_return", "ann_vol", "sharpe_repo", "max_dd", "final_nav", "rows"):
            baseline_wide[f"{key}_{segment}"] = metrics[key]
        baseline_wide[f"ann_return_delta_{segment}"] = 0.0
        baseline_wide[f"max_dd_delta_{segment}"] = 0.0
        baseline_wide[f"sharpe_delta_{segment}"] = 0.0
        baseline_wide[f"execution_days_{segment}"] = 0
    wide_rows.append(baseline_wide)

    daily_candidates: list[tuple[float, str, pd.DataFrame]] = []
    for ma in MA_GRID:
        for days in DAYS_GRID:
            execution_day = _build_execution_signal(amount, ma, days, pd.DatetimeIndex(nav.index))
            trigger = execution_day.shift(-1, fill_value=False)
            for scale in SCALE_GRID:
                label = f"zz2000_cyb_below_ma{ma}_days{days}_scale{str(scale).replace('.', 'p')}"
                ret, scale_series, overlay_cost = _apply_volume_scale(nav, execution_day, scale)
                wide: dict[str, Any] = {
                    "candidate": label,
                    "version": "v2.5",
                    "family": "zz2000_cyb_below",
                    "ma": ma,
                    "days": days,
                    "scale": scale,
                    "trigger_days": int(trigger.sum()),
                    "execution_days": int(execution_day.sum()),
                    "cost_total_full": float(overlay_cost.sum()),
                }
                for segment, offset in WINDOWS.items():
                    idx = _window_index(nav.index, offset)
                    metrics = _metrics(ret.loc[idx])
                    baseline = baseline_by_window[segment]
                    ann_delta = float(metrics["ann_return"] - baseline["ann_return"])
                    dd_delta = float(metrics["max_dd"] - baseline["max_dd"])
                    sharpe_delta = float(metrics["sharpe_repo"] - baseline["sharpe_repo"])
                    summary_rows.append(
                        {
                            "candidate": label,
                            "version": "v2.5",
                            "family": "zz2000_cyb_below",
                            "ma": ma,
                            "days": days,
                            "scale": scale,
                            "segment": segment,
                            "start": str(idx.min().date()),
                            "end": str(idx.max().date()),
                            **metrics,
                            "trigger_days": int(trigger.reindex(idx).fillna(False).sum()),
                            "execution_days": int(execution_day.reindex(idx).fillna(False).sum()),
                            "cost_total": float(overlay_cost.loc[idx].sum()),
                            "ann_return_delta": ann_delta,
                            "max_dd_delta": dd_delta,
                            "sharpe_delta": sharpe_delta,
                        }
                    )
                    for key in ("ann_return", "ann_vol", "sharpe_repo", "max_dd", "final_nav", "rows"):
                        wide[f"{key}_{segment}"] = metrics[key]
                    wide[f"ann_return_delta_{segment}"] = ann_delta
                    wide[f"max_dd_delta_{segment}"] = dd_delta
                    wide[f"sharpe_delta_{segment}"] = sharpe_delta
                    wide[f"execution_days_{segment}"] = int(execution_day.reindex(idx).fillna(False).sum())
                wide["score"] = _score_candidate(wide)
                wide["robust_pass"] = bool(
                    wide["ann_return_delta_full"] >= 0
                    and wide["ann_return_delta_last_10y"] >= 0
                    and wide["ann_return_delta_last_5y"] >= 0
                    and wide["ann_return_delta_last_3y"] >= 0
                    and wide["max_dd_delta_last_10y"] >= -0.005
                )
                wide["decision_hint"] = "candidate_watch" if wide["robust_pass"] else "reject_or_warning_only"
                wide["stability_label"] = "official_zz2000_cyb_narrow_v25_scan"
                wide_rows.append(wide)

                daily = pd.DataFrame(
                    {
                        "date": nav.index,
                        "candidate": label,
                        "base_return_net": base_ret.values,
                        "candidate_return_net": ret.values,
                        "base_nav_net": (1.0 + base_ret).cumprod().values,
                        "candidate_nav_net": (1.0 + ret).cumprod().values,
                        "volume_execution_day": execution_day.values,
                        "volume_execution_scale": scale_series.values,
                        "volume_overlay_cost": overlay_cost.values,
                    }
                )
                daily_candidates.append((float(wide["score"]), label, daily))

    non_base = [row for row in wide_rows if row["family"] != "baseline"]
    for rank, row in enumerate(sorted(non_base, key=lambda item: item["score"], reverse=True), start=1):
        row["rank_within_version"] = rank
    wide_rows[0]["rank_within_version"] = 0

    summary = pd.DataFrame(summary_rows)
    wide = pd.DataFrame(wide_rows)
    wide["rank_within_version"] = wide["rank_within_version"].fillna(0).astype(int)
    summary.to_csv(run_folder / "scan_summary.csv", index=False, encoding="utf-8")
    wide.sort_values(["rank_within_version", "candidate"]).to_csv(run_folder / "window_metrics.csv", index=False, encoding="utf-8")
    top_daily = pd.concat(
        [daily for _score, _label, daily in sorted(daily_candidates, reverse=True, key=lambda item: item[0])[:20]],
        ignore_index=True,
    )
    top_daily.to_csv(run_folder / "daily_top20_v2_5.csv", index=False, encoding="utf-8")
    amount.to_csv(run_folder / "amount_factors.csv", index_label="date", encoding="utf-8")
    sanity = pd.DataFrame(
        [
            {
                "candidate_count": int(len(wide)),
                "summary_rows": int(len(summary)),
                "baseline_rows": int(wide["family"].eq("baseline").sum()),
                "all_finite_return_metrics": bool(np.isfinite(pd.to_numeric(summary["ann_return"], errors="coerce")).all()),
                "all_required_windows": bool(set(summary["segment"].unique()) == set(WINDOWS)),
                "nav_rows": int(len(nav)),
                "amount_rows": int(len(amount)),
            }
        ]
    )
    sanity.to_csv(run_folder / "sanity_checks.csv", index=False, encoding="utf-8")

    data_snapshot = {
        "reference_summary_latest_nav_date": reference_summary.get("latest_nav_date"),
        "csi2000_code": CSI2000_CODE,
        "csi2000_symbol": CSI2000_SYMBOL,
        "csi2000_name": CSI2000_NAME,
        "csi2000_source": "akshare.stock_zh_index_hist_csindex(symbol='932000')",
        "csi2000_start": str(csi_amount.index.min().date()),
        "csi2000_end": str(csi_amount.index.max().date()),
        "csi2000_rows": int(len(csi_amount)),
        "cyb_source": "akshare.stock_zh_index_daily_tx(symbol='sz399006')",
        "cyb_start": str(cyb_amount.index.min().date()),
        "cyb_end": str(cyb_amount.index.max().date()),
        "cyb_rows": int(len(cyb_amount)),
        "common_start": str(common_start.date()),
        "common_end": str(common_end.date()),
        "nav_rows": int(len(nav)),
        "amount_rows": int(len(amount)),
    }
    return summary, wide, data_snapshot


def _write_record(run_folder: Path, wide: pd.DataFrame, data_snapshot: dict[str, Any], meta: dict[str, Any]) -> None:
    ordered = wide.sort_values("score", ascending=False)
    baseline = wide.loc[wide["candidate"].eq("v2_5_baseline")].iloc[0]
    best = ordered.loc[ordered["family"].eq("zz2000_cyb_below")].iloc[0]
    cols = [
        "candidate",
        "ma",
        "days",
        "scale",
        "score",
        "ann_return_full",
        "max_dd_full",
        "ann_return_delta_full",
        "max_dd_delta_full",
        "ann_return_last_10y",
        "max_dd_last_10y",
        "ann_return_delta_last_10y",
        "max_dd_delta_last_10y",
        "ann_return_last_5y",
        "max_dd_last_5y",
        "ann_return_delta_last_5y",
        "max_dd_delta_last_5y",
        "ann_return_last_3y",
        "max_dd_last_3y",
        "ann_return_delta_last_3y",
        "max_dd_delta_last_3y",
        "ann_return_last_1y",
        "max_dd_last_1y",
        "execution_days_full",
        "cost_total_full",
        "decision_hint",
    ]
    table = ordered.loc[ordered["family"].eq("zz2000_cyb_below"), cols].head(18).to_markdown(index=False, floatfmt=".6f")
    lines = [
        "# Quant Parameter Scan Record",
        "",
        "## Run Metadata",
        "",
        f"- Run id: `{run_folder.name}`",
        f"- Created at: {meta['created_at']}",
        "- Project: microcap Top100",
        "- Strategy or version: v2.5",
        "- Sleeve or subsystem: broad-volume warning overlay",
        "- Parameter group: `zz2000_cyb_amount_ma_days_scale`",
        "- Scan type: narrow_parameter_grid",
        f"- Repo or workspace path: `{ROOT}`",
        "- Target entrypoint: `scripts/run_microcap_v2_5_zz2000_cyb_volume_scan.py`",
        f"- Git branch: `{meta['git_branch']}`",
        f"- Git commit: `{meta['git_commit']}`",
        "- Working tree status before: see `scan_meta.json`; repo was already dirty before this run.",
        "",
        "## Research Question",
        "",
        "- Baseline: formal v2.5 costed NAV.",
        "- Candidate rule: if CSI2000 amount and ChiNext amount are both below their MA for a sustained streak, reduce active v2.5 exposure on the next trading day.",
        f"- Grid: MA `{MA_GRID}`, consecutive days `{DAYS_GRID}`, scale `{SCALE_GRID}`.",
        "- Source-change rule: `research_only_no_source_change`.",
        "- Required windows: full, last_10y, last_5y, last_3y, last_1y.",
        "",
        "## Implementation Anchor",
        "",
        "- Official v2.5 output was regenerated through the v2.5 script before overlay application.",
        "- Overlay is applied to v2.5 `return_net` only for research; no strategy constants are changed.",
        "- The condition at T close affects T+1 return, matching the older broad-volume warning timing.",
        "",
        "## Data Snapshot",
        "",
        f"- v2.5 latest NAV date: {data_snapshot['reference_summary_latest_nav_date']}",
        f"- CSI2000 source/range: {data_snapshot['csi2000_source']}; {data_snapshot['csi2000_start']} to {data_snapshot['csi2000_end']}; rows {data_snapshot['csi2000_rows']}.",
        f"- ChiNext source/range: {data_snapshot['cyb_source']}; {data_snapshot['cyb_start']} to {data_snapshot['cyb_end']}; rows {data_snapshot['cyb_rows']}.",
        f"- Common metrics range: {data_snapshot['common_start']} to {data_snapshot['common_end']}; v2.5 rows {data_snapshot['nav_rows']}; amount rows {data_snapshot['amount_rows']}.",
        "- Amount unit is vendor-specific but moving-average comparisons are unit-invariant.",
        "",
        "## Cost and Execution Assumptions",
        "",
        "- Base stream is v2.5 `return_net`, including Top100 basket costs, target-vol scale-change cost, financing, and idle-cash treatment.",
        f"- Added overlay switching cost: `{SCALE_CHANGE_COST} * abs(volume_scale_delta)` only on active-exposure rows.",
        "- Existing full-cash days are unchanged; the warning overlay only scales active v2.5 exposure.",
        "- No open-impact or extra slippage is added beyond the overlay scale-change cost.",
        "",
        "## Commands",
        "",
        "```powershell",
        f"python scripts/run_microcap_v2_5_zz2000_cyb_volume_scan.py --run-folder {run_folder}",
        f"python C:\\Users\\Administrator.DESKTOP-95I7VVU\\.codex\\skills\\quant-param-scan\\scripts\\finalize_quant_param_scan_run.py {run_folder} --decision research_only_compare_zz2000_cyb_volume_on_v25 --stability-label official_zz2000_cyb_narrow_v25_scan",
        f"python C:\\Users\\Administrator.DESKTOP-95I7VVU\\.codex\\skills\\quant-param-scan\\scripts\\check_quant_param_scan_artifacts.py --phase complete --strict {run_folder}",
        "```",
        "",
        "## Output Files",
        "",
        "- `scan_summary.csv`: long-form metrics by candidate/window.",
        "- `window_metrics.csv`: wide ranked candidate table.",
        "- `daily_top20_v2_5.csv`: daily paths for top 20 candidates by score.",
        "- `amount_factors.csv`: aligned CSI2000 and ChiNext amount factors.",
        "- `sanity_checks.csv`: row and finite-metric checks.",
        "",
        "## Full-Sample Results",
        "",
        f"- Baseline annual return {baseline['ann_return_full']:.4%}, max drawdown {baseline['max_dd_full']:.4%}, Sharpe {baseline['sharpe_repo_full']:.3f}.",
        f"- Best candidate `{best['candidate']}` annual return {best['ann_return_full']:.4%} (delta {best['ann_return_delta_full']:.4%}), max drawdown {best['max_dd_full']:.4%} (delta {best['max_dd_delta_full']:.4%}).",
        "",
        "## Window Results",
        "",
        table,
        "",
        "## Stability Classification",
        "",
        "- Label: official_zz2000_cyb_narrow_v25_scan.",
        "- Robust-pass flag requires non-negative full/10Y/5Y/3Y annual-return deltas and no worse than -0.5pp 10Y max-drawdown delta.",
        "- Treat this as a warning/research overlay until it is compared against other environment filters and live display requirements.",
        "",
        "## Decision",
        "",
        "- Decision: research_only_compare_zz2000_cyb_volume_on_v25.",
        "- Recommended next action: inspect whether the best cells form a stable local plateau before considering warning-board or production routing.",
    ]
    (run_folder / "record.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(run_folder: Path) -> None:
    started = time.time()
    run_folder.mkdir(parents=True, exist_ok=True)
    command_log = run_folder / "command_log.txt"
    with command_log.open("a", encoding="utf-8") as fh:
        fh.write(f"\n[{pd.Timestamp.now().isoformat()}] python scripts/run_microcap_v2_5_zz2000_cyb_volume_scan.py --run-folder {run_folder}\n")
    summary, wide, data_snapshot = _scan(run_folder)
    meta = {
        "run_id": run_folder.name,
        "created_at": pd.Timestamp.now().isoformat(),
        "phase": "analysis_written",
        "project": "microcap Top100",
        "strategy": "v2.5",
        "subsystem": "broad-volume warning overlay",
        "parameter_group": "zz2000_cyb_amount_ma_days_scale",
        "repo_root": str(ROOT),
        "entrypoint": "scripts/run_microcap_v2_5_zz2000_cyb_volume_scan.py",
        "git_branch": _git(["branch", "--show-current"]),
        "git_commit": _git(["rev-parse", "HEAD"]),
        "git_status_before": _git(["status", "--short"]),
        "scan_type": "narrow_parameter_grid",
        "candidate_grid": {
            "family": "zz2000_cyb_below",
            "ma": MA_GRID,
            "days": DAYS_GRID,
            "scale": SCALE_GRID,
            "candidate_count_excluding_baseline": int(len(MA_GRID) * len(DAYS_GRID) * len(SCALE_GRID)),
        },
        "baseline": {
            "candidate": "v2_5_baseline",
            "entry_threshold": v25.ENTRY_THRESHOLD,
            "exit_threshold": v25.EXIT_THRESHOLD,
            "target_vol": v25.TARGET_VOL,
            "max_leverage": v25.TARGET_VOL_MAX_LEVERAGE,
            "scale_rebalance_threshold": v25.TARGET_VOL_SCALE_REBALANCE_THRESHOLD,
        },
        "data_snapshot": data_snapshot,
        "cost_model": {
            "base": "formal_v2_5_costed_return_net",
            "overlay_scale_change_cost": SCALE_CHANGE_COST,
            "execution_timing": "T_close_condition_affects_T_plus_1_return",
            "open_impact": "not_added",
        },
        "outputs": {
            "record": str(run_folder / "record.md"),
            "scan_summary": str(run_folder / "scan_summary.csv"),
            "window_metrics": str(run_folder / "window_metrics.csv"),
            "scan_meta": str(run_folder / "scan_meta.json"),
            "command_log": str(command_log),
            "daily_top20_v2_5": str(run_folder / "daily_top20_v2_5.csv"),
            "amount_factors": str(run_folder / "amount_factors.csv"),
            "sanity_checks": str(run_folder / "sanity_checks.csv"),
        },
        "decision": "research_only_compare_zz2000_cyb_volume_on_v25",
        "stability_label": "official_zz2000_cyb_narrow_v25_scan",
        "git_status_after": _git(["status", "--short"]),
        "elapsed_sec": round(time.time() - started, 3),
        "result_rows": {"scan_summary": int(len(summary)), "window_metrics": int(len(wide))},
    }
    _write_json(run_folder / "scan_meta.json", meta)
    _write_record(run_folder, wide, data_snapshot, meta)
    print(run_folder)
    print(f"summary_rows={len(summary)} window_rows={len(wide)} elapsed_sec={meta['elapsed_sec']}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-folder", type=Path, default=RUN_FOLDER)
    args = parser.parse_args()
    run(args.run_folder)


if __name__ == "__main__":
    main()
