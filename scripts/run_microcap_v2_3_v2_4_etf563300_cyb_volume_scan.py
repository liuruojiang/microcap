from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import akshare as ak
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
for path in (ROOT, ROOT / "scripts"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

import microcap_top100_mom16_biweekly_live_v2_3 as v23  # noqa: E402
import microcap_top100_mom16_biweekly_live_v2_4 as v24  # noqa: E402


TRADING_DAYS = 244
SCALE_CHANGE_COST = 0.003
PROXY_SYMBOL = "sh563300"
PROXY_CODE = "563300"
PROXY_NAME = "中证2000ETF华泰柏瑞"
PROXY_LABEL = "etf563300"
PROXY_SOURCE = "etf_sina"
CYB_SYMBOL_TX = "sz399006"
MA_GRID = list(range(30, 81, 5))
DAYS_GRID = list(range(5, 26))
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


def _json_safe(value: object) -> object:
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if not np.isfinite(value) else float(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
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
    ann_return = float(nav.iloc[-1] ** (TRADING_DAYS / rows) - 1.0)
    ann_vol = float(ret.std(ddof=0) * np.sqrt(TRADING_DAYS))
    sharpe = ann_return / ann_vol if ann_vol > 0 else np.nan
    max_dd = float((nav / nav.cummax() - 1.0).min())
    return {
        "rows": rows,
        "ann_return": ann_return,
        "ann_vol": ann_vol,
        "sharpe_repo": float(sharpe),
        "max_dd": max_dd,
        "final_nav": float(nav.iloc[-1]),
    }


def _window_index(index: pd.DatetimeIndex, offset: pd.DateOffset | None) -> pd.DatetimeIndex:
    if offset is None:
        return index
    cutoff = index.max() - offset
    return index[index >= cutoff]


def _load_cyb_amount() -> pd.Series:
    df = ak.stock_zh_index_daily_tx(symbol=CYB_SYMBOL_TX)
    out = (
        df.loc[:, ["date", "amount"]]
        .assign(date=lambda x: pd.to_datetime(x["date"], errors="coerce"))
        .dropna(subset=["date", "amount"])
        .drop_duplicates("date", keep="last")
        .sort_values("date")
        .set_index("date")["amount"]
    )
    return pd.to_numeric(out, errors="coerce").dropna().rename("cyb_amount")


def _load_csi2000_proxy_amount() -> pd.Series:
    if PROXY_SOURCE == "csindex":
        df = ak.stock_zh_index_hist_csindex(symbol=PROXY_CODE, start_date="20100101", end_date=pd.Timestamp.today().strftime("%Y%m%d"))
        if "日期" not in df.columns or "成交金额" not in df.columns:
            raise RuntimeError(f"unexpected CSIndex columns: {df.columns.tolist()}")
        out = (
            df.loc[:, ["日期", "成交金额"]]
            .rename(columns={"日期": "date", "成交金额": "amount"})
            .assign(date=lambda x: pd.to_datetime(x["date"], errors="coerce"))
            .dropna(subset=["date", "amount"])
            .drop_duplicates("date", keep="last")
            .sort_values("date")
            .set_index("date")["amount"]
        )
        return pd.to_numeric(out, errors="coerce").dropna().rename("csi2000_proxy_amount")
    df = ak.fund_etf_hist_sina(symbol=PROXY_SYMBOL)
    if "date" not in df.columns or "amount" not in df.columns:
        raise RuntimeError(f"unexpected ETF proxy columns: {df.columns.tolist()}")
    out = (
        df.loc[:, ["date", "amount"]]
        .assign(date=lambda x: pd.to_datetime(x["date"], errors="coerce"))
        .dropna(subset=["date", "amount"])
        .drop_duplicates("date", keep="last")
        .sort_values("date")
        .set_index("date")["amount"]
    )
    return pd.to_numeric(out, errors="coerce").dropna().rename("csi2000_proxy_amount")


def _strategy_navs() -> dict[str, pd.DataFrame]:
    v23.generate_v2_3_outputs()
    v24.generate_v2_4_outputs()
    return {
        "v2.3": pd.read_csv(v23.COSTED_NAV_CSV, parse_dates=["date"]).sort_values("date").set_index("date"),
        "v2.4": pd.read_csv(v24.COSTED_NAV_CSV, parse_dates=["date"]).sort_values("date").set_index("date"),
    }


def _build_execution_signal(amount: pd.DataFrame, ma: int, days: int, nav_index: pd.DatetimeIndex) -> pd.Series:
    amount = amount.sort_index()
    proxy_below = amount["csi2000_proxy_amount"] < amount["csi2000_proxy_amount"].rolling(ma).mean()
    cyb_below = amount["cyb_amount"] < amount["cyb_amount"].rolling(ma).mean()
    condition = proxy_below.fillna(False) & cyb_below.fillna(False)
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
    scale_change = scale_series.diff().abs().fillna(0.0)
    overlay_cost = scale_change * active_exposure.astype(float) * SCALE_CHANGE_COST
    ret = base_ret * scale_series - overlay_cost
    return ret.rename("return_net"), scale_series.rename("volume_execution_scale"), overlay_cost.rename("volume_overlay_cost")


def _score_candidate(row: dict[str, Any]) -> float:
    return float(
        row["ann_return_delta_last_3y"]
        + row["ann_return_delta_last_1y"]
        + 0.5 * row["ann_return_delta_full"]
        + 0.5 * row["max_dd_delta_full"]
        - max(0.0, -row["ann_return_delta_full"]) * 0.5
    )


def _scan(run_folder: Path) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    csi_amount = _load_csi2000_proxy_amount()
    cyb_amount = _load_cyb_amount()
    amount = pd.concat([csi_amount, cyb_amount], axis=1).dropna().sort_index()
    if amount.empty:
        raise RuntimeError("combined ETF proxy + CYB amount series is empty")
    navs = _strategy_navs()
    common_start = max([df.index.min() for df in navs.values()] + [amount.index.min()])
    common_end = min([df.index.max() for df in navs.values()] + [amount.index.max()])
    navs = {version: df.loc[(df.index >= common_start) & (df.index <= common_end)].copy() for version, df in navs.items()}
    amount = amount.loc[(amount.index >= common_start) & (amount.index <= common_end)].copy()

    summary_rows: list[dict[str, Any]] = []
    wide_rows: list[dict[str, Any]] = []
    daily_outputs: dict[str, pd.DataFrame] = {}

    for version, nav in navs.items():
        base_ret = pd.to_numeric(nav["return_net"], errors="coerce").fillna(0.0)
        baseline_metrics: dict[str, dict[str, float | int]] = {}
        baseline_wide: dict[str, Any] = {
            "candidate": f"{version}__baseline",
            "version": version,
            "family": "baseline",
            "ma": 0,
            "days": 0,
            "scale": 1.0,
            "trigger_days": 0,
            "execution_days": 0,
            "score": 0.0,
            "robust_pass": True,
            "decision_hint": "baseline",
            "stability_label": "official_current",
        }
        for segment, offset in WINDOWS.items():
            idx = _window_index(nav.index, offset)
            metrics = _metrics(base_ret.loc[idx])
            baseline_metrics[segment] = metrics
            summary_rows.append(
                {
                    "candidate": f"{version}__baseline",
                    "version": version,
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
        wide_rows.append(baseline_wide)

        top_daily: list[tuple[str, pd.DataFrame]] = []
        version_wide_start = len(wide_rows)
        for ma in MA_GRID:
            for days in DAYS_GRID:
                execution_day = _build_execution_signal(amount, ma, days, nav.index)
                trigger = execution_day.shift(-1, fill_value=False)
                for scale in SCALE_GRID:
                    family = f"{PROXY_LABEL}_cyb_below"
                    label = f"{version}__{family}_ma{ma}_days{days}_scale{str(scale).replace('.', 'p')}"
                    ret, scale_series, overlay_cost = _apply_volume_scale(nav, execution_day, scale)
                    wide: dict[str, Any] = {
                        "candidate": label,
                        "version": version,
                        "family": family,
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
                        base_metrics = baseline_metrics[segment]
                        ann_delta = float(metrics["ann_return"] - base_metrics["ann_return"])
                        dd_delta = float(metrics["max_dd"] - base_metrics["max_dd"])
                        sharpe_delta = float(metrics["sharpe_repo"] - base_metrics["sharpe_repo"])
                        summary_rows.append(
                            {
                                "candidate": label,
                                "version": version,
                                "family": family,
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
                        and wide["ann_return_delta_last_3y"] >= 0
                        and wide["ann_return_delta_last_1y"] >= 0
                        and wide["max_dd_delta_full"] >= -0.005
                    )
                    wide["decision_hint"] = "candidate_watch" if wide["robust_pass"] else "reject_or_warning_only"
                    wide["stability_label"] = f"{PROXY_LABEL}_etf_proxy_scan"
                    wide_rows.append(wide)
                    if len(top_daily) < 20 or wide["score"] > min(item[1].attrs["score"] for item in top_daily):
                        daily = pd.DataFrame(
                            {
                                "date": nav.index,
                                "version": version,
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
                        daily.attrs["score"] = wide["score"]
                        top_daily.append((label, daily))
                        top_daily = sorted(top_daily, key=lambda item: item[1].attrs["score"], reverse=True)[:20]

        version_wide = sorted(wide_rows[version_wide_start:], key=lambda row: row["score"], reverse=True)
        for rank, row in enumerate(version_wide, start=1):
            row["rank_within_version"] = rank
        daily_outputs[version] = pd.concat([daily for _, daily in top_daily], ignore_index=True)

    summary = pd.DataFrame(summary_rows)
    wide = pd.DataFrame(wide_rows)
    wide["rank_within_version"] = wide["rank_within_version"].fillna(0).astype(int)
    summary.to_csv(run_folder / "scan_summary.csv", index=False, encoding="utf-8")
    wide.sort_values(["version", "rank_within_version", "candidate"]).to_csv(
        run_folder / "window_metrics.csv", index=False, encoding="utf-8"
    )
    for version, daily in daily_outputs.items():
        daily.to_csv(run_folder / f"daily_top20_{version.replace('.', '_')}.csv", index=False, encoding="utf-8")

    data_snapshot = {
        "csi2000_proxy_code": PROXY_CODE,
        "csi2000_proxy_symbol": PROXY_SYMBOL,
        "csi2000_proxy_name": PROXY_NAME,
        "csi2000_proxy_source": (
            f"akshare.stock_zh_index_hist_csindex(symbol='{PROXY_CODE}')"
            if PROXY_SOURCE == "csindex"
            else f"akshare.fund_etf_hist_sina(symbol='{PROXY_SYMBOL}')"
        ),
        "csi2000_proxy_source_kind": PROXY_SOURCE,
        "csi2000_proxy_start": str(csi_amount.index.min().date()),
        "csi2000_proxy_end": str(csi_amount.index.max().date()),
        "csi2000_proxy_rows": int(len(csi_amount)),
        "cyb_source": "akshare.stock_zh_index_daily_tx(symbol='sz399006') Tencent daily amount",
        "cyb_start": str(cyb_amount.index.min().date()),
        "cyb_end": str(cyb_amount.index.max().date()),
        "cyb_rows": int(len(cyb_amount)),
        "common_start": str(common_start.date()),
        "common_end": str(common_end.date()),
        "common_amount_rows": int(len(amount)),
        "strategy_rows": {version: int(len(df)) for version, df in navs.items()},
    }
    return summary, wide, data_snapshot


def _write_record(run_folder: Path, wide: pd.DataFrame, data_snapshot: dict[str, Any]) -> None:
    family = f"{PROXY_LABEL}_cyb_below"
    top = wide[wide["family"].eq(family)].sort_values(["version", "score"], ascending=[True, False])
    lines = [
        "# Quant Parameter Scan Record",
        "",
        "## Run Metadata",
        "",
        f"- Run id: `{run_folder.name}`",
        f"- Created at: {pd.Timestamp.now().isoformat()}",
        "- Project: microcap Top100",
        "- Strategy or version: v2.3 and v2.4",
        "- Sleeve or subsystem: broad-volume warning overlay",
        f"- Parameter group: `{PROXY_LABEL}_cyb_amount_ma_days_scale`",
        "- Scan type: parameter_grid",
        f"- Repo or workspace path: `{ROOT}`",
        "- Target entrypoints: `microcap_top100_mom16_biweekly_live_v2_3.py`, `microcap_top100_mom16_biweekly_live_v2_4.py`",
        f"- Git branch: `{_git(['branch', '--show-current'])}`",
        f"- Git commit: `{_git(['rev-parse', 'HEAD'])}`",
        "- Working tree status before: see `scan_meta.json`; repo was already dirty before this scan.",
        "",
        "## Research Question",
        "",
        f"- Does using `{PROXY_SYMBOL}` / `{PROXY_CODE}` amount recover a useful broad-volume warning for v2.3/v2.4?",
        "- Grid: ETF proxy amount and CYB amount both below their moving averages, consecutive-day confirmation, and exposure scale.",
        "- Source-change rule: `research_only_no_strategy_source_change`.",
        "- Default/current production parameter included: baseline rows for both versions.",
        "",
        "## Implementation Anchor",
        "",
        "- Rebuilt official v2.3 and v2.4 costed NAVs with the real strategy scripts.",
        "- Overlay is applied to official `return_net` only for research; no strategy constants are changed.",
        f"- Broad-volume amount source selected for this run: `{PROXY_SYMBOL}` / `{PROXY_CODE}` / {PROXY_NAME}; source kind `{PROXY_SOURCE}`.",
        "",
        "## Data Snapshot",
        "",
        f"- Broad-volume source: `{data_snapshot['csi2000_proxy_symbol']}` {data_snapshot['csi2000_proxy_name']}.",
        f"- Proxy source/range: {data_snapshot['csi2000_proxy_source']}; {data_snapshot['csi2000_proxy_start']} to {data_snapshot['csi2000_proxy_end']}; rows {data_snapshot['csi2000_proxy_rows']}.",
        f"- CYB source/range: {data_snapshot['cyb_source']}; {data_snapshot['cyb_start']} to {data_snapshot['cyb_end']}; rows {data_snapshot['cyb_rows']}.",
        f"- Common strategy date range: {data_snapshot['common_start']} to {data_snapshot['common_end']}; rows {data_snapshot['strategy_rows']}.",
        "- Window caveat: all windows are constrained by the ETF proxy start date when the proxy has shorter history than the strategy.",
        "- Market/session assumption: T close broad-volume condition affects the next trading day.",
        "- Annualization: 244 trading days.",
        "",
        "## Cost and Execution Assumptions",
        "",
        "- Candidate exposure scale applies only when the base strategy has non-zero current execution exposure.",
        "- Existing cash days keep their base cash return; the warning does not reduce idle-cash yield.",
        f"- Extra switching cost: `{SCALE_CHANGE_COST} * abs(volume_scale_delta)` on active-exposure rows.",
        "- Base strategy `return_net` already includes the official transaction, target-vol, financing, and cash-yield assumptions.",
        "",
        "## Runtime Override Plan",
        "",
        "- No runtime override is promoted.",
        "- This is a post-run research overlay for warning-rule evaluation.",
        "",
        "## Commands",
        "",
        "```powershell",
        "python microcap_top100_mom16_biweekly_live_v2_3.py",
        "python microcap_top100_mom16_biweekly_live_v2_4.py",
        f"python scripts/run_microcap_v2_3_v2_4_etf563300_cyb_volume_scan.py --run-folder {run_folder} --proxy-symbol {PROXY_SYMBOL} --proxy-code {PROXY_CODE} --proxy-name \"{PROXY_NAME}\" --proxy-label {PROXY_LABEL} --proxy-source {PROXY_SOURCE}",
        "```",
        "",
        "## Output Files",
        "",
        "- `scan_summary.csv`: long-form metrics.",
        "- `window_metrics.csv`: wide ranked comparison table.",
        "- `daily_top20_v2_3.csv`, `daily_top20_v2_4.csv`: daily paths for top candidates.",
        "",
        "## Full-Sample Results",
        "",
    ]
    for version in ["v2.3", "v2.4"]:
        base = wide[wide["candidate"].eq(f"{version}__baseline")].iloc[0]
        best = top[top["version"].eq(version)].iloc[0]
        lines.append(
            f"- {version} baseline: annual {base['ann_return_full']:.4%}, maxDD {base['max_dd_full']:.4%}; "
            f"best proxy candidate `{best['candidate']}` annual {best['ann_return_full']:.4%} "
            f"(delta {best['ann_return_delta_full']:.4%}), maxDD {best['max_dd_full']:.4%} "
            f"(delta {best['max_dd_delta_full']:.4%})."
        )
    lines += [
        "",
        "## Window Results",
        "",
    ]
    for version in ["v2.3", "v2.4"]:
        best = top[top["version"].eq(version)].iloc[0]
        lines.append(
            f"- {version} best full/3Y/1Y ann deltas: "
            f"{best['ann_return_delta_full']:.4%}, {best['ann_return_delta_last_3y']:.4%}, "
            f"{best['ann_return_delta_last_1y']:.4%}; "
            f"full/1Y maxDD deltas: {best['max_dd_delta_full']:.4%}, {best['max_dd_delta_last_1y']:.4%}."
        )
    lines += [
        "",
        "## Stability Classification",
        "",
        f"- Label: {PROXY_LABEL}_etf_proxy_scan.",
        "- Robust pass requires non-negative full/3Y/1Y annual-return deltas and no worse than -0.5pp full max-drawdown delta.",
        "- Caveat: when source kind is `csindex`, 成交金额 uses the unit published by 中证指数官网; moving-average comparisons are unit-invariant.",
        "",
        "## Decision",
        "",
        "- Decision: use ETF proxy scan as research evidence; do not promote to production unless the result is stable across windows and versions.",
        "",
        "## User-Facing Summary",
        "",
        "- The scan tests whether broad small-cap amount plus ChiNext amount can define a useful volume warning.",
    ]
    (run_folder / "record.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(run_folder: Path) -> None:
    started = time.time()
    run_folder.mkdir(parents=True, exist_ok=True)
    with (run_folder / "command_log.txt").open("a", encoding="utf-8") as fh:
        fh.write(
            f"\n[{pd.Timestamp.now().isoformat()}] "
            f"python scripts/run_microcap_v2_3_v2_4_etf563300_cyb_volume_scan.py "
            f"--run-folder {run_folder} --proxy-symbol {PROXY_SYMBOL} "
            f"--proxy-code {PROXY_CODE} --proxy-name \"{PROXY_NAME}\" "
            f"--proxy-label {PROXY_LABEL} --proxy-source {PROXY_SOURCE}\n"
        )
    summary, wide, data_snapshot = _scan(run_folder)
    _write_record(run_folder, wide, data_snapshot)
    meta_path = run_folder / "scan_meta.json"
    existing = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.exists() else {}
    existing.update(
        {
            "phase": "analysis_written",
            "scan_type": "parameter_grid",
            "parameter_group": f"{PROXY_LABEL}_cyb_amount_ma_days_scale",
            "baseline": {
                "v2.3": "official v2.3 return_net",
                "v2.4": "official v2.4 return_net",
            },
            "candidate_grid": {
                "family": f"{PROXY_LABEL}_cyb_below",
                "ma": MA_GRID,
                "days": DAYS_GRID,
                "scale": SCALE_GRID,
                "candidate_count_excluding_baseline": int(len(MA_GRID) * len(DAYS_GRID) * len(SCALE_GRID) * 2),
            },
            "data_snapshot": data_snapshot,
            "cost_model": {
                "base": "official costed return_net for each version",
                "volume_scale_change_cost": SCALE_CHANGE_COST,
                "volume_timing": "T close condition affects T+1 return",
                "cash_days": "candidate scale applies only when base current_execution_scale > 0",
            },
            "outputs": {
                "record": str(run_folder / "record.md"),
                "scan_summary": str(run_folder / "scan_summary.csv"),
                "window_metrics": str(run_folder / "window_metrics.csv"),
                "scan_meta": str(run_folder / "scan_meta.json"),
                "command_log": str(run_folder / "command_log.txt"),
                "daily_top20_v2_3": str(run_folder / "daily_top20_v2_3.csv"),
                "daily_top20_v2_4": str(run_folder / "daily_top20_v2_4.csv"),
            },
            "git_branch": _git(["branch", "--show-current"]),
            "git_commit": _git(["rev-parse", "HEAD"]),
            "git_status_after": _git(["status", "--short"]),
            "decision": "etf_proxy_warning_only",
            "stability_label": f"{PROXY_LABEL}_etf_proxy_scan",
            "runtime_seconds": round(time.time() - started, 3),
            "result_rows": {"scan_summary": int(len(summary)), "window_metrics": int(len(wide))},
        }
    )
    _write_json(meta_path, existing)


def main() -> None:
    global PROXY_CODE, PROXY_LABEL, PROXY_NAME, PROXY_SOURCE, PROXY_SYMBOL
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-folder", type=Path, required=True)
    parser.add_argument("--proxy-symbol", default=PROXY_SYMBOL)
    parser.add_argument("--proxy-code", default=PROXY_CODE)
    parser.add_argument("--proxy-name", default=PROXY_NAME)
    parser.add_argument("--proxy-label", default=None)
    parser.add_argument("--proxy-source", choices=["etf_sina", "csindex"], default=PROXY_SOURCE)
    args = parser.parse_args()
    PROXY_SYMBOL = args.proxy_symbol
    PROXY_CODE = args.proxy_code
    PROXY_NAME = args.proxy_name
    PROXY_SOURCE = args.proxy_source
    PROXY_LABEL = args.proxy_label or f"etf{PROXY_CODE}"
    run(args.run_folder)


if __name__ == "__main__":
    main()
