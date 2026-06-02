from __future__ import annotations

import json
import math
import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import microcap_top100_mom16_biweekly_live_v2_3 as v23  # noqa: E402
import microcap_top100_mom16_biweekly_live_v2_5 as v25  # noqa: E402


RUN_FOLDER = ROOT / "quant_param_scan_runs" / "20260602_microcap_top100_v2_3_v2_5_target_vol_overlay_target_vol_maxlev1"
BASE_COSTED_NAV = ROOT / "outputs" / "microcap_top100_mom16_hedge_zz1000_0p8x_biweekly_thursday_16y_v2_0_base_costed_nav.csv"
TURNOVER_CSV = ROOT / "outputs" / "microcap_top100_mom16_biweekly_live_v2_0_base_proxy_turnover.csv"
MAX_LEVERAGE = 1.0
TARGET_VOLS = [round(x / 100.0, 2) for x in range(10, 42, 2)]
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


@contextmanager
def _patched_attr(obj: object, name: str, value: object) -> Iterator[None]:
    old = getattr(obj, name)
    setattr(obj, name, value)
    try:
        yield
    finally:
        setattr(obj, name, old)


@contextmanager
def _patched_function_global(func: object, name: str, value: object) -> Iterator[None]:
    globals_dict = getattr(func, "__globals__")
    old = globals_dict[name]
    globals_dict[name] = value
    try:
        yield
    finally:
        globals_dict[name] = old


def _load_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DatetimeIndex]:
    base = pd.read_csv(BASE_COSTED_NAV, parse_dates=["date"]).sort_values("date").set_index("date")
    required = {"microcap_close", "hedge_close"}
    missing = required - set(base.columns)
    if missing:
        raise RuntimeError(f"base costed NAV missing columns: {sorted(missing)}")
    close_df = base[["microcap_close", "hedge_close"]].rename(
        columns={"microcap_close": "microcap", "hedge_close": "hedge"}
    )
    turnover = pd.read_csv(TURNOVER_CSV)
    if "rebalance_date" not in turnover.columns:
        raise RuntimeError(f"turnover CSV missing rebalance_date: {TURNOVER_CSV}")
    turnover["rebalance_date"] = pd.to_datetime(turnover["rebalance_date"], errors="coerce")
    turnover = turnover.dropna(subset=["rebalance_date"]).sort_values("rebalance_date")
    return close_df, turnover, pd.DatetimeIndex(base.index)


def _build_v23_costed(close_df: pd.DataFrame, turnover: pd.DataFrame, official_index: pd.DatetimeIndex) -> pd.DataFrame:
    common_index = v23.build_v2_3_common_index(close_df, official_index)
    gross = v23.build_spread_log_wls_gross(close_df, common_index)
    buffered = v23.v2_0.base_mod.apply_momentum_gap_exit_buffer(gross, v23.MOMENTUM_GAP_EXIT_BUFFER)
    return v23.v2_0.base_mod.apply_momentum_gap_no_peak_decay_cost_model(buffered, turnover)


def _build_v25_costed(close_df: pd.DataFrame, turnover: pd.DataFrame, official_index: pd.DatetimeIndex) -> pd.DataFrame:
    common_index = v25.build_v2_5_common_index(close_df, official_index)
    gross = v25.build_microcap_log_wls_gross(close_df, common_index)
    return v25.apply_cost(gross, turnover)


def _apply_target_vol(version: str, costed: pd.DataFrame, target_vol: float) -> pd.DataFrame:
    if version == "v2.3":
        with _patched_function_global(v23.v2_0.overlay_mod.apply_target_vol_scaling, "TARGET_VOL_MAX_LEVERAGE", MAX_LEVERAGE):
            out = v23.apply_target_vol(costed, target_vol)
    elif version == "v2.5":
        with _patched_attr(v25, "TARGET_VOL_MAX_LEVERAGE", MAX_LEVERAGE):
            out = v25.apply_target_vol(costed, target_vol)
    else:
        raise ValueError(version)
    out = out.copy()
    out["scan_version"] = version
    out["scan_target_vol"] = float(target_vol)
    out["scan_max_leverage"] = MAX_LEVERAGE
    return out


def _window_bounds(index: pd.DatetimeIndex) -> dict[str, tuple[pd.Timestamp, pd.Timestamp]]:
    start = pd.Timestamp(index.min())
    end = pd.Timestamp(index.max())
    bounds = {"full": (start, end)}
    for name, offset in WINDOWS.items():
        if offset is None:
            continue
        bounds[name] = (max(start, end - offset), end)
    return bounds


def _metrics(out: pd.DataFrame, window_name: str, start: pd.Timestamp, end: pd.Timestamp) -> dict[str, Any]:
    sub = out.loc[(out.index >= start) & (out.index <= end)].copy()
    ret = pd.to_numeric(sub["return_net"], errors="coerce").dropna().astype(float)
    if ret.empty:
        raise RuntimeError(f"empty return series for {window_name}")
    nav = (1.0 + ret).cumprod()
    span_years = (ret.index[-1] - ret.index[0]).days / 365.25
    ann_return = nav.iloc[-1] ** (1.0 / span_years) - 1.0 if span_years > 0 and nav.iloc[-1] > 0 else np.nan
    ann_vol = ret.std(ddof=1) * math.sqrt(float(v23.TRADING_DAYS)) if len(ret) > 1 else 0.0
    sharpe = ann_return / ann_vol if ann_vol > 0 and math.isfinite(float(ann_vol)) else np.nan
    drawdown = nav / nav.cummax() - 1.0
    scale = pd.to_numeric(sub.get("current_execution_scale", pd.Series(0.0, index=sub.index)), errors="coerce").fillna(0.0)
    active = sub["holding"].astype(str).ne("cash")
    return {
        "window": window_name,
        "start_date": str(ret.index[0].date()),
        "end_date": str(ret.index[-1].date()),
        "rows": int(len(ret)),
        "ann_return": float(ann_return),
        "ann_vol": float(ann_vol),
        "sharpe_repo": float(sharpe),
        "max_dd": float(drawdown.min()),
        "final_nav": float(nav.iloc[-1]),
        "active_rows": int(active.loc[ret.index].sum()),
        "cash_days": int((~active.loc[ret.index]).sum()),
        "avg_execution_scale_active": float(scale.where(active).mean()),
        "max_execution_scale": float(scale.max()),
        "cap_days": int(scale.ge(MAX_LEVERAGE - 1e-12).sum()),
        "sum_scale_change_cost": float(pd.to_numeric(sub.get("scale_change_cost", 0.0), errors="coerce").fillna(0.0).sum()),
        "sum_financing_cost": float(pd.to_numeric(sub.get("financing_cost", 0.0), errors="coerce").fillna(0.0).sum()),
    }


def _rank_full_window(summary: pd.DataFrame) -> pd.DataFrame:
    full = summary.loc[summary["segment"].eq("full")].copy()
    full["rank_sharpe"] = full.groupby("version")["sharpe_repo"].rank(ascending=False, method="min")
    full["rank_ann_return"] = full.groupby("version")["ann_return"].rank(ascending=False, method="min")
    full["rank_calmar"] = full.groupby("version")["calmar"].rank(ascending=False, method="min")
    return full


def main() -> None:
    RUN_FOLDER.mkdir(parents=True, exist_ok=True)
    close_df, turnover, official_index = _load_inputs()
    v23_costed = _build_v23_costed(close_df, turnover, official_index)
    v25_costed = _build_v25_costed(close_df, turnover, official_index)
    costed_by_version = {"v2.3": v23_costed, "v2.5": v25_costed}

    window_rows: list[dict[str, Any]] = []
    daily_outputs: list[pd.DataFrame] = []
    for version, costed in costed_by_version.items():
        for target_vol in TARGET_VOLS:
            out = _apply_target_vol(version, costed, target_vol)
            daily_outputs.append(out.reset_index(names="date"))
            candidate = f"{version}_tv{int(round(target_vol * 100)):02d}_maxlev1"
            for window_name, (start, end) in _window_bounds(pd.DatetimeIndex(out.index)).items():
                row = {
                    "candidate": candidate,
                    "version": version,
                    "target_vol": target_vol,
                    "max_leverage": MAX_LEVERAGE,
                    **_metrics(out, window_name, start, end),
                }
                row["calmar"] = row["ann_return"] / abs(row["max_dd"]) if row["max_dd"] < 0 else np.nan
                window_rows.append(row)

    long_summary = pd.DataFrame(window_rows)
    scan_summary = long_summary.rename(columns={"window": "segment", "start_date": "start", "end_date": "end"})
    full_rank = _rank_full_window(scan_summary)
    scan_summary = scan_summary.merge(
        full_rank[["candidate", "rank_sharpe", "rank_ann_return", "rank_calmar"]],
        on="candidate",
        how="left",
    )

    wide_rows: list[dict[str, Any]] = []
    for candidate, group in scan_summary.groupby("candidate", sort=False):
        first = group.iloc[0]
        row: dict[str, Any] = {
            "candidate": candidate,
            "version": first["version"],
            "target_vol": first["target_vol"],
            "max_leverage": first["max_leverage"],
            "rank_sharpe": first["rank_sharpe"],
            "rank_ann_return": first["rank_ann_return"],
            "rank_calmar": first["rank_calmar"],
        }
        for _, item in group.iterrows():
            segment = str(item["segment"])
            for metric in ["ann_return", "ann_vol", "sharpe_repo", "max_dd", "calmar", "avg_execution_scale_active", "cap_days", "sum_scale_change_cost"]:
                row[f"{metric}_{segment}"] = item[metric]
        wide_rows.append(row)
    window_metrics = pd.DataFrame(wide_rows)

    daily = pd.concat(daily_outputs, ignore_index=True)
    scan_summary.to_csv(RUN_FOLDER / "scan_summary.csv", index=False, encoding="utf-8-sig")
    window_metrics.to_csv(RUN_FOLDER / "window_metrics.csv", index=False, encoding="utf-8-sig")
    daily.to_csv(RUN_FOLDER / "daily_results.csv", index=False, encoding="utf-8-sig")

    meta_path = RUN_FOLDER / "scan_meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.exists() else {}
    meta.update(
        {
            "phase": "complete",
            "scan_type": "v2.3_v2.5_target_vol_scan_with_max_leverage_1",
            "candidate_grid": {
                "versions": ["v2.3", "v2.5"],
                "target_vols": TARGET_VOLS,
                "max_leverage": MAX_LEVERAGE,
                "scale_rebalance_threshold": {
                    "v2.3": float(v23.TARGET_VOL_SCALE_REBALANCE_THRESHOLD),
                    "v2.5": float(v25.TARGET_VOL_SCALE_REBALANCE_THRESHOLD),
                },
            },
            "data_snapshot": {
                "base_costed_nav": BASE_COSTED_NAV,
                "turnover_csv": TURNOVER_CSV,
                "base_start_date": pd.Timestamp(official_index.min()),
                "base_end_date": pd.Timestamp(official_index.max()),
                "base_rows": len(official_index),
                "v2_3_rows": len(v23_costed),
                "v2_3_start_date": pd.Timestamp(v23_costed.index.min()),
                "v2_3_end_date": pd.Timestamp(v23_costed.index.max()),
                "v2_5_rows": len(v25_costed),
                "v2_5_start_date": pd.Timestamp(v25_costed.index.min()),
                "v2_5_end_date": pd.Timestamp(v25_costed.index.max()),
                "note": (
                    "scan uses the current common v2.0 base costed NAV and proxy turnover; "
                    f"base costed NAV ends {pd.Timestamp(official_index.max()).date()}, "
                    f"v2.3/v2.5 recomputed streams end {pd.Timestamp(v23_costed.index.max()).date()}"
                ),
            },
            "cost_model": {
                "return_column": "return_net",
                "v2_3": {
                    "signal": "spread log-WLS, signal hedge 1.0, execution hedge 0.8",
                    "cash_day_yield": float(v23.CASH_DAY_YIELD),
                    "scale_change_cost": float(v23.v2_0.overlay_mod.TARGET_VOL_SCALE_CHANGE_COST),
                    "financing_rate": float(v23.v2_0.overlay_mod.TARGET_VOL_FINANCING_RATE),
                    "max_leverage_override": MAX_LEVERAGE,
                },
                "v2_5": {
                    "signal": "microcap-only log-WLS, no hedge",
                    "cash_day_yield": float(v25.IDLE_CASH_YIELD),
                    "scale_change_entry_cost": float(v25.TARGET_VOL_SCALE_CHANGE_ENTRY_COST),
                    "scale_change_exit_cost": float(v25.TARGET_VOL_SCALE_CHANGE_EXIT_COST),
                    "financing_rate": float(v25.TARGET_VOL_FINANCING_RATE),
                    "max_leverage_override": MAX_LEVERAGE,
                },
            },
            "ranking_rule": "primary: highest full-sample sharpe_repo by version; secondary views: full-sample annual return and Calmar",
            "outputs": {
                **meta.get("outputs", {}),
                "scan_summary": str(RUN_FOLDER / "scan_summary.csv"),
                "window_metrics": str(RUN_FOLDER / "window_metrics.csv"),
                "daily_results": str(RUN_FOLDER / "daily_results.csv"),
                "scan_meta": str(RUN_FOLDER / "scan_meta.json"),
                "record": str(RUN_FOLDER / "record.md"),
                "command_log": str(RUN_FOLDER / "command_log.txt"),
            },
            "git_status_after": _git(["status", "--short"]),
        }
    )
    _write_json(meta_path, meta)

    with (RUN_FOLDER / "command_log.txt").open("a", encoding="utf-8") as fh:
        fh.write("\npython scripts/run_microcap_v2_3_v2_5_maxlev1_target_vol_scan.py\n")

    top = (
        scan_summary.loc[scan_summary["segment"].eq("full")]
        .sort_values(["version", "rank_sharpe", "target_vol"])
        .groupby("version", as_index=False)
        .head(5)
    )
    record = f"""# v2.3/v2.5 Max-Leverage-1 Target-Vol Scan

## Question

Control v2.3 and v2.5 target-vol overlays to `max_leverage = 1.0`, then find the best target-vol setting.

## Inputs

- Base costed NAV: `{BASE_COSTED_NAV.relative_to(ROOT)}`
- Turnover CSV: `{TURNOVER_CSV.relative_to(ROOT)}`
- Common base date range: `{official_index.min().date()}` to `{official_index.max().date()}`
- v2.3 costed signal rows before target-vol scan: `{len(v23_costed)}` (`{v23_costed.index.min().date()}` to `{v23_costed.index.max().date()}`)
- v2.5 costed signal rows before target-vol scan: `{len(v25_costed)}` (`{v25_costed.index.min().date()}` to `{v25_costed.index.max().date()}`)

## Grid

- Versions: `v2.3`, `v2.5`
- Target volatility: `{', '.join(f'{x:.0%}' for x in TARGET_VOLS)}`
- Maximum leverage: `1.0`
- Windows: `full`, `last_10y`, `last_5y`, `last_3y`, `last_1y`

## Ranking

Primary ranking is full-sample `sharpe_repo` within each version. `rank_ann_return` and `rank_calmar` are also preserved in `scan_summary.csv`.

## Top Full-Sample Sharpe Rows

{top[["version", "candidate", "target_vol", "ann_return", "ann_vol", "sharpe_repo", "max_dd", "calmar", "rank_ann_return"]].to_string(index=False)}

## Outputs

- `scan_summary.csv`: long window metrics.
- `window_metrics.csv`: wide candidate metrics.
- `daily_results.csv`: daily return/scale paths for all candidates.
- `scan_meta.json`: data, cost, and ranking metadata.

## Decision

Filled after review of the generated artifacts.
"""
    (RUN_FOLDER / "record.md").write_text(record, encoding="utf-8")
    print(f"wrote {RUN_FOLDER}")
    print(top[["version", "candidate", "target_vol", "ann_return", "ann_vol", "sharpe_repo", "max_dd", "calmar", "rank_ann_return"]].to_string(index=False))


if __name__ == "__main__":
    main()
