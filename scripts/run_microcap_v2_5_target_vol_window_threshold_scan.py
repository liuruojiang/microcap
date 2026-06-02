from __future__ import annotations

import json
import math
import subprocess
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import microcap_top100_mom16_biweekly_live_v2_5 as v25  # noqa: E402


RUN_FOLDER = ROOT / "quant_param_scan_runs" / "20260601_microcap_top100_v2_5_target_vol_overlay_vol_window_method_scale_threshold"
SOURCE_NAV = ROOT / "outputs" / "microcap_top100_mom16_biweekly_live_v2_5_scan_preflight_20260601_costed_nav.csv"
TARGET_VOL = 0.30
TRADING_DAYS = int(v25.TRADING_DAYS)
WINDOWS = {
    "full": None,
    "last_10y": pd.DateOffset(years=10),
    "last_5y": pd.DateOffset(years=5),
    "last_3y": pd.DateOffset(years=3),
    "last_1y": pd.DateOffset(years=1),
}
CANDIDATES = [
    {"candidate": "official_roll60_thr030", "method": "rolling", "window": 60, "threshold": 0.30},
    {"candidate": "roll40_thr030", "method": "rolling", "window": 40, "threshold": 0.30},
    {"candidate": "roll20_thr030", "method": "rolling", "window": 20, "threshold": 0.30},
    {"candidate": "ewma20_thr030", "method": "ewma", "window": 20, "threshold": 0.30},
    {"candidate": "official_roll60_thr020", "method": "rolling", "window": 60, "threshold": 0.20},
    {"candidate": "official_roll60_thr010", "method": "rolling", "window": 60, "threshold": 0.10},
    {"candidate": "official_roll60_thr000", "method": "rolling", "window": 60, "threshold": 0.00},
    {"candidate": "roll40_thr020", "method": "rolling", "window": 40, "threshold": 0.20},
    {"candidate": "ewma20_thr020", "method": "ewma", "window": 20, "threshold": 0.20},
]


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


def _git(args: list[str]) -> str:
    try:
        return subprocess.check_output(["git", *args], cwd=ROOT, text=True, stderr=subprocess.STDOUT).strip()
    except Exception as exc:  # noqa: BLE001
        return f"git_error:{exc}"


def _realized_vol(ret: pd.Series, method: str, window: int) -> pd.Series:
    clean = pd.to_numeric(ret, errors="coerce").replace([np.inf, -np.inf], np.nan)
    if method == "rolling":
        return clean.rolling(window, min_periods=window).std(ddof=1) * math.sqrt(TRADING_DAYS)
    if method == "ewma":
        return clean.ewm(halflife=window, min_periods=window, adjust=False).std(bias=False) * math.sqrt(TRADING_DAYS)
    raise ValueError(f"unsupported method: {method}")


def _apply_threshold(desired_scale: pd.Series, active: pd.Series, threshold: float) -> pd.Series:
    desired = pd.to_numeric(desired_scale, errors="coerce").fillna(1.0)
    active = active.fillna(False).astype(bool)
    values: list[float] = []
    last_scale = 0.0
    for is_active, target in zip(active.tolist(), desired.tolist(), strict=True):
        if not is_active:
            last_scale = 0.0
            values.append(0.0)
            continue
        target = float(target)
        if last_scale <= 1e-12 or abs(target - last_scale) >= float(threshold):
            last_scale = target
        values.append(float(last_scale))
    return pd.Series(values, index=desired.index, dtype=float)


def _next_actionable_scale(current_execution_scale: pd.Series, next_session_target_scale: pd.Series, next_holding: pd.Series, threshold: float) -> pd.Series:
    current = pd.to_numeric(current_execution_scale, errors="coerce").fillna(0.0)
    target = pd.to_numeric(next_session_target_scale, errors="coerce").fillna(current)
    next_active = next_holding.fillna("cash").astype(str).ne("cash")
    rebalance = target.sub(current).abs().ge(float(threshold))
    return target.where(next_active & rebalance, current.where(next_active, 0.0)).clip(lower=0.0)


def _microcap_turnover_series(holding: pd.Series, execution_scale: pd.Series) -> pd.Series:
    holding = holding.fillna("cash").astype(str)
    scale = pd.to_numeric(execution_scale, errors="coerce").fillna(0.0)
    leg = scale.where(holding.ne("cash"), 0.0)
    return leg.sub(leg.shift(1).fillna(0.0)).abs().astype(float)


def _scale_change_cost(holding: pd.Series, execution_scale: pd.Series) -> pd.Series:
    holding = holding.fillna("cash").astype(str)
    scale = pd.to_numeric(execution_scale, errors="coerce").fillna(0.0)
    same_holding = holding.eq(holding.shift(1))
    scale_delta = scale.sub(scale.shift(1).fillna(0.0))
    return scale_delta.abs().where(same_holding, 0.0).mul(float(v25.TARGET_VOL_SCALE_CHANGE_COST)).astype(float)


def _base_trade_cost_scale(
    holding: pd.Series,
    next_holding: pd.Series,
    current_execution_scale: pd.Series,
    next_session_actionable_scale: pd.Series,
) -> pd.Series:
    holding = holding.fillna("cash").astype(str)
    next_holding = next_holding.fillna(holding).astype(str)
    current = pd.to_numeric(current_execution_scale, errors="coerce").fillna(0.0)
    actionable = pd.to_numeric(next_session_actionable_scale, errors="coerce").fillna(current)
    scale = pd.Series(0.0, index=holding.index, dtype=float)
    current_active = holding.ne("cash")
    next_active = next_holding.ne("cash")
    scale.loc[~current_active & next_active] = actionable.loc[~current_active & next_active]
    scale.loc[current_active] = current.loc[current_active]
    return scale.clip(lower=0.0)


def apply_variant(base: pd.DataFrame, *, candidate: str, method: str, window: int, threshold: float) -> pd.DataFrame:
    out = base.copy().sort_index()
    holding = out["holding"].fillna("cash").astype(str)
    next_holding = out["next_holding"].fillna(holding).astype(str)
    active = holding.ne("cash")
    base_trade_cost = pd.to_numeric(out["base_trade_cost"], errors="coerce").fillna(0.0)
    base_pre_cost_return = pd.to_numeric(out["base_pre_cost_return"], errors="coerce").fillna(0.0)
    target_vol_return = pd.to_numeric(out["microcap_ret"], errors="coerce").replace([np.inf, -np.inf], np.nan)
    realized_vol = _realized_vol(target_vol_return, method, int(window))
    raw_scale = (TARGET_VOL / realized_vol.replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan).clip(
        lower=float(v25.TARGET_VOL_MIN_LEVERAGE),
        upper=float(v25.TARGET_VOL_MAX_LEVERAGE),
    )
    target_execution_scale = raw_scale.shift(1).fillna(1.0)
    execution_scale = _apply_threshold(target_execution_scale, active, float(threshold))
    next_session_target_scale = raw_scale.copy()
    next_session_target_scale.loc[next_holding.eq("cash")] = 0.0
    next_session_target_scale.loc[next_holding.ne("cash")] = next_session_target_scale.loc[next_holding.ne("cash")].fillna(1.0)
    next_session_target_scale = next_session_target_scale.fillna(0.0)
    next_session_actionable_scale = _next_actionable_scale(execution_scale, next_session_target_scale, next_holding, float(threshold))
    scale_change_cost = _scale_change_cost(holding, execution_scale)
    financing_cost = execution_scale.sub(1.0).clip(lower=0.0) * float(v25.TARGET_VOL_FINANCING_RATE) / TRADING_DAYS
    idle_cash_yield = active.astype(float) * execution_scale.rsub(1.0).clip(lower=0.0, upper=1.0) * float(v25.IDLE_CASH_YIELD) / TRADING_DAYS
    cash_day_yield = active.astype(float).rsub(1.0) * float(v25.IDLE_CASH_YIELD) / TRADING_DAYS
    base_cost_scale = _base_trade_cost_scale(holding, next_holding, execution_scale, next_session_actionable_scale)
    base_trade_cost_scaled = (base_trade_cost * base_cost_scale).clip(lower=0.0, upper=0.99)
    ret = (
        (1.0 + base_pre_cost_return * execution_scale + idle_cash_yield + cash_day_yield)
        * (1.0 - base_trade_cost_scaled)
        * (1.0 - scale_change_cost)
        * (1.0 - financing_cost)
        - 1.0
    )
    out["candidate"] = candidate
    out["vol_method"] = method
    out["vol_window"] = int(window)
    out["scale_rebalance_threshold"] = float(threshold)
    out["target_vol_realized_vol"] = realized_vol
    out["target_vol_scale_raw"] = raw_scale
    out["target_vol_execution_scale_raw"] = target_execution_scale
    out["current_execution_scale"] = execution_scale
    out["next_session_target_scale"] = next_session_target_scale
    out["next_session_actionable_scale"] = next_session_actionable_scale
    out["target_vol_turnover"] = _microcap_turnover_series(holding, execution_scale)
    out["scale_change_cost"] = scale_change_cost
    out["financing_cost"] = financing_cost
    out["idle_cash_yield"] = idle_cash_yield
    out["cash_day_yield"] = cash_day_yield
    out["base_trade_cost_scaled"] = base_trade_cost_scaled
    out["scaled_pre_cost_return"] = base_pre_cost_return * execution_scale + idle_cash_yield + cash_day_yield
    out["return_net"] = ret
    out["nav_net"] = (1.0 + ret.fillna(0.0)).cumprod()
    return out


def _metrics(ret: pd.Series) -> dict[str, float | int]:
    r = pd.to_numeric(ret, errors="coerce").fillna(0.0)
    rows = int(len(r))
    if rows == 0:
        return {"rows": 0, "ann_return": np.nan, "ann_vol": np.nan, "sharpe_repo": np.nan, "max_dd": np.nan, "final_nav": np.nan}
    nav = (1.0 + r).cumprod()
    ann_return = float(nav.iloc[-1] ** (TRADING_DAYS / rows) - 1.0) if nav.iloc[-1] > 0 else np.nan
    ann_vol = float(r.std(ddof=1) * math.sqrt(TRADING_DAYS)) if rows > 1 else 0.0
    sharpe = ann_return / ann_vol if ann_vol > 0 and math.isfinite(ann_vol) else np.nan
    dd = nav.div(nav.cummax()).sub(1.0)
    return {"rows": rows, "ann_return": ann_return, "ann_vol": ann_vol, "sharpe_repo": float(sharpe), "max_dd": float(dd.min()), "final_nav": float(nav.iloc[-1])}


def _window_slices(index: pd.DatetimeIndex) -> dict[str, tuple[pd.Timestamp, pd.Timestamp]]:
    end = pd.Timestamp(index.max())
    start = pd.Timestamp(index.min())
    out = {"full": (start, end)}
    for name, offset in WINDOWS.items():
        if name == "full" or offset is None:
            continue
        out[name] = (max(start, end - offset), end)
    return out


def _window_metrics(out: pd.DataFrame, candidate_cfg: dict[str, Any], window_name: str, start: pd.Timestamp, end: pd.Timestamp) -> dict[str, Any]:
    sub = out.loc[(out.index >= start) & (out.index <= end)].copy()
    active = sub["holding"].astype(str).ne("cash")
    active_rows = int(active.sum())
    metrics = _metrics(sub["return_net"])
    scaled_component = pd.to_numeric(sub.loc[active, "scaled_pre_cost_return"], errors="coerce").fillna(0.0)
    active_ann_vol = float(scaled_component.std(ddof=1) * math.sqrt(TRADING_DAYS)) if len(scaled_component) > 1 else np.nan
    desired = pd.to_numeric(sub["target_vol_execution_scale_raw"], errors="coerce")
    actual = pd.to_numeric(sub["current_execution_scale"], errors="coerce")
    scale_gap = (actual - desired).abs().where(active)
    scale_delta = actual.sub(actual.shift(1).fillna(0.0)).abs().where(active, 0.0)
    row: dict[str, Any] = {
        "candidate": candidate_cfg["candidate"],
        "method": candidate_cfg["method"],
        "vol_window": candidate_cfg["window"],
        "threshold": candidate_cfg["threshold"],
        "window": window_name,
        "start_date": str(start.date()),
        "end_date": str(end.date()),
        **metrics,
        "target_vol": TARGET_VOL,
        "ann_vol_gap": float(metrics["ann_vol"] - TARGET_VOL) if math.isfinite(float(metrics["ann_vol"])) else np.nan,
        "ann_vol_abs_gap": abs(float(metrics["ann_vol"] - TARGET_VOL)) if math.isfinite(float(metrics["ann_vol"])) else np.nan,
        "active_rows": active_rows,
        "active_share": active_rows / len(sub) if len(sub) else np.nan,
        "active_scaled_ann_vol": active_ann_vol,
        "active_scaled_ann_vol_gap": active_ann_vol - TARGET_VOL if math.isfinite(active_ann_vol) else np.nan,
        "avg_execution_scale_active": float(actual.where(active).mean()),
        "avg_raw_scale_active": float(desired.where(active).mean()),
        "mean_abs_scale_gap_active": float(scale_gap.mean()),
        "p95_abs_scale_gap_active": float(scale_gap.quantile(0.95)),
        "scale_change_events": int((scale_delta > 1e-12).sum()),
        "sum_abs_scale_delta": float(scale_delta.sum()),
        "sum_scale_change_cost": float(pd.to_numeric(sub["scale_change_cost"], errors="coerce").fillna(0.0).sum()),
        "sum_base_trade_cost_scaled": float(pd.to_numeric(sub["base_trade_cost_scaled"], errors="coerce").fillna(0.0).sum()),
        "avg_target_vol_estimate": float(pd.to_numeric(sub["target_vol_realized_vol"], errors="coerce").where(active).mean()),
        "cap_days_active": int((actual.where(active) >= float(v25.TARGET_VOL_MAX_LEVERAGE) - 1e-12).sum()),
    }
    return row


def _shock_metrics(out: pd.DataFrame) -> dict[str, Any]:
    active = out["holding"].astype(str).ne("cash")
    microcap_ret = pd.to_numeric(out["microcap_ret"], errors="coerce")
    vol20 = microcap_ret.rolling(20, min_periods=20).std(ddof=1) * math.sqrt(TRADING_DAYS)
    vol60 = microcap_ret.rolling(60, min_periods=60).std(ddof=1) * math.sqrt(TRADING_DAYS)
    ideal20_scale = (TARGET_VOL / vol20.replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan).clip(
        lower=float(v25.TARGET_VOL_MIN_LEVERAGE),
        upper=float(v25.TARGET_VOL_MAX_LEVERAGE),
    ).shift(1)
    shock = active & vol20.gt(0.40) & vol20.sub(vol60).gt(0.15)
    exec_scale = pd.to_numeric(out["current_execution_scale"], errors="coerce")
    excess_vs_ideal20 = (exec_scale - ideal20_scale).where(shock)
    return {
        "shock_days": int(shock.sum()),
        "shock_avg_vol20": float(vol20.where(shock).mean()),
        "shock_avg_vol60": float(vol60.where(shock).mean()),
        "shock_avg_execution_scale": float(exec_scale.where(shock).mean()),
        "shock_avg_ideal20_scale": float(ideal20_scale.where(shock).mean()),
        "shock_avg_excess_scale_vs_ideal20": float(excess_vs_ideal20.mean()),
        "shock_positive_excess_scale_days": int(excess_vs_ideal20.gt(0.05).sum()),
        "shock_mean_return_net": float(pd.to_numeric(out["return_net"], errors="coerce").where(shock).mean()),
    }


def _read_source() -> pd.DataFrame:
    if not SOURCE_NAV.exists():
        raise FileNotFoundError(f"missing refreshed v2.5 source NAV: {SOURCE_NAV}")
    df = pd.read_csv(SOURCE_NAV, parse_dates=["date"]).sort_values("date").set_index("date")
    required = {"holding", "next_holding", "microcap_ret", "base_pre_cost_return", "base_trade_cost", "return_net"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"source NAV missing columns: {sorted(missing)}")
    return df


def main() -> None:
    RUN_FOLDER.mkdir(parents=True, exist_ok=True)
    source = _read_source()
    window_slices = _window_slices(pd.DatetimeIndex(source.index))
    variants: dict[str, pd.DataFrame] = {}
    window_rows: list[dict[str, Any]] = []
    summary_rows: list[dict[str, Any]] = []
    for cfg in CANDIDATES:
        out = apply_variant(source, **cfg)
        variants[cfg["candidate"]] = out
        shock = _shock_metrics(out)
        for window_name, (start, end) in window_slices.items():
            row = _window_metrics(out, cfg, window_name, start, end)
            if window_name == "full":
                summary_rows.append({**row, **shock})
            window_rows.append(row)

    diagnostics = pd.DataFrame(summary_rows)
    summary = pd.DataFrame(window_rows).rename(columns={"window": "segment", "start_date": "start", "end_date": "end"})
    wide_rows: list[dict[str, Any]] = []
    for candidate, group in summary.groupby("candidate", sort=False):
        base_cols = ["method", "vol_window", "threshold"]
        wide: dict[str, Any] = {"candidate": candidate}
        for col in base_cols:
            wide[col] = group.iloc[0][col]
        for _, row in group.iterrows():
            segment = str(row["segment"])
            for metric in ["ann_return", "ann_vol", "max_dd", "sharpe_repo", "active_scaled_ann_vol", "mean_abs_scale_gap_active", "scale_change_events", "sum_scale_change_cost"]:
                wide[f"{metric}_{segment}"] = row[metric]
        full_diag = diagnostics.loc[diagnostics["candidate"].eq(candidate)]
        if not full_diag.empty:
            for metric in ["shock_days", "shock_avg_vol20", "shock_avg_vol60", "shock_avg_execution_scale", "shock_avg_ideal20_scale", "shock_avg_excess_scale_vs_ideal20", "shock_positive_excess_scale_days"]:
                wide[metric] = full_diag.iloc[0][metric]
        wide_rows.append(wide)
    windows = pd.DataFrame(wide_rows)
    summary.to_csv(RUN_FOLDER / "scan_summary.csv", index=False, encoding="utf-8-sig")
    windows.to_csv(RUN_FOLDER / "window_metrics.csv", index=False, encoding="utf-8-sig")

    official = variants["official_roll60_thr030"]
    parity_cols = ["return_net", "current_execution_scale", "target_vol_realized_vol"]
    parity = {
        col: float((pd.to_numeric(official[col], errors="coerce") - pd.to_numeric(source[col], errors="coerce")).abs().max())
        for col in parity_cols
    }
    meta = json.loads((RUN_FOLDER / "scan_meta.json").read_text(encoding="utf-8"))
    meta.update(
        {
            "phase": "complete",
            "scan_type": "target_vol_overlay_estimator_and_scale_threshold_scan",
            "candidate_grid": CANDIDATES,
            "baseline": CANDIDATES[0],
            "data_snapshot": {
                "source_nav": SOURCE_NAV,
                "start_date": pd.Timestamp(source.index.min()),
                "end_date": pd.Timestamp(source.index.max()),
                "rows": len(source),
                "active_rows": int(source["holding"].astype(str).ne("cash").sum()),
                "latest_trading_date": pd.Timestamp(source.index.max()),
                "source_used": "current-code v2.5 scan preflight force-refresh output",
            },
            "cost_model": {
                "base_trade_cost": "scaled embedded-lineage base_trade_cost from v2.5 source stream",
                "scale_change_cost": float(v25.TARGET_VOL_SCALE_CHANGE_COST),
                "financing_rate": float(v25.TARGET_VOL_FINANCING_RATE),
                "cash_day_yield": float(v25.IDLE_CASH_YIELD),
                "execution_timing": "close-confirmed; execution_scale uses prior realized volatility via one-day shift",
                "max_leverage": float(v25.TARGET_VOL_MAX_LEVERAGE),
                "target_vol": TARGET_VOL,
            },
            "parity_check": {
                "baseline_vs_source_max_abs_diff": parity,
                "passed": all(value < 1e-10 for value in parity.values()),
            },
            "outputs": {
                **meta.get("outputs", {}),
                "scan_summary": str(RUN_FOLDER / "scan_summary.csv"),
                "window_metrics": str(RUN_FOLDER / "window_metrics.csv"),
                "scan_meta": str(RUN_FOLDER / "scan_meta.json"),
                "record": str(RUN_FOLDER / "record.md"),
                "command_log": str(RUN_FOLDER / "command_log.txt"),
            },
            "git_status_after": _git(["status", "--short"]),
        }
    )
    _write_json(RUN_FOLDER / "scan_meta.json", meta)

    with (RUN_FOLDER / "command_log.txt").open("a", encoding="utf-8") as fh:
        fh.write("\npython scripts/run_microcap_v2_5_target_vol_window_threshold_scan.py\n")

    record = f"""# v2.5 Target-Vol Window/Threshold Reasonableness Scan

## Question

Test whether v2.5's 60-day realized-volatility estimator reacts too slowly to microcap volatility jumps, and whether the 0.30 scale rebalance threshold leaves target-vol tracking too loose.

## Data And Baseline

- Source stream: `{SOURCE_NAV.relative_to(ROOT)}`
- Date range: `{source.index.min().date()}` to `{source.index.max().date()}`
- Rows: `{len(source)}`
- Active holding rows: `{int(source['holding'].astype(str).ne('cash').sum())}`
- Baseline candidate: `official_roll60_thr030`
- Baseline parity check max abs diff: `{parity}`

## Candidates

{pd.DataFrame(CANDIDATES).to_markdown(index=False)}

## Outputs

- `scan_summary.csv`
- `window_metrics.csv`
- `scan_meta.json`

## Notes

- All candidates reuse the same refreshed v2.5 signal/holding stream and embedded base cost stream.
- Only target-vol estimator/window and scale-threshold behavior are changed.
- Metrics include full, 10Y, 5Y, 3Y, and 1Y windows; no 4Y window is included.
- `ann_vol` is final costed strategy volatility including cash days. `active_scaled_ann_vol` measures the scaled active microcap component on days where v2.5 holds microcap exposure.

## Stability

- Filled after artifact audit and result interpretation.

## Decision

- Research-only; no production source change made by this scan.
"""
    (RUN_FOLDER / "record.md").write_text(record, encoding="utf-8")
    print(f"wrote {RUN_FOLDER}")
    print(f"parity {parity}")
    print(diagnostics[["candidate", "ann_return", "ann_vol", "active_scaled_ann_vol", "max_dd", "mean_abs_scale_gap_active", "scale_change_events", "sum_scale_change_cost", "shock_days", "shock_avg_excess_scale_vs_ideal20"]].to_string(index=False))


if __name__ == "__main__":
    main()
