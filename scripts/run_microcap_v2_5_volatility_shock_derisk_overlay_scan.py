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
SCRIPTS = ROOT / "scripts"
for path in (ROOT, SCRIPTS):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

import microcap_top100_mom16_biweekly_live_v2_5 as v25  # noqa: E402
import run_microcap_v2_5_target_vol_window_threshold_scan as tv_scan  # noqa: E402


RUN_FOLDER = ROOT / "quant_param_scan_runs" / "20260601_microcap_top100_v2_5_target_vol_overlay_volatility_shock_asymmetric_derisk_overlay"
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
    {
        "candidate": "official_v25_no_extra_derisk",
        "mode": "baseline",
        "trigger_vol20": None,
        "trigger_gap": None,
        "exit_vol20": None,
        "exit_gap": None,
        "downside_only": False,
        "cap_blend": 1.0,
    },
    {
        "candidate": "shock_strict_cap20",
        "mode": "vol_spike",
        "trigger_vol20": 0.40,
        "trigger_gap": 0.15,
        "exit_vol20": 0.35,
        "exit_gap": 0.05,
        "downside_only": False,
        "cap_blend": 1.0,
    },
    {
        "candidate": "shock_loose_cap20",
        "mode": "vol_spike",
        "trigger_vol20": 0.35,
        "trigger_gap": 0.10,
        "exit_vol20": 0.30,
        "exit_gap": 0.03,
        "downside_only": False,
        "cap_blend": 1.0,
    },
    {
        "candidate": "shock_strict_half_cap20",
        "mode": "vol_spike",
        "trigger_vol20": 0.40,
        "trigger_gap": 0.15,
        "exit_vol20": 0.35,
        "exit_gap": 0.05,
        "downside_only": False,
        "cap_blend": 0.5,
    },
    {
        "candidate": "downside_shock_cap20",
        "mode": "vol_spike",
        "trigger_vol20": 0.40,
        "trigger_gap": 0.15,
        "exit_vol20": 0.35,
        "exit_gap": 0.05,
        "downside_only": True,
        "downside_ret5": -0.05,
        "downside_ret20": -0.10,
        "cap_blend": 1.0,
    },
    {
        "candidate": "downside_loose_cap20",
        "mode": "vol_spike",
        "trigger_vol20": 0.35,
        "trigger_gap": 0.10,
        "exit_vol20": 0.30,
        "exit_gap": 0.03,
        "downside_only": True,
        "downside_ret5": -0.04,
        "downside_ret20": -0.08,
        "cap_blend": 1.0,
    },
    {
        "candidate": "asym_downside_semivol_cap",
        "mode": "downside_semivol",
        "trigger_downside_vol20": 0.25,
        "trigger_downside_count20": 6,
        "exit_downside_vol20": 0.18,
        "exit_downside_count20": 4,
        "downside_only": True,
        "downside_ret5": -0.03,
        "downside_ret20": -0.06,
        "cap_blend": 1.0,
    },
    {
        "candidate": "asym_downside_semivol_half_cap",
        "mode": "downside_semivol",
        "trigger_downside_vol20": 0.25,
        "trigger_downside_count20": 6,
        "exit_downside_vol20": 0.18,
        "exit_downside_count20": 4,
        "downside_only": True,
        "downside_ret5": -0.03,
        "downside_ret20": -0.06,
        "cap_blend": 0.5,
    },
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


def _read_source() -> pd.DataFrame:
    if not SOURCE_NAV.exists():
        raise FileNotFoundError(f"missing refreshed v2.5 source NAV: {SOURCE_NAV}")
    df = pd.read_csv(SOURCE_NAV, parse_dates=["date"]).sort_values("date").set_index("date")
    required = {
        "holding",
        "next_holding",
        "microcap_ret",
        "base_pre_cost_return",
        "base_trade_cost",
        "return_net",
        "current_execution_scale",
        "next_session_actionable_scale",
        "target_vol_realized_vol",
    }
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"source NAV missing columns: {sorted(missing)}")
    return df


def _window_slices(index: pd.DatetimeIndex) -> dict[str, tuple[pd.Timestamp, pd.Timestamp]]:
    end = pd.Timestamp(index.max())
    start = pd.Timestamp(index.min())
    out = {"full": (start, end)}
    for name, offset in WINDOWS.items():
        if name == "full" or offset is None:
            continue
        out[name] = (max(start, end - offset), end)
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


def _rolling_nav_dd(ret: pd.Series) -> float:
    r = pd.to_numeric(ret, errors="coerce").fillna(0.0)
    if r.empty:
        return np.nan
    nav = (1.0 + r).cumprod()
    return float(nav.div(nav.cummax()).sub(1.0).min())


def _stateful_regime(trigger: pd.Series, exit_signal: pd.Series) -> pd.Series:
    trigger = trigger.fillna(False).astype(bool)
    exit_signal = exit_signal.fillna(False).astype(bool)
    values: list[bool] = []
    active = False
    for should_enter, should_exit in zip(trigger.tolist(), exit_signal.tolist(), strict=True):
        if active and should_exit:
            active = False
        if should_enter:
            active = True
        values.append(active)
    return pd.Series(values, index=trigger.index, dtype=bool)


def _diagnostic_inputs(source: pd.DataFrame) -> dict[str, pd.Series]:
    microcap_ret = pd.to_numeric(source["microcap_ret"], errors="coerce").replace([np.inf, -np.inf], np.nan)
    vol20 = microcap_ret.rolling(20, min_periods=20).std(ddof=1) * math.sqrt(TRADING_DAYS)
    vol60 = microcap_ret.rolling(60, min_periods=60).std(ddof=1) * math.sqrt(TRADING_DAYS)
    ideal20 = (TARGET_VOL / vol20.replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan).clip(
        lower=float(v25.TARGET_VOL_MIN_LEVERAGE),
        upper=float(v25.TARGET_VOL_MAX_LEVERAGE),
    )
    negative = microcap_ret.clip(upper=0.0)
    downside_semivol20 = (negative.pow(2).rolling(20, min_periods=20).mean().pow(0.5) * math.sqrt(TRADING_DAYS) * math.sqrt(2.0))
    downside_ideal = (TARGET_VOL / downside_semivol20.replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan).clip(
        lower=float(v25.TARGET_VOL_MIN_LEVERAGE),
        upper=float(v25.TARGET_VOL_MAX_LEVERAGE),
    )
    down_count20 = microcap_ret.lt(0.0).rolling(20, min_periods=20).sum()
    ret5 = (1.0 + microcap_ret.fillna(0.0)).rolling(5, min_periods=5).apply(np.prod, raw=True) - 1.0
    ret20 = (1.0 + microcap_ret.fillna(0.0)).rolling(20, min_periods=20).apply(np.prod, raw=True) - 1.0
    return {
        "microcap_ret": microcap_ret,
        "vol20": vol20,
        "vol60": vol60,
        "vol_gap20_60": vol20 - vol60,
        "ideal20": ideal20,
        "downside_semivol20": downside_semivol20,
        "downside_ideal": downside_ideal,
        "down_count20": down_count20,
        "ret5": ret5,
        "ret20": ret20,
    }


def _overlay_state_and_cap(source: pd.DataFrame, cfg: dict[str, Any], diag: dict[str, pd.Series]) -> tuple[pd.Series, pd.Series, pd.Series]:
    index = source.index
    if cfg["mode"] == "baseline":
        return (
            pd.Series(False, index=index, dtype=bool),
            pd.Series(np.nan, index=index, dtype=float),
            pd.Series(False, index=index, dtype=bool),
        )

    active_holding = source["holding"].fillna("cash").astype(str).ne("cash")
    downside_filter = pd.Series(True, index=index, dtype=bool)
    if bool(cfg.get("downside_only", False)):
        downside_filter = diag["ret5"].le(float(cfg.get("downside_ret5", -1.0))) | diag["ret20"].le(float(cfg.get("downside_ret20", -1.0)))

    if cfg["mode"] == "vol_spike":
        trigger = (
            diag["vol20"].gt(float(cfg["trigger_vol20"]))
            & diag["vol_gap20_60"].gt(float(cfg["trigger_gap"]))
            & downside_filter
            & active_holding
        )
        exit_signal = diag["vol20"].lt(float(cfg["exit_vol20"])) | diag["vol_gap20_60"].lt(float(cfg["exit_gap"]))
        cap = diag["ideal20"]
    elif cfg["mode"] == "downside_semivol":
        trigger = (
            diag["downside_semivol20"].gt(float(cfg["trigger_downside_vol20"]))
            & diag["down_count20"].ge(float(cfg["trigger_downside_count20"]))
            & downside_filter
            & active_holding
        )
        exit_signal = diag["downside_semivol20"].lt(float(cfg["exit_downside_vol20"])) | diag["down_count20"].lt(float(cfg["exit_downside_count20"]))
        cap = diag["downside_ideal"]
    else:
        raise ValueError(f"unsupported mode: {cfg['mode']}")

    state = _stateful_regime(trigger, exit_signal)
    return state, cap, trigger


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


def _scale_change_cost(holding: pd.Series, execution_scale: pd.Series) -> pd.Series:
    holding = holding.fillna("cash").astype(str)
    scale = pd.to_numeric(execution_scale, errors="coerce").fillna(0.0)
    same_holding = holding.eq(holding.shift(1))
    scale_delta = scale.sub(scale.shift(1).fillna(0.0))
    return scale_delta.abs().where(same_holding, 0.0).mul(float(v25.TARGET_VOL_SCALE_CHANGE_COST)).astype(float)


def _blend_cap(current: pd.Series, cap: pd.Series, blend: float) -> pd.Series:
    current = pd.to_numeric(current, errors="coerce").fillna(0.0)
    cap = pd.to_numeric(cap, errors="coerce")
    full_cap = current.where(cap.isna(), np.minimum(current, cap))
    blend = float(blend)
    return current - blend * (current - full_cap)


def apply_overlay(source: pd.DataFrame, cfg: dict[str, Any]) -> pd.DataFrame:
    out = source.copy().sort_index()
    holding = out["holding"].fillna("cash").astype(str)
    next_holding = out["next_holding"].fillna(holding).astype(str)
    active = holding.ne("cash")
    base_pre_cost_return = pd.to_numeric(out["base_pre_cost_return"], errors="coerce").fillna(0.0)
    base_trade_cost = pd.to_numeric(out["base_trade_cost"], errors="coerce").fillna(0.0)
    official_current = pd.to_numeric(out["current_execution_scale"], errors="coerce").fillna(0.0)
    official_next = pd.to_numeric(out["next_session_actionable_scale"], errors="coerce").fillna(official_current)

    diag = _diagnostic_inputs(out)
    state_after_close, cap_after_close, raw_trigger = _overlay_state_and_cap(out, cfg, diag)
    state_current = state_after_close.shift(1, fill_value=False).astype(bool) & active
    cap_current = cap_after_close.shift(1)

    current_scale = official_current.copy()
    capped_current = _blend_cap(official_current, cap_current, float(cfg.get("cap_blend", 1.0)))
    current_scale.loc[state_current] = capped_current.loc[state_current]
    current_scale = current_scale.clip(lower=0.0, upper=float(v25.TARGET_VOL_MAX_LEVERAGE))

    next_state = state_after_close & next_holding.ne("cash")
    capped_next = _blend_cap(official_next, cap_after_close, float(cfg.get("cap_blend", 1.0)))
    next_actionable = official_next.copy()
    next_actionable.loc[next_state] = capped_next.loc[next_state]
    next_actionable = next_actionable.clip(lower=0.0, upper=float(v25.TARGET_VOL_MAX_LEVERAGE))

    scale_change_cost = _scale_change_cost(holding, current_scale)
    financing_cost = current_scale.sub(1.0).clip(lower=0.0) * float(v25.TARGET_VOL_FINANCING_RATE) / TRADING_DAYS
    idle_cash_yield = active.astype(float) * current_scale.rsub(1.0).clip(lower=0.0, upper=1.0) * float(v25.IDLE_CASH_YIELD) / TRADING_DAYS
    cash_day_yield = active.astype(float).rsub(1.0) * float(v25.IDLE_CASH_YIELD) / TRADING_DAYS
    base_cost_scale = _base_trade_cost_scale(holding, next_holding, current_scale, next_actionable)
    base_trade_cost_scaled = (base_trade_cost * base_cost_scale).clip(lower=0.0, upper=0.99)
    ret = (
        (1.0 + base_pre_cost_return * current_scale + idle_cash_yield + cash_day_yield)
        * (1.0 - base_trade_cost_scaled)
        * (1.0 - scale_change_cost)
        * (1.0 - financing_cost)
        - 1.0
    )
    out["candidate"] = cfg["candidate"]
    out["mode"] = cfg["mode"]
    out["vol20"] = diag["vol20"]
    out["vol60"] = diag["vol60"]
    out["vol_gap20_60"] = diag["vol_gap20_60"]
    out["downside_semivol20"] = diag["downside_semivol20"]
    out["ret5"] = diag["ret5"]
    out["ret20"] = diag["ret20"]
    out["overlay_trigger_raw"] = raw_trigger
    out["overlay_state_after_close"] = state_after_close
    out["overlay_state_current"] = state_current
    out["overlay_cap_after_close"] = cap_after_close
    out["official_execution_scale"] = official_current
    out["current_execution_scale"] = current_scale
    out["next_session_actionable_scale"] = next_actionable
    out["scale_reduction"] = (official_current - current_scale).clip(lower=0.0)
    out["scale_change_cost"] = scale_change_cost
    out["financing_cost"] = financing_cost
    out["idle_cash_yield"] = idle_cash_yield
    out["cash_day_yield"] = cash_day_yield
    out["base_trade_cost_scaled"] = base_trade_cost_scaled
    out["scaled_pre_cost_return"] = base_pre_cost_return * current_scale + idle_cash_yield + cash_day_yield
    out["return_net"] = ret
    out["nav_net"] = (1.0 + ret.fillna(0.0)).cumprod()
    return out


def _shock_mask(out: pd.DataFrame) -> pd.Series:
    return (
        out["holding"].fillna("cash").astype(str).ne("cash")
        & pd.to_numeric(out["vol20"], errors="coerce").gt(0.40)
        & pd.to_numeric(out["vol_gap20_60"], errors="coerce").gt(0.15)
    )


def _window_metrics(out: pd.DataFrame, cfg: dict[str, Any], segment: str, start: pd.Timestamp, end: pd.Timestamp) -> dict[str, Any]:
    sub = out.loc[(out.index >= start) & (out.index <= end)].copy()
    active = sub["holding"].astype(str).ne("cash")
    metrics = _metrics(sub["return_net"])
    official_ret = pd.to_numeric(sub.get("official_return_net", sub["return_net"]), errors="coerce").fillna(0.0)
    active_scaled = pd.to_numeric(sub.loc[active, "scaled_pre_cost_return"], errors="coerce").fillna(0.0)
    shock = _shock_mask(sub)
    overlay = sub["overlay_state_current"].fillna(False).astype(bool)
    return {
        "candidate": cfg["candidate"],
        "segment": segment,
        "start": str(start.date()),
        "end": str(end.date()),
        "mode": cfg["mode"],
        "cap_blend": float(cfg.get("cap_blend", 1.0)),
        **metrics,
        "ann_vol_gap": float(metrics["ann_vol"] - TARGET_VOL) if math.isfinite(float(metrics["ann_vol"])) else np.nan,
        "active_rows": int(active.sum()),
        "active_share": float(active.mean()) if len(active) else np.nan,
        "active_scaled_ann_vol": float(active_scaled.std(ddof=1) * math.sqrt(TRADING_DAYS)) if len(active_scaled) > 1 else np.nan,
        "overlay_days": int(overlay.sum()),
        "overlay_active_days": int((overlay & active).sum()),
        "trigger_days": int(sub["overlay_trigger_raw"].fillna(False).astype(bool).sum()),
        "avg_scale_reduction_overlay": float(pd.to_numeric(sub["scale_reduction"], errors="coerce").where(overlay & active).mean()),
        "sum_scale_reduction": float(pd.to_numeric(sub["scale_reduction"], errors="coerce").fillna(0.0).sum()),
        "sum_scale_change_cost": float(pd.to_numeric(sub["scale_change_cost"], errors="coerce").fillna(0.0).sum()),
        "sum_base_trade_cost_scaled": float(pd.to_numeric(sub["base_trade_cost_scaled"], errors="coerce").fillna(0.0).sum()),
        "shock_days": int(shock.sum()),
        "shock_ann_vol": float(pd.to_numeric(sub.loc[shock, "return_net"], errors="coerce").std(ddof=1) * math.sqrt(TRADING_DAYS)) if int(shock.sum()) > 1 else np.nan,
        "shock_mean_return": float(pd.to_numeric(sub["return_net"], errors="coerce").where(shock).mean()),
        "shock_total_return": float((1.0 + pd.to_numeric(sub["return_net"], errors="coerce").where(shock).dropna()).prod() - 1.0) if int(shock.sum()) else np.nan,
        "shock_max_dd": _rolling_nav_dd(pd.to_numeric(sub["return_net"], errors="coerce").where(shock).dropna()) if int(shock.sum()) else np.nan,
        "excess_return_vs_official": float((1.0 + pd.to_numeric(sub["return_net"], errors="coerce").fillna(0.0)).prod() / (1.0 + official_ret).prod() - 1.0),
    }


def _wide_window_metrics(summary: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for candidate, group in summary.groupby("candidate", sort=False):
        wide: dict[str, Any] = {"candidate": candidate}
        for col in ["mode", "cap_blend"]:
            wide[col] = group.iloc[0][col]
        for _, row in group.iterrows():
            segment = str(row["segment"])
            for metric in [
                "ann_return",
                "ann_vol",
                "max_dd",
                "sharpe_repo",
                "active_scaled_ann_vol",
                "overlay_active_days",
                "avg_scale_reduction_overlay",
                "sum_scale_change_cost",
                "shock_total_return",
                "shock_max_dd",
                "excess_return_vs_official",
            ]:
                wide[f"{metric}_{segment}"] = row[metric]
        rows.append(wide)
    return pd.DataFrame(rows)


def main() -> None:
    RUN_FOLDER.mkdir(parents=True, exist_ok=True)
    source = _read_source()
    official = source.copy()
    official["official_return_net"] = pd.to_numeric(source["return_net"], errors="coerce").fillna(0.0)
    windows = _window_slices(pd.DatetimeIndex(source.index))
    summary_rows: list[dict[str, Any]] = []
    outputs: dict[str, pd.DataFrame] = {}
    for cfg in CANDIDATES:
        out = apply_overlay(official, cfg)
        out["official_return_net"] = official["official_return_net"]
        outputs[cfg["candidate"]] = out
        for segment, (start, end) in windows.items():
            summary_rows.append(_window_metrics(out, cfg, segment, start, end))

    summary = pd.DataFrame(summary_rows)
    wide = _wide_window_metrics(summary)
    summary.to_csv(RUN_FOLDER / "scan_summary.csv", index=False, encoding="utf-8-sig")
    wide.to_csv(RUN_FOLDER / "window_metrics.csv", index=False, encoding="utf-8-sig")

    baseline = outputs["official_v25_no_extra_derisk"]
    parity = {
        "return_net": float((pd.to_numeric(baseline["return_net"], errors="coerce") - pd.to_numeric(source["return_net"], errors="coerce")).abs().max()),
        "current_execution_scale": float((pd.to_numeric(baseline["current_execution_scale"], errors="coerce") - pd.to_numeric(source["current_execution_scale"], errors="coerce")).abs().max()),
    }
    best_by_full_return = summary.loc[summary["segment"].eq("full")].sort_values("ann_return", ascending=False).iloc[0].to_dict()
    meta_path = RUN_FOLDER / "scan_meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.exists() else {}
    meta.update(
        {
            "phase": "complete",
            "scan_type": "volatility_shock_and_asymmetric_downside_derisk_overlay_scan",
            "baseline": CANDIDATES[0],
            "candidate_grid": CANDIDATES,
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
                "execution_timing": "close-confirmed trigger on T close; derisked execution scale starts T+1 via shifted overlay_state_current",
                "max_leverage": float(v25.TARGET_VOL_MAX_LEVERAGE),
                "target_vol": TARGET_VOL,
            },
            "parity_check": {
                "baseline_vs_source_max_abs_diff": parity,
                "passed": all(value < 1e-10 for value in parity.values()),
            },
            "best_full_sample_by_ann_return": best_by_full_return,
            "outputs": {
                "record": str(RUN_FOLDER / "record.md"),
                "scan_summary": str(RUN_FOLDER / "scan_summary.csv"),
                "window_metrics": str(RUN_FOLDER / "window_metrics.csv"),
                "scan_meta": str(RUN_FOLDER / "scan_meta.json"),
                "command_log": str(RUN_FOLDER / "command_log.txt"),
            },
            "git_status_after": _git(["status", "--short"]),
        }
    )
    _write_json(meta_path, meta)
    with (RUN_FOLDER / "command_log.txt").open("a", encoding="utf-8") as fh:
        fh.write("\npython scripts/run_microcap_v2_5_volatility_shock_derisk_overlay_scan.py\n")

    record = f"""# v2.5 Volatility Shock / Asymmetric Downside Derisk Overlay Scan

## Question

Separately test temporary derisk overlays for v2.5 when short-horizon microcap volatility jumps faster than the official 60-day target-vol estimator, including downside-only variants.

## Data

- Source stream: `{SOURCE_NAV.relative_to(ROOT)}`
- Date range: `{source.index.min().date()}` to `{source.index.max().date()}`
- Rows: `{len(source)}`
- Active holding rows: `{int(source['holding'].astype(str).ne('cash').sum())}`

## Candidate Rules

{pd.DataFrame(CANDIDATES).to_markdown(index=False)}

## Timing

- Indicators are computed at T close.
- `overlay_state_current` is shifted by one row, so the scale reduction affects T+1 returns.
- The strategy's existing holding, base costs, scale-change cost, financing, and cash-yield conventions are preserved.

## Outputs

- `scan_summary.csv`
- `window_metrics.csv`
- `scan_meta.json`

## Stability

- Filled after artifact audit and final interpretation.

## Decision

- Research-only; no production source change made by this scan.
"""
    (RUN_FOLDER / "record.md").write_text(record, encoding="utf-8")
    print(f"wrote {RUN_FOLDER}")
    print(f"parity {parity}")
    full = summary.loc[summary["segment"].eq("full"), [
        "candidate",
        "ann_return",
        "ann_vol",
        "active_scaled_ann_vol",
        "max_dd",
        "overlay_active_days",
        "avg_scale_reduction_overlay",
        "sum_scale_change_cost",
        "shock_total_return",
        "shock_max_dd",
        "excess_return_vs_official",
    ]]
    print(full.to_string(index=False))


if __name__ == "__main__":
    main()
