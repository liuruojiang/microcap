from __future__ import annotations

import json
import sys
from pathlib import Path

import matplotlib
import numpy as np
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt

import top100_v14_base_context as v14_context

# v1.6 intentionally reuses the shared v1.4 base/context adapter; recheck this
# module when that adapter changes its v1_1_mod/base_mod or context API.

ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"

OUTPUT_PREFIX = "microcap_top100_mom16_biweekly_live_v1_6"
SUMMARY_JSON = OUTPUT_DIR / f"{OUTPUT_PREFIX}_summary.json"
LATEST_SIGNAL_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_latest_signal.csv"
REALTIME_SIGNAL_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_realtime_signal.csv"
NAV_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_nav.csv"
COSTED_NAV_CSV = OUTPUT_DIR / "microcap_top100_mom16_targetvol25_max1p5_v1_6_costed_nav.csv"
LEGACY_COSTED_NAV_CSV = OUTPUT_DIR / "microcap_top100_mom16_targetvol15_max1p5_v1_6_costed_nav.csv"
PERF_SUMMARY_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_summary.csv"
PERF_YEARLY_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_yearly.csv"
PERF_NAV_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_nav.csv"
PERF_JSON = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_summary.json"
PERF_PNG = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_curve.png"

EXPECTED_VERSION_ROLE = "target_vol_overlay_on_v1_4"
EXPECTED_VERSION_NOTE_PREFIX = "Target-volatility overlay on top of v1.4."
BASE_HEDGE_RATIO = 0.8
V1_6_MOMENTUM_GAP_EXIT_BUFFER = 0.0030
DECAY_RATIO_THRESHOLD = 0.25
DERISK_SCALE = 0.0
RECOVERY_RATIO_THRESHOLD = 0.35
TARGET_VOL = 0.25
TARGET_VOL_WINDOW = 60
TARGET_VOL_MAX_LEVERAGE = 1.5
TARGET_VOL_MIN_LEVERAGE = 0.0
TARGET_VOL_TRADING_DAYS = 244
TARGET_VOL_SCALE_CHANGE_COST = 0.001
TARGET_VOL_SCALE_REBALANCE_THRESHOLD = 0.10
TARGET_VOL_FINANCING_RATE = 0.03
PNL_RETURN_SOURCE = "v1_4_overlay_pre_cost_return_explicit_or_return_net_cost_reversal_fallback"
LIVE_CONTEXT_CACHE = ROOT / ".autobuild_top100_cache" / "context_cache_v1_6.json"


def validate_base_hedge_ratio() -> None:
    v1_1_mod = getattr(v14_context, "v1_1_mod", None)
    if v1_1_mod is None:
        raise RuntimeError("missing v14_context.v1_1_mod; cannot validate v1.6 base hedge ratio")
    base_mod = getattr(v1_1_mod, "base_mod", None)
    if base_mod is None:
        raise RuntimeError("missing v14_context.v1_1_mod.base_mod; cannot validate v1.6 base hedge ratio")
    checks = {
        "v14_context.BASE_HEDGE_RATIO": getattr(v14_context, "BASE_HEDGE_RATIO", None),
        "v14_context.v1_1_mod.base_mod.FIXED_HEDGE_RATIO": getattr(base_mod, "FIXED_HEDGE_RATIO", None),
    }
    for name, value in checks.items():
        if value is None:
            raise RuntimeError(f"missing {name}; cannot validate v1.6 base hedge ratio")
        if abs(float(value) - float(BASE_HEDGE_RATIO)) > 1e-9:
            raise ValueError(f"hedge ratio mismatch: v1.6={BASE_HEDGE_RATIO}, {name}={value}")


def current_base_fingerprint() -> dict[str, object]:
    validate_base_hedge_ratio()
    base = dict(v14_context.current_base_fingerprint())
    base["momentum_gap_exit_buffer"] = V1_6_MOMENTUM_GAP_EXIT_BUFFER
    return {
        "base_version": "1.4",
        "base_v1_4_fingerprint": base,
        "overlay_type": "target_volatility_scaling",
        "base_hedge_ratio": BASE_HEDGE_RATIO,
        "target_vol": TARGET_VOL,
        "vol_window": TARGET_VOL_WINDOW,
        "max_leverage": TARGET_VOL_MAX_LEVERAGE,
        "min_leverage": TARGET_VOL_MIN_LEVERAGE,
        "trading_days": TARGET_VOL_TRADING_DAYS,
        "scale_change_cost": TARGET_VOL_SCALE_CHANGE_COST,
        "scale_change_cost_model": "microcap_long_plus_hedge_leg_net_turnover",
        "scale_rebalance_threshold": TARGET_VOL_SCALE_REBALANCE_THRESHOLD,
        "base_trade_cost_model": "v1_4_total_cost_scaled_by_target_vol_exposure",
        "volatility_return_source_priority": ["return_raw", "base_gross_return", "return_net_fallback_warning"],
        "pnl_return_source": PNL_RETURN_SOURCE,
        "financing_rate": TARGET_VOL_FINANCING_RATE,
        "momentum_gap_exit_buffer": V1_6_MOMENTUM_GAP_EXIT_BUFFER,
        "decay_ratio_threshold": DECAY_RATIO_THRESHOLD,
        "derisk_scale": DERISK_SCALE,
        "recovery_ratio_threshold": RECOVERY_RATIO_THRESHOLD,
    }


def summary_matches_current_v1_6_base(summary: dict[str, object]) -> bool:
    if not isinstance(summary, dict):
        return False
    if str(summary.get("version")) != "1.6":
        return False
    if str(summary.get("version_role")) != EXPECTED_VERSION_ROLE:
        return False
    if not str(summary.get("version_note", "")).startswith(EXPECTED_VERSION_NOTE_PREFIX):
        return False
    return summary.get("base_fingerprint") == current_base_fingerprint()


def invalidate_incompatible_v1_6_outputs() -> list[Path]:
    stale = incompatible_v1_6_outputs()
    removed: list[Path] = []
    for path in stale:
        if path.exists():
            path.unlink(missing_ok=True)
            removed.append(path)
    return removed


def incompatible_v1_6_outputs() -> list[Path]:
    if not SUMMARY_JSON.exists():
        return []
    try:
        summary = json.loads(SUMMARY_JSON.read_text(encoding="utf-8"))
    except Exception:
        summary = None
    if summary_matches_current_v1_6_base(summary):
        return []
    return [
        SUMMARY_JSON,
        LATEST_SIGNAL_CSV,
        REALTIME_SIGNAL_CSV,
        NAV_CSV,
        COSTED_NAV_CSV,
        LEGACY_COSTED_NAV_CSV,
        PERF_SUMMARY_CSV,
        PERF_YEARLY_CSV,
        PERF_NAV_CSV,
        PERF_JSON,
        PERF_PNG,
    ]


def ensure_output_dir() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# Experimental helper: currently unused by the close-confirmed and realtime v1.6 signal paths.
def load_live_context_cache(path: Path = LIVE_CONTEXT_CACHE) -> dict[str, object] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if isinstance(payload, dict) and isinstance(payload.get("context"), dict):
        payload = payload["context"]
    if not isinstance(payload, dict) or not payload.get("close_df"):
        return None
    return payload


# Experimental helper: currently unused by the close-confirmed and realtime v1.6 signal paths.
def _live_close_df(live_context: dict[str, object] | None) -> pd.DataFrame | None:
    if not isinstance(live_context, dict):
        return None
    try:
        live_close = pd.DataFrame(live_context.get("close_df") or [])
    except Exception:
        return None
    if live_close.empty or not {"date", "microcap", "hedge"}.issubset(live_close.columns):
        return None
    live_close["date"] = pd.to_datetime(live_close["date"], errors="coerce")
    live_close = live_close.dropna(subset=["date"]).sort_values("date").drop_duplicates(subset="date", keep="last")
    live_close = live_close.set_index("date")[["microcap", "hedge"]].apply(pd.to_numeric, errors="coerce").dropna()
    return live_close if not live_close.empty else None


# Experimental helper: currently unused by the close-confirmed and realtime v1.6 signal paths.
def _recent_microcap_tail_is_flat(close_df: pd.DataFrame, tail_days: int = 5) -> bool:
    if close_df.empty or "microcap" not in close_df.columns:
        return False
    tail = pd.to_numeric(close_df["microcap"], errors="coerce").dropna().tail(tail_days)
    if len(tail) < max(3, tail_days):
        return False
    return bool(tail.pct_change().dropna().abs().le(1e-12).all())


# Experimental helper: currently unused by the close-confirmed and realtime v1.6 signal paths.
def overlay_live_microcap_tail(
    close_df: pd.DataFrame,
    live_context: dict[str, object] | None,
) -> tuple[pd.DataFrame, dict[str, object]]:
    out = close_df.copy().sort_index()
    meta: dict[str, object] = {"applied": False, "source": str(LIVE_CONTEXT_CACHE)}
    live_close = _live_close_df(live_context)
    if live_close is None:
        meta["reason"] = "missing_live_close_df"
        return out, meta

    overlap = out.index.intersection(live_close.index)
    if overlap.empty:
        meta["reason"] = "no_overlap"
        return out, meta
    bridge_date = pd.Timestamp(overlap.min())
    base_bridge = float(out.at[bridge_date, "microcap"])
    live_bridge = float(live_close.at[bridge_date, "microcap"])
    if not np.isfinite(base_bridge) or not np.isfinite(live_bridge) or abs(live_bridge) <= 1e-12:
        meta["reason"] = "invalid_bridge"
        return out, meta

    scale = base_bridge / live_bridge
    out.loc[overlap, "microcap"] = pd.to_numeric(live_close.loc[overlap, "microcap"], errors="coerce") * scale
    out.loc[overlap, "hedge"] = pd.to_numeric(live_close.loc[overlap, "hedge"], errors="coerce")
    meta.update(
        {
            "applied": True,
            "bridge_date": str(bridge_date.date()),
            "scale": float(scale),
            "live_start": str(pd.Timestamp(live_close.index.min()).date()),
            "live_end": str(pd.Timestamp(live_close.index.max()).date()),
            "replaced_rows": int(len(overlap)),
        }
    )
    return out, meta


def target_vol_legs_for_state(
    holding: str,
    scale: float,
    hedge_ratio: float = BASE_HEDGE_RATIO,
) -> dict[str, float]:
    if str(holding) == "cash" or not np.isfinite(scale) or scale <= 1e-12:
        return {}
    return {
        "microcap_top100": float(scale),
        "hedge_zz1000": -float(hedge_ratio) * float(scale),
    }


def calc_target_vol_turnover(
    prev_holding: str,
    prev_scale: float,
    next_holding: str,
    next_scale: float,
    hedge_ratio: float = BASE_HEDGE_RATIO,
) -> float:
    old_legs = target_vol_legs_for_state(prev_holding, prev_scale, hedge_ratio=hedge_ratio)
    new_legs = target_vol_legs_for_state(next_holding, next_scale, hedge_ratio=hedge_ratio)
    return float(sum(abs(new_legs.get(k, 0.0) - old_legs.get(k, 0.0)) for k in set(old_legs) | set(new_legs)))


def _target_vol_turnover_series(holding: pd.Series, execution_scale: pd.Series) -> pd.Series:
    prev_holding = holding.shift(1).fillna("cash").astype(str)
    prev_scale = execution_scale.shift(1).fillna(0.0)
    values = [
        calc_target_vol_turnover(old_holding, old_scale, new_holding, new_scale)
        for old_holding, old_scale, new_holding, new_scale in zip(
            prev_holding,
            prev_scale,
            holding.astype(str),
            execution_scale.fillna(0.0),
        )
    ]
    return pd.Series(values, index=holding.index, dtype=float)


def calc_scale_change_cost(holding: pd.Series, target_vol_turnover: pd.Series) -> pd.Series:
    return calc_target_vol_costed_turnover(holding, target_vol_turnover) * TARGET_VOL_SCALE_CHANGE_COST


def calc_target_vol_costed_turnover(holding: pd.Series, target_vol_turnover: pd.Series) -> pd.Series:
    same_holding = holding.astype(str).eq(holding.astype(str).shift(1))
    return pd.to_numeric(target_vol_turnover, errors="coerce").fillna(0.0).where(same_holding, 0.0)


def _scale_from_realized_vol(realized_vol: pd.Series) -> pd.Series:
    scale = TARGET_VOL / realized_vol.replace(0.0, np.nan)
    return scale.replace([np.inf, -np.inf], np.nan).clip(
        lower=TARGET_VOL_MIN_LEVERAGE,
        upper=TARGET_VOL_MAX_LEVERAGE,
    )


def apply_scale_rebalance_threshold(
    desired_scale: pd.Series,
    active: pd.Series,
    threshold: float = TARGET_VOL_SCALE_REBALANCE_THRESHOLD,
) -> pd.Series:
    if not desired_scale.index.is_unique:
        raise ValueError("desired_scale must have a unique index")
    if not active.index.is_unique:
        raise ValueError("active must have a unique index")
    desired = pd.to_numeric(desired_scale, errors="coerce").fillna(1.0)
    active_flags = active.reindex(desired.index).fillna(False).astype(bool)
    values: list[float] = []
    last_scale = 0.0
    for dt, target in desired.items():
        if not bool(active_flags.loc[dt]):
            values.append(0.0)
            last_scale = 0.0
            continue
        target = float(target)
        if last_scale <= 1e-12 or abs(target - last_scale) >= float(threshold):
            last_scale = target
        values.append(float(last_scale))
    return pd.Series(values, index=desired.index, dtype=float)


def calc_next_session_actionable_scale(
    current_execution_scale: pd.Series,
    next_session_target_scale: pd.Series,
    next_holding: pd.Series,
    threshold: float = TARGET_VOL_SCALE_REBALANCE_THRESHOLD,
) -> pd.Series:
    current = pd.to_numeric(current_execution_scale, errors="coerce").fillna(0.0)
    target = pd.to_numeric(next_session_target_scale, errors="coerce").fillna(current)
    next_holding = next_holding.astype(str).reindex(current.index).fillna("cash")
    actionable = current.copy()
    to_cash = next_holding.eq("cash")
    enter_from_cash = current.le(1e-12) & next_holding.ne("cash") & target.gt(1e-12)
    rebalance = target.sub(current).abs().ge(float(threshold))
    actionable.loc[to_cash] = 0.0
    actionable.loc[~to_cash & (enter_from_cash | rebalance)] = target.loc[~to_cash & (enter_from_cash | rebalance)]
    return actionable.astype(float)


def calc_base_trade_cost_scale(
    holding: pd.Series,
    next_holding: pd.Series,
    current_execution_scale: pd.Series,
    next_session_actionable_scale: pd.Series,
) -> pd.Series:
    holding = holding.astype(str)
    next_holding = next_holding.astype(str).reindex(holding.index).fillna("cash")
    current = pd.to_numeric(current_execution_scale, errors="coerce").fillna(0.0)
    actionable = pd.to_numeric(next_session_actionable_scale, errors="coerce").fillna(current)
    scale = pd.Series(0.0, index=holding.index, dtype=float)
    current_active = holding.ne("cash")
    next_active = next_holding.ne("cash")
    scale.loc[~current_active & next_active] = actionable.loc[~current_active & next_active]
    scale.loc[current_active] = current.loc[current_active]
    return scale.clip(lower=0.0)


def _select_target_vol_return_source(out: pd.DataFrame, fallback: pd.Series) -> tuple[pd.Series, str]:
    for col in ["return_raw", "base_gross_return"]:
        if col in out.columns:
            series = pd.to_numeric(out[col], errors="coerce")
            if series.notna().any():
                return series.fillna(0.0), col
    return fallback, "return_net_fallback_warning"


def _select_base_pre_cost_return(out: pd.DataFrame, base_return_net: pd.Series, base_trade_cost: pd.Series) -> tuple[pd.Series, str]:
    if "overlay_pre_cost_return" in out.columns:
        series = pd.to_numeric(out["overlay_pre_cost_return"], errors="coerce")
        if series.notna().any():
            return series.fillna(0.0), "overlay_pre_cost_return"
    safe_cost = base_trade_cost.clip(lower=0.0, upper=0.99)
    return (1.0 + base_return_net).div(1.0 - safe_cost).sub(1.0), "return_net_cost_reversal"


def apply_target_vol_scaling(base_result: pd.DataFrame) -> pd.DataFrame:
    validate_base_hedge_ratio()
    out = base_result.copy().sort_index()
    base_return_net = pd.to_numeric(out["return_net"], errors="coerce").fillna(0.0)
    target_vol_return, target_vol_return_source = _select_target_vol_return_source(out, base_return_net)
    holding = out["holding"].astype(str)
    next_holding = out.get("next_holding", holding).astype(str)
    active = holding.ne("cash")
    realized_vol = target_vol_return.rolling(TARGET_VOL_WINDOW).std(ddof=1) * np.sqrt(TARGET_VOL_TRADING_DAYS)
    scale_from_realized_vol = _scale_from_realized_vol(realized_vol)
    target_execution_scale = scale_from_realized_vol.shift(1).fillna(1.0)
    execution_scale = apply_scale_rebalance_threshold(
        target_execution_scale,
        active,
        threshold=TARGET_VOL_SCALE_REBALANCE_THRESHOLD,
    )
    next_session_target_scale = scale_from_realized_vol.copy()
    next_session_target_scale.loc[next_holding.eq("cash")] = 0.0
    next_session_target_scale.loc[next_holding.ne("cash")] = next_session_target_scale.loc[next_holding.ne("cash")].fillna(1.0)
    next_session_target_scale = next_session_target_scale.fillna(0.0)
    next_session_actionable_scale = calc_next_session_actionable_scale(
        execution_scale,
        next_session_target_scale,
        next_holding,
        threshold=TARGET_VOL_SCALE_REBALANCE_THRESHOLD,
    )
    target_vol_turnover = _target_vol_turnover_series(holding, execution_scale)
    scale_change_cost = calc_scale_change_cost(holding, target_vol_turnover)
    financing_cost = execution_scale.sub(1.0).clip(lower=0.0) * TARGET_VOL_FINANCING_RATE / TARGET_VOL_TRADING_DAYS
    base_trade_cost = pd.to_numeric(out.get("total_cost", pd.Series(0.0, index=out.index)), errors="coerce").fillna(0.0)
    base_pre_cost_return, base_pre_cost_return_source = _select_base_pre_cost_return(out, base_return_net, base_trade_cost)
    base_trade_cost_scale = calc_base_trade_cost_scale(
        holding,
        next_holding,
        execution_scale,
        next_session_actionable_scale,
    )
    base_trade_cost_scaled = (base_trade_cost * base_trade_cost_scale).clip(lower=0.0, upper=0.99)
    ret = (
        (1.0 + base_pre_cost_return * execution_scale)
        * (1.0 - base_trade_cost_scaled)
        * (1.0 - scale_change_cost)
        * (1.0 - financing_cost)
        - 1.0
    )

    out["target_vol"] = TARGET_VOL
    out["target_vol_window"] = TARGET_VOL_WINDOW
    out["target_vol_return"] = target_vol_return
    out["target_vol_return_source"] = target_vol_return_source
    out["target_vol_realized_vol"] = realized_vol
    out["latest_realized_vol"] = realized_vol
    out["target_vol_scale_raw"] = scale_from_realized_vol
    out["target_vol_execution_scale_raw"] = target_execution_scale
    out["current_execution_scale"] = execution_scale
    out["next_session_target_scale"] = next_session_target_scale
    out["raw_next_target_scale"] = next_session_target_scale
    out["next_session_actionable_scale"] = next_session_actionable_scale
    out["target_vol_scale_next_session"] = next_session_actionable_scale
    out["execution_scale"] = execution_scale
    out["target_vol_turnover"] = target_vol_turnover
    out["target_vol_costed_turnover"] = calc_target_vol_costed_turnover(holding, target_vol_turnover)
    out["scale_change_cost"] = scale_change_cost
    out["target_vol_trade_cost"] = scale_change_cost
    out["financing_cost"] = financing_cost
    out["base_trade_cost"] = base_trade_cost
    out["base_trade_cost_scale"] = base_trade_cost_scale
    out["base_trade_cost_scaled"] = base_trade_cost_scaled
    out["base_pre_cost_return"] = base_pre_cost_return
    out["base_pre_cost_return_source"] = base_pre_cost_return_source
    out["return_net_v1_4"] = base_return_net
    out["nav_net_v1_4"] = pd.to_numeric(out.get("nav_net", pd.Series(np.nan, index=out.index)), errors="coerce")
    out["return_net"] = ret
    out["nav_net"] = (1.0 + out["return_net"].fillna(0.0)).cumprod()
    out["return"] = out["return_net"]
    out["nav"] = out["nav_net"]
    out["version"] = "1.6"
    out["base_version"] = "1.4"
    out["overlay_type"] = "target_volatility_scaling"
    return out


def _build_signal_row(net_df: pd.DataFrame, reference_summary: dict[str, object]) -> pd.DataFrame:
    latest_row = net_df.iloc[-1]
    latest_signal = dict(reference_summary.get("latest_signal", {}))
    current_holding = str(latest_row.get("holding", latest_signal.get("current_holding", "cash")))
    next_holding = str(latest_row.get("next_holding", latest_signal.get("next_holding", current_holding)))
    holding_trade_state = v14_context.v1_1_mod.base_mod.compute_trade_state(current_holding, next_holding)
    current_execution_scale = float(latest_row.get("current_execution_scale", latest_row.get("execution_scale", 0.0)) or 0.0)
    next_session_target_scale = float(
        latest_row.get(
            "next_session_target_scale",
            latest_row.get("target_vol_scale_next_session", current_execution_scale),
        )
        or 0.0
    )
    raw_next_session_actionable_scale = latest_row.get("next_session_actionable_scale", np.nan)
    if pd.notna(raw_next_session_actionable_scale):
        next_session_actionable_scale = float(raw_next_session_actionable_scale)
    else:
        next_session_actionable_scale = float(
            calc_next_session_actionable_scale(
                pd.Series([current_execution_scale]),
                pd.Series([next_session_target_scale]),
                pd.Series([next_holding]),
                threshold=TARGET_VOL_SCALE_REBALANCE_THRESHOLD,
            ).iloc[0]
        )
    raw_scale_delta = next_session_target_scale - current_execution_scale
    actionable_scale_delta = next_session_actionable_scale - current_execution_scale
    scale_delta = actionable_scale_delta
    scale_trade_required = bool(abs(actionable_scale_delta) >= 1e-12)
    scale_trade_state = "rebalance_scale" if scale_trade_required else "hold_scale"
    next_session_leg_turnover = calc_target_vol_turnover(
        current_holding,
        current_execution_scale,
        next_holding,
        next_session_actionable_scale,
    )
    next_session_leg_cost_est_raw = next_session_leg_turnover * TARGET_VOL_SCALE_CHANGE_COST
    same_holding_next = current_holding == next_holding
    next_session_overlay_cost_est = next_session_leg_cost_est_raw if same_holding_next else 0.0
    next_session_trade_cost_est = next_session_overlay_cost_est
    if holding_trade_state == "hold":
        effective_trade_state = scale_trade_state if scale_trade_required else holding_trade_state
    elif scale_trade_required:
        effective_trade_state = f"{holding_trade_state}_and_rebalance_scale"
    else:
        effective_trade_state = holding_trade_state
    latest_signal["current_holding"] = current_holding
    latest_signal["next_holding"] = next_holding
    latest_signal["trade_state"] = effective_trade_state
    latest_signal["effective_trade_state"] = effective_trade_state
    latest_signal["holding_trade_state"] = holding_trade_state
    latest_signal["momentum_trade_state"] = holding_trade_state
    latest_signal["scale_trade_state"] = scale_trade_state
    latest_signal["scale_trade_required"] = scale_trade_required
    latest_signal["raw_scale_delta"] = float(raw_scale_delta)
    latest_signal["actionable_scale_delta"] = float(actionable_scale_delta)
    latest_signal["scale_delta"] = float(scale_delta)
    latest_signal["current_execution_scale"] = float(current_execution_scale)
    latest_signal["next_session_target_scale"] = float(next_session_target_scale)
    latest_signal["raw_next_target_scale"] = float(next_session_target_scale)
    latest_signal["next_session_actionable_scale"] = float(next_session_actionable_scale)
    latest_signal["target_vol_scale_next_session"] = float(next_session_actionable_scale)
    latest_signal["next_session_turnover"] = float(next_session_leg_turnover)
    latest_signal["next_session_leg_turnover"] = float(next_session_leg_turnover)
    latest_signal["next_session_leg_cost_est_raw"] = float(next_session_leg_cost_est_raw)
    latest_signal["next_session_overlay_cost_est"] = float(next_session_overlay_cost_est)
    latest_signal["next_session_trade_cost_est"] = float(next_session_trade_cost_est)
    latest_signal["next_session_overlay_trade_cost_est"] = float(next_session_overlay_cost_est)
    latest_signal["next_session_trade_cost_est_type"] = "overlay_only"
    latest_signal["next_session_total_trade_cost_est_note"] = (
        "entry/exit base cost handled by v1.4 total_cost; not directly estimable here"
    )
    latest_signal["target_vol_signal_timing"] = "close_confirmed"
    latest_signal["signal_timing"] = "close_confirmed"
    latest_signal["official_close_confirmed_signal"] = True
    for src_col in [
        "microcap_close",
        "hedge_close",
        "microcap_mom",
        "hedge_mom",
        "momentum_gap",
        "gap_peak",
        "gap_decay_ratio",
        "execution_scale",
        "current_execution_scale",
        "target_vol_realized_vol",
        "latest_realized_vol",
        "next_session_target_scale",
        "raw_next_target_scale",
        "next_session_actionable_scale",
        "target_vol_scale_next_session",
        "target_vol_turnover",
        "target_vol_costed_turnover",
        "next_session_turnover",
        "next_session_leg_turnover",
        "next_session_leg_cost_est_raw",
        "next_session_overlay_cost_est",
        "next_session_trade_cost_est",
        "next_session_overlay_trade_cost_est",
        "scale_change_cost",
        "target_vol_trade_cost",
        "financing_cost",
    ]:
        if src_col in latest_row and pd.notna(latest_row[src_col]):
            latest_signal[src_col] = float(latest_row[src_col])
    latest_signal["target_vol_scale_next_session"] = float(next_session_actionable_scale)
    latest_signal["next_session_actionable_scale"] = float(next_session_actionable_scale)
    latest_signal["next_session_turnover"] = float(next_session_leg_turnover)
    latest_signal["next_session_leg_turnover"] = float(next_session_leg_turnover)
    latest_signal["next_session_leg_cost_est_raw"] = float(next_session_leg_cost_est_raw)
    latest_signal["next_session_overlay_cost_est"] = float(next_session_overlay_cost_est)
    latest_signal["next_session_trade_cost_est"] = float(next_session_trade_cost_est)
    latest_signal["signal_quality_derisk_triggered"] = bool(latest_row.get("signal_quality_derisk_triggered", False))
    latest_signal["fixed_hedge_ratio"] = BASE_HEDGE_RATIO
    latest_signal["momentum_gap_exit_buffer"] = V1_6_MOMENTUM_GAP_EXIT_BUFFER
    latest_signal["decay_ratio_threshold"] = DECAY_RATIO_THRESHOLD
    latest_signal["derisk_scale"] = DERISK_SCALE
    latest_signal["recovery_ratio_threshold"] = RECOVERY_RATIO_THRESHOLD
    latest_signal["version"] = "1.6"
    latest_signal["base_version"] = "1.4"
    latest_signal["overlay_type"] = "target_volatility_scaling"
    latest_signal["target_vol"] = TARGET_VOL
    latest_signal["target_vol_window"] = TARGET_VOL_WINDOW
    latest_signal["max_leverage"] = TARGET_VOL_MAX_LEVERAGE
    latest_signal.setdefault("signal_label", next_holding)
    return pd.DataFrame([{**latest_signal, "date": pd.Timestamp(net_df.index.max())}])


def summarize_returns(ret: pd.Series) -> dict[str, float | str | int]:
    ret = ret.dropna().astype(float)
    if ret.empty:
        raise ValueError("empty return series")
    nav = (1.0 + ret).cumprod()
    years = (ret.index[-1] - ret.index[0]).days / 365.25
    annual = nav.iloc[-1] ** (1.0 / years) - 1.0 if years > 0 else 0.0
    vol = ret.std(ddof=1) * (TARGET_VOL_TRADING_DAYS**0.5)
    sharpe = annual / vol if vol > 0 else 0.0
    dd = nav / nav.cummax() - 1.0
    return {
        "start_date": str(pd.Timestamp(ret.index[0]).date()),
        "end_date": str(pd.Timestamp(ret.index[-1]).date()),
        "days": int(len(ret)),
        "final_nav": float(nav.iloc[-1]),
        "total_return_pct": float((nav.iloc[-1] - 1.0) * 100.0),
        "annual_pct": float(annual * 100.0),
        "max_drawdown_pct": float(dd.min() * 100.0),
        "sharpe": float(sharpe),
        "vol_pct": float(vol * 100.0),
    }


def summarize_yearly(ret: pd.Series) -> pd.DataFrame:
    rows = []
    for year, part in ret.groupby(ret.index.year):
        part = part.dropna()
        if part.empty:
            continue
        nav = (1.0 + part).cumprod()
        years = (part.index[-1] - part.index[0]).days / 365.25
        annual = nav.iloc[-1] ** (1.0 / years) - 1.0 if years > 0 and len(part) >= 60 else np.nan
        vol = part.std(ddof=1) * (TARGET_VOL_TRADING_DAYS**0.5)
        sharpe = annual / vol if vol > 0 else 0.0
        dd = nav / nav.cummax() - 1.0
        rows.append(
            {
                "year": str(year),
                "start_date": str(pd.Timestamp(part.index[0]).date()),
                "end_date": str(pd.Timestamp(part.index[-1]).date()),
                "days": int(len(part)),
                "return_pct": float((nav.iloc[-1] - 1.0) * 100.0),
                "max_drawdown_pct": float(dd.min() * 100.0),
                "sharpe": float(sharpe),
                "annual_pct": float(annual * 100.0),
            }
        )
    return pd.DataFrame(rows)


def build_performance_payload(ret: pd.Series) -> dict[str, object]:
    ensure_output_dir()
    summary = summarize_returns(ret)
    yearly_df = summarize_yearly(ret)
    yearly_df.to_csv(PERF_YEARLY_CSV, index=False, encoding="utf-8-sig")

    nav_df = pd.DataFrame(
        {
            "date": ret.index,
            "return_net": ret.values,
            "nav_net": (1.0 + ret.fillna(0.0)).cumprod().values,
        }
    )
    nav_df.to_csv(PERF_NAV_CSV, index=False, encoding="utf-8-sig")
    pd.DataFrame([summary]).to_csv(PERF_SUMMARY_CSV, index=False, encoding="utf-8-sig")

    plt.figure(figsize=(12, 6))
    plt.plot(nav_df["date"], nav_df["nav_net"], linewidth=2.0)
    plt.title("Top100 Microcap Mom16 Biweekly v1.6 Target Volatility")
    plt.ylabel("NAV")
    plt.grid(alpha=0.25)
    plt.tight_layout()
    plt.savefig(PERF_PNG, dpi=160)
    plt.close()

    payload = {
        "period_label": "full_sample",
        "source": "costed_v1_6",
        "start_date": summary["start_date"],
        "end_date": summary["end_date"],
        "summary": summary,
        "yearly": yearly_df.to_dict(orient="records"),
        "files": {
            "summary_csv": str(PERF_SUMMARY_CSV),
            "yearly_csv": str(PERF_YEARLY_CSV),
            "nav_csv": str(PERF_NAV_CSV),
            "chart_png": str(PERF_PNG),
        },
    }
    PERF_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def generate_v1_6_outputs() -> tuple[dict[str, object], pd.DataFrame, pd.DataFrame]:
    ensure_output_dir()
    stale_outputs = incompatible_v1_6_outputs()
    reference_summary, _, base_gross_cached, turnover_df = v14_context._load_base_v1_1_context()
    close_df = base_gross_cached[["microcap_close", "hedge_close"]].rename(
        columns={"microcap_close": "microcap", "hedge_close": "hedge"}
    )
    live_overlay_meta: dict[str, object] = {
        "applied": False,
        "reason": "close_confirmed_signal_uses_official_base_series",
    }
    base_gross = v14_context.v1_1_mod.base_mod.run_signal(close_df).sort_index()
    gross = v14_context.v1_1_mod.base_mod.apply_momentum_gap_exit_buffer(
        base_gross,
        V1_6_MOMENTUM_GAP_EXIT_BUFFER,
    )
    base_v1_4 = v14_context.v1_1_mod.base_mod.apply_momentum_gap_peak_decay_derisk(
        gross_result=gross,
        turnover_df=turnover_df,
        decay_ratio_threshold=DECAY_RATIO_THRESHOLD,
        derisk_scale=DERISK_SCALE,
        recovery_ratio_threshold=RECOVERY_RATIO_THRESHOLD,
    )
    out = apply_target_vol_scaling(base_v1_4)
    out.to_csv(COSTED_NAV_CSV, index_label="date", encoding="utf-8-sig")
    out.rename_axis("date").reset_index().to_csv(NAV_CSV, index=False, encoding="utf-8-sig")

    signal_row = _build_signal_row(out, reference_summary)
    LATEST_SIGNAL_CSV.write_text(signal_row.to_csv(index=False), encoding="utf-8")

    perf_payload = build_performance_payload(out["return_net"].fillna(0.0))

    summary = dict(reference_summary)
    summary["strategy"] = OUTPUT_PREFIX
    summary["version"] = "1.6"
    summary["version_role"] = EXPECTED_VERSION_ROLE
    summary["version_note"] = (
        "Target-volatility overlay on top of v1.4. Uses v1.6-specific 0.30% momentum-gap exit buffer, "
        "60-day realized volatility, 25% annual target volatility, max 1.5x leverage, "
        "10bp leg-turnover scale-change cost, scaled v1.4 base trading cost, "
        "and 3% annual financing cost on exposure above 1.0x."
    )
    summary.setdefault("core_params", {})
    summary["core_params"]["fixed_hedge_ratio"] = BASE_HEDGE_RATIO
    summary["core_params"]["momentum_gap_entry_threshold"] = 0.0
    summary["core_params"]["momentum_gap_exit_buffer"] = V1_6_MOMENTUM_GAP_EXIT_BUFFER
    summary["core_params"]["signal_quality_derisk"] = {
        "type": "momentum_gap_peak_decay_derisk_new_peak_guard",
        "decay_ratio_threshold": DECAY_RATIO_THRESHOLD,
        "derisk_scale": DERISK_SCALE,
        "recovery_ratio_threshold": RECOVERY_RATIO_THRESHOLD,
        "rearm_rule": "must set a new trade gap peak after recovery before a later derisk can trigger again",
    }
    summary["core_params"]["target_volatility_scaling"] = {
        "target_vol": TARGET_VOL,
        "vol_window": TARGET_VOL_WINDOW,
        "max_leverage": TARGET_VOL_MAX_LEVERAGE,
        "min_leverage": TARGET_VOL_MIN_LEVERAGE,
        "scale_change_cost": TARGET_VOL_SCALE_CHANGE_COST,
        "scale_change_cost_model": "microcap_long_plus_hedge_leg_net_turnover",
        "scale_rebalance_threshold": TARGET_VOL_SCALE_REBALANCE_THRESHOLD,
        "base_trade_cost_model": (
            "v1_4_total_cost scaled by transition timing: entry uses next actionable scale, "
            "exit and active rebalance use current execution scale"
        ),
        "entry_exit_overlay_cost_model": "target-vol scale-change cost is skipped on holding transition days to avoid double-counting v1.4 entry/exit cost",
        "target_vol_scale_next_session_semantics": "actionable scale after rebalance threshold; raw model target is raw_next_target_scale",
        "idle_cash_return": "not credited when execution_scale < 1.0",
        "volatility_return_source_priority": ["return_raw", "base_gross_return", "return_net_fallback_warning"],
        "pnl_return_source": PNL_RETURN_SOURCE,
        "financing_rate": TARGET_VOL_FINANCING_RATE,
        "trading_days": TARGET_VOL_TRADING_DAYS,
        "timing": "current execution scale uses T-1 realized volatility; next-session target scale uses T close realized volatility",
    }
    summary["latest_trade_date"] = str(pd.Timestamp(signal_row.iloc[0]["date"]).date())
    summary["latest_nav_date"] = str(pd.Timestamp(out.index.max()).date())
    summary["latest_signal"] = signal_row.iloc[0].drop(labels=["date"], errors="ignore").to_dict()
    summary["performance_snapshot"] = perf_payload["summary"]
    summary["base_fingerprint"] = current_base_fingerprint()
    summary["live_microcap_tail_overlay"] = live_overlay_meta
    SUMMARY_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    regenerated_outputs = {
        SUMMARY_JSON,
        LATEST_SIGNAL_CSV,
        NAV_CSV,
        COSTED_NAV_CSV,
        PERF_SUMMARY_CSV,
        PERF_YEARLY_CSV,
        PERF_NAV_CSV,
        PERF_JSON,
        PERF_PNG,
    }
    for stale_path in stale_outputs:
        if stale_path not in regenerated_outputs and stale_path.exists():
            stale_path.unlink(missing_ok=True)
    return summary, signal_row, out


def build_realtime_v1_6_outputs() -> tuple[pd.DataFrame, dict[str, object], pd.DataFrame]:
    ensure_output_dir()
    v1_4_signal, meta, v1_4_realtime = v14_context.build_realtime_v1_4_outputs()
    reference_summary = v14_context._load_reference_summary()
    out = apply_target_vol_scaling(v1_4_realtime)
    signal_row = _build_signal_row(out, reference_summary)
    passthrough_cols = [
        "member_rebalance_state",
        "member_rebalance_required",
        "member_enter_count",
        "member_exit_count",
        "member_rebalance_label",
        "quote_source",
        "hedge_quote_source",
        "member_price_count",
        "member_count",
        "latest_anchor_trade_date",
        "quote_trade_date",
        "snapshot_time",
        "quote_coverage",
        "tail_jitter_risk",
        "tail_jitter_note",
    ]
    for col in passthrough_cols:
        if col in v1_4_signal.columns:
            signal_row[col] = v1_4_signal.iloc[0].get(col)
    for key, value in meta.items():
        signal_row[key] = value
    signal_row["quote_coverage"] = f"{meta.get('member_price_count', 0)}/{meta.get('member_count', 0)}"
    signal_row["target_vol_signal_timing"] = "intraday_hypothetical_if_now_close"
    signal_row["signal_timing"] = "intraday_hypothetical_if_now_close"
    signal_row["official_close_confirmed_signal"] = False
    REALTIME_SIGNAL_CSV.write_text(signal_row.to_csv(index=False), encoding="utf-8")
    return signal_row, meta, out


def _print_signal_query() -> None:
    _, signal_df, _ = generate_v1_6_outputs()
    row = signal_df.iloc[0]
    print("signal")
    print("strategy_version: v1.6")
    print("base_version: v1.4")
    print(
        "overlay: target volatility "
        f"(target={TARGET_VOL:.0%}, window={TARGET_VOL_WINDOW}, max={TARGET_VOL_MAX_LEVERAGE:.1f}x)"
    )
    print(f"current_holding: {row['current_holding']}")
    print(f"next_holding: {row['next_holding']}")
    print(f"trade_state: {row.get('effective_trade_state', row.get('trade_state', 'hold'))}")
    print(f"holding_trade_state: {row.get('holding_trade_state', row.get('momentum_trade_state', 'hold'))}")
    print(f"scale_trade_state: {row.get('scale_trade_state', 'hold_scale')}")
    print(f"signal_date: {pd.Timestamp(row['date']).strftime('%Y-%m-%d')}")
    print(f"momentum_gap: {float(row.get('momentum_gap', 0.0)):+.4%}")
    print(f"current_execution_scale: {float(row.get('current_execution_scale', row.get('execution_scale', 0.0))):.2f}")
    print(f"target_vol_realized_vol: {float(row.get('target_vol_realized_vol', 0.0)):.4%}")
    print(f"raw_next_target_scale: {float(row.get('raw_next_target_scale', row.get('next_session_target_scale', 0.0))):.2f}")
    print(f"next_session_actionable_scale: {float(row.get('next_session_actionable_scale', row.get('next_session_target_scale', 0.0))):.2f}")
    print(f"raw_scale_delta: {float(row.get('raw_scale_delta', row.get('scale_delta', 0.0))):+.2f}")
    print(f"actionable_scale_delta: {float(row.get('actionable_scale_delta', row.get('scale_delta', 0.0))):+.2f}")
    print(f"scale_delta: {float(row.get('scale_delta', 0.0)):+.2f}")
    print(f"next_session_turnover: {float(row.get('next_session_turnover', 0.0)):.4f}")
    print(f"next_session_leg_turnover: {float(row.get('next_session_leg_turnover', row.get('next_session_turnover', 0.0))):.4f}")
    print(f"next_session_leg_cost_est_raw: {float(row.get('next_session_leg_cost_est_raw', 0.0)):.4%}")
    print(f"next_session_overlay_cost_est: {float(row.get('next_session_overlay_cost_est', row.get('next_session_trade_cost_est', 0.0))):.4%}")
    print(f"next_session_trade_cost_est: {float(row.get('next_session_trade_cost_est', 0.0)):.4%}")
    print(f"next_session_trade_cost_est_type: {row.get('next_session_trade_cost_est_type', 'overlay_only')}")
    print(f"official_close_confirmed_signal: {row.get('official_close_confirmed_signal', True)}")
    print(SUMMARY_JSON)
    print(LATEST_SIGNAL_CSV)


def _print_realtime_signal_query() -> None:
    signal_df, meta, _ = build_realtime_v1_6_outputs()
    row = signal_df.iloc[0]
    print("realtime_signal")
    print("strategy_version: v1.6")
    print("base_version: v1.4")
    print(
        "overlay: target volatility "
        f"(target={TARGET_VOL:.0%}, window={TARGET_VOL_WINDOW}, max={TARGET_VOL_MAX_LEVERAGE:.1f}x)"
    )
    print(f"snapshot_time: {meta.get('snapshot_time')}")
    print(f"latest_anchor_trade_date: {meta.get('latest_anchor_trade_date')}")
    print(f"quote_trade_date: {meta.get('quote_trade_date', '')}")
    print(f"current_holding: {row['current_holding']}")
    print(f"next_holding: {row['next_holding']}")
    print(f"trade_state: {row.get('effective_trade_state', row.get('trade_state', 'hold'))}")
    print(f"holding_trade_state: {row.get('holding_trade_state', row.get('momentum_trade_state', 'hold'))}")
    print(f"scale_trade_state: {row.get('scale_trade_state', 'hold_scale')}")
    print("target_vol_signal_timing: intraday_hypothetical_if_now_close")
    print(f"current_execution_scale: {float(row.get('current_execution_scale', row.get('execution_scale', 0.0))):.2f}")
    print(f"target_vol_realized_vol: {float(row.get('target_vol_realized_vol', 0.0)):.4%}")
    print(f"raw_next_target_scale: {float(row.get('raw_next_target_scale', row.get('next_session_target_scale', 0.0))):.2f}")
    print(f"next_session_actionable_scale: {float(row.get('next_session_actionable_scale', row.get('next_session_target_scale', 0.0))):.2f}")
    print(f"raw_scale_delta: {float(row.get('raw_scale_delta', row.get('scale_delta', 0.0))):+.2f}")
    print(f"actionable_scale_delta: {float(row.get('actionable_scale_delta', row.get('scale_delta', 0.0))):+.2f}")
    print(f"scale_delta: {float(row.get('scale_delta', 0.0)):+.2f}")
    print(f"next_session_turnover: {float(row.get('next_session_turnover', 0.0)):.4f}")
    print(f"next_session_leg_turnover: {float(row.get('next_session_leg_turnover', row.get('next_session_turnover', 0.0))):.4f}")
    print(f"next_session_leg_cost_est_raw: {float(row.get('next_session_leg_cost_est_raw', 0.0)):.4%}")
    print(f"next_session_overlay_cost_est: {float(row.get('next_session_overlay_cost_est', row.get('next_session_trade_cost_est', 0.0))):.4%}")
    print(f"next_session_trade_cost_est: {float(row.get('next_session_trade_cost_est', 0.0)):.4%}")
    print(f"next_session_trade_cost_est_type: {row.get('next_session_trade_cost_est_type', 'overlay_only')}")
    print("official_close_confirmed_signal: False")
    print(f"microcap_mom: {float(row.get('microcap_mom', 0.0)):+.4%}")
    print(f"hedge_mom: {float(row.get('hedge_mom', 0.0)):+.4%}")
    print(f"momentum_gap: {float(row.get('momentum_gap', 0.0)):+.4%}")
    print(f"quote_source: {meta.get('quote_source')}")
    print(f"hedge_quote_source: {meta.get('hedge_quote_source')}")
    print(f"quote_coverage: {meta.get('member_price_count')}/{meta.get('member_count')}")
    print(REALTIME_SIGNAL_CSV)


def _print_performance_query(query: str) -> None:
    generate_v1_6_outputs()
    perf_df = pd.read_csv(COSTED_NAV_CSV, parse_dates=["date"]).sort_values("date").set_index("date")
    v14_context.v1_1_mod.base_mod.build_performance_outputs(
        perf_df=perf_df,
        ret_col="return_net",
        nav_col="nav_net",
        source_label="costed_v1_6",
        query_text=query,
        paths={
            "performance_summary": PERF_SUMMARY_CSV,
            "performance_yearly": PERF_YEARLY_CSV,
            "performance_nav": PERF_NAV_CSV,
            "performance_chart": PERF_PNG,
            "performance_json": PERF_JSON,
        },
    )
    print(PERF_PNG)
    print(PERF_SUMMARY_CSV)
    print(PERF_YEARLY_CSV)
    print(PERF_NAV_CSV)
    print(PERF_JSON)


def _handle_query(query: str) -> None:
    if query in {"信号", "淇″彿"}:
        _print_signal_query()
        return
    if query in {"实时信号", "瀹炴椂淇″彿"}:
        _print_realtime_signal_query()
        return
    if v14_context.v1_1_mod.base_mod.PERFORMANCE_PATTERN.search(query):
        _print_performance_query(query)
        return
    raise ValueError("v1.6 supports: 信号 / 实时信号 / 表现 <区间>")


def main() -> None:
    query = " ".join(sys.argv[1:]).strip()
    if query:
        _handle_query(query)
        return
    generate_v1_6_outputs()
    print(str(SUMMARY_JSON))
    print(str(LATEST_SIGNAL_CSV))
    print(str(COSTED_NAV_CSV))


if __name__ == "__main__":
    main()
