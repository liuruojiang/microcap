from __future__ import annotations

import json
import sys
from pathlib import Path

import matplotlib
import numpy as np
import pandas as pd
import requests

matplotlib.use("Agg")
import matplotlib.pyplot as plt

import top100_v14_base_context as v14_context
import top100_realtime_core as realtime_core

# v1.8 intentionally reuses the shared v1.4 base/context adapter; recheck this
# module when that adapter changes its v1_1_mod/base_mod or context API.

ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"

OUTPUT_PREFIX = "microcap_top100_mom16_biweekly_live_v1_8"
SUMMARY_JSON = OUTPUT_DIR / f"{OUTPUT_PREFIX}_summary.json"
LATEST_SIGNAL_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_latest_signal.csv"
REALTIME_SIGNAL_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_realtime_signal.csv"
NAV_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_nav.csv"
COSTED_NAV_CSV = OUTPUT_DIR / "microcap_top100_mom11_targetvol30_max2_v1_8_costed_nav.csv"
LEGACY_COSTED_NAV_CSV = OUTPUT_DIR / "microcap_top100_mom16_targetvol25_max1p5_v1_8_costed_nav.csv"
PERF_SUMMARY_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_summary.csv"
PERF_YEARLY_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_yearly.csv"
PERF_NAV_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_nav.csv"
PERF_JSON = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_summary.json"
PERF_PNG = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_curve.png"

EXPECTED_VERSION_ROLE = "target_vol_overlay_on_v1_4"
EXPECTED_VERSION_NOTE_PREFIX = "Recommended v1.8 overlay on top of v1.4 data/context."
STRATEGY_VERSION = "1.8"
BASE_HEDGE_RATIO = 0.8
LOOKBACK = 11
ENTRY_GAP_THRESHOLD = 0.006
V1_8_MOMENTUM_GAP_EXIT_BUFFER = 0.006
DECAY_RATIO_THRESHOLD = 0.30
DERISK_SCALE = 0.0
RECOVERY_RATIO_THRESHOLD = 0.30
TARGET_VOL = 0.30
TARGET_VOL_WINDOW = 20
TARGET_VOL_MAX_LEVERAGE = 2.0
TARGET_VOL_MIN_LEVERAGE = 0.0
TARGET_VOL_TRADING_DAYS = 244
TARGET_VOL_SCALE_CHANGE_COST = 0.001
TARGET_VOL_SCALE_REBALANCE_THRESHOLD = 0.25
TARGET_VOL_FINANCING_RATE = 0.03
VOLUME_FILTER_FAMILY = "zz2000_and_cyb"
VOLUME_FILTER_MA = 53
VOLUME_FILTER_CONSECUTIVE_DAYS = 13
VOLUME_FILTER_SCALE = 0.25
VOLUME_FILTER_SCALE_CHANGE_COST = 0.003
NAV_DD_TRIGGER = 0.13
NAV_DD_SCALE = 0.80
NAV_DD_RECOVER = 0.06
NAV_DD_SCALE_CHANGE_COST = 0.0002
PNL_RETURN_SOURCE = "v1_4_overlay_pre_cost_return_explicit_or_return_net_cost_reversal_fallback"
LIVE_CONTEXT_CACHE = ROOT / ".autobuild_top100_cache" / "context_cache_v1_8.json"


def _csv_safe_meta_value(value: object) -> object:
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, default=str)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def validate_base_hedge_ratio() -> None:
    v1_1_mod = getattr(v14_context, "v1_1_mod", None)
    if v1_1_mod is None:
        raise RuntimeError("missing v14_context.v1_1_mod; cannot validate v1.8 base hedge ratio")
    base_mod = getattr(v1_1_mod, "base_mod", None)
    if base_mod is None:
        raise RuntimeError("missing v14_context.v1_1_mod.base_mod; cannot validate v1.8 base hedge ratio")
    checks = {
        "v14_context.BASE_HEDGE_RATIO": getattr(v14_context, "BASE_HEDGE_RATIO", None),
        "v14_context.v1_1_mod.base_mod.FIXED_HEDGE_RATIO": getattr(base_mod, "FIXED_HEDGE_RATIO", None),
    }
    for name, value in checks.items():
        if value is None:
            raise RuntimeError(f"missing {name}; cannot validate v1.8 base hedge ratio")
        if abs(float(value) - float(BASE_HEDGE_RATIO)) > 1e-9:
            raise ValueError(f"hedge ratio mismatch: v1.8={BASE_HEDGE_RATIO}, {name}={value}")


def current_base_fingerprint() -> dict[str, object]:
    validate_base_hedge_ratio()
    base = dict(v14_context.current_base_fingerprint())
    base["momentum_gap_exit_buffer"] = V1_8_MOMENTUM_GAP_EXIT_BUFFER
    return {
        "base_version": "1.4",
        "base_v1_4_fingerprint": base,
        "overlay_type": "target_volatility_nav_dd_scaling",
        "base_hedge_ratio": BASE_HEDGE_RATIO,
        "lookback": LOOKBACK,
        "momentum_gap_entry_threshold": ENTRY_GAP_THRESHOLD,
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
        "momentum_gap_exit_buffer": V1_8_MOMENTUM_GAP_EXIT_BUFFER,
        "decay_ratio_threshold": DECAY_RATIO_THRESHOLD,
        "derisk_scale": DERISK_SCALE,
        "recovery_ratio_threshold": RECOVERY_RATIO_THRESHOLD,
        "broad_volume_filter": "not_used_in_official_v1_8_chain",
        "nav_drawdown_throttle": {
            "trigger_drawdown": NAV_DD_TRIGGER,
            "scale": NAV_DD_SCALE,
            "recover_drawdown": NAV_DD_RECOVER,
            "scale_change_cost": NAV_DD_SCALE_CHANGE_COST,
            "timing": "T close confirmed, T+1 execution",
        },
    }


def summary_matches_current_v1_8_base(summary: dict[str, object]) -> bool:
    if not isinstance(summary, dict):
        return False
    if str(summary.get("version")) != STRATEGY_VERSION:
        return False
    if str(summary.get("version_role")) != EXPECTED_VERSION_ROLE:
        return False
    if not str(summary.get("version_note", "")).startswith(EXPECTED_VERSION_NOTE_PREFIX):
        return False
    return summary.get("base_fingerprint") == current_base_fingerprint()


def invalidate_incompatible_v1_8_outputs() -> list[Path]:
    stale = incompatible_v1_8_outputs()
    removed: list[Path] = []
    for path in stale:
        if path.exists():
            path.unlink(missing_ok=True)
            removed.append(path)
    return removed


def incompatible_v1_8_outputs() -> list[Path]:
    if not SUMMARY_JSON.exists():
        return []
    try:
        summary = json.loads(SUMMARY_JSON.read_text(encoding="utf-8"))
    except Exception:
        summary = None
    if summary_matches_current_v1_8_base(summary):
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


# Experimental helper: currently unused by the close-confirmed and realtime v1.8 signal paths.
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


# Experimental helper: currently unused by the close-confirmed and realtime v1.8 signal paths.
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


# Experimental helper: currently unused by the close-confirmed and realtime v1.8 signal paths.
def _recent_microcap_tail_is_flat(close_df: pd.DataFrame, tail_days: int = 5) -> bool:
    if close_df.empty or "microcap" not in close_df.columns:
        return False
    tail = pd.to_numeric(close_df["microcap"], errors="coerce").dropna().tail(tail_days)
    if len(tail) < max(3, tail_days):
        return False
    return bool(tail.pct_change().dropna().abs().le(1e-12).all())


# Experimental helper: currently unused by the close-confirmed and realtime v1.8 signal paths.
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


def run_candidate_momentum_signal(close_df: pd.DataFrame) -> pd.DataFrame:
    base_mod = v14_context.v1_1_mod.base_mod
    return base_mod.hedge_mod.run_backtest(
        close_df=close_df,
        signal_model="momentum",
        lookback=LOOKBACK,
        bias_n=base_mod.hedge_mod.DEFAULT_BIAS_N,
        bias_mom_day=base_mod.hedge_mod.DEFAULT_BIAS_MOM_DAY,
        futures_drag=base_mod.FUTURES_DRAG * BASE_HEDGE_RATIO,
        require_positive_microcap_mom=base_mod.REQUIRE_POSITIVE_MICROCAP_MOM,
        r2_window=base_mod.hedge_mod.DEFAULT_R2_WINDOW,
        r2_threshold=0.0,
        vol_scale_enabled=False,
        target_vol=base_mod.hedge_mod.DEFAULT_TARGET_VOL,
        vol_window=base_mod.hedge_mod.DEFAULT_VOL_WINDOW,
        max_lev=base_mod.hedge_mod.DEFAULT_MAX_LEV,
        min_lev=base_mod.hedge_mod.DEFAULT_MIN_LEV,
        scale_threshold=base_mod.hedge_mod.DEFAULT_SCALE_THRESHOLD,
        hedge_ratio=BASE_HEDGE_RATIO,
    ).sort_index()


def apply_entry_exit_thresholds(gross_result: pd.DataFrame) -> pd.DataFrame:
    base_mod = v14_context.v1_1_mod.base_mod
    out = gross_result.copy().sort_index()
    required = {"microcap_ret", "hedge_ret", "microcap_mom", "momentum_gap"}
    missing = required.difference(out.columns)
    if missing:
        raise KeyError(f"Missing columns for v1.8 entry/exit threshold logic: {sorted(missing)}")

    holding = False
    exit_gap_threshold = -float(V1_8_MOMENTUM_GAP_EXIT_BUFFER)
    rows: list[dict[str, object]] = []
    for _, row in out.iterrows():
        active_ret = 0.0
        drag = base_mod.FUTURES_DRAG * BASE_HEDGE_RATIO if holding else 0.0
        if holding and pd.notna(row["microcap_ret"]) and pd.notna(row["hedge_ret"]):
            active_ret = float(row["microcap_ret"] - BASE_HEDGE_RATIO * row["hedge_ret"])

        gap = float(row["momentum_gap"]) if pd.notna(row["momentum_gap"]) else np.nan
        microcap_mom = float(row["microcap_mom"]) if pd.notna(row["microcap_mom"]) else np.nan
        valid = pd.notna(gap)
        if base_mod.REQUIRE_POSITIVE_MICROCAP_MOM:
            valid = valid and pd.notna(microcap_mom) and microcap_mom > 0.0
        if not valid:
            signal_on = False
        elif holding:
            signal_on = gap >= exit_gap_threshold
        else:
            signal_on = gap > ENTRY_GAP_THRESHOLD

        day_ret = active_ret - drag
        rows.append(
            {
                "holding": "long_microcap_short_zz1000" if holding else "cash",
                "next_holding": "long_microcap_short_zz1000" if signal_on else "cash",
                "signal_on": bool(signal_on),
                "return_raw": day_ret,
                "return": day_ret,
                "futures_drag": drag,
                "active_spread_ret": active_ret,
            }
        )
        holding = bool(signal_on)

    adjusted = pd.DataFrame(rows, index=out.index)
    for col in adjusted.columns:
        out[col] = adjusted[col]
    out["entry_gap_threshold"] = ENTRY_GAP_THRESHOLD
    out["exit_gap_buffer"] = V1_8_MOMENTUM_GAP_EXIT_BUFFER
    out["exit_gap_threshold"] = exit_gap_threshold
    out["momentum_gap_entry_threshold"] = ENTRY_GAP_THRESHOLD
    out["momentum_gap_exit_buffer"] = V1_8_MOMENTUM_GAP_EXIT_BUFFER
    return out


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
    out["version"] = STRATEGY_VERSION
    out["base_version"] = "1.4"
    out["overlay_type"] = "target_volatility_scaling"
    return out


def fetch_eastmoney_amount(secid: str, name: str) -> pd.DataFrame:
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "secid": secid,
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57",
        "klt": "101",
        "fqt": "0",
        "beg": "20100101",
        "end": pd.Timestamp.today().strftime("%Y%m%d"),
    }
    last_error: Exception | None = None
    for _ in range(3):
        try:
            response = requests.get(url, params=params, timeout=40)
            response.raise_for_status()
            data = response.json().get("data")
            if not data or not data.get("klines"):
                raise RuntimeError(f"empty EastMoney kline data for {secid}")
            rows = []
            for item in data["klines"]:
                parts = item.split(",")
                rows.append((pd.to_datetime(parts[0], errors="coerce"), pd.to_numeric(parts[6], errors="coerce")))
            out = (
                pd.DataFrame(rows, columns=["date", name])
                .dropna()
                .sort_values("date")
                .drop_duplicates("date", keep="last")
                .set_index("date")
            )
            if out.empty:
                raise RuntimeError(f"no valid amount rows for {secid}")
            return out
        except Exception as exc:  # noqa: BLE001 - retry and report final upstream failure.
            last_error = exc
    raise RuntimeError(f"failed EastMoney amount fetch for {secid}: {last_error}")


def load_broad_volume_amount() -> pd.DataFrame:
    frames = [
        fetch_eastmoney_amount("2.932000", "zz2000"),
        fetch_eastmoney_amount("0.399006", "cyb"),
    ]
    return pd.concat(frames, axis=1).dropna(how="all").sort_index()


def build_broad_volume_signal(amount: pd.DataFrame) -> pd.Series:
    amount = amount.sort_index()
    zz_below = amount["zz2000"] < amount["zz2000"].rolling(VOLUME_FILTER_MA).mean()
    cyb_below = amount["cyb"] < amount["cyb"].rolling(VOLUME_FILTER_MA).mean()
    condition = zz_below.fillna(False) & cyb_below.fillna(False)
    run_id = condition.ne(condition.shift(fill_value=False)).cumsum()
    consecutive = condition.groupby(run_id).cumcount() + 1
    return (condition & consecutive.ge(VOLUME_FILTER_CONSECUTIVE_DAYS)).rename("volume_signal")


def apply_broad_volume_filter(base_result: pd.DataFrame, amount_signal: pd.Series) -> pd.DataFrame:
    out = base_result.copy().sort_index()
    base_return = pd.to_numeric(out["return_net"], errors="coerce").fillna(0.0)
    signal_on_nav_dates = amount_signal.reindex(out.index).astype("boolean").fillna(False).astype(bool)
    execution_scale = pd.Series(1.0, index=out.index, dtype=float)
    execution_scale.loc[signal_on_nav_dates.shift(1, fill_value=False)] = float(VOLUME_FILTER_SCALE)
    next_session_scale = pd.Series(1.0, index=out.index, dtype=float)
    next_session_scale.loc[signal_on_nav_dates] = float(VOLUME_FILTER_SCALE)
    scale_change = execution_scale.diff().abs().fillna(0.0)
    active_exposure = pd.to_numeric(
        out.get("current_execution_scale", pd.Series(1.0, index=out.index)),
        errors="coerce",
    ).fillna(0.0).gt(1e-12).astype(float)
    overlay_cost = scale_change * active_exposure * VOLUME_FILTER_SCALE_CHANGE_COST
    ret = base_return * execution_scale - overlay_cost

    out["return_net_before_volume_filter"] = base_return
    out["nav_net_before_volume_filter"] = (1.0 + base_return).cumprod()
    out["volume_signal"] = signal_on_nav_dates
    out["volume_execution_scale"] = execution_scale
    out["volume_next_session_scale"] = next_session_scale
    out["volume_scale_change"] = scale_change
    out["volume_cost_active"] = active_exposure
    out["volume_overlay_cost"] = overlay_cost
    out["return_net"] = ret
    out["nav_net"] = (1.0 + ret.fillna(0.0)).cumprod()
    out["return"] = out["return_net"]
    out["nav"] = out["nav_net"]
    out["version"] = STRATEGY_VERSION
    out["overlay_type"] = "target_volatility_plus_broad_volume_filter"
    return out


def apply_nav_drawdown_throttle(base_result: pd.DataFrame) -> pd.DataFrame:
    out = base_result.copy().sort_index()
    base_return = pd.to_numeric(out["return_net"], errors="coerce").fillna(0.0)
    pre_dd_active = (
        pd.to_numeric(out.get("current_execution_scale", pd.Series(1.0, index=out.index)), errors="coerce").fillna(1.0)
        * pd.to_numeric(out.get("volume_execution_scale", pd.Series(1.0, index=out.index)), errors="coerce").fillna(1.0)
    ).gt(1e-12).astype(float)
    execution_scale_values: list[float] = []
    next_scale_values: list[float] = []
    scale_change_values: list[float] = []
    cost_values: list[float] = []
    ret_values: list[float] = []
    dd_values: list[float] = []

    nav = 1.0
    high_water = 1.0
    prev_scale = 1.0
    current_scale = 1.0
    for value, active_cost_flag in zip(base_return, pre_dd_active):
        scale_change = abs(current_scale - prev_scale)
        cost = scale_change * float(active_cost_flag) * NAV_DD_SCALE_CHANGE_COST
        ret = float(value) * current_scale - cost
        nav *= 1.0 + ret
        high_water = max(high_water, nav)
        drawdown = nav / high_water - 1.0 if high_water > 0 else 0.0
        if drawdown <= -NAV_DD_TRIGGER:
            next_scale = NAV_DD_SCALE
        elif drawdown >= -NAV_DD_RECOVER:
            next_scale = 1.0
        else:
            next_scale = current_scale

        execution_scale_values.append(float(current_scale))
        next_scale_values.append(float(next_scale))
        scale_change_values.append(float(scale_change))
        cost_values.append(float(cost))
        ret_values.append(float(ret))
        dd_values.append(float(drawdown))
        prev_scale = current_scale
        current_scale = float(next_scale)

    execution_scale = pd.Series(execution_scale_values, index=out.index, dtype=float)
    next_session_scale = pd.Series(next_scale_values, index=out.index, dtype=float)
    scale_change = pd.Series(scale_change_values, index=out.index, dtype=float)
    overlay_cost = pd.Series(cost_values, index=out.index, dtype=float)
    ret = pd.Series(ret_values, index=out.index, dtype=float)

    out["return_net_before_nav_dd"] = base_return
    out["nav_net_before_nav_dd"] = (1.0 + base_return).cumprod()
    out["nav_dd_execution_scale"] = execution_scale
    out["nav_dd_next_session_scale"] = next_session_scale
    out["nav_dd_scale_change"] = scale_change
    out["nav_dd_cost_active"] = pre_dd_active
    out["nav_dd_overlay_cost"] = overlay_cost
    out["nav_dd_drawdown"] = pd.Series(dd_values, index=out.index, dtype=float)
    out["nav_dd_triggered"] = next_session_scale.lt(1.0 - 1e-12)
    out["return_net"] = ret
    out["nav_net"] = (1.0 + ret.fillna(0.0)).cumprod()
    out["return"] = out["return_net"]
    out["nav"] = out["nav_net"]
    out["version"] = STRATEGY_VERSION
    out["overlay_type"] = "target_volatility_plus_nav_dd_throttle"
    return out


def finalize_overlay_execution_scales(out: pd.DataFrame) -> pd.DataFrame:
    result = out.copy()
    target_current = pd.to_numeric(result.get("current_execution_scale", pd.Series(0.0, index=result.index)), errors="coerce").fillna(0.0)
    target_next = pd.to_numeric(
        result.get("next_session_actionable_scale", result.get("target_vol_scale_next_session", target_current)),
        errors="coerce",
    ).fillna(target_current)
    volume_current = pd.to_numeric(result.get("volume_execution_scale", pd.Series(1.0, index=result.index)), errors="coerce").fillna(1.0)
    volume_next = pd.to_numeric(result.get("volume_next_session_scale", volume_current), errors="coerce").fillna(volume_current)
    dd_current = pd.to_numeric(result.get("nav_dd_execution_scale", pd.Series(1.0, index=result.index)), errors="coerce").fillna(1.0)
    dd_next = pd.to_numeric(result.get("nav_dd_next_session_scale", dd_current), errors="coerce").fillna(dd_current)

    result["target_vol_current_execution_scale"] = target_current
    result["target_vol_next_session_actionable_scale"] = target_next
    result["current_execution_scale"] = target_current * volume_current * dd_current
    result["execution_scale"] = result["current_execution_scale"]
    result["next_session_actionable_scale"] = target_next * volume_next * dd_next
    result["target_vol_scale_next_session"] = result["next_session_actionable_scale"]
    result["next_session_target_scale"] = result["next_session_actionable_scale"]
    result["raw_next_target_scale"] = target_next * volume_next * dd_next
    holding = result.get("holding", pd.Series("cash", index=result.index)).astype(str)
    next_holding = result.get("next_holding", holding).astype(str)
    result["next_session_turnover"] = [
        calc_target_vol_turnover(h, s, nh, ns)
        for h, s, nh, ns in zip(
            holding,
            result["current_execution_scale"],
            next_holding,
            result["next_session_actionable_scale"],
        )
    ]
    result["next_session_leg_turnover"] = result["next_session_turnover"]
    result["version"] = STRATEGY_VERSION
    result["overlay_type"] = "target_volatility_plus_nav_dd_throttle"
    return result


def apply_v1_8_overlays(target_vol_result: pd.DataFrame) -> pd.DataFrame:
    dd_throttled = apply_nav_drawdown_throttle(target_vol_result)
    return finalize_overlay_execution_scales(dd_throttled)


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
        "target_vol_current_execution_scale",
        "target_vol_next_session_actionable_scale",
        "volume_execution_scale",
        "volume_next_session_scale",
        "volume_scale_change",
        "volume_overlay_cost",
        "nav_dd_execution_scale",
        "nav_dd_next_session_scale",
        "nav_dd_scale_change",
        "nav_dd_overlay_cost",
        "nav_dd_drawdown",
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
    latest_signal["volume_signal"] = bool(latest_row.get("volume_signal", False))
    latest_signal["nav_dd_triggered"] = bool(latest_row.get("nav_dd_triggered", False))
    latest_signal["fixed_hedge_ratio"] = BASE_HEDGE_RATIO
    latest_signal["lookback"] = LOOKBACK
    latest_signal["momentum_gap_entry_threshold"] = ENTRY_GAP_THRESHOLD
    latest_signal["momentum_gap_exit_buffer"] = V1_8_MOMENTUM_GAP_EXIT_BUFFER
    latest_signal["decay_ratio_threshold"] = DECAY_RATIO_THRESHOLD
    latest_signal["derisk_scale"] = DERISK_SCALE
    latest_signal["recovery_ratio_threshold"] = RECOVERY_RATIO_THRESHOLD
    latest_signal["broad_volume_filter_active"] = False
    latest_signal["nav_dd_trigger"] = NAV_DD_TRIGGER
    latest_signal["nav_dd_scale"] = NAV_DD_SCALE
    latest_signal["nav_dd_recover"] = NAV_DD_RECOVER
    latest_signal["version"] = STRATEGY_VERSION
    latest_signal["base_version"] = "1.4"
    latest_signal["overlay_type"] = "target_volatility_plus_nav_dd_throttle"
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
    plt.title("Top100 Microcap Mom11 Biweekly v1.8 Recommended")
    plt.ylabel("NAV")
    plt.grid(alpha=0.25)
    plt.tight_layout()
    plt.savefig(PERF_PNG, dpi=160)
    plt.close()

    payload = {
        "period_label": "full_sample",
        "source": "costed_v1_8",
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


def generate_v1_8_outputs() -> tuple[dict[str, object], pd.DataFrame, pd.DataFrame]:
    ensure_output_dir()
    stale_outputs = incompatible_v1_8_outputs()
    reference_summary, _, base_gross_cached, turnover_df = v14_context._load_base_v1_1_context()
    close_df = base_gross_cached[["microcap_close", "hedge_close"]].rename(
        columns={"microcap_close": "microcap", "hedge_close": "hedge"}
    )
    live_overlay_meta: dict[str, object] = {
        "applied": False,
        "reason": "close_confirmed_signal_uses_official_base_series",
    }
    base_gross = run_candidate_momentum_signal(close_df)
    gross = apply_entry_exit_thresholds(base_gross)
    base_v1_4 = v14_context.v1_1_mod.base_mod.apply_momentum_gap_peak_decay_derisk(
        gross_result=gross,
        turnover_df=turnover_df,
        decay_ratio_threshold=DECAY_RATIO_THRESHOLD,
        derisk_scale=DERISK_SCALE,
        recovery_ratio_threshold=RECOVERY_RATIO_THRESHOLD,
    )
    base_v1_4 = v14_context.v1_1_mod.base_mod.ensure_overlay_pre_cost_return(base_v1_4)
    target_vol = apply_target_vol_scaling(base_v1_4)
    out = apply_v1_8_overlays(target_vol)
    if COSTED_NAV_CSV.exists():
        previous = pd.read_csv(COSTED_NAV_CSV)
        v14_context.v1_1_mod.base_mod.assert_no_historical_rewrite(
            previous=previous,
            candidate=out.rename_axis("date").reset_index(),
            key_columns=["return_net", "holding", "next_holding", "base_pre_cost_return"],
            allowed_tail_rows=5,
            label="v1.8 official costed NAV",
            audit_path=OUTPUT_DIR / f"{OUTPUT_PREFIX}_historical_rewrite_audit.csv",
        )
    out.to_csv(COSTED_NAV_CSV, index_label="date", encoding="utf-8-sig")
    out.rename_axis("date").reset_index().to_csv(NAV_CSV, index=False, encoding="utf-8-sig")

    signal_row = _build_signal_row(out, reference_summary)
    LATEST_SIGNAL_CSV.write_text(signal_row.to_csv(index=False), encoding="utf-8")

    perf_payload = build_performance_payload(out["return_net"].fillna(0.0))

    summary = dict(reference_summary)
    summary["strategy"] = OUTPUT_PREFIX
    summary["version"] = STRATEGY_VERSION
    summary["version_role"] = EXPECTED_VERSION_ROLE
    summary["version_note"] = (
        "Recommended v1.8 overlay on top of v1.4 data/context. Uses 11-day relative momentum, "
        "0.60% entry gap threshold, 0.60% momentum-gap exit buffer, 30-day signal-quality decay/recovery, "
        "20-day realized volatility, 30% annual target volatility, max 2.0x leverage, "
        "and NAV drawdown throttle DD13/80/rec6. Broad-volume is not used in the official v1.8 chain."
    )
    summary.setdefault("core_params", {})
    summary["core_params"]["lookback"] = LOOKBACK
    summary["core_params"]["fixed_hedge_ratio"] = BASE_HEDGE_RATIO
    summary["core_params"]["momentum_gap_entry_threshold"] = ENTRY_GAP_THRESHOLD
    summary["core_params"]["momentum_gap_exit_buffer"] = V1_8_MOMENTUM_GAP_EXIT_BUFFER
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
    summary["core_params"]["broad_volume_filter"] = "not_used_in_official_v1_8_chain"
    summary["core_params"]["nav_drawdown_throttle"] = {
        "trigger_drawdown": NAV_DD_TRIGGER,
        "scale": NAV_DD_SCALE,
        "recover_drawdown": NAV_DD_RECOVER,
        "scale_change_cost": NAV_DD_SCALE_CHANGE_COST,
        "timing": "T close confirmed, T+1 execution",
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


def build_realtime_v1_8_outputs() -> tuple[pd.DataFrame, dict[str, object], pd.DataFrame]:
    ensure_output_dir()
    realtime_base = realtime_core.load_realtime_base()
    meta = realtime_base.meta
    base_gross = run_candidate_momentum_signal(realtime_base.realtime_close_df)
    gross = apply_entry_exit_thresholds(base_gross)
    base = realtime_core.base_mod.apply_momentum_gap_peak_decay_derisk(
        gross_result=gross,
        turnover_df=realtime_base.turnover_df,
        decay_ratio_threshold=DECAY_RATIO_THRESHOLD,
        derisk_scale=DERISK_SCALE,
        recovery_ratio_threshold=RECOVERY_RATIO_THRESHOLD,
    )
    base = realtime_core.base_mod.ensure_overlay_pre_cost_return(base)
    target_vol = apply_target_vol_scaling(base)
    out = apply_v1_8_overlays(target_vol)
    signal_row = _build_signal_row(out, realtime_base.reference_summary)
    signal_row = realtime_core.base_mod.augment_signal_with_member_rebalance(
        signal_row,
        realtime_base.context.get("changes_df"),
    )
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
        if col in signal_row.columns:
            continue
    realtime_core.apply_realtime_meta_to_signal_row(signal_row, meta)
    signal_row["quote_coverage"] = f"{meta.get('member_price_count', 0)}/{meta.get('member_count', 0)}"
    signal_row["target_vol_signal_timing"] = "intraday_hypothetical_if_now_close"
    signal_row["signal_timing"] = "intraday_hypothetical_if_now_close"
    signal_row["official_close_confirmed_signal"] = False
    REALTIME_SIGNAL_CSV.write_text(signal_row.to_csv(index=False), encoding="utf-8")
    return signal_row, meta, out


def _print_signal_query() -> None:
    _, signal_df, _ = generate_v1_8_outputs()
    row = signal_df.iloc[0]
    print("signal")
    print("strategy_version: v1.8")
    print("base_version: v1.4")
    print(
        "overlay: target volatility + NAV-DD "
        f"(target={TARGET_VOL:.0%}, window={TARGET_VOL_WINDOW}, max={TARGET_VOL_MAX_LEVERAGE:.1f}x, broad_volume=False)"
    )
    print(f"current_holding: {row['current_holding']}")
    print(f"next_holding: {row['next_holding']}")
    print(f"trade_state: {row.get('effective_trade_state', row.get('trade_state', 'hold'))}")
    print(f"holding_trade_state: {row.get('holding_trade_state', row.get('momentum_trade_state', 'hold'))}")
    print(f"scale_trade_state: {row.get('scale_trade_state', 'hold_scale')}")
    print(f"signal_date: {pd.Timestamp(row['date']).strftime('%Y-%m-%d')}")
    print(f"momentum_gap: {float(row.get('momentum_gap', 0.0)):+.4%}")
    print(f"current_execution_scale: {float(row.get('current_execution_scale', row.get('execution_scale', 0.0))):.2f}")
    print(f"target_vol_current_execution_scale: {float(row.get('target_vol_current_execution_scale', row.get('current_execution_scale', 0.0))):.2f}")
    print(f"broad_volume_filter_active: {row.get('broad_volume_filter_active', False)}")
    print(f"nav_dd_triggered: {row.get('nav_dd_triggered', False)}")
    print(f"nav_dd_execution_scale: {float(row.get('nav_dd_execution_scale', 1.0)):.2f}")
    print(f"nav_dd_next_session_scale: {float(row.get('nav_dd_next_session_scale', 1.0)):.2f}")
    print(f"nav_dd_drawdown: {float(row.get('nav_dd_drawdown', 0.0)):+.2%}")
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
    signal_df, meta, _ = build_realtime_v1_8_outputs()
    row = signal_df.iloc[0]
    print("realtime_signal")
    print("strategy_version: v1.8")
    print("base_version: v1.4")
    print(
        "overlay: target volatility + NAV-DD "
        f"(target={TARGET_VOL:.0%}, window={TARGET_VOL_WINDOW}, max={TARGET_VOL_MAX_LEVERAGE:.1f}x, broad_volume=False)"
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
    print(f"target_vol_current_execution_scale: {float(row.get('target_vol_current_execution_scale', row.get('current_execution_scale', 0.0))):.2f}")
    print(f"broad_volume_filter_active: {row.get('broad_volume_filter_active', False)}")
    print(f"nav_dd_triggered: {row.get('nav_dd_triggered', False)}")
    print(f"nav_dd_execution_scale: {float(row.get('nav_dd_execution_scale', 1.0)):.2f}")
    print(f"nav_dd_next_session_scale: {float(row.get('nav_dd_next_session_scale', 1.0)):.2f}")
    print(f"nav_dd_drawdown: {float(row.get('nav_dd_drawdown', 0.0)):+.2%}")
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
    generate_v1_8_outputs()
    perf_df = pd.read_csv(COSTED_NAV_CSV, parse_dates=["date"]).sort_values("date").set_index("date")
    v14_context.v1_1_mod.base_mod.build_performance_outputs(
        perf_df=perf_df,
        ret_col="return_net",
        nav_col="nav_net",
        source_label="costed_v1_8",
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
    raise ValueError("v1.8 supports: 信号 / 实时信号 / 表现 <区间>")


def main() -> None:
    query = " ".join(sys.argv[1:]).strip()
    if query:
        _handle_query(query)
        return
    generate_v1_8_outputs()
    print(str(SUMMARY_JSON))
    print(str(LATEST_SIGNAL_CSV))
    print(str(COSTED_NAV_CSV))


if __name__ == "__main__":
    main()


