from __future__ import annotations

import copy
import json
import math
import sys
import time
import warnings
from pathlib import Path

import matplotlib
import numpy as np
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt

import microcap_top100_mom16_biweekly_live_v2_0 as v2_0


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"

OUTPUT_PREFIX = "microcap_top100_mom16_biweekly_live_v2_4"
VERSION = "2.4"
EXPECTED_VERSION_ROLE = "spread_nav_power_wma_gap_peak_decay_target_vol_overlay"
EXPECTED_VERSION_NOTE_PREFIX = "Formal v2.4 spread-NAV Power-WMA target-volatility overlay."
LOOKBACK = 20
POWER = 0.75
MOMENTUM_GAP_EXIT_BUFFER = 0.18
DECAY_RATIO_THRESHOLD = 0.35
DERISK_SCALE = 0.0
RECOVERY_RATIO_THRESHOLD = 0.40
MIN_PEAK_TO_ARM_DECAY = 0.15
TARGET_VOL = 0.25
TARGET_VOL_SCALE_REBALANCE_THRESHOLD = 0.10
FORMAL_START_DATE = pd.Timestamp("2010-05-05")

SIGNAL_SPREAD_HEDGE_RATIO = 1.0
EXECUTION_HEDGE_RATIO = float(v2_0.BASE_HEDGE_RATIO)
BASE_HEDGE_RATIO = EXECUTION_HEDGE_RATIO
TRADING_DAYS = int(v2_0.overlay_mod.TARGET_VOL_TRADING_DAYS)


def _format_float_tag(value: float) -> str:
    text = f"{float(value):.6f}".rstrip("0").rstrip(".")
    if "." not in text:
        text += ".0"
    return text.replace("-", "m").replace(".", "p")


def execution_tag() -> str:
    return f"exec{_format_float_tag(EXECUTION_HEDGE_RATIO)}"


POWER_TAG = f"p{_format_float_tag(POWER)}"
SIGNAL_TAG = f"signal{_format_float_tag(SIGNAL_SPREAD_HEDGE_RATIO)}"
BUFFER_TAG = f"gap{int(round(MOMENTUM_GAP_EXIT_BUFFER * 100)):02d}"
DECAY_TAG = f"decay{int(round(DECAY_RATIO_THRESHOLD * 100)):02d}"
RECOVERY_TAG = f"recovery{int(round(RECOVERY_RATIO_THRESHOLD * 100)):02d}"
TARGET_VOL_TAG = f"targetvol{int(round(TARGET_VOL * 100)):02d}"
SCALE_TAG = f"scale{int(round(TARGET_VOL_SCALE_REBALANCE_THRESHOLD * 100)):03d}"


def signal_model_slug() -> str:
    return (
        f"spread_nav_power_wma_{POWER_TAG}_lb{LOOKBACK}"
        f"_{SIGNAL_TAG}_{execution_tag()}"
    )


def signal_model_human() -> str:
    return (
        f"spread-NAV Power-WMA daily return, power {POWER:g}, lookback {LOOKBACK}, "
        f"signal spread {SIGNAL_SPREAD_HEDGE_RATIO:.1f}x, "
        f"execution hedge {EXECUTION_HEDGE_RATIO:.2f}x, no R2 gate"
    )


EXECUTION_TAG = execution_tag()

SUMMARY_JSON = OUTPUT_DIR / f"{OUTPUT_PREFIX}_summary.json"
LATEST_SIGNAL_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_latest_signal.csv"
REALTIME_SIGNAL_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_realtime_signal.csv"
NAV_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_nav.csv"
COSTED_NAV_CSV = OUTPUT_DIR / (
    f"microcap_top100_mom16_power_{POWER_TAG}_lb{LOOKBACK}_{SIGNAL_TAG}_{EXECUTION_TAG}_"
    f"{BUFFER_TAG}_{DECAY_TAG}_{RECOVERY_TAG}_{TARGET_VOL_TAG}_{SCALE_TAG}_v2_4_costed_nav.csv"
)
LEGACY_COSTED_NAV_CSVS = [
    OUTPUT_DIR / "microcap_top100_mom16_power_p0p75_lb20_signal1p0_exec0p8_gap18_decay35_recovery40_targetvol25_scale010_v2_4_costed_nav.csv",
    OUTPUT_DIR / "microcap_top100_mom16_power_p0p75_lb20_signal1p0_exec0p8_gap18_decay35_recovery40_targetvol20_scale010_v2_4_costed_nav.csv"
]
PERF_SUMMARY_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_summary.csv"
PERF_YEARLY_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_yearly.csv"
PERF_NAV_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_nav.csv"
PERF_JSON = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_summary.json"
PERF_PNG = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_curve.png"
PERF_QUERY_SUMMARY_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_query_summary.csv"
PERF_QUERY_YEARLY_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_query_yearly.csv"
PERF_QUERY_NAV_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_query_nav.csv"
PERF_QUERY_JSON = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_query_summary.json"
PERF_QUERY_PNG = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_query_curve.png"

SIGNAL_MODEL = signal_model_slug()


def _json_sanitize(value: object) -> object:
    if isinstance(value, dict):
        return {key: _json_sanitize(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_sanitize(item) for item in value]
    if isinstance(value, tuple):
        return [_json_sanitize(item) for item in value]
    if isinstance(value, np.ndarray):
        return [_json_sanitize(item) for item in value.tolist()]
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value) if np.isfinite(value) else None
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if value is pd.NaT:
        return None
    return value


def _json_dumps(payload: object) -> str:
    return json.dumps(_json_sanitize(payload), ensure_ascii=False, indent=2, allow_nan=False, default=str)


def _canonical_json_obj(payload: object) -> object:
    return json.loads(_json_dumps(payload))


def _first_notna(row: pd.Series, *cols: str, default: object = np.nan) -> object:
    for col in cols:
        if col in row and pd.notna(row[col]):
            return row[col]
    return default


def _atomic_temp_path(path: Path) -> Path:
    return path.with_suffix(path.suffix + f".tmp.{time.time_ns()}")


def _atomic_write_text(path: Path, text: str, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = _atomic_temp_path(path)
    try:
        tmp.write_text(text, encoding=encoding)
        tmp.replace(path)
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:
            pass


def _atomic_write_csv(frame: pd.DataFrame, path: Path, **kwargs: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = _atomic_temp_path(path)
    try:
        frame.to_csv(tmp, **kwargs)
        tmp.replace(path)
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:
            pass


def ensure_output_dir() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def _validate_close_df(close_df: pd.DataFrame) -> pd.DataFrame:
    required = {"microcap", "hedge"}
    missing = required.difference(close_df.columns)
    if missing:
        raise KeyError(f"close_df missing columns: {sorted(missing)}")
    out = close_df.copy()
    if not isinstance(out.index, pd.DatetimeIndex):
        out.index = pd.to_datetime(out.index, errors="raise")
    out = out.sort_index()
    if not out.index.is_unique:
        dupes = out.index[out.index.duplicated()].unique()[:5]
        examples = [str(pd.Timestamp(dt).date()) for dt in dupes]
        raise ValueError(f"close_df contains duplicated dates: {examples}")
    out[["microcap", "hedge"]] = out[["microcap", "hedge"]].apply(pd.to_numeric, errors="coerce")
    finite = np.isfinite(out[["microcap", "hedge"]].to_numpy(dtype=float))
    bad = out[["microcap", "hedge"]].isna().any(axis=1) | ~pd.Series(finite.all(axis=1), index=out.index)
    if bad.any():
        examples = [str(pd.Timestamp(dt).date()) for dt in out.index[bad][:5]]
        raise ValueError(f"close_df contains missing close prices / missing prices: {examples}")
    non_positive = out[["microcap", "hedge"]].le(0).any(axis=1)
    if non_positive.any():
        examples = [str(pd.Timestamp(dt).date()) for dt in out.index[non_positive][:5]]
        raise ValueError(f"close_df contains non-positive close prices: {examples}")
    return out


def power_weights(lookback: int = LOOKBACK, power: float = POWER) -> tuple[float, ...]:
    raw = np.arange(1, int(lookback) + 1, dtype=float) ** float(power)
    return tuple((raw / raw.sum()).tolist())


def always_on_spread_nav(close_df: pd.DataFrame) -> tuple[pd.Series, pd.Series, pd.Series, float]:
    close_df = _validate_close_df(close_df)
    micro_ret = close_df["microcap"].pct_change(fill_method=None)
    hedge_ret = close_df["hedge"].pct_change(fill_method=None)
    daily_drag = float(v2_0.base_mod.FUTURES_DRAG) * SIGNAL_SPREAD_HEDGE_RATIO
    spread_ret = micro_ret - SIGNAL_SPREAD_HEDGE_RATIO * hedge_ret - daily_drag
    if not spread_ret.empty:
        spread_ret.iloc[0] = 0.0
    if spread_ret.iloc[1:].isna().any():
        bad_dates = [str(pd.Timestamp(dt).date()) for dt in spread_ret.index[spread_ret.isna()][:5]]
        raise ValueError(f"spread return contains missing close prices / missing prices: {bad_dates}")
    spread_nav = (1.0 + spread_ret).cumprod()
    spread_nav.name = "spread_nav"
    return spread_nav, micro_ret, hedge_ret, daily_drag


def power_wma_score(spread_ret: pd.Series, lookback: int = LOOKBACK, power: float = POWER) -> pd.Series:
    weights = np.asarray(power_weights(lookback, power), dtype=float)

    def apply(values: np.ndarray) -> float:
        if not np.isfinite(values).all():
            return np.nan
        return float(np.dot(weights, values))

    return spread_ret.rolling(int(lookback), min_periods=int(lookback)).apply(apply, raw=True) * TRADING_DAYS


def _valid_power_wma_index(close_df: pd.DataFrame) -> pd.DatetimeIndex:
    spread_nav, _micro_ret, _hedge_ret, _daily_drag = always_on_spread_nav(close_df)
    spread_ret = spread_nav.pct_change(fill_method=None)
    if not spread_ret.empty:
        spread_ret.iloc[0] = 0.0
    score = power_wma_score(spread_ret)
    valid = score.notna()
    return pd.DatetimeIndex(score.index[valid])


def build_v2_4_common_index(
    close_df: pd.DataFrame,
    official_index: pd.DatetimeIndex | pd.Index | None = None,
    include_snapshot: bool = False,
) -> pd.DatetimeIndex:
    clean_close_df = _validate_close_df(close_df)
    valid_idx = pd.DatetimeIndex(_valid_power_wma_index(clean_close_df))
    idx = valid_idx
    if official_index is not None:
        idx = pd.DatetimeIndex(idx.intersection(pd.DatetimeIndex(official_index)))
    idx = idx[idx >= FORMAL_START_DATE]
    if include_snapshot and not clean_close_df.empty:
        snapshot_date = pd.Timestamp(clean_close_df.index[-1])
        if snapshot_date >= FORMAL_START_DATE:
            if snapshot_date not in valid_idx:
                raise ValueError(f"snapshot date has no valid v2.4 Power-WMA score: {snapshot_date.date()}")
            if snapshot_date not in idx:
                idx = idx.append(pd.DatetimeIndex([snapshot_date]))
    return pd.DatetimeIndex(idx).unique().sort_values()


def build_spread_power_wma_gross(close_df: pd.DataFrame, index: pd.DatetimeIndex | None = None) -> pd.DataFrame:
    close_df = _validate_close_df(close_df)
    spread_nav, micro_ret, hedge_ret, _signal_daily_drag = always_on_spread_nav(close_df)
    spread_ret = spread_nav.pct_change(fill_method=None)
    if not spread_ret.empty:
        spread_ret.iloc[0] = 0.0
    score_full = power_wma_score(spread_ret)
    common_index = build_v2_4_common_index(close_df) if index is None else pd.DatetimeIndex(index).sort_values()
    if common_index.empty:
        raise ValueError("v2.4 common_index is empty")
    score = pd.to_numeric(score_full.loc[common_index], errors="coerce")
    if score.isna().any():
        examples = [str(pd.Timestamp(dt).date()) for dt in score.index[score.isna()][:5]]
        raise ValueError(f"v2.4 score is missing for common_index dates: {examples}")
    signal_on = score.gt(0.0)
    current_active = signal_on.shift(1, fill_value=False)
    microcap_ret = micro_ret.loc[common_index]
    hedge_ret_part = hedge_ret.loc[common_index]
    execution_daily_drag = float(v2_0.base_mod.FUTURES_DRAG) * EXECUTION_HEDGE_RATIO
    active_spread_ret = microcap_ret - EXECUTION_HEDGE_RATIO * hedge_ret_part
    bad_return = current_active & active_spread_ret.isna()
    if bad_return.any():
        examples = [str(pd.Timestamp(dt).date()) for dt in common_index[bad_return][:5]]
        raise ValueError(f"close_df contains missing close prices / missing prices for active returns: {examples}")
    futures_drag = pd.Series(
        np.where(current_active, execution_daily_drag, 0.0),
        index=common_index,
        dtype=float,
    )
    gross_ret = pd.Series(np.where(current_active, active_spread_ret - futures_drag, 0.0), index=common_index, dtype=float)
    return pd.DataFrame(
        {
            "return_raw": gross_ret,
            "return": gross_ret,
            "holding": np.where(current_active, "long_microcap_short_zz1000", "cash"),
            "next_holding": np.where(signal_on, "long_microcap_short_zz1000", "cash"),
            "signal_on": signal_on.astype(bool),
            "microcap_close": close_df["microcap"].loc[common_index],
            "hedge_close": close_df["hedge"].loc[common_index],
            "microcap_ret": microcap_ret,
            "hedge_ret": hedge_ret_part,
            "microcap_mom": score,
            "hedge_mom": 0.0,
            "momentum_gap": score,
            "annualized_power_wma_score": score,
            "spread_nav": spread_nav.loc[common_index],
            "power": POWER,
            "power_weight_oldest_to_newest": ",".join(f"{w:.8f}" for w in power_weights()),
            "signal_score_label": "annualized_power_wma_score",
            "momentum_gap_legacy_note": "legacy field contains annualized spread-NAV Power-WMA score, not plain microcap-minus-hedge momentum gap",
            "futures_drag": futures_drag,
            "active_spread_ret": pd.Series(np.where(current_active, active_spread_ret, 0.0), index=common_index, dtype=float),
            "weight": 1.0,
            "target_vol_execution_scale": 1.0,
        },
        index=common_index,
    )


def apply_close_executed_peak_decay_derisk(
    gross_result: pd.DataFrame,
    turnover_df: pd.DataFrame,
    decay_ratio_threshold: float,
    derisk_scale: float,
    recovery_ratio_threshold: float | None = None,
) -> pd.DataFrame:
    if decay_ratio_threshold < 0:
        raise ValueError("decay_ratio_threshold must be non-negative.")
    if not (0.0 <= derisk_scale <= 1.0):
        raise ValueError("derisk_scale must be between 0 and 1.")
    if recovery_ratio_threshold is not None:
        recovery_ratio_threshold = float(recovery_ratio_threshold)
        if recovery_ratio_threshold < 0:
            raise ValueError("recovery_ratio_threshold must be non-negative.")

    out = gross_result.copy().sort_index()
    required = {"holding", "next_holding", "return", "momentum_gap"}
    missing = required.difference(out.columns)
    if missing:
        raise KeyError(f"Missing columns for close-executed peak-decay derisk: {sorted(missing)}")

    out["base_holding"] = out["holding"].astype(str)
    out["base_next_holding"] = out["next_holding"].astype(str)
    out["base_signal_on"] = out["base_next_holding"].ne("cash")

    if out.empty:
        out["gap_peak"] = pd.Series(dtype=float)
        out["gap_decay_ratio"] = pd.Series(dtype=float)
        out["min_peak_to_arm_decay"] = pd.Series(dtype=float)
        out["signal_quality_derisk_triggered"] = pd.Series(dtype=bool)
        out["signal_quality_execution_scale"] = pd.Series(dtype=float)
        out["signal_quality_next_scale"] = pd.Series(dtype=float)
        out["execution_scale"] = pd.Series(dtype=float)
        out["signal_quality_scale_turnover"] = pd.Series(dtype=float)
        out["signal_quality_scale_cost"] = pd.Series(dtype=float)
        out["entry_exit_cost"] = pd.Series(dtype=float)
        out["rebalance_cost"] = pd.Series(dtype=float)
        out["total_cost"] = pd.Series(dtype=float)
        out["return_net"] = pd.Series(dtype=float)
        out["nav_net"] = pd.Series(dtype=float)
        out["overlay_pre_cost_return"] = pd.Series(dtype=float)
        return out

    rebalance_base = v2_0.base_mod.freq_mod.cost_mod.map_rebalance_apply_costs(out.index, turnover_df)
    returns = pd.to_numeric(out["return"], errors="coerce")
    if returns.isna().any():
        examples = [str(pd.Timestamp(dt).date()) for dt in returns.index[returns.isna()][:5]]
        raise ValueError(f"gross_result contains missing returns: {examples}")
    momentum_gap_series = pd.to_numeric(out["momentum_gap"], errors="coerce")
    bad_momentum_gap = momentum_gap_series.isna() | ~np.isfinite(momentum_gap_series.to_numpy(dtype=float))
    if bad_momentum_gap.any():
        examples = [str(pd.Timestamp(dt).date()) for dt in momentum_gap_series.index[bad_momentum_gap][:5]]
        raise ValueError(f"momentum_gap contains NaN or infinite values after exit buffer: {examples}")
    entry_rate = float(v2_0.base_mod.freq_mod.cost_mod.ENTRY_COST)
    exit_rate = float(v2_0.base_mod.freq_mod.cost_mod.EXIT_COST)

    executed_holding: list[str] = []
    executed_next_holding: list[str] = []
    executed_signal_on: list[bool] = []
    derisk_flags: list[bool] = []
    current_quality_scales: list[float] = []
    next_quality_scales: list[float] = []
    quality_turnovers: list[float] = []
    quality_costs: list[float] = []
    gap_peaks: list[float | None] = []
    gap_decay_ratios: list[float | None] = []
    entry_exit_costs: list[float] = []
    rebalance_costs: list[float] = []
    total_costs: list[float] = []
    return_nets: list[float] = []
    nav_nets: list[float] = []
    overlay_pre_cost_returns: list[float] = []

    signal_quality_scale = 1.0 if str(out["base_holding"].iloc[0]) != "cash" else 0.0
    gap_peak: float | None = None
    derisked_in_trade = signal_quality_scale <= float(derisk_scale) and str(out["base_holding"].iloc[0]) != "cash"
    waiting_for_new_peak_after_recovery = False
    rearm_peak_level: float | None = None
    nav_net = 1.0

    for dt in out.index:
        base_current_active = str(out.at[dt, "base_holding"]) != "cash"
        base_next_active = str(out.at[dt, "base_next_holding"]) != "cash"
        current_gap = float(momentum_gap_series.loc[dt]) if pd.notna(momentum_gap_series.loc[dt]) else None

        if not base_current_active and base_next_active:
            gap_peak = current_gap
            derisked_in_trade = False
            waiting_for_new_peak_after_recovery = False
            rearm_peak_level = None

        if base_current_active and current_gap is not None:
            gap_peak = current_gap if gap_peak is None else max(float(gap_peak), current_gap)
        if (
            base_current_active
            and waiting_for_new_peak_after_recovery
            and rearm_peak_level is not None
            and gap_peak is not None
            and float(gap_peak) > float(rearm_peak_level)
        ):
            waiting_for_new_peak_after_recovery = False
            rearm_peak_level = None

        gap_decay_ratio = None
        if base_current_active and current_gap is not None and gap_peak is not None and gap_peak > 0:
            gap_decay_ratio = current_gap / gap_peak

        current_quality_scale = float(signal_quality_scale) if base_current_active else 0.0
        next_quality_scale = 1.0 if base_next_active else 0.0
        signal_quality_derisk_triggered = False
        if base_current_active and base_next_active:
            next_quality_scale = current_quality_scale
            if (
                derisked_in_trade
                and recovery_ratio_threshold is not None
                and gap_decay_ratio is not None
                and gap_decay_ratio >= recovery_ratio_threshold
            ):
                next_quality_scale = 1.0
                derisked_in_trade = False
                waiting_for_new_peak_after_recovery = True
                rearm_peak_level = gap_peak
            elif (
                not derisked_in_trade
                and not waiting_for_new_peak_after_recovery
                and gap_decay_ratio is not None
                and gap_peak is not None
                and float(gap_peak) >= float(MIN_PEAK_TO_ARM_DECAY)
                and gap_decay_ratio <= float(decay_ratio_threshold)
            ):
                next_quality_scale = float(derisk_scale)
                derisked_in_trade = True
                signal_quality_derisk_triggered = True

        current_active = base_current_active and current_quality_scale > 0.0
        next_active = base_next_active and next_quality_scale > 0.0
        gross_daily_return = float(returns.loc[dt])
        realized_daily_return = gross_daily_return * current_quality_scale if base_current_active else 0.0

        base_entry_exit_cost = 0.0
        if not base_current_active and base_next_active and next_active:
            base_entry_exit_cost = entry_rate
        elif base_current_active and not base_next_active and current_active:
            base_entry_exit_cost = exit_rate

        quality_delta = 0.0
        if base_current_active and base_next_active:
            quality_delta = float(next_quality_scale - current_quality_scale)
        if quality_delta < 0:
            signal_quality_scale_cost = abs(quality_delta) * exit_rate
        elif quality_delta > 0:
            signal_quality_scale_cost = abs(quality_delta) * entry_rate
        else:
            signal_quality_scale_cost = 0.0
        signal_quality_scale_turnover = abs(quality_delta)
        rebalance_exposure_scale = max(float(current_quality_scale), float(next_quality_scale)) if current_active and next_active else 0.0
        rebalance_cost = float(rebalance_base.loc[dt]) * rebalance_exposure_scale
        total_cost = float(base_entry_exit_cost + signal_quality_scale_cost + rebalance_cost)
        return_net = (1.0 + realized_daily_return) * (1.0 - total_cost) - 1.0
        nav_net *= 1.0 + return_net

        executed_holding.append("long_microcap_short_zz1000" if current_active else "cash")
        executed_next_holding.append("long_microcap_short_zz1000" if next_active else "cash")
        executed_signal_on.append(bool(next_active))
        derisk_flags.append(bool(signal_quality_derisk_triggered))
        current_quality_scales.append(float(current_quality_scale))
        next_quality_scales.append(float(next_quality_scale))
        quality_turnovers.append(float(signal_quality_scale_turnover))
        quality_costs.append(float(signal_quality_scale_cost))
        gap_peaks.append(None if gap_peak is None else float(gap_peak))
        gap_decay_ratios.append(None if gap_decay_ratio is None else float(gap_decay_ratio))
        entry_exit_costs.append(float(base_entry_exit_cost))
        rebalance_costs.append(float(rebalance_cost))
        total_costs.append(float(total_cost))
        return_nets.append(float(return_net))
        nav_nets.append(float(nav_net))
        overlay_pre_cost_returns.append(float(realized_daily_return))

        signal_quality_scale = float(next_quality_scale)
        if not base_next_active:
            signal_quality_scale = 0.0
            gap_peak = None
            derisked_in_trade = False
            waiting_for_new_peak_after_recovery = False
            rearm_peak_level = None

    out["holding"] = executed_holding
    out["next_holding"] = executed_next_holding
    out["signal_on"] = executed_signal_on
    out["gap_peak"] = pd.Series(gap_peaks, index=out.index, dtype=float)
    out["gap_decay_ratio"] = pd.Series(gap_decay_ratios, index=out.index, dtype=float)
    out["min_peak_to_arm_decay"] = float(MIN_PEAK_TO_ARM_DECAY)
    out["signal_quality_derisk_triggered"] = pd.Series(derisk_flags, index=out.index, dtype=bool)
    out["signal_quality_execution_scale"] = pd.Series(current_quality_scales, index=out.index, dtype=float)
    out["signal_quality_next_scale"] = pd.Series(next_quality_scales, index=out.index, dtype=float)
    out["execution_scale"] = pd.Series(current_quality_scales, index=out.index, dtype=float)
    out["signal_quality_scale_turnover"] = pd.Series(quality_turnovers, index=out.index, dtype=float)
    out["signal_quality_scale_cost"] = pd.Series(quality_costs, index=out.index, dtype=float)
    out["entry_exit_cost"] = pd.Series(entry_exit_costs, index=out.index, dtype=float)
    out["rebalance_cost"] = pd.Series(rebalance_costs, index=out.index, dtype=float)
    out["total_cost"] = pd.Series(total_costs, index=out.index, dtype=float)
    out["return_net"] = pd.Series(return_nets, index=out.index, dtype=float)
    out["nav_net"] = pd.Series(nav_nets, index=out.index, dtype=float)
    out["overlay_pre_cost_return"] = pd.Series(overlay_pre_cost_returns, index=out.index, dtype=float)
    return out


def apply_target_vol(costed_base: pd.DataFrame, target_vol: float = TARGET_VOL, *, treat_last_row_as_snapshot: bool = False) -> pd.DataFrame:
    out = v2_0.overlay_mod.apply_target_vol_scaling(
        costed_base,
        treat_last_row_as_snapshot=treat_last_row_as_snapshot,
        target_vol=float(target_vol),
        scale_rebalance_threshold=float(TARGET_VOL_SCALE_REBALANCE_THRESHOLD),
    )
    out["version"] = VERSION
    out["base_version"] = "embedded_v2_base"
    out["overlay_type"] = "spread_nav_power_wma_gap_peak_decay_target_vol"
    out["scale_rebalance_threshold"] = TARGET_VOL_SCALE_REBALANCE_THRESHOLD
    required_after_target_vol = {
        "return_net",
        "nav_net",
        "holding",
        "next_holding",
        "base_pre_cost_return",
    }
    missing = required_after_target_vol.difference(out.columns)
    if missing:
        raise KeyError(f"v2.4 target-vol output missing columns: {sorted(missing)}")
    after_financing = pd.to_numeric(out["return_net"], errors="coerce")
    if after_financing.isna().any():
        examples = [str(pd.Timestamp(dt).date()) for dt in after_financing.index[after_financing.isna()][:5]]
        raise ValueError(f"target-vol output contains missing return_net: {examples}")
    if "financing_cost" in out.columns:
        financing_cost = pd.to_numeric(out["financing_cost"], errors="coerce")
        if financing_cost.isna().any():
            examples = [str(pd.Timestamp(dt).date()) for dt in financing_cost.index[financing_cost.isna()][:5]]
            raise ValueError(f"target-vol output contains missing financing_cost: {examples}")
    else:
        financing_cost = pd.Series(0.0, index=out.index)
    out["target_vol_return_after_financing"] = after_financing
    out["target_vol_return_before_financing"] = (1.0 + after_financing).div(
        (1.0 - financing_cost).clip(lower=1e-12)
    ).sub(1.0)
    return out


def build_v2_4_result(
    close_df: pd.DataFrame,
    turnover_df: pd.DataFrame,
    common_index: pd.DatetimeIndex | None = None,
) -> pd.DataFrame:
    if common_index is None:
        common_index = build_v2_4_common_index(close_df)
    common_index = pd.DatetimeIndex(common_index)
    common_index = common_index[common_index >= FORMAL_START_DATE].sort_values()
    if common_index.empty:
        raise ValueError("v2.4 common_index is empty after valid WMA / official index / FORMAL_START_DATE filters")
    gross = build_spread_power_wma_gross(close_df, common_index)
    buffered = v2_0.base_mod.apply_momentum_gap_exit_buffer(gross, MOMENTUM_GAP_EXIT_BUFFER)
    derisked = apply_close_executed_peak_decay_derisk(
        buffered,
        turnover_df,
        decay_ratio_threshold=DECAY_RATIO_THRESHOLD,
        derisk_scale=DERISK_SCALE,
        recovery_ratio_threshold=RECOVERY_RATIO_THRESHOLD,
    )
    return apply_target_vol(derisked, TARGET_VOL)


def current_base_fingerprint() -> dict[str, object]:
    base = dict(v2_0.embedded_context.current_base_fingerprint())
    return {
        "base_version": "embedded_v2_base",
        "strategy_version": VERSION,
        "version_role": EXPECTED_VERSION_ROLE,
        "formal_start_date": str(FORMAL_START_DATE.date()),
        "output_prefix": OUTPUT_PREFIX,
        "costed_nav_filename": COSTED_NAV_CSV.name,
        "base_fingerprint": base,
        "signal_model": signal_model_slug(),
        "signal_model_human": signal_model_human(),
        "lookback": LOOKBACK,
        "power": POWER,
        "power_weight_oldest_to_newest": list(power_weights()),
        "common_index_source": "intersection of valid spread-NAV Power-WMA signal dates and official v2.0 output index, filtered from 2010-05-05",
        "score_definition": "annualized power-weighted mean of always-on 1.0x hedged signal spread daily returns",
        "r2_gate": None,
        "signal_spread_hedge_ratio": SIGNAL_SPREAD_HEDGE_RATIO,
        "execution_hedge_ratio": EXECUTION_HEDGE_RATIO,
        "base_hedge_ratio": BASE_HEDGE_RATIO,
        "momentum_gap_exit_buffer": MOMENTUM_GAP_EXIT_BUFFER,
        "decay_ratio_threshold": DECAY_RATIO_THRESHOLD,
        "derisk_scale": DERISK_SCALE,
        "recovery_ratio_threshold": RECOVERY_RATIO_THRESHOLD,
        "min_peak_to_arm_decay": MIN_PEAK_TO_ARM_DECAY,
        "signal_quality_execution_timing": "close_decision_next_session_return_v2_4_20260517",
        "target_vol": TARGET_VOL,
        "target_vol_window": int(v2_0.overlay_mod.TARGET_VOL_WINDOW),
        "target_vol_max_leverage": float(v2_0.overlay_mod.TARGET_VOL_MAX_LEVERAGE),
        "target_vol_scale_change_cost": float(v2_0.overlay_mod.TARGET_VOL_SCALE_CHANGE_COST),
        "target_vol_financing_rate": float(v2_0.overlay_mod.TARGET_VOL_FINANCING_RATE),
        "target_vol_scale_rebalance_threshold": TARGET_VOL_SCALE_REBALANCE_THRESHOLD,
    }


def current_base_fingerprint_canonical() -> dict[str, object]:
    canonical = _canonical_json_obj(current_base_fingerprint())
    if not isinstance(canonical, dict):
        raise TypeError("canonical v2.4 base fingerprint must be a dict")
    return canonical


def summary_matches_current_v2_4_base(summary: dict[str, object]) -> bool:
    if not isinstance(summary, dict):
        return False
    if str(summary.get("version")) != VERSION:
        return False
    if str(summary.get("version_role")) != EXPECTED_VERSION_ROLE:
        return False
    if not str(summary.get("version_note", "")).startswith(EXPECTED_VERSION_NOTE_PREFIX):
        return False
    return summary.get("base_fingerprint") == current_base_fingerprint_canonical()


def incompatible_v2_4_outputs() -> list[Path]:
    outputs = [
        SUMMARY_JSON,
        LATEST_SIGNAL_CSV,
        REALTIME_SIGNAL_CSV,
        NAV_CSV,
        COSTED_NAV_CSV,
        *LEGACY_COSTED_NAV_CSVS,
        PERF_SUMMARY_CSV,
        PERF_YEARLY_CSV,
        PERF_NAV_CSV,
        PERF_JSON,
        PERF_PNG,
        PERF_QUERY_SUMMARY_CSV,
        PERF_QUERY_YEARLY_CSV,
        PERF_QUERY_NAV_CSV,
        PERF_QUERY_JSON,
        PERF_QUERY_PNG,
    ]
    if not SUMMARY_JSON.exists():
        return [path for path in outputs if path.exists()]
    try:
        summary = json.loads(SUMMARY_JSON.read_text(encoding="utf-8"))
    except Exception:
        summary = None
    if summary_matches_current_v2_4_base(summary):
        return []
    unmanaged_candidates = [
        path for path in _candidate_v2_4_costed_nav_files()
        if path not in set(outputs)
    ]
    if unmanaged_candidates:
        examples = ", ".join(path.name for path in unmanaged_candidates[:5])
        warnings.warn(
            "stale v2.4 cleanup skipped unmanaged costed NAV files matching the version-family pattern; "
            f"archive or remove manually if obsolete: {examples}",
            RuntimeWarning,
        )
    return outputs


def _candidate_v2_4_costed_nav_files() -> list[Path]:
    pattern = (
        "microcap_top100_mom16_power_*_lb*_signal*_exec*_gap*_decay*_"
        "recovery*_targetvol*_scale*_v2_4_costed_nav.csv"
    )
    return list(OUTPUT_DIR.glob(pattern))


def summarize_returns(ret: pd.Series) -> dict[str, float | str | int]:
    ret = pd.to_numeric(ret, errors="coerce").astype(float)
    if ret.isna().any():
        examples = [str(pd.Timestamp(dt).date()) for dt in ret.index[ret.isna()][:5]]
        raise ValueError(f"return_net contains NaN: {examples}")
    if ret.empty:
        raise ValueError("empty return series")
    nav = (1.0 + ret).cumprod()
    years = (ret.index[-1] - ret.index[0]).days / 365.25
    annual = nav.iloc[-1] ** (1.0 / years) - 1.0 if years > 0 else 0.0
    vol = ret.std(ddof=1) * (TRADING_DAYS**0.5)
    sharpe = annual / vol if vol > 0 else 0.0
    drawdown = nav.div(nav.cummax()).sub(1.0)
    return {
        "start_date": str(pd.Timestamp(ret.index[0]).date()),
        "end_date": str(pd.Timestamp(ret.index[-1]).date()),
        "days": int(len(ret)),
        "final_nav": float(nav.iloc[-1]),
        "total_return_pct": float((nav.iloc[-1] - 1.0) * 100.0),
        "annual_pct": float(annual * 100.0),
        "max_drawdown_pct": float(drawdown.min() * 100.0),
        "sharpe": float(sharpe),
        "vol_pct": float(vol * 100.0),
    }


def summarize_yearly(ret: pd.Series) -> pd.DataFrame:
    ret = pd.to_numeric(ret, errors="coerce").astype(float)
    if ret.isna().any():
        examples = [str(pd.Timestamp(dt).date()) for dt in ret.index[ret.isna()][:5]]
        raise ValueError(f"return_net contains NaN: {examples}")
    rows: list[dict[str, object]] = []
    for year, part in ret.groupby(ret.index.year):
        if part.empty:
            continue
        nav = (1.0 + part).cumprod()
        years = (part.index[-1] - part.index[0]).days / 365.25
        annual = nav.iloc[-1] ** (1.0 / years) - 1.0 if years > 0 and len(part) >= 60 else np.nan
        vol = part.std(ddof=1) * (TRADING_DAYS**0.5)
        drawdown = nav.div(nav.cummax()).sub(1.0)
        rows.append(
            {
                "year": str(year),
                "start_date": str(pd.Timestamp(part.index[0]).date()),
                "end_date": str(pd.Timestamp(part.index[-1]).date()),
                "days": int(len(part)),
                "return_pct": float((nav.iloc[-1] - 1.0) * 100.0),
                "max_drawdown_pct": float(drawdown.min() * 100.0),
                "sharpe": float(annual / vol) if vol > 0 and pd.notna(annual) else 0.0,
                "annual_pct": float(annual * 100.0) if pd.notna(annual) else np.nan,
            }
        )
    return pd.DataFrame(rows)


def build_performance_payload(ret: pd.Series, source_label: str = "costed_v2_4") -> dict[str, object]:
    ensure_output_dir()
    ret = pd.to_numeric(ret, errors="coerce").astype(float)
    if ret.isna().any():
        examples = [str(pd.Timestamp(dt).date()) for dt in ret.index[ret.isna()][:5]]
        raise ValueError(f"return_net contains NaN: {examples}")
    summary = summarize_returns(ret)
    yearly_df = summarize_yearly(ret)
    nav_df = pd.DataFrame(
        {
            "date": ret.index,
            "return_net": ret.values,
            "nav_net": (1.0 + ret).cumprod().values,
        }
    )
    _atomic_write_csv(yearly_df, PERF_YEARLY_CSV, index=False, encoding="utf-8-sig")
    _atomic_write_csv(nav_df, PERF_NAV_CSV, index=False, encoding="utf-8-sig")
    _atomic_write_csv(pd.DataFrame([summary]), PERF_SUMMARY_CSV, index=False, encoding="utf-8-sig")
    plt.figure(figsize=(12, 6))
    plt.plot(nav_df["date"], nav_df["nav_net"], label="v2.4 nav_net")
    plt.title("Top100 Microcap Mom16 v2.4 Costed NAV")
    plt.xlabel("date")
    plt.ylabel("nav_net")
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(PERF_PNG, dpi=150)
    plt.close()
    payload = {
        "source_label": source_label,
        "summary": summary,
        "outputs": {
            "summary_csv": str(PERF_SUMMARY_CSV),
            "yearly_csv": str(PERF_YEARLY_CSV),
            "nav_csv": str(PERF_NAV_CSV),
            "chart": str(PERF_PNG),
        },
    }
    _atomic_write_text(PERF_JSON, _json_dumps(payload), encoding="utf-8")
    return payload


def _build_signal_row(net_df: pd.DataFrame, reference_summary: dict[str, object]) -> pd.DataFrame:
    row = v2_0.overlay_mod._build_signal_row(net_df, reference_summary)
    row["version"] = VERSION
    row["strategy_version"] = f"v{VERSION}"
    row["base_version"] = "embedded_v2_base"
    row["overlay_type"] = "spread_nav_power_wma_gap_peak_decay_target_vol"
    row["signal_model"] = signal_model_slug()
    row["signal_model_human"] = signal_model_human()
    row["signal_spread_hedge_ratio"] = SIGNAL_SPREAD_HEDGE_RATIO
    row["execution_hedge_ratio"] = EXECUTION_HEDGE_RATIO
    row["power"] = POWER
    row["lookback"] = LOOKBACK
    row["momentum_gap_exit_buffer"] = MOMENTUM_GAP_EXIT_BUFFER
    row["decay_ratio_threshold"] = DECAY_RATIO_THRESHOLD
    row["derisk_scale"] = DERISK_SCALE
    row["recovery_ratio_threshold"] = RECOVERY_RATIO_THRESHOLD
    row["min_peak_to_arm_decay"] = MIN_PEAK_TO_ARM_DECAY
    row["signal_score_label"] = "annualized_power_wma_score"
    row["momentum_gap_legacy_note"] = (
        "legacy field contains annualized spread-NAV Power-WMA score, not plain microcap-minus-hedge momentum gap"
    )
    latest = net_df.iloc[-1]
    for col in ["annualized_power_wma_score", "spread_nav"]:
        if col in latest and pd.notna(latest[col]):
            row[col] = float(latest[col])
    realized_vol = latest.get("target_vol_realized_vol", np.nan)
    if pd.notna(realized_vol):
        realized_vol_float = float(realized_vol)
        row["realized_vol_60d"] = realized_vol_float
        if realized_vol_float > 0:
            row["target_scale_raw"] = float(TARGET_VOL) / realized_vol_float
    scale_after_cap = latest.get("target_vol_scale_raw", np.nan)
    if pd.notna(scale_after_cap):
        row["target_scale_after_cap"] = float(scale_after_cap)
        if "target_scale_raw" not in row.columns:
            row["target_scale_raw"] = float(scale_after_cap)
    current_scale = _first_notna(latest, "current_execution_scale", "execution_scale")
    if pd.notna(current_scale):
        row["target_scale_previous"] = float(current_scale)
    actionable_scale = _first_notna(latest, "next_session_actionable_scale", "target_vol_scale_next_session")
    if pd.notna(current_scale) and pd.notna(actionable_scale):
        change_abs = abs(float(actionable_scale) - float(current_scale))
        row["target_scale_change_abs"] = float(change_abs)
        signal = row.iloc[0] if not row.empty else pd.Series(dtype=object)
        scale_trade_state = _first_notna(signal, "scale_trade_state", default=None)
        if scale_trade_state is not None:
            row["scale_rebalance_triggered"] = str(scale_trade_state) != "hold_scale"
        else:
            current_holding = str(_first_notna(signal, "current_holding", default=latest.get("holding", "cash")))
            next_holding = str(_first_notna(signal, "next_holding", default=latest.get("next_holding", current_holding)))
            same_active_holding = current_holding == next_holding and current_holding != "cash"
            row["scale_rebalance_triggered"] = bool(
                same_active_holding
                and change_abs >= v2_0.overlay_mod.SCALE_TRADE_REQUIRED_EPSILON
            )
    for src_col, dst_col in [
        ("financing_cost", "financing_cost_today"),
        ("target_vol_return_before_financing", "target_vol_return_before_financing"),
        ("target_vol_return_after_financing", "target_vol_return_after_financing"),
    ]:
        if src_col in latest and pd.notna(latest[src_col]):
            row[dst_col] = float(latest[src_col])
    row["target_vol"] = TARGET_VOL
    row["target_vol_scale_rebalance_threshold"] = TARGET_VOL_SCALE_REBALANCE_THRESHOLD
    return row


def _close_df_from_base(base_gross_cached: pd.DataFrame) -> pd.DataFrame:
    close_df = base_gross_cached[["microcap_close", "hedge_close"]].rename(
        columns={"microcap_close": "microcap", "hedge_close": "hedge"}
    )
    return _validate_close_df(close_df)


def _validated_realtime_base_gross_index(base_gross: pd.DataFrame) -> pd.DatetimeIndex:
    base_index = pd.DatetimeIndex(pd.to_datetime(base_gross.index, errors="raise"))
    if not base_index.is_unique:
        dupes = base_index[base_index.duplicated()].unique()[:5]
        examples = [str(pd.Timestamp(dt).date()) for dt in dupes]
        raise ValueError(f"realtime base_gross contains duplicated dates: {examples}")
    return base_index.sort_values()


def generate_v2_4_outputs() -> tuple[dict[str, object], pd.DataFrame, pd.DataFrame]:
    ensure_output_dir()
    _, _, official_v2_0_out = v2_0.generate_v2_0_outputs()
    reference_summary, base_gross_cached, turnover_df = v2_0.embedded_context._load_embedded_base_context()
    stale_outputs = incompatible_v2_4_outputs()
    close_df = _close_df_from_base(base_gross_cached)
    common_index = build_v2_4_common_index(close_df, official_v2_0_out.index)
    out = build_v2_4_result(close_df, turnover_df, common_index)
    if COSTED_NAV_CSV.exists() and COSTED_NAV_CSV not in stale_outputs:
        previous = pd.read_csv(COSTED_NAV_CSV, parse_dates=["date"])
        v2_0.base_mod.assert_no_historical_rewrite(
            previous=previous,
            candidate=out.rename_axis("date").reset_index(),
            key_columns=["return_net", "holding", "next_holding", "base_pre_cost_return"],
            allowed_tail_rows=max(LOOKBACK + 20, 40),
            label="v2.4 official costed NAV",
            audit_path=OUTPUT_DIR / f"{OUTPUT_PREFIX}_historical_rewrite_audit.csv",
        )

    _atomic_write_csv(out, COSTED_NAV_CSV, index_label="date", encoding="utf-8-sig")
    _atomic_write_csv(out.rename_axis("date").reset_index(), NAV_CSV, index=False, encoding="utf-8-sig")
    signal_row = _build_signal_row(out, reference_summary)
    _atomic_write_text(LATEST_SIGNAL_CSV, signal_row.to_csv(index=False), encoding="utf-8")
    ret = pd.to_numeric(out["return_net"], errors="coerce")
    if ret.isna().any():
        examples = [str(pd.Timestamp(dt).date()) for dt in ret.index[ret.isna()][:5]]
        raise ValueError(f"v2.4 return_net contains NaN before performance output: {examples}")
    perf_payload = build_performance_payload(ret, source_label="costed_v2_4")

    data_lineage = v2_0.overlay_mod._build_v2_data_lineage()
    summary = copy.deepcopy(reference_summary)
    summary["strategy"] = OUTPUT_PREFIX
    summary["version"] = VERSION
    summary["version_role"] = EXPECTED_VERSION_ROLE
    summary["version_note"] = (
        f"Formal v2.4 spread-NAV Power-WMA target-volatility overlay. Uses power={POWER:g} recency-weighted mean of "
        f"{LOOKBACK} trading days of always-on {SIGNAL_SPREAD_HEDGE_RATIO:.1f}x hedged signal spread daily returns, "
        f"annualized by trading days, executes with {EXECUTION_HEDGE_RATIO:.2f}x CSI1000 hedge, no R2 gate, "
        f"{MOMENTUM_GAP_EXIT_BUFFER:.0%} score exit buffer, close-executed peak-decay derisk with decay/recovery "
        f"thresholds {DECAY_RATIO_THRESHOLD:.2f}/{RECOVERY_RATIO_THRESHOLD:.2f}, min peak {MIN_PEAK_TO_ARM_DECAY:.2f}, "
        f"and derisk scale {DERISK_SCALE:.1f}, {int(v2_0.overlay_mod.TARGET_VOL_WINDOW)}-day realized volatility, "
        f"{TARGET_VOL:.0%} annual target volatility, max {float(v2_0.overlay_mod.TARGET_VOL_MAX_LEVERAGE):.1f}x leverage, "
        f"{TARGET_VOL_SCALE_REBALANCE_THRESHOLD:.0%} scale rebalance threshold, "
        f"{float(v2_0.overlay_mod.TARGET_VOL_SCALE_CHANGE_COST) * 10000:.0f}bp leg-turnover scale-change cost, "
        "scaled embedded-lineage base trading cost, and "
        f"{float(v2_0.overlay_mod.TARGET_VOL_FINANCING_RATE):.0%} annual financing cost on exposure above 1.0x."
    )
    summary.setdefault("core_params", {})
    summary["core_params"]["fixed_hedge_ratio"] = BASE_HEDGE_RATIO
    summary["core_params"]["signal_spread_hedge_ratio"] = SIGNAL_SPREAD_HEDGE_RATIO
    summary["core_params"]["execution_hedge_ratio"] = EXECUTION_HEDGE_RATIO
    summary["core_params"]["signal_model"] = {
        "type": "spread_nav_power_wma_daily_return",
        "lookback": LOOKBACK,
        "power": POWER,
        "weights_oldest_to_newest": list(power_weights()),
        "score_definition": "annualized power-weighted mean of always-on 1.0x hedged signal spread daily returns",
        "r2_gate": None,
        "legacy_momentum_gap_field": "same value as annualized_power_wma_score for v2.0 compatibility",
    }
    summary["core_params"]["momentum_gap_entry_threshold"] = 0.0
    summary["core_params"]["momentum_gap_exit_buffer"] = MOMENTUM_GAP_EXIT_BUFFER
    summary["core_params"]["signal_quality_derisk"] = {
        "type": "momentum_gap_peak_decay_derisk_new_peak_guard",
        "decay_ratio_threshold": DECAY_RATIO_THRESHOLD,
        "min_peak_to_arm_decay": MIN_PEAK_TO_ARM_DECAY,
        "derisk_scale": DERISK_SCALE,
        "recovery_ratio_threshold": RECOVERY_RATIO_THRESHOLD,
    }
    summary["core_params"]["target_volatility_scaling"] = {
        "target_vol": TARGET_VOL,
        "vol_window": int(v2_0.overlay_mod.TARGET_VOL_WINDOW),
        "max_leverage": float(v2_0.overlay_mod.TARGET_VOL_MAX_LEVERAGE),
        "min_leverage": float(v2_0.overlay_mod.TARGET_VOL_MIN_LEVERAGE),
        "scale_change_cost": float(v2_0.overlay_mod.TARGET_VOL_SCALE_CHANGE_COST),
        "scale_rebalance_threshold": float(TARGET_VOL_SCALE_REBALANCE_THRESHOLD),
        "financing_rate": float(v2_0.overlay_mod.TARGET_VOL_FINANCING_RATE),
        "trading_days": TRADING_DAYS,
        "timing": "current execution scale uses T-1 realized volatility; next-session target scale uses T close realized volatility",
    }
    summary["latest_trade_date"] = str(pd.Timestamp(signal_row.iloc[0]["date"]).date())
    summary["latest_nav_date"] = str(pd.Timestamp(out.index.max()).date())
    summary["latest_signal"] = signal_row.iloc[0].drop(labels=["date"], errors="ignore").to_dict()
    summary["data_lineage"] = data_lineage
    summary["performance_source_label"] = "costed_v2_4"
    summary["performance_snapshot"] = perf_payload["summary"]
    summary["base_fingerprint"] = current_base_fingerprint_canonical()
    _atomic_write_text(SUMMARY_JSON, _json_dumps(summary), encoding="utf-8")
    for path in stale_outputs:
        if path not in {SUMMARY_JSON, LATEST_SIGNAL_CSV, NAV_CSV, COSTED_NAV_CSV, PERF_SUMMARY_CSV, PERF_YEARLY_CSV, PERF_NAV_CSV, PERF_JSON, PERF_PNG}:
            path.unlink(missing_ok=True)
    return summary, signal_row, out


def build_realtime_v2_4_outputs() -> tuple[pd.DataFrame, dict[str, object], pd.DataFrame]:
    ensure_output_dir()
    realtime_base = v2_0.realtime_core.load_realtime_base()
    close_df = _validate_close_df(realtime_base.realtime_close_df[["microcap", "hedge"]])
    _, _, official_v2_0_out = v2_0.generate_v2_0_outputs()
    official_index = pd.DatetimeIndex(official_v2_0_out.index).sort_values()
    base_gross = getattr(realtime_base, "base_gross", None)
    if base_gross is not None:
        base_index = _validated_realtime_base_gross_index(base_gross)
        snapshot_date = pd.Timestamp(close_df.index[-1])
        allowed_extra = pd.DatetimeIndex([])
        if snapshot_date not in official_index:
            allowed_extra = pd.DatetimeIndex([snapshot_date])
        if not official_index.empty:
            base_index_to_check = base_index[base_index >= official_index.min()]
        else:
            base_index_to_check = base_index
        unexpected_extra = base_index_to_check.difference(official_index.union(allowed_extra))
        if len(unexpected_extra) > 0:
            examples = [str(pd.Timestamp(dt).date()) for dt in unexpected_extra[:5]]
            raise ValueError(f"realtime base_gross has unexpected extra dates: {examples}")
    common_index = build_v2_4_common_index(
        close_df,
        official_index,
        include_snapshot=True,
    )
    if common_index.empty:
        raise ValueError(
            "v2.4 realtime common_index is empty after valid WMA / official index / snapshot filters"
        )
    gross = build_spread_power_wma_gross(close_df, common_index)
    buffered = v2_0.base_mod.apply_momentum_gap_exit_buffer(gross, MOMENTUM_GAP_EXIT_BUFFER)
    derisked = apply_close_executed_peak_decay_derisk(
        buffered,
        realtime_base.turnover_df,
        decay_ratio_threshold=DECAY_RATIO_THRESHOLD,
        derisk_scale=DERISK_SCALE,
        recovery_ratio_threshold=RECOVERY_RATIO_THRESHOLD,
    )
    out = apply_target_vol(derisked, TARGET_VOL, treat_last_row_as_snapshot=True)
    signal_row = _build_signal_row(out, realtime_base.reference_summary)
    signal_row = v2_0.realtime_core.base_mod.augment_signal_with_member_rebalance(
        signal_row,
        realtime_base.context.get("changes_df"),
    )
    v2_0.overlay_mod._apply_realtime_meta_columns_to_signal_row(signal_row, realtime_base.meta)
    signal_row["quote_coverage"] = f"{realtime_base.meta.get('member_price_count', 0)}/{realtime_base.meta.get('member_count', 0)}"
    signal_row["target_vol_signal_timing"] = "intraday_hypothetical_if_now_close"
    signal_row["signal_timing"] = "intraday_hypothetical_if_now_close"
    signal_row["official_close_confirmed_signal"] = False
    _atomic_write_text(REALTIME_SIGNAL_CSV, signal_row.to_csv(index=False), encoding="utf-8")
    return signal_row, realtime_base.meta, out


def _print_scale_fields(row: pd.Series, include_frozen: bool = False) -> None:
    v2_0.overlay_mod._print_scale_fields(row, include_frozen=include_frozen)


def _print_signal_query() -> None:
    _, signal_df, _ = generate_v2_4_outputs()
    row = signal_df.iloc[0]
    print("signal")
    print("strategy_version: v2.4")
    print("base_version: embedded_v2_base")
    print(f"signal_model: {signal_model_human()}")
    print(
        f"overlay: score buffer {MOMENTUM_GAP_EXIT_BUFFER:.2f}, peak-decay "
        f"{DECAY_RATIO_THRESHOLD:.2f}/{RECOVERY_RATIO_THRESHOLD:.2f}, min peak {MIN_PEAK_TO_ARM_DECAY:.2f}, "
        f"target volatility {TARGET_VOL:.0%}"
    )
    print(f"current_holding: {row['current_holding']}")
    print(f"next_holding: {row['next_holding']}")
    print(f"trade_state: {row.get('effective_trade_state', row.get('trade_state', 'hold'))}")
    print(f"holding_trade_state: {row.get('holding_trade_state', row.get('momentum_trade_state', 'hold'))}")
    print(f"scale_trade_state: {row.get('scale_trade_state', 'hold_scale')}")
    print(f"signal_date: {pd.Timestamp(row['date']).strftime('%Y-%m-%d')}")
    print(f"annualized_power_wma_score: {float(row.get('annualized_power_wma_score', row.get('momentum_gap', 0.0))):+.4%}")
    print("momentum_gap_legacy_note: legacy field is the annualized Power-WMA score, not plain gap")
    _print_scale_fields(row, include_frozen=False)
    print(f"official_close_confirmed_signal: {row.get('official_close_confirmed_signal', True)}")
    print(SUMMARY_JSON)
    print(LATEST_SIGNAL_CSV)


def _print_realtime_signal_query() -> None:
    signal_df, meta, _ = build_realtime_v2_4_outputs()
    row = signal_df.iloc[0]
    print("realtime_signal")
    print("strategy_version: v2.4")
    print("base_version: embedded_v2_base")
    print(f"signal_model: {signal_model_human()}")
    print(
        f"overlay: score buffer {MOMENTUM_GAP_EXIT_BUFFER:.2f}, peak-decay "
        f"{DECAY_RATIO_THRESHOLD:.2f}/{RECOVERY_RATIO_THRESHOLD:.2f}, min peak {MIN_PEAK_TO_ARM_DECAY:.2f}, "
        f"target volatility {TARGET_VOL:.0%}"
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
    _print_scale_fields(row, include_frozen=True)
    print("official_close_confirmed_signal: False")
    print(f"annualized_power_wma_score: {float(row.get('annualized_power_wma_score', row.get('momentum_gap', 0.0))):+.4%}")
    print("momentum_gap_legacy_note: legacy field is the annualized Power-WMA score, not plain gap")
    print(f"quote_source: {meta.get('quote_source')}")
    print(f"hedge_quote_source: {meta.get('hedge_quote_source')}")
    print(f"quote_coverage: {meta.get('member_price_count')}/{meta.get('member_count')}")
    print(REALTIME_SIGNAL_CSV)


def _print_performance_query(query: str) -> None:
    generate_v2_4_outputs()
    perf_df = pd.read_csv(COSTED_NAV_CSV, parse_dates=["date"]).sort_values("date").set_index("date")
    old_title = v2_0.embedded_context.base_mod.STRATEGY_TITLE
    v2_0.embedded_context.base_mod.STRATEGY_TITLE = "Top100 Microcap Mom16 Biweekly v2.4"
    try:
        v2_0.embedded_context.base_mod.build_performance_outputs(
            perf_df=perf_df,
            ret_col="return_net",
            nav_col="nav_net",
            source_label="costed_v2_4",
            query_text=query,
            paths={
                "performance_summary": PERF_QUERY_SUMMARY_CSV,
                "performance_yearly": PERF_QUERY_YEARLY_CSV,
                "performance_nav": PERF_QUERY_NAV_CSV,
                "performance_chart": PERF_QUERY_PNG,
                "performance_json": PERF_QUERY_JSON,
            },
        )
    finally:
        v2_0.embedded_context.base_mod.STRATEGY_TITLE = old_title
    print(PERF_QUERY_PNG)
    print(PERF_QUERY_SUMMARY_CSV)
    print(PERF_QUERY_YEARLY_CSV)
    print(PERF_QUERY_NAV_CSV)
    print(PERF_QUERY_JSON)


def _handle_query(query: str) -> None:
    stripped = query.strip()
    if stripped in {"信号", "信號", "signal"}:
        _print_signal_query()
        return
    if stripped in {"实时信号", "實時信號", "realtime_signal", "live_signal"}:
        _print_realtime_signal_query()
        return
    if v2_0.embedded_context.base_mod.PERFORMANCE_PATTERN.search(query):
        _print_performance_query(query)
        return
    raise ValueError("v2.4 supports: 信号 / 实时信号 / 表现 <区间>")


def main() -> None:
    query = " ".join(sys.argv[1:]).strip()
    if query:
        _handle_query(query)
        return
    generate_v2_4_outputs()
    print(str(SUMMARY_JSON))
    print(str(LATEST_SIGNAL_CSV))
    print(str(COSTED_NAV_CSV))

if __name__ == "__main__":
    main()

