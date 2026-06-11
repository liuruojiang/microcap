from __future__ import annotations

import argparse
import json
import math
import re
import sys
import time
from pathlib import Path

import matplotlib
import numpy as np
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt

import microcap_top100_mom16_biweekly_live_v2_0 as v2_0


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"

OUTPUT_PREFIX = "microcap_top100_mom16_biweekly_live_v2_3"
DEFAULT_OUTPUT_PREFIX = OUTPUT_PREFIX
SUMMARY_JSON = OUTPUT_DIR / f"{OUTPUT_PREFIX}_summary.json"
LATEST_SIGNAL_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_latest_signal.csv"
REALTIME_SIGNAL_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_realtime_signal.csv"
NAV_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_nav.csv"
COSTED_NAV_CSV = OUTPUT_DIR / "microcap_top100_mom16_exp_h3_lb17_signal1p0_exec0p8_gap13_nodecay_targetvol25_scale030_v2_3_costed_nav.csv"
DEFAULT_COSTED_NAV_CSV = COSTED_NAV_CSV
LEGACY_COSTED_NAV_CSVS = [
    OUTPUT_DIR / "microcap_top100_mom16_exp_h4_lb17_signal1p0_exec0p8_gap13_nodecay_targetvol25_scale030_v2_3_costed_nav.csv",
    OUTPUT_DIR / "microcap_top100_mom16_exp_h4_lb17_signal1p0_exec0p8_gap13_decay35_recovery50_targetvol25_scale030_v2_3_costed_nav.csv"
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

VERSION = "2.3"
EXPECTED_VERSION_ROLE = "spread_nav_log_wls_gap_target_vol_overlay"
EXPECTED_VERSION_NOTE_PREFIX = "Formal v2.3 spread-NAV log-WLS target-volatility overlay."
LOOKBACK = 17
HALFLIFE = 3.0
MOMENTUM_GAP_EXIT_BUFFER = 0.13
TARGET_VOL = 0.25
TARGET_VOL_SCALE_REBALANCE_THRESHOLD = 0.30
FORMAL_START_DATE = pd.Timestamp("2010-05-05")
CASH_DAY_YIELD = float(v2_0.overlay_mod.IDLE_CASH_YIELD)
MISMATCH_DIAGNOSTIC_ROLLING_WINDOW = 60
MAX_REALTIME_TARGET_VOL_FROZEN_LAG_DAYS = 5
_OFFICIAL_V2_0_OUT_CACHE: tuple[str, pd.DataFrame] | None = None

SIGNAL_SPREAD_HEDGE_RATIO = 1.0
EXECUTION_HEDGE_RATIO = float(v2_0.BASE_HEDGE_RATIO)
BASE_HEDGE_RATIO = EXECUTION_HEDGE_RATIO
TRADING_DAYS = int(v2_0.overlay_mod.TARGET_VOL_TRADING_DAYS)


def parse_v2_3_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Top100 Mom16 Biweekly v2.3 spread-NAV log-WLS target-vol overlay"
    )
    parser.add_argument("query_tokens", nargs="*", help="信号 / 实时信号 / 表现 <区间>")
    parser.add_argument("--panel-path", type=Path, default=None)
    parser.add_argument("--index-csv", type=Path, default=None)
    parser.add_argument(
        "--v23-costed-nav-csv",
        "--costed-nav-csv",
        dest="v23_costed_nav_csv",
        type=Path,
        default=None,
        help="Override the v2.3 costed NAV CSV written/read by queries.",
    )
    parser.add_argument("--base-costed-nav-csv", type=Path, default=None)
    parser.add_argument("--capital", type=float, default=None)
    parser.add_argument("--max-workers", type=int, default=8)
    parser.add_argument("--realtime-cache-seconds", type=int, default=v2_0.DEFAULT_REALTIME_CACHE_SECONDS)
    parser.add_argument("--allow-stale-realtime", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--bootstrap-deps", action="store_true")
    parser.add_argument("--wheelhouse", type=Path, default=None)
    parser.add_argument(
        "--v23-output-prefix",
        "--output-prefix",
        dest="v23_output_prefix",
        default=None,
        help="Override the v2.3 output prefix for summary, signal, performance, and NAV files.",
    )
    parser.add_argument("--base-output-prefix", default=None)
    return parser.parse_args(argv)


def configure_output_paths(output_prefix: str | None = None, costed_nav_csv: Path | None = None) -> None:
    global OUTPUT_PREFIX
    global SUMMARY_JSON, LATEST_SIGNAL_CSV, REALTIME_SIGNAL_CSV, NAV_CSV, COSTED_NAV_CSV
    global PERF_SUMMARY_CSV, PERF_YEARLY_CSV, PERF_NAV_CSV, PERF_JSON, PERF_PNG
    global PERF_QUERY_SUMMARY_CSV, PERF_QUERY_YEARLY_CSV, PERF_QUERY_NAV_CSV, PERF_QUERY_JSON, PERF_QUERY_PNG

    OUTPUT_PREFIX = str(output_prefix or DEFAULT_OUTPUT_PREFIX)
    SUMMARY_JSON = OUTPUT_DIR / f"{OUTPUT_PREFIX}_summary.json"
    LATEST_SIGNAL_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_latest_signal.csv"
    REALTIME_SIGNAL_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_realtime_signal.csv"
    NAV_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_nav.csv"
    if costed_nav_csv is not None:
        COSTED_NAV_CSV = Path(costed_nav_csv)
    elif OUTPUT_PREFIX == DEFAULT_OUTPUT_PREFIX:
        COSTED_NAV_CSV = DEFAULT_COSTED_NAV_CSV
    else:
        COSTED_NAV_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_costed_nav.csv"
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


def configure_runtime(args: argparse.Namespace) -> None:
    configure_output_paths(
        output_prefix=getattr(args, "v23_output_prefix", None),
        costed_nav_csv=getattr(args, "v23_costed_nav_csv", None),
    )
    v2_0._V2_RUNTIME_ARGS = argparse.Namespace(
        query_tokens=[],
        panel_path=getattr(args, "panel_path", None),
        index_csv=getattr(args, "index_csv", None),
        costed_nav_csv=getattr(args, "base_costed_nav_csv", None),
        output_prefix=getattr(args, "base_output_prefix", None),
        capital=getattr(args, "capital", None),
        max_workers=getattr(args, "max_workers", 8),
        realtime_cache_seconds=getattr(args, "realtime_cache_seconds", v2_0.DEFAULT_REALTIME_CACHE_SECONDS),
        allow_stale_realtime=getattr(args, "allow_stale_realtime", False),
        bootstrap_deps=getattr(args, "bootstrap_deps", False),
        wheelhouse=getattr(args, "wheelhouse", None),
    )


def v2_3_output_lock(
    wait_timeout_seconds: float = v2_0.DEFAULT_V2_LOCK_WAIT_SECONDS,
    stale_lock_seconds: float = v2_0.DEFAULT_V2_STALE_LOCK_SECONDS,
):
    return v2_0._v2_file_lock(
        f"{OUTPUT_PREFIX}_generation.lock",
        wait_timeout_seconds=wait_timeout_seconds,
        stale_lock_seconds=stale_lock_seconds,
    )


def v2_3_realtime_output_lock(
    wait_timeout_seconds: float = v2_0.DEFAULT_V2_LOCK_WAIT_SECONDS,
    stale_lock_seconds: float = v2_0.DEFAULT_V2_STALE_LOCK_SECONDS,
):
    return v2_0._v2_file_lock(
        f"{OUTPUT_PREFIX}_realtime.lock",
        wait_timeout_seconds=wait_timeout_seconds,
        stale_lock_seconds=stale_lock_seconds,
    )


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


def _atomic_temp_path(path: Path) -> Path:
    return path.with_suffix(path.suffix + f".tmp.{time.time_ns()}")


def _replace_with_retry(tmp: Path, path: Path, attempts: int = 5, delay_seconds: float = 0.05) -> None:
    for attempt in range(int(attempts)):
        try:
            tmp.replace(path)
            return
        except OSError:
            if attempt >= int(attempts) - 1:
                raise
            time.sleep(float(delay_seconds) * (2**attempt))


def _atomic_write_text(path: Path, text: str, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = _atomic_temp_path(path)
    try:
        tmp.write_text(text, encoding=encoding)
        _replace_with_retry(tmp, path)
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
        _replace_with_retry(tmp, path)
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:
            pass


def ensure_output_dir() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def exp_weights(lookback: int = LOOKBACK, halflife: float = HALFLIFE) -> tuple[float, ...]:
    age_from_latest = np.arange(int(lookback) - 1, -1, -1, dtype=float)
    raw = 0.5 ** (age_from_latest / float(halflife))
    return tuple((raw / raw.sum()).tolist())


def always_on_spread_nav(close_df: pd.DataFrame) -> tuple[pd.Series, pd.Series, pd.Series, float]:
    close_df = close_df.sort_index()
    micro_ret = close_df["microcap"].pct_change(fill_method=None)
    hedge_ret = close_df["hedge"].pct_change(fill_method=None)
    daily_drag = float(v2_0.base_mod.FUTURES_DRAG) * SIGNAL_SPREAD_HEDGE_RATIO
    spread_ret = micro_ret.fillna(0.0) - SIGNAL_SPREAD_HEDGE_RATIO * hedge_ret.fillna(0.0) - daily_drag
    spread_nav = (1.0 + spread_ret.fillna(0.0)).cumprod()
    spread_nav.name = "spread_nav"
    return spread_nav, micro_ret, hedge_ret, daily_drag


def log_wls_score_and_r2(
    spread_nav: pd.Series,
    lookback: int = LOOKBACK,
    halflife: float = HALFLIFE,
) -> pd.DataFrame:
    lookback = int(lookback)
    if lookback <= 0:
        raise ValueError("lookback must be positive")
    weights = np.asarray(exp_weights(lookback, halflife), dtype=float)
    y = np.log(pd.to_numeric(spread_nav, errors="coerce").replace(0.0, np.nan))
    x = np.arange(lookback, dtype=float)
    w_sum = float(weights.sum())
    x_bar = float((weights * x).sum() / w_sum)
    x_centered = x - x_bar
    denom = float((weights * x_centered**2).sum())
    values = y.to_numpy(dtype=float)
    score = np.full(len(y), np.nan, dtype=float)
    r2 = np.full(len(y), np.nan, dtype=float)
    if len(values) < lookback or denom <= 0:
        return pd.DataFrame({"annualized_log_wls_score": score, "log_wls_r2": r2}, index=y.index)

    windows = np.lib.stride_tricks.sliding_window_view(values, lookback)
    valid = np.isfinite(windows).all(axis=1)
    if valid.any():
        valid_windows = windows[valid]
        y_bar = valid_windows @ weights / w_sum
        y_centered = valid_windows - y_bar[:, None]
        slope = y_centered @ (weights * x_centered) / denom
        fitted = y_bar[:, None] + slope[:, None] * x_centered[None, :]
        ss_tot = (weights * y_centered**2).sum(axis=1)
        ss_res = (weights * (valid_windows - fitted) ** 2).sum(axis=1)
        r2_values = np.ones_like(ss_tot, dtype=float)
        nonzero_tot = ss_tot > 0
        r2_values[nonzero_tot] = np.clip(1.0 - ss_res[nonzero_tot] / ss_tot[nonzero_tot], 0.0, 1.0)
        target_positions = np.flatnonzero(valid) + lookback - 1
        score[target_positions] = slope * TRADING_DAYS
        r2[target_positions] = r2_values
    return pd.DataFrame({"annualized_log_wls_score": score, "log_wls_r2": r2}, index=y.index)


def _valid_log_wls_index(close_df: pd.DataFrame) -> pd.DatetimeIndex:
    spread_nav, _micro_ret, _hedge_ret, _daily_drag = always_on_spread_nav(close_df)
    log_wls = log_wls_score_and_r2(spread_nav)
    valid = log_wls["annualized_log_wls_score"].notna() & log_wls["log_wls_r2"].notna()
    return pd.DatetimeIndex(log_wls.index[valid])


def build_v2_3_common_index(
    close_df: pd.DataFrame,
    official_index: pd.DatetimeIndex | pd.Index | None = None,
) -> pd.DatetimeIndex:
    idx = pd.DatetimeIndex(_valid_log_wls_index(close_df))
    if official_index is not None:
        idx = pd.DatetimeIndex(idx.intersection(pd.DatetimeIndex(official_index)))
    idx = pd.DatetimeIndex(idx)
    return idx[idx >= FORMAL_START_DATE].sort_values()


def build_spread_log_wls_gross(close_df: pd.DataFrame, index: pd.DatetimeIndex | None = None) -> pd.DataFrame:
    close_df = close_df.sort_index()
    spread_nav, micro_ret, hedge_ret, _signal_daily_drag = always_on_spread_nav(close_df)
    log_wls = log_wls_score_and_r2(spread_nav)
    common_index = _valid_log_wls_index(close_df) if index is None else pd.DatetimeIndex(index)
    score = pd.to_numeric(log_wls["annualized_log_wls_score"].loc[common_index], errors="coerce")
    r2 = pd.to_numeric(log_wls["log_wls_r2"].loc[common_index], errors="coerce")
    signal_on = score.gt(0.0)
    current_active = signal_on.shift(1, fill_value=False)
    microcap_ret = micro_ret.loc[common_index]
    hedge_ret_part = hedge_ret.loc[common_index]
    execution_daily_drag = float(v2_0.base_mod.FUTURES_DRAG) * EXECUTION_HEDGE_RATIO
    active_spread_ret = microcap_ret.fillna(0.0) - EXECUTION_HEDGE_RATIO * hedge_ret_part.fillna(0.0)
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
            "annualized_log_wls_score": score,
            "log_wls_r2": r2,
            "spread_nav": spread_nav.loc[common_index],
            "halflife": HALFLIFE,
            "exp_weight_oldest_to_newest": ",".join(f"{w:.8f}" for w in exp_weights()),
            "signal_score_label": "annualized_log_wls_score",
            "momentum_gap_legacy_note": "legacy field contains annualized spread-NAV log-WLS score, not plain microcap-minus-hedge momentum gap",
            "futures_drag": futures_drag,
            "active_spread_ret": pd.Series(np.where(current_active, active_spread_ret, 0.0), index=common_index, dtype=float),
            "weight": 1.0,
            "target_vol_execution_scale": 1.0,
        },
        index=common_index,
    )


def apply_cost(gross: pd.DataFrame, turnover_df: pd.DataFrame) -> pd.DataFrame:
    out = v2_0.base_mod.freq_mod.cost_mod.apply_cost_model(gross, turnover_df)
    out["overlay_pre_cost_return"] = pd.to_numeric(out["return"], errors="coerce").fillna(0.0)
    return out


def apply_cash_day_yield(out: pd.DataFrame) -> pd.DataFrame:
    adjusted = out.copy()
    holding = adjusted["holding"].astype(str) if "holding" in adjusted.columns else pd.Series("cash", index=adjusted.index)
    cash_day = holding.eq("cash")
    daily_yield = float(CASH_DAY_YIELD) / float(TRADING_DAYS)
    cash_day_yield = pd.Series(0.0, index=adjusted.index, dtype=float)
    cash_day_yield.loc[cash_day] = daily_yield
    ret = pd.to_numeric(adjusted["return_net"], errors="coerce").fillna(0.0)
    adjusted["return_net"] = (1.0 + ret) * (1.0 + cash_day_yield) - 1.0
    adjusted["nav_net"] = (1.0 + adjusted["return_net"].fillna(0.0)).cumprod()
    adjusted["cash_day_yield"] = cash_day_yield
    adjusted["cash_day_yield_annual"] = CASH_DAY_YIELD
    adjusted["cash_day_yield_enabled"] = True
    return adjusted


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        if pd.isna(value):
            return float(default)
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def build_signal_execution_mismatch_diagnostics(
    close_df: pd.DataFrame,
    out: pd.DataFrame,
    rolling_window: int = MISMATCH_DIAGNOSTIC_ROLLING_WINDOW,
) -> dict[str, object]:
    close_df = close_df.sort_index()
    micro_ret = close_df["microcap"].pct_change(fill_method=None)
    hedge_ret = close_df["hedge"].pct_change(fill_method=None)
    daily_drag = float(v2_0.base_mod.FUTURES_DRAG)
    signal_ret = micro_ret - SIGNAL_SPREAD_HEDGE_RATIO * hedge_ret - daily_drag * SIGNAL_SPREAD_HEDGE_RATIO
    execution_ret = micro_ret - EXECUTION_HEDGE_RATIO * hedge_ret - daily_drag * EXECUTION_HEDGE_RATIO
    frame = pd.DataFrame(
        {
            "signal_ret": signal_ret,
            "execution_ret": execution_ret,
            "exec_minus_signal_ret": execution_ret - signal_ret,
        }
    ).replace([np.inf, -np.inf], np.nan)
    frame = frame.loc[frame.index.intersection(pd.DatetimeIndex(out.index))].dropna()
    active = out["holding"].astype(str).ne("cash").reindex(frame.index).fillna(False) if "holding" in out.columns else pd.Series(False, index=frame.index)
    active_frame = frame.loc[active]
    rolling_window = max(2, int(rolling_window))
    rolling_corr = frame["signal_ret"].rolling(rolling_window, min_periods=min(rolling_window, len(frame))).corr(frame["execution_ret"])
    active_rolling_corr = active_frame["signal_ret"].rolling(
        rolling_window,
        min_periods=min(rolling_window, len(active_frame)),
    ).corr(active_frame["execution_ret"]) if len(active_frame) else pd.Series(dtype=float)
    return {
        "signal_spread_hedge_ratio": SIGNAL_SPREAD_HEDGE_RATIO,
        "execution_hedge_ratio": EXECUTION_HEDGE_RATIO,
        "net_unhedged_zz1000_ratio": SIGNAL_SPREAD_HEDGE_RATIO - EXECUTION_HEDGE_RATIO,
        "rolling_window": rolling_window,
        "rows": int(len(frame)),
        "active_rows": int(len(active_frame)),
        "rolling_corr_latest": _safe_float(rolling_corr.dropna().iloc[-1], np.nan) if rolling_corr.dropna().size else None,
        f"rolling_corr_{rolling_window}d_latest": _safe_float(rolling_corr.dropna().iloc[-1], np.nan) if rolling_corr.dropna().size else None,
        "active_rolling_corr_latest": _safe_float(active_rolling_corr.dropna().iloc[-1], np.nan) if active_rolling_corr.dropna().size else None,
        f"active_rolling_corr_{rolling_window}d_latest": _safe_float(active_rolling_corr.dropna().iloc[-1], np.nan) if active_rolling_corr.dropna().size else None,
        "cumulative_exec_minus_signal_component": float((1.0 + frame["exec_minus_signal_ret"]).prod() - 1.0) if len(frame) else 0.0,
        "active_cumulative_exec_minus_signal_component": float((1.0 + active_frame["exec_minus_signal_ret"]).prod() - 1.0) if len(active_frame) else 0.0,
        "mean_daily_exec_minus_signal": float(frame["exec_minus_signal_ret"].mean()) if len(frame) else 0.0,
        "active_mean_daily_exec_minus_signal": float(active_frame["exec_minus_signal_ret"].mean()) if len(active_frame) else 0.0,
    }


def apply_signal_execution_mismatch_columns(signal_row: pd.DataFrame, diagnostics: dict[str, object]) -> pd.DataFrame:
    for key, value in diagnostics.items():
        signal_row[f"signal_execution_mismatch_{key}"] = value
    return signal_row


def assert_realtime_target_vol_lag_fresh(out: pd.DataFrame) -> None:
    if out.empty or "target_vol_frozen_lag_days" not in out.columns:
        return
    latest = out.iloc[-1]
    lag_days = int(_safe_float(latest.get("target_vol_frozen_lag_days"), 0.0))
    if lag_days <= MAX_REALTIME_TARGET_VOL_FROZEN_LAG_DAYS:
        return
    source_date = latest.get("target_vol_frozen_source_date", "")
    raise RuntimeError(
        "target-vol frozen lag exceeds realtime limit: "
        f"lag_days={lag_days}, limit={MAX_REALTIME_TARGET_VOL_FROZEN_LAG_DAYS}, source_date={source_date}"
    )


def apply_target_vol(costed_base: pd.DataFrame, target_vol: float = TARGET_VOL, *, treat_last_row_as_snapshot: bool = False) -> pd.DataFrame:
    out = v2_0.overlay_mod.apply_target_vol_scaling(
        costed_base,
        treat_last_row_as_snapshot=treat_last_row_as_snapshot,
        target_vol=float(target_vol),
        scale_rebalance_threshold=float(TARGET_VOL_SCALE_REBALANCE_THRESHOLD),
    )
    out = apply_cash_day_yield(out)
    out["version"] = VERSION
    out["base_version"] = "embedded_v2_base"
    out["overlay_type"] = "spread_nav_log_wls_gap_target_vol"
    out["scale_rebalance_threshold"] = TARGET_VOL_SCALE_REBALANCE_THRESHOLD
    return out


def build_v2_3_result(
    close_df: pd.DataFrame,
    turnover_df: pd.DataFrame,
    common_index: pd.DatetimeIndex | None = None,
) -> pd.DataFrame:
    if common_index is None:
        common_index = build_v2_3_common_index(close_df)
    else:
        common_index = pd.DatetimeIndex(common_index)
        common_index = common_index[common_index >= FORMAL_START_DATE].sort_values()
    gross = build_spread_log_wls_gross(close_df, common_index)
    buffered = v2_0.base_mod.apply_momentum_gap_exit_buffer(gross, MOMENTUM_GAP_EXIT_BUFFER)
    costed = v2_0.base_mod.apply_momentum_gap_no_peak_decay_cost_model(buffered, turnover_df)
    out = apply_target_vol(costed, TARGET_VOL)
    if out.empty:
        raise ValueError(
            "v2.3 output is empty: check close_df, official_v2_0_out.index, "
            "FORMAL_START_DATE, and valid log-WLS window."
        )
    return out


def current_base_fingerprint() -> dict[str, object]:
    base = dict(v2_0.embedded_context.current_base_fingerprint())
    return {
        "base_version": "embedded_v2_base",
        "strategy_version": VERSION,
        "base_fingerprint": base,
        "signal_model": "spread_nav_log_wls_exp_halflife_3p0_lb17_signal1p0_exec0p8",
        "lookback": LOOKBACK,
        "halflife": HALFLIFE,
        "exp_weight_oldest_to_newest": list(exp_weights()),
        "common_index_source": "intersection of valid spread-NAV log-WLS signal dates and official v2.0 output index, filtered from 2010-05-05",
        "score_definition": "annualized weighted log slope of always-on 1.0x hedged signal spread NAV",
        "nav_csv_momentum_gap_column_alias_note": "momentum_gap stores annualized_log_wls_score for v2.0 compatibility, not raw microcap minus hedge gap",
        "r2_gate": None,
        "signal_spread_hedge_ratio": SIGNAL_SPREAD_HEDGE_RATIO,
        "execution_hedge_ratio": EXECUTION_HEDGE_RATIO,
        "base_hedge_ratio": BASE_HEDGE_RATIO,
        "momentum_gap_exit_buffer": MOMENTUM_GAP_EXIT_BUFFER,
        "signal_quality_derisk_enabled": False,
        "target_vol": TARGET_VOL,
        "target_vol_window": int(v2_0.overlay_mod.TARGET_VOL_WINDOW),
        "target_vol_max_leverage": float(v2_0.overlay_mod.TARGET_VOL_MAX_LEVERAGE),
        "target_vol_scale_change_cost": float(v2_0.overlay_mod.TARGET_VOL_SCALE_CHANGE_COST),
        "target_vol_financing_rate": float(v2_0.overlay_mod.TARGET_VOL_FINANCING_RATE),
        "target_vol_scale_rebalance_threshold": TARGET_VOL_SCALE_REBALANCE_THRESHOLD,
        "cash_day_yield": CASH_DAY_YIELD,
        "idle_credit_on_cash_day": True,
        "signal_execution_mismatch_rolling_window": MISMATCH_DIAGNOSTIC_ROLLING_WINDOW,
        "max_realtime_target_vol_frozen_lag_days": MAX_REALTIME_TARGET_VOL_FROZEN_LAG_DAYS,
    }


def summary_matches_current_v2_3_base(summary: dict[str, object]) -> bool:
    if not isinstance(summary, dict):
        return False
    if str(summary.get("version")) != VERSION:
        return False
    if str(summary.get("version_role")) != EXPECTED_VERSION_ROLE:
        return False
    if not str(summary.get("version_note", "")).startswith(EXPECTED_VERSION_NOTE_PREFIX):
        return False
    return summary.get("base_fingerprint") == current_base_fingerprint()


def incompatible_v2_3_outputs() -> list[Path]:
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
    if summary_matches_current_v2_3_base(summary):
        return []
    return outputs


def _stale_outputs_to_remove_after_generate(stale_outputs: list[Path], regenerated_outputs: set[Path]) -> list[Path]:
    protected = set(regenerated_outputs)
    # Close-confirmed generation does not own the realtime signal artifact; the
    # realtime route refreshes it atomically when queried.
    protected.add(REALTIME_SIGNAL_CSV)
    return [path for path in stale_outputs if path not in protected]


def summarize_returns(ret: pd.Series) -> dict[str, float | str | int]:
    ret = ret.dropna().astype(float)
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
    rows: list[dict[str, object]] = []
    for year, part in ret.groupby(ret.index.year):
        part = part.dropna()
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


def build_performance_payload(ret: pd.Series, source_label: str = "costed_v2_3") -> dict[str, object]:
    ensure_output_dir()
    summary = summarize_returns(ret)
    yearly_df = summarize_yearly(ret)
    nav_df = pd.DataFrame(
        {
            "date": ret.index,
            "return_net": ret.values,
            "nav_net": (1.0 + ret.fillna(0.0)).cumprod().values,
        }
    )
    _atomic_write_csv(yearly_df, PERF_YEARLY_CSV, index=False, encoding="utf-8-sig")
    _atomic_write_csv(nav_df, PERF_NAV_CSV, index=False, encoding="utf-8-sig")
    _atomic_write_csv(pd.DataFrame([summary]), PERF_SUMMARY_CSV, index=False, encoding="utf-8-sig")
    plt.figure(figsize=(12, 6))
    plt.plot(nav_df["date"], nav_df["nav_net"], label="v2.3 nav_net")
    plt.title("Top100 Microcap Mom16 v2.3 Costed NAV")
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
    row["overlay_type"] = "spread_nav_log_wls_gap_target_vol"
    row["signal_model"] = "spread_nav_log_wls_exp_halflife_3p0_lb17_signal1p0_exec0p8"
    row["signal_spread_hedge_ratio"] = SIGNAL_SPREAD_HEDGE_RATIO
    row["execution_hedge_ratio"] = EXECUTION_HEDGE_RATIO
    row["halflife"] = HALFLIFE
    row["lookback"] = LOOKBACK
    row["momentum_gap_exit_buffer"] = MOMENTUM_GAP_EXIT_BUFFER
    row["signal_quality_derisk_enabled"] = False
    row["signal_score_label"] = "annualized_log_wls_score"
    row["momentum_gap_legacy_note"] = (
        "legacy field contains annualized spread-NAV log-WLS score, not plain microcap-minus-hedge momentum gap"
    )
    latest = net_df.iloc[-1]
    for col in ["annualized_log_wls_score", "log_wls_r2", "spread_nav"]:
        if col in latest and pd.notna(latest[col]):
            row[col] = float(latest[col])
    row["target_vol"] = TARGET_VOL
    row["target_vol_scale_rebalance_threshold"] = TARGET_VOL_SCALE_REBALANCE_THRESHOLD
    row["cash_day_yield"] = float(latest.get("cash_day_yield", 0.0)) if "cash_day_yield" in latest else 0.0
    row["cash_day_yield_annual"] = CASH_DAY_YIELD
    row["cash_day_yield_enabled"] = True
    return row


def _close_df_from_base(base_gross_cached: pd.DataFrame) -> pd.DataFrame:
    return base_gross_cached[["microcap_close", "hedge_close"]].rename(
        columns={"microcap_close": "microcap", "hedge_close": "hedge"}
    ).sort_index()


def _official_v2_0_cache_key() -> str:
    return json.dumps(
        _json_sanitize(v2_0.current_base_fingerprint()),
        ensure_ascii=False,
        sort_keys=True,
        allow_nan=False,
        default=str,
    )


def _load_official_v2_0_out() -> pd.DataFrame:
    global _OFFICIAL_V2_0_OUT_CACHE
    cache_key = _official_v2_0_cache_key()
    if _OFFICIAL_V2_0_OUT_CACHE is not None and _OFFICIAL_V2_0_OUT_CACHE[0] == cache_key:
        return _OFFICIAL_V2_0_OUT_CACHE[1]
    _, _, official_v2_0_out = v2_0.generate_v2_0_outputs()
    # Recompute after generation because panel shadow/base files may refresh
    # inside generate_v2_0_outputs(); cache the state subsequent calls will see.
    _OFFICIAL_V2_0_OUT_CACHE = (_official_v2_0_cache_key(), official_v2_0_out)
    return official_v2_0_out


V2_3_REWRITE_AUDIT_KEY_COLUMNS = [
    "return_net",
    "holding",
    "next_holding",
    "base_pre_cost_return",
    "current_execution_scale",
    "next_session_actionable_scale",
    "target_vol_realized_vol",
    "base_trade_cost_scaled",
    "scale_change_cost",
    "financing_cost",
    "annualized_log_wls_score",
]


def _generate_v2_3_outputs_unlocked() -> tuple[dict[str, object], pd.DataFrame, pd.DataFrame]:
    ensure_output_dir()
    official_v2_0_out = _load_official_v2_0_out()
    reference_summary, base_gross_cached, turnover_df = v2_0.embedded_context._load_embedded_base_context()
    stale_outputs = incompatible_v2_3_outputs()
    close_df = _close_df_from_base(base_gross_cached)
    common_index = build_v2_3_common_index(close_df, official_v2_0_out.index)
    out = build_v2_3_result(close_df, turnover_df, common_index)
    mismatch_diagnostics = build_signal_execution_mismatch_diagnostics(close_df, out)
    if COSTED_NAV_CSV.exists() and COSTED_NAV_CSV not in stale_outputs:
        previous = pd.read_csv(COSTED_NAV_CSV, parse_dates=["date"])
        v2_0.base_mod.assert_no_historical_rewrite(
            previous=previous,
            candidate=out.rename_axis("date").reset_index(),
            key_columns=V2_3_REWRITE_AUDIT_KEY_COLUMNS,
            allowed_tail_rows=max(LOOKBACK + 20, 40),
            label="v2.3 official costed NAV",
            audit_path=OUTPUT_DIR / f"{OUTPUT_PREFIX}_historical_rewrite_audit.csv",
        )

    _atomic_write_csv(out, COSTED_NAV_CSV, index_label="date", encoding="utf-8-sig")
    _atomic_write_csv(out.rename_axis("date").reset_index(), NAV_CSV, index=False, encoding="utf-8-sig")
    signal_row = _build_signal_row(out, reference_summary)
    apply_signal_execution_mismatch_columns(signal_row, mismatch_diagnostics)
    _atomic_write_text(LATEST_SIGNAL_CSV, signal_row.to_csv(index=False), encoding="utf-8")
    perf_payload = build_performance_payload(out["return_net"].fillna(0.0), source_label="costed_v2_3")

    data_lineage = v2_0.overlay_mod._build_v2_data_lineage()
    summary = dict(reference_summary)
    summary["strategy"] = OUTPUT_PREFIX
    summary["version"] = VERSION
    summary["version_role"] = EXPECTED_VERSION_ROLE
    summary["version_note"] = (
        "Formal v2.3 spread-NAV log-WLS target-volatility overlay. Uses exp half-life 3.0 weighted log slope on "
        "17 trading days of always-on 1.0x hedged signal spread NAV, executes with 0.8x CSI1000 hedge, no R2 gate, "
        "13% score exit buffer, no peak-decay signal-quality derisk, cash-day yield credited at 2% annualized, "
        "60-day realized volatility, 25% annual target volatility, max 1.5x leverage, 30% scale rebalance threshold, "
        "10bp leg-turnover scale-change cost, scaled embedded-lineage base "
        "trading cost, and 3% annual financing cost on exposure above 1.0x."
    )
    summary.setdefault("core_params", {})
    summary["core_params"]["fixed_hedge_ratio"] = BASE_HEDGE_RATIO
    summary["core_params"]["signal_spread_hedge_ratio"] = SIGNAL_SPREAD_HEDGE_RATIO
    summary["core_params"]["execution_hedge_ratio"] = EXECUTION_HEDGE_RATIO
    summary["core_params"]["signal_model"] = {
        "type": "spread_nav_log_wls_exp",
        "lookback": LOOKBACK,
        "halflife": HALFLIFE,
        "weights_oldest_to_newest": list(exp_weights()),
        "score_definition": "annualized weighted log slope of always-on 1.0x hedged signal spread NAV",
        "nav_csv_momentum_gap_column_alias_note": (
            "momentum_gap stores annualized_log_wls_score for v2.0 compatibility, not raw microcap minus hedge gap"
        ),
        "r2_gate": None,
        "legacy_momentum_gap_field": "same value as annualized_log_wls_score for v2.0 compatibility",
    }
    summary["core_params"]["momentum_gap_entry_threshold"] = 0.0
    summary["core_params"]["momentum_gap_exit_buffer"] = MOMENTUM_GAP_EXIT_BUFFER
    summary["core_params"]["signal_quality_derisk"] = {"enabled": False, "type": "removed_no_peak_decay"}
    summary["core_params"]["target_volatility_scaling"] = {
        "target_vol": TARGET_VOL,
        "vol_window": int(v2_0.overlay_mod.TARGET_VOL_WINDOW),
        "max_leverage": float(v2_0.overlay_mod.TARGET_VOL_MAX_LEVERAGE),
        "min_leverage": float(v2_0.overlay_mod.TARGET_VOL_MIN_LEVERAGE),
        "scale_change_cost": float(v2_0.overlay_mod.TARGET_VOL_SCALE_CHANGE_COST),
        "scale_rebalance_threshold": float(TARGET_VOL_SCALE_REBALANCE_THRESHOLD),
        "financing_rate": float(v2_0.overlay_mod.TARGET_VOL_FINANCING_RATE),
        "cash_day_yield": CASH_DAY_YIELD,
        "idle_credit_on_cash_day": True,
        "idle_cash_return": "credited on full cash days; active partial idle cash remains handled by target-vol overlay",
        "trading_days": TRADING_DAYS,
        "timing": "current execution scale uses T-1 realized volatility; next-session target scale uses T close realized volatility",
    }
    summary["core_params"]["signal_execution_mismatch_diagnostics"] = mismatch_diagnostics
    summary["latest_trade_date"] = str(pd.Timestamp(signal_row.iloc[0]["date"]).date())
    summary["latest_nav_date"] = str(pd.Timestamp(out.index.max()).date())
    summary["latest_signal"] = signal_row.iloc[0].drop(labels=["date"], errors="ignore").to_dict()
    summary["data_lineage"] = data_lineage
    summary["performance_source_label"] = "costed_v2_3"
    summary["performance_snapshot"] = perf_payload["summary"]
    summary["base_fingerprint"] = current_base_fingerprint()
    _atomic_write_text(SUMMARY_JSON, _json_dumps(summary), encoding="utf-8")
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
    for path in _stale_outputs_to_remove_after_generate(stale_outputs, regenerated_outputs):
        path.unlink(missing_ok=True)
    return summary, signal_row, out


def generate_v2_3_outputs() -> tuple[dict[str, object], pd.DataFrame, pd.DataFrame]:
    with v2_3_output_lock():
        return _generate_v2_3_outputs_unlocked()


def _build_realtime_v2_3_outputs_unlocked() -> tuple[pd.DataFrame, dict[str, object], pd.DataFrame]:
    ensure_output_dir()
    realtime_base = v2_0.realtime_core.load_realtime_base()
    close_df = realtime_base.realtime_close_df[["microcap", "hedge"]].sort_index()
    official_index = pd.DatetimeIndex(close_df.index)
    common_index = build_v2_3_common_index(close_df, official_index)
    gross = build_spread_log_wls_gross(close_df, common_index)
    buffered = v2_0.base_mod.apply_momentum_gap_exit_buffer(gross, MOMENTUM_GAP_EXIT_BUFFER)
    costed = v2_0.base_mod.apply_momentum_gap_no_peak_decay_cost_model(buffered, realtime_base.turnover_df)
    is_snapshot = bool(realtime_base.meta.get("snapshot_row_appended", False))
    signal_timing = "intraday_hypothetical_if_now_close" if is_snapshot else "close_confirmed_anchor"
    out = apply_target_vol(costed, TARGET_VOL, treat_last_row_as_snapshot=is_snapshot)
    assert_realtime_target_vol_lag_fresh(out)
    mismatch_diagnostics = build_signal_execution_mismatch_diagnostics(close_df, out)
    signal_row = _build_signal_row(out, realtime_base.reference_summary)
    apply_signal_execution_mismatch_columns(signal_row, mismatch_diagnostics)
    signal_row = v2_0.realtime_core.base_mod.augment_signal_with_member_rebalance(
        signal_row,
        realtime_base.context.get("changes_df"),
    )
    v2_0.overlay_mod._apply_realtime_meta_columns_to_signal_row(signal_row, realtime_base.meta)
    signal_row["quote_coverage"] = f"{realtime_base.meta.get('member_price_count', 0)}/{realtime_base.meta.get('member_count', 0)}"
    signal_row["target_vol_signal_timing"] = signal_timing
    signal_row["signal_timing"] = signal_timing
    signal_row["official_close_confirmed_signal"] = not is_snapshot
    _atomic_write_text(REALTIME_SIGNAL_CSV, signal_row.to_csv(index=False), encoding="utf-8")
    return signal_row, realtime_base.meta, out


def build_realtime_v2_3_outputs() -> tuple[pd.DataFrame, dict[str, object], pd.DataFrame]:
    with v2_3_realtime_output_lock():
        return _build_realtime_v2_3_outputs_unlocked()


def _print_scale_fields(row: pd.Series, include_frozen: bool = False) -> None:
    v2_0.overlay_mod._print_scale_fields(row, include_frozen=include_frozen)


def _print_signal_query() -> None:
    _, signal_df, _ = generate_v2_3_outputs()
    row = signal_df.iloc[0]
    print("signal")
    print("strategy_version: v2.3")
    print("base_version: embedded_v2_base")
    print("signal_model: spread-NAV log-WLS exp half-life 3.0, lookback 17, signal spread 1.0x, execution hedge 0.8x, no R2 gate")
    print(f"overlay: score buffer {MOMENTUM_GAP_EXIT_BUFFER:.2f}, no peak-decay derisk, target volatility {TARGET_VOL:.0%}, cash-day yield {CASH_DAY_YIELD:.0%}")
    print(f"current_holding: {row['current_holding']}")
    print(f"next_holding: {row['next_holding']}")
    print(f"trade_state: {row.get('effective_trade_state', row.get('trade_state', 'hold'))}")
    print(f"holding_trade_state: {row.get('holding_trade_state', row.get('momentum_trade_state', 'hold'))}")
    print(f"scale_trade_state: {row.get('scale_trade_state', 'hold_scale')}")
    print(f"signal_date: {pd.Timestamp(row['date']).strftime('%Y-%m-%d')}")
    print(f"annualized_log_wls_score: {float(row.get('annualized_log_wls_score', row.get('momentum_gap', 0.0))):+.4%}")
    print(f"log_wls_r2: {float(row.get('log_wls_r2', 0.0)):.4f}")
    print("momentum_gap_legacy_note: legacy field is the annualized log-WLS score, not plain gap")
    _print_scale_fields(row, include_frozen=False)
    print(f"official_close_confirmed_signal: {row.get('official_close_confirmed_signal', True)}")
    print(SUMMARY_JSON)
    print(LATEST_SIGNAL_CSV)


def _print_realtime_signal_query() -> None:
    def emit() -> None:
        signal_df, meta, _ = build_realtime_v2_3_outputs()
        row = signal_df.iloc[0]
        print("realtime_signal")
        print("strategy_version: v2.3")
        print("base_version: embedded_v2_base")
        print("signal_model: spread-NAV log-WLS exp half-life 3.0, lookback 17, signal spread 1.0x, execution hedge 0.8x, no R2 gate")
        print(f"overlay: score buffer {MOMENTUM_GAP_EXIT_BUFFER:.2f}, no peak-decay derisk, target volatility {TARGET_VOL:.0%}, cash-day yield {CASH_DAY_YIELD:.0%}")
        print(f"snapshot_time: {meta.get('snapshot_time')}")
        print(f"latest_anchor_trade_date: {meta.get('latest_anchor_trade_date')}")
        print(f"quote_trade_date: {meta.get('quote_trade_date', '')}")
        print(f"current_holding: {row['current_holding']}")
        print(f"next_holding: {row['next_holding']}")
        print(f"trade_state: {row.get('effective_trade_state', row.get('trade_state', 'hold'))}")
        print(f"holding_trade_state: {row.get('holding_trade_state', row.get('momentum_trade_state', 'hold'))}")
        print(f"scale_trade_state: {row.get('scale_trade_state', 'hold_scale')}")
        print(f"target_vol_signal_timing: {row.get('target_vol_signal_timing', row.get('signal_timing', ''))}")
        _print_scale_fields(row, include_frozen=True)
        print(f"official_close_confirmed_signal: {row.get('official_close_confirmed_signal', False)}")
        print(f"snapshot_row_appended: {bool(meta.get('snapshot_row_appended', False))}")
        print(f"annualized_log_wls_score: {float(row.get('annualized_log_wls_score', row.get('momentum_gap', 0.0))):+.4%}")
        print(f"log_wls_r2: {float(row.get('log_wls_r2', 0.0)):.4f}")
        print("momentum_gap_legacy_note: legacy field is the annualized log-WLS score, not plain gap")
        print(f"quote_source: {meta.get('quote_source')}")
        print(f"hedge_quote_source: {meta.get('hedge_quote_source')}")
        print(f"quote_coverage: {meta.get('member_price_count')}/{meta.get('member_count')}")
        print(REALTIME_SIGNAL_CSV)

    try:
        v2_0.run_realtime_query_with_fresh_state(emit)
    except Exception as exc:
        if v2_0.is_realtime_actionability_error(exc):
            v2_0.print_realtime_blocked_result("v2.3", exc)
            return
        raise


def _print_performance_query(query: str) -> None:
    generate_v2_3_outputs()
    perf_df = pd.read_csv(COSTED_NAV_CSV, parse_dates=["date"]).sort_values("date").set_index("date")
    old_title = v2_0.embedded_context.base_mod.STRATEGY_TITLE
    v2_0.embedded_context.base_mod.STRATEGY_TITLE = "Top100 Microcap Mom16 Biweekly v2.3"
    try:
        with v2_3_output_lock():
            v2_0.embedded_context.base_mod.build_performance_outputs(
                perf_df=perf_df,
                ret_col="return_net",
                nav_col="nav_net",
                source_label="costed_v2_3",
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


def normalize_v2_3_query_text(query: str) -> str:
    text = str(query or "").strip()
    embedded_context = getattr(v2_0, "embedded_context", None)
    base_mod = getattr(embedded_context, "base_mod", None)
    base_normalizer = getattr(base_mod, "normalize_query_text", None)
    if base_normalizer is None:
        base_normalizer = getattr(v2_0, "normalize_query_text", None)
    if callable(base_normalizer):
        text = base_normalizer(text)
    text = re.sub(r"[\s?？!！。．.]+$", "", text).strip()
    compact = re.sub(r"\s+", "", text)
    ascii_key = re.sub(r"[\s-]+", "_", text.lower())
    if compact in {"信号", "信號"} or ascii_key == "signal":
        return "信号"
    if compact in {"实时信号", "實時信號"} or ascii_key in {"realtime_signal", "live_signal"}:
        return "实时信号"
    return text


def _handle_query(query: str) -> None:
    normalized = normalize_v2_3_query_text(query)
    if normalized == "信号":
        _print_signal_query()
        return
    if normalized == "实时信号":
        _print_realtime_signal_query()
        return
    if v2_0.embedded_context.base_mod.PERFORMANCE_PATTERN.search(query) or (
        normalized != query and v2_0.embedded_context.base_mod.PERFORMANCE_PATTERN.search(normalized)
    ):
        _print_performance_query(query)
        return
    raise ValueError("v2.3 supports: 信号 / 实时信号 / 表现 <区间>")


def main(argv: list[str] | None = None) -> None:
    args = parse_v2_3_args(sys.argv[1:] if argv is None else argv)
    configure_runtime(args)
    query = " ".join(args.query_tokens).strip()
    if query:
        _handle_query(query)
        return
    generate_v2_3_outputs()
    print(str(SUMMARY_JSON))
    print(str(LATEST_SIGNAL_CSV))
    print(str(COSTED_NAV_CSV))

if __name__ == "__main__":
    main()
