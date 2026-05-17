from __future__ import annotations

import json
import math
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

OUTPUT_PREFIX = "microcap_top100_mom16_biweekly_live_v2_1"
SUMMARY_JSON = OUTPUT_DIR / f"{OUTPUT_PREFIX}_summary.json"
LATEST_SIGNAL_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_latest_signal.csv"
REALTIME_SIGNAL_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_realtime_signal.csv"
NAV_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_nav.csv"
COSTED_NAV_CSV = OUTPUT_DIR / "microcap_top100_mom16_recency_sqrt_targetvol25_max1p5_v2_1_costed_nav.csv"
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

VERSION = "2.1"
EXPECTED_VERSION_ROLE = "simplified_recency_weighted_target_vol_overlay"
EXPECTED_VERSION_NOTE_PREFIX = "Simplified v2.1 recency-weighted Mom16 target-volatility overlay."
LOOKBACK = 16
RECENCY_POWER = 0.50
MOMENTUM_GAP_EXIT_BUFFER = 0.0030
TARGET_VOL = 0.25
EXCLUDED_LAYER = "momentum_gap_peak_decay_derisk"

BASE_HEDGE_RATIO = float(v2_0.BASE_HEDGE_RATIO)
TRADING_DAYS = int(v2_0.overlay_mod.TARGET_VOL_TRADING_DAYS)


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


def recency_weights(power: float = RECENCY_POWER) -> tuple[float, ...]:
    raw = np.arange(1, LOOKBACK + 1, dtype=float) ** float(power)
    return tuple((raw / raw.sum()).tolist())


def weighted_daily_return(series: pd.Series, weights: tuple[float, ...]) -> pd.Series:
    daily = series.pct_change(fill_method=None)
    w = np.asarray(weights, dtype=float)
    return daily.rolling(LOOKBACK, min_periods=LOOKBACK).apply(lambda values: float(np.dot(values, w)), raw=True)


def _valid_weighted_index(close_df: pd.DataFrame) -> pd.DatetimeIndex:
    weights = recency_weights()
    valid = weighted_daily_return(close_df["microcap"], weights).notna() & weighted_daily_return(close_df["hedge"], weights).notna()
    return pd.DatetimeIndex(close_df.index[valid])


def build_recency_weighted_gross(close_df: pd.DataFrame, index: pd.DatetimeIndex | None = None) -> pd.DataFrame:
    close_df = close_df.sort_index()
    common_index = _valid_weighted_index(close_df) if index is None else pd.DatetimeIndex(index)
    weights = recency_weights()
    micro_score = (weighted_daily_return(close_df["microcap"], weights) * LOOKBACK).loc[common_index]
    hedge_score = (weighted_daily_return(close_df["hedge"], weights) * LOOKBACK).loc[common_index]
    score = micro_score - hedge_score
    signal_on = score.gt(0.0)
    current_active = signal_on.shift(1, fill_value=False)
    microcap_ret = close_df["microcap"].pct_change(fill_method=None).loc[common_index]
    hedge_ret = close_df["hedge"].pct_change(fill_method=None).loc[common_index]
    active_spread_ret = microcap_ret.fillna(0.0) - BASE_HEDGE_RATIO * hedge_ret.fillna(0.0)
    futures_drag = pd.Series(
        np.where(current_active, float(v2_0.base_mod.FUTURES_DRAG) * BASE_HEDGE_RATIO, 0.0),
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
            "hedge_ret": hedge_ret,
            "microcap_mom": micro_score,
            "hedge_mom": hedge_score,
            "momentum_gap": score,
            "recency_power": RECENCY_POWER,
            "recency_weight_oldest_to_newest": ",".join(f"{w:.8f}" for w in weights),
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


def apply_target_vol(costed_base: pd.DataFrame, target_vol: float = TARGET_VOL, *, treat_last_row_as_snapshot: bool = False) -> pd.DataFrame:
    globals_dict = v2_0.overlay_mod.apply_target_vol_scaling.__globals__
    old_global = globals_dict.get("TARGET_VOL")
    old_attr = v2_0.overlay_mod.TARGET_VOL
    try:
        globals_dict["TARGET_VOL"] = float(target_vol)
        v2_0.overlay_mod.TARGET_VOL = float(target_vol)
        out = v2_0.overlay_mod.apply_target_vol_scaling(costed_base, treat_last_row_as_snapshot=treat_last_row_as_snapshot)
    finally:
        globals_dict["TARGET_VOL"] = old_global
        v2_0.overlay_mod.TARGET_VOL = old_attr
    out["version"] = VERSION
    out["base_version"] = "embedded_v2_base"
    out["overlay_type"] = "recency_weighted_mom16_gap_buffer_target_vol"
    out["signal_quality_derisk_triggered"] = False
    out["excluded_layer"] = EXCLUDED_LAYER
    return out


def build_v2_1_result(
    close_df: pd.DataFrame,
    turnover_df: pd.DataFrame,
    common_index: pd.DatetimeIndex | None = None,
) -> pd.DataFrame:
    if common_index is None:
        common_index = _valid_weighted_index(close_df)
    gross = build_recency_weighted_gross(close_df, common_index)
    buffered = v2_0.base_mod.apply_momentum_gap_exit_buffer(gross, MOMENTUM_GAP_EXIT_BUFFER)
    costed = apply_cost(buffered, turnover_df)
    return apply_target_vol(costed, TARGET_VOL)


def current_base_fingerprint() -> dict[str, object]:
    base = dict(v2_0.embedded_context.current_base_fingerprint())
    return {
        "base_version": "embedded_v2_base",
        "strategy_version": VERSION,
        "base_fingerprint": base,
        "signal_model": "recency_weighted_mom16_daily_return",
        "lookback": LOOKBACK,
        "recency_power": RECENCY_POWER,
        "recency_weight_oldest_to_newest": list(recency_weights()),
        "common_index_source": "intersection of valid recency-weighted signal dates and official v2.0 output index",
        "score_scaled_by_lookback_for_gap_buffer": True,
        "base_hedge_ratio": BASE_HEDGE_RATIO,
        "momentum_gap_exit_buffer": MOMENTUM_GAP_EXIT_BUFFER,
        "target_vol": TARGET_VOL,
        "target_vol_window": int(v2_0.overlay_mod.TARGET_VOL_WINDOW),
        "target_vol_max_leverage": float(v2_0.overlay_mod.TARGET_VOL_MAX_LEVERAGE),
        "target_vol_scale_change_cost": float(v2_0.overlay_mod.TARGET_VOL_SCALE_CHANGE_COST),
        "target_vol_financing_rate": float(v2_0.overlay_mod.TARGET_VOL_FINANCING_RATE),
        "excluded_layer": EXCLUDED_LAYER,
    }


def summary_matches_current_v2_1_base(summary: dict[str, object]) -> bool:
    if not isinstance(summary, dict):
        return False
    if str(summary.get("version")) != VERSION:
        return False
    if str(summary.get("version_role")) != EXPECTED_VERSION_ROLE:
        return False
    if not str(summary.get("version_note", "")).startswith(EXPECTED_VERSION_NOTE_PREFIX):
        return False
    return summary.get("base_fingerprint") == current_base_fingerprint()


def incompatible_v2_1_outputs() -> list[Path]:
    outputs = [
        SUMMARY_JSON,
        LATEST_SIGNAL_CSV,
        REALTIME_SIGNAL_CSV,
        NAV_CSV,
        COSTED_NAV_CSV,
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
    if summary_matches_current_v2_1_base(summary):
        return []
    return outputs


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


def build_performance_payload(ret: pd.Series, source_label: str = "costed_v2_1") -> dict[str, object]:
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
    plt.plot(nav_df["date"], nav_df["nav_net"], label="v2.1 nav_net")
    plt.title("Top100 Microcap Mom16 v2.1 Costed NAV")
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
    row["overlay_type"] = "recency_weighted_mom16_gap_buffer_target_vol"
    row["signal_model"] = "recency_weighted_mom16_daily_return"
    row["recency_power"] = RECENCY_POWER
    row["lookback"] = LOOKBACK
    row["momentum_gap_exit_buffer"] = MOMENTUM_GAP_EXIT_BUFFER
    row["signal_quality_derisk_triggered"] = False
    row["excluded_layer"] = EXCLUDED_LAYER
    row["target_vol"] = TARGET_VOL
    return row


def _close_df_from_base(base_gross_cached: pd.DataFrame) -> pd.DataFrame:
    return base_gross_cached[["microcap_close", "hedge_close"]].rename(
        columns={"microcap_close": "microcap", "hedge_close": "hedge"}
    ).sort_index()


def generate_v2_1_outputs() -> tuple[dict[str, object], pd.DataFrame, pd.DataFrame]:
    ensure_output_dir()
    _, _, official_v2_0_out = v2_0.generate_v2_0_outputs()
    reference_summary, base_gross_cached, turnover_df = v2_0.embedded_context._load_embedded_base_context()
    stale_outputs = incompatible_v2_1_outputs()
    close_df = _close_df_from_base(base_gross_cached)
    common_index = pd.DatetimeIndex(_valid_weighted_index(close_df).intersection(official_v2_0_out.index)).sort_values()
    out = build_v2_1_result(close_df, turnover_df, common_index)
    if COSTED_NAV_CSV.exists() and COSTED_NAV_CSV not in stale_outputs:
        previous = pd.read_csv(COSTED_NAV_CSV, parse_dates=["date"])
        v2_0.base_mod.assert_no_historical_rewrite(
            previous=previous,
            candidate=out.rename_axis("date").reset_index(),
            key_columns=["return_net", "holding", "next_holding", "base_pre_cost_return"],
            allowed_tail_rows=max(LOOKBACK + 20, 40),
            label="v2.1 official costed NAV",
            audit_path=OUTPUT_DIR / f"{OUTPUT_PREFIX}_historical_rewrite_audit.csv",
        )

    _atomic_write_csv(out, COSTED_NAV_CSV, index_label="date", encoding="utf-8-sig")
    _atomic_write_csv(out.rename_axis("date").reset_index(), NAV_CSV, index=False, encoding="utf-8-sig")
    signal_row = _build_signal_row(out, reference_summary)
    _atomic_write_text(LATEST_SIGNAL_CSV, signal_row.to_csv(index=False), encoding="utf-8")
    perf_payload = build_performance_payload(out["return_net"].fillna(0.0), source_label="costed_v2_1")

    data_lineage = v2_0.overlay_mod._build_v2_data_lineage()
    summary = dict(reference_summary)
    summary["strategy"] = OUTPUT_PREFIX
    summary["version"] = VERSION
    summary["version_role"] = EXPECTED_VERSION_ROLE
    summary["version_note"] = (
        "Simplified v2.1 recency-weighted Mom16 target-volatility overlay. Uses sqrt(1..16) recency weights, "
        "0.30% momentum-gap exit buffer, 60-day realized volatility, 25% annual target volatility, max 1.5x leverage, "
        "10bp leg-turnover scale-change cost, scaled embedded-lineage base trading cost, and 3% annual financing cost "
        "on exposure above 1.0x. The v2.0 peak-decay derisk layer is intentionally excluded."
    )
    summary.setdefault("core_params", {})
    summary["core_params"]["fixed_hedge_ratio"] = BASE_HEDGE_RATIO
    summary["core_params"]["signal_model"] = {
        "type": "recency_weighted_mom16_daily_return",
        "lookback": LOOKBACK,
        "recency_power": RECENCY_POWER,
        "weights_oldest_to_newest": list(recency_weights()),
        "score_scaled_by_lookback_for_gap_buffer": True,
    }
    summary["core_params"]["momentum_gap_entry_threshold"] = 0.0
    summary["core_params"]["momentum_gap_exit_buffer"] = MOMENTUM_GAP_EXIT_BUFFER
    summary["core_params"]["excluded_signal_quality_derisk"] = EXCLUDED_LAYER
    summary["core_params"]["target_volatility_scaling"] = {
        "target_vol": TARGET_VOL,
        "vol_window": int(v2_0.overlay_mod.TARGET_VOL_WINDOW),
        "max_leverage": float(v2_0.overlay_mod.TARGET_VOL_MAX_LEVERAGE),
        "min_leverage": float(v2_0.overlay_mod.TARGET_VOL_MIN_LEVERAGE),
        "scale_change_cost": float(v2_0.overlay_mod.TARGET_VOL_SCALE_CHANGE_COST),
        "scale_rebalance_threshold": float(v2_0.overlay_mod.TARGET_VOL_SCALE_REBALANCE_THRESHOLD),
        "financing_rate": float(v2_0.overlay_mod.TARGET_VOL_FINANCING_RATE),
        "trading_days": TRADING_DAYS,
        "timing": "current execution scale uses T-1 realized volatility; next-session target scale uses T close realized volatility",
    }
    summary["latest_trade_date"] = str(pd.Timestamp(signal_row.iloc[0]["date"]).date())
    summary["latest_nav_date"] = str(pd.Timestamp(out.index.max()).date())
    summary["latest_signal"] = signal_row.iloc[0].drop(labels=["date"], errors="ignore").to_dict()
    summary["data_lineage"] = data_lineage
    summary["performance_source_label"] = "costed_v2_1"
    summary["performance_snapshot"] = perf_payload["summary"]
    summary["base_fingerprint"] = current_base_fingerprint()
    _atomic_write_text(SUMMARY_JSON, _json_dumps(summary), encoding="utf-8")
    for path in stale_outputs:
        if path not in {SUMMARY_JSON, LATEST_SIGNAL_CSV, NAV_CSV, COSTED_NAV_CSV, PERF_SUMMARY_CSV, PERF_YEARLY_CSV, PERF_NAV_CSV, PERF_JSON, PERF_PNG}:
            path.unlink(missing_ok=True)
    return summary, signal_row, out


def build_realtime_v2_1_outputs() -> tuple[pd.DataFrame, dict[str, object], pd.DataFrame]:
    ensure_output_dir()
    realtime_base = v2_0.realtime_core.load_realtime_base()
    close_df = realtime_base.realtime_close_df[["microcap", "hedge"]].sort_index()
    common_index = _valid_weighted_index(close_df)
    gross = build_recency_weighted_gross(close_df, common_index)
    buffered = v2_0.base_mod.apply_momentum_gap_exit_buffer(gross, MOMENTUM_GAP_EXIT_BUFFER)
    costed = apply_cost(buffered, realtime_base.turnover_df)
    out = apply_target_vol(costed, TARGET_VOL, treat_last_row_as_snapshot=True)
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
    _, signal_df, _ = generate_v2_1_outputs()
    row = signal_df.iloc[0]
    print("signal")
    print("strategy_version: v2.1")
    print("base_version: embedded_v2_base")
    print("signal_model: recency-weighted Mom16 sqrt(1..16)")
    print(f"overlay: gap buffer {MOMENTUM_GAP_EXIT_BUFFER:.2%}, target volatility {TARGET_VOL:.0%}, no peak-decay")
    print(f"current_holding: {row['current_holding']}")
    print(f"next_holding: {row['next_holding']}")
    print(f"trade_state: {row.get('effective_trade_state', row.get('trade_state', 'hold'))}")
    print(f"holding_trade_state: {row.get('holding_trade_state', row.get('momentum_trade_state', 'hold'))}")
    print(f"scale_trade_state: {row.get('scale_trade_state', 'hold_scale')}")
    print(f"signal_date: {pd.Timestamp(row['date']).strftime('%Y-%m-%d')}")
    print(f"microcap_mom: {float(row.get('microcap_mom', 0.0)):+.4%}")
    print(f"hedge_mom: {float(row.get('hedge_mom', 0.0)):+.4%}")
    print(f"momentum_gap: {float(row.get('momentum_gap', 0.0)):+.4%}")
    _print_scale_fields(row, include_frozen=False)
    print(f"official_close_confirmed_signal: {row.get('official_close_confirmed_signal', True)}")
    print(SUMMARY_JSON)
    print(LATEST_SIGNAL_CSV)


def _print_realtime_signal_query() -> None:
    signal_df, meta, _ = build_realtime_v2_1_outputs()
    row = signal_df.iloc[0]
    print("realtime_signal")
    print("strategy_version: v2.1")
    print("base_version: embedded_v2_base")
    print("signal_model: recency-weighted Mom16 sqrt(1..16)")
    print(f"overlay: gap buffer {MOMENTUM_GAP_EXIT_BUFFER:.2%}, target volatility {TARGET_VOL:.0%}, no peak-decay")
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
    print(f"microcap_mom: {float(row.get('microcap_mom', 0.0)):+.4%}")
    print(f"hedge_mom: {float(row.get('hedge_mom', 0.0)):+.4%}")
    print(f"momentum_gap: {float(row.get('momentum_gap', 0.0)):+.4%}")
    print(f"quote_source: {meta.get('quote_source')}")
    print(f"hedge_quote_source: {meta.get('hedge_quote_source')}")
    print(f"quote_coverage: {meta.get('member_price_count')}/{meta.get('member_count')}")
    print(REALTIME_SIGNAL_CSV)


def _print_performance_query(query: str) -> None:
    generate_v2_1_outputs()
    perf_df = pd.read_csv(COSTED_NAV_CSV, parse_dates=["date"]).sort_values("date").set_index("date")
    old_title = v2_0.embedded_context.base_mod.STRATEGY_TITLE
    v2_0.embedded_context.base_mod.STRATEGY_TITLE = "Top100 Microcap Mom16 Biweekly v2.1"
    try:
        v2_0.embedded_context.base_mod.build_performance_outputs(
            perf_df=perf_df,
            ret_col="return_net",
            nav_col="nav_net",
            source_label="costed_v2_1",
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
    if query in {"信号", "淇″彿", "娣団€冲娇"}:
        _print_signal_query()
        return
    if query in {"实时信号", "瀹炴椂淇″彿", "鐎圭偞妞傛穱鈥冲娇"}:
        _print_realtime_signal_query()
        return
    if v2_0.embedded_context.base_mod.PERFORMANCE_PATTERN.search(query):
        _print_performance_query(query)
        return
    raise ValueError("v2.1 supports: 信号 / 实时信号 / 表现 <区间>")


def main() -> None:
    query = " ".join(sys.argv[1:]).strip()
    if query:
        _handle_query(query)
        return
    generate_v2_1_outputs()
    print(str(SUMMARY_JSON))
    print(str(LATEST_SIGNAL_CSV))
    print(str(COSTED_NAV_CSV))


if __name__ == "__main__":
    main()
