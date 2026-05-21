from __future__ import annotations

import itertools
import json
import math
import subprocess
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd


RUN_DIR = Path(__file__).resolve().parent
REPO = RUN_DIR.parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import microcap_top100_mom16_biweekly_live_v2_0 as v2_0
import microcap_top100_mom16_biweekly_live_v2_3 as v2_3


LOOKBACKS = [12, 15, 17, 20, 24]
HALFLIFES = [3.0, 4.0, 5.0, 6.0, 8.0]
EXIT_BUFFERS = [0.05, 0.08, 0.13, 0.20]
TARGET_VOLS = [0.15, 0.20, 0.25, 0.30]
SCALE_THRESHOLDS = [0.10, 0.20, 0.30, 0.50]
EXEC_HEDGE_RATIOS = [0.6, 0.8, 1.0]
SIGNAL_SPREAD_HEDGE_RATIO = 1.0

TRADING_DAYS = int(v2_0.overlay_mod.TARGET_VOL_TRADING_DAYS)
FORMAL_START_DATE = pd.Timestamp("2010-05-05")
HOLDING_LONG = "long_microcap_short_zz1000"
WINDOWS = {
    "full": None,
    "last_10y": pd.DateOffset(years=10),
    "last_8y": pd.DateOffset(years=8),
    "last_5y": pd.DateOffset(years=5),
    "last_3y": pd.DateOffset(years=3),
    "last_1y": pd.DateOffset(years=1),
}


def git_value(args: list[str]) -> str:
    try:
        return subprocess.check_output(args, cwd=REPO, text=True, encoding="utf-8", errors="replace").strip()
    except Exception as exc:
        return f"ERROR: {exc}"


def pct_label(value: float) -> str:
    return f"{int(round(value * 100)):02d}"


def ratio_label(value: float) -> str:
    return f"{value:.2f}".replace(".", "p")


def candidate_label(
    *,
    version: str,
    lookback: int,
    halflife: float | None,
    exit_buffer: float,
    target_vol: float,
    scale_threshold: float,
    execution_hedge_ratio: float,
) -> str:
    parts = [version, f"lb{lookback}"]
    if halflife is not None:
        parts.append(f"h{ratio_label(halflife)}")
    parts.extend(
        [
            f"gap{pct_label(exit_buffer)}",
            f"tv{pct_label(target_vol)}",
            f"thr{pct_label(scale_threshold)}",
            f"exec{ratio_label(execution_hedge_ratio)}",
        ]
    )
    return "_".join(parts)


def exp_weights(lookback: int, halflife: float) -> np.ndarray:
    age_from_latest = np.arange(int(lookback) - 1, -1, -1, dtype=float)
    raw = 0.5 ** (age_from_latest / float(halflife))
    return raw / raw.sum()


def log_wls_score_and_r2(spread_nav: pd.Series, lookback: int, halflife: float) -> pd.DataFrame:
    weights = exp_weights(lookback, halflife)
    y = np.log(pd.to_numeric(spread_nav, errors="coerce").replace(0.0, np.nan))
    x = np.arange(int(lookback), dtype=float)
    w_sum = float(weights.sum())
    x_bar = float((weights * x).sum() / w_sum)
    x_centered = x - x_bar
    denom = float((weights * x_centered**2).sum())
    values = y.to_numpy(dtype=float)
    score = np.full(len(y), np.nan, dtype=float)
    r2 = np.full(len(y), np.nan, dtype=float)
    for end in range(int(lookback) - 1, len(values)):
        window = values[end - int(lookback) + 1 : end + 1]
        if len(window) != int(lookback) or not np.isfinite(window).all() or denom <= 0:
            continue
        y_bar = float((weights * window).sum() / w_sum)
        slope = float((weights * x_centered * (window - y_bar)).sum() / denom)
        fitted = y_bar + slope * x_centered
        ss_tot = float((weights * (window - y_bar) ** 2).sum())
        ss_res = float((weights * (window - fitted) ** 2).sum())
        score[end] = slope * TRADING_DAYS
        r2[end] = 1.0 if ss_tot <= 0 else max(0.0, min(1.0, 1.0 - ss_res / ss_tot))
    return pd.DataFrame({"annualized_log_wls_score": score, "log_wls_r2": r2}, index=y.index)


def apply_gap_buffer(gross: pd.DataFrame, exit_buffer: float, hedge_ratio: float) -> pd.DataFrame:
    out = gross.copy().sort_index()
    holding = False
    rows: list[dict[str, object]] = []
    for _dt, row in out.iterrows():
        active_ret = 0.0
        drag = float(v2_0.base_mod.FUTURES_DRAG) * float(hedge_ratio) if holding else 0.0
        if holding and pd.notna(row["microcap_ret"]) and pd.notna(row["hedge_ret"]):
            active_ret = float(row["microcap_ret"] - float(hedge_ratio) * row["hedge_ret"])
        gap = float(row["momentum_gap"]) if pd.notna(row["momentum_gap"]) else np.nan
        signal_on = bool(gap >= -float(exit_buffer)) if holding and pd.notna(gap) else bool(pd.notna(gap) and gap > 0.0)
        rows.append(
            {
                "holding": HOLDING_LONG if holding else "cash",
                "next_holding": HOLDING_LONG if signal_on else "cash",
                "signal_on": signal_on,
                "return_raw": active_ret - drag,
                "return": active_ret - drag,
                "futures_drag": drag,
                "active_spread_ret": active_ret,
            }
        )
        holding = signal_on
    adjusted = pd.DataFrame(rows, index=out.index)
    for col in adjusted.columns:
        out[col] = adjusted[col]
    out["momentum_gap_exit_buffer"] = float(exit_buffer)
    return out


def build_v23_gross(
    close_df: pd.DataFrame,
    index: pd.DatetimeIndex,
    lookback: int,
    halflife: float,
    signal_spread_hedge_ratio: float,
    execution_hedge_ratio: float,
) -> pd.DataFrame:
    close_df = close_df.sort_index()
    micro_ret = close_df["microcap"].pct_change(fill_method=None)
    hedge_ret = close_df["hedge"].pct_change(fill_method=None)
    signal_drag = float(v2_0.base_mod.FUTURES_DRAG) * float(signal_spread_hedge_ratio)
    spread_ret = micro_ret.fillna(0.0) - float(signal_spread_hedge_ratio) * hedge_ret.fillna(0.0) - signal_drag
    spread_nav = (1.0 + spread_ret.fillna(0.0)).cumprod()
    log_wls = log_wls_score_and_r2(spread_nav, lookback, halflife)
    common_index = pd.DatetimeIndex(index)
    score = pd.to_numeric(log_wls["annualized_log_wls_score"].loc[common_index], errors="coerce")
    r2 = pd.to_numeric(log_wls["log_wls_r2"].loc[common_index], errors="coerce")
    signal_on = score.gt(0.0)
    current_active = signal_on.shift(1, fill_value=False)
    active_spread_ret = micro_ret.loc[common_index].fillna(0.0) - float(execution_hedge_ratio) * hedge_ret.loc[common_index].fillna(0.0)
    execution_drag = float(v2_0.base_mod.FUTURES_DRAG) * float(execution_hedge_ratio)
    futures_drag = pd.Series(np.where(current_active, execution_drag, 0.0), index=common_index, dtype=float)
    gross_ret = pd.Series(np.where(current_active, active_spread_ret - futures_drag, 0.0), index=common_index, dtype=float)
    return pd.DataFrame(
        {
            "return_raw": gross_ret,
            "return": gross_ret,
            "holding": np.where(current_active, HOLDING_LONG, "cash"),
            "next_holding": np.where(signal_on, HOLDING_LONG, "cash"),
            "signal_on": signal_on.astype(bool),
            "microcap_close": close_df["microcap"].loc[common_index],
            "hedge_close": close_df["hedge"].loc[common_index],
            "microcap_ret": micro_ret.loc[common_index],
            "hedge_ret": hedge_ret.loc[common_index],
            "microcap_mom": score,
            "hedge_mom": 0.0,
            "momentum_gap": score,
            "annualized_log_wls_score": score,
            "log_wls_r2": r2,
            "spread_nav": spread_nav.loc[common_index],
            "halflife": float(halflife),
            "futures_drag": futures_drag,
            "active_spread_ret": pd.Series(np.where(current_active, active_spread_ret, 0.0), index=common_index, dtype=float),
            "weight": 1.0,
            "target_vol_execution_scale": 1.0,
        },
        index=common_index,
    )


def build_v20_gross(close_df: pd.DataFrame, index: pd.DatetimeIndex, lookback: int, hedge_ratio: float) -> pd.DataFrame:
    gross = v2_0.hedge_mod.run_backtest(
        close_df=close_df,
        signal_model="momentum",
        lookback=int(lookback),
        bias_n=v2_0.hedge_mod.DEFAULT_BIAS_N,
        bias_mom_day=v2_0.hedge_mod.DEFAULT_BIAS_MOM_DAY,
        futures_drag=float(v2_0.base_mod.FUTURES_DRAG) * float(hedge_ratio),
        require_positive_microcap_mom=False,
        r2_window=v2_0.hedge_mod.DEFAULT_R2_WINDOW,
        r2_threshold=0.0,
        vol_scale_enabled=False,
        target_vol=v2_0.hedge_mod.DEFAULT_TARGET_VOL,
        vol_window=v2_0.hedge_mod.DEFAULT_VOL_WINDOW,
        max_lev=v2_0.hedge_mod.DEFAULT_MAX_LEV,
        min_lev=v2_0.hedge_mod.DEFAULT_MIN_LEV,
        scale_threshold=v2_0.hedge_mod.DEFAULT_SCALE_THRESHOLD,
        hedge_ratio=float(hedge_ratio),
    ).sort_index()
    return gross.loc[pd.DatetimeIndex(index)].copy()


def target_vol_turnover_series(holding: pd.Series, execution_scale: pd.Series, hedge_ratio: float) -> pd.Series:
    prev_holding = holding.shift(1).fillna("cash")
    prev_scale = execution_scale.shift(1).fillna(0.0)
    values = [
        v2_0.overlay_mod.calc_target_vol_turnover(old_holding, old_scale, new_holding, new_scale, hedge_ratio=float(hedge_ratio))
        for old_holding, old_scale, new_holding, new_scale in zip(
            prev_holding,
            prev_scale,
            holding,
            execution_scale.fillna(0.0),
        )
    ]
    return pd.Series(values, index=holding.index, dtype=float)


def apply_target_vol_param(
    base_result: pd.DataFrame,
    target_vol: float,
    scale_threshold: float,
    hedge_ratio: float,
) -> pd.DataFrame:
    out = base_result.copy().sort_index()
    base_return_net = pd.to_numeric(out["return_net"], errors="coerce").fillna(0.0)
    micro = pd.to_numeric(out["microcap_close"], errors="coerce").pct_change(fill_method=None)
    hedge = pd.to_numeric(out["hedge_close"], errors="coerce").pct_change(fill_method=None)
    target_vol_return = (micro - float(hedge_ratio) * hedge).replace([np.inf, -np.inf], np.nan)
    holding = pd.Series(out["holding"].astype(str), index=out.index).replace({"nan": "cash"}).fillna("cash")
    next_holding = pd.Series(out.get("next_holding", holding).astype(str), index=out.index).replace({"nan": "cash"}).fillna(holding)
    active = holding.ne("cash")
    realized_vol = target_vol_return.rolling(v2_0.overlay_mod.TARGET_VOL_WINDOW, min_periods=v2_0.overlay_mod.TARGET_VOL_WINDOW).std(ddof=1) * math.sqrt(TRADING_DAYS)
    scale_raw = (float(target_vol) / realized_vol.replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan).clip(
        lower=float(v2_0.overlay_mod.TARGET_VOL_MIN_LEVERAGE),
        upper=float(v2_0.overlay_mod.TARGET_VOL_MAX_LEVERAGE),
    )
    target_execution_scale = scale_raw.shift(1).fillna(1.0)
    execution_scale = v2_0.overlay_mod.apply_scale_rebalance_threshold(target_execution_scale, active, threshold=float(scale_threshold))
    next_session_target_scale = scale_raw.copy()
    next_session_target_scale.loc[next_holding.eq("cash")] = 0.0
    next_session_target_scale.loc[next_holding.ne("cash")] = next_session_target_scale.loc[next_holding.ne("cash")].fillna(1.0)
    next_session_target_scale = next_session_target_scale.fillna(0.0)
    next_session_actionable_scale = v2_0.overlay_mod.calc_next_session_actionable_scale(
        execution_scale,
        next_session_target_scale,
        next_holding,
        threshold=float(scale_threshold),
    )
    target_vol_turnover = target_vol_turnover_series(holding, execution_scale, hedge_ratio)
    same_holding = holding.eq(holding.shift(1))
    target_vol_costed_turnover = target_vol_turnover.where(same_holding, 0.0).fillna(0.0)
    scale_change_cost = target_vol_costed_turnover * float(v2_0.overlay_mod.TARGET_VOL_SCALE_CHANGE_COST)
    financing_cost = execution_scale.sub(1.0).clip(lower=0.0) * float(v2_0.overlay_mod.TARGET_VOL_FINANCING_RATE) / TRADING_DAYS
    idle_cash_yield = active.astype(float) * execution_scale.rsub(1.0).clip(lower=0.0, upper=1.0) * float(v2_0.overlay_mod.IDLE_CASH_YIELD) / TRADING_DAYS
    base_trade_cost = pd.to_numeric(out.get("total_cost", pd.Series(0.0, index=out.index)), errors="coerce").fillna(0.0)
    if "overlay_pre_cost_return" in out.columns:
        base_pre_cost_return = pd.to_numeric(out["overlay_pre_cost_return"], errors="coerce").fillna(0.0)
    else:
        safe_cost = base_trade_cost.clip(lower=0.0, upper=0.99)
        base_pre_cost_return = (1.0 + base_return_net).div(1.0 - safe_cost).sub(1.0)
    base_trade_cost_scale = v2_0.overlay_mod.calc_base_trade_cost_scale(
        holding,
        next_holding,
        execution_scale,
        next_session_actionable_scale,
    )
    base_trade_cost_scaled = (base_trade_cost * base_trade_cost_scale).clip(lower=0.0, upper=0.99)
    ret = (
        (1.0 + base_pre_cost_return * execution_scale + idle_cash_yield)
        * (1.0 - base_trade_cost_scaled)
        * (1.0 - scale_change_cost)
        * (1.0 - financing_cost)
        - 1.0
    )
    out["target_vol"] = float(target_vol)
    out["target_vol_window"] = int(v2_0.overlay_mod.TARGET_VOL_WINDOW)
    out["target_vol_return"] = target_vol_return.fillna(0.0)
    out["target_vol_return_source"] = "constructed_microcap_minus_param_hedge"
    out["target_vol_realized_vol"] = realized_vol
    out["target_vol_scale_raw"] = scale_raw
    out["target_vol_execution_scale_raw"] = target_execution_scale
    out["current_execution_scale"] = execution_scale
    out["execution_scale"] = execution_scale
    out["next_session_target_scale"] = next_session_target_scale
    out["next_session_actionable_scale"] = next_session_actionable_scale
    out["target_vol_turnover"] = target_vol_turnover
    out["target_vol_costed_turnover"] = target_vol_costed_turnover
    out["scale_change_cost"] = scale_change_cost
    out["target_vol_trade_cost"] = scale_change_cost
    out["financing_cost"] = financing_cost
    out["idle_cash_yield"] = idle_cash_yield
    out["base_trade_cost"] = base_trade_cost
    out["base_trade_cost_scale"] = base_trade_cost_scale
    out["base_trade_cost_scaled"] = base_trade_cost_scaled
    out["base_pre_cost_return"] = base_pre_cost_return
    out["return_net"] = ret
    out["nav_net"] = (1.0 + ret.fillna(0.0)).cumprod()
    out["return"] = out["return_net"]
    out["nav"] = out["nav_net"]
    return out


def build_costed(gross: pd.DataFrame, turnover_df: pd.DataFrame) -> pd.DataFrame:
    costed = v2_0.base_mod.apply_momentum_gap_no_peak_decay_cost_model(gross, turnover_df)
    costed["overlay_pre_cost_return"] = pd.to_numeric(costed["return"], errors="coerce").fillna(0.0)
    return costed


def metrics_for_segment(out: pd.DataFrame, segment: str, offset: pd.DateOffset | None) -> dict[str, object]:
    frame = out.copy()
    if offset is not None:
        cutoff = pd.Timestamp(frame.index.max()) - offset
        frame = frame.loc[frame.index >= cutoff]
    ret = pd.to_numeric(frame["return_net"], errors="coerce").dropna().astype(float)
    if ret.empty:
        raise ValueError(f"empty return series for {segment}")
    nav = (1.0 + ret).cumprod()
    years = (pd.Timestamp(ret.index[-1]) - pd.Timestamp(ret.index[0])).days / 365.25
    ann = nav.iloc[-1] ** (1.0 / years) - 1.0 if years > 0 else 0.0
    vol = ret.std(ddof=1) * math.sqrt(TRADING_DAYS)
    dd = nav.div(nav.cummax()).sub(1.0).min()
    holding = frame.loc[ret.index, "holding"].astype(str)
    active = holding.ne("cash")
    cost_cols = [col for col in ["total_cost", "scale_change_cost", "financing_cost", "base_trade_cost_scaled"] if col in frame.columns]
    return {
        "segment": segment,
        "start": str(pd.Timestamp(ret.index[0]).date()),
        "end": str(pd.Timestamp(ret.index[-1]).date()),
        "rows": int(len(ret)),
        "ann_return": float(ann),
        "ann_vol": float(vol),
        "sharpe_repo": float(ann / vol) if vol > 0 else 0.0,
        "max_dd": float(dd),
        "final_nav": float(nav.iloc[-1]),
        "holding_days": int(active.sum()),
        "holding_day_ratio": float(active.mean()),
        "avg_execution_scale": float(pd.to_numeric(frame.loc[ret.index, "execution_scale"], errors="coerce").fillna(0.0).mean())
        if "execution_scale" in frame.columns
        else 0.0,
        "avg_target_vol_turnover": float(pd.to_numeric(frame.loc[ret.index, "target_vol_costed_turnover"], errors="coerce").fillna(0.0).mean())
        if "target_vol_costed_turnover" in frame.columns
        else 0.0,
        "cost_total": float(sum(pd.to_numeric(frame.loc[ret.index, col], errors="coerce").fillna(0.0).sum() for col in cost_cols)),
        "cost_days": int(pd.to_numeric(frame.loc[ret.index, cost_cols].sum(axis=1), errors="coerce").fillna(0.0).gt(0).sum()) if cost_cols else 0,
    }


def append_metrics(rows: list[dict[str, object]], candidate: str, params: dict[str, object], out: pd.DataFrame) -> None:
    for segment, offset in WINDOWS.items():
        row = {"candidate": candidate, **params}
        row.update(metrics_for_segment(out, segment, offset))
        rows.append(row)


def wide_window_metrics(summary: pd.DataFrame) -> pd.DataFrame:
    value_cols = [
        "ann_return",
        "ann_vol",
        "sharpe_repo",
        "max_dd",
        "holding_day_ratio",
        "avg_execution_scale",
        "avg_target_vol_turnover",
        "cost_total",
        "cost_days",
    ]
    index_cols = [
        "candidate",
        "version",
        "lookback",
        "halflife",
        "exit_buffer",
        "target_vol",
        "scale_threshold",
        "execution_hedge_ratio",
        "signal_spread_hedge_ratio",
    ]
    pivot_source = summary.copy()
    pivot_source["halflife"] = pd.to_numeric(pivot_source["halflife"], errors="coerce").fillna(-1.0)
    pivot_source["signal_spread_hedge_ratio"] = pd.to_numeric(
        pivot_source["signal_spread_hedge_ratio"],
        errors="coerce",
    ).fillna(-1.0)
    pivot = pivot_source.groupby([*index_cols, "segment"], dropna=False)[value_cols].first().unstack("segment")
    pivot.columns = [f"{metric}_{segment}" for metric, segment in pivot.columns]
    out = pivot.reset_index()
    out.loc[out["halflife"].eq(-1.0), "halflife"] = np.nan
    out.loc[out["signal_spread_hedge_ratio"].eq(-1.0), "signal_spread_hedge_ratio"] = np.nan
    return out


def param_sensitivity(window_metrics: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    params = ["lookback", "halflife", "exit_buffer", "target_vol", "scale_threshold", "execution_hedge_ratio"]
    metrics = ["ann_return_full", "sharpe_repo_full", "max_dd_full", "ann_return_last_8y", "ann_return_last_5y", "ann_return_last_3y"]
    for version, version_df in window_metrics.groupby("version"):
        for param in params:
            if version_df[param].dropna().nunique() <= 1:
                continue
            for value, part in version_df.groupby(param, dropna=True):
                row = {"version": version, "parameter": param, "value": value, "candidate_count": int(len(part))}
                for metric in metrics:
                    series = pd.to_numeric(part[metric], errors="coerce").dropna()
                    row[f"{metric}_mean"] = float(series.mean()) if not series.empty else np.nan
                    row[f"{metric}_std"] = float(series.std(ddof=1)) if len(series) > 1 else 0.0
                    row[f"{metric}_min"] = float(series.min()) if not series.empty else np.nan
                    row[f"{metric}_max"] = float(series.max()) if not series.empty else np.nan
                rows.append(row)
    return pd.DataFrame(rows)


def neighborhood_metrics(window_metrics: pd.DataFrame) -> pd.DataFrame:
    default_rows = {
        "v2_3": {
            "lookback": 17,
            "halflife": 4.0,
            "exit_buffer": 0.13,
            "target_vol": 0.25,
            "scale_threshold": 0.30,
            "execution_hedge_ratio": 0.8,
        },
        "v2_0": {
            "lookback": 16,
            "halflife": np.nan,
            "exit_buffer": 0.003,
            "target_vol": 0.25,
            "scale_threshold": 0.10,
            "execution_hedge_ratio": 0.8,
        },
    }
    rows: list[pd.DataFrame] = []
    for version, defaults in default_rows.items():
        part = window_metrics.loc[window_metrics["version"] == version].copy()
        if part.empty:
            continue
        part["distance"] = 0.0
        for key, default in defaults.items():
            if key not in part.columns or pd.isna(default):
                continue
            scale = max(float(pd.to_numeric(part[key], errors="coerce").dropna().std(ddof=0)), 1e-9)
            part["distance"] += ((pd.to_numeric(part[key], errors="coerce") - float(default)).abs() / scale).fillna(0.0)
        rows.append(part.sort_values(["distance", "sharpe_repo_full"], ascending=[True, False]).head(50))
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def summarize_stability(window_metrics: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for version, part in window_metrics.groupby("version"):
        row = {"version": version, "candidate_count": int(len(part))}
        for metric in ["ann_return_full", "sharpe_repo_full", "max_dd_full", "ann_return_last_8y", "ann_return_last_5y", "ann_return_last_3y"]:
            series = pd.to_numeric(part[metric], errors="coerce").dropna()
            row[f"{metric}_median"] = float(series.median())
            row[f"{metric}_p25"] = float(series.quantile(0.25))
            row[f"{metric}_p75"] = float(series.quantile(0.75))
            row[f"{metric}_min"] = float(series.min())
            row[f"{metric}_max"] = float(series.max())
        row["positive_3y_share"] = float((pd.to_numeric(part["ann_return_last_3y"], errors="coerce") > 0).mean())
        row["positive_5y_share"] = float((pd.to_numeric(part["ann_return_last_5y"], errors="coerce") > 0).mean())
        row["max_dd_worse_than_30_share"] = float((pd.to_numeric(part["max_dd_full"], errors="coerce") < -0.30).mean())
        rows.append(row)
    return pd.DataFrame(rows)


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def main() -> None:
    started = time.time()
    command_log = RUN_DIR / "command_log.txt"
    with command_log.open("a", encoding="utf-8") as f:
        f.write("\n# robustness scan\n")
        f.write("python run_v20_v23_robustness_scan.py\n")

    print("refreshing official v2.0/v2.3 outputs before scan...", flush=True)
    v2_3.generate_v2_3_outputs()
    reference_summary, base_gross_cached, turnover_df = v2_0.embedded_context._load_embedded_base_context()
    official_summary, _official_signal, official_v20_out = v2_0.generate_v2_0_outputs()
    close_df = base_gross_cached[["microcap_close", "hedge_close"]].rename(columns={"microcap_close": "microcap", "hedge_close": "hedge"}).sort_index()
    official_index = pd.DatetimeIndex(official_v20_out.index)
    scan_index = pd.DatetimeIndex(official_index.intersection(close_df.index))
    scan_index = scan_index[scan_index >= FORMAL_START_DATE].sort_values()
    if scan_index.empty:
        raise RuntimeError("empty scan index")

    rows: list[dict[str, object]] = []
    candidate_count = 0
    v23_total = len(LOOKBACKS) * len(HALFLIFES) * len(EXIT_BUFFERS) * len(TARGET_VOLS) * len(SCALE_THRESHOLDS) * len(EXEC_HEDGE_RATIOS)
    v20_total = len(LOOKBACKS) * len(EXIT_BUFFERS) * len(TARGET_VOLS) * len(SCALE_THRESHOLDS) * len(EXEC_HEDGE_RATIOS)

    v23_gross_cache: dict[tuple[int, float, float], pd.DataFrame] = {}
    v23_costed_cache: dict[tuple[int, float, float, float], pd.DataFrame] = {}
    for lookback, halflife, hedge_ratio in itertools.product(LOOKBACKS, HALFLIFES, EXEC_HEDGE_RATIOS):
        v23_gross_cache[(lookback, halflife, hedge_ratio)] = build_v23_gross(
            close_df,
            scan_index,
            lookback,
            halflife,
            SIGNAL_SPREAD_HEDGE_RATIO,
            hedge_ratio,
        )
    print(f"cached v2.3 gross layers: {len(v23_gross_cache)}", flush=True)
    for lookback, halflife, hedge_ratio, exit_buffer in itertools.product(LOOKBACKS, HALFLIFES, EXEC_HEDGE_RATIOS, EXIT_BUFFERS):
        gross = v23_gross_cache[(lookback, halflife, hedge_ratio)]
        buffered = apply_gap_buffer(gross, exit_buffer, hedge_ratio)
        v23_costed_cache[(lookback, halflife, hedge_ratio, exit_buffer)] = build_costed(buffered, turnover_df)
    print(f"cached v2.3 costed layers: {len(v23_costed_cache)}", flush=True)
    for lookback, halflife, hedge_ratio, exit_buffer, target_vol, scale_threshold in itertools.product(
        LOOKBACKS,
        HALFLIFES,
        EXEC_HEDGE_RATIOS,
        EXIT_BUFFERS,
        TARGET_VOLS,
        SCALE_THRESHOLDS,
    ):
        candidate = candidate_label(
            version="v2_3",
            lookback=lookback,
            halflife=halflife,
            exit_buffer=exit_buffer,
            target_vol=target_vol,
            scale_threshold=scale_threshold,
            execution_hedge_ratio=hedge_ratio,
        )
        costed = v23_costed_cache[(lookback, halflife, hedge_ratio, exit_buffer)]
        out = apply_target_vol_param(costed, target_vol, scale_threshold, hedge_ratio)
        params = {
            "version": "v2_3",
            "lookback": lookback,
            "halflife": halflife,
            "exit_buffer": exit_buffer,
            "target_vol": target_vol,
            "scale_threshold": scale_threshold,
            "execution_hedge_ratio": hedge_ratio,
            "signal_spread_hedge_ratio": SIGNAL_SPREAD_HEDGE_RATIO,
        }
        append_metrics(rows, candidate, params, out)
        candidate_count += 1
        if candidate_count % 500 == 0:
            print(f"v2.3 progress {candidate_count}/{v23_total}", flush=True)

    v20_done = 0
    v20_gross_cache: dict[tuple[int, float], pd.DataFrame] = {}
    v20_costed_cache: dict[tuple[int, float, float], pd.DataFrame] = {}
    for lookback, hedge_ratio in itertools.product(LOOKBACKS, EXEC_HEDGE_RATIOS):
        v20_gross_cache[(lookback, hedge_ratio)] = build_v20_gross(close_df, scan_index, lookback, hedge_ratio)
    print(f"cached v2.0 gross layers: {len(v20_gross_cache)}", flush=True)
    for lookback, hedge_ratio, exit_buffer in itertools.product(LOOKBACKS, EXEC_HEDGE_RATIOS, EXIT_BUFFERS):
        gross = v20_gross_cache[(lookback, hedge_ratio)]
        buffered = apply_gap_buffer(gross, exit_buffer, hedge_ratio)
        v20_costed_cache[(lookback, hedge_ratio, exit_buffer)] = build_costed(buffered, turnover_df)
    print(f"cached v2.0 costed layers: {len(v20_costed_cache)}", flush=True)
    for lookback, hedge_ratio, exit_buffer, target_vol, scale_threshold in itertools.product(
        LOOKBACKS,
        EXEC_HEDGE_RATIOS,
        EXIT_BUFFERS,
        TARGET_VOLS,
        SCALE_THRESHOLDS,
    ):
        candidate = candidate_label(
            version="v2_0",
            lookback=lookback,
            halflife=None,
            exit_buffer=exit_buffer,
            target_vol=target_vol,
            scale_threshold=scale_threshold,
            execution_hedge_ratio=hedge_ratio,
        )
        costed = v20_costed_cache[(lookback, hedge_ratio, exit_buffer)]
        out = apply_target_vol_param(costed, target_vol, scale_threshold, hedge_ratio)
        params = {
            "version": "v2_0",
            "lookback": lookback,
            "halflife": np.nan,
            "exit_buffer": exit_buffer,
            "target_vol": target_vol,
            "scale_threshold": scale_threshold,
            "execution_hedge_ratio": hedge_ratio,
            "signal_spread_hedge_ratio": np.nan,
        }
        append_metrics(rows, candidate, params, out)
        v20_done += 1
        if v20_done % 300 == 0:
            print(f"v2.0 progress {v20_done}/{v20_total}", flush=True)

    summary = pd.DataFrame(rows)
    window = wide_window_metrics(summary)
    sensitivity = param_sensitivity(window)
    neighborhood = neighborhood_metrics(window)
    stability = summarize_stability(window)

    summary.to_csv(RUN_DIR / "scan_summary.csv", index=False, encoding="utf-8")
    window.to_csv(RUN_DIR / "window_metrics.csv", index=False, encoding="utf-8")
    sensitivity.to_csv(RUN_DIR / "parameter_sensitivity.csv", index=False, encoding="utf-8")
    neighborhood.to_csv(RUN_DIR / "default_neighborhood_top50.csv", index=False, encoding="utf-8")
    stability.to_csv(RUN_DIR / "stability_overview.csv", index=False, encoding="utf-8")

    meta_path = RUN_DIR / "scan_meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    meta.update(
        {
            "phase": "scan_complete",
            "scan_type": "full_factorial_local_robustness_grid",
            "baseline": {
                "v2_3": {
                    "lookback": v2_3.LOOKBACK,
                    "halflife": v2_3.HALFLIFE,
                    "signal_spread_hedge_ratio": v2_3.SIGNAL_SPREAD_HEDGE_RATIO,
                    "execution_hedge_ratio": v2_3.EXECUTION_HEDGE_RATIO,
                    "exit_buffer": v2_3.MOMENTUM_GAP_EXIT_BUFFER,
                    "target_vol": v2_3.TARGET_VOL,
                    "scale_threshold": v2_3.TARGET_VOL_SCALE_REBALANCE_THRESHOLD,
                },
                "v2_0": {
                    "lookback": int(v2_0.embedded_context.base_mod.LOOKBACK),
                    "execution_hedge_ratio": float(v2_0.BASE_HEDGE_RATIO),
                    "exit_buffer": float(v2_0.V2_0_MOMENTUM_GAP_EXIT_BUFFER),
                    "target_vol": float(v2_0.overlay_mod.TARGET_VOL),
                    "scale_threshold": float(v2_0.overlay_mod.TARGET_VOL_SCALE_REBALANCE_THRESHOLD),
                },
            },
            "candidate_grid": {
                "lookback": LOOKBACKS,
                "halflife_v2_3_only": HALFLIFES,
                "exit_buffer": EXIT_BUFFERS,
                "target_vol": TARGET_VOLS,
                "scale_threshold": SCALE_THRESHOLDS,
                "execution_hedge_ratio": EXEC_HEDGE_RATIOS,
                "signal_spread_hedge_ratio_v2_3_fixed": SIGNAL_SPREAD_HEDGE_RATIO,
                "v2_3_candidates": v23_total,
                "v2_0_candidates": v20_total,
            },
            "data_snapshot": {
                "scan_start": str(pd.Timestamp(scan_index[0]).date()),
                "scan_end": str(pd.Timestamp(scan_index[-1]).date()),
                "scan_rows": int(len(scan_index)),
                "close_df_start": str(pd.Timestamp(close_df.index.min()).date()),
                "close_df_end": str(pd.Timestamp(close_df.index.max()).date()),
                "close_df_rows": int(len(close_df)),
                "turnover_rows": int(len(turnover_df)),
                "turnover_start": str(pd.to_datetime(turnover_df["rebalance_date"]).min().date()) if "rebalance_date" in turnover_df.columns and len(turnover_df) else "",
                "turnover_end": str(pd.to_datetime(turnover_df["rebalance_date"]).max().date()) if "rebalance_date" in turnover_df.columns and len(turnover_df) else "",
                "official_v2_0_latest_nav_date": official_summary.get("latest_nav_date", ""),
                "reference_latest_trade_date": reference_summary.get("latest_trade_date", ""),
            },
            "cost_model": {
                "base_cost_model": "v2_0.base_mod.apply_momentum_gap_no_peak_decay_cost_model",
                "target_vol_scale_change_cost": float(v2_0.overlay_mod.TARGET_VOL_SCALE_CHANGE_COST),
                "target_vol_financing_rate": float(v2_0.overlay_mod.TARGET_VOL_FINANCING_RATE),
                "idle_cash_yield": float(v2_0.overlay_mod.IDLE_CASH_YIELD),
                "futures_drag_daily": float(v2_0.base_mod.FUTURES_DRAG),
                "execution_timing": "close-confirmed signal; holding applies from next row, same as official gap-buffer semantics",
                "return_column": "return_net",
            },
            "outputs": {
                "record": str(RUN_DIR / "record.md"),
                "scan_summary": str(RUN_DIR / "scan_summary.csv"),
                "window_metrics": str(RUN_DIR / "window_metrics.csv"),
                "scan_meta": str(RUN_DIR / "scan_meta.json"),
                "command_log": str(RUN_DIR / "command_log.txt"),
                "parameter_sensitivity": str(RUN_DIR / "parameter_sensitivity.csv"),
                "default_neighborhood_top50": str(RUN_DIR / "default_neighborhood_top50.csv"),
                "stability_overview": str(RUN_DIR / "stability_overview.csv"),
            },
            "git_status_after": git_value(["git", "status", "--short"]),
            "elapsed_seconds": round(time.time() - started, 3),
        }
    )
    write_json(meta_path, meta)

    record = RUN_DIR / "record.md"
    with record.open("a", encoding="utf-8") as f:
        f.write("\n## User-Facing Summary\n\n")
        f.write(f"- Completed full-factorial robustness scan: v2.3 {v23_total} candidates, v2.0 {v20_total} candidates.\n")
        f.write(f"- Metrics use `return_net` on common close-confirmed index {scan_index[0].date()} to {scan_index[-1].date()}.\n")
        f.write("- Additional outputs: `parameter_sensitivity.csv`, `default_neighborhood_top50.csv`, `stability_overview.csv`.\n")
        f.write("- Decision: see final assistant summary; this run is research evidence, not a source-code promotion.\n")

    print(f"wrote {RUN_DIR / 'scan_summary.csv'}", flush=True)
    print(f"wrote {RUN_DIR / 'window_metrics.csv'}", flush=True)
    print(f"elapsed_seconds={time.time() - started:.1f}", flush=True)


if __name__ == "__main__":
    main()
