from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import microcap_top100_mom16_biweekly_live_v2_3 as v23  # noqa: E402


TRADING_DAYS = int(v23.TRADING_DAYS)
RUN_FOLDER = ROOT / "quant_param_scan_runs" / "20260523_microcap_v2_3_microcap_only_entry40_exit40_target_vol"
ENTRY_THRESHOLD = 0.40
EXIT_THRESHOLD = 0.40
TARGET_VOL_VALUES: tuple[float | None, ...] = (None, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50)
VOL_WINDOW = int(v23.v2_0.overlay_mod.TARGET_VOL_WINDOW)
MAX_LEVERAGE = float(v23.v2_0.overlay_mod.TARGET_VOL_MAX_LEVERAGE)
MIN_LEVERAGE = float(v23.v2_0.overlay_mod.TARGET_VOL_MIN_LEVERAGE)
SCALE_REBALANCE_THRESHOLD = float(v23.TARGET_VOL_SCALE_REBALANCE_THRESHOLD)
SCALE_CHANGE_COST = float(v23.v2_0.overlay_mod.TARGET_VOL_SCALE_CHANGE_COST)
FINANCING_RATE = float(v23.v2_0.overlay_mod.TARGET_VOL_FINANCING_RATE)
IDLE_CASH_YIELD = float(v23.v2_0.overlay_mod.IDLE_CASH_YIELD)


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
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
    except Exception as exc:  # pragma: no cover
        return f"unavailable: {exc}"


def _target_label(target_vol: float | None) -> str:
    if target_vol is None:
        return "no_target_vol"
    return f"tv{int(round(float(target_vol) * 100)):02d}"


def _metrics(ret: pd.Series) -> dict[str, float | int]:
    r = pd.to_numeric(ret, errors="coerce").fillna(0.0).astype(float)
    rows = int(len(r))
    if rows <= 0:
        return {
            "rows": 0,
            "ann_return": np.nan,
            "ann_vol": np.nan,
            "sharpe_repo": np.nan,
            "max_dd": np.nan,
            "final_nav": np.nan,
        }
    nav = (1.0 + r).cumprod()
    final_nav = float(nav.iloc[-1])
    ann_return = final_nav ** (TRADING_DAYS / rows) - 1.0 if final_nav > 0 else np.nan
    ann_vol = float(r.std(ddof=1) * math.sqrt(TRADING_DAYS)) if rows > 1 else 0.0
    sharpe = ann_return / ann_vol if ann_vol and math.isfinite(ann_vol) else np.nan
    dd = nav / nav.cummax() - 1.0
    return {
        "rows": rows,
        "ann_return": float(ann_return),
        "ann_vol": float(ann_vol),
        "sharpe_repo": float(sharpe),
        "max_dd": float(dd.min()),
        "final_nav": final_nav,
    }


def _window_slices(index: pd.DatetimeIndex) -> dict[str, tuple[pd.Timestamp, pd.Timestamp]]:
    end = pd.Timestamp(index.max())
    first = pd.Timestamp(index.min())
    windows = {"full": (first, end)}
    for years in (10, 5, 3, 1):
        windows[f"last_{years}y"] = (max(first, end - pd.DateOffset(years=years)), end)
    return windows


def _base_signal_frame(close_df: pd.DataFrame, official_index: pd.DatetimeIndex) -> pd.DataFrame:
    close_df = close_df.sort_index()
    micro_ret = close_df["microcap"].pct_change(fill_method=None)
    hedge_ret = close_df["hedge"].pct_change(fill_method=None)
    micro_nav = (1.0 + micro_ret.fillna(0.0)).cumprod()
    score_frame = v23.log_wls_score_and_r2(micro_nav, lookback=v23.LOOKBACK, halflife=v23.HALFLIFE)
    valid = score_frame["annualized_log_wls_score"].notna()
    common_index = pd.DatetimeIndex(score_frame.index[valid])
    common_index = pd.DatetimeIndex(common_index.intersection(pd.DatetimeIndex(official_index)))
    common_index = common_index[common_index >= v23.FORMAL_START_DATE].sort_values()
    return pd.DataFrame(
        {
            "microcap_close": close_df["microcap"].loc[common_index],
            "hedge_close": close_df["hedge"].loc[common_index],
            "microcap_ret": micro_ret.loc[common_index],
            "hedge_ret": hedge_ret.loc[common_index],
            "microcap_nav": micro_nav.loc[common_index],
            "annualized_log_wls_score": pd.to_numeric(
                score_frame["annualized_log_wls_score"].loc[common_index],
                errors="coerce",
            ),
            "log_wls_r2": pd.to_numeric(score_frame["log_wls_r2"].loc[common_index], errors="coerce"),
        },
        index=common_index,
    )


def _build_entry40_exit40_gross(base: pd.DataFrame) -> pd.DataFrame:
    score = pd.to_numeric(base["annualized_log_wls_score"], errors="coerce")
    ret = pd.to_numeric(base["microcap_ret"], errors="coerce").fillna(0.0)
    current_active = False
    holdings: list[str] = []
    next_holdings: list[str] = []
    signal_on_values: list[bool] = []
    returns: list[float] = []
    for dt in base.index:
        active_before_signal = bool(current_active)
        holdings.append("long_microcap_top100" if active_before_signal else "cash")
        returns.append(float(ret.loc[dt]) if active_before_signal else 0.0)
        current_score = score.loc[dt]
        if pd.isna(current_score):
            next_active = False
        elif active_before_signal:
            next_active = float(current_score) > EXIT_THRESHOLD
        else:
            next_active = float(current_score) > ENTRY_THRESHOLD
        next_holdings.append("long_microcap_top100" if next_active else "cash")
        signal_on_values.append(bool(next_active))
        current_active = bool(next_active)

    gross_ret = pd.Series(returns, index=base.index, dtype=float)
    out = pd.DataFrame(
        {
            "return_raw": gross_ret,
            "return": gross_ret,
            "holding": holdings,
            "next_holding": next_holdings,
            "signal_on": signal_on_values,
            "microcap_close": base["microcap_close"],
            "hedge_close": base["hedge_close"],
            "microcap_ret": base["microcap_ret"],
            "hedge_ret": base["hedge_ret"],
            "microcap_mom": score,
            "hedge_mom": 0.0,
            "momentum_gap": score,
            "annualized_log_wls_score": score,
            "log_wls_r2": base["log_wls_r2"],
            "microcap_nav": base["microcap_nav"],
            "signal_score_label": "microcap_only_annualized_log_wls_score",
            "entry_threshold": ENTRY_THRESHOLD,
            "exit_threshold": EXIT_THRESHOLD,
            "futures_drag": 0.0,
            "active_spread_ret": gross_ret,
            "weight": 1.0,
            "target_vol_execution_scale": 1.0,
        },
        index=base.index,
    )
    out["nav_gross"] = (1.0 + out["return"].fillna(0.0)).cumprod()
    return out


def _apply_base_cost(gross: pd.DataFrame, turnover_df: pd.DataFrame) -> pd.DataFrame:
    out = v23.v2_0.base_mod.freq_mod.cost_mod.apply_cost_model(gross, turnover_df)
    out["nav_gross"] = gross["nav_gross"]
    out["strategy_variant"] = "v2_3_microcap_only_entry40_exit40_cost_only"
    out["lookback"] = int(v23.LOOKBACK)
    out["halflife"] = float(v23.HALFLIFE)
    out["hedge_removed"] = True
    out["target_vol_enabled"] = False
    out["cash_yield_enabled"] = False
    return out


def _apply_scale_rebalance_threshold(desired_scale: pd.Series, active: pd.Series) -> pd.Series:
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
        if last_scale <= 1e-12 or abs(target - last_scale) >= SCALE_REBALANCE_THRESHOLD:
            last_scale = target
        values.append(float(last_scale))
    return pd.Series(values, index=desired.index, dtype=float)


def _calc_next_session_actionable_scale(
    current_execution_scale: pd.Series,
    next_session_target_scale: pd.Series,
    next_holding: pd.Series,
) -> pd.Series:
    current = pd.to_numeric(current_execution_scale, errors="coerce").fillna(0.0)
    target = pd.to_numeric(next_session_target_scale, errors="coerce").fillna(current)
    next_holding = next_holding.fillna("cash").astype(str)
    actionable = current.copy()
    to_cash = next_holding.eq("cash")
    enter_from_cash = current.le(1e-12) & next_holding.ne("cash") & target.gt(1e-12)
    rebalance = target.sub(current).abs().ge(SCALE_REBALANCE_THRESHOLD)
    actionable.loc[to_cash] = 0.0
    actionable.loc[~to_cash & (enter_from_cash | rebalance)] = target.loc[~to_cash & (enter_from_cash | rebalance)]
    return actionable.astype(float)


def _microcap_turnover_series(holding: pd.Series, execution_scale: pd.Series) -> pd.Series:
    holding = holding.fillna("cash").astype(str)
    scale = pd.to_numeric(execution_scale, errors="coerce").fillna(0.0)
    leg = scale.where(holding.ne("cash"), 0.0)
    return leg.sub(leg.shift(1).fillna(0.0)).abs().astype(float)


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


def _apply_microcap_only_target_vol(base_costed: pd.DataFrame, target_vol: float | None) -> pd.DataFrame:
    out = base_costed.copy().sort_index()
    if target_vol is None:
        out["target_vol_enabled"] = False
        out["target_vol"] = np.nan
        out["target_vol_realized_vol"] = np.nan
        out["current_execution_scale"] = np.where(out["holding"].astype(str).ne("cash"), 1.0, 0.0)
        out["target_vol_turnover"] = 0.0
        out["target_vol_costed_turnover"] = 0.0
        out["scale_change_cost"] = 0.0
        out["financing_cost"] = 0.0
        out["idle_cash_yield"] = 0.0
        return out

    target_vol_value = float(target_vol)
    holding = out["holding"].fillna("cash").astype(str)
    next_holding = out["next_holding"].fillna(holding).astype(str)
    active = holding.ne("cash")
    base_return_net = pd.to_numeric(out["return_net"], errors="coerce").fillna(0.0)
    base_trade_cost = pd.to_numeric(out.get("total_cost", pd.Series(0.0, index=out.index)), errors="coerce").fillna(0.0)
    base_pre_cost_return = (1.0 + base_return_net).div(1.0 - base_trade_cost.clip(lower=0.0, upper=0.99)).sub(1.0)
    target_vol_return = pd.to_numeric(out["microcap_ret"], errors="coerce").replace([np.inf, -np.inf], np.nan)
    realized_vol = target_vol_return.rolling(VOL_WINDOW, min_periods=VOL_WINDOW).std(ddof=1) * math.sqrt(TRADING_DAYS)
    raw_scale = (target_vol_value / realized_vol.replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan).clip(
        lower=MIN_LEVERAGE,
        upper=MAX_LEVERAGE,
    )
    target_execution_scale = raw_scale.shift(1).fillna(1.0)
    execution_scale = _apply_scale_rebalance_threshold(target_execution_scale, active)
    next_session_target_scale = raw_scale.copy()
    next_session_target_scale.loc[next_holding.eq("cash")] = 0.0
    next_session_target_scale.loc[next_holding.ne("cash")] = next_session_target_scale.loc[next_holding.ne("cash")].fillna(1.0)
    next_session_target_scale = next_session_target_scale.fillna(0.0)
    next_session_actionable_scale = _calc_next_session_actionable_scale(
        execution_scale,
        next_session_target_scale,
        next_holding,
    )
    target_vol_turnover = _microcap_turnover_series(holding, execution_scale)
    same_holding = holding.eq(holding.shift(1))
    target_vol_costed_turnover = target_vol_turnover.where(same_holding, 0.0)
    scale_change_cost = target_vol_costed_turnover * SCALE_CHANGE_COST
    financing_cost = execution_scale.sub(1.0).clip(lower=0.0) * FINANCING_RATE / TRADING_DAYS
    idle_cash_yield = active.astype(float) * execution_scale.rsub(1.0).clip(lower=0.0, upper=1.0) * IDLE_CASH_YIELD / TRADING_DAYS
    base_cost_scale = _base_trade_cost_scale(holding, next_holding, execution_scale, next_session_actionable_scale)
    base_trade_cost_scaled = (base_trade_cost * base_cost_scale).clip(lower=0.0, upper=0.99)
    ret = (
        (1.0 + base_pre_cost_return * execution_scale + idle_cash_yield)
        * (1.0 - base_trade_cost_scaled)
        * (1.0 - scale_change_cost)
        * (1.0 - financing_cost)
        - 1.0
    )

    out["target_vol_enabled"] = True
    out["target_vol"] = target_vol_value
    out["target_vol_window"] = VOL_WINDOW
    out["target_vol_return"] = target_vol_return.fillna(0.0)
    out["target_vol_return_source"] = "microcap_pct_change_unhedged"
    out["target_vol_realized_vol"] = realized_vol
    out["target_vol_scale_raw"] = raw_scale
    out["target_vol_execution_scale_raw"] = target_execution_scale
    out["current_execution_scale"] = execution_scale
    out["execution_scale"] = execution_scale
    out["next_session_target_scale"] = next_session_target_scale
    out["next_session_actionable_scale"] = next_session_actionable_scale
    out["target_vol_scale_next_session"] = next_session_actionable_scale
    out["target_vol_turnover"] = target_vol_turnover
    out["target_vol_costed_turnover"] = target_vol_costed_turnover
    out["scale_change_cost"] = scale_change_cost
    out["target_vol_trade_cost"] = scale_change_cost
    out["financing_cost"] = financing_cost
    out["idle_cash_yield"] = idle_cash_yield
    out["base_trade_cost"] = base_trade_cost
    out["base_trade_cost_scale"] = base_cost_scale
    out["base_trade_cost_scaled"] = base_trade_cost_scaled
    out["base_pre_cost_return"] = base_pre_cost_return
    out["embedded_lineage_return_net"] = base_return_net
    out["embedded_lineage_nav_net"] = pd.to_numeric(out.get("nav_net", pd.Series(np.nan, index=out.index)), errors="coerce")
    out["return_net"] = ret
    out["nav_net"] = (1.0 + out["return_net"].fillna(0.0)).cumprod()
    out["return"] = out["return_net"]
    out["nav"] = out["nav_net"]
    out["strategy_variant"] = "v2_3_microcap_only_entry40_exit40_target_vol"
    return out


def _transition_counts(out: pd.DataFrame) -> dict[str, int]:
    holding = out["holding"].astype(str)
    prev = holding.shift(1).fillna("cash")
    return {
        "holding_days": int(holding.ne("cash").sum()),
        "cash_days": int(holding.eq("cash").sum()),
        "entry_days": int((holding.ne("cash") & prev.eq("cash")).sum()),
        "exit_days": int((holding.eq("cash") & prev.ne("cash")).sum()),
    }


def _decision_score(row: pd.Series) -> float:
    return (
        float(row["ann_return_last_10y"])
        + 0.75 * float(row["max_dd_last_10y"])
        + 0.25 * float(row["ann_return_last_3y"])
        + 0.25 * float(row["max_dd_last_3y"])
    )


def _scan(run_folder: Path) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    reference_summary, _signal_df, official_out = v23.generate_v2_3_outputs()
    _reference_summary2, base_gross_cached, turnover_df = v23.v2_0.embedded_context._load_embedded_base_context()
    close_df = v23._close_df_from_base(base_gross_cached)
    base = _base_signal_frame(close_df, pd.DatetimeIndex(official_out.index))
    gross = _build_entry40_exit40_gross(base)
    base_costed = _apply_base_cost(gross, turnover_df)

    summary_rows: list[dict[str, Any]] = []
    wide_rows: list[dict[str, Any]] = []
    verification_rows: list[dict[str, Any]] = []
    for target_vol in TARGET_VOL_VALUES:
        label = _target_label(target_vol)
        out = _apply_microcap_only_target_vol(base_costed, target_vol)
        out.rename_axis("date").reset_index().to_csv(run_folder / f"daily_{label}.csv", index=False, encoding="utf-8")
        full_counts = _transition_counts(out)
        costed_final_nav = float(out["nav_net"].iloc[-1])
        no_tv_final_nav = float(base_costed["nav_net"].iloc[-1])
        verification_rows.append(
            {
                "candidate": label,
                "target_vol": np.nan if target_vol is None else float(target_vol),
                "rows_match": bool(len(out) == len(base_costed)),
                "finite_return_net": bool(np.isfinite(pd.to_numeric(out["return_net"], errors="coerce").fillna(0.0)).all()),
                "final_nav": costed_final_nav,
                "no_target_vol_final_nav": no_tv_final_nav,
            }
        )
        wide: dict[str, Any] = {
            "candidate": label,
            "target_vol": np.nan if target_vol is None else float(target_vol),
            "entry_threshold": ENTRY_THRESHOLD,
            "exit_threshold": EXIT_THRESHOLD,
            "target_vol_enabled": target_vol is not None,
            "vol_window": VOL_WINDOW,
            "max_leverage": MAX_LEVERAGE,
            "scale_rebalance_threshold": SCALE_REBALANCE_THRESHOLD,
            "holding_days_full": full_counts["holding_days"],
            "cash_days_full": full_counts["cash_days"],
            "entry_days_full": full_counts["entry_days"],
            "exit_days_full": full_counts["exit_days"],
        }
        for segment, (start, end) in _window_slices(pd.DatetimeIndex(out.index)).items():
            part = out.loc[(out.index >= start) & (out.index <= end)]
            m = _metrics(part["return_net"])
            counts = _transition_counts(part)
            scale_mean = float(pd.to_numeric(part.get("current_execution_scale", np.nan), errors="coerce").mean()) if len(part) else np.nan
            scale_max = float(pd.to_numeric(part.get("current_execution_scale", np.nan), errors="coerce").max()) if len(part) else np.nan
            scale_lev_days = int(pd.to_numeric(part.get("current_execution_scale", 0.0), errors="coerce").fillna(0.0).gt(1.0).sum()) if len(part) else 0
            scale_cost_total = float(pd.to_numeric(part.get("scale_change_cost", 0.0), errors="coerce").fillna(0.0).sum()) if len(part) else 0.0
            financing_cost_total = float(pd.to_numeric(part.get("financing_cost", 0.0), errors="coerce").fillna(0.0).sum()) if len(part) else 0.0
            base_cost_total = float(pd.to_numeric(part.get("base_trade_cost_scaled", part.get("total_cost", 0.0)), errors="coerce").fillna(0.0).sum()) if len(part) else 0.0
            row = {
                "candidate": label,
                "segment": segment,
                "start": str(pd.Timestamp(start).date()),
                "end": str(pd.Timestamp(end).date()),
                "rows": int(m["rows"]),
                "ann_return": m["ann_return"],
                "ann_vol": m["ann_vol"],
                "sharpe_repo": m["sharpe_repo"],
                "max_dd": m["max_dd"],
                "final_nav": m["final_nav"],
                "holding_days": counts["holding_days"],
                "cash_days": counts["cash_days"],
                "holding_day_ratio": counts["holding_days"] / len(part) if len(part) else np.nan,
                "entry_days": counts["entry_days"],
                "exit_days": counts["exit_days"],
                "avg_execution_scale": scale_mean,
                "max_execution_scale": scale_max,
                "leverage_gt_1_days": scale_lev_days,
                "base_trade_cost_sum": base_cost_total,
                "scale_change_cost_sum": scale_cost_total,
                "financing_cost_sum": financing_cost_total,
                "target_vol": np.nan if target_vol is None else float(target_vol),
            }
            summary_rows.append(row)
            for metric in (
                "ann_return",
                "ann_vol",
                "sharpe_repo",
                "max_dd",
                "final_nav",
                "holding_day_ratio",
                "avg_execution_scale",
                "max_execution_scale",
                "leverage_gt_1_days",
                "base_trade_cost_sum",
                "scale_change_cost_sum",
                "financing_cost_sum",
            ):
                wide[f"{metric}_{segment}"] = row[metric]
        wide["decision_score"] = np.nan
        wide["decision_hint"] = "compare_only"
        wide["stability_label"] = "candidate"
        wide_rows.append(wide)

    summary = pd.DataFrame(summary_rows)
    wide = pd.DataFrame(wide_rows)
    wide["decision_score"] = wide.apply(_decision_score, axis=1)
    baseline_label = "no_target_vol"
    best_label = str(wide.sort_values("decision_score", ascending=False).iloc[0]["candidate"])
    wide.loc[wide["candidate"].eq(baseline_label), "decision_hint"] = "entry40_exit40_no_target_vol"
    wide.loc[wide["candidate"].eq(best_label), "decision_hint"] = "best_10y_balanced_score"
    wide.loc[wide["candidate"].eq(best_label), "stability_label"] = "watchlist_best"

    verification = pd.DataFrame(verification_rows)
    summary.to_csv(run_folder / "scan_summary.csv", index=False, encoding="utf-8")
    wide.to_csv(run_folder / "window_metrics.csv", index=False, encoding="utf-8")
    verification.to_csv(run_folder / "target_vol_sanity_checks.csv", index=False, encoding="utf-8")
    return summary, wide, {
        "reference_summary": reference_summary,
        "turnover_rows": int(len(turnover_df)),
        "metrics_start": str(pd.Timestamp(base.index.min()).date()),
        "metrics_end": str(pd.Timestamp(base.index.max()).date()),
        "rows": int(len(base)),
        "close_df_start": str(pd.Timestamp(close_df.index.min()).date()),
        "close_df_end": str(pd.Timestamp(close_df.index.max()).date()),
        "baseline_label": baseline_label,
        "best_label": best_label,
        "candidate_count": int(len(wide)),
        "all_rows_match": bool(verification["rows_match"].all()),
        "all_finite_return_net": bool(verification["finite_return_net"].all()),
    }


def _write_record(run_folder: Path, wide: pd.DataFrame, context: dict[str, Any], meta: dict[str, Any]) -> None:
    ordered = wide.sort_values("decision_score", ascending=False)
    best = ordered.iloc[0]
    baseline = wide.loc[wide["candidate"].eq(context["baseline_label"])].iloc[0]
    top10 = ordered.head(10)[
        [
            "candidate",
            "decision_score",
            "ann_return_full",
            "max_dd_full",
            "ann_return_last_10y",
            "max_dd_last_10y",
            "ann_return_last_5y",
            "max_dd_last_5y",
            "ann_return_last_3y",
            "max_dd_last_3y",
            "ann_return_last_1y",
            "max_dd_last_1y",
            "avg_execution_scale_full",
            "max_execution_scale_full",
        ]
    ]
    lines = [
        "# Quant Parameter Scan Record",
        "",
        "## Run Metadata",
        "",
        f"- Run id: `{run_folder.name}`",
        f"- Created at: {meta['created_at']}",
        "- Project: microcap Top100",
        "- Strategy or version: v2.3 derived",
        "- Sleeve or subsystem: microcap-only target-vol",
        "- Parameter group: `target_volatility`",
        "- Scan type: target_volatility_grid",
        f"- Repo or workspace path: `{ROOT}`",
        "- Target entrypoint: `scripts/run_microcap_v2_3_microcap_only_target_vol_scan.py`",
        f"- Git branch: `{meta['git_branch']}`",
        f"- Git commit: `{meta['git_commit']}`",
        "- Working tree status before: see `scan_meta.json`.",
        "",
        "## Research Question",
        "",
        "- Baseline signal: microcap-only annualized log-WLS, `entry=40%`, `exit=40%`.",
        f"- Candidate grid: target volatility `{list(TARGET_VOL_VALUES)}`.",
        "- Decision target: test target-vol layer after selecting the unified 40% signal threshold.",
        "- Source-change rule: `research_only_no_source_change`.",
        "- Required windows: full, last_10y, last_5y, last_3y, last_1y.",
        "",
        "## Implementation Anchor",
        "",
        "- Base source: `microcap_top100_mom16_biweekly_live_v2_3.py` for refreshed v2.3 index and local Top100 data.",
        "- Target-vol return source: unhedged microcap Top100 pct-change, not microcap-minus-hedge spread.",
        "- Target-vol turnover model: microcap single leg only; no ZZ1000 hedge leg turnover.",
        f"- Vol window: {VOL_WINDOW}; max leverage: {MAX_LEVERAGE:.1f}x; scale rebalance threshold: {SCALE_REBALANCE_THRESHOLD:.2f}.",
        "- Existing Top100 base transaction-cost model retained and scaled by execution exposure.",
        "- No production constants changed.",
        "",
        "## Data Snapshot",
        "",
        f"- Metrics start: {context['metrics_start']}",
        f"- Metrics end: {context['metrics_end']}",
        f"- Rows: {context['rows']}",
        f"- Close data start: {context['close_df_start']}",
        f"- Close data end: {context['close_df_end']}",
        f"- Turnover rows: {context['turnover_rows']}",
        f"- Reference v2.3 latest NAV date: {context['reference_summary'].get('latest_nav_date')}",
        "- Trading calendar: strategy local trading-date index; annualization uses 244 trading days.",
        "",
        "## Cost and Execution Assumptions",
        "",
        "- Retained: Top100 basket entry/exit/rebalance transaction costs, scaled by target-vol exposure.",
        "- Added: same-holding target-vol scale-change cost at 10bp of microcap single-leg turnover.",
        "- Added: 3% annual financing cost on exposure above 1.0x.",
        "- Added: 2% annual idle-cash credit only on active days with exposure below 1.0x; full cash days remain 0 return.",
        "- Removed: ZZ1000 hedge, futures drag, hedge-leg turnover, broad-volume overlays, R2 gate, and cash-day full idle yield.",
        "",
        "## Commands",
        "",
        "```powershell",
        f"python scripts/run_microcap_v2_3_microcap_only_target_vol_scan.py --run-folder {run_folder}",
        f"python C:\\Users\\Administrator.DESKTOP-95I7VVU\\.codex\\skills\\quant-param-scan\\scripts\\finalize_quant_param_scan_run.py {run_folder} --decision research_only_watchlist_target_vol --stability-label target_vol_first_pass",
        f"python C:\\Users\\Administrator.DESKTOP-95I7VVU\\.codex\\skills\\quant-param-scan\\scripts\\check_quant_param_scan_artifacts.py --phase complete --strict {run_folder}",
        "```",
        "",
        "## Output Files",
        "",
        "- `scan_summary.csv`: long-form window metrics.",
        "- `window_metrics.csv`: wide candidate table.",
        "- `target_vol_sanity_checks.csv`: finite-return and row-count checks.",
        "- `daily_*.csv`: candidate daily paths.",
        "",
        "## Full-Sample Results",
        "",
        f"- Baseline `{context['baseline_label']}`: annual return {baseline['ann_return_full']:.4%}, max drawdown {baseline['max_dd_full']:.4%}, Sharpe {baseline['sharpe_repo_full']:.3f}.",
        f"- Best balanced candidate `{best['candidate']}`: annual return {best['ann_return_full']:.4%}, max drawdown {best['max_dd_full']:.4%}, Sharpe {best['sharpe_repo_full']:.3f}.",
        "",
        "## Window Results",
        "",
        f"- Best 10Y: annual return {best['ann_return_last_10y']:.4%}, max drawdown {best['max_dd_last_10y']:.4%}.",
        f"- Best 5Y: annual return {best['ann_return_last_5y']:.4%}, max drawdown {best['max_dd_last_5y']:.4%}.",
        f"- Best 3Y: annual return {best['ann_return_last_3y']:.4%}, max drawdown {best['max_dd_last_3y']:.4%}.",
        f"- Best 1Y: annual return {best['ann_return_last_1y']:.4%}, max drawdown {best['max_dd_last_1y']:.4%}.",
        "",
        "## Top Candidates",
        "",
        top10.to_markdown(index=False, floatfmt=".6f"),
        "",
        "## Stability Classification",
        "",
        "- Label: target_vol_first_pass.",
        "- Evidence: compare `window_metrics.csv`; this is not a production promotion.",
        f"- Candidate count: {context['candidate_count']}.",
        f"- Sanity: all candidates row-match baseline: {context['all_rows_match']}; finite return_net: {context['all_finite_return_net']}.",
        "",
        "## Decision",
        "",
        "- Decision: research_only_watchlist_target_vol.",
        "- Recommended next action: inspect target-vol plateau and decide whether max leverage or scale threshold should be scanned next.",
    ]
    (run_folder / "record.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(run_folder: Path) -> None:
    started = time.time()
    run_folder.mkdir(parents=True, exist_ok=True)
    command_log = run_folder / "command_log.txt"
    with command_log.open("a", encoding="utf-8") as fh:
        fh.write(f"\n[{pd.Timestamp.now().isoformat()}] python scripts/run_microcap_v2_3_microcap_only_target_vol_scan.py --run-folder {run_folder}\n")

    _summary, wide, context = _scan(run_folder)
    meta = {
        "run_id": run_folder.name,
        "created_at": pd.Timestamp.now().isoformat(),
        "phase": "analysis_written",
        "project": "microcap Top100",
        "strategy": "v2.3 derived",
        "subsystem": "microcap-only target-vol",
        "repo_root": str(ROOT),
        "entrypoint": "scripts/run_microcap_v2_3_microcap_only_target_vol_scan.py",
        "git_branch": _git(["branch", "--show-current"]),
        "git_commit": _git(["rev-parse", "HEAD"]),
        "git_status_before": _git(["status", "--short"]),
        "git_status_after": _git(["status", "--short"]),
        "scan_type": "target_volatility_grid",
        "parameter_group": "target_volatility",
        "baseline": {
            "candidate": context["baseline_label"],
            "entry_threshold": ENTRY_THRESHOLD,
            "exit_threshold": EXIT_THRESHOLD,
            "target_vol": None,
            "lookback": int(v23.LOOKBACK),
            "halflife": float(v23.HALFLIFE),
        },
        "candidate_grid": wide["candidate"].tolist(),
        "data_snapshot": {
            "metrics_start": context["metrics_start"],
            "metrics_end": context["metrics_end"],
            "rows": context["rows"],
            "turnover_rows": context["turnover_rows"],
            "reference_summary_latest_nav_date": context["reference_summary"].get("latest_nav_date"),
            "close_df_start": context["close_df_start"],
            "close_df_end": context["close_df_end"],
        },
        "cost_model": {
            "retained": "top100_basket_transaction_cost_model_scaled_by_exposure",
            "target_vol_return_source": "microcap_pct_change_unhedged",
            "target_vol_turnover_model": "microcap_single_leg_only",
            "entry_buy_one_side": float(v23.v2_0.base_mod.freq_mod.cost_mod.ENTRY_COST),
            "exit_sell_one_side": float(v23.v2_0.base_mod.freq_mod.cost_mod.EXIT_COST),
            "scale_change_cost": SCALE_CHANGE_COST,
            "scale_rebalance_threshold": SCALE_REBALANCE_THRESHOLD,
            "financing_rate": FINANCING_RATE,
            "idle_cash_yield": IDLE_CASH_YIELD,
            "max_leverage": MAX_LEVERAGE,
            "vol_window": VOL_WINDOW,
            "hedge_removed": True,
            "cash_day_full_yield_enabled": False,
        },
        "verification": {
            "all_rows_match": context["all_rows_match"],
            "all_finite_return_net": context["all_finite_return_net"],
        },
        "outputs": {
            "record": str(run_folder / "record.md"),
            "scan_summary": str(run_folder / "scan_summary.csv"),
            "window_metrics": str(run_folder / "window_metrics.csv"),
            "scan_meta": str(run_folder / "scan_meta.json"),
            "command_log": str(command_log),
            "target_vol_sanity_checks": str(run_folder / "target_vol_sanity_checks.csv"),
        },
        "decision": "research_only_watchlist_target_vol",
        "stability_label": "target_vol_first_pass",
        "elapsed_sec": round(time.time() - started, 3),
    }
    _write_json(run_folder / "scan_meta.json", meta)
    _write_record(run_folder, wide, context, meta)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-folder", type=Path, default=RUN_FOLDER)
    args = parser.parse_args()
    run(args.run_folder)


if __name__ == "__main__":
    main()
