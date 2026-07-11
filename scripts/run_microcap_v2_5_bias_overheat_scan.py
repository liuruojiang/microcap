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

import microcap_top100_mom16_biweekly_live_v2_5 as v25  # noqa: E402
from scripts import microcap_v2_5_scan_common as scan_common  # noqa: E402


RUN_FOLDER = ROOT / "quant_param_scan_runs" / "20260523_microcap_v2_5_ma60_bias_overheat"
TRADING_DAYS = int(v25.TRADING_DAYS)
MA_WINDOWS = (60,)
HOT_THRESHOLDS = (0.15, 0.175, 0.20, 0.225, 0.25, 0.275, 0.30, 0.325, 0.35)
COOL_THRESHOLDS = (0.08, 0.10, 0.12, 0.15, 0.18, 0.20, 0.22, 0.25)
ONE_SIDE_TRADE_COST = float(v25.v2_0.base_mod.freq_mod.cost_mod.ENTRY_COST)


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


def _calc_ma_bias(close: pd.Series, ma_window: int = 60) -> pd.Series:
    close = pd.to_numeric(close, errors="coerce")
    ma = close.rolling(int(ma_window), min_periods=int(ma_window)).mean()
    return close.div(ma).sub(1.0)


def _parse_ma_windows(text: str) -> tuple[int, ...]:
    values: list[int] = []
    for part in str(text or "").split(","):
        raw = part.strip()
        if not raw:
            continue
        value = int(raw)
        if value <= 0:
            raise ValueError("MA windows must be positive integers")
        if value not in values:
            values.append(value)
    if not values:
        raise ValueError("at least one MA window is required")
    return tuple(values)


def _parse_float_values(text: str) -> tuple[float, ...]:
    values: list[float] = []
    for part in str(text or "").split(","):
        raw = part.strip()
        if not raw:
            continue
        value = float(raw)
        if value not in values:
            values.append(value)
    if not values:
        raise ValueError("at least one threshold value is required")
    return tuple(values)


def _parse_ma_threshold_grid(text: str) -> tuple[tuple[int, ...], dict[int, tuple[float, ...]], dict[int, tuple[float, ...]]]:
    ma_windows: list[int] = []
    hot_by_ma: dict[int, tuple[float, ...]] = {}
    cool_by_ma: dict[int, tuple[float, ...]] = {}
    for spec in str(text or "").split(";"):
        spec = spec.strip()
        if not spec:
            continue
        ma_part, thresholds = spec.split(":", 1)
        hot_part, cool_part = thresholds.split("|", 1)
        ma_window = int(ma_part.strip())
        if ma_window <= 0:
            raise ValueError("MA windows must be positive integers")
        ma_windows.append(ma_window)
        hot_by_ma[ma_window] = _parse_float_values(hot_part)
        cool_by_ma[ma_window] = _parse_float_values(cool_part)
    if not ma_windows:
        raise ValueError("at least one MA threshold grid entry is required")
    return tuple(dict.fromkeys(ma_windows)), hot_by_ma, cool_by_ma


def _label(ma_window: int | None, hot_threshold: float | None, cool_threshold: float | None) -> str:
    if hot_threshold is None:
        return "no_bias_overheat"
    return (
        f"ma{int(ma_window):02d}_hot{int(round(float(hot_threshold) * 1000)):03d}"
        f"_cool{int(round(float(cool_threshold) * 1000)):03d}"
    )


def _candidate_label(
    ma_window: int | None,
    hot_threshold: float | None,
    cool_threshold: float | None,
    *,
    trigger_mode: str,
    pullback_threshold: float,
) -> str:
    label = _label(ma_window, hot_threshold, cool_threshold)
    if hot_threshold is None or trigger_mode == "level":
        return label
    return f"{label}_pb{int(round(float(pullback_threshold) * 1000)):03d}"


def _candidate_grid(
    ma_windows: tuple[int, ...],
    *,
    hot_thresholds_by_ma: dict[int, tuple[float, ...]] | None = None,
    cool_thresholds_by_ma: dict[int, tuple[float, ...]] | None = None,
) -> list[tuple[int | None, float | None, float | None]]:
    pairs: list[tuple[int | None, float | None, float | None]] = [(None, None, None)]
    for ma_window in ma_windows:
        hot_values = (hot_thresholds_by_ma or {}).get(ma_window, HOT_THRESHOLDS)
        cool_values = (cool_thresholds_by_ma or {}).get(ma_window, COOL_THRESHOLDS)
        for hot in hot_values:
            for cool in cool_values:
                if 0.0 <= cool < hot:
                    pairs.append((int(ma_window), float(hot), float(cool)))
    return pairs


def _load_v2_5_shadow() -> tuple[dict[str, Any], pd.DataFrame]:
    return scan_common.load_fresh_official_v25()


def _apply_bias_overheat_overlay(
    shadow: pd.DataFrame,
    *,
    bias: pd.Series,
    hot_threshold: float | None,
    cool_threshold: float | None,
    one_side_trade_cost: float = ONE_SIDE_TRADE_COST,
    trigger_mode: str = "level",
    pullback_threshold: float = 0.0,
) -> pd.DataFrame:
    out = shadow.copy().sort_index()
    bias = pd.to_numeric(bias.reindex(out.index), errors="coerce")
    shadow_scale = pd.to_numeric(out["current_execution_scale"], errors="coerce").fillna(0.0)
    shadow_ret = pd.to_numeric(out["return_net"], errors="coerce").fillna(0.0)
    base_next_active = out["next_holding"].astype(str).ne("cash")

    if hot_threshold is None:
        out["bias_overheat_hot_threshold"] = np.nan
        out["bias_overheat_cool_threshold"] = np.nan
        out["bias_overheat_risk_off"] = False
        out["bias_overheat_exit_triggered"] = False
        out["bias_overheat_reentry_triggered"] = False
        out["actual_execution_scale"] = shadow_scale
        out["overlay_trade_cost"] = 0.0
        out["ma_bias"] = bias
        out["bias_overheat_trigger_mode"] = trigger_mode
        out["bias_overheat_pullback_threshold"] = np.nan
        return out

    hot_thr = float(hot_threshold)
    cool_thr = float(cool_threshold)
    mode = str(trigger_mode or "level")
    if mode not in {"level", "turn_down"}:
        raise ValueError("trigger_mode must be 'level' or 'turn_down'")
    pullback = float(pullback_threshold)
    if mode == "turn_down" and pullback <= 0.0:
        raise ValueError("pullback_threshold must be positive in turn_down mode")
    if cool_thr < 0.0 or cool_thr >= hot_thr:
        raise ValueError("cool_threshold must be non-negative and below hot_threshold")

    risk_off = False
    bias_peak = np.nan
    prev_actual_scale = 0.0
    prev_risk_off = False
    actual_returns: list[float] = []
    actual_scales: list[float] = []
    overlay_costs: list[float] = []
    exit_flags: list[bool] = []
    reentry_flags: list[bool] = []
    risk_off_flags: list[bool] = []

    for dt in out.index:
        current_risk_off = risk_off
        target_scale = 0.0 if current_risk_off else float(shadow_scale.loc[dt])
        overlay_turnover = abs(target_scale - prev_actual_scale) if current_risk_off != prev_risk_off else 0.0
        overlay_cost = overlay_turnover * float(one_side_trade_cost)
        day_ret = 0.0 if current_risk_off else float(shadow_ret.loc[dt])
        actual_ret = (1.0 + day_ret) * (1.0 - min(max(overlay_cost, 0.0), 0.99)) - 1.0

        current_bias = bias.loc[dt]
        next_active = bool(base_next_active.loc[dt])
        exit_trigger = False
        reentry_trigger = False
        if not next_active or pd.isna(current_bias):
            risk_off = False
            bias_peak = np.nan
        elif risk_off:
            if float(current_bias) <= cool_thr:
                risk_off = False
                reentry_trigger = True
                bias_peak = float(current_bias)
            else:
                bias_peak = max(float(bias_peak), float(current_bias)) if pd.notna(bias_peak) else float(current_bias)
        else:
            bias_peak = max(float(bias_peak), float(current_bias)) if pd.notna(bias_peak) else float(current_bias)
            if mode == "level":
                if float(current_bias) >= hot_thr:
                    risk_off = True
                    exit_trigger = True
            elif bias_peak >= hot_thr and bias_peak - float(current_bias) >= pullback:
                risk_off = True
                exit_trigger = True

        actual_returns.append(actual_ret)
        actual_scales.append(target_scale)
        overlay_costs.append(overlay_cost)
        exit_flags.append(exit_trigger)
        reentry_flags.append(reentry_trigger)
        risk_off_flags.append(current_risk_off)
        prev_actual_scale = target_scale
        prev_risk_off = current_risk_off

    ret = pd.Series(actual_returns, index=out.index, dtype=float)
    out["bias_overheat_hot_threshold"] = hot_thr
    out["bias_overheat_cool_threshold"] = cool_thr
    out["bias_overheat_risk_off"] = pd.Series(risk_off_flags, index=out.index, dtype=bool)
    out["bias_overheat_exit_triggered"] = pd.Series(exit_flags, index=out.index, dtype=bool)
    out["bias_overheat_reentry_triggered"] = pd.Series(reentry_flags, index=out.index, dtype=bool)
    out["actual_execution_scale"] = pd.Series(actual_scales, index=out.index, dtype=float)
    out["overlay_trade_cost"] = pd.Series(overlay_costs, index=out.index, dtype=float)
    out["ma_bias"] = bias
    out["bias_overheat_trigger_mode"] = mode
    out["bias_overheat_pullback_threshold"] = pullback
    out["return_net"] = ret
    out["nav_net"] = (1.0 + ret.fillna(0.0)).cumprod()
    out["return"] = out["return_net"]
    out["nav"] = out["nav_net"]
    return out


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
    dd = nav.div(nav.cummax()).sub(1.0)
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
        + 0.35 * float(row["ann_return_last_5y"])
        + 0.25 * float(row["max_dd_last_5y"])
        + 0.25 * float(row["ann_return_last_3y"])
        + 0.25 * float(row["max_dd_last_3y"])
    )


def _scan(
    run_folder: Path,
    ma_windows: tuple[int, ...] = MA_WINDOWS,
    *,
    hot_thresholds_by_ma: dict[int, tuple[float, ...]] | None = None,
    cool_thresholds_by_ma: dict[int, tuple[float, ...]] | None = None,
    trigger_mode: str = "level",
    pullback_thresholds: tuple[float, ...] = (0.0,),
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    reference_summary, shadow = _load_v2_5_shadow()
    shadow = shadow.copy().sort_index()
    bias_by_ma = {
        ma_window: _calc_ma_bias(pd.to_numeric(shadow["microcap_close"], errors="coerce"), ma_window=ma_window)
        for ma_window in ma_windows
    }

    summary_rows: list[dict[str, Any]] = []
    wide_rows: list[dict[str, Any]] = []
    verification_rows: list[dict[str, Any]] = []

    grid = _candidate_grid(
        ma_windows,
        hot_thresholds_by_ma=hot_thresholds_by_ma,
        cool_thresholds_by_ma=cool_thresholds_by_ma,
    )
    mode = str(trigger_mode or "level")
    active_pullbacks = (0.0,) if mode == "level" else tuple(float(value) for value in pullback_thresholds)
    for ma_window, hot, cool in grid:
        for pullback in ((0.0,) if hot is None else active_pullbacks):
            label = _candidate_label(
                ma_window,
                hot,
                cool,
                trigger_mode=mode,
                pullback_threshold=float(pullback),
            )
            bias = pd.Series(np.nan, index=shadow.index, dtype=float) if ma_window is None else bias_by_ma[int(ma_window)]
            out = _apply_bias_overheat_overlay(
                shadow,
                bias=bias,
                hot_threshold=hot,
                cool_threshold=cool,
                one_side_trade_cost=ONE_SIDE_TRADE_COST,
                trigger_mode=mode,
                pullback_threshold=float(pullback),
            )
            out["bias_ma_window"] = np.nan if ma_window is None else int(ma_window)
            out["candidate"] = label
            out.rename_axis("date").reset_index().to_csv(run_folder / f"daily_{label}.csv", index=False, encoding="utf-8")
            counts = _transition_counts(out)
            verification_rows.append(
                {
                    "candidate": label,
                    "rows_match": bool(len(out) == len(shadow)),
                    "finite_return_net": bool(np.isfinite(pd.to_numeric(out["return_net"], errors="coerce").fillna(0.0)).all()),
                    "exit_count": int(out["bias_overheat_exit_triggered"].fillna(False).sum()),
                    "reentry_count": int(out["bias_overheat_reentry_triggered"].fillna(False).sum()),
                    "risk_off_days": int(out["bias_overheat_risk_off"].fillna(False).sum()),
                    "final_nav": float(out["nav_net"].iloc[-1]),
                }
            )
            wide: dict[str, Any] = {
                "candidate": label,
                "bias_ma_window": np.nan if ma_window is None else int(ma_window),
                "bias_hot_threshold": np.nan if hot is None else float(hot),
                "bias_cool_threshold": np.nan if cool is None else float(cool),
                "bias_overheat_trigger_mode": mode,
                "bias_overheat_pullback_threshold": np.nan if hot is None else float(pullback),
                "target_vol": v25.TARGET_VOL,
                "max_leverage": v25.TARGET_VOL_MAX_LEVERAGE,
                "scale_rebalance_threshold": v25.TARGET_VOL_SCALE_REBALANCE_THRESHOLD,
                "holding_days_full": counts["holding_days"],
                "cash_days_full": counts["cash_days"],
                "entry_days_full": counts["entry_days"],
                "exit_days_full": counts["exit_days"],
                "bias_overheat_exit_count_full": int(out["bias_overheat_exit_triggered"].fillna(False).sum()),
                "bias_overheat_reentry_count_full": int(out["bias_overheat_reentry_triggered"].fillna(False).sum()),
                "bias_overheat_risk_off_days_full": int(out["bias_overheat_risk_off"].fillna(False).sum()),
                "overlay_trade_cost_sum_full": float(pd.to_numeric(out["overlay_trade_cost"], errors="coerce").fillna(0.0).sum()),
            }
            for segment, (start, end) in _window_slices(pd.DatetimeIndex(out.index)).items():
                part = out.loc[(out.index >= start) & (out.index <= end)]
                m = _metrics(part["return_net"])
                part_counts = _transition_counts(part)
                actual_scale = pd.to_numeric(part.get("actual_execution_scale", part.get("current_execution_scale", 0.0)), errors="coerce")
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
                    "holding_days": part_counts["holding_days"],
                    "cash_days": part_counts["cash_days"],
                    "holding_day_ratio": part_counts["holding_days"] / len(part) if len(part) else np.nan,
                    "avg_execution_scale": float(actual_scale.mean()) if len(part) else np.nan,
                    "max_execution_scale": float(actual_scale.max()) if len(part) else np.nan,
                    "bias_overheat_exit_count": int(part["bias_overheat_exit_triggered"].fillna(False).sum()),
                    "bias_overheat_reentry_count": int(part["bias_overheat_reentry_triggered"].fillna(False).sum()),
                    "bias_overheat_risk_off_days": int(part["bias_overheat_risk_off"].fillna(False).sum()),
                    "overlay_trade_cost_sum": float(pd.to_numeric(part["overlay_trade_cost"], errors="coerce").fillna(0.0).sum()),
                    "bias_ma_window": np.nan if ma_window is None else int(ma_window),
                    "bias_hot_threshold": np.nan if hot is None else float(hot),
                    "bias_cool_threshold": np.nan if cool is None else float(cool),
                    "bias_overheat_trigger_mode": mode,
                    "bias_overheat_pullback_threshold": np.nan if hot is None else float(pullback),
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
                    "bias_overheat_exit_count",
                    "bias_overheat_reentry_count",
                    "bias_overheat_risk_off_days",
                    "overlay_trade_cost_sum",
                ):
                    wide[f"{metric}_{segment}"] = row[metric]
            wide["decision_score"] = np.nan
            wide["decision_hint"] = "compare_only"
            wide["stability_label"] = "candidate"
            wide_rows.append(wide)

    summary = pd.DataFrame(summary_rows)
    wide = pd.DataFrame(wide_rows)
    wide["decision_score"] = wide.apply(_decision_score, axis=1)
    best_label = str(wide.sort_values("decision_score", ascending=False).iloc[0]["candidate"])
    wide.loc[wide["candidate"].eq("no_bias_overheat"), "decision_hint"] = "v2_5_no_bias_overheat"
    wide.loc[wide["candidate"].eq(best_label), "decision_hint"] = "best_10y_5y_3y_balanced_score"
    wide.loc[wide["candidate"].eq(best_label), "stability_label"] = "watchlist_best"

    verification = pd.DataFrame(verification_rows)
    summary.to_csv(run_folder / "scan_summary.csv", index=False, encoding="utf-8")
    wide.to_csv(run_folder / "window_metrics.csv", index=False, encoding="utf-8")
    verification.to_csv(run_folder / "bias_overheat_sanity_checks.csv", index=False, encoding="utf-8")
    return summary, wide, {
        "reference_summary": reference_summary,
        "metrics_start": str(pd.Timestamp(shadow.index.min()).date()),
        "metrics_end": str(pd.Timestamp(shadow.index.max()).date()),
        "rows": int(len(shadow)),
        "baseline_label": "no_bias_overheat",
        "best_label": best_label,
        "candidate_count": int(len(wide)),
        "all_rows_match": bool(verification["rows_match"].all()),
        "all_finite_return_net": bool(verification["finite_return_net"].all()),
    }


def _write_record(run_folder: Path, wide: pd.DataFrame, context: dict[str, Any], meta: dict[str, Any]) -> None:
    ordered = wide.sort_values("decision_score", ascending=False)
    best = ordered.iloc[0]
    baseline = wide.loc[wide["candidate"].eq(context["baseline_label"])].iloc[0]
    top = ordered.head(15)[
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
            "bias_overheat_exit_count_full",
            "bias_overheat_reentry_count_full",
            "bias_overheat_risk_off_days_full",
            "overlay_trade_cost_sum_full",
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
        "- Strategy or version: v2.5 derived",
        "- Sleeve or subsystem: microcap-only MA bias overheat",
        "- Parameter group: `ma60_bias_hot_cool`",
        "- Scan type: ma_bias_overheat_overlay_grid",
        f"- Repo or workspace path: `{ROOT}`",
        "- Target entrypoint: `scripts/run_microcap_v2_5_bias_overheat_scan.py`",
        f"- Git branch: `{meta['git_branch']}`",
        f"- Git commit: `{meta['git_commit']}`",
        "- Working tree status before: see `scan_meta.json`.",
        "",
        "## Research Question",
        "",
        "- Baseline: formal v2.5 no-overheat costed path.",
        "- Candidate grid: exit when microcap close is stretched above its moving average; recover when bias cools below a lower threshold and the base signal remains active.",
        f"- MA windows: `{meta['ma_windows']}`.",
        f"- Hot thresholds by MA: `{meta.get('hot_thresholds_by_ma') or {'default': list(HOT_THRESHOLDS)}}`.",
        f"- Cooling thresholds by MA: `{meta.get('cool_thresholds_by_ma') or {'default': list(COOL_THRESHOLDS)}}` where cooling < hot.",
        f"- Trigger mode: `{meta.get('trigger_mode', 'level')}`.",
        f"- Pullback thresholds: `{meta.get('pullback_thresholds', [0.0])}`.",
        "- Source-change rule: `research_only_no_source_change`.",
        "- Required windows: full, last_10y, last_5y, last_3y, last_1y.",
        "",
        "## Implementation Anchor",
        "",
        "- Shadow path: official v2.5 costed strategy without overheat overlay.",
        "- Bias definition: `microcap_close / rolling_mean(microcap_close, MA) - 1`.",
        "- Level exit trigger: after close on T, if MA bias >= hot_threshold, set overlay risk-off for T+1.",
        "- Turn-down exit trigger: after close on T, wait until the current holding episode has reached hot_threshold and MA bias has pulled back from that episode peak by at least pullback_threshold, then set overlay risk-off for T+1.",
        "- Recovery trigger: after close on T, if MA bias <= cool_threshold and the base signal is active, resume following the shadow strategy from T+1.",
        "- Actual path: returns are set to cash while overlay risk-off; overlay exit/reentry charges one-side microcap turnover only when this overlay changes risk state.",
        "",
        "## Data Snapshot",
        "",
        f"- Metrics start: {context['metrics_start']}",
        f"- Metrics end: {context['metrics_end']}",
        f"- Rows: {context['rows']}",
        f"- Reference v2.5 latest NAV date: {context['reference_summary'].get('latest_nav_date')}",
        "- Trading calendar: strategy local trading-date index; annualization uses 244 trading days.",
        "",
        "## Cost and Execution Assumptions",
        "",
        "- Retained: v2.5 base trading cost, target-vol scale-change cost, financing, and close-to-close execution timing.",
        "- Added: overlay microcap one-side trading cost on bias-overheat risk-off/risk-on scale changes.",
        "- No hedge leg, futures drag, stop-loss, drawdown stop, momentum-decay layer, or main-score overheat layer.",
        "",
        "## Runtime Override Plan",
        "",
        "- No production source defaults are changed; this is a scratch research run.",
        "",
        "## Commands",
        "",
        "```powershell",
        f"python scripts/run_microcap_v2_5_bias_overheat_scan.py --run-folder {run_folder}",
        f"python C:\\Users\\Administrator.DESKTOP-95I7VVU\\.codex\\skills\\quant-param-scan\\scripts\\finalize_quant_param_scan_run.py {run_folder} --decision research_only_watchlist_bias_overheat --stability-label ma60_bias_first_pass",
        f"python C:\\Users\\Administrator.DESKTOP-95I7VVU\\.codex\\skills\\quant-param-scan\\scripts\\check_quant_param_scan_artifacts.py --phase complete --strict {run_folder}",
        "```",
        "",
        "## Output Files",
        "",
        "- `scan_summary.csv`: long-form metrics by candidate and window.",
        "- `window_metrics.csv`: wide candidate comparison table.",
        "- `bias_overheat_sanity_checks.csv`: row, finite-return, trigger, and final NAV checks.",
        "",
        "## Full-Sample Results",
        "",
        f"- Baseline `{context['baseline_label']}`: annual return {baseline['ann_return_full']:.4%}, max drawdown {baseline['max_dd_full']:.4%}, Sharpe {baseline['sharpe_repo_full']:.3f}.",
        f"- Best balanced candidate `{best['candidate']}`: annual return {best['ann_return_full']:.4%}, max drawdown {best['max_dd_full']:.4%}, Sharpe {best['sharpe_repo_full']:.3f}.",
        "",
        "## Window Results",
        "",
        top.to_markdown(index=False, floatfmt=".6f"),
        "",
        "## Stability Classification",
        "",
        "- Label: ma60_bias_first_pass.",
        "- Evidence: compare `window_metrics.csv`; this is not a production promotion.",
        f"- Candidate count: {context['candidate_count']}.",
        f"- Sanity: all candidates row-match baseline: {context['all_rows_match']}; finite return_net: {context['all_finite_return_net']}.",
        "",
        "## Decision",
        "",
        "- Decision: research_only_watchlist_bias_overheat.",
        "- Recommended next action: compare top MA60 bias candidates against no-overlay and main-score-overheat candidates before any formal v2.5 change.",
        "",
        "## User-Facing Summary",
        "",
        "- This run tests MA60 bias as an overheat filter on top of v2.5. It is research-only and does not change official v2.5 signals.",
    ]
    (run_folder / "record.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(
    run_folder: Path,
    ma_windows: tuple[int, ...] = MA_WINDOWS,
    *,
    hot_thresholds_by_ma: dict[int, tuple[float, ...]] | None = None,
    cool_thresholds_by_ma: dict[int, tuple[float, ...]] | None = None,
    trigger_mode: str = "level",
    pullback_thresholds: tuple[float, ...] = (0.0,),
) -> None:
    started = time.time()
    run_folder.mkdir(parents=True, exist_ok=True)
    command_log = run_folder / "command_log.txt"
    with command_log.open("a", encoding="utf-8") as fh:
        fh.write(f"\n[{pd.Timestamp.now().isoformat()}] python scripts/run_microcap_v2_5_bias_overheat_scan.py --run-folder {run_folder}\n")

    _summary, wide, context = _scan(
        run_folder,
        ma_windows=ma_windows,
        hot_thresholds_by_ma=hot_thresholds_by_ma,
        cool_thresholds_by_ma=cool_thresholds_by_ma,
        trigger_mode=trigger_mode,
        pullback_thresholds=pullback_thresholds,
    )
    meta = {
        "run_id": run_folder.name,
        "created_at": pd.Timestamp.now().isoformat(),
        "phase": "analysis_written",
        "project": "microcap Top100",
        "strategy": "v2.5 derived",
        "subsystem": "microcap-only MA bias overheat",
        "repo_root": str(ROOT),
        "entrypoint": "scripts/run_microcap_v2_5_bias_overheat_scan.py",
        "git_branch": _git(["branch", "--show-current"]),
        "git_commit": _git(["rev-parse", "HEAD"]),
        "git_status_before": _git(["status", "--short"]),
        "git_status_after": _git(["status", "--short"]),
        "scan_type": "ma_bias_overheat_overlay_grid",
        "parameter_group": "ma60_bias_hot_cool",
        "ma_windows": list(ma_windows),
        "hot_thresholds_by_ma": {str(k): list(v) for k, v in (hot_thresholds_by_ma or {}).items()},
        "cool_thresholds_by_ma": {str(k): list(v) for k, v in (cool_thresholds_by_ma or {}).items()},
        "trigger_mode": trigger_mode,
        "pullback_thresholds": list(pullback_thresholds),
        "baseline": {
            "candidate": context["baseline_label"],
            "strategy_version": "v2.5",
            "entry_threshold": v25.ENTRY_THRESHOLD,
            "exit_threshold": v25.EXIT_THRESHOLD,
            "target_vol": v25.TARGET_VOL,
            "max_leverage": v25.TARGET_VOL_MAX_LEVERAGE,
            "scale_rebalance_threshold": v25.TARGET_VOL_SCALE_REBALANCE_THRESHOLD,
        },
        "candidate_grid": wide["candidate"].tolist(),
        "data_snapshot": {
            "metrics_start": context["metrics_start"],
            "metrics_end": context["metrics_end"],
            "rows": context["rows"],
            "reference_summary_latest_nav_date": context["reference_summary"].get("latest_nav_date"),
        },
        "cost_model": {
            "base": "formal_v2_5_costed",
            "overlay_trade_cost": ONE_SIDE_TRADE_COST,
            "overlay_trade_cost_model": "one_side_microcap_turnover_only_when_bias_overheat_overlay_changes_risk_state",
            "execution_timing": "close_confirmed_t_signal_next_session_execution",
            "target_vol_return_source": "microcap_pct_change_unhedged",
            "hedge_removed": True,
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
            "bias_overheat_sanity_checks": str(run_folder / "bias_overheat_sanity_checks.csv"),
        },
        "decision": "research_only_watchlist_bias_overheat",
        "stability_label": "ma60_bias_first_pass",
        "elapsed_sec": round(time.time() - started, 3),
    }
    _write_json(run_folder / "scan_meta.json", meta)
    _write_record(run_folder, wide, context, meta)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-folder", type=Path, default=RUN_FOLDER)
    parser.add_argument(
        "--ma-windows",
        default=",".join(str(value) for value in MA_WINDOWS),
        help="Comma-separated moving-average windows for bias overheat scans, e.g. 40,60,80.",
    )
    parser.add_argument(
        "--ma-threshold-grid",
        default="",
        help=(
            "Optional per-MA grid like "
            "'40:0.24,0.25|0.18,0.19;60:0.32,0.33|0.20,0.21'. "
            "When set, it overrides --ma-windows and default threshold constants."
        ),
    )
    parser.add_argument(
        "--trigger-mode",
        choices=("level", "turn_down"),
        default="level",
        help="Overheat trigger mode. level exits immediately at hot; turn_down waits for a pullback from the hot bias peak.",
    )
    parser.add_argument(
        "--pullback-thresholds",
        default="0.01,0.02,0.03,0.04",
        help="Comma-separated peak-to-current MA bias pullback thresholds used when --trigger-mode turn_down.",
    )
    args = parser.parse_args()
    if args.ma_threshold_grid:
        ma_windows, hot_by_ma, cool_by_ma = _parse_ma_threshold_grid(args.ma_threshold_grid)
    else:
        ma_windows = _parse_ma_windows(args.ma_windows)
        hot_by_ma = None
        cool_by_ma = None
    pullbacks = _parse_float_values(args.pullback_thresholds) if args.trigger_mode == "turn_down" else (0.0,)
    run(
        args.run_folder,
        ma_windows=ma_windows,
        hot_thresholds_by_ma=hot_by_ma,
        cool_thresholds_by_ma=cool_by_ma,
        trigger_mode=args.trigger_mode,
        pullback_thresholds=pullbacks,
    )


if __name__ == "__main__":
    main()
