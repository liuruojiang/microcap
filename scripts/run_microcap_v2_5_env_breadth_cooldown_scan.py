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


RUN_FOLDER = (
    ROOT
    / "quant_param_scan_runs"
    / "20260523_microcap_top100_v2_5_derived_market_env_breadth_cooldown_overlays_env_breadth_cooldown_filters"
)
PANEL_CSV = ROOT / "outputs" / "microcap_top100_mom16_biweekly_live_v2_0_base_panel_refreshed.csv"
MEMBERS_CSV = ROOT / "outputs" / "microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv"
ADJ_PRICE_DIR = Path(v25.v2_0.freq_mod.ADJ_PRICE_DIR)
SHARED_ADJ_PRICE_DIR = getattr(v25.v2_0.freq_mod, "SHARED_ADJ_PRICE_DIR", None)
TRADING_DAYS = int(v25.TRADING_DAYS)
ONE_SIDE_TRADE_COST = float(v25.v2_0.base_mod.freq_mod.cost_mod.ENTRY_COST)

INDEX_SPECS = {
    "csi300": ("1.000300", "CSI300 large-cap proxy"),
    "csi1000": ("1.000852", "CSI1000 small-cap beta proxy"),
    "chinext": ("0.399006", "ChiNext growth beta proxy"),
    "shcomp": ("1.000001", "Shanghai Composite broad-market proxy"),
}
INDEX_MA_GRID = (20, 60)
INDEX_SCALE_GRID = (0.0, 0.5)
BREADTH_UP_RATIO_GRID = (0.40, 0.45, 0.50, 0.55)
BREADTH_WIDTH_RATIO_GRID = (0.40, 0.45, 0.50, 0.55, 0.60)
BREADTH_SCALE_GRID = (0.0, 0.5)
WIDTH_MA = 20
COOLDOWN_DAYS_GRID = (3, 5, 10)
WINDOWS = {
    "full": None,
    "last_10y": pd.DateOffset(years=10),
    "last_5y": pd.DateOffset(years=5),
    "last_3y": pd.DateOffset(years=3),
    "last_1y": pd.DateOffset(years=1),
}


def _git(args: list[str]) -> str:
    try:
        return subprocess.check_output(["git", *args], cwd=ROOT, text=True, stderr=subprocess.STDOUT).strip()
    except Exception as exc:  # noqa: BLE001
        return f"git_error:{exc}"


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


def _metrics(ret: pd.Series) -> dict[str, float | int]:
    r = pd.to_numeric(ret, errors="coerce").fillna(0.0)
    rows = int(len(r))
    if rows == 0:
        return {
            "rows": 0,
            "ann_return": np.nan,
            "ann_vol": np.nan,
            "sharpe_repo": np.nan,
            "max_dd": np.nan,
            "final_nav": np.nan,
        }
    nav = (1.0 + r).cumprod()
    ann_return = float(nav.iloc[-1] ** (TRADING_DAYS / rows) - 1.0) if nav.iloc[-1] > 0 else np.nan
    ann_vol = float(r.std(ddof=1) * math.sqrt(TRADING_DAYS)) if rows > 1 else 0.0
    sharpe = ann_return / ann_vol if ann_vol > 0 and math.isfinite(ann_vol) else np.nan
    dd = nav.div(nav.cummax()).sub(1.0)
    return {
        "rows": rows,
        "ann_return": ann_return,
        "ann_vol": ann_vol,
        "sharpe_repo": float(sharpe) if math.isfinite(sharpe) else np.nan,
        "max_dd": float(dd.min()),
        "final_nav": float(nav.iloc[-1]),
    }


def _window_index(index: pd.DatetimeIndex, offset: pd.DateOffset | None) -> pd.DatetimeIndex:
    if offset is None:
        return index
    cutoff = index.max() - offset
    return index[index >= cutoff]


def _load_v2_5_shadow() -> tuple[dict[str, Any], pd.DataFrame]:
    summary, shadow = scan_common.load_fresh_official_v25()
    return summary, shadow


def _load_index_panel() -> pd.DataFrame:
    panel = pd.read_csv(PANEL_CSV, parse_dates=["date"]).sort_values("date").set_index("date")
    return panel


def _symbol_to_file_stem(symbol: object) -> str:
    text = str(symbol).strip()
    if text.endswith(".0"):
        text = text[:-2]
    if text.isdigit():
        return f"{int(text):06d}"
    return text.zfill(6)


def _load_price_series(symbol: object) -> pd.Series | None:
    clean_symbol = _symbol_to_file_stem(symbol)
    path = v25.v2_0.freq_mod.resolve_cache_path(ADJ_PRICE_DIR, SHARED_ADJ_PRICE_DIR, clean_symbol)
    if path is None:
        return None
    df = pd.read_csv(path, parse_dates=["date"])
    return_col = next((col for col in ("close_qfq", "close_adj") if col in df.columns), None)
    if "date" not in df.columns or return_col is None:
        raise RuntimeError(f"adjusted breadth cache schema invalid for {clean_symbol}: {path}")
    out = (
        df.loc[:, ["date", return_col]]
        .dropna(subset=["date", return_col])
        .drop_duplicates("date", keep="last")
        .sort_values("date")
        .set_index("date")[return_col]
    )
    return pd.to_numeric(out, errors="coerce").dropna()


def _load_members_by_rebalance() -> dict[pd.Timestamp, list[str]]:
    members = pd.read_csv(MEMBERS_CSV, parse_dates=["rebalance_date"])
    members = members.dropna(subset=["rebalance_date", "symbol"]).sort_values(["rebalance_date", "rank"])
    grouped: dict[pd.Timestamp, list[str]] = {}
    for dt, part in members.groupby("rebalance_date"):
        grouped[pd.Timestamp(dt)] = [_symbol_to_file_stem(value) for value in part["symbol"].tolist()]
    return grouped


def _build_breadth_frame(nav_index: pd.DatetimeIndex) -> tuple[pd.DataFrame, dict[str, Any]]:
    members_by_rebalance = _load_members_by_rebalance()
    rebalances = sorted(members_by_rebalance)
    price_cache: dict[str, pd.Series | None] = {}
    rows: list[pd.DataFrame] = []
    missing_symbols: set[str] = set()
    used_symbols: set[str] = set()

    for i, rebalance_date in enumerate(rebalances):
        next_rebalance = rebalances[i + 1] if i + 1 < len(rebalances) else nav_index.max() + pd.Timedelta(days=1)
        seg_index = nav_index[(nav_index >= rebalance_date) & (nav_index < next_rebalance)]
        if len(seg_index) == 0:
            continue
        close_parts: list[pd.Series] = []
        for symbol in members_by_rebalance[rebalance_date]:
            if symbol not in price_cache:
                price_cache[symbol] = _load_price_series(symbol)
            series = price_cache[symbol]
            if series is None:
                missing_symbols.add(symbol)
                continue
            used_symbols.add(symbol)
            close_parts.append(series.rename(symbol))
        if not close_parts:
            continue
        close_history = pd.concat(close_parts, axis=1).sort_index()
        return_history = close_history.pct_change(fill_method=None)
        moving_average_history = close_history.rolling(WIDTH_MA, min_periods=WIDTH_MA).mean()
        close = close_history.reindex(seg_index)
        ret = return_history.reindex(seg_index)
        valid = ret.notna().sum(axis=1)
        required_coverage = max(95, math.ceil(0.95 * len(members_by_rebalance[rebalance_date])))
        bad_coverage = valid.loc[valid < required_coverage]
        if len(bad_coverage):
            examples = ", ".join(
                f"{pd.Timestamp(dt).date()}={int(count)}" for dt, count in bad_coverage.iloc[:10].items()
            )
            raise RuntimeError(
                f"Top100 adjusted breadth return coverage below {required_coverage}: {examples}"
            )
        up_ratio = ret.gt(0.0).sum(axis=1).div(valid.replace(0, np.nan))
        width_valid = close.notna().sum(axis=1)
        bad_width_coverage = width_valid.loc[width_valid < required_coverage]
        if len(bad_width_coverage):
            examples = ", ".join(
                f"{pd.Timestamp(dt).date()}={int(count)}" for dt, count in bad_width_coverage.iloc[:10].items()
            )
            raise RuntimeError(
                f"Top100 adjusted breadth price coverage below {required_coverage}: {examples}"
            )
        above_ma = close.gt(moving_average_history.reindex(seg_index))
        width_ratio = above_ma.sum(axis=1).div(width_valid.replace(0, np.nan))
        ew_return = ret.mean(axis=1, skipna=True)
        rows.append(
            pd.DataFrame(
                {
                    "top100_up_ratio": up_ratio,
                    "top100_ma20_width": width_ratio,
                    "top100_equal_weight_return": ew_return,
                    "top100_valid_members": valid,
                },
                index=seg_index,
            )
        )

    if not rows:
        raise RuntimeError("failed to build Top100 breadth frame from local member and price caches")
    out = pd.concat(rows).sort_index()
    out = out[~out.index.duplicated(keep="last")].reindex(nav_index)
    meta = {
        "member_rebalance_rows": len(rebalances),
        "price_symbols_used": len(used_symbols),
        "price_symbols_missing": len(missing_symbols),
        "price_adjustment_mode": "qfq_or_adjusted_close_required",
        "minimum_valid_members": 95,
        "minimum_member_coverage": 0.95,
        "missing_symbol_examples": sorted(missing_symbols)[:20],
        "breadth_start": str(out.dropna(how="all").index.min().date()),
        "breadth_end": str(out.dropna(how="all").index.max().date()),
        "amount_diffusion_status": "not_measured_no_local_historical_member_amount_cache",
    }
    return out, meta


def _apply_scaled_condition(
    shadow: pd.DataFrame,
    condition: pd.Series,
    *,
    scale: float,
    candidate: str,
    filter_group: str,
) -> pd.DataFrame:
    out = shadow.copy().sort_index()
    base_scale = pd.to_numeric(out.get("current_execution_scale", pd.Series(1.0, index=out.index)), errors="coerce").fillna(0.0)
    active = base_scale.gt(1e-12)
    condition_t1 = condition.reindex(out.index).fillna(False).astype(bool).shift(1, fill_value=False)
    scale_series = pd.Series(1.0, index=out.index, dtype=float)
    scale_series.loc[condition_t1 & active] = float(scale)
    next_scale_series = pd.Series(1.0, index=out.index, dtype=float)
    next_scale_series.loc[condition.reindex(out.index).fillna(False).astype(bool)] = float(scale)
    out = scan_common.replay_scale_multiplier(
        out,
        scale_series,
        next_multiplier=next_scale_series,
        one_side_scale_cost=ONE_SIDE_TRADE_COST,
        label=candidate,
    )
    out["candidate"] = candidate
    out["filter_group"] = filter_group
    out["filter_condition_t1"] = condition_t1
    out["filter_execution_scale"] = scale_series
    out["filter_overlay_cost"] = out["overlay_scale_change_cost"]
    return out


def _apply_cooldown(shadow: pd.DataFrame, cooldown_days: int) -> pd.DataFrame:
    out = shadow.copy().sort_index()
    base_ret = pd.to_numeric(out["return_net"], errors="coerce").fillna(0.0)
    base_scale = pd.to_numeric(out.get("current_execution_scale", pd.Series(1.0, index=out.index)), errors="coerce").fillna(0.0)
    holding = out["holding"].fillna("cash").astype(str)
    next_holding = out["next_holding"].fillna(holding).astype(str)
    base_active = base_scale.gt(1e-12)
    remaining = 0
    blocked_flags: list[bool] = []
    scale_values: list[float] = []
    prev_scale = 0.0
    prev_deviation = 0.0
    overlay_costs: list[float] = []
    returns: list[float] = []
    exit_flags: list[bool] = []

    for dt in out.index:
        blocked = remaining > 0 and bool(base_active.loc[dt])
        target_scale = 0.0 if blocked else float(base_scale.loc[dt])
        deviation = target_scale - float(base_scale.loc[dt])
        overlay_turnover = abs(deviation - prev_deviation)
        overlay_cost = overlay_turnover * ONE_SIDE_TRADE_COST if overlay_turnover > 1e-12 else 0.0
        day_ret = 0.0 if blocked else float(base_ret.loc[dt])
        ret = (1.0 + day_ret) * (1.0 - min(max(overlay_cost, 0.0), 0.99)) - 1.0
        exit_signal = holding.loc[dt] != "cash" and next_holding.loc[dt] == "cash"
        if exit_signal:
            remaining = int(cooldown_days)
        elif remaining > 0:
            remaining -= 1
        blocked_flags.append(blocked)
        scale_values.append(target_scale)
        overlay_costs.append(overlay_cost)
        returns.append(ret)
        exit_flags.append(bool(exit_signal))
        prev_scale = target_scale
        prev_deviation = deviation

    ret_series = pd.Series(returns, index=out.index, dtype=float)
    out["return_net"] = ret_series
    out["nav_net"] = (1.0 + ret_series).cumprod()
    out["nav"] = out["nav_net"]
    out["candidate"] = f"cooldown_{cooldown_days}d_after_exit"
    out["filter_group"] = "cooldown"
    out["cooldown_days"] = int(cooldown_days)
    out["cooldown_blocked"] = pd.Series(blocked_flags, index=out.index, dtype=bool)
    out["filter_execution_scale"] = pd.Series(scale_values, index=out.index, dtype=float)
    out["filter_overlay_cost"] = pd.Series(overlay_costs, index=out.index, dtype=float)
    out["cooldown_exit_signal"] = pd.Series(exit_flags, index=out.index, dtype=bool)
    out["base_holding_state"] = holding
    out["base_next_holding_state"] = next_holding
    out["base_current_execution_scale"] = base_scale
    out["current_execution_scale"] = out["filter_execution_scale"]
    out["holding"] = np.where(out["current_execution_scale"].gt(1e-12), holding, "cash")
    next_scale = out["current_execution_scale"].shift(-1).fillna(0.0)
    out["next_session_actionable_scale"] = next_scale
    out["next_holding"] = np.where(next_scale.gt(1e-12), next_holding, "cash")
    scan_common.assert_candidate_state_consistent(out, f"cooldown {cooldown_days}d")
    return out


def _append_candidate_metrics(
    candidate_df: pd.DataFrame,
    *,
    candidate: str,
    filter_group: str,
    params: dict[str, Any],
    baseline_by_window: dict[str, dict[str, float | int]],
    summary_rows: list[dict[str, Any]],
    wide_rows: list[dict[str, Any]],
) -> None:
    ret = pd.to_numeric(candidate_df["return_net"], errors="coerce").fillna(0.0)
    wide: dict[str, Any] = {"candidate": candidate, "filter_group": filter_group, **params}
    for window_name, offset in WINDOWS.items():
        idx = _window_index(candidate_df.index, offset)
        metrics = _metrics(ret.loc[idx])
        baseline = baseline_by_window[window_name]
        row = {
            "candidate": candidate,
            "filter_group": filter_group,
            "segment": window_name,
            "window": window_name,
            "start": str(pd.Timestamp(idx.min()).date()) if len(idx) else "",
            "end": str(pd.Timestamp(idx.max()).date()) if len(idx) else "",
            **params,
            **metrics,
            "ann_return_delta": float(metrics["ann_return"] - baseline["ann_return"]),
            "max_dd_delta": float(metrics["max_dd"] - baseline["max_dd"]),
            "sharpe_delta": float(metrics["sharpe_repo"] - baseline["sharpe_repo"]),
        }
        if "filter_condition_t1" in candidate_df.columns:
            row["filter_days"] = int(candidate_df.loc[idx, "filter_condition_t1"].fillna(False).astype(bool).sum())
        if "cooldown_blocked" in candidate_df.columns:
            row["filter_days"] = int(candidate_df.loc[idx, "cooldown_blocked"].fillna(False).astype(bool).sum())
        if "filter_overlay_cost" in candidate_df.columns:
            overlay_cost = pd.to_numeric(candidate_df["filter_overlay_cost"], errors="coerce").reindex(idx).fillna(0.0)
        else:
            overlay_cost = pd.Series(0.0, index=idx, dtype=float)
        row["overlay_cost_sum"] = float(overlay_cost.sum())
        summary_rows.append(row)
        for key, value in metrics.items():
            wide[f"{key}_{window_name}"] = value
        wide[f"ann_return_delta_{window_name}"] = row["ann_return_delta"]
        wide[f"max_dd_delta_{window_name}"] = row["max_dd_delta"]
        wide[f"sharpe_delta_{window_name}"] = row["sharpe_delta"]
        wide[f"filter_days_{window_name}"] = row.get("filter_days", 0)
        wide[f"overlay_cost_sum_{window_name}"] = row["overlay_cost_sum"]
    wide["decision_score"] = (
        wide["ann_return_delta_last_5y"]
        + wide["ann_return_delta_last_3y"]
        + 0.5 * wide["ann_return_delta_last_10y"]
        + 0.75 * wide["max_dd_delta_last_10y"]
        + 0.75 * wide["max_dd_delta_last_5y"]
        - max(0.0, -wide["ann_return_delta_full"]) * 0.5
    )
    wide["decision_hint"] = "compare_only"
    wide["stability_label"] = "research_candidate"
    wide_rows.append(wide)


def _scan(run_folder: Path) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    reference_summary, shadow = _load_v2_5_shadow()
    shadow = shadow.sort_index()
    panel = _load_index_panel().reindex(shadow.index)
    breadth, breadth_meta = _build_breadth_frame(pd.DatetimeIndex(shadow.index))
    baseline_by_window: dict[str, dict[str, float | int]] = {}
    base_ret = pd.to_numeric(shadow["return_net"], errors="coerce").fillna(0.0)
    for window_name, offset in WINDOWS.items():
        baseline_by_window[window_name] = _metrics(base_ret.loc[_window_index(shadow.index, offset)])

    summary_rows: list[dict[str, Any]] = []
    wide_rows: list[dict[str, Any]] = []
    _append_candidate_metrics(
        shadow.assign(filter_overlay_cost=0.0),
        candidate="v2_5_baseline",
        filter_group="baseline",
        params={"scale": 1.0},
        baseline_by_window=baseline_by_window,
        summary_rows=summary_rows,
        wide_rows=wide_rows,
    )

    top_daily: dict[str, pd.DataFrame] = {}
    for index_label, (column, description) in INDEX_SPECS.items():
        close = pd.to_numeric(panel[column], errors="coerce")
        for ma in INDEX_MA_GRID:
            condition = close.lt(close.rolling(ma, min_periods=ma).mean())
            for scale in INDEX_SCALE_GRID:
                candidate = f"env_{index_label}_below_ma{ma}_scale{int(scale * 100):03d}"
                out = _apply_scaled_condition(shadow, condition, scale=scale, candidate=candidate, filter_group="market_env")
                params = {
                    "index_label": index_label,
                    "index_column": column,
                    "index_description": description,
                    "ma": ma,
                    "scale": scale,
                }
                _append_candidate_metrics(
                    out,
                    candidate=candidate,
                    filter_group="market_env",
                    params=params,
                    baseline_by_window=baseline_by_window,
                    summary_rows=summary_rows,
                    wide_rows=wide_rows,
                )

    up_ratio = pd.to_numeric(breadth["top100_up_ratio"], errors="coerce")
    for threshold in BREADTH_UP_RATIO_GRID:
        condition = up_ratio.lt(threshold)
        for scale in BREADTH_SCALE_GRID:
            candidate = f"breadth_up_lt{int(threshold * 100):02d}_scale{int(scale * 100):03d}"
            out = _apply_scaled_condition(shadow, condition, scale=scale, candidate=candidate, filter_group="breadth_up_ratio")
            _append_candidate_metrics(
                out,
                candidate=candidate,
                filter_group="breadth_up_ratio",
                params={"breadth_threshold": threshold, "scale": scale},
                baseline_by_window=baseline_by_window,
                summary_rows=summary_rows,
                wide_rows=wide_rows,
            )

    width_ratio = pd.to_numeric(breadth["top100_ma20_width"], errors="coerce")
    for threshold in BREADTH_WIDTH_RATIO_GRID:
        condition = width_ratio.lt(threshold)
        for scale in BREADTH_SCALE_GRID:
            candidate = f"breadth_width20_lt{int(threshold * 100):02d}_scale{int(scale * 100):03d}"
            out = _apply_scaled_condition(shadow, condition, scale=scale, candidate=candidate, filter_group="breadth_width")
            _append_candidate_metrics(
                out,
                candidate=candidate,
                filter_group="breadth_width",
                params={"breadth_threshold": threshold, "width_ma": WIDTH_MA, "scale": scale},
                baseline_by_window=baseline_by_window,
                summary_rows=summary_rows,
                wide_rows=wide_rows,
            )

    for cooldown_days in COOLDOWN_DAYS_GRID:
        out = _apply_cooldown(shadow, cooldown_days)
        _append_candidate_metrics(
            out,
            candidate=f"cooldown_{cooldown_days}d_after_exit",
            filter_group="cooldown",
            params={"cooldown_days": cooldown_days, "scale": 0.0},
            baseline_by_window=baseline_by_window,
            summary_rows=summary_rows,
            wide_rows=wide_rows,
        )

    summary = pd.DataFrame(summary_rows)
    wide = pd.DataFrame(wide_rows)
    wide.loc[wide["candidate"].eq("v2_5_baseline"), "decision_hint"] = "v2_5_baseline"
    wide.loc[wide["candidate"].eq("v2_5_baseline"), "stability_label"] = "baseline"
    best_by_group = (
        wide.loc[wide["filter_group"].ne("baseline")]
        .sort_values("decision_score", ascending=False)
        .groupby("filter_group", as_index=False)
        .head(1)["candidate"]
        .tolist()
    )
    wide.loc[wide["candidate"].isin(best_by_group), "decision_hint"] = "best_in_filter_group"
    wide.loc[wide["candidate"].isin(best_by_group), "stability_label"] = "watchlist_best"
    top_candidates = wide.sort_values("decision_score", ascending=False).head(8)["candidate"].tolist()
    for candidate in top_candidates:
        if candidate == "v2_5_baseline":
            continue
        # Keep a compact daily audit trail for the most relevant candidates.
        if candidate.startswith("cooldown_"):
            days = int(candidate.split("_")[1].replace("d", ""))
            top_daily[candidate] = _apply_cooldown(shadow, days)
    summary.to_csv(run_folder / "scan_summary.csv", index=False, encoding="utf-8")
    wide.to_csv(run_folder / "window_metrics.csv", index=False, encoding="utf-8")
    breadth.to_csv(run_folder / "breadth_factors.csv", index_label="date", encoding="utf-8")
    sanity = pd.DataFrame(
        [
            {
                "candidate_count": int(len(wide)),
                "summary_rows": int(len(summary)),
                "all_windows_present": bool(set(summary["segment"].unique()) == set(WINDOWS)),
                "all_finite_return_metrics": bool(np.isfinite(pd.to_numeric(summary["ann_return"], errors="coerce")).all()),
                "baseline_rows": int(wide["candidate"].eq("v2_5_baseline").sum()),
                "shadow_rows": int(len(shadow)),
            }
        ]
    )
    sanity.to_csv(run_folder / "sanity_checks.csv", index=False, encoding="utf-8")
    return summary, wide, {
        "reference_summary": reference_summary,
        "metrics_start": str(pd.Timestamp(shadow.index.min()).date()),
        "metrics_end": str(pd.Timestamp(shadow.index.max()).date()),
        "rows": int(len(shadow)),
        "panel_columns_used": {label: spec[0] for label, spec in INDEX_SPECS.items()},
        "panel_start": str(panel.dropna(how="all").index.min().date()),
        "panel_end": str(panel.dropna(how="all").index.max().date()),
        "breadth_meta": breadth_meta,
        "candidate_count": int(len(wide)),
        "best_by_group": best_by_group,
        "baseline": wide.loc[wide["candidate"].eq("v2_5_baseline")].iloc[0].to_dict(),
    }


def _write_record(run_folder: Path, wide: pd.DataFrame, context: dict[str, Any], meta: dict[str, Any]) -> None:
    top = wide.sort_values("decision_score", ascending=False).head(18)
    baseline = wide.loc[wide["candidate"].eq("v2_5_baseline")].iloc[0]
    lines = [
        "# Quant Parameter Scan Record",
        "",
        "## Run Metadata",
        "",
        f"- Run id: `{run_folder.name}`",
        f"- Created at: {meta['created_at']}",
        "- Project: microcap Top100",
        "- Strategy or version: v2.5 derived",
        "- Sleeve or subsystem: market environment, Top100 breadth, and cooldown filters",
        "- Parameter group: `env_breadth_cooldown_filters`",
        "- Scan type: defensive_filter_overlay_grid",
        f"- Repo or workspace path: `{ROOT}`",
        "- Target entrypoint: `scripts/run_microcap_v2_5_env_breadth_cooldown_scan.py`",
        f"- Git branch: `{meta['git_branch']}`",
        f"- Git commit: `{meta['git_commit']}`",
        "",
        "## Research Question",
        "",
        "- Baseline: formal v2.5 costed path.",
        "- Market filters: index close below MA20/MA60, exposure scaled to 0% or 50% on the next trading day.",
        "- Breadth filters: Top100 current-member up ratio or MA20 width below thresholds, exposure scaled to 0% or 50% on the next trading day.",
        "- Cooldown filters: after a v2.5 long-to-cash exit signal, block active reentry for 3/5/10 trading days.",
        "- Source-change rule: `research_only_no_source_change`.",
        "",
        "## Implementation Anchor",
        "",
        "- Shadow path: official v2.5 costed NAV.",
        "- Market environment factors use the refreshed v2.0 base index panel.",
        "- Breadth factors use each rebalance period's Top100 member list and local raw close cache.",
        "- Cooldown is applied as a research overlay after v2.5 long-to-cash exits because formal v2.5 has no stop-loss or take-profit layer.",
        "",
        "## Data Snapshot",
        "",
        f"- Metrics start: {context['metrics_start']}",
        f"- Metrics end: {context['metrics_end']}",
        f"- Rows: {context['rows']}",
        f"- Reference v2.5 latest NAV date: {context['reference_summary'].get('latest_nav_date')}",
        f"- Index panel: `{PANEL_CSV}`; columns used {context['panel_columns_used']}.",
        f"- Top100 members: `{MEMBERS_CSV}`.",
        f"- Member adjusted close cache: `{ADJ_PRICE_DIR}`; symbols used {context['breadth_meta']['price_symbols_used']}, missing {context['breadth_meta']['price_symbols_missing']}.",
        f"- Amount diffusion: {context['breadth_meta']['amount_diffusion_status']}.",
        "",
        "## Cost and Execution Assumptions",
        "",
        "- Retained: v2.5 base costed return stream, target-vol scaling, financing, and embedded turnover costs.",
        f"- Added: overlay exposure-change cost `{ONE_SIDE_TRADE_COST}` times active one-side scale delta.",
        "- Timing: T close filter condition affects T+1 return. This is a defensive overlay simulation, not a production signal change.",
        "",
        "## Runtime Override Plan",
        "",
        "- No production source defaults are changed; this is a scratch research run.",
        "",
        "## Commands",
        "",
        "```powershell",
        f"python scripts/run_microcap_v2_5_env_breadth_cooldown_scan.py --run-folder {run_folder}",
        f"python C:\\Users\\Administrator.DESKTOP-95I7VVU\\.codex\\skills\\quant-param-scan\\scripts\\finalize_quant_param_scan_run.py {run_folder} --decision research_only_compare_defensive_filters --stability-label first_pass_watchlist",
        f"python C:\\Users\\Administrator.DESKTOP-95I7VVU\\.codex\\skills\\quant-param-scan\\scripts\\check_quant_param_scan_artifacts.py --phase complete --strict {run_folder}",
        "```",
        "",
        "## Output Files",
        "",
        "- `scan_summary.csv`: long-form metrics by candidate and segment.",
        "- `window_metrics.csv`: wide candidate comparison table.",
        "- `breadth_factors.csv`: daily Top100 breadth factors rebuilt from local member prices.",
        "- `sanity_checks.csv`: row and metric checks.",
        "",
        "## Full-Sample Results",
        "",
        f"- Annual return full: {baseline['ann_return_full']:.4%}; max drawdown full: {baseline['max_dd_full']:.4%}; Sharpe full: {baseline['sharpe_repo_full']:.3f}.",
        f"- Annual return last_5y: {baseline['ann_return_last_5y']:.4%}; max drawdown last_5y: {baseline['max_dd_last_5y']:.4%}.",
        "",
        "## Window Results",
        "",
        top[
            [
                "candidate",
                "filter_group",
                "decision_score",
                "ann_return_delta_full",
                "max_dd_delta_full",
                "ann_return_delta_last_10y",
                "max_dd_delta_last_10y",
                "ann_return_delta_last_5y",
                "max_dd_delta_last_5y",
                "ann_return_delta_last_3y",
                "max_dd_delta_last_3y",
                "filter_days_full",
            ]
        ].to_markdown(index=False, floatfmt=".6f"),
        "",
        "## Decision",
        "",
        "- Decision: research_only_compare_defensive_filters.",
        "- Stability label: first_pass_watchlist.",
        "- Recommended next action: only inspect group winners further; do not promote any filter until combined-overfit and trade-timing sanity checks are run.",
        "",
        "## Stability Classification",
        "",
        "- Label: first_pass_watchlist.",
        "- Evidence: full required-window metrics are in `window_metrics.csv`; group winners are research candidates only.",
        "",
        "## User-Facing Summary",
        "",
        "- This run compares market-beta environment filters, Top100 breadth filters, and post-exit cooldowns on the same v2.5 costed baseline.",
    ]
    (run_folder / "record.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(run_folder: Path) -> None:
    started = time.time()
    run_folder.mkdir(parents=True, exist_ok=True)
    command_log = run_folder / "command_log.txt"
    with command_log.open("a", encoding="utf-8") as fh:
        fh.write(f"\n[{pd.Timestamp.now().isoformat()}] python scripts/run_microcap_v2_5_env_breadth_cooldown_scan.py --run-folder {run_folder}\n")
    summary, wide, context = _scan(run_folder)
    meta = {
        "run_id": run_folder.name,
        "created_at": pd.Timestamp.now().isoformat(),
        "phase": "analysis_written",
        "project": "microcap Top100",
        "strategy": "v2.5 derived",
        "subsystem": "market environment, Top100 breadth, and cooldown filters",
        "parameter_group": "env_breadth_cooldown_filters",
        "repo_root": ROOT,
        "entrypoint": "scripts/run_microcap_v2_5_env_breadth_cooldown_scan.py",
        "git_branch": _git(["branch", "--show-current"]),
        "git_commit": _git(["rev-parse", "HEAD"]),
        "git_status_before": _git(["status", "--short"]),
        "git_status_after": _git(["status", "--short"]),
        "scan_type": "defensive_filter_overlay_grid",
        "candidate_count": int(len(wide)),
        "windows": list(WINDOWS),
        "baseline": {
            "strategy_version": "v2.5",
            "target_vol": v25.TARGET_VOL,
            "max_leverage": v25.TARGET_VOL_MAX_LEVERAGE,
            "entry_threshold": v25.ENTRY_THRESHOLD,
            "exit_threshold": v25.EXIT_THRESHOLD,
        },
        "data_snapshot": {
            "metrics_start": context["metrics_start"],
            "metrics_end": context["metrics_end"],
            "rows": context["rows"],
            "index_panel": PANEL_CSV,
            "members_csv": MEMBERS_CSV,
            "adjusted_price_dir": ADJ_PRICE_DIR,
            "breadth": context["breadth_meta"],
        },
        "cost_model": {
            "base": "formal_v2_5_costed",
            "overlay_trade_cost": ONE_SIDE_TRADE_COST,
            "overlay_trade_cost_model": "one_side_microcap_scale_delta_when_filter_changes_effective_exposure",
            "execution_timing": "T_close_filter_affects_T_plus_1_return",
        },
        "outputs": {
            "record": run_folder / "record.md",
            "scan_summary": run_folder / "scan_summary.csv",
            "window_metrics": run_folder / "window_metrics.csv",
            "scan_meta": run_folder / "scan_meta.json",
            "command_log": command_log,
            "breadth_factors": run_folder / "breadth_factors.csv",
            "sanity_checks": run_folder / "sanity_checks.csv",
        },
        "decision": "research_only_compare_defensive_filters",
        "stability_label": "first_pass_watchlist",
        "elapsed_sec": round(time.time() - started, 3),
    }
    _write_json(run_folder / "scan_meta.json", meta)
    _write_record(run_folder, wide, context, meta)
    print(run_folder)
    print(f"candidates={len(wide)} summary_rows={len(summary)} elapsed_sec={meta['elapsed_sec']}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-folder", type=Path, default=RUN_FOLDER)
    args = parser.parse_args()
    run(args.run_folder)


if __name__ == "__main__":
    main()
