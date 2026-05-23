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


RUN_FOLDER = ROOT / "quant_param_scan_runs" / "20260523_microcap_top100_v2_5_pool_rebalance_frequency"
TRADING_DAYS = int(v25.TRADING_DAYS)
ONE_SIDE_REBALANCE_COST = 0.003
WINDOWS = {
    "full": None,
    "last_10y": pd.DateOffset(years=10),
    "last_5y": pd.DateOffset(years=5),
    "last_3y": pd.DateOffset(years=3),
    "last_1y": pd.DateOffset(years=1),
}
VARIANTS = [
    {
        "candidate": "v25_official_top100_biweekly",
        "top_n": 100,
        "rebalance_frequency": "biweekly",
        "source": "official_v25_output",
    },
    {
        "candidate": "rebuilt_top100_biweekly",
        "top_n": 100,
        "rebalance_frequency": "biweekly",
        "source": "local_cache_rebuild",
    },
    {
        "candidate": "top50_biweekly",
        "top_n": 50,
        "rebalance_frequency": "biweekly",
        "source": "local_cache_rebuild",
    },
    {
        "candidate": "top200_biweekly",
        "top_n": 200,
        "rebalance_frequency": "biweekly",
        "source": "local_cache_rebuild",
    },
    {
        "candidate": "top100_weekly",
        "top_n": 100,
        "rebalance_frequency": "weekly",
        "source": "local_cache_rebuild",
    },
    {
        "candidate": "top100_monthly",
        "top_n": 100,
        "rebalance_frequency": "monthly",
        "source": "local_cache_rebuild",
    },
]


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


def _window_slices(index: pd.DatetimeIndex) -> dict[str, tuple[pd.Timestamp, pd.Timestamp]]:
    end = pd.Timestamp(index.max())
    start = pd.Timestamp(index.min())
    out = {"full": (start, end)}
    for name, offset in WINDOWS.items():
        if name == "full" or offset is None:
            continue
        out[name] = (max(start, end - offset), end)
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


def _weekly_rebalance_dates(trading_dates: pd.DatetimeIndex) -> pd.DatetimeIndex:
    freq = v25.v2_0.base_mod.WEEK_FREQ_BY_START[v25.v2_0.base_mod.REBALANCE_WEEKDAY]
    periods = trading_dates.to_period(freq)
    selected = trading_dates.to_series().groupby(periods).min().dropna()
    return pd.DatetimeIndex(selected.tolist())


def _monthly_rebalance_dates(trading_dates: pd.DatetimeIndex) -> pd.DatetimeIndex:
    periods = trading_dates.to_period("M")
    selected = trading_dates.to_series().groupby(periods).min().dropna()
    return pd.DatetimeIndex(selected.tolist())


def _rebalance_dates(trading_dates: pd.DatetimeIndex, frequency: str) -> pd.DatetimeIndex:
    if frequency == "biweekly":
        return v25.v2_0.base_mod.build_biweekly_rebalance_dates(trading_dates)
    if frequency == "weekly":
        return _weekly_rebalance_dates(trading_dates)
    if frequency == "monthly":
        return _monthly_rebalance_dates(trading_dates)
    raise ValueError(f"unsupported rebalance frequency: {frequency}")


def _load_official_v25() -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    summary, _signal, official_v25 = v25.generate_v2_5_outputs()
    official_v20 = v25._load_official_v2_0_out()
    reference_summary, base_gross_cached, _turnover_df = v25.v2_0.embedded_context._load_embedded_base_context()
    if not official_v25.index.equals(pd.DatetimeIndex(official_v25.index)):
        official_v25.index = pd.DatetimeIndex(official_v25.index)
    return summary, official_v25.sort_index(), official_v20.sort_index(), reference_summary, base_gross_cached.sort_index()


def _prepare_cache_inputs(
    max_workers: int,
    refreshed_trading_dates: pd.DatetimeIndex,
) -> tuple[
    pd.DatetimeIndex,
    dict[str, pd.DatetimeIndex],
    pd.DataFrame,
    dict[pd.Timestamp, dict[str, float]],
    pd.DataFrame,
    pd.DataFrame,
    dict[str, str],
    list[str],
]:
    trading_dates = pd.DatetimeIndex(refreshed_trading_dates).drop_duplicates().sort_values()
    schedules = {
        frequency: _rebalance_dates(trading_dates, frequency)
        for frequency in sorted({str(item["rebalance_frequency"]) for item in VARIANTS if item["source"] == "local_cache_rebuild"})
    }
    all_cap_dates = pd.DatetimeIndex(sorted(set().union(*[set(dates) for dates in schedules.values()])))
    symbols = v25.v2_0.freq_mod.load_current_universe()
    returns_df, caps_by_date, buyable_df, sellable_df = v25.v2_0.freq_mod.load_cache_panels(
        symbols=symbols,
        trading_dates=trading_dates,
        cap_dates=all_cap_dates,
        max_workers=max_workers,
        trade_constraint_mode=v25.v2_0.base_mod.TRADE_CONSTRAINT_MODE,
        exclude_historical_st_from_caps=False,
    )
    name_map = v25.v2_0.base_mod.load_name_map()
    return trading_dates, schedules, returns_df, caps_by_date, buyable_df, sellable_df, name_map, symbols


def _build_variant_output(
    *,
    candidate: str,
    top_n: int,
    rebalance_frequency: str,
    schedules: dict[str, pd.DatetimeIndex],
    returns_df: pd.DataFrame,
    caps_by_date: dict[pd.Timestamp, dict[str, float]],
    buyable_df: pd.DataFrame,
    sellable_df: pd.DataFrame,
    name_map: dict[str, str],
    official_index: pd.DatetimeIndex,
    base_gross_cached: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    rebalance_dates = schedules[rebalance_frequency]
    target_members_map = v25.v2_0.base_mod.build_live_target_members_map(
        caps_by_date=caps_by_date,
        rebalance_dates=rebalance_dates,
        name_map=name_map,
        top_n=top_n,
    )
    members_df = v25.v2_0.freq_mod.build_target_members_frame(target_members_map, caps_by_date, name_map=name_map)
    index_df, turnover_df, _effective_members = v25.v2_0.freq_mod.simulate_rebalance_path(
        trading_dates=pd.DatetimeIndex(returns_df.index),
        returns_df=returns_df,
        target_members_map=target_members_map,
        rebalance_dates=rebalance_dates,
        buyable_df=buyable_df,
        sellable_df=sellable_df,
        one_side_cost_rate=ONE_SIDE_REBALANCE_COST,
        top_n=top_n,
        execution_timing=v25.v2_0.base_mod.EXECUTION_TIMING,
    )
    index_df["holding_effective"] = index_df["holding_count"].gt(0)
    index_df, members_df, turnover_df, effective_start = v25.v2_0.base_mod.trim_proxy_history(
        index_df,
        members_df,
        turnover_df,
    )
    microcap = index_df.set_index("date")["close"].astype(float).rename("microcap")
    hedge = base_gross_cached["hedge_close"].astype(float).rename("hedge")
    close_df = pd.concat([microcap, hedge], axis=1).dropna().sort_index()
    common_index = v25.build_v2_5_common_index(close_df, official_index)
    out = v25.build_v2_5_result(close_df, turnover_df, common_index)
    out["candidate"] = candidate
    out["top_n"] = int(top_n)
    out["rebalance_frequency"] = rebalance_frequency
    out["variant_source"] = "local_cache_rebuild"
    meta = {
        "candidate": candidate,
        "top_n": int(top_n),
        "rebalance_frequency": rebalance_frequency,
        "rebalance_dates": int(len(rebalance_dates)),
        "effective_start": None if effective_start is None else str(pd.Timestamp(effective_start).date()),
        "members_rows": int(0 if members_df is None else len(members_df)),
        "turnover_rows": int(0 if turnover_df is None else len(turnover_df)),
        "index_start": str(pd.Timestamp(index_df["date"].min()).date()),
        "index_end": str(pd.Timestamp(index_df["date"].max()).date()),
        "metrics_start": str(pd.Timestamp(out.index.min()).date()),
        "metrics_end": str(pd.Timestamp(out.index.max()).date()),
    }
    return out, index_df, turnover_df, meta


def _summarize_candidate(out: pd.DataFrame, candidate: str, extra: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    summary_rows: list[dict[str, Any]] = []
    wide: dict[str, Any] = {
        "candidate": candidate,
        **extra,
        **{f"{key}_full": value for key, value in _transition_counts(out).items()},
    }
    return_net = pd.to_numeric(out["return_net"], errors="coerce").fillna(0.0)
    current_scale = pd.to_numeric(out.get("current_execution_scale", pd.Series(np.nan, index=out.index)), errors="coerce")
    for segment, (start, end) in _window_slices(pd.DatetimeIndex(out.index)).items():
        part = out.loc[(out.index >= start) & (out.index <= end)]
        m = _metrics(part["return_net"])
        counts = _transition_counts(part)
        scale_part = current_scale.reindex(part.index)
        total_cost = pd.to_numeric(part.get("total_cost", pd.Series(0.0, index=part.index)), errors="coerce").fillna(0.0)
        row = {
            "candidate": candidate,
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
            "entry_days": counts["entry_days"],
            "exit_days": counts["exit_days"],
            "holding_day_ratio": counts["holding_days"] / len(part) if len(part) else np.nan,
            "avg_execution_scale": float(scale_part.mean()) if len(part) else np.nan,
            "max_execution_scale": float(scale_part.max()) if len(part) else np.nan,
            "total_cost_sum": float(total_cost.sum()),
            "top_n": extra.get("top_n"),
            "rebalance_frequency": extra.get("rebalance_frequency"),
            "variant_source": extra.get("variant_source"),
        }
        summary_rows.append(row)
        for key, value in row.items():
            if key in {"candidate", "segment", "start", "end", "top_n", "rebalance_frequency", "variant_source"}:
                continue
            wide[f"{key}_{segment}"] = value
    wide["return_sum_full"] = float(return_net.sum())
    wide["finite_return_net"] = bool(np.isfinite(return_net).all())
    return summary_rows, wide


def _write_record(run_folder: Path, wide: pd.DataFrame, context: dict[str, Any], meta: dict[str, Any]) -> None:
    ordered = wide.sort_values("ann_return_last_10y", ascending=False)
    cols = [
        "candidate",
        "top_n",
        "rebalance_frequency",
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
        "total_cost_sum_full",
    ]
    table = ordered[cols].to_string(index=False)
    baseline = wide.loc[wide["candidate"].eq("v25_official_top100_biweekly")].iloc[0]
    requested = wide.loc[
        wide["candidate"].isin(["top50_biweekly", "top200_biweekly", "top100_weekly", "top100_monthly"])
    ][cols]
    lines = [
        "# Quant Parameter Scan Record",
        "",
        "## Run Metadata",
        "",
        f"- Run id: `{run_folder.name}`",
        f"- Created at: {meta['created_at']}",
        "- Project: microcap Top100",
        "- Strategy or version: v2.5 derived",
        "- Sleeve or subsystem: pool size and rebalance frequency",
        "- Parameter group: `top_n_rebalance_frequency`",
        "- Scan type: local_cache_rebuild_variant_compare",
        f"- Repo or workspace path: `{ROOT}`",
        "- Target entrypoint: `scripts/run_microcap_v2_5_pool_rebalance_frequency_scan.py`",
        f"- Git branch: `{meta['git_branch']}`",
        f"- Git commit: `{meta['git_commit']}`",
        "- Working tree status before: see `scan_meta.json`.",
        "",
        "## Research Question",
        "",
        "- Baseline: official formal v2.5 Top100 biweekly costed path.",
        "- Candidate grid: Top50 biweekly, Top200 biweekly, Top100 weekly, Top100 monthly.",
        "- Extra parity row: rebuilt Top100 biweekly from the same local-cache path used by variants.",
        "- Source-change rule: `research_only_no_source_change`.",
        "- Required windows: full, last_10y, last_5y, last_3y, last_1y.",
        "",
        "## Implementation Anchor",
        "",
        "- Official entrypoint: `microcap_top100_mom16_biweekly_live_v2_5.py`.",
        "- Variant rebuild functions: `v2_0.freq_mod.load_cache_panels`, `v2_0.base_mod.build_live_target_members_map`, `v2_0.freq_mod.simulate_rebalance_path`, `v2_5.build_v2_5_result`.",
        "- Weekly schedule: every Thursday-anchored week using the same official weekday anchor as biweekly.",
        "- Monthly schedule: first trading day of each calendar month.",
        "",
        "## Data Snapshot",
        "",
        f"- Metrics start: {context['metrics_start']}",
        f"- Metrics end: {context['metrics_end']}",
        f"- Trading dates: {context['trading_start']} to {context['trading_end']}",
        f"- Current-universe symbols loaded: {context['symbols_loaded']}",
        f"- Official v2.5 latest NAV date: {context['official_latest_nav_date']}",
        "- Annualization: 244 trading days.",
        "",
        "## Cost and Execution Assumptions",
        "",
        "- Retained: v2.5 base trading cost, target-vol scale-change cost, scaled embedded base costs, idle-cash treatment, and financing above 1.0x.",
        "- Basket rebalance cost: `0.003 * ((buys + sells) / top_n)` using the existing close-execution turnover table.",
        "- Execution timing: close execution with the existing buyable/sellable and limit-lock constraints.",
        "- Hedge leg: removed, matching formal v2.5.",
        "",
        "## Commands",
        "",
        "```powershell",
        f"python scripts/run_microcap_v2_5_pool_rebalance_frequency_scan.py --run-folder {run_folder}",
        f"python C:\\Users\\Administrator.DESKTOP-95I7VVU\\.codex\\skills\\quant-param-scan\\scripts\\finalize_quant_param_scan_run.py {run_folder} --decision research_only_compare_pool_and_rebalance_frequency --stability-label first_pass_variant_compare",
        f"python C:\\Users\\Administrator.DESKTOP-95I7VVU\\.codex\\skills\\quant-param-scan\\scripts\\check_quant_param_scan_artifacts.py --phase complete --strict {run_folder}",
        "```",
        "",
        "## Output Files",
        "",
        "- `scan_summary.csv`: long-form metrics by candidate and window.",
        "- `window_metrics.csv`: wide candidate comparison table.",
        "- `variant_sanity_checks.csv`: row, finite-return, and rebuilt-baseline parity checks.",
        "- `daily_*.csv`: per-candidate daily paths.",
        "",
        "## Full-Sample Results",
        "",
        f"- Baseline annual return {baseline['ann_return_full']:.4%}, max drawdown {baseline['max_dd_full']:.4%}, Sharpe {baseline['sharpe_repo_full']:.3f}.",
        "",
        "## Window Results",
        "",
        table,
        "",
        "## Requested Variants",
        "",
        requested.to_string(index=False),
        "",
        "## Stability Classification",
        "",
        "- Label: first_pass_variant_compare.",
        f"- Rebuilt baseline max abs return diff vs official: {context['rebuilt_baseline_max_abs_return_diff']:.12g}.",
        f"- Sanity: all candidates finite return_net: {context['all_finite_return_net']}; all candidates have rows: {context['all_have_rows']}.",
        "",
        "## Decision",
        "",
        "- Decision: research_only_compare_pool_and_rebalance_frequency.",
        "- Recommended next action: inspect the strongest requested variant against turnover and recent-window drawdown before promotion.",
    ]
    (run_folder / "record.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(run_folder: Path, max_workers: int) -> None:
    started = time.time()
    run_folder.mkdir(parents=True, exist_ok=True)
    command_log = run_folder / "command_log.txt"
    with command_log.open("a", encoding="utf-8") as fh:
        fh.write(
            f"\n[{pd.Timestamp.now().isoformat()}] "
            f"python scripts/run_microcap_v2_5_pool_rebalance_frequency_scan.py --run-folder {run_folder} --max-workers {max_workers}\n"
        )

    official_summary, official_v25, official_v20, _reference_summary, base_gross_cached = _load_official_v25()
    trading_dates, schedules, returns_df, caps_by_date, buyable_df, sellable_df, name_map, symbols = _prepare_cache_inputs(
        max_workers,
        pd.DatetimeIndex(base_gross_cached.index),
    )

    summary_rows: list[dict[str, Any]] = []
    wide_rows: list[dict[str, Any]] = []
    sanity_rows: list[dict[str, Any]] = []
    variant_meta: list[dict[str, Any]] = []
    daily_outputs: dict[str, str] = {}

    official = official_v25.copy().sort_index()
    official["candidate"] = "v25_official_top100_biweekly"
    official["top_n"] = 100
    official["rebalance_frequency"] = "biweekly"
    official["variant_source"] = "official_v25_output"
    official_path = run_folder / "daily_v25_official_top100_biweekly.csv"
    official.rename_axis("date").reset_index().to_csv(official_path, index=False, encoding="utf-8")
    daily_outputs["v25_official_top100_biweekly"] = str(official_path)
    rows, wide = _summarize_candidate(
        official,
        "v25_official_top100_biweekly",
        {"top_n": 100, "rebalance_frequency": "biweekly", "variant_source": "official_v25_output"},
    )
    summary_rows.extend(rows)
    wide_rows.append(wide)
    sanity_rows.append(
        {
            "candidate": "v25_official_top100_biweekly",
            "rows": int(len(official)),
            "finite_return_net": bool(np.isfinite(pd.to_numeric(official["return_net"], errors="coerce").fillna(0.0)).all()),
            "max_abs_return_diff_vs_official": 0.0,
        }
    )

    for variant in [item for item in VARIANTS if item["source"] == "local_cache_rebuild"]:
        candidate = str(variant["candidate"])
        out, index_df, turnover_df, info = _build_variant_output(
            candidate=candidate,
            top_n=int(variant["top_n"]),
            rebalance_frequency=str(variant["rebalance_frequency"]),
            schedules=schedules,
            returns_df=returns_df,
            caps_by_date=caps_by_date,
            buyable_df=buyable_df,
            sellable_df=sellable_df,
            name_map=name_map,
            official_index=pd.DatetimeIndex(official_v20.index),
            base_gross_cached=base_gross_cached,
        )
        daily_path = run_folder / f"daily_{candidate}.csv"
        out.rename_axis("date").reset_index().to_csv(daily_path, index=False, encoding="utf-8")
        daily_outputs[candidate] = str(daily_path)
        (run_folder / f"proxy_{candidate}.csv").write_text(index_df.to_csv(index=False), encoding="utf-8")
        (run_folder / f"turnover_{candidate}.csv").write_text(turnover_df.to_csv(index=False), encoding="utf-8")
        variant_meta.append(info)
        rows, wide = _summarize_candidate(
            out,
            candidate,
            {
                "top_n": int(variant["top_n"]),
                "rebalance_frequency": str(variant["rebalance_frequency"]),
                "variant_source": "local_cache_rebuild",
                "rebalance_events": int(len(turnover_df)),
            },
        )
        summary_rows.extend(rows)
        wide_rows.append(wide)
        common = official.index.intersection(out.index)
        max_abs_diff = np.nan
        if len(common):
            max_abs_diff = float(
                pd.to_numeric(out.loc[common, "return_net"], errors="coerce")
                .sub(pd.to_numeric(official.loc[common, "return_net"], errors="coerce"))
                .abs()
                .max()
            )
        sanity_rows.append(
            {
                "candidate": candidate,
                "rows": int(len(out)),
                "finite_return_net": bool(np.isfinite(pd.to_numeric(out["return_net"], errors="coerce").fillna(0.0)).all()),
                "max_abs_return_diff_vs_official": max_abs_diff,
                "proxy_rows": int(len(index_df)),
                "turnover_rows": int(len(turnover_df)),
                "rebalance_frequency": str(variant["rebalance_frequency"]),
                "top_n": int(variant["top_n"]),
            }
        )

    summary = pd.DataFrame(summary_rows)
    wide_df = pd.DataFrame(wide_rows)
    sanity = pd.DataFrame(sanity_rows)
    summary.to_csv(run_folder / "scan_summary.csv", index=False, encoding="utf-8")
    wide_df.to_csv(run_folder / "window_metrics.csv", index=False, encoding="utf-8")
    sanity.to_csv(run_folder / "variant_sanity_checks.csv", index=False, encoding="utf-8")

    rebuilt_diff = float(
        sanity.loc[sanity["candidate"].eq("rebuilt_top100_biweekly"), "max_abs_return_diff_vs_official"].iloc[0]
    )
    context = {
        "metrics_start": str(pd.Timestamp(official.index.min()).date()),
        "metrics_end": str(pd.Timestamp(official.index.max()).date()),
        "trading_start": str(pd.Timestamp(trading_dates.min()).date()),
        "trading_end": str(pd.Timestamp(trading_dates.max()).date()),
        "symbols_loaded": int(len(symbols)),
        "official_latest_nav_date": official_summary.get("latest_nav_date"),
        "rebuilt_baseline_max_abs_return_diff": rebuilt_diff,
        "all_finite_return_net": bool(sanity["finite_return_net"].fillna(False).all()),
        "all_have_rows": bool(sanity["rows"].gt(0).all()),
    }
    meta = {
        "run_id": run_folder.name,
        "created_at": pd.Timestamp.now().isoformat(),
        "phase": "analysis_written",
        "project": "microcap Top100",
        "strategy": "v2.5 derived",
        "subsystem": "pool size and rebalance frequency",
        "repo_root": str(ROOT),
        "entrypoint": "scripts/run_microcap_v2_5_pool_rebalance_frequency_scan.py",
        "git_branch": _git(["branch", "--show-current"]),
        "git_commit": _git(["rev-parse", "HEAD"]),
        "git_status_before": _git(["status", "--short"]),
        "git_status_after": _git(["status", "--short"]),
        "scan_type": "local_cache_rebuild_variant_compare",
        "parameter_group": "top_n_rebalance_frequency",
        "baseline": {
            "candidate": "v25_official_top100_biweekly",
            "strategy_version": "v2.5",
            "top_n": 100,
            "rebalance_frequency": "biweekly",
            "entry_threshold": v25.ENTRY_THRESHOLD,
            "exit_threshold": v25.EXIT_THRESHOLD,
            "target_vol": v25.TARGET_VOL,
            "max_leverage": v25.TARGET_VOL_MAX_LEVERAGE,
            "scale_rebalance_threshold": v25.TARGET_VOL_SCALE_REBALANCE_THRESHOLD,
        },
        "candidate_grid": wide_df["candidate"].tolist(),
        "data_snapshot": {
            **context,
            "schedule_counts": {key: int(len(value)) for key, value in schedules.items()},
        },
        "variant_meta": variant_meta,
        "cost_model": {
            "base": "formal_v2_5_costed",
            "basket_rebalance_one_side": ONE_SIDE_REBALANCE_COST,
            "rebalance_cost_formula": "one_side_cost * ((buys + sells) / top_n)",
            "execution_timing": v25.v2_0.base_mod.EXECUTION_TIMING,
            "trade_constraint_mode": v25.v2_0.base_mod.TRADE_CONSTRAINT_MODE,
            "target_vol": v25.TARGET_VOL,
            "scale_change_cost": v25.TARGET_VOL_SCALE_CHANGE_COST,
            "financing_rate": v25.TARGET_VOL_FINANCING_RATE,
            "hedge_removed": True,
        },
        "verification": {
            "sanity_checks": str(run_folder / "variant_sanity_checks.csv"),
            "rebuilt_baseline_max_abs_return_diff": rebuilt_diff,
            "all_finite_return_net": context["all_finite_return_net"],
            "all_have_rows": context["all_have_rows"],
        },
        "outputs": {
            "record": str(run_folder / "record.md"),
            "scan_summary": str(run_folder / "scan_summary.csv"),
            "window_metrics": str(run_folder / "window_metrics.csv"),
            "scan_meta": str(run_folder / "scan_meta.json"),
            "command_log": str(command_log),
            "variant_sanity_checks": str(run_folder / "variant_sanity_checks.csv"),
            "daily_outputs": daily_outputs,
        },
        "decision": "research_only_compare_pool_and_rebalance_frequency",
        "stability_label": "first_pass_variant_compare",
        "elapsed_sec": round(time.time() - started, 3),
    }
    _write_json(run_folder / "scan_meta.json", meta)
    _write_record(run_folder, wide_df, context, meta)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-folder", type=Path, default=RUN_FOLDER)
    parser.add_argument("--max-workers", type=int, default=8)
    args = parser.parse_args()
    run(Path(args.run_folder), max_workers=max(1, int(args.max_workers)))


if __name__ == "__main__":
    main()
