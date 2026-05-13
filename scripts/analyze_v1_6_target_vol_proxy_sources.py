from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import microcap_top100_mom16_biweekly_live_v1_6 as v16


WINDOWS = {
    "full": None,
    "last_10y": 10,
    "last_5y": 5,
    "last_3y": 3,
    "last_1y": 1,
}


Selector = Callable[[pd.DataFrame, pd.Series], tuple[pd.Series, str]]


def _finite_float(value: object, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return float(default)
    return out if np.isfinite(out) else float(default)


def _series_or_none(out: pd.DataFrame, col: str) -> pd.Series | None:
    if col not in out.columns:
        return None
    series = pd.to_numeric(out[col], errors="coerce")
    return series if series.notna().sum() >= v16.TARGET_VOL_WINDOW else None


def _constructed_spread_return(out: pd.DataFrame) -> pd.Series | None:
    if not {"microcap_close", "hedge_close"}.issubset(out.columns):
        return None
    micro = pd.to_numeric(out["microcap_close"], errors="coerce").pct_change()
    hedge = pd.to_numeric(out["hedge_close"], errors="coerce").pct_change()
    spread = micro - float(v16.BASE_HEDGE_RATIO) * hedge
    return spread.fillna(0.0)


def current_selector(out: pd.DataFrame, fallback: pd.Series) -> tuple[pd.Series, str]:
    for col in ["return_raw", "base_gross_return"]:
        series = _series_or_none(out, col)
        if series is not None:
            return series.fillna(0.0), col
    return fallback, "return_net_fallback_warning"


def proposed_priority_selector(out: pd.DataFrame, fallback: pd.Series) -> tuple[pd.Series, str]:
    for col in ["overlay_pre_cost_return", "base_gross_return"]:
        series = _series_or_none(out, col)
        if series is not None:
            return series.fillna(0.0), col
    spread = _constructed_spread_return(out)
    if spread is not None:
        return spread, "constructed_microcap_minus_hedge"
    return fallback, "return_net_fallback_warning"


def constructed_spread_selector(out: pd.DataFrame, fallback: pd.Series) -> tuple[pd.Series, str]:
    spread = _constructed_spread_return(out)
    if spread is not None:
        return spread, "constructed_microcap_minus_hedge"
    return fallback, "return_net_fallback_warning"


CANDIDATES: dict[str, Selector] = {
    "current_return_raw": current_selector,
    "proposed_overlay_first": proposed_priority_selector,
    "constructed_spread_always": constructed_spread_selector,
}


def build_v1_4_base_for_v1_6() -> pd.DataFrame:
    reference_summary, _base_signal, base_gross_cached, turnover_df = v16.v14_context._load_base_v1_1_context()
    close_df = base_gross_cached[["microcap_close", "hedge_close"]].rename(
        columns={"microcap_close": "microcap", "hedge_close": "hedge"}
    )
    base_gross = v16.v14_context.v1_1_mod.base_mod.run_signal(close_df).sort_index()
    gross = v16.v14_context.v1_1_mod.base_mod.apply_momentum_gap_exit_buffer(
        base_gross,
        v16.V1_6_MOMENTUM_GAP_EXIT_BUFFER,
    )
    base_v1_4 = v16.v14_context.v1_1_mod.base_mod.apply_momentum_gap_peak_decay_derisk(
        gross_result=gross,
        turnover_df=turnover_df,
        decay_ratio_threshold=v16.DECAY_RATIO_THRESHOLD,
        derisk_scale=v16.DERISK_SCALE,
        recovery_ratio_threshold=v16.RECOVERY_RATIO_THRESHOLD,
    )
    base_v1_4 = v16.v14_context.v1_1_mod.base_mod.ensure_overlay_pre_cost_return(base_v1_4)
    base_v1_4.attrs["reference_strategy"] = str(reference_summary.get("strategy", ""))
    return base_v1_4


def run_candidate(base_v1_4: pd.DataFrame, name: str, selector: Selector) -> pd.DataFrame:
    original_selector = v16._select_target_vol_return_source
    try:
        v16._select_target_vol_return_source = selector
        out = v16.apply_target_vol_scaling(base_v1_4)
    finally:
        v16._select_target_vol_return_source = original_selector
    out = out.copy()
    out["candidate"] = name
    return out


def _window_slice(frame: pd.DataFrame, years: int | None) -> pd.DataFrame:
    if years is None:
        return frame
    end = pd.Timestamp(frame.index.max())
    return frame.loc[frame.index >= end - pd.DateOffset(years=years)]


def _metric_row(candidate: str, segment: str, frame: pd.DataFrame) -> dict[str, object]:
    ret = pd.to_numeric(frame["return_net"], errors="coerce").dropna().astype(float)
    summary = v16.summarize_returns(ret)
    scale = pd.to_numeric(frame["current_execution_scale"], errors="coerce").fillna(0.0)
    active = frame["holding"].astype(str).ne("cash")
    max_leverage_days = int(scale.ge(float(v16.TARGET_VOL_MAX_LEVERAGE) - 1e-9).sum())
    rows = int(len(frame))
    return {
        "candidate": candidate,
        "segment": segment,
        "start": summary["start_date"],
        "end": summary["end_date"],
        "rows": rows,
        "ann_return": _finite_float(summary["annual_pct"]) / 100.0,
        "ann_vol": _finite_float(summary["vol_pct"]) / 100.0,
        "sharpe_repo": _finite_float(summary["sharpe"]),
        "max_dd": _finite_float(summary["max_drawdown_pct"]) / 100.0,
        "total_return": _finite_float(summary["total_return_pct"]) / 100.0,
        "final_nav": _finite_float(summary["final_nav"]),
        "avg_weight": _finite_float(scale.mean()),
        "held_day_avg_weight": _finite_float(scale.loc[active].mean()) if bool(active.any()) else 0.0,
        "holding_days": int(active.sum()),
        "holding_day_ratio": _finite_float(active.mean()),
        "avg_turnover": _finite_float(pd.to_numeric(frame["target_vol_costed_turnover"], errors="coerce").mean()),
        "scale_raw": _finite_float(pd.to_numeric(frame["target_vol_scale_raw"], errors="coerce").mean()),
        "final_weight": _finite_float(scale.iloc[-1]),
        "max_scale": _finite_float(scale.max()),
        "max_leverage_days": max_leverage_days,
        "max_leverage_day_ratio": max_leverage_days / rows if rows else 0.0,
        "avg_realized_vol": _finite_float(pd.to_numeric(frame["target_vol_realized_vol"], errors="coerce").mean()),
        "target_vol_return_source": str(frame["target_vol_return_source"].dropna().iloc[-1]),
    }


def build_metrics(candidate_outputs: dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows: list[dict[str, object]] = []
    for candidate, frame in candidate_outputs.items():
        for segment, years in WINDOWS.items():
            part = _window_slice(frame, years)
            if part.empty:
                continue
            rows.append(_metric_row(candidate, segment, part))
    scan_summary = pd.DataFrame(rows)

    wide_rows = []
    for candidate, group in scan_summary.groupby("candidate", sort=False):
        row: dict[str, object] = {
            "candidate": candidate,
            "vol_proxy_source": str(group["target_vol_return_source"].iloc[0]),
            "TARGET_VOL": v16.TARGET_VOL,
            "TARGET_VOL_WINDOW": v16.TARGET_VOL_WINDOW,
            "TARGET_VOL_MAX_LEVERAGE": v16.TARGET_VOL_MAX_LEVERAGE,
        }
        for _, item in group.iterrows():
            segment = str(item["segment"])
            row[f"ann_return_{segment}"] = item["ann_return"]
            row[f"max_dd_{segment}"] = item["max_dd"]
            row[f"sharpe_repo_{segment}"] = item["sharpe_repo"]
            row[f"avg_weight_{segment}"] = item["avg_weight"]
            row[f"holding_day_ratio_{segment}"] = item["holding_day_ratio"]
            row[f"final_nav_{segment}"] = item["final_nav"]
            row[f"max_leverage_day_ratio_{segment}"] = item["max_leverage_day_ratio"]
        wide_rows.append(row)
    window_metrics = pd.DataFrame(wide_rows)
    return scan_summary, window_metrics


def build_daily_comparison(candidate_outputs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    pieces = []
    for candidate, frame in candidate_outputs.items():
        part = frame[
            [
                "return_net",
                "nav_net",
                "current_execution_scale",
                "next_session_target_scale",
                "target_vol_realized_vol",
                "target_vol_return",
                "target_vol_return_source",
                "holding",
                "next_holding",
            ]
        ].copy()
        part.columns = [f"{candidate}_{col}" for col in part.columns]
        pieces.append(part)
    return pd.concat(pieces, axis=1).rename_axis("date").reset_index()


def update_meta(run_folder: Path, base_v1_4: pd.DataFrame, outputs: dict[str, Path]) -> None:
    meta_path = run_folder / "scan_meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    index = pd.DatetimeIndex(base_v1_4.index)
    meta.update(
        {
            "scan_type": "vol_proxy_source_comparison",
            "baseline": {
                "candidate": "current_return_raw",
                "production_selector_priority": ["return_raw", "base_gross_return", "return_net_fallback_warning"],
            },
            "candidate_grid": list(CANDIDATES.keys()),
            "data_snapshot": {
                "source": "rebuilt v1.4 base through official v1.6 context",
                "start": str(index.min().date()),
                "end": str(index.max().date()),
                "rows": int(len(base_v1_4)),
                "duplicate_dates": int(index.duplicated().sum()),
            },
            "cost_model": {
                "target_vol": v16.TARGET_VOL,
                "target_vol_window": v16.TARGET_VOL_WINDOW,
                "max_leverage": v16.TARGET_VOL_MAX_LEVERAGE,
                "scale_change_cost": v16.TARGET_VOL_SCALE_CHANGE_COST,
                "financing_rate": v16.TARGET_VOL_FINANCING_RATE,
                "base_hedge_ratio": v16.BASE_HEDGE_RATIO,
                "pnl_return_source": v16.PNL_RETURN_SOURCE,
            },
        }
    )
    meta.setdefault("outputs", {})
    meta["outputs"].update({key: str(value) for key, value in outputs.items()})
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")


def write_record(run_folder: Path, scan_summary: pd.DataFrame, window_metrics: pd.DataFrame) -> None:
    full = scan_summary.loc[scan_summary["segment"].eq("full")].copy()
    full["ann_return_pct"] = full["ann_return"] * 100.0
    full["max_dd_pct"] = full["max_dd"] * 100.0
    full["avg_weight_pct"] = full["avg_weight"] * 100.0
    table = full[
        [
            "candidate",
            "target_vol_return_source",
            "ann_return_pct",
            "max_dd_pct",
            "sharpe_repo",
            "final_nav",
            "avg_weight_pct",
            "max_leverage_day_ratio",
        ]
    ].to_markdown(index=False, floatfmt=".4f")
    record = f"""# v1.6 Target-Vol Vol-Proxy Source Comparison

## Run Metadata

- Strategy: microcap Top100 v1.6
- Entrypoint: microcap_top100_mom16_biweekly_live_v1_6.py
- Run folder: {run_folder}

## Research Question

Does the current production target-volatility volatility source, return_raw, materially differ from proposed
underlying-risk proxies for realized-vol and leverage sizing?

## Implementation Anchor

The base v1.4 path was rebuilt through the official v1.6 context. The production default was not changed.
Each candidate reuses v1.6 apply_target_vol_scaling with only _select_target_vol_return_source overridden at runtime.

## Data Snapshot

- Start: {scan_summary['start'].min()}
- End: {scan_summary['end'].max()}
- Full rows: {int(scan_summary.loc[scan_summary['segment'].eq('full'), 'rows'].max())}

## Cost and Execution Assumptions

- Target vol: {v16.TARGET_VOL:.2%}
- Window: {v16.TARGET_VOL_WINDOW}
- Max leverage: {v16.TARGET_VOL_MAX_LEVERAGE:.2f}
- Scale-change cost: {v16.TARGET_VOL_SCALE_CHANGE_COST:.4%}
- Financing rate: {v16.TARGET_VOL_FINANCING_RATE:.2%}
- PnL source: {v16.PNL_RETURN_SOURCE}

## Runtime Override Plan

Runtime-only selector overrides were used. No production default selector was changed.

## Commands

- python scripts/analyze_v1_6_target_vol_proxy_sources.py --run-folder {run_folder}

## Output Files

- scan_summary.csv
- window_metrics.csv
- daily_comparison.csv

## Full-Sample Results

{table}

## Window Results

See window_metrics.csv for full/10y/5y/3y/1y wide metrics.

## Stability Classification

Diagnostic comparison only. A production switch still requires user approval and a dedicated signal/NAV migration patch.

## Decision

Do not switch production default in this run.

## User-Facing Summary

This run measures the impact of candidate vol proxies without changing official v1.6 output semantics.
"""
    (run_folder / "record.md").write_text(record, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare v1.6 target-vol realized-vol proxy sources.")
    parser.add_argument(
        "--run-folder",
        type=Path,
        required=True,
        help="Quant-param-scan run folder created by init_quant_param_scan_run.py.",
    )
    args = parser.parse_args()
    run_folder = args.run_folder
    run_folder.mkdir(parents=True, exist_ok=True)

    base_v1_4 = build_v1_4_base_for_v1_6()
    candidate_outputs = {
        name: run_candidate(base_v1_4, name, selector)
        for name, selector in CANDIDATES.items()
    }

    scan_summary, window_metrics = build_metrics(candidate_outputs)
    daily_comparison = build_daily_comparison(candidate_outputs)

    scan_summary_path = run_folder / "scan_summary.csv"
    window_metrics_path = run_folder / "window_metrics.csv"
    daily_path = run_folder / "daily_comparison.csv"
    scan_summary.to_csv(scan_summary_path, index=False, encoding="utf-8-sig")
    window_metrics.to_csv(window_metrics_path, index=False, encoding="utf-8-sig")
    daily_comparison.to_csv(daily_path, index=False, encoding="utf-8-sig")

    outputs = {
        "scan_summary": scan_summary_path,
        "window_metrics": window_metrics_path,
        "daily_comparison": daily_path,
        "record": run_folder / "record.md",
        "scan_meta": run_folder / "scan_meta.json",
        "command_log": run_folder / "command_log.txt",
    }
    update_meta(run_folder, base_v1_4, outputs)
    write_record(run_folder, scan_summary, window_metrics)
    with (run_folder / "command_log.txt").open("a", encoding="utf-8") as f:
        f.write("\npython scripts/analyze_v1_6_target_vol_proxy_sources.py ")
        f.write(f"--run-folder {run_folder}\n")

    print(scan_summary_path)
    print(window_metrics_path)
    print(daily_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
