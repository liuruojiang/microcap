from __future__ import annotations

import json
import math
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd


RUN_DIR = Path(__file__).resolve().parent
REPO = RUN_DIR.parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import microcap_top100_mom16_biweekly_live_v2_0 as v2_0
import microcap_top100_mom16_biweekly_live_v2_3 as v2_3


TARGET_VOLS = [0.10, 0.12, 0.15, 0.18, 0.20, 0.22, 0.25, 0.28, 0.30, 0.32, 0.35]
TRADING_DAYS = int(v2_0.overlay_mod.TARGET_VOL_TRADING_DAYS)
WINDOWS = {
    "full": None,
    "last_10y": pd.DateOffset(years=10),
    "last_5y": pd.DateOffset(years=5),
    "last_3y": pd.DateOffset(years=3),
    "last_1y": pd.DateOffset(years=1),
}


def git_value(args: list[str]) -> str:
    try:
        return subprocess.check_output(args, cwd=REPO, text=True, encoding="utf-8", errors="replace").strip()
    except Exception as exc:
        return f"ERROR: {exc}"


def label_target_vol(value: float) -> str:
    return f"tv{int(round(value * 100)):02d}"


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
    active = frame.loc[ret.index, "holding"].astype(str).ne("cash")
    scale = pd.to_numeric(frame.loc[ret.index, "execution_scale"], errors="coerce").fillna(0.0)
    raw_scale = pd.to_numeric(frame.loc[ret.index, "target_vol_scale_raw"], errors="coerce")
    cost_cols = [
        col
        for col in ["total_cost", "scale_change_cost", "financing_cost", "base_trade_cost_scaled"]
        if col in frame.columns
    ]
    cost_total = float(
        sum(pd.to_numeric(frame.loc[ret.index, col], errors="coerce").fillna(0.0).sum() for col in cost_cols)
    )
    clipped_days = int(raw_scale.ge(float(v2_0.overlay_mod.TARGET_VOL_MAX_LEVERAGE) - 1e-12).sum())
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
        "avg_execution_scale": float(scale.mean()),
        "max_execution_scale": float(scale.max()),
        "cap_clipped_days": clipped_days,
        "cap_clipped_ratio": float(clipped_days / len(ret)),
        "avg_target_vol_turnover": float(
            pd.to_numeric(frame.loc[ret.index, "target_vol_costed_turnover"], errors="coerce").fillna(0.0).mean()
        ),
        "cost_total": cost_total,
        "cost_days": int(
            pd.to_numeric(frame.loc[ret.index, cost_cols].sum(axis=1), errors="coerce").fillna(0.0).gt(0).sum()
        )
        if cost_cols
        else 0,
    }


def wide_window_metrics(summary: pd.DataFrame) -> pd.DataFrame:
    value_cols = [
        "ann_return",
        "ann_vol",
        "sharpe_repo",
        "max_dd",
        "holding_day_ratio",
        "avg_execution_scale",
        "max_execution_scale",
        "cap_clipped_ratio",
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
        "vol_window",
        "max_leverage",
        "scale_threshold",
        "execution_hedge_ratio",
        "signal_spread_hedge_ratio",
        "financing_rate",
    ]
    pivot = summary.groupby([*index_cols, "segment"], dropna=False)[value_cols].first().unstack("segment")
    pivot.columns = [f"{metric}_{segment}" for metric, segment in pivot.columns]
    return pivot.reset_index()


def build_base_costed() -> tuple[dict[str, object], dict[str, object], pd.DataFrame, pd.DataFrame]:
    official_summary, _official_signal, official_out = v2_3.generate_v2_3_outputs()
    reference_summary, base_gross_cached, turnover_df = v2_0.embedded_context._load_embedded_base_context()
    close_df = base_gross_cached[["microcap_close", "hedge_close"]].rename(
        columns={"microcap_close": "microcap", "hedge_close": "hedge"}
    ).sort_index()
    common_index = v2_3.build_v2_3_common_index(close_df, official_index=official_out.index)
    gross = v2_3.build_spread_log_wls_gross(close_df, common_index)
    buffered = v2_0.base_mod.apply_momentum_gap_exit_buffer(gross, float(v2_3.MOMENTUM_GAP_EXIT_BUFFER))
    base_costed = v2_0.base_mod.apply_momentum_gap_no_peak_decay_cost_model(buffered, turnover_df)
    base_costed = base_costed.loc[pd.DatetimeIndex(official_out.index.intersection(base_costed.index))].copy()
    if base_costed.empty:
        raise RuntimeError("empty v2.3 base costed frame")
    return official_summary, reference_summary, base_costed, turnover_df


def main() -> None:
    started = time.time()
    command = "python run_v23_target_vol_scan.py"
    with (RUN_DIR / "command_log.txt").open("a", encoding="utf-8") as f:
        f.write("\n# v2.3 target-vol level scan\n")
        f.write(command + "\n")

    print("refreshing official v2.3 outputs before target-vol scan...", flush=True)
    official_summary, reference_summary, base_costed, turnover_df = build_base_costed()

    rows: list[dict[str, object]] = []
    for target_vol in TARGET_VOLS:
        out = v2_3.apply_target_vol(base_costed, target_vol=float(target_vol))
        params = {
            "candidate": f"v2_3_{label_target_vol(target_vol)}",
            "version": "v2.3",
            "lookback": int(v2_3.LOOKBACK),
            "halflife": float(v2_3.HALFLIFE),
            "exit_buffer": float(v2_3.MOMENTUM_GAP_EXIT_BUFFER),
            "target_vol": float(target_vol),
            "vol_window": int(v2_0.overlay_mod.TARGET_VOL_WINDOW),
            "max_leverage": float(v2_0.overlay_mod.TARGET_VOL_MAX_LEVERAGE),
            "scale_threshold": float(v2_3.TARGET_VOL_SCALE_REBALANCE_THRESHOLD),
            "execution_hedge_ratio": float(v2_3.EXECUTION_HEDGE_RATIO),
            "signal_spread_hedge_ratio": float(v2_3.SIGNAL_SPREAD_HEDGE_RATIO),
            "financing_rate": float(v2_0.overlay_mod.TARGET_VOL_FINANCING_RATE),
        }
        for segment, offset in WINDOWS.items():
            row = dict(params)
            row.update(metrics_for_segment(out, segment, offset))
            rows.append(row)

    summary = pd.DataFrame(rows)
    window = wide_window_metrics(summary)
    ranking = window.sort_values(
        ["sharpe_repo_full", "ann_return_full", "max_dd_full"],
        ascending=[False, False, False],
    )
    summary.to_csv(RUN_DIR / "scan_summary.csv", index=False, encoding="utf-8")
    window.to_csv(RUN_DIR / "window_metrics.csv", index=False, encoding="utf-8")
    ranking.to_csv(RUN_DIR / "candidate_ranking.csv", index=False, encoding="utf-8")

    meta_path = RUN_DIR / "scan_meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    meta.update(
        {
            "phase": "scan_complete",
            "scan_type": "single_parameter_target_vol_level_scan",
            "source_change_rule": "research_only_no_source_change",
            "baseline": {
                "candidate": f"v2_3_{label_target_vol(float(v2_3.TARGET_VOL))}",
                "lookback": int(v2_3.LOOKBACK),
                "halflife": float(v2_3.HALFLIFE),
                "exit_buffer": float(v2_3.MOMENTUM_GAP_EXIT_BUFFER),
                "target_vol": float(v2_3.TARGET_VOL),
                "vol_window": int(v2_0.overlay_mod.TARGET_VOL_WINDOW),
                "max_leverage": float(v2_0.overlay_mod.TARGET_VOL_MAX_LEVERAGE),
                "min_leverage": float(v2_0.overlay_mod.TARGET_VOL_MIN_LEVERAGE),
                "scale_threshold": float(v2_3.TARGET_VOL_SCALE_REBALANCE_THRESHOLD),
                "execution_hedge_ratio": float(v2_3.EXECUTION_HEDGE_RATIO),
                "signal_spread_hedge_ratio": float(v2_3.SIGNAL_SPREAD_HEDGE_RATIO),
                "financing_rate": float(v2_0.overlay_mod.TARGET_VOL_FINANCING_RATE),
            },
            "candidate_grid": {"target_vol": TARGET_VOLS, "candidate_count": len(TARGET_VOLS)},
            "data_snapshot": {
                "scan_start": str(pd.Timestamp(base_costed.index[0]).date()),
                "scan_end": str(pd.Timestamp(base_costed.index[-1]).date()),
                "scan_rows": int(len(base_costed)),
                "turnover_rows": int(len(turnover_df)),
                "official_v2_3_latest_nav_date": official_summary.get("latest_nav_date", ""),
                "reference_latest_trade_date": reference_summary.get("latest_trade_date", ""),
            },
            "cost_model": {
                "base_cost_model": "v2_0.base_mod.apply_momentum_gap_no_peak_decay_cost_model",
                "target_vol_model": "v2_3.apply_target_vol -> v2_0.overlay_mod.apply_target_vol_scaling",
                "target_vol_scale_change_cost": float(v2_0.overlay_mod.TARGET_VOL_SCALE_CHANGE_COST),
                "target_vol_financing_rate": float(v2_0.overlay_mod.TARGET_VOL_FINANCING_RATE),
                "idle_cash_yield": float(v2_0.overlay_mod.IDLE_CASH_YIELD),
                "futures_drag_daily": float(v2_0.base_mod.FUTURES_DRAG),
                "execution_timing": "close-confirmed signal; next row holding applies",
                "return_column": "return_net",
            },
            "outputs": {
                "record": "record.md",
                "scan_summary": "scan_summary.csv",
                "window_metrics": "window_metrics.csv",
                "candidate_ranking": "candidate_ranking.csv",
                "scan_meta": "scan_meta.json",
                "command_log": "command_log.txt",
            },
            "git_status_after": git_value(["git", "status", "--short"]),
            "elapsed_seconds": round(time.time() - started, 3),
        }
    )
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    best_sharpe = ranking.iloc[0].to_dict()
    default_row = window.loc[window["target_vol"].eq(float(v2_3.TARGET_VOL))].iloc[0].to_dict()
    with (RUN_DIR / "record.md").open("a", encoding="utf-8") as f:
        f.write("\n## Scan Completion\n\n")
        f.write(f"- Command: `{command}`\n")
        f.write(f"- Common window: `{base_costed.index[0].date()}` to `{base_costed.index[-1].date()}`.\n")
        f.write(f"- Official v2.3 latest nav date after refresh: `{official_summary.get('latest_nav_date', '')}`.\n")
        f.write("- Fixed parameters: official v2.3 log-WLS signal, gap buffer, cost model, 60-day realized-vol window, 1.5x max leverage, 30% scale rebalance threshold.\n")
        f.write(f"- Best full-sample Sharpe row: `{best_sharpe['candidate']}`.\n")
        f.write(f"- Current default row: `{default_row['candidate']}`.\n")

    print(f"wrote {RUN_DIR / 'scan_summary.csv'}", flush=True)
    print(f"wrote {RUN_DIR / 'window_metrics.csv'}", flush=True)
    print(f"wrote {RUN_DIR / 'candidate_ranking.csv'}", flush=True)
    print(f"elapsed_seconds={time.time() - started:.1f}", flush=True)


if __name__ == "__main__":
    main()
