from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

import microcap_top100_mom16_biweekly_live_v1_1 as v1_1_mod


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"

BASE_SUMMARY_JSON = OUTPUT_DIR / "microcap_top100_mom16_biweekly_live_v1_1_summary.json"
BASE_COSTED_NAV_CSV = OUTPUT_DIR / "microcap_top100_mom16_hedge_zz1000_0p8x_biweekly_thursday_16y_costed_nav.csv"
V1_0_SUMMARY_JSON = OUTPUT_DIR / "microcap_top100_mom16_biweekly_live_summary.json"

BASE_HEDGE_RATIO = 0.8
V1_4_MOMENTUM_GAP_EXIT_BUFFER = 0.0025
DECAY_RATIO_THRESHOLD = 0.25
DERISK_SCALE = 0.0
RECOVERY_RATIO_THRESHOLD = 0.35

base_mod = v1_1_mod.base_mod


@dataclass(frozen=True)
class RealtimeBase:
    context: dict[str, object]
    turnover_df: pd.DataFrame
    reference_summary: dict[str, object]
    meta: dict[str, object]
    realtime_close_df: pd.DataFrame
    base_gross: pd.DataFrame


def csv_safe_meta_value(value: object) -> object:
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, default=str)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def apply_realtime_meta_to_signal_row(signal_row: pd.DataFrame, meta: dict[str, object]) -> None:
    for key, value in meta.items():
        signal_row[key] = csv_safe_meta_value(value)


def build_v1_1_args(max_workers: int = 8) -> argparse.Namespace:
    return argparse.Namespace(
        query_tokens=[],
        panel_path=base_mod.hedge_mod.DEFAULT_PANEL,
        index_csv=base_mod.DEFAULT_INDEX_CSV,
        costed_nav_csv=base_mod.DEFAULT_COSTED_NAV_CSV,
        output_prefix=base_mod.DEFAULT_OUTPUT_PREFIX,
        capital=None,
        max_workers=max_workers,
        realtime_cache_seconds=30,
        rebuild_index_if_missing=True,
        force_refresh=False,
        max_stale_anchor_days=base_mod.DEFAULT_MAX_STALE_ANCHOR_DAYS,
        allow_stale_realtime=False,
    )


def ensure_base_outputs() -> None:
    base_paths = base_mod.build_output_paths(base_mod.DEFAULT_OUTPUT_PREFIX)
    v1_1_mod.prepare_current_v1_1_outputs(paths=base_paths, costed_nav_csv=BASE_COSTED_NAV_CSV)
    if base_paths["proxy_turnover"].exists() and BASE_COSTED_NAV_CSV.exists():
        return
    args = build_v1_1_args()
    resolved_panel_path, target_end_date = base_mod.build_refreshed_panel_shadow(args, base_paths)
    base_mod.ensure_strategy_files(args, base_paths, resolved_panel_path, target_end_date)


def load_reference_summary() -> dict[str, object]:
    if BASE_SUMMARY_JSON.exists():
        try:
            summary = json.loads(BASE_SUMMARY_JSON.read_text(encoding="utf-8"))
            if v1_1_mod.summary_is_current_v1_1(summary):
                return summary
        except Exception:
            pass
    if V1_0_SUMMARY_JSON.exists():
        return json.loads(V1_0_SUMMARY_JSON.read_text(encoding="utf-8"))
    raise FileNotFoundError("Neither current v1.1 summary nor v1.0 reference summary is available.")


def load_realtime_context() -> tuple[dict[str, object], pd.DataFrame, dict[str, object]]:
    ensure_base_outputs()
    args = build_v1_1_args()
    base_paths = base_mod.build_output_paths(base_mod.DEFAULT_OUTPUT_PREFIX)
    panel_path, target_end_date = base_mod.refresh_history_anchor(args, base_paths)
    try:
        base_context = base_mod.ensure_realtime_query_base_context(args, base_paths, panel_path, target_end_date)
    except (FileNotFoundError, ValueError):
        base_context = base_mod.ensure_base_signal_fresh(args, base_paths, panel_path, target_end_date)
    member_context = base_mod.ensure_static_members_fresh(
        args,
        base_paths,
        panel_path,
        target_end_date,
        base_context,
    )
    turnover_df = pd.read_csv(base_paths["proxy_turnover"])
    turnover_df["rebalance_date"] = pd.to_datetime(turnover_df["rebalance_date"], errors="coerce")
    turnover_df = turnover_df.dropna(subset=["rebalance_date"]).sort_values("rebalance_date")
    return member_context, turnover_df, load_reference_summary()


def load_realtime_base() -> RealtimeBase:
    context, turnover_df, reference_summary = load_realtime_context()
    _, meta = base_mod.build_realtime_signal_fast(context)
    snapshot_ts = pd.Timestamp(meta["snapshot_time"])
    realtime_close_df = context["close_df"].copy().sort_index()
    realtime_close_df = base_mod.apply_realtime_close_to_signal_frame(
        close_df=realtime_close_df,
        latest_trade_date=pd.Timestamp(meta["latest_anchor_trade_date"]),
        snapshot_ts=snapshot_ts,
        microcap_rt_close=float(meta["microcap_rt_close"]),
        hedge_rt_close=float(meta["hedge_rt_close"]),
        quote_trade_date=meta.get("quote_trade_date", ""),
    )
    base_gross = base_mod.run_signal(realtime_close_df).sort_index()
    return RealtimeBase(
        context=context,
        turnover_df=turnover_df,
        reference_summary=reference_summary,
        meta=meta,
        realtime_close_df=realtime_close_df,
        base_gross=base_gross,
    )


def build_realtime_overlay_base(realtime_base: RealtimeBase) -> pd.DataFrame:
    gross = base_mod.apply_momentum_gap_exit_buffer(
        realtime_base.base_gross,
        V1_4_MOMENTUM_GAP_EXIT_BUFFER,
    )
    out = base_mod.apply_momentum_gap_peak_decay_derisk(
        gross_result=gross,
        turnover_df=realtime_base.turnover_df,
        decay_ratio_threshold=DECAY_RATIO_THRESHOLD,
        derisk_scale=DERISK_SCALE,
        recovery_ratio_threshold=RECOVERY_RATIO_THRESHOLD,
    )
    return base_mod.ensure_overlay_pre_cost_return(out)
