from __future__ import annotations

import argparse
import json
import math
import re
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import matplotlib
import numpy as np
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt

import microcap_top100_mom16_biweekly_live_v2_3 as v23
import microcap_top100_mom16_biweekly_live_v2_5 as v25


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"

OUTPUT_PREFIX = "microcap_top100_mom16_biweekly_live_v2_6"
DEFAULT_OUTPUT_PREFIX = OUTPUT_PREFIX
SUMMARY_JSON = OUTPUT_DIR / f"{OUTPUT_PREFIX}_summary.json"
LATEST_SIGNAL_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_latest_signal.csv"
NAV_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_nav.csv"
COSTED_NAV_CSV = OUTPUT_DIR / "microcap_top100_mom16_v23_v25_75_75_tv26_maxlev1_v2_6_costed_nav.csv"
DEFAULT_COSTED_NAV_CSV = COSTED_NAV_CSV
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

VERSION = "2.6"
EXPECTED_VERSION_ROLE = "v23_v25_75_75_tv26_maxlev1_combo"
EXPECTED_VERSION_NOTE_PREFIX = "Formal v2.6 75/75 v2.3 plus v2.5 combo."
SOURCE_TARGET_VOL = 0.26
SOURCE_MAX_LEVERAGE = 1.0
WEIGHT_V23 = 0.75
WEIGHT_V25 = 0.75
GROSS_WEIGHT = WEIGHT_V23 + WEIGHT_V25
COMBO_FINANCING_RATE = 0.03
TRADING_DAYS = 244
FORMAL_START_DATE = pd.Timestamp("2010-05-05")


def parse_v2_6_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Top100 Mom16 Biweekly v2.6 75/75 v2.3+v2.5 combo")
    parser.add_argument("query_tokens", nargs="*", help="signal / performance query")
    parser.add_argument("--panel-path", type=Path, default=None)
    parser.add_argument("--index-csv", type=Path, default=None)
    parser.add_argument("--base-costed-nav-csv", type=Path, default=None)
    parser.add_argument("--v26-costed-nav-csv", "--costed-nav-csv", dest="v26_costed_nav_csv", type=Path, default=None)
    parser.add_argument("--v26-output-prefix", "--output-prefix", dest="v26_output_prefix", default=None)
    parser.add_argument("--base-output-prefix", default=None)
    parser.add_argument("--capital", type=float, default=None)
    parser.add_argument("--max-workers", type=int, default=8)
    parser.add_argument("--realtime-cache-seconds", type=int, default=v23.v2_0.DEFAULT_REALTIME_CACHE_SECONDS)
    parser.add_argument("--allow-stale-realtime", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--bootstrap-deps", action="store_true")
    parser.add_argument("--wheelhouse", type=Path, default=None)
    return parser.parse_args(argv)


def configure_output_paths(output_prefix: str | None = None, costed_nav_csv: Path | None = None) -> None:
    global OUTPUT_PREFIX
    global SUMMARY_JSON, LATEST_SIGNAL_CSV, NAV_CSV, COSTED_NAV_CSV
    global PERF_SUMMARY_CSV, PERF_YEARLY_CSV, PERF_NAV_CSV, PERF_JSON, PERF_PNG
    global PERF_QUERY_SUMMARY_CSV, PERF_QUERY_YEARLY_CSV, PERF_QUERY_NAV_CSV, PERF_QUERY_JSON, PERF_QUERY_PNG

    OUTPUT_PREFIX = str(output_prefix or DEFAULT_OUTPUT_PREFIX)
    SUMMARY_JSON = OUTPUT_DIR / f"{OUTPUT_PREFIX}_summary.json"
    LATEST_SIGNAL_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_latest_signal.csv"
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
        output_prefix=getattr(args, "v26_output_prefix", None),
        costed_nav_csv=getattr(args, "v26_costed_nav_csv", None),
    )
    v23.v2_0._V2_RUNTIME_ARGS = argparse.Namespace(
        query_tokens=[],
        panel_path=getattr(args, "panel_path", None),
        index_csv=getattr(args, "index_csv", None),
        costed_nav_csv=getattr(args, "base_costed_nav_csv", None),
        output_prefix=getattr(args, "base_output_prefix", None),
        capital=getattr(args, "capital", None),
        max_workers=getattr(args, "max_workers", 8),
        realtime_cache_seconds=getattr(args, "realtime_cache_seconds", v23.v2_0.DEFAULT_REALTIME_CACHE_SECONDS),
        allow_stale_realtime=getattr(args, "allow_stale_realtime", False),
        bootstrap_deps=getattr(args, "bootstrap_deps", False),
        wheelhouse=getattr(args, "wheelhouse", None),
    )


def v2_6_output_lock(
    wait_timeout_seconds: float = v23.v2_0.DEFAULT_V2_LOCK_WAIT_SECONDS,
    stale_lock_seconds: float = v23.v2_0.DEFAULT_V2_STALE_LOCK_SECONDS,
):
    return v23.v2_0._v2_file_lock(
        f"{OUTPUT_PREFIX}_generation.lock",
        wait_timeout_seconds=wait_timeout_seconds,
        stale_lock_seconds=stale_lock_seconds,
    )


@contextmanager
def _patched_attr(obj: object, name: str, value: object) -> Iterator[None]:
    old = getattr(obj, name)
    setattr(obj, name, value)
    try:
        yield
    finally:
        setattr(obj, name, old)


@contextmanager
def _patched_function_global(func: object, name: str, value: object) -> Iterator[None]:
    globals_dict = getattr(func, "__globals__")
    old = globals_dict[name]
    globals_dict[name] = value
    try:
        yield
    finally:
        globals_dict[name] = old


def ensure_output_dir() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


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


def _json_dumps(payload: object) -> str:
    return json.dumps(_json_safe(payload), ensure_ascii=False, indent=2, allow_nan=False, default=str)


def _combo_holding(left: pd.Series, right: pd.Series, prefix_left: str, prefix_right: str) -> pd.Series:
    left_text = left.fillna("cash").astype(str)
    right_text = right.fillna("cash").astype(str)
    values: list[str] = []
    for left_value, right_value in zip(left_text, right_text, strict=True):
        parts = []
        if left_value != "cash":
            parts.append(f"{prefix_left}_{left_value}")
        if right_value != "cash":
            parts.append(f"{prefix_right}_{right_value}")
        values.append("__".join(parts) if parts else "cash")
    return pd.Series(values, index=left.index, dtype=object)


def _numeric_column(frame: pd.DataFrame, column: str, default: float = 0.0) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(float(default), index=frame.index, dtype=float)
    return pd.to_numeric(frame[column], errors="coerce").fillna(default).astype(float)


def build_v2_6_combo(
    sleeve_v23: pd.DataFrame,
    sleeve_v25: pd.DataFrame,
    *,
    w_v23: float = WEIGHT_V23,
    w_v25: float = WEIGHT_V25,
    financing_rate: float = COMBO_FINANCING_RATE,
    trading_days: int = TRADING_DAYS,
) -> pd.DataFrame:
    left = sleeve_v23.copy().sort_index()
    right = sleeve_v25.copy().sort_index()
    common_index = pd.DatetimeIndex(left.index).intersection(pd.DatetimeIndex(right.index)).sort_values()
    common_index = common_index[common_index >= FORMAL_START_DATE]
    if len(common_index) == 0:
        raise RuntimeError("v2.6 combo has no common source dates")
    left = left.loc[common_index]
    right = right.loc[common_index]
    ret_v23 = _numeric_column(left, "return_net")
    ret_v25 = _numeric_column(right, "return_net")
    gross_weight = float(w_v23) + float(w_v25)
    combo_financing_cost = max(gross_weight - 1.0, 0.0) * float(financing_rate) / float(trading_days)
    return_before_financing = float(w_v23) * ret_v23 + float(w_v25) * ret_v25
    return_net = return_before_financing - combo_financing_cost
    scale_v23 = _numeric_column(left, "current_execution_scale")
    scale_v25 = _numeric_column(right, "current_execution_scale")
    next_scale_v23 = _numeric_column(left, "next_session_actionable_scale", default=scale_v23.iloc[-1])
    next_scale_v25 = _numeric_column(right, "next_session_actionable_scale", default=scale_v25.iloc[-1])
    out = pd.DataFrame(index=common_index)
    out["return_v23"] = ret_v23
    out["return_v25"] = ret_v25
    out["w_v23"] = float(w_v23)
    out["w_v25"] = float(w_v25)
    out["gross_weight"] = gross_weight
    out["return_before_combo_financing"] = return_before_financing
    out["combo_financing_cost"] = combo_financing_cost
    out["return_net"] = return_net
    out["nav_net"] = (1.0 + out["return_net"].fillna(0.0)).cumprod()
    out["return"] = out["return_net"]
    out["nav"] = out["nav_net"]
    out["v23_current_execution_scale"] = scale_v23
    out["v25_current_execution_scale"] = scale_v25
    out["v23_next_session_actionable_scale"] = next_scale_v23
    out["v25_next_session_actionable_scale"] = next_scale_v25
    out["actual_portfolio_exposure"] = float(w_v23) * scale_v23 + float(w_v25) * scale_v25
    out["current_execution_scale"] = out["actual_portfolio_exposure"]
    out["next_session_portfolio_exposure"] = float(w_v23) * next_scale_v23 + float(w_v25) * next_scale_v25
    out["next_session_actionable_scale"] = out["next_session_portfolio_exposure"]
    out["holding"] = _combo_holding(left["holding"], right["holding"], "v23", "v25")
    out["next_holding"] = _combo_holding(left["next_holding"], right["next_holding"], "v23", "v25")
    out["v23_holding"] = left["holding"].astype(str)
    out["v25_holding"] = right["holding"].astype(str)
    out["v23_next_holding"] = left["next_holding"].astype(str)
    out["v25_next_holding"] = right["next_holding"].astype(str)
    for source, frame in [("v23", left), ("v25", right)]:
        for column in ["annualized_log_wls_score", "log_wls_r2", "momentum_gap", "microcap_mom"]:
            if column in frame.columns:
                out[f"{source}_{column}"] = frame[column]
    out["version"] = VERSION
    out["base_version"] = "embedded_v2_base"
    out["overlay_type"] = EXPECTED_VERSION_ROLE
    out["source_target_vol"] = SOURCE_TARGET_VOL
    out["source_max_leverage"] = SOURCE_MAX_LEVERAGE
    out["combo_financing_rate"] = float(financing_rate)
    out["combo_trading_days"] = int(trading_days)
    return out


def _build_v2_3_source_sleeve(
    close_df: pd.DataFrame,
    turnover_df: pd.DataFrame,
    official_index: pd.DatetimeIndex,
) -> pd.DataFrame:
    common_index = v23.build_v2_3_common_index(close_df, official_index)
    gross = v23.build_spread_log_wls_gross(close_df, common_index)
    buffered = v23.v2_0.base_mod.apply_momentum_gap_exit_buffer(gross, v23.MOMENTUM_GAP_EXIT_BUFFER)
    costed = v23.v2_0.base_mod.apply_momentum_gap_no_peak_decay_cost_model(buffered, turnover_df)
    with _patched_function_global(v23.v2_0.overlay_mod.apply_target_vol_scaling, "TARGET_VOL_MAX_LEVERAGE", SOURCE_MAX_LEVERAGE):
        out = v23.apply_target_vol(costed, SOURCE_TARGET_VOL)
    out = out.copy()
    out["v2_6_source_version"] = "v2.3"
    return out


def _build_v2_5_source_sleeve(
    close_df: pd.DataFrame,
    turnover_df: pd.DataFrame,
    official_index: pd.DatetimeIndex,
) -> pd.DataFrame:
    common_index = v25.build_v2_5_common_index(close_df, official_index)
    gross = v25.build_microcap_log_wls_gross(close_df, common_index)
    costed = v25.apply_cost(gross, turnover_df)
    with _patched_attr(v25, "TARGET_VOL_MAX_LEVERAGE", SOURCE_MAX_LEVERAGE):
        out = v25.apply_target_vol(costed, SOURCE_TARGET_VOL)
    out = out.copy()
    out["v2_6_source_version"] = "v2.5"
    return out


def build_v2_6_result() -> tuple[dict[str, object], pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    official_v2_0_out = v23._load_official_v2_0_out()
    reference_summary, base_gross_cached, turnover_df = v23.v2_0.embedded_context._load_embedded_base_context()
    close_df = v23._close_df_from_base(base_gross_cached)
    sleeve_v23 = _build_v2_3_source_sleeve(close_df, turnover_df, pd.DatetimeIndex(official_v2_0_out.index))
    sleeve_v25 = _build_v2_5_source_sleeve(close_df, turnover_df, pd.DatetimeIndex(official_v2_0_out.index))
    out = build_v2_6_combo(sleeve_v23, sleeve_v25)
    summary = {
        "reference_summary": reference_summary,
        "official_v2_0_rows": int(len(official_v2_0_out)),
        "official_v2_0_start": str(pd.Timestamp(official_v2_0_out.index.min()).date()),
        "official_v2_0_end": str(pd.Timestamp(official_v2_0_out.index.max()).date()),
        "v23_rows": int(len(sleeve_v23)),
        "v23_start": str(pd.Timestamp(sleeve_v23.index.min()).date()),
        "v23_end": str(pd.Timestamp(sleeve_v23.index.max()).date()),
        "v25_rows": int(len(sleeve_v25)),
        "v25_start": str(pd.Timestamp(sleeve_v25.index.min()).date()),
        "v25_end": str(pd.Timestamp(sleeve_v25.index.max()).date()),
        "combo_rows": int(len(out)),
        "combo_start": str(pd.Timestamp(out.index.min()).date()),
        "combo_end": str(pd.Timestamp(out.index.max()).date()),
    }
    return summary, sleeve_v23, sleeve_v25, out


V2_6_REWRITE_AUDIT_KEY_COLUMNS = [
    "return_net",
    "holding",
    "next_holding",
    "return_v23",
    "return_v25",
    "actual_portfolio_exposure",
    "combo_financing_cost",
]


def build_performance_payload(ret: pd.Series, source_label: str = "costed_v2_6") -> dict[str, object]:
    ensure_output_dir()
    summary = v23.summarize_returns(ret)
    yearly_df = v23.summarize_yearly(ret)
    nav_df = pd.DataFrame(
        {
            "date": ret.index,
            "return_net": ret.values,
            "nav_net": (1.0 + ret.fillna(0.0)).cumprod().values,
        }
    )
    v23._atomic_write_csv(yearly_df, PERF_YEARLY_CSV, index=False, encoding="utf-8-sig")
    v23._atomic_write_csv(nav_df, PERF_NAV_CSV, index=False, encoding="utf-8-sig")
    v23._atomic_write_csv(pd.DataFrame([summary]), PERF_SUMMARY_CSV, index=False, encoding="utf-8-sig")
    plt.figure(figsize=(12, 6))
    try:
        plt.plot(nav_df["date"], nav_df["nav_net"], label="v2.6 nav_net")
        plt.title("Top100 Microcap Mom16 v2.6 75/75 Combo Costed NAV")
        plt.xlabel("date")
        plt.ylabel("nav_net")
        plt.grid(True, alpha=0.3)
        plt.legend()
        plt.tight_layout()
        plt.savefig(PERF_PNG, dpi=150)
    finally:
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
    v23._atomic_write_text(PERF_JSON, _json_dumps(payload), encoding="utf-8")
    return payload


def _build_signal_row(net_df: pd.DataFrame, reference_summary: dict[str, object]) -> pd.DataFrame:
    row = v23.v2_0.overlay_mod._build_signal_row(net_df, reference_summary)
    row["version"] = VERSION
    row["strategy_version"] = f"v{VERSION}"
    row["base_version"] = "embedded_v2_base"
    row["overlay_type"] = EXPECTED_VERSION_ROLE
    row["signal_model"] = "combo_75_75_of_v2_3_spread_nav_and_v2_5_microcap_only"
    row["source_v23_target_vol"] = SOURCE_TARGET_VOL
    row["source_v25_target_vol"] = SOURCE_TARGET_VOL
    row["source_max_leverage"] = SOURCE_MAX_LEVERAGE
    row["w_v23"] = WEIGHT_V23
    row["w_v25"] = WEIGHT_V25
    row["gross_weight"] = GROSS_WEIGHT
    row["combo_financing_rate"] = COMBO_FINANCING_RATE
    row["combo_financing_cost_daily"] = max(GROSS_WEIGHT - 1.0, 0.0) * COMBO_FINANCING_RATE / TRADING_DAYS
    latest = net_df.iloc[-1]
    for column in [
        "return_v23",
        "return_v25",
        "v23_current_execution_scale",
        "v25_current_execution_scale",
        "actual_portfolio_exposure",
        "v23_annualized_log_wls_score",
        "v25_annualized_log_wls_score",
        "v23_log_wls_r2",
        "v25_log_wls_r2",
    ]:
        if column in latest and pd.notna(latest[column]):
            row[column] = float(latest[column])
    return row


def _generate_v2_6_outputs_unlocked() -> tuple[dict[str, object], pd.DataFrame, pd.DataFrame]:
    ensure_output_dir()
    data_snapshot, _sleeve_v23, _sleeve_v25, out = build_v2_6_result()
    if COSTED_NAV_CSV.exists():
        previous = pd.read_csv(COSTED_NAV_CSV, parse_dates=["date"])
        v23.v2_0.base_mod.assert_no_historical_rewrite(
            previous=previous,
            candidate=out.rename_axis("date").reset_index(),
            key_columns=V2_6_REWRITE_AUDIT_KEY_COLUMNS,
            allowed_tail_rows=80,
            label="v2.6 official costed NAV",
            audit_path=OUTPUT_DIR / f"{OUTPUT_PREFIX}_historical_rewrite_audit.csv",
        )
    v23._atomic_write_csv(out, COSTED_NAV_CSV, index_label="date", encoding="utf-8-sig")
    v23._atomic_write_csv(out.rename_axis("date").reset_index(), NAV_CSV, index=False, encoding="utf-8-sig")
    reference_summary = dict(data_snapshot["reference_summary"])
    signal_row = _build_signal_row(out, reference_summary)
    v23._atomic_write_text(LATEST_SIGNAL_CSV, signal_row.to_csv(index=False), encoding="utf-8")
    perf_payload = build_performance_payload(out["return_net"].fillna(0.0), source_label="costed_v2_6")
    data_lineage = dict(v23.v2_0.overlay_mod._build_v2_data_lineage())
    data_lineage["v2_6_data_snapshot"] = {k: v for k, v in data_snapshot.items() if k != "reference_summary"}
    summary = dict(reference_summary)
    summary["strategy"] = OUTPUT_PREFIX
    summary["version"] = VERSION
    summary["version_role"] = EXPECTED_VERSION_ROLE
    summary["version_note"] = (
        "Formal v2.6 75/75 v2.3 plus v2.5 combo. Rebuilds v2.3 and v2.5 source sleeves from the same refreshed "
        "embedded v2.0 base, forces both source sleeves to 26% target volatility and max 1.0x leverage, allocates "
        "75% capital to each sleeve, and deducts portfolio-level financing at 3% annualized on gross exposure above 1.0x."
    )
    summary.setdefault("core_params", {})
    summary["core_params"]["source_sleeves"] = {
        "v2.3": {
            "weight": WEIGHT_V23,
            "target_vol": SOURCE_TARGET_VOL,
            "max_leverage": SOURCE_MAX_LEVERAGE,
            "source_model": "v2.3 spread-NAV log-WLS signal, execution hedge 0.8x",
        },
        "v2.5": {
            "weight": WEIGHT_V25,
            "target_vol": SOURCE_TARGET_VOL,
            "max_leverage": SOURCE_MAX_LEVERAGE,
            "source_model": "v2.5 microcap-only log-WLS entry/exit threshold signal",
        },
    }
    summary["core_params"]["combo"] = {
        "gross_weight": GROSS_WEIGHT,
        "portfolio_financing_rate": COMBO_FINANCING_RATE,
        "portfolio_financing_rule": "max(w_v23 + w_v25 - 1, 0) * 3% / 244 deducted daily",
        "extra_rebalance_cost_between_sleeves": 0.0,
        "trading_days": TRADING_DAYS,
    }
    summary["latest_trade_date"] = str(pd.Timestamp(signal_row.iloc[0]["date"]).date())
    summary["latest_nav_date"] = str(pd.Timestamp(out.index.max()).date())
    summary["latest_signal"] = signal_row.iloc[0].drop(labels=["date"], errors="ignore").to_dict()
    summary["data_lineage"] = data_lineage
    summary["performance_source_label"] = "costed_v2_6"
    summary["performance_snapshot"] = perf_payload["summary"]
    summary["base_fingerprint"] = {
        "strategy_version": VERSION,
        "source_v23": v23.current_base_fingerprint(),
        "source_v25": v25.current_base_fingerprint(),
        "combo_weights": {"v23": WEIGHT_V23, "v25": WEIGHT_V25},
        "combo_financing_rate": COMBO_FINANCING_RATE,
    }
    v23._atomic_write_text(SUMMARY_JSON, _json_dumps(summary), encoding="utf-8")
    return summary, signal_row, out


def generate_v2_6_outputs() -> tuple[dict[str, object], pd.DataFrame, pd.DataFrame]:
    with v2_6_output_lock():
        return _generate_v2_6_outputs_unlocked()


def _print_signal_query() -> None:
    _, signal_df, _ = generate_v2_6_outputs()
    row = signal_df.iloc[0]
    print("signal")
    print("version: v2.6")
    print("signal_model: 75/75 combo of v2.3(tv26,max1) and v2.5(tv26,max1)")
    print(f"signal_date: {pd.Timestamp(row['date']).strftime('%Y-%m-%d')}")
    print(f"current_holding: {row.get('current_holding', '')}")
    print(f"next_holding: {row.get('next_holding', '')}")
    print(f"trade_state: {row.get('trade_state', '')}")
    print(f"current_execution_scale: {float(row.get('current_execution_scale', 0.0)):.6f}")
    print(f"next_session_actionable_scale: {float(row.get('next_session_actionable_scale', 0.0)):.6f}")
    print(f"gross_weight: {GROSS_WEIGHT:.2f}")


def _print_performance_query(query: str) -> None:
    _summary, _signal_row, perf_df = _generate_v2_6_outputs_unlocked()
    v23.v2_0.embedded_context.base_mod.build_performance_outputs(
        perf_df.reset_index(names="date"),
        ret_col="return_net",
        nav_col="nav_net",
        source_label="costed_v2_6",
        query_text=query,
        paths={
            "performance_summary": PERF_QUERY_SUMMARY_CSV,
            "performance_yearly": PERF_QUERY_YEARLY_CSV,
            "performance_nav": PERF_QUERY_NAV_CSV,
            "performance_json": PERF_QUERY_JSON,
            "performance_chart": PERF_QUERY_PNG,
        },
    )
    summary_df = pd.read_csv(PERF_QUERY_SUMMARY_CSV)
    row = summary_df.iloc[0]
    print("performance")
    print(f"version: v2.6")
    print(f"query: {query}")
    for col in summary_df.columns:
        print(f"{col}: {row[col]}")


def normalize_v2_6_query_text(query: str) -> str:
    text = str(query or "").strip()
    compact = re.sub(r"\s+", "", text)
    ascii_key = re.sub(r"[\s_-]+", "_", text.lower())
    if compact in {"信号", "信號"} or ascii_key == "signal":
        return "signal"
    if compact in {"实时信号", "實時信號"} or ascii_key in {"realtime_signal", "live_signal"}:
        return "realtime_signal"
    return text


def _handle_query(query: str) -> None:
    normalized = normalize_v2_6_query_text(query)
    if normalized == "signal":
        _print_signal_query()
        return
    if normalized == "realtime_signal":
        raise RuntimeError("v2.6 realtime signal is not wired yet; use close-confirmed signal or request realtime wiring.")
    if v23.v2_0.embedded_context.base_mod.PERFORMANCE_PATTERN.search(query) or "performance" in normalized.lower():
        _print_performance_query(query)
        return
    raise SystemExit(f"Unsupported v2.6 query: {query}")


def main(argv: list[str] | None = None) -> None:
    args = parse_v2_6_args(argv)
    configure_runtime(args)
    query = " ".join(args.query_tokens).strip()
    if query:
        _handle_query(query)
    else:
        start = time.perf_counter()
        summary, _signal, out = generate_v2_6_outputs()
        elapsed = time.perf_counter() - start
        print(f"wrote {COSTED_NAV_CSV}")
        print(f"version: v{summary['version']}")
        print(f"rows: {len(out)}")
        print(f"date_range: {out.index.min().date()} to {out.index.max().date()}")
        print(f"elapsed_seconds: {elapsed:.1f}")


if __name__ == "__main__":
    main(sys.argv[1:])
