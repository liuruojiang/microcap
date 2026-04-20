from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

import matplotlib
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt

import microcap_top100_mom16_biweekly_live_v1_1 as v1_1_mod


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"

BASE_SUMMARY_JSON = OUTPUT_DIR / "microcap_top100_mom16_biweekly_live_v1_1_summary.json"
BASE_SIGNAL_CSV = OUTPUT_DIR / "microcap_top100_mom16_biweekly_live_v1_1_latest_signal.csv"
BASE_COSTED_NAV_CSV = OUTPUT_DIR / "microcap_top100_mom16_hedge_zz1000_0p8x_biweekly_thursday_16y_costed_nav.csv"
V1_0_SUMMARY_JSON = OUTPUT_DIR / "microcap_top100_mom16_biweekly_live_summary.json"

OUTPUT_PREFIX = "microcap_top100_mom16_biweekly_live_v1_4"
SUMMARY_JSON = OUTPUT_DIR / f"{OUTPUT_PREFIX}_summary.json"
LATEST_SIGNAL_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_latest_signal.csv"
NAV_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_nav.csv"
COSTED_NAV_CSV = OUTPUT_DIR / "microcap_top100_mom16_hedge_zz1000_0p8x_gapderisk_newpeak_v1_4_costed_nav.csv"
PERF_SUMMARY_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_summary.csv"
PERF_YEARLY_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_yearly.csv"
PERF_NAV_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_nav.csv"
PERF_JSON = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_summary.json"
PERF_PNG = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_curve.png"

EXPECTED_VERSION_ROLE = "signal_quality_derisk_alternative"
EXPECTED_VERSION_NOTE_PREFIX = "Signal-quality derisk alternative."
BASE_HEDGE_RATIO = 0.8
DECAY_RATIO_THRESHOLD = 0.25
DERISK_SCALE = 0.0
RECOVERY_RATIO_THRESHOLD = 0.35


def _file_sha1(path: Path) -> str:
    digest = hashlib.sha1()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _build_v1_1_args(max_workers: int = 8) -> argparse.Namespace:
    base_mod = v1_1_mod.base_mod
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


def current_base_fingerprint() -> dict[str, object]:
    return {
        "base_version": "1.1",
        "base_costed_nav_csv": str(BASE_COSTED_NAV_CSV),
        "base_costed_nav_sha1": _file_sha1(BASE_COSTED_NAV_CSV),
        "research_stack_version": v1_1_mod.base_mod.RESEARCH_STACK_VERSION,
        "overlay_type": "momentum_gap_peak_decay_derisk_new_peak_guard",
        "decay_ratio_threshold": DECAY_RATIO_THRESHOLD,
        "derisk_scale": DERISK_SCALE,
        "recovery_ratio_threshold": RECOVERY_RATIO_THRESHOLD,
    }


def summary_matches_current_v1_4_base(summary: dict[str, object]) -> bool:
    if not isinstance(summary, dict):
        return False
    if str(summary.get("version")) != "1.4":
        return False
    if str(summary.get("version_role")) != EXPECTED_VERSION_ROLE:
        return False
    if not str(summary.get("version_note", "")).startswith(EXPECTED_VERSION_NOTE_PREFIX):
        return False
    return summary.get("base_fingerprint") == current_base_fingerprint()


def invalidate_incompatible_v1_4_outputs() -> list[Path]:
    if not SUMMARY_JSON.exists():
        return []
    try:
        summary = json.loads(SUMMARY_JSON.read_text(encoding="utf-8"))
    except Exception:
        summary = None
    if summary_matches_current_v1_4_base(summary):
        return []
    removed: list[Path] = []
    for path in [
        SUMMARY_JSON,
        LATEST_SIGNAL_CSV,
        NAV_CSV,
        COSTED_NAV_CSV,
        PERF_SUMMARY_CSV,
        PERF_YEARLY_CSV,
        PERF_NAV_CSV,
        PERF_JSON,
        PERF_PNG,
    ]:
        if path.exists():
            path.unlink(missing_ok=True)
            removed.append(path)
    return removed


def _ensure_base_outputs() -> None:
    base_paths = v1_1_mod.base_mod.build_output_paths(v1_1_mod.base_mod.DEFAULT_OUTPUT_PREFIX)
    v1_1_mod.prepare_current_v1_1_outputs(paths=base_paths, costed_nav_csv=BASE_COSTED_NAV_CSV)
    if base_paths["proxy_turnover"].exists() and BASE_COSTED_NAV_CSV.exists():
        return
    args = _build_v1_1_args()
    resolved_panel_path, target_end_date = v1_1_mod.base_mod.build_refreshed_panel_shadow(args, base_paths)
    v1_1_mod.base_mod.ensure_strategy_files(args, base_paths, resolved_panel_path, target_end_date)


def _load_reference_summary() -> dict[str, object]:
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


def _build_signal_row(net_df: pd.DataFrame, reference_summary: dict[str, object]) -> pd.DataFrame:
    latest_row = net_df.iloc[-1]
    latest_signal = dict(reference_summary.get("latest_signal", {}))
    current_holding = str(latest_row.get("holding", latest_signal.get("current_holding", "cash")))
    next_holding = str(latest_row.get("next_holding", latest_signal.get("next_holding", current_holding)))
    latest_signal["current_holding"] = current_holding
    latest_signal["next_holding"] = next_holding
    latest_signal["trade_state"] = v1_1_mod.base_mod.compute_trade_state(current_holding, next_holding)
    latest_signal["momentum_trade_state"] = latest_signal["trade_state"]
    for src_col in [
        "microcap_close",
        "hedge_close",
        "microcap_mom",
        "hedge_mom",
        "momentum_gap",
        "gap_peak",
        "gap_decay_ratio",
        "execution_scale",
    ]:
        if src_col in latest_row and pd.notna(latest_row[src_col]):
            latest_signal[src_col] = float(latest_row[src_col])
    latest_signal["signal_quality_derisk_triggered"] = bool(latest_row.get("signal_quality_derisk_triggered", False))
    latest_signal["fixed_hedge_ratio"] = BASE_HEDGE_RATIO
    latest_signal["decay_ratio_threshold"] = DECAY_RATIO_THRESHOLD
    latest_signal["derisk_scale"] = DERISK_SCALE
    latest_signal["recovery_ratio_threshold"] = RECOVERY_RATIO_THRESHOLD
    latest_signal.setdefault("signal_label", next_holding)
    return pd.DataFrame([{**latest_signal, "date": pd.Timestamp(net_df.index.max())}])


def _load_base_v1_1_context() -> tuple[dict[str, object], pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    _ensure_base_outputs()
    args = _build_v1_1_args()
    base_paths = v1_1_mod.base_mod.build_output_paths(v1_1_mod.base_mod.DEFAULT_OUTPUT_PREFIX)
    panel_path, target_end_date = v1_1_mod.base_mod.refresh_history_anchor(args, base_paths)
    v1_1_mod.base_mod.ensure_strategy_nav_fresh(args, base_paths, panel_path, target_end_date)
    close_df = v1_1_mod.base_mod.load_close_df(panel_path, args.index_csv)
    gross = v1_1_mod.base_mod.run_signal(close_df).sort_index()
    turnover_df = pd.read_csv(base_paths["proxy_turnover"])
    if "rebalance_date" not in turnover_df.columns:
        raise KeyError(f"Column 'rebalance_date' not found in {base_paths['proxy_turnover']}.")
    turnover_df["rebalance_date"] = pd.to_datetime(turnover_df["rebalance_date"], errors="coerce")
    turnover_df = turnover_df.dropna(subset=["rebalance_date"]).sort_values("rebalance_date")
    reference_summary = _load_reference_summary()
    base_signal = _build_signal_row(
        v1_1_mod.base_mod.freq_mod.cost_mod.apply_cost_model(gross, turnover_df),
        reference_summary,
    )
    return reference_summary, base_signal, gross, turnover_df


def summarize_returns(ret: pd.Series) -> dict[str, float | str | int]:
    ret = ret.fillna(0.0)
    nav = (1.0 + ret).cumprod()
    years = (ret.index[-1] - ret.index[0]).days / 365.25
    annual = nav.iloc[-1] ** (1.0 / years) - 1.0 if years > 0 else 0.0
    vol = ret.std(ddof=1) * (252**0.5)
    sharpe = annual / vol if vol > 0 else 0.0
    dd = nav / nav.cummax() - 1.0
    return {
        "start_date": str(pd.Timestamp(ret.index[0]).date()),
        "end_date": str(pd.Timestamp(ret.index[-1]).date()),
        "days": int(len(ret)),
        "final_nav": float(nav.iloc[-1]),
        "total_return_pct": float((nav.iloc[-1] - 1.0) * 100.0),
        "annual_pct": float(annual * 100.0),
        "max_drawdown_pct": float(dd.min() * 100.0),
        "sharpe": float(sharpe),
        "vol_pct": float(vol * 100.0),
    }


def summarize_yearly(ret: pd.Series) -> pd.DataFrame:
    rows = []
    for year, part in ret.groupby(ret.index.year):
        part = part.dropna()
        if part.empty:
            continue
        nav = (1.0 + part).cumprod()
        years = (part.index[-1] - part.index[0]).days / 365.25
        annual = nav.iloc[-1] ** (1.0 / years) - 1.0 if years > 0 else 0.0
        vol = part.std(ddof=1) * (252**0.5)
        sharpe = annual / vol if vol > 0 else 0.0
        dd = nav / nav.cummax() - 1.0
        rows.append(
            {
                "year": str(year),
                "start_date": str(pd.Timestamp(part.index[0]).date()),
                "end_date": str(pd.Timestamp(part.index[-1]).date()),
                "days": int(len(part)),
                "return_pct": float((nav.iloc[-1] - 1.0) * 100.0),
                "max_drawdown_pct": float(dd.min() * 100.0),
                "sharpe": float(sharpe),
                "annual_pct": float(annual * 100.0),
            }
        )
    return pd.DataFrame(rows)


def build_performance_payload(ret: pd.Series) -> dict[str, object]:
    summary = summarize_returns(ret)
    yearly_df = summarize_yearly(ret)
    yearly_df.to_csv(PERF_YEARLY_CSV, index=False, encoding="utf-8-sig")

    nav_df = pd.DataFrame(
        {
            "date": ret.index,
            "return_net": ret.values,
            "nav_net": (1.0 + ret.fillna(0.0)).cumprod().values,
        }
    )
    nav_df.to_csv(PERF_NAV_CSV, index=False, encoding="utf-8-sig")
    pd.DataFrame([summary]).to_csv(PERF_SUMMARY_CSV, index=False, encoding="utf-8-sig")

    plt.figure(figsize=(12, 6))
    plt.plot(nav_df["date"], nav_df["nav_net"], linewidth=2.0)
    plt.title("Top100 Microcap Mom16 Biweekly v1.4 Signal-Quality Derisk")
    plt.ylabel("NAV")
    plt.grid(alpha=0.25)
    plt.tight_layout()
    plt.savefig(PERF_PNG, dpi=160)
    plt.close()

    payload = {
        "period_label": "full_sample",
        "source": "costed_v1_4",
        "start_date": summary["start_date"],
        "end_date": summary["end_date"],
        "summary": summary,
        "yearly": yearly_df.to_dict(orient="records"),
        "files": {
            "summary_csv": str(PERF_SUMMARY_CSV),
            "yearly_csv": str(PERF_YEARLY_CSV),
            "nav_csv": str(PERF_NAV_CSV),
            "chart_png": str(PERF_PNG),
        },
    }
    PERF_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def generate_v1_4_outputs() -> tuple[dict[str, object], pd.DataFrame, pd.DataFrame]:
    invalidate_incompatible_v1_4_outputs()
    reference_summary, _, gross, turnover_df = _load_base_v1_1_context()
    out = v1_1_mod.base_mod.apply_momentum_gap_peak_decay_derisk(
        gross_result=gross,
        turnover_df=turnover_df,
        decay_ratio_threshold=DECAY_RATIO_THRESHOLD,
        derisk_scale=DERISK_SCALE,
        recovery_ratio_threshold=RECOVERY_RATIO_THRESHOLD,
    )
    out.to_csv(COSTED_NAV_CSV, encoding="utf-8-sig")
    out.reset_index().to_csv(NAV_CSV, index=False, encoding="utf-8-sig")

    signal_row = _build_signal_row(out, reference_summary)
    signal_row["version"] = "1.4"
    signal_row["base_version"] = "1.1"
    signal_row["overlay_type"] = "momentum_gap_peak_decay_derisk_new_peak_guard"
    LATEST_SIGNAL_CSV.write_text(signal_row.to_csv(index=False), encoding="utf-8")

    perf_payload = build_performance_payload(out["return_net"].fillna(0.0))

    summary = dict(reference_summary)
    summary["strategy"] = OUTPUT_PREFIX
    summary["version"] = "1.4"
    summary["version_role"] = EXPECTED_VERSION_ROLE
    summary["version_note"] = (
        "Signal-quality derisk alternative. Same as v1.1 (0.8x hedge), "
        "plus momentum-gap peak-decay derisk with new-peak rearm guard."
    )
    summary.setdefault("core_params", {})
    summary["core_params"]["fixed_hedge_ratio"] = BASE_HEDGE_RATIO
    summary["core_params"]["signal_quality_derisk"] = {
        "type": "momentum_gap_peak_decay_derisk_new_peak_guard",
        "decay_ratio_threshold": DECAY_RATIO_THRESHOLD,
        "derisk_scale": DERISK_SCALE,
        "recovery_ratio_threshold": RECOVERY_RATIO_THRESHOLD,
        "rearm_rule": "must set a new trade gap peak after recovery before a later derisk can trigger again",
    }
    summary["latest_trade_date"] = str(pd.Timestamp(signal_row.iloc[0]["date"]).date())
    summary["latest_nav_date"] = str(pd.Timestamp(out.index.max()).date())
    summary["latest_signal"] = signal_row.iloc[0].drop(labels=["date"], errors="ignore").to_dict()
    summary["performance_snapshot"] = perf_payload["summary"]
    summary["base_fingerprint"] = current_base_fingerprint()
    SUMMARY_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary, signal_row, out


def _print_signal_query() -> None:
    summary, signal_df, _ = generate_v1_4_outputs()
    row = signal_df.iloc[0]
    print("signal")
    print("strategy_version: v1.4")
    print("base_version: v1.1")
    print(
        "overlay: momentum_gap peak-decay derisk "
        f"(decay={DECAY_RATIO_THRESHOLD:.0%}, scale={DERISK_SCALE:.0%}, recover={RECOVERY_RATIO_THRESHOLD:.0%})"
    )
    print(f"current_holding: {row['current_holding']}")
    print(f"next_holding: {row['next_holding']}")
    print(f"trade_state: {row.get('trade_state', 'hold')}")
    print(f"signal_date: {pd.Timestamp(row['date']).strftime('%Y-%m-%d')}")
    print(f"momentum_gap: {float(row.get('momentum_gap', 0.0)):+.4%}")
    print(f"gap_peak: {float(row.get('gap_peak', 0.0)):+.4%}")
    print(f"gap_decay_ratio: {float(row.get('gap_decay_ratio', 0.0)):+.4%}")
    print(f"execution_scale: {float(row.get('execution_scale', 1.0)):.2f}")
    print(f"signal_quality_derisk_triggered: {bool(row.get('signal_quality_derisk_triggered', False))}")
    print(SUMMARY_JSON)
    print(LATEST_SIGNAL_CSV)


def _print_performance_query(query: str) -> None:
    generate_v1_4_outputs()
    perf_df = pd.read_csv(COSTED_NAV_CSV, parse_dates=["date"]).sort_values("date").set_index("date")
    v1_1_mod.base_mod.build_performance_outputs(
        perf_df=perf_df,
        ret_col="return_net",
        nav_col="nav_net",
        source_label="costed_v1_4",
        query_text=query,
        paths={
            "performance_summary": PERF_SUMMARY_CSV,
            "performance_yearly": PERF_YEARLY_CSV,
            "performance_nav": PERF_NAV_CSV,
            "performance_chart": PERF_PNG,
            "performance_json": PERF_JSON,
        },
    )
    print(PERF_PNG)
    print(PERF_SUMMARY_CSV)
    print(PERF_YEARLY_CSV)
    print(PERF_NAV_CSV)
    print(PERF_JSON)


def _handle_query(query: str) -> None:
    if query == "信号":
        _print_signal_query()
        return
    if v1_1_mod.base_mod.PERFORMANCE_PATTERN.search(query):
        _print_performance_query(query)
        return
    raise ValueError("v1.4 supports: 信号 / 表现 <区间>")


def main() -> None:
    query = " ".join(sys.argv[1:]).strip()
    if query:
        _handle_query(query)
        return
    generate_v1_4_outputs()
    print(str(SUMMARY_JSON))
    print(str(LATEST_SIGNAL_CSV))
    print(str(COSTED_NAV_CSV))


if __name__ == "__main__":
    main()
