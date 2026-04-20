from __future__ import annotations

import json
import hashlib
import sys
from pathlib import Path

import matplotlib
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt

import microcap_top100_mom16_biweekly_live_v1_2 as v1_2_mod


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"

BASE_SUMMARY_JSON = OUTPUT_DIR / "microcap_top100_mom16_biweekly_live_v1_2_summary.json"
BASE_SIGNAL_CSV = OUTPUT_DIR / "microcap_top100_mom16_biweekly_live_v1_2_latest_signal.csv"
BASE_COSTED_NAV_CSV = OUTPUT_DIR / "microcap_top100_mom16_hedge_zz1000_0p8x_nav4_8_biweekly_thursday_16y_costed_nav.csv"

OUTPUT_PREFIX = "microcap_top100_mom16_biweekly_live_v1_5"
SUMMARY_JSON = OUTPUT_DIR / f"{OUTPUT_PREFIX}_summary.json"
LATEST_SIGNAL_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_latest_signal.csv"
NAV_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_nav.csv"
COSTED_NAV_CSV = OUTPUT_DIR / "microcap_top100_mom16_hedge_zz1000_0p8x_nav4_8_gapexit_newpeak_v1_5_costed_nav.csv"
PERF_SUMMARY_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_summary.csv"
PERF_YEARLY_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_yearly.csv"
PERF_NAV_CSV = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_nav.csv"
PERF_JSON = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_summary.json"
PERF_PNG = OUTPUT_DIR / f"{OUTPUT_PREFIX}_performance_curve.png"

EXPECTED_VERSION_ROLE = "signal_quality_overlay_on_v1_2"
EXPECTED_VERSION_NOTE_PREFIX = "Signal-quality overlay on top of v1.2."
BASE_HEDGE_RATIO = 0.8
DECAY_RATIO_THRESHOLD = 0.30
DERISK_SCALE = 0.0
RECOVERY_RATIO_THRESHOLD = 0.40


def _file_sha1(path: Path) -> str:
    digest = hashlib.sha1()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def current_base_fingerprint() -> dict[str, object]:
    return {
        "base_version": "1.2",
        "base_costed_nav_csv": str(BASE_COSTED_NAV_CSV),
        "base_costed_nav_sha1": _file_sha1(BASE_COSTED_NAV_CSV),
        "overlay_type": "momentum_gap_peak_decay_exit_new_peak_guard_on_v1_2",
        "decay_ratio_threshold": DECAY_RATIO_THRESHOLD,
        "derisk_scale": DERISK_SCALE,
        "recovery_ratio_threshold": RECOVERY_RATIO_THRESHOLD,
    }


def summary_matches_current_v1_5_base(summary: dict[str, object]) -> bool:
    if not isinstance(summary, dict):
        return False
    if str(summary.get("version")) != "1.5":
        return False
    if str(summary.get("version_role")) != EXPECTED_VERSION_ROLE:
        return False
    if not str(summary.get("version_note", "")).startswith(EXPECTED_VERSION_NOTE_PREFIX):
        return False
    return summary.get("base_fingerprint") == current_base_fingerprint()


def invalidate_incompatible_v1_5_outputs() -> list[Path]:
    if not SUMMARY_JSON.exists():
        return []
    try:
        summary = json.loads(SUMMARY_JSON.read_text(encoding="utf-8"))
    except Exception:
        summary = None
    if summary_matches_current_v1_5_base(summary):
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


def _load_base_v1_2_context() -> tuple[dict[str, object], pd.DataFrame, pd.DataFrame]:
    summary, signal_df, out = v1_2_mod.generate_v1_2_outputs()
    frame = out.copy().sort_index()
    if "date" in frame.columns:
        frame["date"] = pd.to_datetime(frame["date"])
        frame = frame.set_index("date")
    return summary, signal_df, frame


def apply_v1_2_signal_quality_overlay(
    base_net: pd.DataFrame,
    decay_ratio_threshold: float,
    derisk_scale: float,
    recovery_ratio_threshold: float,
) -> pd.DataFrame:
    out = base_net.copy().sort_index()
    required = {"holding", "next_holding", "momentum_gap", "return_net_v1_2"}
    missing = required.difference(out.columns)
    if missing:
        raise KeyError(f"Missing columns for v1.5 overlay: {sorted(missing)}")

    current_active = str(out["holding"].iloc[0]) != "cash"
    active_scale = 1.0
    derisked_in_trade = False
    waiting_for_new_peak_after_recovery = False
    rearm_peak_level: float | None = None
    gap_peak: float | None = None
    current_trade_id: int | None = 1 if current_active else None
    next_trade_id = 1 if current_active else 0

    execution_scales: list[float] = []
    derisk_flags: list[bool] = []
    recovery_flags: list[bool] = []
    gap_peaks: list[float | None] = []
    gap_decay_ratios: list[float | None] = []
    return_nets: list[float] = []
    nav_nets: list[float] = []
    trade_ids: list[int | None] = []
    executed_holding: list[str] = []
    executed_next_holding: list[str] = []

    nav_net = 1.0
    for dt in out.index:
        desired_next_active = str(out.at[dt, "next_holding"]) != "cash"
        current_gap = out.at[dt, "momentum_gap"]
        current_gap = float(current_gap) if pd.notna(current_gap) else None

        if not current_active and desired_next_active:
            next_trade_id += 1
            current_trade_id = next_trade_id
            gap_peak = current_gap
            active_scale = 1.0
            derisked_in_trade = False
            waiting_for_new_peak_after_recovery = False
            rearm_peak_level = None

        if current_active and current_gap is not None:
            gap_peak = current_gap if gap_peak is None else max(float(gap_peak), current_gap)

        if (
            current_active
            and waiting_for_new_peak_after_recovery
            and rearm_peak_level is not None
            and gap_peak is not None
            and float(gap_peak) > float(rearm_peak_level)
        ):
            waiting_for_new_peak_after_recovery = False
            rearm_peak_level = None

        gap_decay_ratio = None
        if current_active and current_gap is not None and gap_peak is not None and gap_peak > 0:
            gap_decay_ratio = current_gap / gap_peak

        derisk_triggered = False
        recovery_triggered = False
        applied_scale = active_scale if current_active else 1.0

        if (
            current_active
            and desired_next_active
            and derisked_in_trade
            and gap_decay_ratio is not None
            and gap_decay_ratio >= recovery_ratio_threshold
        ):
            active_scale = 1.0
            applied_scale = 1.0
            derisked_in_trade = False
            waiting_for_new_peak_after_recovery = True
            rearm_peak_level = gap_peak
            recovery_triggered = True

        if (
            current_active
            and desired_next_active
            and not derisked_in_trade
            and not waiting_for_new_peak_after_recovery
            and gap_decay_ratio is not None
            and gap_decay_ratio <= decay_ratio_threshold
        ):
            active_scale = derisk_scale
            applied_scale = derisk_scale
            derisked_in_trade = True
            derisk_triggered = True

        base_ret = float(pd.to_numeric(pd.Series([out.at[dt, "return_net_v1_2"]]), errors="coerce").fillna(0.0).iloc[0])
        realized_ret = base_ret if not current_active else base_ret * applied_scale
        nav_net *= 1.0 + realized_ret

        execution_scales.append(float(applied_scale))
        derisk_flags.append(bool(derisk_triggered))
        recovery_flags.append(bool(recovery_triggered))
        gap_peaks.append(None if gap_peak is None else float(gap_peak))
        gap_decay_ratios.append(None if gap_decay_ratio is None else float(gap_decay_ratio))
        return_nets.append(float(realized_ret))
        nav_nets.append(float(nav_net))
        trade_ids.append(current_trade_id if (current_active or desired_next_active) else None)
        executed_holding.append("long_microcap_short_zz1000" if current_active else "cash")
        executed_next_holding.append("long_microcap_short_zz1000" if desired_next_active else "cash")

        current_active = desired_next_active
        if not current_active:
            current_trade_id = None
            gap_peak = None
            active_scale = 1.0
            derisked_in_trade = False
            waiting_for_new_peak_after_recovery = False
            rearm_peak_level = None

    out["holding_overlay"] = executed_holding
    out["next_holding_overlay"] = executed_next_holding
    out["trade_id_overlay"] = pd.Series(trade_ids, index=out.index, dtype="Int64")
    out["execution_scale_overlay"] = pd.Series(execution_scales, index=out.index, dtype=float)
    out["signal_quality_derisk_triggered_overlay"] = pd.Series(derisk_flags, index=out.index, dtype=bool)
    out["recovery_triggered_overlay"] = pd.Series(recovery_flags, index=out.index, dtype=bool)
    out["gap_peak_overlay"] = pd.Series(gap_peaks, index=out.index, dtype=float)
    out["gap_decay_ratio_overlay"] = pd.Series(gap_decay_ratios, index=out.index, dtype=float)
    out["return_net_overlay"] = pd.Series(return_nets, index=out.index, dtype=float)
    out["nav_net_overlay"] = pd.Series(nav_nets, index=out.index, dtype=float)
    return out


def _build_signal_row(net_df: pd.DataFrame, reference_summary: dict[str, object], base_signal: pd.DataFrame) -> pd.DataFrame:
    latest_row = net_df.iloc[-1]
    latest_signal = dict(reference_summary.get("latest_signal", {}))
    if not base_signal.empty:
        latest_signal.update(base_signal.iloc[0].drop(labels=["date"], errors="ignore").to_dict())
    current_holding = str(latest_row.get("holding_overlay", latest_signal.get("current_holding", "cash")))
    next_holding = str(latest_row.get("next_holding_overlay", latest_signal.get("next_holding", current_holding)))
    latest_signal["current_holding"] = current_holding
    latest_signal["next_holding"] = next_holding
    latest_signal["trade_state"] = v1_2_mod.v1_1_mod.base_mod.compute_trade_state(current_holding, next_holding)
    for src_col, dst_col in [
        ("momentum_gap", "momentum_gap"),
        ("gap_peak_overlay", "gap_peak"),
        ("gap_decay_ratio_overlay", "gap_decay_ratio"),
        ("execution_scale_overlay", "execution_scale"),
    ]:
        if src_col in latest_row and pd.notna(latest_row[src_col]):
            latest_signal[dst_col] = float(latest_row[src_col])
    latest_signal["signal_quality_derisk_triggered"] = bool(latest_row.get("signal_quality_derisk_triggered_overlay", False))
    latest_signal["derisk_scale"] = DERISK_SCALE
    latest_signal["decay_ratio_threshold"] = DECAY_RATIO_THRESHOLD
    latest_signal["recovery_ratio_threshold"] = RECOVERY_RATIO_THRESHOLD
    latest_signal.setdefault("signal_label", next_holding)
    return pd.DataFrame([{**latest_signal, "date": pd.Timestamp(net_df.index.max())}])


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
    plt.title("Top100 Microcap Mom16 Biweekly v1.5 Signal-Quality Overlay on v1.2")
    plt.ylabel("NAV")
    plt.grid(alpha=0.25)
    plt.tight_layout()
    plt.savefig(PERF_PNG, dpi=160)
    plt.close()

    payload = {
        "period_label": "full_sample",
        "source": "costed_v1_5",
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


def generate_v1_5_outputs() -> tuple[dict[str, object], pd.DataFrame, pd.DataFrame]:
    invalidate_incompatible_v1_5_outputs()
    reference_summary, base_signal, base_net = _load_base_v1_2_context()
    out = apply_v1_2_signal_quality_overlay(
        base_net=base_net,
        decay_ratio_threshold=DECAY_RATIO_THRESHOLD,
        derisk_scale=DERISK_SCALE,
        recovery_ratio_threshold=RECOVERY_RATIO_THRESHOLD,
    )
    out.to_csv(COSTED_NAV_CSV, encoding="utf-8-sig")
    out.reset_index().to_csv(NAV_CSV, index=False, encoding="utf-8-sig")

    signal_row = _build_signal_row(out, reference_summary, base_signal)
    signal_row["version"] = "1.5"
    signal_row["base_version"] = "1.2"
    signal_row["overlay_type"] = "momentum_gap_peak_decay_exit_new_peak_guard_on_v1_2"
    LATEST_SIGNAL_CSV.write_text(signal_row.to_csv(index=False), encoding="utf-8")

    perf_payload = build_performance_payload(out["return_net_overlay"].fillna(0.0))

    summary = dict(reference_summary)
    summary["strategy"] = OUTPUT_PREFIX
    summary["version"] = "1.5"
    summary["version_role"] = EXPECTED_VERSION_ROLE
    summary["version_note"] = (
        "Signal-quality overlay on top of v1.2. Keep v1.2 NAV throttle, "
        "then fully exit when momentum-gap peak-decay hits 30%, recover at 40%, "
        "with new-peak rearm guard."
    )
    summary.setdefault("core_params", {})
    summary["core_params"]["fixed_hedge_ratio"] = BASE_HEDGE_RATIO
    summary["core_params"]["signal_quality_overlay"] = {
        "type": "momentum_gap_peak_decay_exit_new_peak_guard_on_v1_2",
        "decay_ratio_threshold": DECAY_RATIO_THRESHOLD,
        "derisk_scale": DERISK_SCALE,
        "recovery_ratio_threshold": RECOVERY_RATIO_THRESHOLD,
        "rearm_rule": "must set a new trade gap peak after recovery before a later exit can trigger again",
    }
    summary["latest_trade_date"] = str(pd.Timestamp(signal_row.iloc[0]["date"]).date())
    summary["latest_nav_date"] = str(pd.Timestamp(out.index.max()).date())
    summary["latest_signal"] = signal_row.iloc[0].drop(labels=["date"], errors="ignore").to_dict()
    summary["performance_snapshot"] = perf_payload["summary"]
    summary["base_fingerprint"] = current_base_fingerprint()
    SUMMARY_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary, signal_row, out


def _print_signal_query() -> None:
    summary, signal_df, _ = generate_v1_5_outputs()
    row = signal_df.iloc[0]
    print("signal")
    print("strategy_version: v1.5")
    print("base_version: v1.2")
    print(
        "overlay: momentum_gap peak-decay exit "
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
    generate_v1_5_outputs()
    perf_df = pd.read_csv(COSTED_NAV_CSV, parse_dates=["date"]).sort_values("date").set_index("date")
    v1_2_mod.v1_1_mod.base_mod.build_performance_outputs(
        perf_df=perf_df,
        ret_col="return_net_overlay",
        nav_col="nav_net_overlay",
        source_label="costed_v1_5",
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
    if v1_2_mod.v1_1_mod.base_mod.PERFORMANCE_PATTERN.search(query):
        _print_performance_query(query)
        return
    raise ValueError("v1.5 supports: 信号 / 表现 <区间>")


def main() -> None:
    query = " ".join(sys.argv[1:]).strip()
    if query:
        _handle_query(query)
        return
    generate_v1_5_outputs()
    print(str(SUMMARY_JSON))
    print(str(LATEST_SIGNAL_CSV))
    print(str(COSTED_NAV_CSV))


if __name__ == "__main__":
    main()
