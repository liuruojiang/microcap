"""Independent clock and disabled-module check of frozen v2.0 files.

This is a diagnostic replay. It intentionally does not import the strategy.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parent
SRC = ROOT / "microcap_top100_mom16_biweekly_live_v2_0.py"
NAV = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_nav.csv"
MICRO = ROOT / "outputs/wind_microcap_top_100_biweekly_thursday_16y_cached.csv"
PANEL = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_panel_refreshed.csv"
TURN = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_turnover.csv"
ACTIVE = "long_microcap_short_zz1000"


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def assert_disabled(frame: pd.DataFrame) -> None:
    boolean_columns = (
        "signal_quality_derisk_enabled", "signal_quality_derisk_triggered",
        "target_vol_enabled", "overheat_enabled", "overheat_triggered",
        "blocked_until_signal_reset", "peak_decay_enabled", "net_drawdown_stop_enabled",
    )
    zero_columns = (
        "signal_quality_scale_cost", "scale_change_cost", "financing_cost",
        "target_vol_trade_cost", "idle_cash_yield",
    )
    for column in boolean_columns:
        if frame[column].fillna(False).astype(bool).any():
            raise AssertionError(f"disabled module flag active: {column}")
    for column in zero_columns:
        if pd.to_numeric(frame[column], errors="coerce").fillna(0.0).abs().gt(1e-12).any():
            raise AssertionError(f"disabled module economics active: {column}")
    scale = pd.to_numeric(frame["current_execution_scale"], errors="coerce")
    expected = frame["holding"].eq(ACTIVE).astype(float)
    if not np.allclose(scale, expected, atol=1e-12, rtol=0):
        raise AssertionError("disabled module left a non-unit execution scale")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for path in (SRC, NAV, MICRO, PANEL, TURN):
        if not path.is_file():
            raise FileNotFoundError(path)

    frame = pd.read_csv(NAV, parse_dates=["date"]).sort_values("date").set_index("date")
    micro = pd.read_csv(MICRO, parse_dates=["date"]).set_index("date")["close"].astype(float)
    hedge = pd.read_csv(PANEL, parse_dates=["date"]).set_index("date")["1.000852"].astype(float)
    turnover = pd.read_csv(TURN)
    turnover["rebalance_date"] = pd.to_datetime(turnover["rebalance_date"], format="mixed")
    if not frame.index.is_unique or not frame.index.is_monotonic_increasing:
        raise AssertionError("NAV dates not unique and ascending")
    if not turnover["rebalance_date"].is_unique:
        raise AssertionError("turnover rebalance dates not unique")

    # Derive signals from pre-existing input closes. T close is required to know T Mom16.
    micro_mom = (micro / micro.shift(16) - 1).reindex(frame.index)
    hedge_mom = (hedge / hedge.shift(16) - 1).reindex(frame.index)
    micro_ret = micro.pct_change(fill_method=None).reindex(frame.index)
    hedge_ret = hedge.pct_change(fill_method=None).reindex(frame.index)
    signal = micro_mom.gt(hedge_mom)
    next_active = frame["next_holding"].eq(ACTIVE)
    current_active = frame["holding"].eq(ACTIVE)
    checks = {
        "microcap_close_max_abs": float((micro.reindex(frame.index) - frame["microcap_close"]).abs().max()),
        "hedge_close_max_abs": float((hedge.reindex(frame.index) - frame["hedge_close"]).abs().max()),
        "microcap_mom16_max_abs": float((micro_mom - frame["microcap_mom"]).abs().max()),
        "hedge_mom16_max_abs": float((hedge_mom - frame["hedge_mom"]).abs().max()),
        "microcap_daily_return_max_abs": float((micro_ret - frame["microcap_ret"]).abs().max()),
        "hedge_daily_return_max_abs": float((hedge_ret - frame["hedge_ret"]).abs().max()),
        "signal_mismatch": int((signal != next_active).sum()),
        "holding_lag_mismatch": int((current_active.iloc[1:].to_numpy() != next_active.iloc[:-1].to_numpy()).sum()),
    }
    gross = np.where(current_active, micro_ret - 0.8 * hedge_ret - 0.00024, 0.0)
    checks["gross_return_max_abs"] = float(np.nanmax(np.abs(gross - frame["return_raw"])))
    switch = current_active != next_active
    expected_entry_exit_cost = switch.astype(float) * 0.003
    checks["entry_exit_cost_max_abs"] = float((expected_entry_exit_cost - frame["entry_exit_cost"]).abs().max())
    expected_rebalance = turnover.set_index("rebalance_date")["two_side_cost_rate"].reindex(frame.index).fillna(0.0)
    expected_rebalance = expected_rebalance.where(current_active & next_active, 0.0)
    checks["rebalance_cost_max_abs"] = float((expected_rebalance - frame["rebalance_cost"]).abs().max())
    net = (1.0 + gross) * (1.0 - expected_entry_exit_cost - expected_rebalance) - 1.0
    checks["net_return_max_abs"] = float(np.nanmax(np.abs(net - frame["return_net"])))
    if any(value > 1e-10 for key, value in checks.items() if key.endswith("_max_abs")):
        raise AssertionError(checks)
    if checks["signal_mismatch"] or checks["holding_lag_mismatch"]:
        raise AssertionError(checks)
    assert_disabled(frame)

    # The price used by T Mom16 and the T portfolio change/cost share a close.
    event_rows = []
    turnover_by_date = turnover.set_index("rebalance_date")
    for date, row in frame.iterrows():
        if not bool(switch.loc[date]) and date not in turnover_by_date.index:
            continue
        t = turnover_by_date.loc[date] if date in turnover_by_date.index else None
        event_rows.append({
            "date": str(date.date()),
            "signal_switch": bool(switch.loc[date]),
            "signal_from": str(row["holding"]),
            "signal_to": str(row["next_holding"]),
            "momentum_gap": float(micro_mom.loc[date] - hedge_mom.loc[date]),
            "entry_exit_cost": float(row["entry_exit_cost"]),
            "member_rebalance": t is not None,
            "execution_timing": "" if t is None else str(t["execution_timing"]),
            "rebalance_entry_count": 0 if t is None else int(t["entry_count"]),
            "rebalance_exit_count": 0 if t is None else int(t["exit_count"]),
            "rebalance_cost": float(row["rebalance_cost"]),
        })
    event_frame = pd.DataFrame(event_rows)
    event_frame.to_csv(OUT / "timing_events.csv", index=False, encoding="utf-8")
    end = frame.index.max()
    windows = {}
    for years in (5, 3, 1):
        start = end - pd.DateOffset(years=years)
        events = event_frame[pd.to_datetime(event_frame["date"]) >= start]
        windows[f"last_{years}y"] = {
            "start": str(start.date()), "end": str(end.date()),
            "signal_switches": int(events["signal_switch"].sum()),
            "member_rebalances": int(events["member_rebalance"].sum()),
            "member_rebalances_close_timing": int((events["execution_timing"] == "close").sum()),
            "member_entries": int(events["rebalance_entry_count"].sum()),
            "member_exits": int(events["rebalance_exit_count"].sum()),
            "switches_charged_on_signal_date": int((events["signal_switch"] & events["entry_exit_cost"].gt(0)).sum()),
            "active_member_rebalance_cost_days": int(events["rebalance_cost"].gt(0).sum()),
        }

    # Fault injections must be rejected by independent checks.
    injected = frame.copy()
    injected.loc[pd.Timestamp("2026-09-22"), "overheat_triggered"] = True
    overheat_rejected = False
    try:
        assert_disabled(injected)
    except AssertionError:
        overheat_rejected = True
    injected = frame.copy()
    injected.loc[pd.Timestamp("2026-09-22"), "scale_change_cost"] = 0.001
    disabled_cost_rejected = False
    try:
        assert_disabled(injected)
    except AssertionError:
        disabled_cost_rejected = True
    if not overheat_rejected or not disabled_cost_rejected:
        raise AssertionError("disabled module fault injection was not rejected")

    sample = event_frame[event_frame["date"].between("2026-09-22", "2026-09-24")].to_dict("records")
    summary = {
        "label": "diagnostic_frozen_old_proxy_not_formal_performance",
        "input_sha256": {str(path.relative_to(ROOT)): sha256(path) for path in (SRC, NAV, MICRO, PANEL, TURN)},
        "nav_first": str(frame.index.min().date()), "nav_last": str(end.date()), "nav_rows": len(frame),
        "turnover_first": str(turnover["rebalance_date"].min().date()),
        "turnover_last": str(turnover["rebalance_date"].max().date()), "turnover_rows": len(turnover),
        "checks": checks, "windows": windows,
        "disabled_module_injections_rejected": {"overheat_flag": overheat_rejected, "scale_change_cost": disabled_cost_rejected},
        "september_2026_switch_example": sample,
    }
    (OUT / "audit.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
