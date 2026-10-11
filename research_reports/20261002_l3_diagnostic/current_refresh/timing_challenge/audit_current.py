"""Read-only v2.0 timing tail check after 2026-09-30 delivery refresh."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[4]
OUT = Path(__file__).resolve().parent
NAV = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_nav.csv"
MICRO = ROOT / "outputs/wind_microcap_top_100_biweekly_thursday_16y_cached.csv"
PANEL = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_panel_refreshed.csv"
TURN = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_turnover.csv"
SRC = ROOT / "microcap_top100_mom16_biweekly_live_v2_0.py"
FROZEN = ROOT / "research_reports/20261002_l3_diagnostic/timing_challenge/audit.json"
ACTIVE = "long_microcap_short_zz1000"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def assert_t1_close_fill_clock(signal_holding: str, holding_on_fill_day: str) -> None:
    """A T close decision may only change holdings after the T+1 close return."""
    if holding_on_fill_day != signal_holding:
        raise AssertionError("T+1 intraday return was assigned to a T+1 close fill")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    old = json.loads(FROZEN.read_text(encoding="utf-8"))
    frame = pd.read_csv(NAV, parse_dates=["date"]).set_index("date")
    micro = pd.read_csv(MICRO, parse_dates=["date"]).set_index("date")["close"].astype(float)
    hedge = pd.read_csv(PANEL, parse_dates=["date"]).set_index("date")["1.000852"].astype(float)
    turnover = pd.read_csv(TURN)
    turnover["rebalance_date"] = pd.to_datetime(turnover["rebalance_date"], format="mixed")
    assert frame.index.is_unique and frame.index.is_monotonic_increasing
    assert frame.index[-1] == pd.Timestamp("2026-09-30")
    assert len(frame) == old["nav_rows"] + 1

    mom_micro = (micro / micro.shift(16) - 1).reindex(frame.index)
    mom_hedge = (hedge / hedge.shift(16) - 1).reindex(frame.index)
    switches = frame["holding"].ne(frame["next_holding"])
    checks = {
        "mom_micro_max_abs": float((mom_micro - frame["microcap_mom"]).abs().max()),
        "mom_hedge_max_abs": float((mom_hedge - frame["hedge_mom"]).abs().max()),
        "signal_mismatch": int((mom_micro.gt(mom_hedge) != frame["next_holding"].eq(ACTIVE)).sum()),
        "holding_lag_mismatch": int((frame["holding"].iloc[1:].to_numpy() != frame["next_holding"].iloc[:-1].to_numpy()).sum()),
        "entry_exit_fee_mismatch": int((switches.astype(float) * 0.003 - frame["entry_exit_cost"]).abs().gt(1e-12).sum()),
    }
    if checks["mom_micro_max_abs"] > 1e-10 or checks["mom_hedge_max_abs"] > 1e-10:
        raise AssertionError(checks)
    if any(checks[key] for key in ("signal_mismatch", "holding_lag_mismatch", "entry_exit_fee_mismatch")):
        raise AssertionError(checks)

    disabled_bools = (
        "signal_quality_derisk_enabled", "signal_quality_derisk_triggered", "target_vol_enabled",
        "overheat_enabled", "overheat_triggered", "blocked_until_signal_reset",
        "peak_decay_enabled", "net_drawdown_stop_enabled",
    )
    disabled_costs = ("signal_quality_scale_cost", "scale_change_cost", "financing_cost", "target_vol_trade_cost", "idle_cash_yield")
    disabled_counts = {name: int(frame[name].fillna(False).astype(bool).sum()) for name in disabled_bools}
    disabled_counts.update({name: int(pd.to_numeric(frame[name], errors="coerce").fillna(0).abs().gt(1e-12).sum()) for name in disabled_costs})
    if any(disabled_counts.values()):
        raise AssertionError(disabled_counts)
    if not np.allclose(frame["current_execution_scale"], frame["holding"].eq(ACTIVE).astype(float), atol=1e-12, rtol=0):
        raise AssertionError("execution scale inconsistent with disabled modules")

    end = frame.index[-1]
    windows = {}
    for years in (5, 3, 1):
        start = end - pd.DateOffset(years=years)
        part = frame.loc[frame.index >= start]
        turns = turnover.loc[turnover["rebalance_date"].ge(start) & turnover["rebalance_date"].le(end)]
        counts = {
            "start": str(start.date()), "end": str(end.date()),
            "signal_switches": int(part["holding"].ne(part["next_holding"]).sum()),
            "switches_charged_on_signal_date": int((part["holding"].ne(part["next_holding"]) & part["entry_exit_cost"].gt(0)).sum()),
            "member_rebalances": len(turns),
            "member_rebalances_close_timing": int(turns["execution_timing"].eq("close").sum()),
            "member_entries": int(turns["entry_count"].sum()),
            "member_exits": int(turns["exit_count"].sum()),
            "active_member_rebalance_cost_days": int(part["rebalance_cost"].gt(0).sum()),
        }
        frozen_counts = old["windows"][f"last_{years}y"]
        counts["delta_from_old"] = {key: counts[key] - frozen_counts[key] for key in counts if key in frozen_counts and isinstance(counts[key], int)}
        windows[f"last_{years}y"] = counts

    tail = frame.loc[frame.index >= pd.Timestamp("2026-09-22")]
    tail_fields = ("holding", "next_holding", "microcap_mom", "hedge_mom", "return_raw", "entry_exit_cost", "rebalance_cost", "return_net")
    tail_records = [{"date": str(date.date()), **{key: (str(row[key]) if key in ("holding", "next_holding") else float(row[key])) for key in tail_fields}} for date, row in tail.iterrows()]
    example_kept = all(frame.loc[pd.Timestamp(day), "entry_exit_cost"] == 0.003 for day in ("2026-09-22", "2026-09-23", "2026-09-24"))
    if not example_kept:
        raise AssertionError("September clock example changed")

    # Inject an early fill into a valid two-day T+1-close toy path; reject it.
    assert_t1_close_fill_clock("cash", "cash")
    injected_early_fill_rejected = False
    try:
        assert_t1_close_fill_clock("cash", ACTIVE)
    except AssertionError:
        injected_early_fill_rejected = True
    if not injected_early_fill_rejected:
        raise AssertionError("clock fault injection not rejected")
    actual_t1_clock_rejected = False
    try:
        assert_t1_close_fill_clock(str(frame.loc[pd.Timestamp("2026-09-22"), "holding"]), str(frame.loc[pd.Timestamp("2026-09-23"), "holding"]))
    except AssertionError:
        actual_t1_clock_rejected = True
    if not actual_t1_clock_rejected:
        raise AssertionError("old 9/22 signal did not violate T+1 close clock")

    result = {
        "label": "read_only_current_tail_diagnostic_not_formal_performance",
        "input_sha256": {str(path.relative_to(ROOT)): sha256(path) for path in (SRC, NAV, MICRO, PANEL, TURN)},
        "nav_rows": len(frame), "nav_first": str(frame.index[0].date()), "nav_last": str(end.date()),
        "turnover_rows": len(turnover), "turnover_last": str(turnover["rebalance_date"].max().date()),
        "checks": checks, "disabled_nonzero_counts": disabled_counts, "windows": windows,
        "september_example_kept": example_kept,
        "injected_early_fill_rejected": injected_early_fill_rejected,
        "actual_september_t1_clock_rejected": actual_t1_clock_rejected,
        "tail_records": tail_records,
    }
    (OUT / "audit_current.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
