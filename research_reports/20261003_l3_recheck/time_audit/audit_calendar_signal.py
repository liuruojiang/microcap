"""Independent read-only L3 calendar/signal challenge of frozen old v2.0 outputs.

This script deliberately does not import strategy, L2 executor, or L3 executor code.
It does not construct or certify a corrected NAV.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
PROXY = ROOT / "outputs/wind_microcap_top_100_biweekly_thursday_16y_cached.csv"
PANEL = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_panel_refreshed.csv"
TURNOVER = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_turnover.csv"
BASE = ROOT / "outputs/microcap_top100_mom16_hedge_zz1000_0p8x_biweekly_thursday_16y_v2_0_base_costed_nav.csv"
NAV = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_nav.csv"
V23 = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_3_nav.csv"
V25 = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_5_nav.csv"
CAL = ROOT / "research_reports/20261002_l2_full_repair/data/trade_calendar_evidence/baostock_trade_calendar_20100222_20260930.csv"
SRC = ROOT / "microcap_top100_mom16_biweekly_live_v2_0.py"
HOLIDAY = pd.Timestamp("2018-06-18")
SIGNAL_DATE = pd.Timestamp("2018-06-29")
NEXT_SESSION = pd.Timestamp("2018-07-02")
CLOCK_T = pd.Timestamp("2026-09-22")
CLOCK_FILL = pd.Timestamp("2026-09-23")
ACTIVE = "long_microcap_short_zz1000"


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def read_dates(path: Path, key: str) -> tuple[pd.DataFrame, dict[str, object]]:
    frame = pd.read_csv(path, low_memory=False)
    dates = pd.to_datetime(frame[key], errors="raise")
    if not dates.is_monotonic_increasing or dates.duplicated().any():
        raise AssertionError(f"unordered_or_duplicate:{path.name}")
    return frame, {"rows": len(frame), "first": str(dates.iloc[0].date()), "last": str(dates.iloc[-1].date())}


def momentum(prices: pd.Series) -> pd.Series:
    return prices / prices.shift(16) - 1.0


def check_calendar(dates: pd.Index, official_trade_dates: set[pd.Timestamp]) -> None:
    rejected = [d for d in dates if d not in official_trade_dates]
    if rejected:
        raise AssertionError("nontrading_observation:" + ",".join(str(d.date()) for d in rejected[:3]))


def check_t1_close_clock(old_holding: str, fill_day_return_holding: str) -> None:
    if old_holding != fill_day_return_holding:
        raise AssertionError("T+1_close_order_assigned_T+1_intraday_return")


def check_disabled(frame: pd.DataFrame) -> None:
    disabled_bools = (
        "signal_quality_derisk_enabled", "signal_quality_derisk_triggered",
        "target_vol_enabled", "overheat_enabled", "overheat_triggered",
        "blocked_until_signal_reset", "peak_decay_enabled", "net_drawdown_stop_enabled",
    )
    disabled_costs = (
        "signal_quality_scale_cost", "scale_change_cost", "financing_cost",
        "target_vol_trade_cost", "idle_cash_yield",
    )
    for key in disabled_bools:
        if frame[key].astype(str).str.lower().eq("true").any():
            raise AssertionError(f"disabled_module_residual:{key}")
    for key in disabled_costs:
        if pd.to_numeric(frame[key], errors="raise").abs().gt(1e-12).any():
            raise AssertionError(f"disabled_cost_residual:{key}")


def term(prices: pd.Series, date: pd.Timestamp) -> dict[str, object]:
    loc = prices.index.get_loc(date)
    prior = prices.index[loc - 16]
    return {
        "numerator_date": str(date.date()), "numerator_close": float(prices.loc[date]),
        "denominator_date": str(prior.date()), "denominator_close": float(prices.loc[prior]),
        "mom16": float(prices.loc[date] / prices.loc[prior] - 1.0),
    }


def main() -> None:
    paths = (PROXY, PANEL, TURNOVER, BASE, NAV, V23, V25)
    info = {}
    frames = {}
    for path in paths:
        key = "rebalance_date" if path == TURNOVER else "date"
        frame, row_info = read_dates(path, key)
        info[str(path.relative_to(ROOT)).replace("\\", "/")] = row_info
        frames[path] = frame
    for path in (PROXY, PANEL, BASE, NAV, V23, V25):
        if info[str(path.relative_to(ROOT)).replace("\\", "/")]["last"] != "2026-09-30":
            raise AssertionError(f"local_close_not_aligned:{path.name}")

    proxy = frames[PROXY].set_index(pd.to_datetime(frames[PROXY]["date"]))
    panel = frames[PANEL].set_index(pd.to_datetime(frames[PANEL]["date"]))
    nav = frames[NAV].set_index(pd.to_datetime(frames[NAV]["date"]))
    calendar = pd.read_csv(CAL, parse_dates=["calendar_date"])
    official_trade_dates = set(calendar.loc[calendar["is_trading_day"].eq(1), "calendar_date"])
    proxy_scope = proxy.index[proxy.index >= calendar["calendar_date"].min()]
    panel_scope = panel.index[(panel.index >= calendar["calendar_date"].min()) & (panel.index <= calendar["calendar_date"].max())]
    nontrading_proxy = sorted(set(proxy_scope) - official_trade_dates)
    nontrading_nav = sorted(set(nav.index) - official_trade_dates)
    nontrading_panel = sorted(set(panel_scope) - official_trade_dates)
    if nontrading_proxy != [HOLIDAY] or nontrading_nav != [HOLIDAY] or nontrading_panel != [HOLIDAY]:
        raise AssertionError(f"unexpected_nontrading_dates:{nontrading_proxy}:{nontrading_nav}:{nontrading_panel[:3]}")
    if HOLIDAY in set(pd.to_datetime(frames[TURNOVER]["rebalance_date"])):
        raise AssertionError("holiday_rebalance")

    old_micro = proxy["close"].astype(float)
    old_hedge = panel["1.000852"].astype(float).reindex(old_micro.index)
    clean_micro = old_micro.drop(index=HOLIDAY)
    clean_hedge = old_hedge.drop(index=HOLIDAY)
    old_m, old_h = momentum(old_micro), momentum(old_hedge)
    clean_m, clean_h = momentum(clean_micro), momentum(clean_hedge)
    old_m_error = float((old_m.reindex(nav.index) - nav["microcap_mom"]).abs().max())
    old_h_error = float((old_h.reindex(nav.index) - nav["hedge_mom"]).abs().max())
    if old_m_error > 1e-10:
        raise AssertionError("old_micro_mom_does_not_replay")
    if old_h_error > 1e-10:
        raise AssertionError("old_hedge_mom_does_not_replay")
    old_signal_all = old_m.reindex(nav.index).gt(old_h.reindex(nav.index))
    if (old_signal_all != nav["next_holding"].eq(ACTIVE)).any():
        raise AssertionError("old_trailing_close_signal_mismatch")
    if not nav["current_execution_scale"].eq(nav["holding"].eq(ACTIVE).astype(float)).all():
        raise AssertionError("disabled_overlay_changed_scale")
    lead_mutant = old_micro.shift(-1).div(old_micro.shift(15)).sub(1.0).reindex(nav.index)
    lead_mutant_mismatches = int((lead_mutant - nav["microcap_mom"]).abs().gt(1e-10).sum())
    if lead_mutant_mismatches == 0:
        raise AssertionError("lead_price_mutant_not_rejected")

    common = nav.index.intersection(clean_micro.index)
    after = common[common > HOLIDAY]
    changed_micro = after[(old_m.loc[after] - clean_m.loc[after]).abs() > 1e-10]
    changed_hedge = after[(old_h.loc[after] - clean_h.loc[after]).abs() > 1e-10]
    old_signal = old_m.loc[after].gt(old_h.loc[after])
    clean_signal = clean_m.loc[after].gt(clean_h.loc[after])
    signal_diff = after[old_signal != clean_signal]
    if len(changed_micro) != 15 or len(changed_hedge) != 15 or list(signal_diff) != [SIGNAL_DATE]:
        raise AssertionError("calendar_counterfactual_changed_unexpected_dates")
    if (not nav.loc[pd.Timestamp("2018-06-28"), "next_holding"] == ACTIVE
            or not nav.loc[SIGNAL_DATE, "holding"] == ACTIVE
            or not nav.loc[SIGNAL_DATE, "next_holding"] == ACTIVE
            or not nav.loc[NEXT_SESSION, "holding"] == ACTIVE):
        raise AssertionError("old_20180629_signal_chain_changed")
    if bool(clean_signal.loc[SIGNAL_DATE]):
        raise AssertionError("clean_signal_was_not_cash")
    if (float(nav.loc[pd.Timestamp("2018-06-28"), "entry_exit_cost"]) != 0.003
            or float(nav.loc[SIGNAL_DATE, "entry_exit_cost"]) != 0.0):
        raise AssertionError("old_entry_cost_chain_changed")

    # Inject the nontrading row back. This must recreate the contaminated 16-day
    # continuation signal, and the calendar gate must reject it independently.
    reinjected_micro = clean_micro.copy()
    reinjected_hedge = clean_hedge.copy()
    reinjected_micro.loc[HOLIDAY] = old_micro.loc[HOLIDAY]
    reinjected_hedge.loc[HOLIDAY] = old_hedge.loc[HOLIDAY]
    reinjected_micro = reinjected_micro.sort_index()
    reinjected_hedge = reinjected_hedge.sort_index()
    if not momentum(reinjected_micro).loc[SIGNAL_DATE] > momentum(reinjected_hedge).loc[SIGNAL_DATE]:
        raise AssertionError("holiday_reinsertion_did_not_recreate_false_continuation")
    holiday_mutant_rejected = False
    try:
        check_calendar(reinjected_micro.index[reinjected_micro.index >= calendar["calendar_date"].min()], official_trade_dates)
    except AssertionError:
        holiday_mutant_rejected = True

    # Separate, recent clock counterexample: a T close cash-to-long order
    # cannot own T+1 intraday return if its fill is only at T+1 close.
    t1_early_fill_rejected = False
    try:
        if nav.loc[CLOCK_T, "holding"] != "cash" or nav.loc[CLOCK_T, "next_holding"] != ACTIVE:
            raise AssertionError("recent_clock_example_changed")
        check_t1_close_clock("cash", str(nav.loc[CLOCK_FILL, "holding"]))
    except AssertionError:
        t1_early_fill_rejected = True
    check_disabled(nav)
    residual_mutant_rejected = False
    mutant = nav.loc[[SIGNAL_DATE]].copy()
    mutant.loc[SIGNAL_DATE, "overheat_triggered"] = True
    try:
        check_disabled(mutant)
    except AssertionError:
        residual_mutant_rejected = True
    if not (holiday_mutant_rejected and t1_early_fill_rejected and residual_mutant_rejected):
        raise AssertionError("mutation_not_rejected")

    result = {
        "scope": "read_only_L3_time_source_challenge_of_old_v2_0; no_corrected_NAV",
        "source_revision": str(nav["strategy_revision"].dropna().iloc[-1]),
        "input_sha256": {str(p.relative_to(ROOT)).replace("\\", "/"): digest(p) for p in (*paths, CAL, SRC)},
        "input_rows_dates": info,
        "calendar": {"file_rows": len(calendar), "trade_rows": len(official_trade_dates),
                     "proxy_nontrading_dates": [str(x.date()) for x in nontrading_proxy],
                     "panel_nontrading_dates": [str(x.date()) for x in nontrading_panel],
                     "nav_nontrading_dates": [str(x.date()) for x in nontrading_nav]},
        "first_calendar_difference": str(HOLIDAY.date()),
        "whole_series_trailing_mom_replay": {"v2_0_days": len(nav), "micro_max_abs_error": old_m_error,
                                             "hedge_max_abs_error": old_h_error,
                                             "next_holding_signal_mismatch": 0,
                                             "future_price_lead_mutant_mismatch_days": lead_mutant_mismatches},
        "first_momentum_difference": str(changed_micro[0].date()),
        "last_momentum_difference": str(changed_micro[-1].date()),
        "changed_micro_momentum_days": len(changed_micro),
        "changed_hedge_momentum_days": len(changed_hedge),
        "signal_difference_dates": [str(x.date()) for x in signal_diff],
        "first_signal_difference": str(signal_diff[0].date()),
        "first_conditional_legacy_fee_difference": {
            "date": str(SIGNAL_DATE.date()),
            "old_entry_exit_cost": float(nav.loc[SIGNAL_DATE, "entry_exit_cost"]),
            "calendar_only_legacy_exit_cost_if_same_rule": 0.003,
        },
        "first_conditional_legacy_holding_difference_date": str(NEXT_SESSION.date()),
        "first_signal_detail": {
            "date": str(SIGNAL_DATE.date()),
            "previous_signal_date": "2018-06-28",
            "previous_old_signal": str(nav.loc[pd.Timestamp("2018-06-28"), "next_holding"]),
            "previous_calendar_corrected_raw_signal": ACTIVE,
            "previous_old_entry_cost": float(nav.loc[pd.Timestamp("2018-06-28"), "entry_exit_cost"]),
            "old_proxy_16d": term(old_micro, SIGNAL_DATE),
            "old_hedge_16d": term(old_hedge, SIGNAL_DATE),
            "clean_proxy_16d": term(clean_micro, SIGNAL_DATE),
            "clean_hedge_16d": term(clean_hedge, SIGNAL_DATE),
            "old_next_holding": str(nav.loc[SIGNAL_DATE, "next_holding"]),
            "old_holding": str(nav.loc[SIGNAL_DATE, "holding"]),
            "calendar_corrected_raw_signal": "cash (exit at T close under legacy signal convention)",
            "old_same_day_entry_exit_cost": float(nav.loc[SIGNAL_DATE, "entry_exit_cost"]),
            "old_return_net": float(nav.loc[SIGNAL_DATE, "return_net"]),
        },
        "first_following_session": {
            "date": str(NEXT_SESSION.date()),
            "old_holding": str(nav.loc[NEXT_SESSION, "holding"]),
            "old_return_raw": float(nav.loc[NEXT_SESSION, "return_raw"]),
            "old_return_net": float(nav.loc[NEXT_SESSION, "return_net"]),
            "calendar_corrected_legacy_signal_lag_holding": "cash",
            "T_plus_1_close_contract_intraday_holding": "depends on 2018-06-29 paper buy fill, could be long before the 2018-07-02 close exit attempt",
            "note": "A calendar-only legacy signal counterfactual is distinct from the T+1-close share-account model. No corrected NAV is calculated.",
        },
        "disabled_module_rows_checked": len(nav),
        "recent_clock_example": {"signal_date": str(CLOCK_T.date()), "fill_date": str(CLOCK_FILL.date()),
                                 "old_T_holding": str(nav.loc[CLOCK_T, "holding"]),
                                 "old_T_next_holding": str(nav.loc[CLOCK_T, "next_holding"]),
                                 "old_T_plus_1_holding": str(nav.loc[CLOCK_FILL, "holding"])},
        "mutations": {"reinsert_holiday_recreates_false_continuation": True,
                      "reinsert_holiday_rejected_by_calendar": holiday_mutant_rejected,
                      "future_price_lead_mutant_rejected": lead_mutant_mismatches > 0,
                      "early_T_plus_1_fill_rejected": t1_early_fill_rejected,
                      "overheat_residual_rejected": residual_mutant_rejected},
    }
    HERE.mkdir(parents=True, exist_ok=True)
    (HERE / "audit_result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"signal_difference_dates": result["signal_difference_dates"],
                      "first_momentum_difference": result["first_momentum_difference"],
                      "last_momentum_difference": result["last_momentum_difference"],
                      "mutations": result["mutations"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
