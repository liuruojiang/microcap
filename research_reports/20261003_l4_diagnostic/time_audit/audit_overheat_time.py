"""Independent read-only audit of frozen v2.3 overheat timing and state.

This intentionally does not import strategy or executor helpers. It reads the
old output only, reconstructs the volatility and state with stdlib arithmetic,
and writes diagnostic evidence beside this script.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "outputs" / "microcap_top100_mom16_lb25_hl2p5_r2off_eb0p08_vol10_oh26_recovery20_exec0p8_v2_3_costed_nav.csv"
SOURCE = ROOT / "microcap_top100_mom16_biweekly_live_v2_3.py"
HERE = Path(__file__).resolve().parent
WINDOW = 10
TRADING_DAYS = 244
TRIGGER = 0.26
RECOVER = 0.20


def is_active(value: str) -> bool:
    return value != "cash"


def decide(base_next: bool, current: bool, risk_off: bool, value: float | None):
    hot = value is not None and value >= TRIGGER
    cool = value is not None and value <= RECOVER
    exit_event = reentry_event = blocked_event = False
    if risk_off:
        if cool:
            next_active, next_risk_off = base_next, False
            reentry_event = base_next
        else:
            next_active, next_risk_off = False, True
    else:
        next_active, next_risk_off = base_next, False
        if next_active and hot:
            if current:
                exit_event = True
            else:
                blocked_event = True
            next_active, next_risk_off = False, True
    return next_active, next_risk_off, exit_event, reentry_event, blocked_event


def std_feature(nav: list[float]) -> list[float | None]:
    returns = [0.0] + [nav[i] / nav[i - 1] - 1 for i in range(1, len(nav))]
    result: list[float | None] = [None] * len(nav)
    for i in range(WINDOW - 1, len(nav)):
        xs = returns[i - WINDOW + 1 : i + 1]
        mean = sum(xs) / WINDOW
        var = sum((x - mean) ** 2 for x in xs) / (WINDOW - 1)
        result[i] = math.sqrt(var * TRADING_DAYS)
    return result


def close_spread_nav(rows: list[dict[str, str]], seed: float) -> list[float]:
    """Rebuild the signal spread from output close prices, including daily drag."""
    nav = [seed]
    for previous, current in zip(rows[:-1], rows[1:]):
        micro_ret = float(current["microcap_close"]) / float(previous["microcap_close"]) - 1
        hedge_ret = float(current["hedge_close"]) / float(previous["hedge_close"]) - 1
        nav.append(nav[-1] * (1 + micro_ret - hedge_ret - 0.0003))
    return nav


def wls_scores(nav: list[float]) -> list[float | None]:
    lookback = 25
    halflife = 2.5
    weights = [0.5 ** ((lookback - 1 - j) / halflife) for j in range(lookback)]
    total_weight = sum(weights)
    x_bar = sum(j * w for j, w in enumerate(weights)) / total_weight
    denominator = sum(w * (j - x_bar) ** 2 for j, w in enumerate(weights))
    logs = [math.log(value) for value in nav]
    scores: list[float | None] = [None] * len(nav)
    for i in range(lookback - 1, len(nav)):
        y = logs[i - lookback + 1 : i + 1]
        y_bar = sum(w * v for w, v in zip(weights, y, strict=True)) / total_weight
        slope = sum(w * (j - x_bar) * (v - y_bar) for j, (w, v) in enumerate(zip(weights, y, strict=True))) / denominator
        scores[i] = slope * TRADING_DAYS
    return scores


def replay(rows: list[dict[str, str]], values: list[float | None]):
    state = is_active(rows[0]["base_holding"])
    risk_off = False
    expected = []
    for row, feature in zip(rows, values, strict=True):
        next_active, risk_off_next, exit_event, reentry_event, blocked_event = decide(
            is_active(row["base_next_holding"]), state, risk_off, feature
        )
        expected.append((state, next_active, risk_off, exit_event, reentry_event, blocked_event))
        state, risk_off = next_active, risk_off_next
    return expected


def flag(value: str) -> bool:
    if value not in {"True", "False"}:
        raise ValueError(f"unexpected bool: {value!r}")
    return value == "True"


def disabled_modules_are_off(rows: list[dict[str, str]]) -> bool:
    return all(
        row["target_vol_enabled"] == "False"
        and row["cash_day_yield_enabled"] == "False"
        and row["financing_enabled"] == "False"
        and float(row["scale_change_cost"]) == 0
        and float(row["financing_cost"]) == 0
        and float(row["cash_day_yield"]) == 0
        for row in rows
    )


def main() -> None:
    with OUT.open(encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    dates = [row["date"] for row in rows]
    assert dates == sorted(set(dates))
    assert len(rows) == 3988 and dates[-1] == "2026-09-30"
    assert all(row["overheat_kind"] == "vol" for row in rows)
    assert all(int(row["overheat_feature_window"]) == WINDOW for row in rows)
    assert all(float(row["overheat_trigger_threshold"]) == TRIGGER for row in rows)
    assert all(float(row["overheat_recovery_threshold"]) == RECOVER for row in rows)
    assert all(float(row["r2_entry_gate"]) == 0 for row in rows)
    assert disabled_modules_are_off(rows)
    injected_row = dict(rows[0])
    injected_row["target_vol_enabled"] = "True"
    assert not disabled_modules_are_off([injected_row] + rows[1:])

    nav = [float(row["spread_nav"]) for row in rows]
    values = std_feature(nav)
    feature_mismatch = []
    max_feature_error = 0.0
    for i, (row, value) in enumerate(zip(rows, values, strict=True)):
        saved = row["overheat_feature_value"]
        if value is None:
            if saved:
                feature_mismatch.append(dates[i])
        elif not saved:
            feature_mismatch.append(dates[i])
        else:
            error = abs(value - float(saved))
            max_feature_error = max(error, max_feature_error)
            if error > 1e-11:
                feature_mismatch.append(dates[i])
    expected = replay(rows, values)
    fields = [
        "holding", "next_holding", "overheat_risk_off", "overheat_exit_triggered",
        "overheat_reentry_triggered", "overheat_block_entry_triggered",
    ]
    mismatches: dict[str, list[str]] = {k: [] for k in fields}
    event_dates: dict[str, list[str]] = {k: [] for k in fields[3:]}
    for i, (row, state) in enumerate(zip(rows, expected, strict=True)):
        current, nxt, risk_off, exit_event, reentry_event, blocked_event = state
        actual = (
            is_active(row["holding"]), is_active(row["next_holding"]),
            flag(row["overheat_risk_off"]), flag(row["overheat_exit_triggered"]),
            flag(row["overheat_reentry_triggered"]), flag(row["overheat_block_entry_triggered"]),
        )
        for j, key in enumerate(fields):
            if actual[j] != state[j]:
                mismatches[key].append(dates[i])
        for j, key in enumerate(fields[3:], start=3):
            if state[j]:
                event_dates[key].append(dates[i])
        if i:
            assert current == expected[i - 1][1]
    no_overlay = [is_active(row["base_next_holding"]) for row in rows]
    first_next_difference = next((dates[i] for i, state in enumerate(expected) if state[1] != no_overlay[i]), None)
    next_difference_days = sum(state[1] != no_overlay[i] for i, state in enumerate(expected))
    current_difference_days = sum(state[0] != is_active(row["base_holding"]) for state, row in zip(expected, rows, strict=True))
    blocked_cash_return_violations = [
        dates[i] for i, (state, row) in enumerate(zip(expected, rows, strict=True))
        if not state[0] and abs(float(row["base_pre_cost_return"])) > 1e-12
    ]
    # On a current cash day, a close-executed entry cost may produce a negative
    # net return; positive market P&L is forbidden.
    cash_positive_market_pnl = [
        dates[i] for i, (state, row) in enumerate(zip(expected, rows, strict=True))
        if not state[0] and float(row["base_pre_cost_return"]) > 1e-12
    ]

    # Mutations: borrowing tomorrow's feature and lagging by one completed row.
    lead_values = values[1:] + [values[-1]]
    lag_values = [None] + values[:-1]
    lead_state = replay(rows, lead_values)
    lag_state = replay(rows, lag_values)
    lead_mismatch = [dates[i] for i in range(len(rows)) if lead_state[i] != expected[i]]
    lag_mismatch = [dates[i] for i in range(len(rows)) if lag_state[i] != expected[i]]

    # The 2018-06-18 inserted holiday is isolated here only at feature level.
    phantom_index = dates.index("2018-06-18")
    clean_rows = rows[:phantom_index] + rows[phantom_index + 1 :]
    clean_nav = nav[:phantom_index] + nav[phantom_index + 1 :]
    clean_feature = std_feature(clean_nav)
    clean_dates = [r["date"] for r in clean_rows]
    original_by_date = dict(zip(dates, values, strict=True))
    feature_changed = [
        date for date, value in zip(clean_dates, clean_feature, strict=True)
        if value is not None and original_by_date[date] is not None
        and abs(value - original_by_date[date]) > 1e-12
    ]
    clean_state = replay(clean_rows, clean_feature)
    old_state_by_date = dict(zip(dates, expected, strict=True))
    holiday_fixed_base_next_diffs = [
        date for date, state in zip(clean_dates, clean_state, strict=True)
        if state != old_state_by_date[date]
    ]

    # Rebuild the real v2.3 score input from the same frozen close prices after
    # removing the holiday. This remains a local calendar-only counterfactual.
    old_rebuilt_nav = close_spread_nav(rows, nav[0])
    old_nav_error = max(abs(a - b) for a, b in zip(old_rebuilt_nav, nav, strict=True))
    old_scores = wls_scores(old_rebuilt_nav)
    score_error = max(
        abs(score - float(row["annualized_log_wls_score"]))
        for score, row in zip(old_scores, rows, strict=True) if score is not None
    )
    clean_rebuilt_nav = close_spread_nav(clean_rows, nav[0])
    clean_scores = wls_scores(clean_rebuilt_nav)
    clean_score_by_date = dict(zip(clean_dates, clean_scores, strict=True))
    old_score_by_date = {row["date"]: float(row["annualized_log_wls_score"]) for row in rows}
    clean_score_changed = [
        date for date in clean_dates[phantom_index:]
        if clean_score_by_date[date] is not None
        and abs(clean_score_by_date[date] - old_score_by_date[date]) > 1e-10
    ]
    clean_signal_rows = [dict(row) for row in clean_rows]
    base_active = is_active(clean_signal_rows[phantom_index - 1]["base_next_holding"])
    for i in range(phantom_index, len(clean_signal_rows)):
        score = clean_scores[i]
        base_active = bool(score is not None and (score >= -0.08 if base_active else score > 0))
        clean_signal_rows[i]["base_next_holding"] = "long_microcap_short_zz1000" if base_active else "cash"
    corrected_base_next = [
        row["date"] for row in clean_signal_rows[phantom_index:]
        if row["base_next_holding"] != next(old for old in rows if old["date"] == row["date"])["base_next_holding"]
    ]
    clean_full_feature = std_feature(clean_rebuilt_nav)
    clean_full_state = replay(clean_signal_rows, clean_full_feature)
    clean_full_state_diffs = [
        date for date, state in zip(clean_dates[phantom_index:], clean_full_state[phantom_index:], strict=True)
        if state != old_state_by_date[date]
    ]

    # Exact threshold behavior and missing warmup cannot accidentally trigger.
    below_trigger = math.nextafter(TRIGGER, -math.inf)
    above_recover = math.nextafter(RECOVER, math.inf)
    boundary = {
        "exact_trigger_current_active_exits": decide(True, True, False, TRIGGER)[2],
        "just_below_trigger_does_not_exit": not decide(True, True, False, below_trigger)[2],
        "exact_trigger_cash_blocks": decide(True, False, False, TRIGGER)[4],
        "exact_recovery_reenters": decide(True, False, True, RECOVER)[3],
        "just_above_recovery_stays_off": decide(True, False, True, above_recover)[1],
        "missing_feature_does_not_trigger": not decide(True, True, False, None)[2],
    }
    assert all(boundary.values())

    result = {
        "scope": "frozen-old-input v2.3 diagnostic only; independent stdlib readback, no formal refresh",
        "source": str(OUT.relative_to(ROOT)).replace("\\", "/"),
        "sha256": hashlib.sha256(OUT.read_bytes()).hexdigest(),
        "current_source_sha256": hashlib.sha256(SOURCE.read_bytes()).hexdigest(),
        "rows": len(rows), "first_date": dates[0], "last_date": dates[-1],
        "effective_parameters": {"vol_window": WINDOW, "annualization_days": TRADING_DAYS, "trigger": TRIGGER, "recovery": RECOVER, "r2_entry_gate": 0.0},
        "feature": {"first_finite_date": dates[WINDOW - 1], "max_abs_error": max_feature_error, "mismatch_count": len(feature_mismatch), "mismatch_examples": feature_mismatch[:10]},
        "state_mismatch_count": {key: len(value) for key, value in mismatches.items()},
        "state_mismatch_examples": {key: value[:5] for key, value in mismatches.items()},
        "events": {key: {"count": len(value), "first": value[:5], "last": value[-5:]} for key, value in event_dates.items()},
        "ablation": {"first_next_state_difference": first_next_difference, "next_state_difference_days": next_difference_days, "current_state_difference_days": current_difference_days},
        "first_exit_clock": {
            "date": "2020-09-10",
            "feature": float(rows[dates.index("2020-09-10")]["overheat_feature_value"]),
            "holding_on_event_day": rows[dates.index("2020-09-10")]["holding"],
            "next_holding_on_event_day": rows[dates.index("2020-09-10")]["next_holding"],
            "exit_cost_on_event_day": float(rows[dates.index("2020-09-10")]["entry_exit_cost"]),
            "return_net_on_event_day": float(rows[dates.index("2020-09-10")]["return_net"]),
            "holding_next_session": rows[dates.index("2020-09-11")]["holding"],
            "market_return_while_cash_next_session": float(rows[dates.index("2020-09-11")]["base_pre_cost_return"]),
        },
        "timing_mutants": {"tomorrow_feature_mismatch_count": len(lead_mismatch), "tomorrow_first": lead_mismatch[:5], "extra_lag_mismatch_count": len(lag_mismatch), "extra_lag_first": lag_mismatch[:5]},
        "holiday": {"phantom_date": "2018-06-18", "feature_changed_count_fixed_stored_spread_nav": len(feature_changed), "first_feature_change": feature_changed[:5], "last_feature_change": feature_changed[-5:], "fixed_base_next_state_difference_count": len(holiday_fixed_base_next_diffs), "fixed_base_next_first_differences": holiday_fixed_base_next_diffs[:5], "close_rebuild_old_nav_max_abs_error": old_nav_error, "close_rebuild_old_wls_max_abs_error_after_25_rows": score_error, "close_rebuild_removed_holiday_score_changed_count": len(clean_score_changed), "close_rebuild_removed_holiday_first_score_changes": clean_score_changed[:5], "close_rebuild_removed_holiday_last_score_changes": clean_score_changed[-5:], "close_rebuild_removed_holiday_base_next_signal_difference_count": len(corrected_base_next), "close_rebuild_removed_holiday_first_base_next_signal_differences": corrected_base_next[:10], "close_rebuild_removed_holiday_overheat_state_difference_count": len(clean_full_state_diffs), "close_rebuild_removed_holiday_first_overheat_state_differences": clean_full_state_diffs[:10], "qualification": "Calendar-only counterfactual rebuilds v2.3 WLS from frozen old close data, not PIT members/corporate actions or T+1 account; formal corrected result requires upstream rebuild."},
        "cash": {"nonzero_base_pre_cost_return_cash_days": len(blocked_cash_return_violations), "positive_market_pnl_cash_days": len(cash_positive_market_pnl)},
        "boundary_injections": boundary,
        "disabled_module_mutant_rejected": not disabled_modules_are_off([injected_row] + rows[1:]),
    }
    HERE.mkdir(parents=True, exist_ok=True)
    (HERE / "audit_result.json").write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
