"""Independent L3 audit of the frozen v2.0 proportional-paper baseline.

No strategy, executor, accounting, or metric helper is imported. Read-only
inputs; all evidence is written next to this script.
"""
from __future__ import annotations

import csv
import hashlib
import json
import math
from datetime import date
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
FILES = {
    "proxy": ROOT / "outputs/wind_microcap_top_100_biweekly_thursday_16y_cached.csv",
    "hedge": ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_panel_refreshed.csv",
    "rebalance": ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_turnover.csv",
    "base": ROOT / "outputs/microcap_top100_mom16_hedge_zz1000_0p8x_biweekly_thursday_16y_v2_0_base_costed_nav.csv",
    "v2": ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_nav.csv",
}
EXPECTED_SHA = {
    "proxy": "bf880cd9fc630091e5a16ef32db7f5977b26e57e900941f819a09914a3e64973",
    "hedge": "3a1b80b14438f1582826a8643a058372f48ede2aae5193a39bf38958793df054",
    "rebalance": "602a23b38fe7a8274c6358e8ffdb4c78620b0c745be9dffea46cb7790bf3828d",
    "base": "38555e86ea8ed754c0821b5aa54d34fb3265c546b966546cccbef1bbf8e87b45",
    "v2": "19447fc80b0c0b2422c47d6f73d31a2423bb178f154e5face8bb51aedcf19eca",
}
TOL = 1e-9


def read_file(name: str) -> tuple[list[dict[str, str]], dict]:
    raw = FILES[name].read_bytes()
    sha = hashlib.sha256(raw).hexdigest()
    if sha != EXPECTED_SHA[name]:
        raise RuntimeError(f"frozen {name} hash changed: {sha}")
    with FILES[name].open(encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    key = "rebalance_date" if name == "rebalance" else "date"
    days = [r[key] for r in rows]
    if not days or days != sorted(set(days)):
        raise RuntimeError(f"{name} date order / uniqueness failed")
    return rows, {"path": str(FILES[name].relative_to(ROOT)).replace("\\", "/"),
                  "sha256": sha, "rows": len(rows), "first": days[0], "last": days[-1]}


def number(row: dict[str, str], field: str) -> float:
    x = float(row[field])
    if not math.isfinite(x):
        raise RuntimeError(f"nonfinite {field} at {row.get('date')}")
    return x


def window_stats(rows: list[dict], years: int | None) -> dict:
    if years is not None:
        end = date.fromisoformat(rows[-1]["date"])
        try:
            start = end.replace(year=end.year - years)
        except ValueError:
            start = end.replace(year=end.year - years, day=28)
        rows = [r for r in rows if r["date"] >= start.isoformat()]
    wealth = peak = 1.0
    mdd = 0.0
    trough = None
    for r in rows:
        wealth *= 1 + r["net"]
        peak = max(peak, wealth)
        dd = wealth / peak - 1
        if dd < mdd:
            mdd, trough = dd, r["date"]
    return {"first": rows[0]["date"], "last": rows[-1]["date"],
            "N": len(rows), "terminal_nav": wealth,
            "annualized_244_over_N": wealth ** (244 / len(rows)) - 1,
            "mdd_including_initial_1": mdd, "trough": trough}


def main() -> None:
    loaded, sources = {}, {}
    for name in FILES:
        loaded[name], sources[name] = read_file(name)
    hedge = {r["date"]: number(r, "1.000852") for r in loaded["hedge"] if r["1.000852"]}
    closes = [(r["date"], number(r, "close"), hedge[r["date"]])
              for r in loaded["proxy"] if r["date"] in hedge]
    # Event ledger proves the old close execution and the stated two-side rate.
    event_fee = {}
    for e in loaded["rebalance"]:
        day = e["execution_date"][:10]
        if e["execution_timing"] != "close" or day != e["rebalance_date"]:
            raise RuntimeError(f"rebalance event clock mismatch: {day}")
        fee = .003 * (int(e["entry_count"]) + int(e["exit_count"])) / 100
        if abs(number(e, "two_side_cost_rate") - fee) > TOL:
            raise RuntimeError(f"rebalance event fee mismatch: {day}")
        event_fee[day] = event_fee.get(day, 0) + fee

    official = {r["date"]: r for r in loaded["v2"]}
    base = {r["date"]: r for r in loaded["base"]}
    failures = []
    first = {}
    maxima = {}
    held = False
    wealth = 1.0
    audit_rows = []
    wrong_signal_days = []
    off_fields = ["overheat_enabled", "target_vol_enabled", "peak_decay_enabled",
                  "net_drawdown_stop_enabled", "signal_quality_derisk_enabled",
                  "overheat_triggered", "signal_quality_derisk_triggered",
                  "blocked_until_signal_reset"]
    zero_fields = ["signal_quality_scale_cost", "target_vol_trade_cost",
                   "scale_change_cost", "financing_cost", "idle_cash_yield",
                   "target_vol_costed_turnover", "signal_quality_scale_turnover",
                   "target_vol_turnover"]
    off_residual = {k: [] for k in off_fields}
    zero_residual = {k: [] for k in zero_fields}
    for i in range(17, len(closes)):
        day, px, hx = closes[i]
        if day < sources["v2"]["first"]:
            continue
        prev_px, prev_hx = closes[i - 1][1:]
        hist_px, hist_hx = closes[i - 16][1:]
        micro_return = px / prev_px - 1
        hedge_return = hx / prev_hx - 1
        micro_mom = px / hist_px - 1
        hedge_mom = hx / hist_hx - 1
        signal = micro_mom > hedge_mom
        if (micro_mom > .8 * hedge_mom) != signal:
            wrong_signal_days.append(day)
        gross = micro_return - .8 * hedge_return - .00024 if held else 0.0
        state_fee = .003 if signal != held else 0.0
        member_fee = event_fee.get(day, 0.0) if held and signal else 0.0
        fee = state_fee + member_fee
        net = (1 + gross) * (1 - fee) - 1
        wealth *= 1 + net
        expected_state = "long_microcap_short_zz1000" if held else "cash"
        expected_next = "long_microcap_short_zz1000" if signal else "cash"
        saved = official.get(day)
        if saved is None:
            raise RuntimeError(f"official v2 row absent: {day}")
        actual = {"microcap_close": px, "hedge_close": hx,
                  "microcap_ret": micro_return, "hedge_ret": hedge_return,
                  "microcap_mom": micro_mom, "hedge_mom": hedge_mom,
                  "momentum_gap": micro_mom - hedge_mom,
                  "entry_exit_cost": state_fee, "rebalance_cost": member_fee,
                  "total_cost": fee, "base_pre_cost_return": gross,
                  "return_net": net, "nav_net": wealth}
        for key, predicted in actual.items():
            diff = abs(predicted - number(saved, key))
            maxima[key] = max(maxima.get(key, 0.0), diff)
            if diff > TOL:
                first.setdefault(key, {"date": day, "predicted": predicted,
                                       "saved": number(saved, key), "abs_error": diff})
        for key, expected in {"holding": expected_state, "next_holding": expected_next}.items():
            if saved[key] != expected:
                first.setdefault(key, {"date": day, "predicted": expected, "saved": saved[key]})
        for key in off_fields:
            if saved[key] not in ("False", "0", "0.0", ""):
                off_residual[key].append(day)
        for key in zero_fields:
            if abs(number(saved, key)) > TOL:
                zero_residual[key].append(day)
        audit_rows.append({"date": day, "net": net, "gross": gross, "cost": fee,
                           "signal": signal, "held": held, "nav": wealth})
        held = signal
    if [r["date"] for r in audit_rows] != list(official):
        raise RuntimeError("v2 date index differs from reconstructed index")
    common = [d for d in official if d in base]
    base_diff = [d for d in common if abs(number(official[d], "return_net") - number(base[d], "return_net")) > TOL]

    # Remove the independently proven 2018-06-18 non-session row. This probes
    # signal-window contamination, not an approved rewrite of formal history.
    clean = [x for x in closes if x[0] != "2018-06-18"]
    clean_signals = {}
    for i in range(16, len(clean)):
        d, px, hx = clean[i]
        clean_signals[d] = px / clean[i - 16][1] > hx / clean[i - 16][2]
    calendar_diffs = [d for d, s in clean_signals.items()
                      if d in official and d > "2018-06-18" and s != (official[d]["signal_on"] == "True")]
    clean_path = {}
    clean_held = False
    for i in range(17, len(clean)):
        d, px, hx = clean[i]
        if d < sources["v2"]["first"]:
            continue
        pm, ph = clean[i - 1][1:]
        lm, lh = clean[i - 16][1:]
        mm, hm = px / lm - 1, hx / lh - 1
        is_on = mm > hm
        gross = px / pm - 1 - .8 * (hx / ph - 1) - .00024 if clean_held else 0.0
        fee = (.003 if is_on != clean_held else 0.0)
        if clean_held and is_on:
            fee += event_fee.get(d, 0.0)
        clean_path[d] = {"micro_mom": mm, "hedge_mom": hm,
                         "gap": mm - hm, "held": clean_held, "next": is_on,
                         "gross": gross, "cost": fee,
                         "net": (1 + gross) * (1 - fee) - 1}
        clean_held = is_on
    trace_dates = ("2018-06-28", "2018-06-29", "2018-07-02")
    trace = {}
    for d in trace_dates:
        saved = official[d]
        trace[d] = {
            "old": {"gap": number(saved, "momentum_gap"),
                    "held": saved["holding"] == "long_microcap_short_zz1000",
                    "next": saved["next_holding"] == "long_microcap_short_zz1000",
                    "gross": number(saved, "base_pre_cost_return"),
                    "cost": number(saved, "total_cost"),
                    "net": number(saved, "return_net")},
            "remove_holiday_old_formula_only": clean_path[d],
        }
    clean_first_net_diff = next((d for d in clean_path if d in official
                                 and abs(clean_path[d]["net"] - number(official[d], "return_net")) > TOL), None)
    holiday = official["2018-06-18"]
    holiday_probe = {
        "holiday": "2018-06-18", "published_as_return_day": True,
        "published_net": number(holiday, "return_net"),
        "published_cost": number(holiday, "total_cost"),
        "clean_calendar_signal_difference_count": len(calendar_diffs),
        "first_clean_calendar_signal_difference": calendar_diffs[0] if calendar_diffs else None,
        "clean_calendar_signal_difference_dates": calendar_diffs,
        "first_clean_calendar_net_difference": clean_first_net_diff,
        "local_event_trace": trace,
    }
    windows = {label: window_stats(audit_rows, years) for label, years in
               (("full", None), ("10Y", 10), ("5Y", 5), ("3Y", 3), ("1Y", 1))}
    no_holiday = [r for r in audit_rows if r["date"] != "2018-06-18"]
    first_fee = next(r for r in audit_rows if r["cost"] > 0)
    sep22 = official["2026-09-22"]
    premature_gross = (number(sep22, "microcap_ret")
                       - .8 * number(sep22, "hedge_ret") - .00024)
    premature_net = (1 + premature_gross) * (1 - number(sep22, "total_cost")) - 1
    faults = {
        "wrong_signal_hedge_weight_0p8": {"changed_days": len(wrong_signal_days),
                                          "first": wrong_signal_days[0] if wrong_signal_days else None},
        "omit_first_fee": {"date": first_fee["date"], "fee": first_fee["cost"],
                           "one_day_net_error": abs(first_fee["gross"] - first_fee["net"])},
        "premature_same_signal_holding": {
            "date": "2026-09-22", "actual_holding": sep22["holding"],
            "actual_next_holding": sep22["next_holding"],
            "actual_net": number(sep22, "return_net"),
            "mutated_net_if_new_long_earns_whole_day": premature_net,
            "abs_error": abs(premature_net - number(sep22, "return_net"))},
        "N_minus_1_denominator": {
            "correct": windows["full"]["annualized_244_over_N"],
            "mutated": windows["full"]["terminal_nav"] ** (244 / (len(audit_rows) - 1)) - 1},
        "holiday_row_denominator_only": {
            "old_N": len(audit_rows), "clean_N": len(no_holiday),
            "old_annualized": windows["full"]["annualized_244_over_N"],
            "clean_same_returns_annualized": window_stats(no_holiday, None)["annualized_244_over_N"]},
    }
    result = {"scope": "frozen old proportion model, diagnostic only", "sources": sources,
              "replay": {"rows": len(audit_rows), "first": audit_rows[0]["date"],
                         "last": audit_rows[-1]["date"], "first_differences": first,
                         "max_abs_error": maxima, "pass": not first,
                         "off_module_residual_counts": {k: len(v) for k, v in off_residual.items()},
                         "zero_module_residual_counts": {k: len(v) for k, v in zero_residual.items()},
                         "cost_days": sum(r["cost"] > 0 for r in audit_rows),
                         "sum_cost_rates_not_cash_spent": sum(r["cost"] for r in audit_rows)},
              "same_window_v2_vs_base": {"common_days": len(common),
                                          "difference_days": len(base_diff),
                                          "first_difference": base_diff[0] if base_diff else None},
              "conditional_metrics_not_formal": windows,
              "calendar_counterexample": holiday_probe,
              "fault_injections": faults}
    (HERE / "result.json").write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps({"replay": result["replay"]["pass"], "rows": len(audit_rows),
                      "first_difference": first, "calendar_signal_diffs": len(calendar_diffs)},
                     ensure_ascii=False))


if __name__ == "__main__":
    main()
