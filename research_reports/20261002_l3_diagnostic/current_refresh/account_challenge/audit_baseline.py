"""Independent, read-only L3 low-complexity baseline arithmetic challenge.

This intentionally imports no strategy or project metric function. It reads the
saved proxy close, hedge close and rebalance event ledger, then reconstructs the
plain Mom16 / 0.8 hedge / costed proportional return path. The reconstruction
is a diagnostic of old paper inputs, not a share-level self-financing account.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
from datetime import date
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
HERE = Path(__file__).resolve().parent
PATHS = {
    "proxy": ROOT / "outputs/wind_microcap_top_100_biweekly_thursday_16y_cached.csv",
    "panel": ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_panel_refreshed.csv",
    "turnover": ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_turnover.csv",
    "base": ROOT / "outputs/microcap_top100_mom16_hedge_zz1000_0p8x_biweekly_thursday_16y_v2_0_base_costed_nav.csv",
    "v2_0": ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_nav.csv",
}
PREVIOUS_AUDIT = ROOT / "research_reports/20261002_l3_diagnostic/account_challenge/result.json"
CUTOFFS = ("2026-09-30",)
TOL = 1e-9


def load(name: str) -> list[dict[str, str]]:
    with PATHS[name].open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    key = "rebalance_date" if name == "turnover" else "date"
    dates = [row[key] for row in rows]
    if not dates or dates != sorted(set(dates)):
        raise ValueError(f"{name}: empty, duplicated or unordered dates")
    return rows


def value(row: dict[str, str], key: str) -> float:
    raw = row.get(key, "")
    if raw is None or not raw.strip():
        raise ValueError(f"missing {key} at {row.get('date') or row.get('rebalance_date')}")
    result = float(raw)
    if not math.isfinite(result):
        raise ValueError(f"nonfinite {key} at {row.get('date') or row.get('rebalance_date')}")
    return result


def years_ago(end: date, years: int) -> date:
    try:
        return end.replace(year=end.year - years)
    except ValueError:
        return end.replace(year=end.year - years, day=28)


def metrics(rows: list[dict], years: int | None) -> dict:
    if years is not None:
        lower = years_ago(date.fromisoformat(rows[-1]["date"]), years).isoformat()
        rows = [row for row in rows if row["date"] >= lower]
    capital = 1.0
    peak = 1.0
    max_dd = 0.0
    trough = None
    for row in rows:
        capital *= 1.0 + row["return_net"]
        peak = max(peak, capital)
        dd = capital / peak - 1.0
        if dd < max_dd:
            max_dd = dd
            trough = row["date"]
    n = len(rows)
    return {
        "first": rows[0]["date"], "last": rows[-1]["date"], "return_days": n,
        "terminal_nav_window_anchor_1": capital,
        "annualized_244_N": capital ** (244.0 / n) - 1.0,
        "max_drawdown_initial_1": max_dd,
        "max_drawdown_trough": trough,
        "cost_days": sum(row["cost"] > 0 for row in rows),
        "sum_daily_cost_rate": sum(row["cost"] for row in rows),
    }


def reconstruct(cutoff: str, data: dict[str, list[dict[str, str]]]) -> dict:
    proxy = [row for row in data["proxy"] if row["date"] <= cutoff]
    panel = {row["date"]: row for row in data["panel"] if row["date"] <= cutoff}
    base = {row["date"]: row for row in data["base"] if row["date"] <= cutoff}
    turn = [row for row in data["turnover"] if row["rebalance_date"] <= cutoff]
    strategy = [row for row in data["v2_0"] if row["date"] <= cutoff]
    if any(row["execution_timing"] != "close" for row in turn):
        raise ValueError("unexpected turnover clock")
    if any(row["execution_date"][:10] != row["rebalance_date"] for row in turn):
        raise ValueError("unexpected turnover execution date")
    turnover_cost = {}
    for row in turn:
        expected = .003 * (int(row["entry_count"]) + int(row["exit_count"])) / 100
        if abs(value(row, "two_side_cost_rate") - expected) > TOL:
            raise ValueError(f"turnover rate mismatch: {row['rebalance_date']}")
        execution_day = row["execution_date"][:10]
        turnover_cost[execution_day] = turnover_cost.get(execution_day, 0) + expected
    paired = [(row["date"], value(row, "close"), value(panel[row["date"]], "1.000852"))
              for row in proxy if row["date"] in panel and panel[row["date"]].get("1.000852", "").strip()]
    if len(paired) <= 17:
        raise ValueError("too few paired closes")
    rows = []
    held = False
    capital = 1.0
    maxima = {"close": 0.0, "micro_return": 0.0, "hedge_return": 0.0,
              "momentum_gap": 0.0, "gross": 0.0, "cost": 0.0,
              "net": 0.0, "nav": 0.0}
    bad_dates = {key: None for key in maxima}
    state_mismatches = []
    for i in range(17, len(paired)):
        dt, micro, hedge = paired[i]
        prev_micro, prev_hedge = paired[i - 1][1:]
        old_micro, old_hedge = paired[i - 16][1:]
        micro_ret = micro / prev_micro - 1.0
        hedge_ret = hedge / prev_hedge - 1.0
        micro_mom = micro / old_micro - 1.0
        hedge_mom = hedge / old_hedge - 1.0
        signal = micro_mom > hedge_mom  # 1.0 hedge momentum in signal
        gross = micro_ret - .8 * hedge_ret - .00024 if held else 0.0
        entry_exit = .003 if signal != held else 0.0
        rebal = turnover_cost.get(dt, 0.0) if held and signal else 0.0
        fee = entry_exit + rebal
        net = (1.0 + gross) * (1.0 - fee) - 1.0
        capital *= 1.0 + net
        saved = base.get(dt)
        if saved is None:
            raise ValueError(f"base row missing: {dt}")
        expected_values = {
            "close": (micro, value(saved, "microcap_close")),
            "micro_return": (micro_ret, value(saved, "microcap_ret")),
            "hedge_return": (hedge_ret, value(saved, "hedge_ret")),
            "momentum_gap": (micro_mom - hedge_mom, value(saved, "momentum_gap")),
            "gross": (gross, value(saved, "return")),
            "cost": (fee, value(saved, "total_cost")),
            "net": (net, value(saved, "return_net")),
            "nav": (capital, value(saved, "nav_net")),
        }
        for name, (actual, reference) in expected_values.items():
            err = abs(actual - reference)
            if err > maxima[name]:
                maxima[name], bad_dates[name] = err, dt
        expected_held = "long_microcap_short_zz1000" if held else "cash"
        expected_next = "long_microcap_short_zz1000" if signal else "cash"
        if saved["holding"] != expected_held or saved["next_holding"] != expected_next:
            state_mismatches.append(dt)
        rows.append({"date": dt, "return_net": net, "gross": gross, "cost": fee,
                     "nav": capital, "holding": expected_held, "next_holding": expected_next})
        held = signal
    if [row["date"] for row in rows] != list(base):
        raise ValueError("reconstructed/base date alignment mismatch")
    first_cost = next(row for row in rows if row["cost"] > 0)
    omitted_fee_return = first_cost["gross"]
    fee_injection_delta = abs(omitted_fee_return - first_cost["return_net"])
    first = rows[0]
    calendar_days = (date.fromisoformat(rows[-1]["date"]) - date.fromisoformat(first["date"])).days
    full = metrics(rows, None)
    calendar_annualized = full["terminal_nav_window_anchor_1"] ** (365.25 / calendar_days) - 1.0
    windows = {"full": full, **{f"{n}Y": metrics(rows, n) for n in (10, 5, 3, 1)}}
    strategy_base_common = [r for r in strategy if r["date"] in base]
    strategy_diff_rows = [r["date"] for r in strategy_base_common
                          if abs(value(r, "return_net") - value(base[r["date"]], "return_net")) > TOL]
    return {
        "cutoff": cutoff, "rows": len(rows), "first": rows[0]["date"], "last": rows[-1]["date"],
        "paired_close_rows": len(paired), "proxy_rows": len(proxy), "turnover_events": len(turn),
        "turnover_last_event": turn[-1]["rebalance_date"],
        "base_max_abs_error": maxima, "base_max_error_dates": bad_dates,
        "base_state_mismatch_count": len(state_mismatches),
        "base_first_state_mismatch": state_mismatches[0] if state_mismatches else None,
        "reconstruction_pass": all(x <= TOL for x in maxima.values()) and not state_mismatches,
        "v2_0_strategy_rows": len(strategy),
        "v2_0_vs_plain_baseline_different_return_days": len(strategy_diff_rows),
        "v2_0_vs_plain_baseline_first_difference": strategy_diff_rows[0] if strategy_diff_rows else None,
        "windows_conditional_arithmetic_only": windows,
        "fee_omission_counterexample": {
            "date": first_cost["date"], "expected_fee": first_cost["cost"],
            "return_error_if_omitted": fee_injection_delta, "rejected": fee_injection_delta > TOL},
        "first_day_denominator_counterexample": {
            "first_day_return": first["return_net"],
            "annualized_244_N": full["annualized_244_N"],
            "annualized_244_N_minus_1_using_same_terminal_nav":
                full["terminal_nav_window_anchor_1"] ** (244.0 / (len(rows) - 1)) - 1.0,
            "annualized_calendar_from_first_to_last": calendar_annualized,
        },
        "capital_account_certified": False,
    }


def main() -> None:
    data = {name: load(name) for name in PATHS}
    previous_sources = json.loads(PREVIOUS_AUDIT.read_text(encoding="utf-8"))["sources"]
    sources = {}
    for name, path in PATHS.items():
        key = "rebalance_date" if name == "turnover" else "date"
        raw = path.read_bytes()
        old = previous_sources[name]
        prefix_sha = hashlib.sha256(b"".join(raw.splitlines(keepends=True)[:-1])).hexdigest() if name != "turnover" else None
        append_only = (len(data[name]) == old["rows"] + 1 and prefix_sha == old["sha256"]) if name != "turnover" else (len(data[name]) == old["rows"] and hashlib.sha256(raw).hexdigest() == old["sha256"])
        sources[name] = {"path": str(path.relative_to(ROOT)).replace("\\", "/"),
                         "sha256": hashlib.sha256(raw).hexdigest(),
                         "rows": len(data[name]), "first": data[name][0][key],
                         "last": data[name][-1][key],
                         "previous_2026_09_29_sha256": old["sha256"],
                         "prefix_sha256_without_2026_09_30": prefix_sha,
                         "single_row_append_or_unchanged_verified": append_only}
    if not all(src["single_row_append_or_unchanged_verified"] for src in sources.values()):
        raise ValueError("source lineage is not a single-row append of the frozen 2026-09-29 inputs")
    results = {"scope": "old-input conditional proportional-account arithmetic only",
               "sources": sources,
               "cutoffs": {cutoff: reconstruct(cutoff, data) for cutoff in CUTOFFS}}
    out = HERE / "result.json"
    out.write_text(json.dumps(results, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({cutoff: {"pass": result["reconstruction_pass"],
                               "rows": result["rows"],
                               "max_abs_error": max(result["base_max_abs_error"].values())}
                      for cutoff, result in results["cutoffs"].items()}, ensure_ascii=False))


if __name__ == "__main__":
    main()
