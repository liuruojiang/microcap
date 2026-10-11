"""Independently compare frozen proxy dates with BaoStock trade dates."""

import csv
import hashlib
import json
from collections import Counter
from datetime import date
from pathlib import Path

import baostock as bs

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
ROOT = HERE.parent
PROXY = REPO / "outputs/wind_microcap_top_100_biweekly_thursday_16y_cached.csv"
MEMBERS = REPO / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv"
ENGINE = ROOT / "engine"


def rows(path):
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def market_calendar(start, end):
    login = bs.login()
    assert login.error_code == "0", login.error_msg
    try:
        query = bs.query_trade_dates(start_date=start, end_date=end)
        assert query.error_code == "0", (query.error_code, query.error_msg)
        result = {}
        while query.next():
            item = dict(zip(query.fields, query.get_row_data()))
            result[item["calendar_date"]] = item["is_trading_day"] == "1"
        return result
    finally:
        bs.logout()


def main():
    proxy = rows(PROXY)
    dates = [r["date"] for r in proxy]
    assert dates == sorted(set(dates))
    calendar = market_calendar(dates[0], dates[-1])
    assert all(d in calendar for d in dates)
    nontrading = [r for r in proxy if not calendar[r["date"]]]
    daily = rows(ENGINE / "full_history_daily_account.csv")
    rights = rows(ENGINE / "full_history_rights_events.csv")
    orders = rows(ENGINE / "full_history_settled_orders.csv")
    members = rows(MEMBERS)
    bad = {r["date"] for r in nontrading}
    journal = {
        "daily_rows": [r for r in daily if r["date"] in bad],
        "rights_rows": [r for r in rights if r["date"] in bad],
        "signal_orders": [r for r in orders if r["signal_date"] in bad],
        "execution_orders": [r for r in orders if r["execution_date"] in bad],
        "booked_execution_orders": [r for r in orders if r["execution_date"] in bad and r["booked"] == "True"],
    }
    grouped = Counter(r["date"][:4] for r in nontrading)
    branches = {}
    for branch in ("prefunded","netted"):
        observed = [r["date"] for r in daily if r["branch"] == branch]
        if not observed:
            continue
        expected = [d for d in dates if observed[0] <= d <= observed[-1] and calendar[d]]
        branches[branch] = {"first":observed[0],"last":observed[-1],"observed_days":len(observed),"expected_trade_days":len(expected),"first_missing_trade_day":next((d for d in expected if d not in set(observed)),None),"first_nontrading_daily_row":next((d for d in observed if not calendar[d]),None)}
    out = {
        "status": "NONTRADING_ROWS_FOUND" if bad else "NO_NONTRADING_PROXY_ROWS",
        "source": "BaoStock query_trade_dates; independent of frozen proxy index and strategy accounts",
        "proxy_sha256": sha(PROXY),
        "engine_code_sha256": sha(ENGINE / "full_history.py"),
        "proxy_first": dates[0], "proxy_last": dates[-1], "proxy_rows": len(proxy),
        "nontrading_count": len(nontrading),
        "nontrading_dates": [r["date"] for r in nontrading],
        "nontrading_by_year": dict(sorted(grouped.items())),
        "first_nontrading_row": nontrading[0] if nontrading else None,
        "previous_and_next_for_first": None,
        "journal_counts": {k: len(v) for k, v in journal.items()},
        "member_nontrading_dates": sorted({r["rebalance_date"] for r in members if r["rebalance_date"] in bad}),
        "branches_against_trade_calendar": branches,
        "journal_first_examples": {k: v[:2] for k, v in journal.items()},
        "annualization_effect_if_252_day_count": "Adding non-trading daily rows inflates the observation denominator and changes 252-day annualization; exact performance effect requires a full corrected replay.",
    }
    if nontrading:
        first = nontrading[0]["date"]
        index = dates.index(first)
        out["previous_and_next_for_first"] = {"previous":proxy[index-1],"synthetic":proxy[index],"next":proxy[index+1]}
    (HERE/"proxy_nontrading_calendar_adversarial_audit.json").write_text(json.dumps(out,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    print(json.dumps({k:v for k,v in out.items() if k not in ("nontrading_dates","journal_first_examples","previous_and_next_for_first")},ensure_ascii=False,indent=2))


if __name__ == "__main__":
    main()
