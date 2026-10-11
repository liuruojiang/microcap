"""Read-only comparison of frozen Top100 dates to two independent A-share calendars."""

import csv
import hashlib
import json
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path

import akshare as ak
import baostock as bs
import requests

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
BAO_CAL = ROOT / "research_reports/20261002_l2_full_repair/data/trade_calendar_evidence/baostock_trade_calendar_20100222_20260930.csv"
OUTPUTS = ROOT / "outputs"
ENGINE = ROOT / "research_reports/20261002_l2_full_repair/engine"
DATA = ROOT / "research_reports/20261002_l2_full_repair/data"
NAVS = [
    "microcap_top100_mom16_biweekly_live_v2_0_base_nav.csv",
    "microcap_top100_mom16_biweekly_live_v2_0_nav.csv",
    "microcap_top100_mom16_biweekly_live_v2_3_nav.csv",
    "microcap_top100_mom16_biweekly_live_v2_5_nav.csv",
]
TURNOVER = OUTPUTS / "microcap_top100_mom16_biweekly_live_v2_0_base_proxy_turnover.csv"
MEMBERS = OUTPUTS / "microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv"
RIGHTS = ENGINE / "full_history_rights_events.csv"
CANDIDATES = DATA / "event_candidates_2010_20260930.csv"
END = "2026-09-30"
SINA_URL = "https://finance.sina.com.cn/realstock/company/klc_td_sh.txt"
SSE_HOLIDAY_URL = "https://www.sse.com.cn/disclosure/announcement/general/c/c_20180606_4567900.shtml"
SZSE_HOLIDAY_URL = "https://www.szse.cn/disclosure/notice/general/t20171222_502255.html"


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(path):
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write(path, rows, fields):
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fields)
        writer.writeheader()
        writer.writerows(rows)


def main():
    HERE.mkdir(parents=True, exist_ok=True)
    raw = requests.get(SINA_URL, timeout=30)
    raw.raise_for_status()
    (HERE / "sina_trade_calendar_encoded_raw.txt").write_bytes(raw.content)
    sina_frame = ak.tool_trade_date_hist_sina()
    sina_days = {str(d) for d in sina_frame["trade_date"] if "2010-01-01" <= str(d) <= END}
    write(HERE / "sina_trade_dates_20100101_20260930.csv", [{"date": d} for d in sorted(sina_days)], ["date"])
    bao_rows = read(BAO_CAL)
    bao_days = {r["calendar_date"] for r in bao_rows if r["is_trading_day"] == "1"}
    comparable_sina = {d for d in sina_days if "2010-02-22" <= d <= END}
    source_calendar_disagreement = sorted(bao_days ^ comparable_sina)

    login = bs.login()
    assert login.error_code == "0", login.error_msg
    try:
        result = bs.query_history_k_data_plus("sh.000001", "date,code,close,preclose,volume,amount", start_date="2018-06-15", end_date="2018-06-20", frequency="d", adjustflag="3")
        assert result.error_code == "0", result.error_msg
        index_rows = []
        while result.next():
            index_rows.append(dict(zip(result.fields, result.get_row_data())))
    finally:
        bs.logout()
    assert [r["date"] for r in index_rows] == ["2018-06-15", "2018-06-19", "2018-06-20"]
    assert all(float(r["volume"]) > 0 for r in index_rows)
    write(HERE / "sh000001_20180615_20_baostock_real_trades.csv", index_rows, ["date", "code", "close", "preclose", "volume", "amount"])

    nav_counts = {}
    fake_rows = []
    input_hashes = {str(BAO_CAL.relative_to(ROOT)): sha(BAO_CAL), str(TURNOVER.relative_to(ROOT)): sha(TURNOVER), str(MEMBERS.relative_to(ROOT)): sha(MEMBERS), str(RIGHTS.relative_to(ROOT)): sha(RIGHTS), str(CANDIDATES.relative_to(ROOT)): sha(CANDIDATES)}
    for name in NAVS:
        path = OUTPUTS / name
        input_hashes[str(path.relative_to(ROOT))] = sha(path)
        rows = read(path)
        dates = [r["date"] for r in rows]
        assert len(dates) == len(set(dates)), name
        suspects = [r for r in rows if r["date"] not in sina_days]
        nav_counts[name] = {"rows": len(rows), "start": min(dates), "end": max(dates), "nontrading_dates": [r["date"] for r in suspects]}
        for r in suspects:
            fake_rows.append({"file": name, **{key: r.get(key, "") for key in ("date", "return_raw", "return", "return_net", "nav", "nav_net", "microcap_close", "microcap_ret", "hedge_close", "hedge_ret", "microcap_mom", "hedge_mom", "signal_on")}})
    assert {r["date"] for r in fake_rows} == {"2018-06-18"}
    fields = ["file", "date", "return_raw", "return", "return_net", "nav", "nav_net", "microcap_close", "microcap_ret", "hedge_close", "hedge_ret", "microcap_mom", "hedge_mom", "signal_on"]
    write(HERE / "nontrading_nav_rows.csv", fake_rows, fields)

    rebalance_rows = read(TURNOVER)
    member_rows = read(MEMBERS)
    rights_rows = read(RIGHTS)
    event_rows = read(CANDIDATES)
    affected = "2018-06-18"
    impacts = {
        "proxy_turnover_rebalances": [r for r in rebalance_rows if r["rebalance_date"] == affected],
        "proxy_ranked_members": sum(r["rebalance_date"] == affected for r in member_rows),
        "old_account_rights_phases": dict(Counter(r["phase"] for r in rights_rows if r["date"] == affected)),
        "normalized_events_ex_on_date": sum(r["ex_date"] == affected for r in event_rows),
        "normalized_events_record_on_date": sum(r["record_date"] == affected for r in event_rows),
    }
    report = {
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "scope": "frozen Top100 output dates 2010-2026; read-only lineage audit, no performance certification",
        "sina_calendar_source_url": SINA_URL,
        "akshare_function": "tool_trade_date_hist_sina; decodes Sina calendar; includes an unrelated 1992 date correction",
        "baostock_calendar_source": "query_trade_dates; frozen source file starts 2010-02-22",
        "sina_days_2010_01_01_to_2026_09_30": len(sina_days),
        "baostock_days_2010_02_22_to_2026_09_30": len(bao_days),
        "calendar_source_disagreement_from_2010_02_22": source_calendar_disagreement,
        "nav_scan": nav_counts,
        "all_suspected_nontrading_output_dates": sorted({r["date"] for r in fake_rows}),
        "first_difference": affected,
        "first_difference_weekday": date.fromisoformat(affected).strftime("%A"),
        "first_difference_in_sina_calendar": affected in sina_days,
        "first_difference_in_baostock_calendar": affected in bao_days,
        "exchange_holiday_sources": [SSE_HOLIDAY_URL, SZSE_HOLIDAY_URL],
        "sse_notice_2018_06_18": "Closed 2018-06-16 through 2018-06-18; 2018-06-19 opens",
        "index_real_trades_on_surrounding_days": index_rows,
        "legacy_actions_on_first_difference": impacts,
        "interpretation": "2018-06-18 is a synthetic zero-return row in frozen base and three formal NAV streams. It has no real market session or frozen proxy rebalance/rights event. Even with zero reported daily return, it changes rolling signal fields and date alignment; do not claim formal performance parity from deleting the row without a full lineage rebuild.",
        "input_sha256": input_hashes,
        "output_sha256": {name: sha(HERE / name) for name in ("sina_trade_calendar_encoded_raw.txt", "sina_trade_dates_20100101_20260930.csv", "sh000001_20180615_20_baostock_real_trades.csv", "nontrading_nav_rows.csv")},
    }
    (HERE / "calendar_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"calendar_disagreements": len(source_calendar_disagreement), "suspect_dates": report["all_suspected_nontrading_output_dates"], "nav_counts": nav_counts, "impacts": impacts, "report_sha256": sha(HERE / "calendar_report.json")}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
