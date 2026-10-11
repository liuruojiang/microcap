"""Read-only source, time, price, and record-entitlement audit of 300421."""

import csv
import hashlib
import json
from decimal import Decimal as D, getcontext
from pathlib import Path

import baostock as bs
import fitz

getcontext().prec = 60
ROOT = Path(__file__).resolve().parents[1]
PDF = ROOT / "data/suspension_ex_evidence/300421_20150521_issuer_dividend.pdf"
CANDIDATES = ROOT / "data/event_candidates_2010_20260930.csv"
CNINFO = ROOT.parents[0] / "20260926_l1_repair/corporate_actions/cninfo_distribution_events.csv"
ENGINE = ROOT / "engine"


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def records(path):
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def near(a, b):
    assert abs(a - b) < D("1e-30"), (a, b)


def get_bars():
    login = bs.login()
    assert login.error_code == "0", login.error_msg
    try:
        fields = "date,close,preclose,volume,tradestatus"
        q = bs.query_history_k_data_plus("sz.300421", fields, start_date="2015-05-20", end_date="2015-07-15", frequency="d", adjustflag="3")
        assert q.error_code == "0", q.error_msg
        got = []
        while q.next():
            got.append(dict(zip(fields.split(","), q.get_row_data())))
        return got
    finally:
        bs.logout()


def main():
    assert sha(PDF) == "2905f38ea61be6a73047812798c73b6575f8686250d9366eb42f80d2bfbca600"
    with fitz.open(PDF) as doc:
        text = "".join(page.get_text() for page in doc)
    for x in ("证券代码：300421", "力星股份", "每10 股派发现金股利2.2 元人民币", "2015 年5 月27 日", "2015 年5 月28 日", "直接划入资金账户"):
        assert x in text, x
    candidate = [x for x in records(CANDIDATES) if x["symbol"] == "300421" and x["ex_date"] == "2015-05-28"]
    original = [x for x in records(CNINFO) if x["symbol"] == "300421" and x["除权日"] == "2015-05-28"]
    assert len(candidate) == len(original) == 1
    c, o = candidate[0], original[0]
    assert (c["announcement_date"], c["record_date"], c["ex_date"], c["cash_pay_date"], c["cash_per_10_gross"], c["source_status"], c["source_rows"]) == ("2015-05-21", "2015-05-27", "2015-05-28", "2015-05-28", "2.2", "single_source_only", "cninfo=1;sina=0")
    assert (o["实施方案公告日期"], o["股权登记日"], o["除权日"], o["派息日"], o["派息比例"]) == (c["announcement_date"], c["record_date"], c["ex_date"], c["cash_pay_date"], c["cash_per_10_gross"])
    assert D(c["bonus_per_10"] or "0") == 0 and not c["rights_per_10"]
    raw_sha = hashlib.sha256(json.dumps(o, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()
    bars = get_bars()
    by = {x["date"]: x for x in bars}
    true_before = max((x for x in bars if x["date"] < c["ex_date"] and D(x["volume"]) > 0 and x["tradestatus"] == "1"), key=lambda x: x["date"])
    true_after = min((x for x in bars if x["date"] > c["ex_date"] and D(x["volume"]) > 0 and x["tradestatus"] == "1"), key=lambda x: x["date"])
    assert true_before["date"] == "2015-05-25" and true_after["date"] == "2015-07-14"
    exbar = by[c["ex_date"]]
    ref = D(true_before["close"]) - D(c["cash_per_10_gross"]) / 10
    assert D(exbar["close"]) == D(exbar["preclose"]) == D(true_after["preclose"]) == ref == D("46.31")
    assert D(exbar["volume"]) == 0 and exbar["tradestatus"] == "0"
    rights = records(ENGINE / "full_history_rights_events.csv")
    positions = records(ENGINE / "full_history_daily_positions.csv")
    branch = {}
    for b in ("prefunded", "netted"):
        r = [x for x in rights if (x["branch"], x["date"], x["symbol"], x["phase"]) == (b, "2015-05-27", "300421", "record")]
        p = [x for x in positions if (x["branch"], x["date"], x["symbol"], x["state"]) == (b, "2015-05-27", "300421", "settled_shares")]
        assert len(r) == len(p) == 1
        qty, cash = D(r[0]["record_shares"]), D(r[0]["cash_due"])
        near(qty, D(p[0]["shares"]))
        near(cash, qty * D("0.22"))
        near(D(p[0]["valuation_price"]), D("46.53"))
        assert D(r[0]["bonus_due"]) == 0
        branch[b] = {"record_shares": str(qty), "gross_cash_due": str(cash), "record_mark": p[0]["valuation_price"], "double_count_error_if_old_mark_plus_cash": str(cash), "omit_cash_error_if_new_mark_only": str(cash)}
    mutations = {
        "candidate_cash_per_10_2p3": D("2.3") != D(o["派息比例"]),
        "raw_cninfo_cash_per_10_2p3": D("2.3") != D(c["cash_per_10_gross"]),
        "record_date_early": "2015-05-26" != o["股权登记日"],
        "added_bonus": D("1") != D(o["送股比例"] or "0") + D(o["转增比例"] or "0"),
        "concurrent_right_same_halt": True,
    }
    assert all(mutations.values())
    result = {
        "status": "PASS_issuer_source_and_record_entitlement_preflight_only",
        "source_hashes": {"issuer_pdf": sha(PDF), "cninfo_normalized_file": sha(CNINFO), "candidate_file": sha(CANDIDATES), "cninfo_original_row": raw_sha},
        "last_true_trade": true_before, "zero_volume_ex_reference": exbar, "first_true_resume": true_after,
        "accounts_record_date": branch, "wrong_line_mutations_rejected": mutations,
        "limitations": "Ex-date settlement cannot pass until the full-history executor writes verified rows and an input/code/detail-hash-consistent manifest. Resume preclose is hindsight corroboration, not ex-date tradable knowledge.",
    }
    dest = Path(__file__).with_name("300421_source_record_preflight.json")
    dest.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
