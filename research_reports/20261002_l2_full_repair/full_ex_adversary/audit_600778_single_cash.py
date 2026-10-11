"""Independent exact-event admission and account audit of 600778 cash ex-date."""

import csv
import hashlib
import json
from decimal import Decimal as D, getcontext
from pathlib import Path

import fitz

getcontext().prec = 60
ROOT = Path(__file__).resolve().parents[1]
REPO = ROOT.parents[1]
PDF = ROOT / "data/suspension_ex_evidence/600778_20150624_issuer_dividend.pdf"
CNINFO = REPO / "research_reports/20260926_l1_repair/corporate_actions/cninfo_distribution_events.csv"
CANDIDATES = ROOT / "data/event_candidates_2010_20260930.csv"
ENGINE = ROOT / "engine"


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def rows(path):
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def near(a, b):
    assert abs(a-b) < D("1e-30"), (a, b)


def main():
    mpath = ENGINE / "full_history_result.json"
    m = json.loads(mpath.read_text(encoding="utf-8"))
    assert sha(ENGINE / "full_history.py") == m["code_sha256"]
    for name, value in m["detail_sha256"].items():
        assert sha(ENGINE / name) == value, name
    for name, value in m["input_sha256"].items():
        assert sha(REPO / name) == value, name
    assert sha(PDF) == "4665a8fae757f04fce8e4bd11713d6b6b485bf7737c1aa60efc43aad0189bb81"
    with fitz.open(PDF) as doc:
        text = "".join(page.get_text() for page in doc)
    for phrase in ("证券代码：600778", "每股现金红利（扣税前）", "0.09", "2015 年6 月30 日", "2015 年7 月1 日", "每10股派发现金红利0.9元"):
        assert phrase in text, phrase
    o = [r for r in rows(CNINFO) if r["symbol"] == "600778" and r["除权日"] == "2015-07-01"]
    c = [r for r in rows(CANDIDATES) if r["symbol"] == "600778" and r["ex_date"] == "2015-07-01"]
    assert len(o) == len(c) == 1
    o, c = o[0], c[0]
    assert (o["实施方案公告日期"], o["股权登记日"], o["除权日"], o["派息日"], o["派息比例"]) == (c["announcement_date"], c["record_date"], c["ex_date"], c["cash_pay_date"], c["cash_per_10_gross"])
    assert (c["announcement_date"], c["record_date"], c["ex_date"], c["cash_pay_date"], c["cash_per_10_gross"], c["source_status"], c["source_rows"]) == ("2015-06-24", "2015-06-30", "2015-07-01", "2015-07-01", "0.9", "single_source_only", "cninfo=1;sina=0")
    assert not c["rights_per_10"] and D(c["bonus_per_10"] or "0") == 0
    line_sha = hashlib.sha256(json.dumps(o, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()
    marks = [r for r in m["cash_halt_marks_used"] if r["symbol"] == "600778" and r["ex_date"] == "2015-07-01"]
    assert len(marks) == 1
    mark = marks[0]
    assert mark["cninfo_original_row_sha256"] == line_sha
    assert mark["issuer_sha256"] == sha(PDF)
    assert (mark["last_true_trade"], mark["resume_true_trade"], mark["raw_anchor"], mark["theoretical_mark"], mark["resume_preclose"]) == ("2015-06-01", "2015-10-13", "16.1", "16.01", "16.0100")
    rights = rows(ENGINE / "full_history_rights_events.csv")
    positions = rows(ENGINE / "full_history_daily_positions.csv")
    orders = rows(ENGINE / "full_history_settled_orders.csv")
    branches = {}
    for branch in ("prefunded", "netted"):
        r = {phase: [x for x in rights if (x["branch"], x["symbol"], x["phase"], x["date"]) == (branch, "600778", phase, date)] for phase, date in (("record", "2015-06-30"), ("ex", "2015-07-01"), ("cash_paid", "2015-07-01"))}
        assert all(len(x) == 1 for x in r.values())
        q = D(r["record"][0]["record_shares"])
        cash = D(r["record"][0]["cash_due"])
        near(cash, q * D("0.09"))
        near(D(r["ex"][0]["cash_due"]), cash)
        near(D(r["cash_paid"][0]["amount"]), cash)
        ps = {date: [x for x in positions if (x["branch"], x["symbol"], x["state"], x["date"]) == (branch, "600778", "settled_shares", date)] for date in ("2015-06-30", "2015-07-01")}
        assert all(len(x) == 1 for x in ps.values())
        old, new = ps["2015-06-30"][0], ps["2015-07-01"][0]
        near(D(old["shares"]), q)
        near(D(new["shares"]), q)
        near(D(old["valuation_price"]), D("16.1"))
        near(D(new["valuation_price"]), D("16.01"))
        near(q*D("16.1"), q*D("16.01")+cash)
        booked = [x for x in orders if x["branch"] == branch and x["symbol"] == "600778" and x["execution_date"] == "2015-07-01" and x["booked"] == "True"]
        assert not booked
        mutations = {
            "candidate_cash_per_10_1p0": D("1.0") != D(o["派息比例"]),
            "raw_official_cash_per_10_1p0": D("1.0") != D(c["cash_per_10_gross"]),
            "wrong_record_day": "2015-06-29" != o["股权登记日"],
            "double_credit": abs(q*D("16.01")+2*cash-q*D("16.1")) > D("1"),
            "missing_cash": abs(q*D("16.01")-q*D("16.1")) > D("1"),
            "old_mark_plus_cash": abs(q*D("16.1")+cash-q*D("16.1")) > D("1"),
        }
        assert all(mutations.values())
        branches[branch] = {"record_shares": str(q), "gross_cash_paid": str(cash), "old_stock_value": str(q*D("16.1")), "ex_stock_plus_cash": str(q*D("16.01")+cash), "mutations_rejected": mutations, "booked_ex_orders": 0}
    out = {
        "status": "PASS_600778_exact_event_until_current_first_block",
        "manifest_sha256": sha(mpath), "source_sha256": {"issuer_pdf": sha(PDF), "cninfo_file": sha(CNINFO), "cninfo_row": line_sha, "candidate_file": sha(CANDIDATES)},
        "accounts": branches, "first_blocks": {b: m["branches"][b]["first_block"] for b in branches},
        "limitations": "Gross paper cash, not verified investor-specific net payment. Zero-volume reference mark is not an executable close; 2015-10-13 preclose is future corroboration only.",
    }
    dest = Path(__file__).with_name("600778_cash_adversarial_audit.json")
    dest.write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
