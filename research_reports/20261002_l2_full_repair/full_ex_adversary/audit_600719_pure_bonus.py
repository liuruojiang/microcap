"""Independent paper pending/listing clock audit for 600719 2015 bonus shares."""

import csv
import hashlib
import json
from decimal import Decimal as D, getcontext
from pathlib import Path

import baostock as bs
import fitz

getcontext().prec = 60
ROOT = Path(__file__).resolve().parents[1]
REPO = ROOT.parents[1]
ENGINE = ROOT / "engine"
PDF = ROOT / "data/suspension_ex_evidence/600719_20151124_issuer_bonus.pdf"
CANDIDATES = ROOT / "data/event_candidates_2010_20260930.csv"


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def rows(path):
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def near(a, b):
    assert abs(a-b) < D("1e-30"), (a, b)


def market():
    login = bs.login()
    assert login.error_code == "0"
    try:
        fields = "date,close,preclose,volume,tradestatus"
        q = bs.query_history_k_data_plus("sh.600719", fields, start_date="2015-11-26", end_date="2015-12-02", frequency="d", adjustflag="3")
        assert q.error_code == "0", q.error_msg
        out = []
        while q.next():
            out.append(dict(zip(fields.split(","), q.get_row_data())))
        return {r["date"]: r for r in out}
    finally:
        bs.logout()


def main():
    mpath = ENGINE / "full_history_result.json"
    m = json.loads(mpath.read_text(encoding="utf-8"))
    assert sha(ENGINE / "full_history.py") == m["code_sha256"], "stale_code_hash"
    for name, wanted in m["detail_sha256"].items():
        assert sha(ENGINE / name) == wanted, ("stale_detail_hash", name)
    for name, wanted in m["input_sha256"].items():
        assert sha(REPO / name) == wanted, ("stale_input_hash", name)
    assert sha(PDF) == "91c32510cbabeb5c1a599c4b900d6c817da1b1503a83f1e40b2e786f7ee75248"
    with fitz.open(PDF) as doc:
        issuer = "".join(page.get_text() for page in doc)
    for phrase in ("证券代码：600719", "每10 股转增10 股", "2015 年11 月27 日", "2015 年11 月30 日", "2015 年12 月1 日", "直接记入公司股东账户"):
        assert phrase in issuer, phrase
    c = [r for r in rows(CANDIDATES) if r["symbol"] == "600719" and r["ex_date"] == "2015-11-30"]
    assert len(c) == 1
    c = c[0]
    assert (c["record_date"], c["cash_pay_date"], c["share_credit_date"], c["bonus_per_10"], c["cash_per_10_gross"], c["source_status"]) == ("2015-11-27", "", "", "10.0", "0.0", "two_source_match")
    bars = market()
    assert D(bars["2015-11-27"]["close"]) == D("15.22")
    assert D(bars["2015-11-30"]["preclose"]) == D("7.61")
    assert D(bars["2015-11-30"]["close"]) == D("7.64")
    assert D(bars["2015-12-01"]["close"]) == D("7.99")
    assert all(D(bars[d]["volume"]) > 0 and bars[d]["tradestatus"] == "1" for d in ("2015-11-27", "2015-11-30", "2015-12-01"))
    rights = rows(ENGINE / "full_history_rights_events.csv")
    positions = rows(ENGINE / "full_history_daily_positions.csv")
    orders = rows(ENGINE / "full_history_settled_orders.csv")
    posted = all(m["branches"][b]["last_settled_date"] >= "2015-12-01" for b in ("prefunded", "netted"))
    cases = {}
    for branch in ("prefunded", "netted"):
        event = lambda date, phase: [r for r in rights if (r["branch"], r["symbol"], r["date"], r["phase"]) == (branch, "600719", date, phase)]
        rec = event("2015-11-27", "record")
        assert len(rec) == 1
        qty, bonus = D(rec[0]["record_shares"]), D(rec[0]["bonus_due"])
        near(bonus, qty)
        assert D(rec[0]["cash_due"]) == 0
        pos = lambda date, state: [r for r in positions if (r["branch"], r["symbol"], r["date"], r["state"]) == (branch, "600719", date, state)]
        old = pos("2015-11-27", "settled_shares")
        assert len(old) == 1
        near(D(old[0]["shares"]), qty)
        near(D(old[0]["valuation_price"]), D("15.22"))
        info = {"record_shares": str(qty), "bonus_due": str(bonus), "record_value": str(qty * D("15.22"))}
        if posted:
            ex, credited = event("2015-11-30", "ex"), event("2015-12-01", "stock_credited")
            assert len(ex) == len(credited) == 1
            near(D(ex[0]["bonus_due"]), bonus)
            near(D(credited[0]["shares"]), bonus)
            ex_old, ex_pending, release = pos("2015-11-30", "settled_shares"), pos("2015-11-30", "pending_bonus_shares"), pos("2015-12-01", "settled_shares")
            assert len(ex_old) == len(ex_pending) == len(release) == 1
            assert not pos("2015-11-27", "pending_bonus_shares") and not pos("2015-12-01", "pending_bonus_shares")
            near(D(ex_old[0]["shares"]), qty)
            near(D(ex_pending[0]["shares"]), bonus)
            near(D(release[0]["shares"]), qty + bonus)
            near(D(ex_old[0]["valuation_price"]), D("7.64"))
            near(D(ex_pending[0]["valuation_price"]), D("7.64"))
            near(D(release[0]["valuation_price"]), D("7.99"))
            assert all(x["price_basis"] == "true_raw_close" for x in (old[0], ex_old[0], ex_pending[0], release[0]))
            ex_orders = [r for r in orders if r["branch"] == branch and r["symbol"] == "600719" and r["execution_date"] == "2015-11-30"]
            assert not [r for r in ex_orders if r["booked"] == "True" and r["side"] == "sell" and D(r["frozen_shares"]) > qty]
            early_sell_mutant = {"date": "2015-11-30", "sell_new_shares": str(bonus)}
            assert early_sell_mutant["date"] < "2015-12-01" and D(early_sell_mutant["sell_new_shares"]) > 0
            ex_piece = (qty + bonus) * D("7.64")
            early_credit_piece = (qty + bonus) * D("7.64")
            near(ex_piece, early_credit_piece)
            info.update({"ex_old_shares": ex_old[0]["shares"], "ex_pending_shares": ex_pending[0]["shares"], "ex_value_with_pending": str(ex_piece), "paper_release_shares": release[0]["shares"], "early_sell_mutant_rejected_by_listing_clock": early_sell_mutant, "ex_orders": ex_orders})
        cases[branch] = info
    result = {
        "status": "PASS_600719_source_and_paper_pending" if posted else "PASS_600719_source_record_only",
        "manifest_sha256": sha(mpath), "issuer_pdf_sha256": sha(PDF), "candidate_sha256": sha(CANDIDATES),
        "true_trade_bars": {d: bars[d] for d in ("2015-11-27", "2015-11-30", "2015-12-01")},
        "accounts": cases, "first_blocks": {b: m["branches"][b]["first_block"] for b in cases},
        "limitations": "Actual broker credit date remains unproved; transfer on 2015-12-01 is paper trade eligibility from issuer listing date. Real ex-date close 7.64, not 7.61 reference.",
    }
    dest = Path(__file__).with_name("600719_pending_bonus_adversarial_audit.json")
    dest.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
