"""Read-only source/time/account check for 600985 cash-plus-bonus real-trade days."""

import csv
import hashlib
import json
from decimal import Decimal as D, ROUND_HALF_UP, getcontext
from pathlib import Path

import baostock as bs
import fitz

getcontext().prec = 60
ROOT = Path(__file__).resolve().parents[1]
REPO = ROOT.parents[1]
ENGINE = ROOT / "engine"
PDF = ROOT / "data/suspension_ex_evidence/600985_20150703_issuer_mixed.pdf"
CANDIDATES = ROOT / "data/event_candidates_2010_20260930.csv"


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def rows(path):
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def near(a, b):
    assert abs(a-b) < D("1e-30"), (a, b)


def may_sell_new_shares(execution_date, bonus_shares):
    return execution_date >= "2015-07-13" and bonus_shares > 0


def market():
    login = bs.login()
    assert login.error_code == "0", login.error_msg
    try:
        fields = "date,close,preclose,volume,tradestatus"
        q = bs.query_history_k_data_plus("sh.600985", fields, start_date="2015-07-08", end_date="2015-07-14", frequency="d", adjustflag="3")
        assert q.error_code == "0", q.error_msg
        out = []
        while q.next():
            out.append(dict(zip(fields.split(","), q.get_row_data())))
        return {r["date"]: r for r in out}
    finally:
        bs.logout()


def main():
    manifest_path = ENGINE / "full_history_result.json"
    m = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert sha(ENGINE / "full_history.py") == m["code_sha256"], "stale_code_hash"
    for name, wanted in m["detail_sha256"].items():
        assert sha(ENGINE / name) == wanted, ("stale_detail_hash", name)
    for name, wanted in m["input_sha256"].items():
        assert sha(REPO / name) == wanted, ("stale_input_hash", name)
    assert sha(PDF) == "8dd86ad3ca9344131809e1a38c1e0dd181b9426c0448a8de4d50f08980946f7c"
    with fitz.open(PDF) as doc:
        issuer = "".join(page.get_text() for page in doc)
    for phrase in ("证券代码：600985", "每股派发现金红利0.17 元", "每股转增0.5", "2015 年7 月9 日", "2015 年7 月10 日", "新增无限售条件流通股份上市日：2015 年7 月13 日", "将所分派股份直接记入股东账户"):
        assert phrase in issuer, phrase
    c = [x for x in rows(CANDIDATES) if x["symbol"] == "600985" and x["ex_date"] == "2015-07-10"]
    assert len(c) == 1
    c = c[0]
    assert (c["record_date"], c["cash_pay_date"], c["share_credit_date"], c["bonus_per_10"], c["cash_per_10_gross"], c["source_status"]) == ("2015-07-09", "2015-07-10", "", "5.0", "1.7", "two_source_match")
    bars = market()
    for date, close, volume in (("2015-07-09", "14.76", "5766024"), ("2015-07-10", "10.70", "2950679"), ("2015-07-13", "11.77", "6839410")):
        assert D(bars[date]["close"]) == D(close) and bars[date]["volume"] == volume and bars[date]["tradestatus"] == "1"
    reference = ((D(bars["2015-07-09"]["close"]) - D("0.17")) / D("1.5")).quantize(D("0.01"), rounding=ROUND_HALF_UP)
    assert reference == D(bars["2015-07-10"]["preclose"]) == D("9.73")
    rights = rows(ENGINE / "full_history_rights_events.csv")
    positions = rows(ENGINE / "full_history_daily_positions.csv")
    orders = rows(ENGINE / "full_history_settled_orders.csv")
    cases = {}
    for branch in ("prefunded", "netted"):
        pick_event = lambda date, phase: [r for r in rights if (r["branch"], r["symbol"], r["date"], r["phase"]) == (branch, "600985", date, phase)]
        record = pick_event("2015-07-09", "record")
        ex = pick_event("2015-07-10", "ex")
        paid = pick_event("2015-07-10", "cash_paid")
        transfer = pick_event("2015-07-13", "stock_credited")
        assert len(record) == len(ex) == len(paid) == len(transfer) == 1
        q = D(record[0]["record_shares"])
        bonus = D(record[0]["bonus_due"])
        cash = D(record[0]["cash_due"])
        near(bonus, q * D("0.5"))
        near(cash, q * D("0.17"))
        near(D(ex[0]["bonus_due"]), bonus)
        near(D(ex[0]["cash_due"]), cash)
        near(D(paid[0]["amount"]), cash)
        near(D(transfer[0]["shares"]), bonus)
        pos = lambda date, state: [r for r in positions if (r["branch"], r["symbol"], r["date"], r["state"]) == (branch, "600985", date, state)]
        old, ex_old, ex_pending, release = (pos(*k) for k in (("2015-07-09", "settled_shares"), ("2015-07-10", "settled_shares"), ("2015-07-10", "pending_bonus_shares"), ("2015-07-13", "settled_shares")))
        assert len(old) == len(ex_old) == len(ex_pending) == len(release) == 1
        assert not pos("2015-07-09", "pending_bonus_shares") and not pos("2015-07-13", "pending_bonus_shares")
        near(D(old[0]["shares"]), q)
        near(D(ex_old[0]["shares"]), q)
        near(D(ex_pending[0]["shares"]), bonus)
        near(D(release[0]["shares"]), q + bonus)
        near(D(old[0]["valuation_price"]), D("14.76"))
        near(D(ex_old[0]["valuation_price"]), D("10.70"))
        near(D(ex_pending[0]["valuation_price"]), D("10.70"))
        near(D(release[0]["valuation_price"]), D("11.77"))
        assert all(x["price_basis"] == "true_raw_close" for x in (old[0], ex_old[0], ex_pending[0], release[0]))
        ex_orders = [r for r in orders if r["branch"] == branch and r["symbol"] == "600985" and r["execution_date"] == "2015-07-10"]
        assert all(r["booked"] == "False" for r in ex_orders)
        assert all(r["side"] == "buy" for r in ex_orders)
        # A genuine future 7/10 sell of bonus shares would violate issuer listing clock
        # even though the security itself had volume that day.
        sell_new_shares_on_ex = {"execution_date": "2015-07-10", "side": "sell", "from_bonus": str(bonus), "booked": "True"}
        assert not may_sell_new_shares(sell_new_shares_on_ex["execution_date"], D(sell_new_shares_on_ex["from_bonus"]))
        assert may_sell_new_shares("2015-07-13", bonus)
        pre_value = q * D("14.76")
        ex_piece = (q + bonus) * D("10.70") + cash
        release_piece = (q + bonus) * D("11.77") + cash
        early_credit_same_value = q * D("10.70") + bonus * D("10.70") + cash
        near(ex_piece, early_credit_same_value)  # account arithmetic alone cannot reject premature tradability
        cases[branch] = {
            "record_shares": str(q), "bonus_pending_7_10": str(bonus), "gross_cash_paid_7_10": str(cash),
            "record_equity_piece": str(pre_value), "ex_equity_piece_with_pending_and_cash": str(ex_piece),
            "paper_release_equity_piece_7_13": str(release_piece), "booked_ex_orders": 0,
            "blocked_ex_buy_orders": len(ex_orders), "premature_bonus_sale_mutant_rejected_by_listing_clock": sell_new_shares_on_ex,
            "state_check_needed": "Early bonus credit has the same arithmetic equity and is caught only by entitlement/tradability state, not NAV summation.",
        }
    out = {
        "status": "PASS_600985_paper_pending_and_listing_clock_until_current_first_block",
        "manifest_sha256": sha(manifest_path), "code_sha256": m["code_sha256"],
        "issuer_pdf_sha256": sha(PDF), "candidate_sha256": sha(CANDIDATES),
        "true_trade_bars": {d: bars[d] for d in ("2015-07-09", "2015-07-10", "2015-07-13")},
        "ex_reference": str(reference), "accounts": cases,
        "first_blocks": {b: m["branches"][b]["first_block"] for b in cases},
        "limitations": "Issuer establishes listing eligibility on 2015-07-13, not actual broker credit. 2015-07-10 is a real-trade close for existing stock but newly converted shares remain paper pending. Fractional shares and gross cash are paper approximations.",
    }
    dest = Path(__file__).with_name("600985_pending_tradability_adversarial_audit.json")
    dest.write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
