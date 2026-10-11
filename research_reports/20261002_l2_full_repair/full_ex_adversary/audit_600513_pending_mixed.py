"""Independent source/real-price/account/tradability check for 600513."""

import csv
import hashlib
import json
from decimal import Decimal as D, ROUND_HALF_UP, getcontext
from pathlib import Path

import baostock as bs
import fitz

getcontext().prec = 60
HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
REPO = ROOT.parents[1]
ENGINE = ROOT / "engine"
PDF = ROOT / "data/suspension_ex_evidence/600513_20160630_issuer_mixed.pdf"
CANDIDATES = ROOT / "data/event_candidates_2010_20260930.csv"


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def rows(path):
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def one(items, **key):
    found = [r for r in items if all(r[k] == value for k, value in key.items())]
    assert len(found) == 1, (key, len(found))
    return found[0]


def near(a, b, label):
    assert abs(a-b) < D("1e-30"), (label, str(a), str(b))


def market():
    login = bs.login()
    assert login.error_code == "0", login.error_msg
    try:
        fields = "date,close,preclose,volume,tradestatus"
        query = bs.query_history_k_data_plus("sh.600513", fields, start_date="2016-07-05", end_date="2016-07-07", frequency="d", adjustflag="3")
        assert query.error_code == "0", query.error_msg
        result = {}
        while query.next():
            item = dict(zip(fields.split(","), query.get_row_data()))
            result[item["date"]] = item
        return result
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
    assert sha(PDF) == "afe4005e2a3182d819343fb0b990504335fa6126458b5bc0561ab67d4850464a"
    with fitz.open(PDF) as doc:
        source = "".join(page.get_text() for page in doc)
    for phrase in ("证券代码：600513", "每10股派发现金红利0.6元", "每10股转增3股", "股权登记日：2016年7月5日", "除权除息日：2016年7月6日", "新增无限售条件流通股份上市日：2016年7月7日"):
        assert phrase in source, phrase
    candidate = one(rows(CANDIDATES), symbol="600513", ex_date="2016-07-06")
    assert (candidate["record_date"], candidate["cash_pay_date"], candidate["share_credit_date"], D(candidate["bonus_per_10"]), D(candidate["cash_per_10_gross"]), candidate["source_status"]) == ("2016-07-05", "2016-07-06", "", D("3"), D("0.6"), "two_source_match")
    bars = market()
    for date, close, volume in (("2016-07-05", "19.77", "4973965"), ("2016-07-06", "15.61", "4510726"), ("2016-07-07", "15.44", "8047508")):
        assert D(bars[date]["close"]) == D(close) and bars[date]["volume"] == volume and bars[date]["tradestatus"] == "1"
    reference = ((D("19.77") - D("0.06")) / D("1.3")).quantize(D("0.01"), rounding=ROUND_HALF_UP)
    assert reference == D(bars["2016-07-06"]["preclose"]) == D("15.16")
    rights = rows(ENGINE / "full_history_rights_events.csv")
    positions = rows(ENGINE / "full_history_daily_positions.csv")
    orders = rows(ENGINE / "full_history_settled_orders.csv")
    out = {"manifest_sha256": sha(manifest_path), "code_sha256": m["code_sha256"], "issuer_pdf_sha256": sha(PDF), "candidate_sha256": sha(CANDIDATES), "market": bars, "reference": str(reference), "branches": {}, "first_blocks": {b: m["branches"][b]["first_block"] for b in ("prefunded", "netted")}}
    for branch in ("prefunded", "netted"):
        record = one(rights, branch=branch, symbol="600513", date="2016-07-05", phase="record")
        ex = one(rights, branch=branch, symbol="600513", date="2016-07-06", phase="ex")
        paid = one(rights, branch=branch, symbol="600513", date="2016-07-06", phase="cash_paid")
        credit = one(rights, branch=branch, symbol="600513", date="2016-07-07", phase="stock_credited")
        q = D(record["record_shares"])
        bonus = D(record["bonus_due"])
        cash = D(record["cash_due"])
        near(bonus, q * D("0.3"), "bonus_entitlement")
        near(cash, q * D("0.06"), "cash_entitlement")
        near(D(ex["bonus_due"]), bonus, "ex_bonus")
        near(D(ex["cash_due"]), cash, "ex_cash")
        near(D(paid["amount"]), cash, "paid")
        near(D(credit["shares"]), bonus, "credit")
        old = one(positions, branch=branch, symbol="600513", date="2016-07-05", state="settled_shares")
        ex_old = one(positions, branch=branch, symbol="600513", date="2016-07-06", state="settled_shares")
        ex_pending = one(positions, branch=branch, symbol="600513", date="2016-07-06", state="pending_bonus_shares")
        released = one(positions, branch=branch, symbol="600513", date="2016-07-07", state="settled_shares")
        assert not [r for r in positions if (r["branch"],r["symbol"],r["date"],r["state"]) == (branch,"600513","2016-07-07","pending_bonus_shares")]
        for got, expected, label in ((D(old["shares"]), q, "old"),(D(ex_old["shares"]), q, "ex_old"),(D(ex_pending["shares"]), bonus, "pending"),(D(released["shares"]), q+bonus, "release")):
            near(got, expected, label)
        for item, price in ((old,D("19.77")),(ex_old,D("15.61")),(ex_pending,D("15.61")),(released,D("15.44"))):
            assert D(item["valuation_price"]) == price and item["price_basis"] == "true_raw_close"
        ex_orders = [r for r in orders if (r["branch"],r["symbol"],r["execution_date"]) == (branch,"600513","2016-07-06")]
        assert not [r for r in ex_orders if r["booked"] == "True" and r["side"] == "sell"], ex_orders
        # Early release and pending valuation have identical NAV. The listing date catches the mutation.
        early_release_equity = (q+bonus)*D("15.61") + cash
        correct_pending_equity = q*D("15.61") + bonus*D("15.61") + cash
        near(early_release_equity, correct_pending_equity, "equal_nav_but_wrong_tradability")
        assert "2016-07-06" < "2016-07-07" and bonus > 0
        out["branches"][branch] = {"record_shares":str(q),"bonus_pending_7_6":str(bonus),"gross_cash_7_6":str(cash),"ex_equity_with_pending":str(correct_pending_equity),"booked_ex_orders":sum(x["booked"] == "True" for x in ex_orders),"premature_bonus_sale_mutant_rejected_by_listing_clock":str(bonus)}
    out["status"] = "PASS_PAPER_PENDING_AND_LISTING_CLOCK_UNTIL_CURRENT_FIRST_BLOCK"
    out["limitations"] = "The issuer's 7/7 unrestricted-share listing date is a conservative paper trading clock, not proof of actual broker credit. Fractional shares and gross cash are model approximations."
    path = HERE / "600513_pending_mixed_adversarial_audit.json"
    path.write_text(json.dumps(out, ensure_ascii=False, indent=2)+"\n",encoding="utf-8")
    print(json.dumps({"status":out["status"],"manifest_sha256":out["manifest_sha256"],"branches":out["branches"],"first_blocks":out["first_blocks"]},ensure_ascii=False))


if __name__ == "__main__":
    main()
