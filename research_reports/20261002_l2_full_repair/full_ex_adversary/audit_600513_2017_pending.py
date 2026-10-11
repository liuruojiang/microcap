"""Read-only independent 2017 600513 mixed event and new-share listing audit."""

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
PDF = ROOT / "data/suspension_ex_evidence/600513_20170512_issuer_mixed.pdf"
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
        query = bs.query_history_k_data_plus("sh.600513", fields, start_date="2017-05-18", end_date="2017-05-22", frequency="d", adjustflag="3")
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
    assert sha(PDF) == "5ed5f2220804d8d71ac39061e9e73f3f4680c25b6c998122c018a777201d9338"
    with fitz.open(PDF) as doc:
        source = "".join(page.get_text() for page in doc)
    for phrase in ("证券代码：600513", "A 股每股现金红利0.086 元", "每股转增股份0.3 股", "2017/5/18", "2017/5/19", "2017/5/22"):
        assert phrase in source, phrase
    candidate = one(rows(CANDIDATES), symbol="600513", ex_date="2017-05-19")
    assert (candidate["record_date"], candidate["cash_pay_date"], candidate["share_credit_date"], D(candidate["bonus_per_10"]), D(candidate["cash_per_10_gross"]), candidate["source_status"]) == ("2017-05-18", "2017-05-19", "", D("3"), D("0.86"), "two_source_match")
    bars = market()
    for date, close, volume in (("2017-05-18", "13.39", "1726977"), ("2017-05-19", "10.29", "1169116"), ("2017-05-22", "9.95", "2191597")):
        assert D(bars[date]["close"]) == D(close) and bars[date]["volume"] == volume and bars[date]["tradestatus"] == "1"
    reference = ((D("13.39") - D("0.086")) / D("1.3")).quantize(D("0.01"), rounding=ROUND_HALF_UP)
    assert reference == D(bars["2017-05-19"]["preclose"]) == D("10.23")
    rights = rows(ENGINE / "full_history_rights_events.csv")
    positions = rows(ENGINE / "full_history_daily_positions.csv")
    orders = rows(ENGINE / "full_history_settled_orders.csv")
    out = {"manifest_sha256": sha(manifest_path), "code_sha256": m["code_sha256"], "issuer_pdf_sha256": sha(PDF), "candidate_sha256": sha(CANDIDATES), "market": bars, "reference": str(reference), "branches": {}, "first_blocks": {b: m["branches"][b]["first_block"] for b in ("prefunded", "netted")}}
    for branch in ("prefunded", "netted"):
        branch_days = [x["date"] for x in rows(ENGINE / "full_history_daily_account.csv") if x["branch"] == branch]
        if max(branch_days) < "2017-05-22":
            out["branches"][branch] = {"status": "NOT_REACHED_IN_CURRENT_BRANCH", "last": max(branch_days)}
            continue
        record = one(rights, branch=branch, symbol="600513", date="2017-05-18", phase="record")
        ex = one(rights, branch=branch, symbol="600513", date="2017-05-19", phase="ex")
        paid = one(rights, branch=branch, symbol="600513", date="2017-05-19", phase="cash_paid")
        credit = one(rights, branch=branch, symbol="600513", date="2017-05-22", phase="stock_credited")
        q = D(record["record_shares"])
        bonus = D(record["bonus_due"])
        cash = D(record["cash_due"])
        near(bonus, q * D("0.3"), "bonus_entitlement")
        near(cash, q * D("0.086"), "cash_entitlement")
        near(D(ex["bonus_due"]), bonus, "ex_bonus")
        near(D(ex["cash_due"]), cash, "ex_cash")
        near(D(paid["amount"]), cash, "paid")
        near(D(credit["shares"]), bonus, "credit")
        old = one(positions, branch=branch, symbol="600513", date="2017-05-18", state="settled_shares")
        ex_old = one(positions, branch=branch, symbol="600513", date="2017-05-19", state="settled_shares")
        ex_pending = one(positions, branch=branch, symbol="600513", date="2017-05-19", state="pending_bonus_shares")
        released = one(positions, branch=branch, symbol="600513", date="2017-05-22", state="settled_shares")
        assert not [r for r in positions if (r["branch"],r["symbol"],r["date"],r["state"]) == (branch,"600513","2017-05-22","pending_bonus_shares")]
        for got, expected, label in ((D(old["shares"]), q, "old"),(D(ex_old["shares"]), q, "ex_old"),(D(ex_pending["shares"]), bonus, "pending"),(D(released["shares"]), q+bonus, "release")):
            near(got, expected, label)
        for item, price in ((old,D("13.39")),(ex_old,D("10.29")),(ex_pending,D("10.29")),(released,D("9.95"))):
            assert D(item["valuation_price"]) == price and item["price_basis"] == "true_raw_close"
        ex_orders = [r for r in orders if (r["branch"],r["symbol"],r["execution_date"]) == (branch,"600513","2017-05-19")]
        assert not [r for r in ex_orders if r["booked"] == "True" and r["side"] == "sell"], ex_orders
        early_release_equity = (q+bonus)*D("10.29") + cash
        correct_pending_equity = q*D("10.29") + bonus*D("10.29") + cash
        near(early_release_equity, correct_pending_equity, "equal_nav_but_wrong_tradability")
        assert "2017-05-19" < "2017-05-22" and bonus > 0
        out["branches"][branch] = {"status":"PASS_PAPER_PENDING_AND_LISTING_CLOCK", "record_shares":str(q),"bonus_pending_5_19":str(bonus),"gross_cash_5_19":str(cash),"ex_equity_with_pending":str(correct_pending_equity),"booked_ex_orders":sum(x["booked"] == "True" for x in ex_orders),"premature_bonus_sale_mutant_rejected_by_listing_clock":str(bonus)}
    out["status"] = "PASS_FOR_BRANCHES_REACHING_2017_05_22"
    out["limitations"] = "Issuer lists newly unrestricted shares 5/22; this is paper tradability, not actual broker credit. Gross cash and fractional shares are approximations."
    path = HERE / "600513_2017_pending_adversarial_audit.json"
    path.write_text(json.dumps(out, ensure_ascii=False, indent=2)+"\n",encoding="utf-8")
    print(json.dumps({"status":out["status"],"manifest_sha256":out["manifest_sha256"],"branches":out["branches"],"first_blocks":out["first_blocks"]},ensure_ascii=False))


if __name__ == "__main__":
    main()
