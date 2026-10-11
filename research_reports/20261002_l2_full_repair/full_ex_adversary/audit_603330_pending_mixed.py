"""Independent 603330 2018 mixed dividend and new-share listing check."""

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
PDF = ROOT / "data/suspension_ex_evidence/603330_20180619_issuer_newspaper.pdf"
CANDIDATES = ROOT / "data/event_candidates_2010_20260930.csv"
ORIGINAL = REPO / "research_reports/20260926_l1_repair/corporate_actions/cninfo_distribution_events.csv"


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
        query = bs.query_history_k_data_plus("sh.603330", fields, start_date="2018-06-25", end_date="2018-06-27", frequency="d", adjustflag="3")
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
    assert sha(PDF) == "75aa6d8a6bc07f94a40e14082934950e57c33ff7aa77881245ed7305b33dad5d"
    with fitz.open(PDF) as doc:
        source = "".join(page.get_text() for page in doc)
    for phrase in ("603330", "每股现金红利0.258 元", "每股转增股份0.3 股", "2018/6/25", "2018/6/26", "2018/6/27"):
        assert phrase in source, phrase
    c = one(rows(CANDIDATES), symbol="603330", ex_date="2018-06-26")
    o = one(rows(ORIGINAL), symbol="603330", 除权日="2018-06-26")
    o_sha = hashlib.sha256(json.dumps(o, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()
    assert o_sha == "0a37a40ff61e8440bad0bc331eb20093b6ae63bc8d5f54102952ff4180db0e72"
    assert (c["record_date"], c["cash_pay_date"], c["share_credit_date"], D(c["bonus_per_10"]), D(c["cash_per_10_gross"]), c["source_status"]) == ("2018-06-25", "2018-06-26", "", D("3"), D("2.58"), "single_source_only")
    assert (o["股权登记日"],o["派息日"],D(o["转增比例"]),D(o["派息比例"])) == (c["record_date"],c["cash_pay_date"],D("3"),D("2.58"))
    bars = market()
    for date, close, volume in (("2018-06-25", "28.10", "377900"), ("2018-06-26", "21.62", "294500"), ("2018-06-27", "21.45", "480510")):
        assert D(bars[date]["close"]) == D(close) and bars[date]["volume"] == volume and bars[date]["tradestatus"] == "1"
    reference = ((D("28.10") - D("0.258")) / D("1.3")).quantize(D("0.01"), rounding=ROUND_HALF_UP)
    assert reference == D(bars["2018-06-26"]["preclose"]) == D("21.42")
    rights = rows(ENGINE / "full_history_rights_events.csv")
    positions = rows(ENGINE / "full_history_daily_positions.csv")
    orders = rows(ENGINE / "full_history_settled_orders.csv")
    out = {"manifest_sha256":sha(manifest_path),"code_sha256":m["code_sha256"],"issuer_pdf_sha256":sha(PDF),"original_row_sha256":o_sha,"candidate_sha256":sha(CANDIDATES),"market":bars,"reference":str(reference),"branches":{},"first_blocks":{b:m["branches"][b]["first_block"] for b in ("prefunded","netted")}}
    for branch in ("prefunded","netted"):
        branch_days = [x["date"] for x in rows(ENGINE / "full_history_daily_account.csv") if x["branch"] == branch]
        if max(branch_days) < "2018-06-27":
            out["branches"][branch] = {"status":"NOT_REACHED_IN_CURRENT_BRANCH","last":max(branch_days)}
            continue
        record = one(rights, branch=branch, symbol="603330", date="2018-06-25", phase="record")
        ex = one(rights, branch=branch, symbol="603330", date="2018-06-26", phase="ex")
        paid = one(rights, branch=branch, symbol="603330", date="2018-06-26", phase="cash_paid")
        credit = one(rights, branch=branch, symbol="603330", date="2018-06-27", phase="stock_credited")
        q, bonus, cash = (D(record[k]) for k in ("record_shares","bonus_due","cash_due"))
        for got, expected, label in ((bonus,q*D("0.3"),"bonus"),(cash,q*D("0.258"),"cash"),(D(ex["bonus_due"]),bonus,"ex_bonus"),(D(ex["cash_due"]),cash,"ex_cash"),(D(paid["amount"]),cash,"paid"),(D(credit["shares"]),bonus,"credit")):
            near(got,expected,label)
        old = one(positions,branch=branch,symbol="603330",date="2018-06-25",state="settled_shares")
        ex_old = one(positions,branch=branch,symbol="603330",date="2018-06-26",state="settled_shares")
        pending = one(positions,branch=branch,symbol="603330",date="2018-06-26",state="pending_bonus_shares")
        release = one(positions,branch=branch,symbol="603330",date="2018-06-27",state="settled_shares")
        next_orders = [r for r in orders if (r["branch"],r["symbol"],r["signal_date"],r["execution_date"]) == (branch,"603330","2018-06-26","2018-06-27")]
        net_booked = sum((D(r["frozen_shares"]) * (D(1) if r["side"] == "buy" else D(-1)) for r in next_orders if r["booked"] == "True"),D(0))
        for item, qty, price in ((old,q,D("28.10")),(ex_old,q,D("21.62")),(pending,bonus,D("21.62")),(release,q+bonus+net_booked,D("21.45"))):
            near(D(item["shares"]),qty,"position_qty")
            assert D(item["valuation_price"]) == price and item["price_basis"] == "true_raw_close"
        assert not [r for r in positions if (r["branch"],r["symbol"],r["date"],r["state"]) == (branch,"603330","2018-06-27","pending_bonus_shares")]
        ex_orders = [r for r in orders if (r["branch"],r["symbol"],r["execution_date"]) == (branch,"603330","2018-06-26")]
        assert not [r for r in ex_orders if r["booked"] == "True" and r["side"] == "sell"], ex_orders
        near((q+bonus)*D("21.62")+cash,q*D("21.62")+bonus*D("21.62")+cash,"early_release_same_nav")
        assert "2018-06-26" < "2018-06-27" and bonus > 0
        out["branches"][branch] = {"status":"PASS_PAPER_PENDING_AND_LISTING_CLOCK","record_shares":str(q),"bonus_pending_6_26":str(bonus),"gross_cash_6_26":str(cash),"ex_equity_piece":str((q+bonus)*D("21.62")+cash),"booked_ex_orders":sum(r["booked"]=="True" for r in ex_orders),"next_session_net_booked_shares":str(net_booked),"next_session_after_release_shares":release["shares"],"premature_bonus_sale_mutant_rejected_by_listing_clock":str(bonus)}
    out["status"] = "PASS_FOR_BRANCHES_REACHING_2018_06_27"
    out["limitations"] = "Issuer's 6/8 unrestricted-share listing establishes conservative paper tradability, not actual broker credit. Gross cash and fractional shares remain model approximations."
    (HERE/"603330_pending_mixed_adversarial_audit.json").write_text(json.dumps(out,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    print(json.dumps({"status":out["status"],"manifest_sha256":out["manifest_sha256"],"branches":out["branches"],"first_blocks":out["first_blocks"]},ensure_ascii=False))


if __name__ == "__main__":
    main()
