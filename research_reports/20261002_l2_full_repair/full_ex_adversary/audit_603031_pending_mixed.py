"""Independent 603031 2018 mixed dividend and new-share listing check."""

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
PDF = ROOT / "data/suspension_ex_evidence/603031_20180601_issuer_mixed.pdf"
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
        query = bs.query_history_k_data_plus("sh.603031", fields, start_date="2018-06-06", end_date="2018-06-08", frequency="d", adjustflag="3")
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
    assert sha(PDF) == "0705aaf7dc9595ef0ea1bb178bad26916caae0e1eb32f3b33508c787b18150d4"
    with fitz.open(PDF) as doc:
        source = "".join(page.get_text() for page in doc)
    for phrase in ("证券代码：603031", "每股现金红利0.15 元", "每股转增股份0.4 股", "2018/6/6", "2018/6/7", "2018/6/8"):
        assert phrase in source, phrase
    c = one(rows(CANDIDATES), symbol="603031", ex_date="2018-06-07")
    o = one(rows(ORIGINAL), symbol="603031", 除权日="2018-06-07")
    o_sha = hashlib.sha256(json.dumps(o, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()
    assert o_sha == "8fd4e986607b9bd4a34da2929d4b190be7480c81dd492fdc9608b17899c5d294"
    assert (c["record_date"], c["cash_pay_date"], c["share_credit_date"], D(c["bonus_per_10"]), D(c["cash_per_10_gross"]), c["source_status"]) == ("2018-06-06", "2018-06-07", "", D("4"), D("1.5"), "single_source_only")
    assert (o["股权登记日"],o["派息日"],D(o["转增比例"]),D(o["派息比例"])) == (c["record_date"],c["cash_pay_date"],D("4"),D("1.5"))
    bars = market()
    for date, close, volume in (("2018-06-06", "23.00", "2446100"), ("2018-06-07", "15.87", "1829300"), ("2018-06-08", "15.35", "3264020")):
        assert D(bars[date]["close"]) == D(close) and bars[date]["volume"] == volume and bars[date]["tradestatus"] == "1"
    reference = ((D("23") - D("0.15")) / D("1.4")).quantize(D("0.01"), rounding=ROUND_HALF_UP)
    assert reference == D(bars["2018-06-07"]["preclose"]) == D("16.32")
    rights = rows(ENGINE / "full_history_rights_events.csv")
    positions = rows(ENGINE / "full_history_daily_positions.csv")
    orders = rows(ENGINE / "full_history_settled_orders.csv")
    out = {"manifest_sha256":sha(manifest_path),"code_sha256":m["code_sha256"],"issuer_pdf_sha256":sha(PDF),"original_row_sha256":o_sha,"candidate_sha256":sha(CANDIDATES),"market":bars,"reference":str(reference),"branches":{},"first_blocks":{b:m["branches"][b]["first_block"] for b in ("prefunded","netted")}}
    for branch in ("prefunded","netted"):
        branch_days = [x["date"] for x in rows(ENGINE / "full_history_daily_account.csv") if x["branch"] == branch]
        if max(branch_days) < "2018-06-08":
            out["branches"][branch] = {"status":"NOT_REACHED_IN_CURRENT_BRANCH","last":max(branch_days)}
            continue
        record = one(rights, branch=branch, symbol="603031", date="2018-06-06", phase="record")
        ex = one(rights, branch=branch, symbol="603031", date="2018-06-07", phase="ex")
        paid = one(rights, branch=branch, symbol="603031", date="2018-06-07", phase="cash_paid")
        credit = one(rights, branch=branch, symbol="603031", date="2018-06-08", phase="stock_credited")
        q, bonus, cash = (D(record[k]) for k in ("record_shares","bonus_due","cash_due"))
        for got, expected, label in ((bonus,q*D("0.4"),"bonus"),(cash,q*D("0.15"),"cash"),(D(ex["bonus_due"]),bonus,"ex_bonus"),(D(ex["cash_due"]),cash,"ex_cash"),(D(paid["amount"]),cash,"paid"),(D(credit["shares"]),bonus,"credit")):
            near(got,expected,label)
        old = one(positions,branch=branch,symbol="603031",date="2018-06-06",state="settled_shares")
        ex_old = one(positions,branch=branch,symbol="603031",date="2018-06-07",state="settled_shares")
        pending = one(positions,branch=branch,symbol="603031",date="2018-06-07",state="pending_bonus_shares")
        release = one(positions,branch=branch,symbol="603031",date="2018-06-08",state="settled_shares")
        next_orders = [r for r in orders if (r["branch"],r["symbol"],r["signal_date"],r["execution_date"]) == (branch,"603031","2018-06-07","2018-06-08")]
        net_booked = sum((D(r["frozen_shares"]) * (D(1) if r["side"] == "buy" else D(-1)) for r in next_orders if r["booked"] == "True"),D(0))
        for item, qty, price in ((old,q,D("23")),(ex_old,q,D("15.87")),(pending,bonus,D("15.87")),(release,q+bonus+net_booked,D("15.35"))):
            near(D(item["shares"]),qty,"position_qty")
            assert D(item["valuation_price"]) == price and item["price_basis"] == "true_raw_close"
        assert not [r for r in positions if (r["branch"],r["symbol"],r["date"],r["state"]) == (branch,"603031","2018-06-08","pending_bonus_shares")]
        ex_orders = [r for r in orders if (r["branch"],r["symbol"],r["execution_date"]) == (branch,"603031","2018-06-07")]
        assert not [r for r in ex_orders if r["booked"] == "True" and r["side"] == "sell"], ex_orders
        near((q+bonus)*D("15.87")+cash,q*D("15.87")+bonus*D("15.87")+cash,"early_release_same_nav")
        assert "2018-06-07" < "2018-06-08" and bonus > 0
        out["branches"][branch] = {"status":"PASS_PAPER_PENDING_AND_LISTING_CLOCK","record_shares":str(q),"bonus_pending_6_7":str(bonus),"gross_cash_6_7":str(cash),"ex_equity_piece":str((q+bonus)*D("15.87")+cash),"booked_ex_orders":sum(r["booked"]=="True" for r in ex_orders),"next_session_net_booked_shares":str(net_booked),"next_session_after_release_shares":release["shares"],"premature_bonus_sale_mutant_rejected_by_listing_clock":str(bonus)}
    out["status"] = "PASS_FOR_BRANCHES_REACHING_2018_06_08"
    out["limitations"] = "Issuer's 6/8 unrestricted-share listing establishes conservative paper tradability, not actual broker credit. Gross cash and fractional shares remain model approximations."
    (HERE/"603031_pending_mixed_adversarial_audit.json").write_text(json.dumps(out,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    print(json.dumps({"status":out["status"],"manifest_sha256":out["manifest_sha256"],"branches":out["branches"],"first_blocks":out["first_blocks"]},ensure_ascii=False))


if __name__ == "__main__":
    main()
