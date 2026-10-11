"""Independent official-single-source mixed distribution and halted account audit."""

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
PDF = ROOT / "data/suspension_ex_evidence/300423_20160514_issuer_mixed.pdf"
CNINFO = REPO / "research_reports/20260926_l1_repair/corporate_actions/cninfo_distribution_events.csv"
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
        q = bs.query_history_k_data_plus("sz.300423", fields, start_date="2016-03-15", end_date="2016-10-01", frequency="d", adjustflag="3")
        assert q.error_code == "0", q.error_msg
        out = []
        while q.next():
            out.append(dict(zip(fields.split(","), q.get_row_data())))
        return out
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
    assert sha(PDF) == "6aaca9ae25b55c8153cf299bea517ee1f30745445f2dad6fc0b30564e8950188"
    with fitz.open(PDF) as doc:
        issuer = "".join(page.get_text() for page in doc)
    for phrase in ("证券代码：300423", "每10 股派1.000000 元人民币现金", "每10 股转增2.000000 股", "2016 年5 月19 日", "2016 年5 月20 日", "直接记入股东证券账户", "起始交易日为2016 年5 月20 日"):
        assert phrase in issuer, phrase
    c = [r for r in rows(CANDIDATES) if r["symbol"] == "300423" and r["ex_date"] == "2016-05-20"]
    o = [r for r in rows(CNINFO) if r["symbol"] == "300423" and r["除权日"] == "2016-05-20"]
    assert len(c) == len(o) == 1
    c, o = c[0], o[0]
    assert (c["announcement_date"], c["record_date"], c["cash_pay_date"], c["share_credit_date"], c["bonus_per_10"], c["cash_per_10_gross"], c["source_status"], c["source_rows"]) == ("2016-05-14", "2016-05-19", "2016-05-20", "2016-05-20", "2.0", "1.0", "single_source_only", "cninfo=1;sina=0")
    assert (o["实施方案公告日期"], o["股权登记日"], o["除权日"], o["派息日"], o["股份到账日"], o["转增比例"], o["派息比例"]) == (c["announcement_date"], c["record_date"], c["ex_date"], c["cash_pay_date"], c["share_credit_date"], c["bonus_per_10"], c["cash_per_10_gross"])
    original_sha = hashlib.sha256(json.dumps(o, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()
    assert original_sha == "c44581f6f32cd6b7b9172904f92e36913bb88e8389d0e5ac7b20bb00d904a83a"
    source_mutations = {"wrong_cash_1p1": D("1.1") != D(o["派息比例"]), "wrong_bonus_3": D("3") != D(o["转增比例"]), "wrong_credit_day": "2016-05-23" != o["股份到账日"], "late_announcement": "2016-05-21" > o["股权登记日"]}
    assert all(source_mutations.values())
    bars = market()
    before = max((r for r in bars if r["date"] < "2016-05-20" and D(r["volume"]) > 0 and r["tradestatus"] == "1"), key=lambda r: r["date"])
    after = min((r for r in bars if r["date"] > "2016-05-20" and D(r["volume"]) > 0 and r["tradestatus"] == "1"), key=lambda r: r["date"])
    by = {r["date"]: r for r in bars}
    ex = by["2016-05-20"]
    price = ((D(before["close"]) - D("0.10")) / D("1.2")).quantize(D("0.01"), rounding=ROUND_HALF_UP)
    assert before["date"] == "2016-03-18" and after["date"] == "2016-09-30"
    assert price == D("28.55") == D(ex["close"]) == D(ex["preclose"]) == D(after["preclose"])
    assert ex["tradestatus"] == "0" and D(ex["volume"]) == 0
    rights = rows(ENGINE / "full_history_rights_events.csv")
    positions = rows(ENGINE / "full_history_daily_positions.csv")
    orders = rows(ENGINE / "full_history_settled_orders.csv")
    posted = all(m["branches"][b]["last_settled_date"] >= "2016-05-20" for b in ("prefunded", "netted"))
    cases = {}
    for branch in ("prefunded", "netted"):
        event = lambda date, phase: [r for r in rights if (r["branch"], r["symbol"], r["date"], r["phase"]) == (branch, "300423", date, phase)]
        rec = event("2016-05-19", "record")
        assert len(rec) == 1
        qty, bonus, cash = (D(rec[0][k]) for k in ("record_shares", "bonus_due", "cash_due"))
        near(bonus, qty*D("0.2"))
        near(cash, qty*D("0.1"))
        old = [r for r in positions if (r["branch"], r["symbol"], r["date"], r["state"]) == (branch, "300423", "2016-05-19", "settled_shares")]
        assert len(old) == 1
        near(D(old[0]["shares"]), qty)
        near(D(old[0]["valuation_price"]), D("34.36"))
        info = {"record_shares": str(qty), "bonus_due": str(bonus), "gross_cash_due": str(cash)}
        if posted:
            exevent, paid, credit = event("2016-05-20", "ex"), event("2016-05-20", "cash_paid"), event("2016-05-20", "stock_credited")
            assert len(exevent) == len(paid) == len(credit) == 1
            near(D(exevent[0]["bonus_due"]), bonus)
            near(D(exevent[0]["cash_due"]), cash)
            near(D(paid[0]["amount"]), cash)
            near(D(credit[0]["shares"]), bonus)
            ex_pos = [r for r in positions if (r["branch"], r["symbol"], r["date"], r["state"]) == (branch, "300423", "2016-05-20", "settled_shares")]
            assert len(ex_pos) == 1
            near(D(ex_pos[0]["shares"]), qty+bonus)
            near(D(ex_pos[0]["valuation_price"]), price)
            assert not [r for r in positions if (r["branch"], r["symbol"], r["date"], r["state"]) == (branch, "300423", "2016-05-20", "pending_bonus_shares")]
            attempts = [r for r in orders if r["branch"] == branch and r["symbol"] == "300423" and before["date"] < r["execution_date"] < after["date"]]
            assert all(r["booked"] == "False" for r in attempts)
            correct = (qty+bonus)*price+cash
            near(correct, qty*D("34.36"))
            wrong = {"missing_bonus": qty*price+cash, "double_bonus": (qty+2*bonus)*price+cash, "missing_cash": (qty+bonus)*price, "double_cash": (qty+bonus)*price+2*cash, "stale_price": (qty+bonus)*D("34.36")+cash}
            assert all(abs(v-correct)>D("1") for v in wrong.values())
            info.update({"ex_shares": ex_pos[0]["shares"], "ex_mark": ex_pos[0]["valuation_price"], "correct_equity_piece": str(correct), "mutation_errors": {k:str(v-correct) for k,v in wrong.items()}, "halt_booked_orders":0, "halt_blocked_attempts":len(attempts)})
        cases[branch] = info
    result = {"status":"PASS_300423_exact_event_account" if posted else "PASS_300423_source_record_only", "manifest_sha256":sha(mpath), "issuer_pdf_sha256":sha(PDF), "cninfo_original_row_sha256":original_sha, "candidate_sha256":sha(CANDIDATES), "source_mutations_rejected":source_mutations, "last_true_trade":before, "ex_zero_volume_reference":ex, "first_true_resume":after, "theoretical_mark":str(price), "accounts":cases, "first_blocks":{b:m["branches"][b]["first_block"] for b in cases}, "limitations":"Gross fractional paper shares/cash; legal new-share trading date 2016-05-20 does not create a zero-volume fill. Resume preclose is retrospective validation."}
    dest = Path(__file__).with_name("300423_single_mixed_adversarial_audit.json")
    dest.write_text(json.dumps(result, ensure_ascii=False, indent=2)+"\n",encoding="utf-8")
    print(json.dumps(result,ensure_ascii=False,indent=2))


if __name__ == "__main__":
    main()
