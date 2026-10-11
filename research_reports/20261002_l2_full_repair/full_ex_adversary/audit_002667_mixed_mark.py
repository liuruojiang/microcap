"""Independent halted cash-plus-bonus source and account audit for 002667."""

import csv
import hashlib
import json
from decimal import Decimal as D, ROUND_HALF_UP, getcontext
from pathlib import Path

import baostock as bs
import fitz

getcontext().prec = 60
ROOT = Path(__file__).resolve().parents[1]
ENGINE = ROOT / "engine"
PDF = ROOT / "data/suspension_ex_evidence/002667_20150525_issuer_mixed.pdf"
CANDIDATES = ROOT / "data/event_candidates_2010_20260930.csv"


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def records(path):
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def near(a, b):
    assert abs(a - b) < D("1e-30"), (a, b)


def query_bars():
    login = bs.login()
    assert login.error_code == "0"
    try:
        fields = "date,close,preclose,volume,tradestatus"
        q = bs.query_history_k_data_plus("sz.002667", fields, start_date="2015-04-01", end_date="2015-11-27", frequency="d", adjustflag="3")
        assert q.error_code == "0", q.error_msg
        out = []
        while q.next():
            out.append(dict(zip(fields.split(","), q.get_row_data())))
        return out
    finally:
        bs.logout()


def main():
    assert sha(PDF) == "aedeeefccb6425f994f84ebe63941f490810ebc04e9326611d914c79d3ace09c"
    with fitz.open(PDF) as doc:
        issuer = "".join(page.get_text() for page in doc)
    for phrase in ("证券代码：002667", "每10 股派0.700000 元人民币现金", "每10 股转增10.000000 股", "2015 年5 月28 日", "2015 年5 月29 日", "直接记入股东证券账户"):
        assert phrase in issuer, phrase
    c = [r for r in records(CANDIDATES) if r["symbol"] == "002667" and r["ex_date"] == "2015-05-29"]
    assert len(c) == 1
    c = c[0]
    assert (c["announcement_date"], c["record_date"], c["cash_pay_date"], c["share_credit_date"], c["bonus_per_10"], c["cash_per_10_gross"], c["source_status"], c["source_rows"]) == ("2015-05-25", "2015-05-28", "2015-05-29", "2015-05-29", "10.0", "0.7", "two_source_match", "cninfo=1;sina=1")
    assert not c["rights_per_10"]
    concurrent = [r for r in records(CANDIDATES) if r["symbol"] == "002667" and "2015-04-03" < r["ex_date"] < "2015-11-26" and r["ex_date"] != c["ex_date"]]
    assert not concurrent
    bars = query_bars()
    exdate = c["ex_date"]
    before = max((r for r in bars if r["date"] < exdate and D(r["volume"]) > 0 and r["tradestatus"] == "1"), key=lambda r: r["date"])
    after = min((r for r in bars if r["date"] > exdate and D(r["volume"]) > 0 and r["tradestatus"] == "1"), key=lambda r: r["date"])
    by = {r["date"]: r for r in bars}
    exbar = by[exdate]
    expected = ((D(before["close"]) - D("0.07")) / (D(1) + D(1))).quantize(D("0.01"), rounding=ROUND_HALF_UP)
    wrong_cash_omitted_mark = (D(before["close"]) / D(2)).quantize(D("0.01"), rounding=ROUND_HALF_UP)
    assert before["date"] == "2015-04-03" and after["date"] == "2015-11-26"
    assert D(before["close"]) == D("47.65") and expected == D("23.79")
    assert wrong_cash_omitted_mark == D("23.83") != expected
    assert D(exbar["close"]) == D(exbar["preclose"]) == D(after["preclose"]) == expected
    assert D(exbar["volume"]) == 0 and exbar["tradestatus"] == "0"
    manifest_path = ENGINE / "full_history_result.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert sha(ENGINE / "full_history.py") == manifest["code_sha256"], "stale_code_hash"
    for name, wanted in manifest["detail_sha256"].items():
        assert sha(ENGINE / name) == wanted, ("stale_detail_hash", name)
    for name, wanted in manifest["input_sha256"].items():
        assert sha(ROOT.parents[1] / name) == wanted, ("stale_input_hash", name)
    rights = records(ENGINE / "full_history_rights_events.csv")
    positions = records(ENGINE / "full_history_daily_positions.csv")
    orders = records(ENGINE / "full_history_settled_orders.csv")
    posted = all(manifest["branches"][b]["last_settled_date"] >= exdate for b in ("prefunded", "netted"))
    cases = {}
    for branch in ("prefunded", "netted"):
        rec = [r for r in rights if (r["branch"], r["date"], r["symbol"], r["phase"]) == (branch, "2015-05-28", "002667", "record")]
        old = [r for r in positions if (r["branch"], r["date"], r["symbol"], r["state"]) == (branch, "2015-05-28", "002667", "settled_shares")]
        assert len(rec) == len(old) == 1
        qty, bonus, cash = (D(rec[0][k]) for k in ("record_shares", "bonus_due", "cash_due"))
        near(qty, D(old[0]["shares"]))
        near(bonus, qty)
        near(cash, qty * D("0.07"))
        near(D(old[0]["valuation_price"]), D("47.65"))
        item = {"record_shares": str(qty), "bonus_due": str(bonus), "gross_cash_due": str(cash), "old_equity_piece": str(qty * D("47.65"))}
        if posted:
            events = [r for r in rights if r["branch"] == branch and r["date"] == exdate and r["symbol"] == "002667"]
            phases = {r["phase"]: r for r in events}
            assert set(phases) == {"ex", "stock_credited", "cash_paid"}
            near(D(phases["ex"]["bonus_due"]), bonus)
            near(D(phases["ex"]["cash_due"]), cash)
            near(D(phases["stock_credited"]["shares"]), bonus)
            near(D(phases["cash_paid"]["amount"]), cash)
            expositions = [r for r in positions if (r["branch"], r["date"], r["symbol"], r["state"]) == (branch, exdate, "002667", "settled_shares")]
            assert len(expositions) == 1
            pos = expositions[0]
            near(D(pos["shares"]), qty + bonus)
            near(D(pos["valuation_price"]), expected)
            near(D(pos["market_value"]), (qty + bonus) * expected)
            attempts = [r for r in orders if r["branch"] == branch and r["symbol"] == "002667" and before["date"] < r["execution_date"] < after["date"]]
            assert all(r["booked"] == "False" for r in attempts)
            correct = (qty + bonus) * expected + cash
            near(correct, qty * D("47.65"))
            wrong = {
                "missing_bonus": qty * expected + cash,
                "double_bonus": (qty + 2 * bonus) * expected + cash,
                "missing_cash": (qty + bonus) * expected,
                "double_cash": (qty + bonus) * expected + 2 * cash,
                "stale_price": (qty + bonus) * D("47.65") + cash,
                "premature_record_credit": (qty + bonus) * D("47.65"),
            }
            assert all(abs(v - correct) > D("1") for k, v in wrong.items() if k != "premature_record_credit")
            assert abs(wrong["premature_record_credit"] - qty * D("47.65")) > D("1")
            item.update({"ex_shares": pos["shares"], "ex_stock_mark": pos["valuation_price"], "ex_equity_piece_with_cash": str(correct), "halt_booked_orders": 0, "halt_blocked_attempts": len(attempts), "mutant_errors": {k: str(v - (qty * D("47.65") if k == "premature_record_credit" else correct)) for k, v in wrong.items()}})
        cases[branch] = item
    result = {
        "status": "PASS_002667_account_and_source" if posted else "PASS_002667_source_record_only",
        "manifest_sha256": sha(manifest_path), "code_sha256": sha(ENGINE / "full_history.py"),
        "source_sha256": {"issuer_pdf": sha(PDF), "candidate_file": sha(CANDIDATES)},
        "last_true_trade": before, "ex_zero_volume": exbar, "first_true_resume": after,
        "theoretical_mark": str(expected), "accounts": cases,
        "formula_cash_omission_mutant_mark_rejected": str(wrong_cash_omitted_mark),
        "first_blocks": {b: manifest["branches"][b]["first_block"] for b in ("prefunded", "netted")},
        "limitations": "Fractional shares, gross cash, zero-volume reference. First post-halt true trade is 2015-11-26; reference is not a 2015-05-29 fill.",
    }
    dest = Path(__file__).with_name("002667_mixed_adversarial_audit.json")
    dest.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
