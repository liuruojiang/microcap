"""Independent, read-only audit of the 300319 halted bonus-share event."""

import csv
import hashlib
import json
from decimal import Decimal, ROUND_HALF_UP, getcontext
from pathlib import Path

getcontext().prec = 60
ROOT = Path(__file__).resolve().parents[1]
ENGINE = ROOT / "engine"
SOURCE = ROOT / "corporate_conflict_evidence"
D = Decimal


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def rows(path):
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def exact(a, b):
    assert abs(a - b) < D("1e-30"), (a, b)


def reject_mutation(label, altered, expected_qty, expected_mark):
    """Check entitlement and mark independently of row arithmetic."""
    try:
        exact(D(altered["shares"]), expected_qty)
        exact(D(altered["valuation_price"]), expected_mark)
        exact(D(altered["market_value"]), D(altered["shares"]) * D(altered["valuation_price"]))
    except AssertionError:
        return label
    raise AssertionError(f"mutation_not_rejected:{label}")


def main():
    mpath = ENGINE / "full_history_result.json"
    m = json.loads(mpath.read_text(encoding="utf-8"))
    codehash = sha(ENGINE / "full_history.py")
    assert codehash == m["code_sha256"]
    marks = [x for x in m["bonus_halt_marks_used"] if x["symbol"] == "300319"]
    assert len(marks) == 1
    mark = marks[0]
    assert (mark["record_date"], mark["ex_date"], mark["share_credit_date"]) == ("2014-09-17", "2014-09-18", "2014-09-18")
    assert mark["status"] == "two_source_match"
    assert D(mark["bonus_per_share"]) == D("1.5")
    assert sha(SOURCE / "300319_20140910_issuer_distribution.pdf") == "965d399e1d7c53952a5a4d060e8a0cdc597960a66bbf00650ee2a39218823d6c"
    bars_path = SOURCE / "300319_baostock_unadjusted_window.csv"
    assert sha(bars_path) == "e12fc0bd78d21b56b2f9fea4991a9e9a26e5c36757f17179233c2dc61fffc8da"
    bars = rows(bars_path)
    bydate = {x["date"]: x for x in bars}
    true = [x for x in bars if D(x["volume"]) > 0 and x["tradestatus"] == "1"]
    prev = max((x for x in true if x["date"] < mark["ex_date"]), key=lambda x: x["date"])
    nxt = min((x for x in true if x["date"] > mark["ex_date"]), key=lambda x: x["date"])
    exbar = bydate["2014-09-18"]
    anchor = D(prev["close"])
    ref = (anchor / D("2.5")).quantize(D("0.01"), rounding=ROUND_HALF_UP)
    assert (prev["date"], nxt["date"]) == ("2014-07-18", "2015-01-05")
    assert D(exbar["volume"]) == 0 and exbar["tradestatus"] == "0"
    assert D(exbar["preclose"]) == ref == D(nxt["preclose"]) == D(mark["theoretical_mark"])
    rights = rows(ENGINE / "full_history_rights_events.csv")
    positions = rows(ENGINE / "full_history_daily_positions.csv")
    orders = rows(ENGINE / "full_history_settled_orders.csv")
    cases = {}
    for branch in ("prefunded", "netted"):
        ev = [r for r in rights if r["branch"] == branch and r["symbol"] == "300319" and r["date"] in ("2014-09-17", "2014-09-18")]
        assert [(r["date"], r["phase"]) for r in ev] == [("2014-09-17", "record"), ("2014-09-18", "ex"), ("2014-09-18", "stock_credited")]
        rec, ex, credited = ev
        qty = D(rec["record_shares"])
        bonus = D(rec["bonus_due"])
        exact(bonus, qty * D("1.5"))
        exact(D(ex["bonus_due"]), bonus)
        exact(D(credited["shares"]), bonus)
        assert D(rec["cash_due"]) == D(ex["cash_due"]) == 0
        ps = {r["date"]: r for r in positions if r["branch"] == branch and r["symbol"] == "300319" and r["state"] == "settled_shares" and r["date"] in ("2014-09-17", "2014-09-18", "2014-09-19")}
        assert len(ps) == 3
        prepos, expos, postpos = (ps[d] for d in ("2014-09-17", "2014-09-18", "2014-09-19"))
        exact(D(prepos["shares"]), qty)
        exact(D(prepos["valuation_price"]), anchor)
        exact(D(expos["shares"]), qty + bonus)
        exact(D(postpos["shares"]), qty + bonus)
        exact(D(expos["valuation_price"]), ref)
        exact(D(postpos["valuation_price"]), ref)
        assert expos["price_basis"] == "two_source_verified_suspension_bonus_theoretical_mark"
        exact(D(prepos["market_value"]), qty * anchor)
        exact(D(expos["market_value"]), (qty + bonus) * ref)
        held_orders = [r for r in orders if r["branch"] == branch and r["symbol"] == "300319" and prev["date"] < r["execution_date"] < nxt["date"]]
        assert not [r for r in held_orders if r["booked"] == "True"]
        correct = (qty + bonus) * ref
        missing_bonus = qty * ref
        stale_price = (qty + bonus) * anchor
        double_bonus = (qty + 2 * bonus) * ref
        assert abs(correct - missing_bonus) > D("1")
        assert abs(stale_price - correct) > D("1")
        assert abs(double_bonus - correct) > D("1")
        mutations = [
            reject_mutation("record_day_early_credit", {**prepos, "shares": str(qty + bonus), "market_value": str((qty + bonus) * anchor)}, qty, anchor),
            reject_mutation("ex_day_missing_bonus", {**expos, "shares": str(qty), "market_value": str(missing_bonus)}, qty + bonus, ref),
            reject_mutation("ex_day_double_bonus", {**expos, "shares": str(qty + 2 * bonus), "market_value": str(double_bonus)}, qty + bonus, ref),
            reject_mutation("ex_day_stale_price", {**expos, "valuation_price": str(anchor), "market_value": str(stale_price)}, qty + bonus, ref),
        ]
        cases[branch] = {
            "record_shares": str(qty), "credited_bonus_shares": str(bonus),
            "ex_shares": expos["shares"], "pre_value": prepos["market_value"],
            "ex_value": expos["market_value"],
            "mark_rounding_gain": str(correct - qty * anchor),
            "missing_bonus_understatement": str(correct - missing_bonus),
            "stale_price_overstatement": str(stale_price - correct),
            "double_bonus_overstatement": str(double_bonus - correct),
            "halt_booked_orders": 0, "halt_blocked_attempts": len(held_orders),
            "mutations_rejected": mutations,
        }
    out = {
        "status": "PASS_300319_targeted_through_current_first_block_only",
        "manifest_sha256": sha(mpath), "code_sha256": codehash,
        "source_sha256": {"issuer_distribution_pdf": sha(SOURCE / "300319_20140910_issuer_distribution.pdf"), "baostock_unadjusted_window": sha(bars_path)},
        "last_true_trade": {"date": prev["date"], "close": prev["close"], "volume": prev["volume"]},
        "ex_halt_bar": {"date": exbar["date"], "preclose": exbar["preclose"], "volume": exbar["volume"]},
        "first_true_resume": {"date": nxt["date"], "preclose": nxt["preclose"], "volume": nxt["volume"]},
        "theoretical_mark": str(ref), "accounts": cases,
        "first_blocks": {b: m["branches"][b]["first_block"] for b in ("prefunded", "netted")},
        "limitation": "Fractional paper shares and zero-volume reference marks are not executable fills; issuer fractional-share allocation is account-specific.",
    }
    dest = Path(__file__).with_name("300319_bonus_adversarial_audit.json")
    dest.write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
