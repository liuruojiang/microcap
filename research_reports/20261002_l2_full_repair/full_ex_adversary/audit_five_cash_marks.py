"""Independent five-event cash-mark check; imports only our own ledger checker."""

from __future__ import annotations

import csv
import hashlib
import json
from decimal import Decimal as D, getcontext
from pathlib import Path

from audit_both_cash_ex_ledger import main as audit_accounts


getcontext().prec = 50
HERE = Path(__file__).resolve().parent
ENGINE = HERE.parent / "engine"
ROOT = HERE.parents[2]
CASES = {
    "002624": ("sz.002624", "2014-04-01", "2014-09-03", "2014-05-22", "2014-05-23", D("0.05"), "2014-04-04", "2014-09-01"),
    "600758": ("sh.600758", "2014-01-01", "2014-07-15", "2014-06-16", "2014-06-17", D("0.03"), "2014-01-28", "2014-07-14"),
    "002319": ("sz.002319", "2014-06-01", "2014-07-15", "2014-06-19", "2014-06-20", D("0.03"), "2014-06-18", "2014-07-10"),
    "002112": ("sz.002112", "2014-07-01", "2014-08-08", "2014-07-10", "2014-07-11", D("0.01"), "2014-07-10", "2014-08-05"),
    "000560": ("sz.000560", "2014-04-01", "2014-10-02", "2014-08-21", "2014-08-22", D("0.10"), "2014-04-04", "2014-09-30"),
}


def records(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def near(a: D, b: D, name: str) -> None:
    if abs(a-b) > D("1e-20"):
        raise AssertionError(f"{name}:{a}!={b}")


def main() -> None:
    manifest_path = ENGINE / "full_history_result.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert sha(ENGINE / "full_history.py") == manifest["code_sha256"]
    for name, wanted in manifest["detail_sha256"].items():
        assert sha(ENGINE / name) == wanted, name
    for name, wanted in manifest["input_sha256"].items():
        assert sha(ROOT / name) == wanted, name
    marks = {x["symbol"]: x for x in manifest["cash_halt_marks_used"]}
    assert set(CASES) <= set(marks), set(CASES) - set(marks)
    candidates_path = ROOT / "research_reports/20261002_l2_full_repair/data/event_candidates_2010_20260930.csv"
    candidates = records(candidates_path)
    for symbol, (_, _, _, record, ex, div, last, resume) in CASES.items():
        m = marks[symbol]
        assert (m["record_date"], m["ex_date"], m["cash_pay_date"], m["last_true_trade"], m["resume_true_trade"]) == (record, ex, ex, last, resume)
        near(D(m["cash_per_share_gross"]), div, f"manifest_div:{symbol}")
        near(D(m["raw_anchor"]) - D(m["theoretical_mark"]), div, f"manifest_mark:{symbol}")
        assert m["status"] == "two_source_match"
        ev = [r for r in candidates if r["symbol"].zfill(6) == symbol and r["ex_date"] == ex]
        assert len(ev) == 1, (symbol, len(ev))
        ev = ev[0]
        assert (ev["record_date"], ev["cash_pay_date"], ev["source"], ev["source_status"], ev["source_rows"]) == (record, ex, "cninfo", "two_source_match", "cninfo=1;sina=1")
        near(D(ev["cash_per_10_gross"]) / 10, div, f"source_div:{symbol}")
        assert D(ev["bonus_per_10"]) == 0 and not ev["rights_per_10"]

    accounts = audit_accounts(CASES, "five_cash_marks_ledger_audit.json")
    assert len(accounts["cases"]) == 5
    # Local adversarial mutations of actual entries; no on-disk engine files changed.
    rights = records(ENGINE / "full_history_rights_events.csv")
    positions = records(ENGINE / "full_history_daily_positions.csv")
    daily = records(ENGINE / "full_history_daily_account.csv")
    bonus = {}
    for branch in ("netted", "prefunded"):
        credited = [r for r in rights if (r["branch"], r["date"], r["symbol"], r["phase"]) == (branch, "2014-06-11", "300120", "stock_credited")]
        before = [r for r in positions if (r["branch"], r["date"], r["symbol"], r["state"]) == (branch, "2014-06-10", "300120", "settled_shares")]
        after = [r for r in positions if (r["branch"], r["date"], r["symbol"], r["state"]) == (branch, "2014-06-11", "300120", "settled_shares")]
        assert len(credited) == len(before) == len(after) == 1
        qbonus = D(credited[0]["shares"])
        near(D(after[0]["shares"]), D(before[0]["shares"]) + qbonus, f"bonus_roll:{branch}")
        false_extra = (D(after[0]["shares"]) + qbonus) - (D(before[0]["shares"]) + qbonus)
        near(false_extra, qbonus, f"bonus_double_credit_detected:{branch}")
        bonus[branch] = {"bonus_shares": str(qbonus), "double_credit_share_error": str(false_extra),
                         "double_credit_value_error_at_ex_close": str(false_extra * D(after[0]["valuation_price"]))}
    paid = [r for r in rights if (r["branch"], r["date"], r["symbol"], r["phase"]) == ("netted", "2014-05-23", "002624", "cash_paid")]
    assert len(paid) == 1
    cash = D(paid[0]["amount"])
    today = next(r for r in daily if (r["branch"], r["date"]) == ("netted", "2014-05-23"))
    falsely_added = D(today["cash"]) + cash
    near(falsely_added - D(today["cash"]), cash, "cash_double_credit_detected")
    result = {
        "status": "PASS_five_marks_and_adversarial_mutations_until_first_block",
        "manifest_sha256": sha(manifest_path),
        "code_sha256": manifest["code_sha256"],
        "input_file_count": len(manifest["input_sha256"]),
        "candidate_sha256": sha(candidates_path),
        "five_cash_marks": [marks[s] for s in sorted(CASES)],
        "bonus_double_credit_injection": bonus,
        "cash_double_credit_injection": {"symbol": "002624", "branch": "netted", "date": "2014-05-23", "error": str(cash)},
        "first_blocks": {k: v["first_block"] for k, v in manifest["branches"].items()},
        "limitations": "Gross fractional paper model; five zero-volume marks are valuations and not fills. The source does not prove investor-specific net cash.",
    }
    (HERE / "five_cash_marks_adversarial_audit.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
