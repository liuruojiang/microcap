"""Cross-audit recorded corporate actions without engine account functions."""

from __future__ import annotations

import csv
import hashlib
import json
from decimal import Decimal, getcontext
from pathlib import Path


getcontext().prec = 40
D = Decimal
ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
ENGINE = ROOT / "research_reports/20261002_l2_full_repair/engine/result.json"
DAILY = ROOT / "research_reports/20261002_l2_full_repair/engine/daily_account.csv"
RIGHTS = ROOT / "research_reports/20260926_l1_repair/corporate_actions/cninfo_distribution_events.csv"
TOL = D("1e-20")


def csvrows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


def num(value: str | None) -> Decimal:
    if value in (None, "", "nan"):
        return D(0)
    return D(value)


def main() -> None:
    engine = json.loads(ENGINE.read_text(encoding="utf-8"))
    rights = csvrows(RIGHTS)
    daily = csvrows(DAILY)
    daily_index = {(r["branch"], r["date"]): r for r in daily}
    check = []
    for branch, data in engine["branches"].items():
        events = [e for group in data["executed_rebalances"] for e in group.get("corporate_events", [])]
        record = {(e["symbol"], group["date"]): e
                  for group in data["executed_rebalances"] for e in group.get("corporate_events", [])
                  if e["phase"] == "record"}
        assert len(record) == sum(e["phase"] == "record" for e in events)
        assert all(num(r["cash"]) >= 0 and num(r["cash_receivable"]) >= 0 and num(r["pending_share_value"]) >= 0
                   for r in daily if r["branch"] == branch)
        for (symbol, date), e in record.items():
            matches = [r for r in rights if r["symbol"] == symbol and r["股权登记日"] == date]
            assert len(matches) == 1, (branch, symbol, date, len(matches))
            source = matches[0]
            q = D(e["record_shares"])
            gross_due = q * num(source["派息比例"]) / D(10)
            bonus_due = q * (num(source["送股比例"]) + num(source["转增比例"])) / D(10)
            assert abs(gross_due - D(e["cash_due"])) <= TOL
            assert abs(bonus_due - D(e["bonus_due"])) <= TOL
            ex = source["除权日"]
            associated = [(group["date"], event) for group in data["executed_rebalances"]
                          for event in group.get("corporate_events", []) if event["symbol"] == symbol]
            settled = data["last_settled_date"]
            if ex <= settled:
                assert sum(event["phase"] == "ex" and d == ex for d, event in associated) == 1
            if gross_due:
                paid = source["派息日"]
                if paid <= settled:
                    assert sum(event["phase"] == "cash_paid" and d == paid and abs(D(event["amount"]) - gross_due) <= TOL
                               for d, event in associated) == 1
                row = daily_index.get((branch, paid))
                prev_dates = [d for b, d in daily_index if b == branch and d < paid]
                if row and prev_dates and num(row["fees"]) == 0:
                    prior = daily_index[(branch, max(prev_dates))]
                    # If no other cash event/trade occurred, cash delta should
                    # equal the payout.  Multiple events are summed below.
                    payments = sum((D(x["amount"]) for group in data["executed_rebalances"]
                                    if group.get("date") == paid for x in group.get("corporate_events", [])
                                    if x["phase"] == "cash_paid"), D(0))
                    trades = any(group.get("execution_date") == paid for group in data["executed_rebalances"])
                    if not trades:
                        assert abs(num(row["cash"]) - num(prior["cash"]) - payments) <= TOL
            if bonus_due:
                credited = source["股份到账日"]
                if credited <= settled:
                    assert sum(event["phase"] == "stock_credited" and d == credited and abs(D(event["shares"]) - bonus_due) <= TOL
                               for d, event in associated) == 1
            check.append({"branch": branch, "symbol": symbol, "record_date": date, "ex_date": ex,
                          "record_shares": str(q), "gross_cash_due": str(gross_due),
                          "bonus_shares_due": str(bonus_due), "source_match": True,
                          "settlement_observed": ex <= settled})
    output = {"status": "observed_engine_rights_arithmetic_pass",
              "engine_result_sha256": hashlib.sha256(ENGINE.read_bytes()).hexdigest(),
              "daily_sha256": hashlib.sha256(DAILY.read_bytes()).hexdigest(),
              "source_sha256": hashlib.sha256(RIGHTS.read_bytes()).hexdigest(),
              "event_count": len(check), "events": check,
              "limits": "Only events actually emitted by the engine are checked; no proof that source inventory is complete or that all held events were emitted."}
    HERE.mkdir(parents=True, exist_ok=True)
    (HERE / "engine_rights_audit.json").write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": output["status"], "event_count": len(check)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
