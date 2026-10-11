"""Independent structural checks for final engine daily and result artifacts."""

from __future__ import annotations

import csv
import hashlib
import json
from collections import defaultdict
from decimal import Decimal
from pathlib import Path


D = Decimal
ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
ENGINE_DIR = ROOT / "research_reports/20261002_l2_full_repair/engine"
RESULT = ENGINE_DIR / "result.json"
CODE = ENGINE_DIR / "extend_ledger.py"
DAILY = ENGINE_DIR / "daily_account.csv"
CALENDAR = ROOT / "outputs/wind_microcap_top_100_biweekly_thursday_16y_cached.csv"
TOL = D("1e-20")


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def csvrows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


def d(row: dict[str, str], key: str) -> Decimal:
    return D(row.get(key, "0") or "0")


def main() -> None:
    result = json.loads(RESULT.read_text(encoding="utf-8"))
    by_branch: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in csvrows(DAILY):
        by_branch[row["branch"]].append(row)
    calendar = [row["date"] for row in csvrows(CALENDAR)]
    assert len(calendar) == len(set(calendar)) and calendar == sorted(calendar)
    branch_out = {}
    for branch, item in result["branches"].items():
        rows = by_branch[branch]
        assert rows and len(rows) == item["settled_days_after_prior"]
        assert rows[-1]["date"] == item["last_settled_date"]
        assert [r["date"] for r in rows] == [date for date in calendar if "2010-02-12" < date <= rows[-1]["date"]]
        assert len({r["date"] for r in rows}) == len(rows)
        max_residual = D(0)
        fee_days = 0
        for row in rows:
            components = sum((d(row, key) for key in ("cash", "stock_value", "cash_receivable", "pending_share_value")), D(0))
            residual = abs(components - d(row, "equity"))
            max_residual = max(max_residual, residual)
            assert residual <= TOL, (branch, row["date"], residual)
            assert d(row, "cash") >= 0 and d(row, "cash_receivable") >= 0 and d(row, "pending_share_value") >= 0
            assert d(row, "fees") >= 0 and 0 <= int(row["held_count"]) <= 100
            if d(row, "fees"):
                fee_days += 1
        assert abs(d(rows[-1], "cash") - D(item["last_cash"])) <= TOL
        assert len(item["last_positions"]) == int(rows[-1]["held_count"])
        trades = [event for event in item["executed_rebalances"] if "execution_date" in event]
        for trade in trades:
            day = next(r for r in rows if r["date"] == trade["execution_date"])
            assert abs(d(day, "fees") - D(trade["fees"])) <= TOL
        branch_out[branch] = {"rows": len(rows), "last_date": rows[-1]["date"],
                              "max_component_residual": str(max_residual), "fee_days": fee_days,
                              "reported_rebalances": len(trades), "first_block": item["first_block"]}
    current_code_sha = digest(CODE)
    out = {"status": "totals_pass" if result["code_sha256"] == current_code_sha else "totals_pass_but_code_sha_stale",
           "recorded_code_sha256": result["code_sha256"], "current_code_sha256": current_code_sha,
           "result_sha256": digest(RESULT), "daily_sha256": digest(DAILY), "branches": branch_out,
           "limits": "Totals, date continuity and reported trade fees only; no independent per-stock daily reconstruction without position/order detail."}
    HERE.mkdir(parents=True, exist_ok=True)
    (HERE / "engine_totals_audit.json").write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": out["status"], "branches": branch_out}, ensure_ascii=False))


if __name__ == "__main__":
    main()
