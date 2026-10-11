"""Independent readback of 002624/600758 dividend marks and ledger clocks."""

from __future__ import annotations

import csv
import hashlib
import json
from decimal import Decimal as D, getcontext
from pathlib import Path

import baostock as bs


getcontext().prec = 50
HERE = Path(__file__).resolve().parent
ENGINE = HERE.parent / "engine"
BAR_FIELDS = ["date", "preclose", "close", "volume", "tradestatus"]
CASES = {
    "002624": ("sz.002624", "2014-04-01", "2014-09-03", "2014-05-22", "2014-05-23", D("0.05"), "2014-04-04", "2014-09-01"),
    "600758": ("sh.600758", "2014-01-01", "2014-07-15", "2014-06-16", "2014-06-17", D("0.03"), "2014-01-28", "2014-07-14"),
}


def records(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


def near(a: D, b: D, what: str) -> None:
    if abs(a - b) > D("1e-20"):
        raise AssertionError(f"{what}: {a} != {b}")


def get_bars(code: str, start: str, end: str) -> list[dict[str, str]]:
    query = bs.query_history_k_data_plus(
        code, ",".join(BAR_FIELDS), start_date=start, end_date=end,
        frequency="d", adjustflag="3",
    )
    if query.error_code != "0":
        raise RuntimeError(query.error_msg)
    rows = []
    while query.next():
        rows.append(dict(zip(BAR_FIELDS, query.get_row_data())))
    return rows


def main(cases=CASES, output_name="both_cash_ex_ledger_audit.json") -> dict:
    rights_path = ENGINE / "full_history_rights_events.csv"
    pos_path = ENGINE / "full_history_daily_positions.csv"
    orders_path = ENGINE / "full_history_settled_orders.csv"
    rights, positions, orders = map(records, (rights_path, pos_path, orders_path))
    login = bs.login()
    if login.error_code != "0":
        raise RuntimeError(login.error_msg)
    evidence = {}
    try:
        for symbol, (code, start, end, record_date, ex_date, div, want_last, want_next) in cases.items():
            bars = get_bars(code, start, end)
            by_date = {r["date"]: r for r in bars}
            real_before = max(
                (r for r in bars if r["date"] < ex_date and r["tradestatus"] == "1" and D(r["volume"]) > 0),
                key=lambda r: r["date"],
            )
            real_after = min(
                (r for r in bars if r["date"] >= ex_date and r["tradestatus"] == "1" and D(r["volume"]) > 0),
                key=lambda r: r["date"],
            )
            assert real_before["date"] == want_last and real_after["date"] == want_next
            assert by_date[ex_date]["tradestatus"] == "0" and D(by_date[ex_date]["volume"]) == 0
            assert (by_date[record_date]["tradestatus"] == "0" and D(by_date[record_date]["volume"]) == 0) or (by_date[record_date]["tradestatus"] == "1" and D(by_date[record_date]["volume"]) > 0)
            prior_mark, ex_mark = D(by_date[record_date]["close"]), D(by_date[ex_date]["close"])
            near(prior_mark - ex_mark, div, f"mark_drop:{symbol}")
            near(D(real_before["close"]), prior_mark, f"last_true_close:{symbol}")
            near(D(real_after["preclose"]), ex_mark, f"resumption_preclose:{symbol}")
            branch_rows = []
            branches = sorted({r["branch"] for r in rights if r["symbol"] == symbol and r["date"] == record_date and r["phase"] == "record"})
            for branch in branches:
                hit = lambda rows, date, phase: [r for r in rows if r["branch"] == branch and r["symbol"] == symbol and r["date"] == date and r.get("phase") == phase]
                rec = hit(rights, record_date, "record")
                ex = hit(rights, ex_date, "ex")
                paid = hit(rights, ex_date, "cash_paid")
                assert len(rec) == len(ex) == len(paid) == 1, (symbol, branch, len(rec), len(ex), len(paid))
                q = D(rec[0]["record_shares"])
                cash = D(rec[0]["cash_due"])
                near(cash, q * div, f"dividend_due:{symbol}:{branch}")
                near(D(ex[0]["cash_due"]), cash, f"ex_due:{symbol}:{branch}")
                near(D(paid[0]["amount"]), cash, f"cash_paid:{symbol}:{branch}")
                phit = lambda date: [r for r in positions if r["branch"] == branch and r["symbol"] == symbol and r["date"] == date and r["state"] == "settled_shares"]
                p_record, p_ex = phit(record_date), phit(ex_date)
                assert len(p_record) == len(p_ex) == 1
                near(D(p_record[0]["shares"]), q, f"record_position:{symbol}:{branch}")
                near(D(p_ex[0]["shares"]), q, f"ex_position:{symbol}:{branch}")
                near(D(p_record[0]["valuation_price"]), prior_mark, f"record_mark:{symbol}:{branch}")
                near(D(p_ex[0]["valuation_price"]), ex_mark, f"ex_mark:{symbol}:{branch}")
                assert p_ex[0]["price_basis"] in {
                    "issuer_verified_suspension_ex_theoretical_mark",
                    "two_source_verified_suspension_cash_theoretical_mark",
                    "verified_suspension_cash_theoretical_mark",
                }
                trades = [r for r in orders if r["branch"] == branch and r["symbol"] == symbol and r["execution_date"] == ex_date]
                assert all(r["booked"] == "False" and r["reason"] == "blocked_trade" for r in trades), (symbol, branch, trades)
                old_value, paper_value = q * prior_mark, q * ex_mark + cash
                near(old_value, paper_value, f"economic_conservation:{symbol}:{branch}")
                branch_rows.append({
                    "branch": branch, "record_shares": str(q), "cash_gross": str(cash),
                    "old_stock_value": str(old_value), "paper_stock_plus_cash": str(paper_value),
                    "double_count_excess": str(q * prior_mark + cash - old_value),
                    "omit_cash_shortfall": str(old_value - q * ex_mark),
                    "record_basis": p_record[0]["price_basis"], "ex_basis": p_ex[0]["price_basis"],
                    "ex_booked_orders": 0,
                    "ex_blocked_attempts": len(trades),
                })
            evidence[symbol] = {
                "last_true_trade": real_before,
                "record_bar": by_date[record_date],
                "ex_bar": by_date[ex_date],
                "next_true_trade": real_after,
                "accounts": branch_rows,
            }
    finally:
        bs.logout()
    result = {
        "status": "PASS_through_current_first_block_only",
        "ledger_sha256": {
            p.name: hashlib.sha256(p.read_bytes()).hexdigest()
            for p in (rights_path, pos_path, orders_path)
        },
        "cases": evidence,
        "limitation": "Gross fractional paper model; zero-volume ex marks are not fills or account-specific net cash.",
    }
    (HERE / output_name).write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return result


if __name__ == "__main__":
    main()
