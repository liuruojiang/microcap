"""Independent event-clock and counterexample audit of replay CSVs."""

from __future__ import annotations

import csv
import hashlib
import json
from decimal import Decimal as D, getcontext
from pathlib import Path


getcontext().prec = 50
HERE = Path(__file__).resolve().parent
ENGINE = HERE.parent / "engine"
TOL = D("1e-20")


def csv_rows(name: str) -> list[dict[str, str]]:
    with (ENGINE / name).open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def decimal(item: dict[str, str], key: str) -> D:
    return D(item[key] or "0")


def exactly(actual: D, expected: D, label: str) -> None:
    if abs(actual - expected) > TOL:
        raise AssertionError(f"{label}: actual={actual}, expected={expected}, diff={actual-expected}")


def one(items, *, branch: str, date: str, symbol: str, **criteria):
    matches = [x for x in items if x["branch"] == branch and x["date"] == date
               and x["symbol"] == symbol and all(x[k] == value for k, value in criteria.items())]
    if len(matches) != 1:
        raise AssertionError(f"event_row_cardinality:{branch}:{date}:{symbol}:{criteria}:{len(matches)}")
    return matches[0]


def check() -> dict:
    positions = csv_rows("full_history_daily_positions.csv")
    rights = csv_rows("full_history_rights_events.csv")
    orders = csv_rows("full_history_settled_orders.csv")
    bars = list(csv.DictReader((HERE / "two_event_baostock_raw.csv").open(encoding="utf-8")))
    source = {(r["date"], r["code"]): r for r in bars}
    for day, symbol in (("2010-09-07", "sh.600211"), ("2011-06-17", "sz.000810")):
        bar = source[(day, symbol)]
        if bar["tradestatus"] != "0" or D(bar["volume"] or "0") != 0:
            raise AssertionError(f"not_verified_suspended:{day}:{symbol}")

    output = {"source_bars_sha256": hashlib.sha256((HERE / "two_event_baostock_raw.csv").read_bytes()).hexdigest(),
              "engine_files_sha256": {name: hashlib.sha256((ENGINE / name).read_bytes()).hexdigest()
                                      for name in ("full_history_daily_positions.csv", "full_history_rights_events.csv",
                                                   "full_history_settled_orders.csv")}, "events": {}}
    # Only the netted branch owns this particular 600211 stock on record close.
    b = "netted"
    symbol = "600211"
    before = one(positions, branch=b, date="2010-09-06", symbol=symbol, state="settled_shares")
    ex_old = one(positions, branch=b, date="2010-09-07", symbol=symbol, state="settled_shares")
    ex_pending = one(positions, branch=b, date="2010-09-07", symbol=symbol, state="pending_bonus_shares")
    credited = one(positions, branch=b, date="2010-09-08", symbol=symbol, state="settled_shares")
    record = one(rights, branch=b, date="2010-09-06", symbol=symbol, phase="record")
    ex_right = one(rights, branch=b, date="2010-09-07", symbol=symbol, phase="ex")
    credit_right = one(rights, branch=b, date="2010-09-08", symbol=symbol, phase="stock_credited")
    q = decimal(before, "shares")
    exactly(decimal(record, "record_shares"), q, "600211_record_qty")
    exactly(decimal(ex_old, "shares"), q, "600211_old_qty_on_halt")
    exactly(decimal(ex_pending, "shares"), q / D(10), "600211_pending_qty")
    exactly(decimal(ex_right, "bonus_due"), q / D(10), "600211_ex_due")
    exactly(decimal(credit_right, "shares"), q / D(10), "600211_credit_due")
    exactly(decimal(credited, "shares"), q * D("1.1"), "600211_credited_qty")
    for row, price, basis in ((before, D("15.36"), "true_raw_close"),
                              (ex_old, D("14.63"), "issuer_verified_suspension_ex_theoretical_mark"),
                              (ex_pending, D("14.63"), "issuer_verified_suspension_ex_theoretical_mark"),
                              (credited, D("14.69"), "true_raw_close")):
        exactly(decimal(row, "valuation_price"), price, "600211_mark")
        if row["price_basis"] != basis:
            raise AssertionError(f"600211_wrong_mark_basis:{row['date']}:{row['state']}")
    if any(x for x in orders if x["branch"] == b and x["execution_date"] == "2010-09-07"
           and x["symbol"] == symbol and x["booked"] == "True"):
        raise AssertionError("600211_traded_during_halt")
    correct = decimal(ex_old, "market_value") + decimal(ex_pending, "market_value")
    exactly(correct, q * D("1.1") * D("14.63"), "600211_correct_value")
    ordinary_bonus = q * D("1.1") * (D("15.36") / D("1.1"))
    stale_mark = q * D("1.1") * D("15.36")
    if not ordinary_bonus < correct < stale_mark:
        raise AssertionError("600211_counterexample_order")
    output["events"][symbol] = {"record_qty": str(q), "ex_old_mark": "14.63",
                                  "pending_qty": str(q / D(10)), "credited_date": "2010-09-08",
                                  "correct_ex_value": str(correct),
                                  "ordinary_bonus_understates_by": str(correct - ordinary_bonus),
                                  "stale_mark_overstates_by": str(stale_mark - correct),
                                  "counterexamples_rejected": True}

    symbol = "000810"
    output["events"][symbol] = {}
    for b in ("prefunded", "netted"):
        before = one(positions, branch=b, date="2011-06-16", symbol=symbol, state="settled_shares")
        ex = one(positions, branch=b, date="2011-06-17", symbol=symbol, state="settled_shares")
        last_halt = one(positions, branch=b, date="2012-02-09", symbol=symbol, state="settled_shares")
        record = one(rights, branch=b, date="2011-06-16", symbol=symbol, phase="record")
        ex_right = one(rights, branch=b, date="2011-06-17", symbol=symbol, phase="ex")
        paid = one(rights, branch=b, date="2011-06-17", symbol=symbol, phase="cash_paid")
        q = decimal(before, "shares")
        exactly(decimal(record, "record_shares"), q, "000810_record_qty")
        exactly(decimal(ex, "shares"), q, "000810_ex_qty")
        exactly(decimal(before, "valuation_price"), D("14"), "000810_pre_mark")
        exactly(decimal(ex, "valuation_price"), D("13.80"), "000810_ex_mark")
        exactly(decimal(last_halt, "valuation_price"), D("13.80"), "000810_last_halt_mark")
        if ex["price_basis"] != "issuer_verified_suspension_ex_theoretical_mark":
            raise AssertionError("000810_wrong_mark_basis")
        for row, field in ((record, "cash_due"), (ex_right, "cash_due"), (paid, "amount")):
            exactly(decimal(row, field), q * D("0.20"), f"000810_{field}_gross")
        exactly(decimal(ex, "market_value") + decimal(paid, "amount"),
                decimal(before, "market_value"), "000810_ex_cash_conservation")
        blocked = [x for x in orders if x["branch"] == b and x["execution_date"] == "2011-06-17"
                   and x["symbol"] == symbol and x["side"] == "sell"]
        resumed = [x for x in orders if x["branch"] == b and x["execution_date"] == "2012-02-10"
                   and x["symbol"] == symbol and x["side"] == "sell"]
        if len(blocked) != 1 or blocked[0]["booked"] != "False" or len(resumed) != 1 or resumed[0]["booked"] != "True":
            raise AssertionError(f"000810_sell_clock:{b}")
        exactly(decimal(resumed[0], "execution_raw_price"), D("13.46"), "000810_first_real_sell_close")
        wrong_stale = q * D("14.00") + q * D("0.20")
        wrong_omit = q * D("13.80")
        correct = q * D("13.80") + q * D("0.20")
        if not wrong_omit < correct < wrong_stale:
            raise AssertionError("000810_counterexample_order")
        output["events"][symbol][b] = {"record_qty": str(q), "paid_tax_before": str(q * D("0.20")),
                                         "ex_mark": "13.80", "first_real_sell": "2012-02-10",
                                         "stale_mark_overstates_by": str(wrong_stale - correct),
                                         "omit_cash_understates_by": str(correct - wrong_omit),
                                         "counterexamples_rejected": True}
    output["status"] = "PASS_two_issuer_events_only"
    return output


if __name__ == "__main__":
    result = check()
    (HERE / "two_event_independent_audit.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
