"""Independent reconciliation of the separate engine's saved opening orders."""

from __future__ import annotations

import csv
import json
from decimal import Decimal, getcontext
from pathlib import Path


getcontext().prec = 40
HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
ENGINE = ROOT / "research_reports/20260926_l2_repair/fixed_share_engine"
ORDERS = ENGINE / "opening_orders.csv"
RESULT = ENGINE / "result.json"
RAW = ROOT / ".microcap_index_cache/prices_raw"
FEE = Decimal("0.003")


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as file:
        return list(csv.DictReader(file))


def raw_close(symbol: str, date: str) -> Decimal | None:
    values = [Decimal(r["close_raw"]) for r in read_csv(RAW / f"{symbol}.csv") if r["date"] == date]
    if len(values) > 1:
        raise ValueError(f"duplicate raw close {symbol} {date}")
    return values[0] if values else None


def run() -> dict[str, object]:
    orders = read_csv(ORDERS)
    saved = json.loads(RESULT.read_text(encoding="utf-8"))
    assert len(orders) == 100 and sorted(int(r["rank"]) for r in orders) == list(range(1, 101))
    cash = Decimal(saved["start_cash"])
    shares = {}
    booked = []
    rejected = []
    fees = Decimal(0)
    stock = Decimal(0)
    for r in sorted(orders, key=lambda r: int(r["rank"])):
        symbol = r["symbol"].zfill(6)
        known = raw_close(symbol, "2010-01-04")
        future = raw_close(symbol, "2010-01-05")
        assert known is not None and future is not None
        frozen = Decimal(r["frozen_fractional_shares"])
        expected = Decimal("10000") / (known * (1 + FEE))
        assert abs(frozen - expected) < Decimal("1e-30"), symbol
        assert Decimal(r["signal_raw_close"]) == known
        assert Decimal(r["execution_raw_close"]) == future
        debit = frozen * future * (1 + FEE)
        should_book = debit <= cash
        assert should_book == (r["conditional_booked"] == "true"), symbol
        if should_book:
            cash -= debit
            fees += frozen * future * FEE
            stock += frozen * future
            shares[symbol] = frozen
            booked.append(symbol)
            assert Decimal(r["paper_gross"]) == frozen * future
            assert Decimal(r["paper_fee"]) == frozen * future * FEE
        else:
            rejected.append(symbol)
            assert Decimal(r["paper_gross"]) == Decimal(r["paper_fee"]) == 0
        assert cash >= 0
    assert len(booked) == int(saved["conditional_booked_orders"]) == 98
    assert rejected == [r["symbol"] for r in saved["rejected_orders"]]
    assert abs(cash - Decimal(saved["cash_after"])) < Decimal("1e-27")
    assert abs(stock - Decimal(saved["stock_value_after"])) < Decimal("1e-27")
    assert abs(fees - Decimal(saved["paper_fee_total"])) < Decimal("1e-27")
    assert abs(cash + stock + fees - Decimal(saved["start_cash"])) < Decimal("1e-25")

    # This deliberately changes priority only, with the same T-frozen orders.
    # The account's accept/reject identity is path dependent, not uniquely
    # determined by the set of 100 proposed shares.
    reverse_cash = Decimal(saved["start_cash"])
    reverse_rejected = []
    for r in sorted(orders, key=lambda r: int(r["rank"]), reverse=True):
        debit = Decimal(r["frozen_fractional_shares"]) * Decimal(r["execution_raw_close"]) * (1 + FEE)
        if debit <= reverse_cash:
            reverse_cash -= debit
        else:
            reverse_rejected.append(r["symbol"].zfill(6))
    assert set(reverse_rejected) != set(rejected)

    missing_mark = [s for s in shares if raw_close(s, "2010-01-06") is None]
    assert missing_mark == ["600982"]
    out: dict[str, object] = {
        "reconciliation": "PASS for saved conditional stock-only arithmetic",
        "frozen_quantity_uses_only_T_close": True,
        "cash_and_fee_residual": str(cash + stock + fees - Decimal(saved["start_cash"])),
        "original_rank_order_rejected": rejected,
        "reversed_rank_order_rejected": reverse_rejected,
        "rank_priority_changes_rejection_identity": True,
        "next_day_missing_true_close": missing_mark,
        "execution_trade_status_limit_auction_fill_checked_in_engine": False,
        "rights_ledger_exhaustiveness_certified": False,
        "continuous_account_certified": False,
    }
    (HERE / "engine_audit.json").write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return out


if __name__ == "__main__":
    print(json.dumps(run(), ensure_ascii=False, indent=2))
