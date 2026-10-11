"""Minimal adversarial clock/funding/capacity cases; synthetic only."""

from decimal import Decimal as D

from contract import freeze_orders, settle_next_close


def main():
    old = {"A": D(1), "B": D(1)}
    prices_t = {"A": D(100), "B": D(100), "C": D(100)}
    prices_next = {"A": D(110), "B": D(90), "C": D(50)}
    frozen = freeze_orders(old, D(10), prices_t, ["B", "C"])
    assert [(o["symbol"], o["side"], o["frozen_shares"]) for o in frozen] == [
        ("A", "sell", D(1)), ("B", "buy", D("0.05")), ("C", "buy", D("1.05"))]
    p = settle_next_close(old, D(10), frozen, prices_next, set(), "prefunded")
    n = settle_next_close(old, D(10), frozen, prices_next, set(), "netted")
    assert p["old_equity_at_next_close"] == n["old_equity_at_next_close"] == D(210)
    assert [e["booked"] for e in p["events"]] == [True, True, False]
    assert p["events"][-1]["reason"] == "insufficient_prefunded_cash"
    assert [e["booked"] for e in n["events"]] == [True, True, True]
    assert "C" not in p["positions_after"] and "C" in n["positions_after"]
    blocked = settle_next_close(old, D(10), frozen, prices_next, {"A"}, "netted", max_members=2)
    assert blocked["events"][0]["reason"] == "blocked_trade"
    assert blocked["events"][-1]["reason"] == "member_capacity"
    assert len(blocked["positions_after"]) == 2
    assert all(x["cash_after"] >= 0 for x in (p, n, blocked))
    for x in (p, n, blocked):
        assert x["old_equity_at_next_close"] - x["fees"] == x["equity_after"]
    try:
        settle_next_close(old, D(10), frozen, {"A": D(110), "B": D(90)}, set(), "netted")
    except ValueError as exc:
        assert "Missing/nonpositive T+1" in str(exc)
    else:
        raise AssertionError("Missing next close must fail closed")
    print("synthetic_contract_check: PASS; frozen quantities, old-day mark, funding branches, blocked sell, capacity, missing price")


if __name__ == "__main__":
    main()
