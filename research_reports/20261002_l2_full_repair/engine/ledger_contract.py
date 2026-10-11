"""Small auditable T-close order / T+1-close stock-account contract.

Pure arithmetic; no assertion that a closing-auction order actually filled.
"""

from __future__ import annotations

from decimal import Decimal


FEE = Decimal("0.003")


def freeze_orders(positions, cash_t, prices_t, ranked_target):
    """Freeze every order quantity using only T-close information."""
    if len(ranked_target) != len(set(ranked_target)) or len(ranked_target) > 100:
        raise ValueError("Bad target rank list")
    required = set(positions) | set(ranked_target)
    if any(s not in prices_t or prices_t[s] <= 0 for s in required):
        raise ValueError("Missing/nonpositive T raw close")
    equity_t = cash_t + sum((q * prices_t[s] for s, q in positions.items()), Decimal(0))
    per_target = equity_t / Decimal(len(ranked_target))
    desired = {s: per_target / prices_t[s] for s in ranked_target}
    sells = [{"symbol": s, "side": "sell", "frozen_shares": q - desired.get(s, Decimal(0))}
             for s, q in sorted(positions.items()) if q > desired.get(s, Decimal(0))]
    buys = [{"symbol": s, "side": "buy", "frozen_shares": desired[s] - positions.get(s, Decimal(0))}
            for s in ranked_target if desired[s] > positions.get(s, Decimal(0))]
    return sells + buys


def settle_next_close(positions_t, cash_t, frozen_orders, prices_next, blocked, funding, max_members=100):
    """Mark old lots first; execute fixed orders at the later paper close.

    prefunded: buys use only T cash. netted: sells then buys at the same
    close, an explicitly theoretical settlement ordering.
    """
    if funding not in {"prefunded", "netted"}:
        raise ValueError(funding)
    needed = set(positions_t) | {x["symbol"] for x in frozen_orders}
    if any(s not in prices_next or prices_next[s] <= 0 for s in needed):
        raise ValueError("Missing/nonpositive T+1 raw close; no stale fill")
    held = dict(positions_t)
    starting_member_count = len(held)
    cash = cash_t
    old_equity_at_next_close = cash + sum((q * prices_next[s] for s, q in held.items()), Decimal(0))
    buy_budget = cash_t
    events = []
    for side in ("sell", "buy"):
        for order in (o for o in frozen_orders if o["side"] == side):
            symbol, q = order["symbol"], order["frozen_shares"]
            if q <= 0:
                raise ValueError("Nonpositive frozen order")
            gross = q * prices_next[symbol]
            fee = gross * FEE
            if symbol in blocked:
                reason = "blocked_trade"
            elif side == "sell" and held.get(symbol, Decimal(0)) < q:
                reason = "shares_unavailable"
            elif side == "buy" and symbol not in held and len(held) >= max_members:
                reason = "member_capacity"
            elif side == "buy" and gross + fee > buy_budget:
                reason = "insufficient_prefunded_cash" if funding == "prefunded" else "insufficient_netted_cash"
            else:
                reason = ""
            if not reason:
                if side == "sell":
                    held[symbol] -= q
                    if held[symbol] == 0:
                        del held[symbol]
                    cash += gross - fee
                    if funding == "netted":
                        buy_budget = cash
                else:
                    cash -= gross + fee
                    buy_budget -= gross + fee
                    held[symbol] = held.get(symbol, Decimal(0)) + q
            # Passive rights shares can create a 101st holding after a stock
            # distribution.  Active buys remain capped at max_members above;
            # a passive excess is not a bookkeeping failure.
            if cash < 0 or buy_budget < 0 or len(held) > max(max_members, starting_member_count):
                raise AssertionError("Cash/capacity violated")
            events.append({**order, "price_next": str(prices_next[symbol]),
                           "booked": not bool(reason), "reason": reason,
                           "gross": str(gross if not reason else Decimal(0)),
                           "fee": str(fee if not reason else Decimal(0))})
    stock_after = sum((q * prices_next[s] for s, q in held.items()), Decimal(0))
    equity_after = cash + stock_after
    fees = sum((Decimal(x["fee"]) for x in events), Decimal(0))
    if abs(old_equity_at_next_close - fees - equity_after) > Decimal("1e-25"):
        raise AssertionError("Cash/stock/fee conservation failed")
    return {"funding": funding, "old_equity_at_next_close": old_equity_at_next_close,
            "positions_after": held, "cash_after": cash, "stock_after": stock_after,
            "fees": fees, "equity_after": equity_after, "events": events}
