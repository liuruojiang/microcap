"""First rebalance T+1-close price/constraint feasibility, no NAV or fills."""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal, ROUND_HALF_UP

import pandas as pd

from clock_candidate import OUT, PATHS, ROOT, close_path


SIGNAL = "2010-01-14"
EXECUTION = "2010-01-15"
TARGET_PATH = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv"


def at_limit(price: Decimal, prior: Decimal, is_st: bool, side: str) -> bool:
    ratio = Decimal("0.05") if is_st else Decimal("0.10")
    direction = Decimal("1") if side == "buy" else Decimal("-1")
    limit = (prior * (Decimal("1") + direction * ratio)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return price == limit


def main() -> None:
    seed = set(pd.read_csv(PATHS["seed"], dtype={"symbol": str}).symbol.str.zfill(6))
    target = pd.read_csv(TARGET_PATH, dtype={"symbol": str})
    target.symbol = target.symbol.str.zfill(6)
    desired = set(target.loc[target.rebalance_date.eq(SIGNAL), "symbol"])
    assert len(seed) == len(desired) == 100
    bars_path = OUT / "baostock_raw_open_windows.csv"
    bars = pd.read_csv(bars_path, dtype={"symbol": str, "date": str, "tradestatus": str, "isST": str})
    day = bars.loc[bars.date.eq(EXECUTION)].set_index("symbol")
    rows = []
    for side, symbols in (("sell", sorted(seed - desired)), ("buy", sorted(desired - seed))):
        for symbol in symbols:
            bar = day.loc[symbol] if symbol in day.index else None
            local = pd.read_csv(close_path(symbol))
            local_today = local.loc[local.date.eq(EXECUTION), "close_raw"]
            actual_trade = (bar is not None and str(bar.tradestatus) == "1"
                and len(local_today) == 1 and Decimal(str(local_today.iloc[0])) == Decimal(str(bar["close"])))
            prior = None if bar is None else Decimal(str(bar.preclose))
            close = None if bar is None else Decimal(str(bar["close"]))
            is_st = None if bar is None else str(bar.isST) == "1"
            limit = bool(actual_trade and prior is not None and close is not None and at_limit(close, prior, is_st, side))
            rows.append({"signal_close": SIGNAL, "candidate_execution_close": EXECUTION,
                "symbol": symbol, "side": side, "baostock_trade_status": None if bar is None else bar.tradestatus,
                "baostock_isST_diagnostic": is_st,
                "baostock_preclose": None if prior is None else str(prior),
                "baostock_close": None if close is None else str(close),
                "local_real_close": None if len(local_today) != 1 else str(local_today.iloc[0]),
                "raw_close_verified_trading": bool(actual_trade), "at_side_close_limit": limit,
                "paper_close_eligible": bool(actual_trade and not limit),
                "actual_auction_fill_verified": False})
    table = pd.DataFrame(rows)
    table.to_csv(OUT / "next_close_first_rebalance_orders.csv", index=False, encoding="utf-8")
    blocked_exits = set(table.loc[table.side.eq("sell") & ~table.paper_close_eligible, "symbol"])
    executed_exits = set(table.loc[table.side.eq("sell") & table.paper_close_eligible, "symbol"])
    slots = 100 - len(seed & desired) - len(blocked_exits)
    candidate_order = target.loc[target.rebalance_date.eq(SIGNAL)].sort_values("rank").symbol.tolist()
    eligible_buys = set(table.loc[table.side.eq("buy") & table.paper_close_eligible, "symbol"])
    selected_buys = [s for s in candidate_order if s in eligible_buys][:slots]
    capacity_blocked = sorted(eligible_buys - set(selected_buys))
    paper_after = (seed - executed_exits) | set(selected_buys)
    assert len(paper_after) == 100
    result = {"signal_close": SIGNAL, "candidate_execution_close": EXECUTION,
        "seed_members": len(seed), "target_members": len(desired),
        "proposed_sell": int(table.side.eq("sell").sum()), "proposed_buy": int(table.side.eq("buy").sum()),
        "real_raw_close_verified": int(table.raw_close_verified_trading.sum()),
        "nontrading_or_raw_missing": table.loc[~table.raw_close_verified_trading, ["symbol", "side"]].to_dict("records"),
        "at_side_close_limit": table.loc[table.at_side_close_limit, ["symbol", "side"]].to_dict("records"),
        "paper_close_eligible": int(table.paper_close_eligible.sum()),
        "paper_exits_after_capacity": len(executed_exits),
        "paper_entries_after_capacity": len(selected_buys),
        "paper_blocked_by_capacity": capacity_blocked,
        "paper_member_count_after": len(paper_after),
        "actual_auction_fills_verified": 0,
        "input_sha256": {"target": hashlib.sha256(TARGET_PATH.read_bytes()).hexdigest(),
            "seed": hashlib.sha256(PATHS["seed"].read_bytes()).hexdigest(),
            "baostock_raw": hashlib.sha256(bars_path.read_bytes()).hexdigest()},
        "warning": "BaoStock isST is diagnostic and L1 status is unresolved; close-limit does not prove auction fill"}
    (OUT / "next_close_first_rebalance.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
