"""No-rebalance continuation requires evidenced observations on every tail day."""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from scripts.exchange_calendar import sessions_for_day
from scripts.stock_return_pipeline import ReturnBasisError, load_return_prices
from scripts.tradeability_evidence import enrich_price_with_trade_status


def load_tail_returns(*, root, symbol, raw_path, master, tail_dates):
    dates = pd.DatetimeIndex(tail_dates)
    if len(dates) < 2 or dates.has_duplicates or not dates.is_monotonic_increasing:
        raise ReturnBasisError("Tail dates must contain an anchor and ordered unique sessions")
    raw = pd.read_csv(raw_path, parse_dates=["date"])
    raw = raw.loc[raw.date.le(dates[-1])].copy()
    calendar = pd.DatetimeIndex(sessions_for_day(dates[-1].date()))
    calendar = calendar[(calendar >= raw.date.min()) & (calendar <= dates[-1])]
    if not dates.equals(calendar[calendar >= dates[0]]):
        raise ReturnBasisError("Tail omits a completed exchange session")
    observed = enrich_price_with_trade_status(
        root=Path(root), symbol=symbol, raw_path=Path(raw_path), price=raw,
        master=master, trading_dates=calendar, require_evidence=True,
    )
    verified_dates = pd.DatetimeIndex(observed.attrs.get("verified_trade_status_dates", []))
    if not dates.isin(verified_dates).all():
        raise ReturnBasisError(f"Tail trade-status source coverage is incomplete for {symbol}")
    observed = observed.set_index("date")
    if not dates.isin(observed.index).all():
        raise ReturnBasisError(f"Unexplained missing tail observation for {symbol}")
    adjusted = load_return_prices(root=root, symbol=symbol, raw_path=raw_path,
                                  master=master, as_of=dates[-1], allow_network=False)
    actual_dates = observed.index[observed.tradestatus.eq(1)]
    prices = adjusted.set_index("date").close_adj.reindex(actual_dates)
    if prices.isna().any():
        raise ReturnBasisError(f"Incomplete adjusted traded-date coverage for {symbol}")
    # Only explicit, source-bound nontrading rows can receive carried valuation.
    prices = prices.reindex(calendar).ffill().reindex(dates)
    if prices.isna().any() or not np.isfinite(prices).all() or prices.le(0).any():
        raise ReturnBasisError(f"Missing positive tail valuation for {symbol}")
    returns = prices.pct_change(fill_method=None)
    returns.iloc[0] = 0.0  # The first row is the already realized anchor.
    return returns
