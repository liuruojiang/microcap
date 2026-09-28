"""Natural counterexample for sparse-date tradeability after a long suspension."""

from pathlib import Path
import sys

import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
import microcap_top100_mom16_biweekly_live_v2_0 as runtime


def test_reopening_limit_requires_full_calendar() -> None:
    symbol = "600862"
    reopen = pd.Timestamp("2014-09-18")
    sparse_dates = pd.DatetimeIndex(pd.to_datetime(["2014-09-17", "2014-09-18"]))
    proxy = pd.read_csv(ROOT / "outputs/wind_microcap_top_100_biweekly_thursday_16y_cached.csv", usecols=["date"])
    all_dates = pd.DatetimeIndex(pd.to_datetime(proxy["date"]))
    full_dates = all_dates[(all_dates >= pd.Timestamp("2014-02-28")) & (all_dates <= reopen)]
    sparse = runtime.freq_mod.load_symbol_cache(symbol, sparse_dates, pd.DatetimeIndex([]), "close", False)
    full = runtime.freq_mod.load_symbol_cache(symbol, full_dates, pd.DatetimeIndex([]), "close", False)
    assert runtime.freq_mod.detect_close_limit_blocks(symbol, reopen, 3.03, 3.33) == (True, False)
    assert bool(sparse[3].loc[reopen]) is True  # missing prior traded close
    assert bool(full[3].loc[reopen]) is False  # correct price-limit block

