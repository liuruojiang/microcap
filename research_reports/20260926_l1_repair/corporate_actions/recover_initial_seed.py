"""Read-only recovery of trimmed 2010-01-04 Top100 target and executed seed."""

from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
import microcap_top100_mom16_biweekly_live_v2_0 as runtime

OUT = Path(__file__).resolve().parent


def main() -> None:
    trade_dates = pd.DatetimeIndex(pd.to_datetime(["2010-01-04", "2010-01-05"]))
    cap_dates = pd.DatetimeIndex([trade_dates[0]])
    symbols = runtime.freq_mod.load_universe()
    returns, caps, buyable, sellable = runtime.freq_mod.load_cache_panels(
        symbols=symbols,
        trading_dates=trade_dates,
        cap_dates=cap_dates,
        max_workers=16,
        trade_constraint_mode=runtime.base_mod.TRADE_CONSTRAINT_MODE,
        exclude_historical_st_from_caps=True,
    )
    names = runtime.base_mod.load_name_map()
    target_map = runtime.base_mod.build_live_target_members_map(
        caps_by_date=caps,
        rebalance_dates=cap_dates,
        name_map=names,
        top_n=100,
    )
    target = target_map[trade_dates[0]]
    result = runtime.freq_mod.apply_trade_constraints(
        current_members=[],
        target_members=target,
        trade_date=trade_dates[0],
        buyable_df=buyable,
        sellable_df=sellable,
        top_n=100,
    )
    executed = result["members_after"]
    path = OUT / "initial_2010_01_04_executed_seed.csv"
    pd.DataFrame({"rank": range(1, len(executed) + 1), "symbol": executed}).to_csv(path, index=False, encoding="utf-8")
    print(f"universe={len(symbols)} loaded={len(returns.columns)} target={len(target)} executed={len(executed)} blocked={len(result['blocked_entries'])}")
    print(f"seed={path}")


if __name__ == "__main__":
    main()
