import pandas as pd
import pytest

import microcap_top100_mom16_biweekly_live as live
import microcap_top100_mom16_biweekly_live_v2_0 as live_v20

REALTIME_MODULES = [live, live_v20.realtime_core.base_mod]


@pytest.mark.parametrize("module", REALTIME_MODULES)
def test_member_realtime_return_is_zero_when_quote_date_matches_anchor(module):
    quotes = pd.DataFrame(
        {
            "code": ["000001"],
            "rt_price": [95.0],
            "pre_close": [100.0],
            "trade_date": ["2026-05-15"],
        }
    ).set_index("code")

    ret = module.compute_member_realtime_return(
        "000001",
        {"000001": {"date": "2026-05-15", "close": 100.0}},
        quotes,
        pd.Timestamp("2026-05-15"),
    )

    assert ret == 0.0


@pytest.mark.parametrize("module", REALTIME_MODULES)
def test_same_day_quote_does_not_overwrite_close_confirmed_anchor(module):
    close_df = pd.DataFrame(
        {"microcap": [100.0], "hedge": [200.0]},
        index=pd.DatetimeIndex([pd.Timestamp("2026-05-15")]),
    )

    out = module.apply_realtime_close_to_signal_frame(
        close_df=close_df,
        latest_trade_date=pd.Timestamp("2026-05-15"),
        snapshot_ts=pd.Timestamp("2026-05-17 16:30:00+08:00"),
        microcap_rt_close=95.0,
        hedge_rt_close=198.0,
        quote_trade_date="2026-05-15",
    )

    assert float(out.loc[pd.Timestamp("2026-05-15"), "microcap"]) == 100.0
    assert float(out.loc[pd.Timestamp("2026-05-15"), "hedge"]) == 200.0


@pytest.mark.parametrize("module", REALTIME_MODULES)
def test_later_quote_date_adds_realtime_snapshot_row(module):
    close_df = pd.DataFrame(
        {"microcap": [100.0], "hedge": [200.0]},
        index=pd.DatetimeIndex([pd.Timestamp("2026-05-15")]),
    )

    out = module.apply_realtime_close_to_signal_frame(
        close_df=close_df,
        latest_trade_date=pd.Timestamp("2026-05-15"),
        snapshot_ts=pd.Timestamp("2026-05-18 10:30:00+08:00"),
        microcap_rt_close=101.0,
        hedge_rt_close=201.0,
        quote_trade_date="2026-05-18",
    )

    assert float(out.loc[pd.Timestamp("2026-05-15"), "microcap"]) == 100.0
    assert float(out.loc[pd.Timestamp("2026-05-18"), "microcap"]) == 101.0
    assert float(out.loc[pd.Timestamp("2026-05-18"), "hedge"]) == 201.0
