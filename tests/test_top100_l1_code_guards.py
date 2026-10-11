"""Regression cases from the L1 independent member and price-limit audit."""

import pandas as pd
import pytest

import analyze_top100_rebalance_frequency as frequency
import analyze_microcap_zz1000_hedge as hedge
import microcap_top100_mom16_biweekly_live as live
import microcap_top100_mom16_biweekly_live_v2_0 as v20


@pytest.mark.parametrize("module", [frequency, v20.freq_mod])
def test_st_prefixes_and_non_st_word(module):
    assert module.is_st_name("S ST样例")
    assert module.is_st_name("G*ST样例")
    assert not module.is_st_name("STABLE")


@pytest.mark.parametrize("module", [live, v20.base_mod])
def test_current_member_gate_rejects_prefixed_st(module):
    assert not module.is_live_tradable_name("S ST样例")
    assert not module.is_live_tradable_name("G*ST样例")
    assert module.is_live_tradable_name("STABLE")


@pytest.mark.parametrize("module", [frequency, v20.freq_mod])
def test_decimal_half_cent_limit_case(module):
    # 4.10 * 0.95 = 3.895, rounded half-up to 3.90 yuan.
    assert module.is_price_at_limit(3.90, 4.10, 0.05, -1)
    assert not module.is_price_at_limit(3.89, 4.10, 0.05, -1)


@pytest.mark.parametrize("module", [frequency, v20.freq_mod])
def test_main_board_st_limit_switch(module):
    assert module.get_price_limit_ratio("600001", pd.Timestamp("2026-07-03"), True) == 0.05
    assert module.get_price_limit_ratio("600001", pd.Timestamp("2026-07-06"), True) == 0.10


@pytest.mark.parametrize("module", [hedge, v20.hedge_mod])
def test_common_performance_metrics_count_return_sessions(module):
    first_day = pd.Series([-0.02], index=pd.to_datetime(["2026-09-03"]))
    result = module.calc_metrics(first_day)
    assert result.annual == pytest.approx(0.98 ** module.CN_TRADING_DAYS - 1.0)
    assert result.max_dd == pytest.approx(-0.02)


@pytest.mark.parametrize("module", [frequency, v20.freq_mod])
def test_historical_date_loader_rejects_holiday_panel(module, tmp_path, monkeypatch):
    from datetime import date
    from scripts import exchange_calendar

    panel = tmp_path / "panel.csv"
    panel.write_text("date\n2018-06-15\n2018-06-18\n2018-06-19\n", encoding="utf-8")
    monkeypatch.setattr(module.hedge_mod, "DEFAULT_PANEL", panel)
    monkeypatch.setattr(module, "START_DATE", "2018-06-15")
    monkeypatch.setattr(module, "END_DATE", "2018-06-19")
    monkeypatch.setattr(exchange_calendar, "sessions_for_day",
                        lambda _day: (date(2018, 6, 15), date(2018, 6, 19)))

    with pytest.raises(ValueError, match="2018-06-18"):
        module.load_trading_dates()
