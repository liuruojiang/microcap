"""Exchange price-limit change must not misclassify 2026 ST close fills."""

import pandas as pd

import microcap_top100_mom16_biweekly_live_v2_0 as strategy


def test_mainboard_st_limit_changes_on_2026_07_06():
    ratio = strategy.freq_mod.get_price_limit_ratio
    assert ratio("600123", pd.Timestamp("2026-07-03"), is_st=True) == 0.05
    assert ratio("600123", pd.Timestamp("2026-07-06"), is_st=True) == 0.10
    assert ratio("000123", pd.Timestamp("2026-07-03"), is_st=True) == 0.05
    assert ratio("000123", pd.Timestamp("2026-07-06"), is_st=True) == 0.10


def test_chinext_and_star_keep_their_own_rules():
    ratio = strategy.freq_mod.get_price_limit_ratio
    assert ratio("300123", pd.Timestamp("2020-08-21"), is_st=True) == 0.05
    assert ratio("300123", pd.Timestamp("2020-08-24"), is_st=True) == 0.20
    assert ratio("688123", pd.Timestamp("2026-07-06"), is_st=True) == 0.20
    assert ratio("600123", pd.Timestamp("2026-07-06"), is_st=False) == 0.10
