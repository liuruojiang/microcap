import numpy as np
import pandas as pd
import pytest

import analyze_top100_rebalance_frequency as standalone
import microcap_top100_mom16_biweekly_live_v2_0 as v20


@pytest.fixture(params=[standalone, v20.freq_mod], ids=['standalone', 'embedded'])
def engine(request):
    return request.param


def run_case(engine, missing, targets=True):
    dates = pd.bdate_range('2020-08-24', periods=3)
    returns = pd.DataFrame({'000001': [0, .02, .02],
                            '000002': [0, 0, missing],
                            '000003': [np.nan] * 3}, index=dates)
    buy = pd.DataFrame(True, index=dates, columns=returns.columns)
    sell = buy.copy()
    sell.loc[dates[-1], '000002'] = False
    return engine.simulate_rebalance_path(
        dates, returns, {dates[0]: ['000001', '000002']} if targets else {},
        dates[:1], buy, sell, .003, top_n=2,
        execution_timing=engine.EXECUTION_TIMING_CLOSE)


@pytest.mark.parametrize('missing', [np.nan, np.inf, -np.inf])
def test_missing_held_return_cannot_become_zero(engine, missing):
    with pytest.raises(RuntimeError, match=r'Unvalued held securities on 2020-08-26: 000002'):
        run_case(engine, missing)


def test_finite_zero_and_unheld_missing_remain_valid(engine):
    index, _, _ = run_case(engine, 0)
    np.testing.assert_allclose(index.daily_return.iloc[1:], [.01, .01])
    assert index.close.iloc[-1] == pytest.approx(1020.1)


def test_cash_does_not_require_unheld_prices(engine):
    index, _, _ = run_case(engine, np.nan, targets=False)
    assert index.close.eq(1000).all()
