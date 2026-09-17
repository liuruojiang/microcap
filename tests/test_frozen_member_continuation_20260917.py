"""Synthetic boundary tests, not strategy performance or investment signals."""
from types import SimpleNamespace

import pandas as pd
import pytest

import microcap_top100_mom16_biweekly_live_v2_0 as strategy


base = strategy.base_mod
freq = base.freq_mod


def simulate(dates, *, seed=None, targets=None, rebalances=None, blocked_buy=None):
    dates = pd.DatetimeIndex(dates)
    returns = pd.DataFrame({"000001": 0.02, "000002": 0.04, "000003": 0.50}, index=dates)
    buy = pd.DataFrame(True, index=dates, columns=returns.columns)
    sell = buy.copy()
    if blocked_buy:
        buy.loc[pd.Timestamp(blocked_buy), "000003"] = False
    return freq.simulate_rebalance_path(
        trading_dates=dates, returns_df=returns,
        target_members_map=targets or {},
        rebalance_dates=pd.DatetimeIndex(rebalances or []),
        buyable_df=buy, sellable_df=sell, one_side_cost_rate=0.003,
        top_n=2, execution_timing=freq.EXECUTION_TIMING_CLOSE,
        initial_members=["000001", "000002"] if seed is None else seed,
    )


def test_first_appended_return_uses_frozen_seed():
    index, turnover, effective = simulate(["2026-09-16", "2026-09-17"])
    assert index.iloc[0].holding_count == 2
    assert index.iloc[1].daily_return == pytest.approx(0.03)
    assert index.iloc[1].close == pytest.approx(1030.0)
    assert turnover.empty
    assert effective == {}


def test_already_executed_bridge_rebalance_is_not_replayed():
    bridge = pd.Timestamp("2026-09-17")
    index, turnover, effective = simulate(
        [bridge, "2026-09-18"], rebalances=[bridge],
        targets={bridge: ["000002", "000003"]},
    )
    assert index.iloc[1].daily_return == pytest.approx(0.03)
    assert turnover.empty
    assert effective == {}


def test_new_close_rebalance_updates_subsequent_return_and_effective_members():
    day = pd.Timestamp("2026-09-17")
    index, turnover, effective = simulate(
        ["2026-09-16", day, "2026-09-18"], rebalances=[day],
        targets={day: ["000002", "000003"]},
    )
    assert index.iloc[1].daily_return == pytest.approx(0.03)
    assert index.iloc[2].daily_return == pytest.approx(0.27)
    assert effective[day] == ["000002", "000003"]
    assert turnover.iloc[0].entry_count == turnover.iloc[0].exit_count == 1
    assert turnover.iloc[0].two_side_cost_rate == pytest.approx(0.003)


def test_final_close_rebalance_is_persisted_without_changing_earned_return():
    day = pd.Timestamp("2026-09-17")
    index, turnover, effective = simulate(
        ["2026-09-16", day], rebalances=[day],
        targets={day: ["000002", "000003"]},
    )
    assert index.iloc[-1].daily_return == pytest.approx(0.03)
    assert effective[day] == ["000002", "000003"]
    assert len(turnover) == 1
    assert pd.isna(turnover.iloc[0].return_start_date)


@pytest.mark.parametrize("seed", [[], ["000001"], ["000001", "000001"], ["000001", "999999"]])
def test_incomplete_duplicate_or_unpriced_seed_is_rejected(seed):
    with pytest.raises(ValueError, match="Continuation"):
        simulate(["2026-09-16", "2026-09-17"], seed=seed)


def seed_files(tmp_path, seed_date="2026-09-03", turnover_dates=None):
    effective = tmp_path / "effective.csv"
    turnover = tmp_path / "turnover.csv"
    pd.DataFrame({"as_of_date": seed_date, "rank": range(1, 101),
                  "symbol": [f"{i:06d}" for i in range(100)]}).to_csv(effective, index=False)
    pd.DataFrame({"rebalance_date": turnover_dates or ["2026-09-03"]}).to_csv(turnover, index=False)
    return {"proxy_effective_members": effective, "proxy_turnover": turnover}


def test_seed_covers_bridge_without_intervening_execution(tmp_path):
    paths = seed_files(tmp_path)
    assert len(base._continuation_members_at_bridge(paths, pd.Timestamp("2026-09-16"))) == 100


def test_missing_seed_refuses_implicit_reconstruction(tmp_path):
    paths = seed_files(tmp_path)
    paths["proxy_effective_members"].unlink()
    with pytest.raises(RuntimeError, match="Missing executed member seed"):
        base._continuation_members_at_bridge(paths, pd.Timestamp("2026-09-16"))


@pytest.mark.parametrize("seed_date,rebalance_dates", [
    ("2026-09-18", ["2026-09-03"]),
    ("2026-08-20", ["2026-08-20", "2026-09-03"]),
])
def test_future_or_execution_gap_seed_rejected(tmp_path, seed_date, rebalance_dates):
    paths = seed_files(tmp_path, seed_date, rebalance_dates)
    with pytest.raises(RuntimeError, match="does not cover the frozen bridge"):
        base._continuation_members_at_bridge(paths, pd.Timestamp("2026-09-16"))


def test_panel_missing_bridge_date_rejected_before_candidate_refresh(tmp_path, monkeypatch):
    index_path = tmp_path / "index.csv"
    panel_path = tmp_path / "panel.csv"
    pd.DataFrame({"date": ["2026-09-16"], "close": [1000.0]}).to_csv(index_path, index=False)
    pd.DataFrame({"date": ["2026-09-17"]}).to_csv(panel_path, index=False)
    def forbidden(**kwargs):
        pytest.fail("candidate refresh must not run without a bridge")
    monkeypatch.setattr(base, "select_recent_candidate_symbols", forbidden)
    with pytest.raises(RuntimeError, match="does not overlap"):
        base.extend_index_recent_window(SimpleNamespace(index_csv=index_path), {}, panel_path, pd.Timestamp("2026-09-17"))


def test_no_rebalance_extension_preserves_frozen_members_and_advances_effective(tmp_path, monkeypatch):
    paths = seed_files(tmp_path)
    paths["proxy_members"] = tmp_path / "members.csv"
    paths["proxy_meta"] = tmp_path / "meta.json"
    pd.DataFrame({"rebalance_date": ["2026-09-03"], "symbol": ["000001"]}).to_csv(paths["proxy_members"], index=False)
    old_members = paths["proxy_members"].read_text()
    index_path, panel_path = tmp_path / "index.csv", tmp_path / "panel.csv"
    pd.DataFrame({"date": ["2026-09-15"], "close": [2000.0], "daily_return": [0.01], "holding_count": [100]}).to_csv(index_path, index=False)
    pd.DataFrame({"date": ["2026-09-15", "2026-09-16"]}).to_csv(panel_path, index=False)
    captured = {}
    monkeypatch.setattr(base, "select_recent_candidate_symbols", lambda **kwargs: ["999999"])
    monkeypatch.setattr(base, "refresh_price_cache_tail", lambda *args, **kwargs: None)
    def build(**kwargs):
        captured.update(kwargs)
        return (pd.DataFrame({"date": pd.to_datetime(["2026-09-15", "2026-09-16"]),
                              "close": [1000.0, 1010.0], "daily_return": [float("nan"), 0.01], "holding_count": [100, 100]}),
                pd.DataFrame(), pd.DataFrame(),
                {"continuation_effective_members": kwargs["initial_members"]})
    monkeypatch.setattr(base, "build_local_proxy_bundle", build)
    base.extend_index_recent_window(SimpleNamespace(index_csv=index_path, max_workers=1, force_refresh=False),
                                    paths, panel_path, pd.Timestamp("2026-09-16"))
    assert len(captured["symbols"]) == 101
    assert len(captured["initial_members"]) == 100
    assert pd.read_csv(index_path).iloc[-1].close == pytest.approx(2020.0)
    assert pd.read_csv(paths["proxy_effective_members"]).as_of_date.unique().tolist() == ["2026-09-16"]
    assert paths["proxy_members"].read_text() == old_members
