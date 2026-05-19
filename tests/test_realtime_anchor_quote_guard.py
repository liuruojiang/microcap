import pandas as pd
import pytest
from contextlib import nullcontext
from types import SimpleNamespace

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


def test_v2_realtime_context_uses_cached_proxy_when_anchor_refresh_fails(tmp_path, monkeypatch):
    index_csv = tmp_path / "index.csv"
    costed_nav_csv = tmp_path / "nav.csv"
    panel_shadow = tmp_path / "panel_shadow.csv"
    proxy_turnover = tmp_path / "proxy_turnover.csv"
    proxy_meta = tmp_path / "proxy_meta.json"

    for path in (index_csv, costed_nav_csv, panel_shadow):
        path.write_text("date\n2026-05-15\n", encoding="utf-8")
    proxy_turnover.write_text("rebalance_date\n2026-05-15\n", encoding="utf-8")
    proxy_meta.write_text("{}", encoding="utf-8")

    args = SimpleNamespace(
        index_csv=index_csv,
        costed_nav_csv=costed_nav_csv,
        max_stale_anchor_days=5,
        allow_stale_realtime=False,
    )
    paths = {
        "panel_shadow": panel_shadow,
        "proxy_turnover": proxy_turnover,
        "proxy_meta": proxy_meta,
    }

    def raise_refresh_error(_args, _paths):
        raise RuntimeError("Free index history sources returned empty data for 1.000852")

    seen = {}

    def cached_proxy_context(_args, _paths, panel_path, target_end_date, reason):
        seen["panel_path"] = panel_path
        seen["target_end_date"] = pd.Timestamp(target_end_date)
        seen["reason"] = reason
        return {"latest_rebalance": pd.Timestamp("2026-05-15")}

    monkeypatch.setattr(live_v20, "_v2_base_build_lock", lambda: nullcontext())
    monkeypatch.setattr(live_v20, "_ensure_base_outputs_unlocked", lambda: None)
    monkeypatch.setattr(live_v20, "_build_base_args", lambda: args)
    monkeypatch.setattr(live_v20, "_load_reference_summary_unlocked", lambda: {"source": "test"})
    monkeypatch.setattr(live_v20.base_mod, "DEFAULT_OUTPUT_PREFIX", "test")
    monkeypatch.setattr(live_v20.base_mod, "build_output_paths", lambda _prefix: paths)
    monkeypatch.setattr(live_v20.base_mod, "refresh_history_anchor", raise_refresh_error)
    monkeypatch.setattr(live_v20.base_mod, "proxy_meta_matches_execution_model", lambda _meta: True)
    monkeypatch.setattr(live_v20.base_mod, "build_realtime_context_from_cached_proxy", cached_proxy_context)
    monkeypatch.setattr(
        live_v20.base_mod,
        "ensure_static_members_fresh",
        lambda _args, _paths, _panel_path, _target_end_date, base_context: dict(base_context),
    )

    context, turnover_df, reference_summary = live_v20.load_realtime_context()

    assert seen["panel_path"] == panel_shadow
    assert seen["target_end_date"] == pd.Timestamp("2026-05-15")
    assert "Free index history sources returned empty data" in seen["reason"]
    assert context["latest_rebalance"] == pd.Timestamp("2026-05-15")
    assert turnover_df["rebalance_date"].iloc[0] == pd.Timestamp("2026-05-15")
    assert reference_summary == {"source": "test"}
