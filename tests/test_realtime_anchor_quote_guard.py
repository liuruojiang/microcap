import pandas as pd
import pytest
import sys
from contextlib import nullcontext
from types import SimpleNamespace

import microcap_top100_mom16_biweekly_live as live
import microcap_top100_mom16_biweekly_live_v2_0 as live_v20
from scripts import realtime_state_bundle

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


def test_v2_state_only_realtime_context_does_not_refresh_anchor(tmp_path, monkeypatch):
    index_csv = tmp_path / "index.csv"
    costed_nav_csv = tmp_path / "nav.csv"
    panel_shadow = tmp_path / "panel_shadow.csv"
    proxy_turnover = tmp_path / "proxy_turnover.csv"
    proxy_meta = tmp_path / "proxy_meta.json"

    for path in (index_csv, costed_nav_csv, panel_shadow):
        path.write_text("date\n2026-05-19\n", encoding="utf-8")
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

    def fail_refresh(_args, _paths):
        raise AssertionError("state-only realtime path must not refresh history anchor")

    seen = {}

    def cached_proxy_context(_args, _paths, panel_path, target_end_date, reason):
        seen["panel_path"] = panel_path
        seen["target_end_date"] = pd.Timestamp(target_end_date)
        seen["reason"] = reason
        return {"latest_rebalance": pd.Timestamp("2026-05-15")}

    monkeypatch.setenv("TOP100_REALTIME_REQUIRE_STATE", "1")
    monkeypatch.setattr(live_v20, "_v2_base_build_lock", lambda: nullcontext())
    monkeypatch.setattr(live_v20, "_ensure_base_outputs_unlocked", lambda: None)
    monkeypatch.setattr(live_v20, "_build_base_args", lambda: args)
    monkeypatch.setattr(live_v20, "_load_reference_summary_unlocked", lambda: {"source": "test"})
    monkeypatch.setattr(live_v20.base_mod, "DEFAULT_OUTPUT_PREFIX", "test")
    monkeypatch.setattr(live_v20.base_mod, "build_output_paths", lambda _prefix: paths)
    monkeypatch.setattr(live_v20.base_mod, "refresh_history_anchor", fail_refresh)
    monkeypatch.setattr(live_v20.base_mod, "build_realtime_context_from_cached_proxy", cached_proxy_context)
    monkeypatch.setattr(
        live_v20.base_mod,
        "ensure_static_members_fresh",
        lambda _args, _paths, _panel_path, _target_end_date, base_context: dict(base_context),
    )

    context, turnover_df, reference_summary = live_v20.load_realtime_context()

    assert seen["panel_path"] == panel_shadow
    assert seen["target_end_date"] == pd.Timestamp("2026-05-19")
    assert "production state-only mode" in seen["reason"]
    assert context["latest_rebalance"] == pd.Timestamp("2026-05-15")
    assert turnover_df["rebalance_date"].iloc[0] == pd.Timestamp("2026-05-15")
    assert reference_summary == {"source": "test"}


def test_refresh_state_rejects_fresh_run_when_anchor_lags_target(tmp_path, monkeypatch):
    root = tmp_path
    for rel in realtime_state_bundle.REQUIRED_FILES:
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        if rel.endswith(".json"):
            path.write_text("{}", encoding="utf-8")
        elif rel.endswith("proxy_turnover.csv"):
            path.write_text("rebalance_date\n2026-05-15\n", encoding="utf-8")
        else:
            path.write_text("date\n2026-05-15\n", encoding="utf-8")

    def ensure_static_realtime_inputs(force_refresh=False):
        return None

    fake_runner = SimpleNamespace(ensure_static_realtime_inputs=ensure_static_realtime_inputs)

    def build_v1_1_args(max_workers=8):
        return SimpleNamespace(max_workers=max_workers)

    fake_base_mod = SimpleNamespace(
        DEFAULT_OUTPUT_PREFIX="microcap_top100_mom16_biweekly_live_v1_1",
        build_output_paths=lambda _prefix: {},
        build_refreshed_panel_shadow=lambda _args, _paths: (root / "panel.csv", pd.Timestamp("2026-05-19")),
        ensure_strategy_files=lambda _args, _paths, _panel_path, _target_end_ts: None,
        ensure_realtime_query_base_context=lambda _args, _paths, _panel_path, _target_end_ts: {"ok": True},
        ensure_static_members_fresh=lambda _args, _paths, _panel_path, _target_end_ts, _base_context: None,
    )
    fake_v1_1_mod = SimpleNamespace(prepare_current_v1_1_outputs=lambda paths, costed_nav_csv: None)
    fake_core = SimpleNamespace(
        build_v1_1_args=build_v1_1_args,
        base_mod=fake_base_mod,
        v1_1_mod=fake_v1_1_mod,
        BASE_COSTED_NAV_CSV=root / "base_nav.csv",
    )
    monkeypatch.setitem(sys.modules, "run_top100_v1_6_v1_8_realtime_signals", fake_runner)
    monkeypatch.setitem(sys.modules, "top100_realtime_core", fake_core)

    with pytest.raises(RuntimeError, match="older than refresh target"):
        realtime_state_bundle.refresh_state(root)
