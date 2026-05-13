from __future__ import annotations

import json
import sys
import types
from pathlib import Path

import pytest

from scripts import realtime_state_bundle


def _write_csv(path: Path, header: str, row: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"{header}\n{row}\n", encoding="utf-8")


def _write_minimal_state(root: Path) -> None:
    outputs = root / "outputs"
    cache = root / ".microcap_index_cache"
    outputs.mkdir(parents=True, exist_ok=True)
    cache.mkdir(parents=True, exist_ok=True)

    _write_csv(
        outputs / "wind_microcap_top_100_biweekly_thursday_16y_cached.csv",
        "date,close,daily_return",
        "2026-05-11,1000,0.01",
    )
    _write_csv(
        outputs / "microcap_top100_mom16_biweekly_live_v1_1_proxy_members.csv",
        "rebalance_date,symbol,rank",
        "2026-05-07,000001,1",
    )
    _write_csv(
        outputs / "microcap_top100_mom16_biweekly_live_v1_1_proxy_turnover.csv",
        "rebalance_date,turnover",
        "2026-05-07,0.25",
    )
    _write_csv(
        outputs / "microcap_top100_mom16_hedge_zz1000_0p8x_biweekly_thursday_16y_costed_nav.csv",
        "date,nav_net",
        "2026-05-11,1.1",
    )
    _write_csv(cache / "active_universe.csv", "code,name", "000001,example")
    _write_csv(cache / "current_st.csv", "code", "000002")
    _write_csv(
        cache / "realtime/microcap_top100_mom16_biweekly_live_v1_1_static_effective_members.csv",
        "symbol,rank",
        "000001,1",
    )
    _write_csv(
        cache / "prices_raw/000001.csv",
        "date,close_raw",
        "2026-05-11,10.0",
    )
    _write_csv(
        cache / "share_change/000001.csv",
        "change_date,total_shares_10k",
        "2026-05-01,10000",
    )

    (outputs / "microcap_top100_mom16_biweekly_live_summary.json").write_text(
        json.dumps({"latest_trade_date": "2026-05-11"}),
        encoding="utf-8",
    )
    (outputs / "microcap_top100_mom16_biweekly_live_v1_1_proxy_meta.json").write_text(
        json.dumps({"core_params": {"research_stack_version": "test"}}),
        encoding="utf-8",
    )


def test_validate_reports_missing_required_state(tmp_path: Path) -> None:
    report = realtime_state_bundle.validate_state(tmp_path)

    assert report["ok"] is False
    assert "missing required file: outputs/wind_microcap_top_100_biweekly_thursday_16y_cached.csv" in report["errors"]


def test_pack_restore_round_trip_validates_required_state(tmp_path: Path) -> None:
    source = tmp_path / "source"
    restored = tmp_path / "restored"
    bundle = tmp_path / "state.zip"
    _write_minimal_state(source)

    pack_report = realtime_state_bundle.pack_state(source, bundle, max_anchor_age_days=None)
    restore_report = realtime_state_bundle.restore_state(restored, bundle, max_anchor_age_days=None)

    assert pack_report["ok"] is True
    assert restore_report["ok"] is True
    assert (restored / "outputs/microcap_top100_mom16_biweekly_live_v1_1_proxy_meta.json").is_file()
    assert (restored / ".microcap_index_cache/active_universe.csv").is_file()
    assert (restored / ".microcap_index_cache/prices_raw/000001.csv").is_file()
    assert (restored / ".microcap_index_cache/share_change/000001.csv").is_file()


def test_validate_rejects_missing_current_member_price_cache(tmp_path: Path) -> None:
    _write_minimal_state(tmp_path)
    (tmp_path / ".microcap_index_cache/prices_raw/000001.csv").unlink()

    report = realtime_state_bundle.validate_state(tmp_path)

    assert report["ok"] is True
    assert "missing current member price cache: .microcap_index_cache/prices_raw/000001.csv" in report["warnings"]


def test_validate_rejects_stale_anchor(tmp_path: Path) -> None:
    _write_minimal_state(tmp_path)

    report = realtime_state_bundle.validate_state(
        tmp_path,
        max_anchor_age_days=1,
        today=realtime_state_bundle.date.fromisoformat("2026-05-14"),
    )

    assert report["ok"] is False
    assert any("proxy_index is stale" in error for error in report["errors"])


def test_refresh_reuses_existing_valid_state_when_external_refresh_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_minimal_state(tmp_path)
    calls: list[str] = []

    runner_module = types.ModuleType("run_top100_v1_6_v1_8_realtime_signals")

    def ensure_static_realtime_inputs(force_refresh: bool = False) -> None:
        calls.append(f"static:{force_refresh}")

    runner_module.ensure_static_realtime_inputs = ensure_static_realtime_inputs

    base_mod = types.SimpleNamespace(
        DEFAULT_OUTPUT_PREFIX="microcap_top100_mom16_biweekly_live_v1_1",
        build_output_paths=lambda _prefix: {
            "proxy_meta": tmp_path / "outputs/microcap_top100_mom16_biweekly_live_v1_1_proxy_meta.json",
            "proxy_members": tmp_path / "outputs/microcap_top100_mom16_biweekly_live_v1_1_proxy_members.csv",
            "proxy_turnover": tmp_path / "outputs/microcap_top100_mom16_biweekly_live_v1_1_proxy_turnover.csv",
        },
        build_refreshed_panel_shadow=lambda *_args: (tmp_path / "panel.csv", realtime_state_bundle.date.fromisoformat("2026-05-13")),
        ensure_strategy_files=lambda *_args: (_ for _ in ()).throw(RuntimeError("provider refresh failed")),
    )
    core_module = types.ModuleType("top100_realtime_core")
    core_module.BASE_COSTED_NAV_CSV = tmp_path / "outputs/microcap_top100_mom16_hedge_zz1000_0p8x_biweekly_thursday_16y_costed_nav.csv"
    core_module.base_mod = base_mod
    core_module.v1_1_mod = types.SimpleNamespace(prepare_current_v1_1_outputs=lambda **_kwargs: None)
    core_module.build_v1_1_args = lambda max_workers=8: types.SimpleNamespace(max_workers=max_workers)

    monkeypatch.setitem(sys.modules, "run_top100_v1_6_v1_8_realtime_signals", runner_module)
    monkeypatch.setitem(sys.modules, "top100_realtime_core", core_module)

    report = realtime_state_bundle.refresh_state(tmp_path, max_workers=1)

    assert report["ok"] is True
    assert report["refresh_source"] == "existing_validated_state"
    assert "provider refresh failed" in report["refresh_warning"]
    assert calls == ["static:False"]
