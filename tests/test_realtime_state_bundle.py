from __future__ import annotations

import json
from pathlib import Path

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


def test_validate_rejects_stale_anchor(tmp_path: Path) -> None:
    _write_minimal_state(tmp_path)

    report = realtime_state_bundle.validate_state(
        tmp_path,
        max_anchor_age_days=1,
        today=realtime_state_bundle.date.fromisoformat("2026-05-14"),
    )

    assert report["ok"] is False
    assert any("proxy_index is stale" in error for error in report["errors"])
