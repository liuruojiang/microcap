"""Packing includes executed holdings outside the displayed static target list."""
import hashlib
import zipfile
from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest

from scripts import realtime_state_bundle as state


@pytest.fixture
def transport_root(tmp_path, monkeypatch):
    actual = [f"{i:06d}" for i in range(1, 101)]
    target = actual[:-1] + ["000200"]
    effective = tmp_path / state.PROXY_EFFECTIVE_MEMBERS_REL
    effective.parent.mkdir(parents=True)
    pd.DataFrame({"as_of_date": ["2026-09-16"] * 100, "rank": range(1, 101), "symbol": actual}).to_csv(effective, index=False)
    monkeypatch.setattr(state, "_current_member_symbols", lambda root: target)
    monkeypatch.setattr(state, "_latest_proxy_member_symbols", lambda root: target)
    monkeypatch.setattr(state, "_has_current_v2_static_member_context", lambda root: False)
    monkeypatch.setattr(state, "validate_state", lambda *a, **kw: {"ok": True, "errors": [], "anchor_dates": {"proxy_index": "2026-09-17"}})
    monkeypatch.setattr(state, "validate_reference_summary", lambda *a: [])
    for symbol in sorted(set(actual + target)):
        for directory, content in (
            (state.PRICE_CACHE_DIR, "date,close_raw\n2026-09-17,10.0\n"),
            (state.SHARE_CACHE_DIR, "change_date,total_shares_10k\n2026-09-01,10000.0\n"),
        ):
            path = tmp_path / directory / f"{symbol}.csv"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")
    adjusted = tmp_path / state.ADJUSTED_PRICE_CACHE_DIR / "000100.csv"
    adjusted.parent.mkdir(parents=True)
    adjusted.write_text("date,close_qfq\n2026-09-17,9.5\n", encoding="utf-8")
    return tmp_path


def test_new_bundle_contains_actual_position_outside_static_target_and_optional_qfq(transport_root):
    bundle = transport_root / "state.zip"
    report = state.pack_state(transport_root, bundle, None)
    assert report["ok"], report
    with zipfile.ZipFile(bundle) as archive:
        names = set(archive.namelist())
        assert state.PROXY_EFFECTIVE_MEMBERS_REL in names
        for directory in (state.PRICE_CACHE_DIR, state.SHARE_CACHE_DIR, state.ADJUSTED_PRICE_CACHE_DIR):
            assert f"{directory}/000100.csv" in names
        assert f"{state.PRICE_CACHE_DIR}/000200.csv" in names
        state._verify_bundle_manifest(archive)


@pytest.mark.parametrize("missing", [state.PRICE_CACHE_DIR, state.SHARE_CACHE_DIR])
def test_missing_actual_position_cache_blocks_new_pack_without_partial_zip(transport_root, missing):
    (transport_root / missing / "000100.csv").unlink()
    bundle = transport_root / "state.zip"
    report = state.pack_state(transport_root, bundle, None)
    assert report["ok"] is False
    assert any("000100.csv" in error for error in report["errors"])
    assert not bundle.exists()


def test_new_pack_requires_actual_effective_positions_even_with_static_target(transport_root):
    (transport_root / state.PROXY_EFFECTIVE_MEMBERS_REL).unlink()
    report = state.pack_state(transport_root, transport_root / "state.zip", None)
    assert report["ok"] is False
    assert any("actual proxy effective-member" in error for error in report["errors"])


def test_stale_actual_member_price_blocks_new_pack(transport_root):
    (transport_root / state.PRICE_CACHE_DIR / "000100.csv").write_text("date,close_raw\n2026-09-16,10\n", encoding="utf-8")
    assert state.validate_member_cache_transport(transport_root, date(2026, 9, 17))


def test_transport_inventory_does_not_require_effective_state_for_legacy_readback(tmp_path, monkeypatch):
    monkeypatch.setattr(state, "_current_member_symbols", lambda root: [])
    monkeypatch.setattr(state, "_latest_proxy_member_symbols", lambda root: [])
    monkeypatch.setattr(state, "_has_current_v2_static_member_context", lambda root: False)
    assert state._iter_bundle_files(tmp_path) == []


def official_runtime(root):
    import microcap_top100_mom16_biweekly_live_v2_0 as v2
    shared = root / "shared"
    return SimpleNamespace(
        PRICE_DIR=root / state.PRICE_CACHE_DIR,
        SHARE_DIR=root / state.SHARE_CACHE_DIR,
        ADJ_PRICE_DIR=root / state.ADJUSTED_PRICE_CACHE_DIR,
        SHARED_PRICE_DIR=shared / "prices_raw",
        SHARED_SHARE_DIR=shared / "share_change",
        SHARED_ADJ_PRICE_DIR=shared / "prices_qfq",
        resolve_cache_path=v2.base_mod.freq_mod.resolve_cache_path,
    )


def test_materialize_uses_official_resolver_preserves_exact_source_and_existing_local(transport_root):
    runtime = official_runtime(transport_root)
    for local, shared, symbol in (
        (runtime.SHARE_DIR, runtime.SHARED_SHARE_DIR, "000100"),
        (runtime.PRICE_DIR, runtime.SHARED_PRICE_DIR, "000200"),
        (runtime.ADJ_PRICE_DIR, runtime.SHARED_ADJ_PRICE_DIR, "000100"),
    ):
        shared.mkdir(parents=True, exist_ok=True)
        (local / f"{symbol}.csv").rename(shared / f"{symbol}.csv")
    canonical = runtime.PRICE_DIR / "000001.csv"
    before = canonical.read_bytes()
    runtime.SHARED_PRICE_DIR.mkdir(exist_ok=True)
    (runtime.SHARED_PRICE_DIR / "000001.csv").write_bytes(b"different shared bytes must not replace local")
    report = state.materialize_member_cache_inputs(transport_root, runtime)
    assert len(report) == 3
    for row in report:
        local = transport_root / row["local"]
        expected = hashlib.sha256(local.read_bytes()).hexdigest()
        assert expected == row["source_sha256"] == row["local_sha256"]
    assert canonical.read_bytes() == before
    assert state.validate_member_cache_transport(transport_root, date(2026, 9, 17)) == []
    assert state.materialize_member_cache_inputs(transport_root, runtime) == []


def test_materialize_does_not_invent_absent_optional_adjusted_prices(transport_root):
    runtime = official_runtime(transport_root)
    assert not (runtime.ADJ_PRICE_DIR / "000001.csv").exists()
    assert state.materialize_member_cache_inputs(transport_root, runtime) == []
    assert not (runtime.ADJ_PRICE_DIR / "000001.csv").exists()


def test_materialize_missing_required_source_fails_before_any_copy(transport_root):
    runtime = official_runtime(transport_root)
    (runtime.SHARE_DIR / "000100.csv").unlink()
    with pytest.raises(FileNotFoundError, match="required member cache"):
        state.materialize_member_cache_inputs(transport_root, runtime)
    assert not (runtime.SHARE_DIR / "000100.csv").exists()
