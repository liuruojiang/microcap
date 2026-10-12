"""Independent counterexamples for formal ST loading and whole-state recovery."""

from pathlib import Path

import pandas as pd
import pytest

import microcap_top100_mom16_biweekly_live_v2_0 as v20
from scripts import top100_cloud_delivery as cloud
from scripts import top100_delivery as delivery
from scripts import realtime_state_bundle as state
from test_security_meta_current_st_history_resolution_20261012 import unresolved_688496  # noqa: F401
from test_top100_cloud_delivery import prepare
from test_top100_delivery import workspace as workspace_factory


@pytest.fixture(scope="module")
def frozen_bundle(tmp_path_factory):
    """Pack once through the formal implementation; each case restores separately."""
    source = tmp_path_factory.mktemp("frozen-whole-delivery")
    with pytest.MonkeyPatch.context() as monkeypatch:
        workspace_factory.__wrapped__(source, monkeypatch)
        real_check_output = cloud.subprocess.check_output
        bundle, _unused_target = prepare(source, monkeypatch)
        monkeypatch.setattr(cloud.subprocess, "check_output", real_check_output)
        before = {path: state._sha256(path) for path in source.rglob("*") if path.is_file()}
        bundle_sha = state._sha256(bundle)
        yield source, bundle
        assert state._sha256(bundle) == bundle_sha
        assert {path: state._sha256(path) for path in source.rglob("*") if path.is_file()} == before


def fresh_target(frozen_bundle, tmp_path):
    source, bundle = frozen_bundle
    target = tmp_path / "target"
    target.mkdir()
    for name in cloud.SOURCE_FILES:
        cloud.copy_file(source / name, target / name)
    return source, bundle, target


def _unavailable_official_history(unresolved_688496, tmp_path, monkeypatch):
    ns, cache, old = unresolved_688496
    price = tmp_path / "688496-prices.csv"
    share = tmp_path / "688496-shares.csv"
    price.write_text("date,close_raw\n2022-12-28,1.0\n2026-09-10,1.0\n", encoding="utf-8")
    share.write_text("change_date,total_shares_10k\n2022-12-28,100.0\n", encoding="utf-8")

    def resolve_cache(base_dir, _shared_dir, _symbol):
        if base_dir == ns["PRICE_DIR"]:
            return price
        if base_dir == ns["SHARE_DIR"]:
            return share
        return None

    def unavailable_notices(**_kwargs):
        raise RuntimeError("independent counterexample: official notice source unavailable")

    monkeypatch.setitem(ns, "resolve_cache_path", resolve_cache)
    monkeypatch.setitem(ns, "load_security_master", lambda: pd.DataFrame())
    monkeypatch.setitem(ns, "fetch_cninfo_st_notices", unavailable_notices)
    return ns, cache, cache.read_bytes()


def test_actual_unresolved_builder_error_survives_official_meta_loader(
    unresolved_688496, tmp_path, monkeypatch
):
    ns, cache, before = _unavailable_official_history(unresolved_688496, tmp_path, monkeypatch)
    with pytest.raises(ns["SecurityMetaHistoryEvidenceUnresolved"], match="688496"):
        ns["load_security_meta"]("688496")
    assert cache.read_bytes() == before


def test_actual_unresolved_builder_blocks_official_cache_panel_loader(
    unresolved_688496, tmp_path, monkeypatch
):
    ns, cache, before = _unavailable_official_history(unresolved_688496, tmp_path, monkeypatch)
    dates = pd.DatetimeIndex([pd.Timestamp("2026-09-10")])
    with pytest.raises(RuntimeError, match="symbol cache panel load failed") as exc:
        v20.base_mod.freq_mod.load_cache_panels(
            ["688496"], trading_dates=dates, cap_dates=dates, max_workers=1
        )
    assert "SecurityMetaHistoryEvidenceUnresolved" in str(exc.value)
    assert cache.read_bytes() == before


@pytest.mark.parametrize("version", ["0", "3", "5"])
@pytest.mark.parametrize("suffix", ["costed", "nav.csv", "latest_signal.csv", "performance_nav.csv"])
def test_whole_restore_preserves_newer_final_stream_even_with_incomplete_base(
    frozen_bundle, tmp_path, monkeypatch, version, suffix
):
    workspace, bundle, target = fresh_target(frozen_bundle, tmp_path)
    prefix = f"outputs/microcap_top100_mom16_biweekly_live_v2_{version}"
    name = f"outputs/{delivery.COSTED[version]}" if suffix == "costed" else f"{prefix}_{suffix}"
    path = target / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes((workspace / name).read_bytes().replace(b"2026-09-03", b"2026-09-04"))
    before = {p.relative_to(target): p.read_bytes() for p in target.rglob("*") if p.is_file()}
    monkeypatch.setattr(cloud.os, "open", lambda *a, **kw: pytest.fail("restore created a lock before checking local final dates"))
    with pytest.raises(ValueError, match="newer local state"):
        cloud.restore(target, bundle, "2026-09-03")
    after = {p.relative_to(target): p.read_bytes() for p in target.rglob("*") if p.is_file()}
    assert after == before


@pytest.mark.parametrize("version", ["0", "3", "5"])
@pytest.mark.parametrize("bad_date", ["not-a-date", "2026-09-03T14:00:00+08:00"])
def test_whole_restore_rejects_damaged_final_date_before_any_write(
    frozen_bundle, tmp_path, monkeypatch, version, bad_date
):
    workspace, bundle, target = fresh_target(frozen_bundle, tmp_path)
    name = f"outputs/microcap_top100_mom16_biweekly_live_v2_{version}_latest_signal.csv"
    path = target / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes((workspace / name).read_bytes().replace(b"2026-09-03", bad_date.encode("ascii")))
    before = {p.relative_to(target): p.read_bytes() for p in target.rglob("*") if p.is_file()}
    monkeypatch.setattr(cloud.os, "open", lambda *a, **kw: pytest.fail("restore wrote before checking a damaged local date"))
    with pytest.raises(ValueError):
        cloud.restore(target, bundle, "2026-09-03")
    after = {p.relative_to(target): p.read_bytes() for p in target.rglob("*") if p.is_file()}
    assert after == before


@pytest.mark.parametrize("version", ["0", "3", "5"])
def test_whole_restore_can_recover_older_final_stream_with_missing_base(
    frozen_bundle, tmp_path, monkeypatch, version
):
    workspace, bundle, target = fresh_target(frozen_bundle, tmp_path)
    name = f"outputs/microcap_top100_mom16_biweekly_live_v2_{version}_latest_signal.csv"
    path = target / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes((workspace / name).read_bytes().replace(b"2026-09-03", b"2026-09-02"))
    older = path.read_bytes()
    result = cloud.restore(target, bundle, "2026-09-03")
    assert result["ok"] and result["signal_ready"] is False
    assert path.read_bytes() == (workspace / name).read_bytes()
    assert (Path(result["backup"]) / name).read_bytes() == older
