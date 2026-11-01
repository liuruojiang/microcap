"""Transport retains authenticated share sources, including referenced history."""
import pytest
from test_share_change_source import install_fixture
from scripts import realtime_state_bundle as bundle


def test_share_source_is_in_transport(tmp_path, monkeypatch):
    cache, state, _ = install_fixture(tmp_path)
    monkeypatch.setattr(bundle, '_iter_current_member_cache_files', lambda _: [cache.relative_to(tmp_path).as_posix()])
    files = bundle._iter_bundle_files(tmp_path)
    assert state.relative_to(tmp_path).as_posix() in files
    assert '.microcap_index_cache/share_sources/source.json' in files
    assert cache.relative_to(tmp_path).as_posix() in files


def test_share_transport_rejects_missing_source(tmp_path, monkeypatch):
    install_fixture(tmp_path)
    (tmp_path / '.microcap_index_cache/share_sources/source.json').unlink()
    monkeypatch.setattr(bundle, '_iter_current_member_cache_files', lambda _: [])
    with pytest.raises((ValueError, FileNotFoundError)):
        bundle._iter_bundle_files(tmp_path)
