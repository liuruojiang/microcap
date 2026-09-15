"""Regression: a validated transport must not trigger an implicit realtime rebuild."""
import hashlib
import json
from types import SimpleNamespace

import pytest
import microcap_top100_mom16_biweekly_live_v2_0 as v2
from scripts import realtime_state_bundle as state


@pytest.mark.parametrize("failure", ["missing", "hedge", "metadata", "none"])
def test_real_base_guard_preserves_files_and_never_refreshes(tmp_path, monkeypatch, failure):
    ns = v2.realtime_core.load_realtime_base.__globals__
    paths = {key: tmp_path / key for key in ("proxy_meta", "proxy_members", "proxy_turnover")}
    resolved = SimpleNamespace(index_csv=tmp_path / "index", costed_nav_csv=tmp_path / "nav", output_paths=paths)
    for path in [resolved.index_csv, resolved.costed_nav_csv, *paths.values()]:
        path.write_bytes(b"preserve-me")
    if failure == "missing":
        paths["proxy_members"].unlink()
    before = {p.name: p.read_bytes() for p in tmp_path.iterdir()}
    monkeypatch.setitem(ns, "_sync_embedded_base_config", lambda: None)
    monkeypatch.setitem(ns, "_build_base_args", lambda: None)
    monkeypatch.setitem(ns, "_resolve_base_paths", lambda *a: resolved)
    monkeypatch.setitem(ns, "_base_costed_nav_matches_current_hedge_ratio", lambda *a: failure != "hedge")
    monkeypatch.setitem(ns, "_proxy_meta_matches_execution_model", lambda *a: failure != "metadata")
    monkeypatch.setitem(ns, "_seed_proxy_bundle", lambda *a: pytest.fail("seed write"))
    monkeypatch.setattr(ns["base_mod"], "build_refreshed_panel_shadow", lambda *a: pytest.fail("refresh write"))
    if failure == "none":
        ns["_ensure_base_outputs_unlocked"](state_only=True)
    else:
        with pytest.raises(RuntimeError, match="state-only"):
            ns["_ensure_base_outputs_unlocked"](state_only=True)
    assert before == {p.name: p.read_bytes() for p in tmp_path.iterdir()}


def test_real_realtime_call_passes_guard_before_data_access(monkeypatch):
    ns = v2.realtime_core.load_realtime_base.__globals__
    from contextlib import nullcontext
    monkeypatch.setenv("TOP100_REALTIME_REQUIRE_STATE", "1")
    monkeypatch.setitem(ns, "_v2_base_build_lock", nullcontext)
    def guard(*, state_only=False):
        assert state_only is True
        raise RuntimeError("checked-before-refresh")
    monkeypatch.setitem(ns, "_ensure_base_outputs_unlocked", guard)
    with pytest.raises(RuntimeError, match="checked-before-refresh"):
        ns["load_realtime_context"]()


def test_missing_reference_summary_never_rebuilds(monkeypatch):
    ns = v2.realtime_core.load_realtime_base.__globals__
    monkeypatch.setitem(ns, "_read_current_reference_summary", lambda *a: None)
    monkeypatch.setitem(ns, "_ensure_base_outputs_unlocked", lambda: pytest.fail("rebuild"))
    with pytest.raises(RuntimeError, match="reference summary"):
        ns["_load_reference_summary_unlocked"](state_only=True)


def test_metadata_transport_and_changed_bytes_are_checked(tmp_path):
    meta = tmp_path / ".microcap_index_cache/security_meta/000001.json"
    meta.parent.mkdir(parents=True)
    meta.write_bytes(b'{"st_intervals": []}\n')
    expected = {"present_count": 1, "missing_count": 0,
                "sha256": hashlib.sha256(b"000001\0" + meta.read_bytes() + b"\0").hexdigest()}
    proxy = tmp_path / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_meta.json"
    proxy.parent.mkdir()
    proxy.write_text(json.dumps({"core_params": {"security_meta_cache_fingerprint": expected}}))
    assert len(state.validate_security_metadata(tmp_path)) == 1
    assert meta.relative_to(tmp_path).as_posix() in state._iter_bundle_files(tmp_path)
    meta.write_bytes(b'{"st_intervals": ["changed"]}\n')
    with pytest.raises(ValueError, match="fingerprint mismatch"):
        state.validate_security_metadata(tmp_path)
    meta.unlink()
    with pytest.raises(ValueError, match="fingerprint mismatch"):
        state.validate_security_metadata(tmp_path)
