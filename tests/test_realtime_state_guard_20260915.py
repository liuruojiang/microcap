"""Regression: a validated transport must not trigger an implicit realtime rebuild."""
import hashlib
import json
from types import SimpleNamespace

import pytest
import pandas as pd
import microcap_top100_mom16_biweekly_live_v2_0 as v2
from scripts import realtime_state_bundle as state


@pytest.mark.parametrize("change", ["new_st_label", "updated_st_label", "unknown_field",
                                   "changed_interval", "unresolved_history", "tampered_bytes"])
def test_cached_replay_snapshot_label_preserves_semantic_guards(tmp_path, monkeypatch, change):
    """A real approved 000972 baseline; derived labels are isolated guard inputs."""
    from contextlib import nullcontext
    import base64
    import copy
    from pathlib import Path
    from scripts import st_metadata_refresh as metadata

    fixture = Path(__file__).parent / "fixtures/st_snapshot_label_replay_20261103"
    container = json.loads((fixture / "000972.json").read_bytes())
    assert container["schema"] == "exact_native_metadata_bytes_v1"
    raw = base64.b64decode(container["raw_utf8_base64"], validate=True)
    provenance = json.loads((fixture / "provenance.json").read_bytes())
    assert hashlib.sha256(raw).hexdigest() == provenance["sha256"]
    old = json.loads(raw)
    assert old["symbol"] == "000972" and old["current_st_snapshot_name"] is None
    assert old["current_st_history_resolved"] is True
    if change == "updated_st_label":
        old["current_st_snapshot_name"] = "*ST中基"
    if change == "unresolved_history":
        old["current_st_history_resolved"] = False
    new = copy.deepcopy(old)
    new["current_st_snapshot_name"] = "ST中基" if change == "updated_st_label" else "*ST中基"
    if change == "unknown_field":
        new["board"] = "unapproved board change"
    if change == "changed_interval":
        new["st_intervals"][-1]["start"] = "2026-11-04"
    semantic_match = metadata._history_equal(old, new, "2026-11-04")
    assert semantic_match is (change != "changed_interval")
    cache = tmp_path / ".microcap_index_cache"
    (cache / "backup").mkdir(parents=True)
    (cache / "security_meta").mkdir()
    oldraw, newraw = json.dumps(old).encode(), json.dumps(new).encode()
    (cache / "backup/000972.json").write_bytes(oldraw)
    current = cache / "security_meta/000972.json"
    current.write_bytes(newraw + (b" " if change == "tampered_bytes" else b""))
    marker = dict(status="complete", target="2026-11-04", work=".microcap_index_cache",
                  old_hashes={"000972": metadata._sha(oldraw)},
                  new_hashes={"000972": metadata._sha(newraw)})
    (tmp_path / metadata.MARKER).write_text(json.dumps(marker))
    paths = {k: tmp_path / (k + ".csv") for k in
             ("proxy_meta", "proxy_members", "proxy_turnover", "proxy_effective_members")}
    for path in paths.values():
        path.write_text("date\n2026-11-03\n")
    paths["proxy_meta"].write_text(json.dumps({"return_basis_consumption": {
        "revision": "stock_return_consumption_v1", "full_rebuild_anchor": "2026-11-03"}}))
    panel = tmp_path / "panel.csv"
    panel.write_text("date\n2026-11-03\n2026-11-04\n")
    args = SimpleNamespace(index_csv=tmp_path / "index.csv", costed_nav_csv=tmp_path / "nav.csv",
                           allow_full_rebuild=False)
    args.index_csv.write_text("date\n2026-11-03\n")
    args.costed_nav_csv.write_text("date\n2026-11-03\n")

    class ReachedNativeBuilder(BaseException):
        pass

    def native_boundary(staged_args, dates):
        assert staged_args.allow_full_rebuild is False
        assert list(dates) == [pd.Timestamp("2026-11-03")]
        raise ReachedNativeBuilder()

    base = SimpleNamespace(proxy_meta_matches_execution_model=lambda meta: False,
                           build_biweekly_rebalance_dates=lambda dates: pd.DatetimeIndex([]),
                           build_local_proxy_bundle=native_boundary)
    monkeypatch.setattr(metadata, "frozen_notice_sources", lambda root: nullcontext())
    kwargs = dict(root=tmp_path, base=base, args=args, paths=paths, panel_path=panel, target="2026-11-04")
    if change in ("new_st_label", "updated_st_label"):
        with pytest.raises(ReachedNativeBuilder):
            metadata.try_cached_history_rebuild(**kwargs)
    elif change == "tampered_bytes":
        with pytest.raises(ValueError, match="Changed cached replay metadata"):
            metadata.try_cached_history_rebuild(**kwargs)
    else:
        assert metadata.try_cached_history_rebuild(**kwargs) is False
        assert not (cache / "scoped_proxy_rebuild").exists()
    assert args.allow_full_rebuild is False


@pytest.mark.parametrize("cached_replay", [False, True])
@pytest.mark.parametrize("explicit_full_rebuild", [False, True])
def test_metadata_refresh_never_grants_implicit_full_rebuild(
    tmp_path, monkeypatch, cached_replay, explicit_full_rebuild
):
    """Exercise the real refresh driver through its downstream strategy call."""
    import sys
    from scripts import st_metadata_refresh, st_notice_refresh

    for name in ("MICROCAP_ST_NOTICE_ARCHIVE", "MICROCAP_ST_NOTICE_ARCHIVE_SHA256"):
        monkeypatch.delenv(name, raising=False)
    pointer = tmp_path / ".microcap_index_cache/st_notice_archive_pointer.json"
    pointer.parent.mkdir()
    pointer.write_text("{}")
    calls = []

    class ReachedStrategyBoundary(BaseException):
        pass

    def replay(**kwargs):
        calls.append(("cached_replay", kwargs["args"].allow_full_rebuild))
        return cached_replay

    def strategy(args, *unused):
        calls.append(("strategy", args.allow_full_rebuild))
        assert args.allow_full_rebuild is explicit_full_rebuild
        raise ReachedStrategyBoundary()

    base = SimpleNamespace(
        DEFAULT_OUTPUT_PREFIX="isolated", freq_mod=object(),
        build_output_paths=lambda prefix: {},
        build_refreshed_panel_shadow=lambda *args: (tmp_path / "panel.csv", "2026-11-03"),
        read_csv_last_date=lambda path: "2026-11-02", ensure_strategy_files=strategy,
    )
    runtime = SimpleNamespace(
        _sync_embedded_base_config=lambda: None,
        _build_base_args=lambda **kwargs: SimpleNamespace(index_csv=tmp_path / "index.csv"),
        base_mod=base,
    )
    monkeypatch.setitem(sys.modules, "microcap_top100_mom16_biweekly_live_v2_0", runtime)
    monkeypatch.setitem(sys.modules, "run_top100_v1_6_v1_8_realtime_signals",
                        SimpleNamespace(ensure_static_realtime_inputs=lambda **kwargs: None))
    monkeypatch.setattr(state, "_reuse_same_day_notice_proof", lambda *args: None)
    monkeypatch.setattr(st_notice_refresh, "refresh_notice_archive", lambda **kwargs: {})
    monkeypatch.setattr(st_metadata_refresh, "refresh_metadata_after_notice",
                        lambda **kwargs: {"requires_full_rebuild": True})
    monkeypatch.setattr(st_metadata_refresh, "try_cached_history_rebuild", replay)
    with pytest.raises(ReachedStrategyBoundary):
        state.refresh_state(tmp_path, allow_full_rebuild=explicit_full_rebuild)
    assert calls == [("cached_replay", explicit_full_rebuild),
                     ("strategy", explicit_full_rebuild)]


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


def test_close_confirmed_generation_uses_validated_state_without_refresh(monkeypatch):
    ns = v2.embedded_context._load_embedded_base_context.__globals__
    from contextlib import nullcontext
    monkeypatch.setenv("TOP100_REALTIME_REQUIRE_STATE", "1")
    monkeypatch.setitem(ns, "_v2_base_build_lock", nullcontext)
    monkeypatch.setitem(ns, "_ensure_base_outputs_unlocked", lambda *, state_only: (
        None if state_only else pytest.fail("close-confirmed delivery rebuilt base state")
    ))
    resolved = SimpleNamespace(
        index_csv=pytest.fail,
        costed_nav_csv=pytest.fail,
        output_paths={"panel_shadow": pytest.fail},
    )
    # The guard is asserted before any live history refresh; no data fixture is
    # needed to prove that the unsafe branch is unavailable.
    monkeypatch.setitem(ns, "_build_base_args", lambda: None)
    monkeypatch.setitem(ns, "_resolve_base_paths", lambda *_: resolved)
    monkeypatch.setattr(ns["base_mod"], "read_csv_last_date", lambda *_: None)
    with pytest.raises(RuntimeError, match="state-only panel"):
        ns["_load_embedded_base_context"]()


def test_missing_reference_summary_never_rebuilds(tmp_path, monkeypatch):
    ns = v2.realtime_core.load_realtime_base.__globals__
    monkeypatch.setitem(ns, "_read_current_reference_summary", lambda *a: None)
    monkeypatch.setitem(ns, "_resolved_base_summary_json", lambda: tmp_path / "missing.json")
    monkeypatch.setitem(ns, "_ensure_base_outputs_unlocked", lambda: pytest.fail("rebuild"))
    with pytest.raises(RuntimeError, match="reference summary"):
        ns["_load_reference_summary_unlocked"](state_only=True)


def test_certified_dated_summary_is_accepted_after_delivery_only_code_change(tmp_path, monkeypatch):
    ns = v2.realtime_core.load_realtime_base.__globals__
    summary = tmp_path / "summary.json"
    summary.write_text(json.dumps({"latest_trade_date": "2026-09-16", "summary_version_key": "old"}))
    monkeypatch.setitem(ns, "_read_current_reference_summary", lambda *a: None)
    monkeypatch.setitem(ns, "_resolved_base_summary_json", lambda: summary)
    assert ns["_load_reference_summary_unlocked"](pd.Timestamp("2026-09-16"), state_only=True)["summary_version_key"] == "old"


def test_state_only_summary_can_carry_exactly_one_no_rebalance_session(tmp_path, monkeypatch):
    ns = v2.realtime_core.load_realtime_base.__globals__
    summary = tmp_path / "summary.json"
    summary.write_text(json.dumps({"latest_trade_date": "2026-09-15", "summary_version_key": "old", "latest_signal": {"current_holding": "microcap"}}))
    monkeypatch.setitem(ns, "_read_current_reference_summary", lambda *a: None)
    monkeypatch.setitem(ns, "_resolved_base_summary_json", lambda: summary)
    carried = ns["_load_reference_summary_unlocked"](
        pd.Timestamp("2026-09-16"), state_only=True,
        available_dates=pd.DatetimeIndex(["2026-09-15", "2026-09-16"]),
        turnover_df=pd.DataFrame({"rebalance_date": ["2026-09-03"]}),
    )
    assert carried["latest_signal"] == {}
    assert carried["state_only_summary_carry_forward"]["source_latest_trade_date"] == "2026-09-15"


def test_state_only_summary_can_carry_long_fresh_tail_without_rebalance(tmp_path, monkeypatch):
    ns = v2.realtime_core.load_realtime_base.__globals__
    summary = tmp_path / "summary.json"
    summary.write_text(json.dumps({"latest_trade_date": "2026-09-03", "summary_version_key": "old"}))
    monkeypatch.setitem(ns, "_read_current_reference_summary", lambda *a: None)
    monkeypatch.setitem(ns, "_resolved_base_summary_json", lambda: summary)
    carried = ns["_load_reference_summary_unlocked"](
        pd.Timestamp("2026-09-16"), state_only=True,
        available_dates=pd.DatetimeIndex(["2026-09-03", "2026-09-15", "2026-09-16"]),
        turnover_df=pd.DataFrame({"rebalance_date": ["2026-09-03"]}),
    )
    assert carried["state_only_summary_carry_forward"]["target_latest_trade_date"] == "2026-09-16"


def test_state_only_summary_rejects_rebalance_tail(tmp_path, monkeypatch):
    ns = v2.realtime_core.load_realtime_base.__globals__
    summary = tmp_path / "summary.json"
    summary.write_text(json.dumps({"latest_trade_date": "2026-09-15", "summary_version_key": "old"}))
    monkeypatch.setitem(ns, "_read_current_reference_summary", lambda *a: None)
    monkeypatch.setitem(ns, "_resolved_base_summary_json", lambda: summary)
    with pytest.raises(RuntimeError, match="reference summary"):
        ns["_load_reference_summary_unlocked"](
            pd.Timestamp("2026-09-16"), state_only=True,
            available_dates=pd.DatetimeIndex(["2026-09-15", "2026-09-16"]),
            turnover_df=pd.DataFrame({"rebalance_date": ["2026-09-16"]}),
        )


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
