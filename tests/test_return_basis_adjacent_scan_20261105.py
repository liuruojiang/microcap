"""Small real transport fixture: preserve both byte passes, trim only one walk."""
import hashlib
import os
from pathlib import Path

import pytest

from scripts import realtime_state_bundle as state
from scripts import stock_return_pipeline as pipeline
from test_stock_return_basis_transport import prepare


@pytest.fixture
def case(tmp_path, monkeypatch):
    state._RETURN_BASIS_SUCCESS_MEMO.clear()
    state._RETURN_BASIS_SCAN_METRICS.clear()
    anchor, cache = prepare(tmp_path, monkeypatch)
    native = pipeline.verify_return_basis_transport
    calls = []

    def counted(*args):
        calls.append(args)
        return native(*args)

    monkeypatch.setattr(pipeline, "verify_return_basis_transport", counted)
    return tmp_path, cache, anchor, calls


def verify(case):
    return state._verify_return_basis_transport_memoized(case[0], case[2], ["000001"])


def native_path(path):
    return os.path.normcase(str(path.resolve()).removeprefix("\\\\?\\"))


def reference(root, anchor):
    source = (Path(__file__).parent / "fixtures/return_basis_scan_20261105/original_fingerprint.py.txt").read_text(encoding="utf8")
    assert hashlib.sha256(source.encode()).hexdigest() == "faa854973a1a2672b266e6cee93cea45f63b5ce8511786248873a78abb81dc2b"
    namespace = dict(vars(state))
    exec(compile(source, "frozen_5bd_fingerprint", "exec"), namespace)
    return namespace["_return_basis_input_fingerprint"](root, anchor, ("000001",))


def test_fingerprint_matches_frozen_original_algorithm(case):
    assert state._return_basis_input_fingerprint(case[0], case[2], ("000001",)) == reference(case[0], case[2])


def test_hit_preserves_two_complete_byte_reads_and_removes_only_one_walk(case, monkeypatch):
    verify(case)
    original_open = Path.open
    opens = []
    roots = [case[1] / name for name in ("prices_raw", "return_basis", "trade_status")]
    wanted = {native_path(p) for r in roots for p in r.rglob("*") if p.is_file()}

    def counted_open(path, *args, **kwargs):
        if args and args[0] == "rb" and native_path(path) in wanted:
            opens.append(native_path(path))
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", counted_open)
    before = verify(case)
    stats = state.return_basis_scan_metrics(case[0])
    assert before["read_only"] and len(case[3]) == 1
    assert wanted and all(opens.count(p) == 2 for p in wanted)
    assert len(opens) == 2 * len(wanted)
    assert stats["memo_outcome"] == "hit_verified_bytes"
    assert [x["inventory_scans"] for x in stats["fingerprint_passes"]] == [2, 1]
    assert [x["adjacent_inventory_reuses"] for x in stats["fingerprint_passes"]] == [0, 1]
    assert stats["fingerprint_passes"][0]["hashed_bytes"] == stats["fingerprint_passes"][1]["hashed_bytes"] > 0


def test_full_verifier_intervenes_so_cold_path_keeps_four_walks(case):
    verify(case)
    stats = state.return_basis_scan_metrics(case[0])
    assert stats["memo_outcome"] == "miss_empty"
    assert [x["inventory_scans"] for x in stats["fingerprint_passes"]] == [2, 2]
    assert not any(x["adjacent_inventory_reuses"] for x in stats["fingerprint_passes"])


def arm_between_scans(case, monkeypatch, mutation):
    """Stable function identity; mutate when second real fingerprint starts."""
    real_calendar = pipeline.sessions_for_day
    control = {"armed": False, "calls": 0}

    def calendar(day):
        if control["armed"]:
            control["calls"] += 1
            if control["calls"] == 2:
                mutation()
        return real_calendar(day)

    monkeypatch.setattr(pipeline, "sessions_for_day", calendar)
    verify(case)
    control["armed"] = True
    return control


@pytest.mark.parametrize("change", ["add", "delete", "lock", "directory"])
def test_tree_change_between_adjacent_scans_fails_closed(case, monkeypatch, change):
    path = case[1] / "return_basis/appeared.json"

    def mutate():
        if change == "add":
            path.write_text("{}")
        elif change == "delete":
            (case[1] / "return_basis/sources/000001/qfq.js").unlink()
        elif change == "lock":
            (case[1] / "return_basis/locks/new.lock").write_text("active")
        else:
            (case[1] / "return_basis/empty_new_directory").mkdir()

    arm_between_scans(case, monkeypatch, mutate)
    with pytest.raises((RuntimeError, FileNotFoundError)):
        verify(case)
    assert not state._RETURN_BASIS_SUCCESS_MEMO
    assert state.return_basis_scan_metrics(case[0])["memo_outcome"] == "failed"


def test_same_size_same_mtime_content_change_cannot_hit(case, monkeypatch):
    path = case[1] / "prices_raw/000001.csv"
    before = path.read_bytes()
    stamp = path.stat()

    def mutate():
        changed = before.replace(b"close_raw", b"wrong_raw", 1)
        assert changed != before and len(changed) == len(before)
        path.write_bytes(changed)
        os.utime(path, ns=(stamp.st_atime_ns, stamp.st_mtime_ns))

    arm_between_scans(case, monkeypatch, mutate)
    with pytest.raises((ValueError, KeyError)):
        verify(case)
    assert len(case[3]) == 2 and not state._RETURN_BASIS_SUCCESS_MEMO


def test_link_substitution_is_rejected_by_second_final_inventory(case, monkeypatch):
    target = case[1] / "return_basis"
    original = Path.is_symlink
    control = {"linked": False}
    # Inject the platform link flag without requiring Windows symlink privilege.
    monkeypatch.setattr(Path, "is_symlink", lambda p: control["linked"] and native_path(p) == native_path(target) or original(p))
    arm_between_scans(case, monkeypatch, lambda: control.update(linked=True))
    with pytest.raises(RuntimeError, match="changed during fingerprint"):
        verify(case)
    assert not state._RETURN_BASIS_SUCCESS_MEMO


def test_source_module_content_change_revalidates_instead_of_reusing(case, monkeypatch):
    module_path = case[0] / "pipeline_source.py"
    module_path.write_bytes(Path(pipeline.__file__).read_bytes())
    monkeypatch.setattr(pipeline, "__file__", str(module_path))
    arm_between_scans(case, monkeypatch, lambda: module_path.write_bytes(module_path.read_bytes() + b"\n# changed\n"))
    verify(case)
    assert len(case[3]) == 2
    assert state.return_basis_scan_metrics(case[0])["memo_outcome"] == "miss_changed_between_scans"


def test_calendar_change_between_passes_discards_handoff_and_rejects(case, monkeypatch):
    original = pipeline.sessions_for_day
    control = {"armed": False, "calls": 0}

    def calendar(day):
        if control["armed"]:
            control["calls"] += 1
        days = original(day)
        return days[:-1] if control["calls"] >= 2 else days

    monkeypatch.setattr(pipeline, "sessions_for_day", calendar)
    verify(case)
    control["armed"] = True
    with pytest.raises(ValueError):
        verify(case)
    assert not state._RETURN_BASIS_SUCCESS_MEMO
    passes = state.return_basis_scan_metrics(case[0])["fingerprint_passes"]
    assert passes[1]["inventory_scans"] == 2
    assert passes[1]["adjacent_inventory_reuses"] == 0


def test_replaced_fingerprint_uses_independent_original_api(case, monkeypatch):
    original = state._return_basis_input_fingerprint
    calls = []

    def wrapper(*args):
        calls.append(args)
        return original(*args)

    monkeypatch.setattr(state, "_return_basis_input_fingerprint", wrapper)
    verify(case)
    verify(case)
    assert len(calls) == 4 and all(len(args) == 3 for args in calls)
    assert len(case[3]) == 1
    assert not any(x["adjacent_inventory_reuses"] for x in state.return_basis_scan_metrics(case[0])["fingerprint_passes"])


def test_diagnostics_are_detached_and_never_authorize_reuse(case):
    verify(case)
    report = state.return_basis_scan_metrics(case[0])
    report["memo_outcome"] = "forged"
    report["fingerprint_passes"].clear()
    assert state.return_basis_scan_metrics(case[0])["memo_outcome"] != "forged"
    state._RETURN_BASIS_SUCCESS_MEMO.clear()
    verify(case)
    assert len(case[3]) == 2


def test_metrics_storage_has_bounded_roots(tmp_path, monkeypatch):
    monkeypatch.setattr(state, "_return_basis_input_fingerprint", lambda *args: (None, False))
    monkeypatch.setattr(pipeline, "verify_return_basis_transport", lambda *args: {"verified_count": 0})
    state._RETURN_BASIS_SCAN_METRICS.clear()
    for i in range(12):
        state._verify_return_basis_transport_memoized(tmp_path / str(i), "2026-10-23", [])
    assert len(state._RETURN_BASIS_SCAN_METRICS) == 8
