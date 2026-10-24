"""Synthetic elapsed-work clocks; real builders, guards and atomic filesystem writes."""
from datetime import date
from io import StringIO
from pathlib import Path

import pandas as pd
import pytest

import microcap_top100_mom16_biweekly_live_v2_0 as v20
import microcap_top100_mom16_biweekly_live_v2_3 as v23
import microcap_top100_mom16_biweekly_live_v2_5 as v25
from test_realtime_member_action_contract import _fake_realtime_base, _patch_native_realtime_builder
from test_realtime_publication_deadline import meta


def setup_builder(monkeypatch, tmp_path, version, start):
    from scripts import exchange_calendar
    monkeypatch.setattr(exchange_calendar, "sessions_for_day", lambda day: (
        date(2026, 9, 30), date(2026, 10, 8), date(2026, 10, 9)))
    builders = {
        "v2.0": v20._build_realtime_v2_0_outputs_unlocked,
        "v2.3": v23._build_realtime_v2_3_outputs_unlocked,
        "v2.5": v25._build_realtime_v2_5_outputs_unlocked,
    }
    builder = builders[version]
    ns = builder.__globals__
    writer = ns["_atomic_write_text"]
    payload = _fake_realtime_base(latest_anchor_trade_date="2026-09-30",
                                 quote_trade_date="2026-10-08", official_rebalance=False)
    payload.meta.update(meta(start))
    payload.context["latest_rebalance"] = pd.Timestamp("2026-09-17")
    _patch_native_realtime_builder(monkeypatch, version, payload)
    monkeypatch.setitem(ns, "_atomic_write_text", writer)
    monkeypatch.setitem(ns, "ensure_output_dir", lambda: None)
    dest = tmp_path / "signal.csv"
    dest.write_bytes(b"prior-publication\n")
    monkeypatch.setitem(ns, "REALTIME_SIGNAL_CSV", dest)
    clock = [pd.Timestamp(start)]
    guard_ns = v20.base_mod.assert_realtime_meta_is_actionable.__globals__
    real_clock = guard_ns["_cn_timestamp"]
    monkeypatch.setitem(guard_ns, "_cn_timestamp", lambda value=None: clock[0] if value is None else real_clock(value))
    monkeypatch.setitem(guard_ns, "_cn_local_day", lambda value=None: pd.Timestamp(clock[0].date()))
    return builder, payload, clock, dest


@pytest.mark.parametrize("version", ["v2.0", "v2.3", "v2.5"])
@pytest.mark.parametrize("stage", ["serialization", "temp_write", "replace_retry"])
@pytest.mark.parametrize("start,finish,blocked", [
    ("2026-10-08T14:59:59+08:00", "2026-10-08T15:00:00+08:00", True),
    ("2026-10-08T14:50:00+08:00", "2026-10-08T14:52:01+08:00", True),
    ("2026-10-08T14:50:00+08:00", "2026-10-08T14:50:01+08:00", False),
])
def test_real_commit_rechecks_elapsed_work(monkeypatch, tmp_path, version, stage, start, finish, blocked):
    builder, payload, clock, dest = setup_builder(monkeypatch, tmp_path, version, start)
    attempted = []
    real_replace = Path.replace

    def replace(path, target):
        if Path(target) == dest:
            attempted.append(clock[0])
            if stage == "replace_retry" and len(attempted) == 1:
                clock[0] = pd.Timestamp(finish)
                raise PermissionError("synthetic transient file lock")
        return real_replace(path, target)

    monkeypatch.setattr(Path, "replace", replace)
    if stage == "serialization":
        real_csv = pd.DataFrame.to_csv
        def to_csv(frame, *args, **kwargs):
            result = real_csv(frame, *args, **kwargs)
            clock[0] = pd.Timestamp(finish)
            return result
        monkeypatch.setattr(pd.DataFrame, "to_csv", to_csv)
    elif stage == "temp_write":
        real_write = Path.write_text
        def write(path, *args, **kwargs):
            result = real_write(path, *args, **kwargs)
            if path.name.startswith("signal.csv.tmp."):
                clock[0] = pd.Timestamp(finish)
            return result
        monkeypatch.setattr(Path, "write_text", write)
    if blocked:
        with pytest.raises(RuntimeError):
            builder()
        assert dest.read_bytes() == b"prior-publication\n"
        assert len(attempted) == (1 if stage == "replace_retry" else 0)
    else:
        signal, returned_meta, _ = builder()
        assert returned_meta is payload.meta
        assert pd.read_csv(dest).iloc[0]["signal_timing"] == "intraday_hypothetical_if_now_close"
        assert len(attempted) == (2 if stage == "replace_retry" else 1)
        pd.testing.assert_frame_equal(pd.read_csv(dest), pd.read_csv(StringIO(signal.to_csv(index=False))))
    assert not list(tmp_path.glob("signal.csv.tmp.*"))


@pytest.mark.parametrize("start,finish,blocked", [
    ("2026-10-08T14:59:59+08:00", "2026-10-08T15:00:00+08:00", True),
    ("2026-10-08T14:50:00+08:00", "2026-10-08T14:52:01+08:00", True),
    ("2026-10-08T14:50:00+08:00", "2026-10-08T14:50:01+08:00", False),
])
def test_v25_real_fingerprint_finishes_before_commit_guard(monkeypatch, tmp_path, start, finish, blocked):
    fingerprint = v25.current_base_fingerprint
    builder, _, clock, dest = setup_builder(monkeypatch, tmp_path, "v2.5", start)
    def elapsed_fingerprint():
        result = fingerprint()
        clock[0] = pd.Timestamp(finish)
        return result
    monkeypatch.setattr(v25, "current_base_fingerprint", elapsed_fingerprint)
    if blocked:
        with pytest.raises(RuntimeError):
            builder()
        assert dest.read_bytes() == b"prior-publication\n"
    else:
        builder()
        assert "signal_timing" in pd.read_csv(dest).columns
    assert not list(tmp_path.glob("signal.csv.tmp.*"))


@pytest.mark.parametrize("version", ["v2.0", "v2.3", "v2.5"])
def test_commit_does_not_accept_incomplete_proof(monkeypatch, tmp_path, version):
    builder, payload, _, dest = setup_builder(monkeypatch, tmp_path, version, "2026-10-08T14:50:00+08:00")
    del payload.meta["expected_latest_completed_trade_date_source"]
    with pytest.raises(RuntimeError):
        builder()
    assert dest.read_bytes() == b"prior-publication\n"
    assert not list(tmp_path.glob("signal.csv.tmp.*"))
