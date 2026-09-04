import json
import subprocess
from pathlib import Path

import pytest

from scripts import top100_delivery as delivery


def write(root, name, content):
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


@pytest.fixture
def workspace(tmp_path):
    daily = "date,return_net\n2026-09-02,0.01\n2026-09-03,0.02\n"
    for name in (*delivery.BASE_FILES.values(), delivery.BASE_PANEL):
        write(tmp_path, f"outputs/{name}", daily)
    write(tmp_path, f"outputs/{delivery.BASE_FILES['proxy_turnover']}",
          "rebalance_date\n2026-09-03\n")
    write(tmp_path, delivery.AUTHORITY, "{}")
    for version, costed in delivery.COSTED.items():
        prefix = f"microcap_top100_mom16_biweekly_live_v2_{version}"
        write(tmp_path, f"{prefix}.py", "# source\n")
        summary = {"version": f"2.{version}", "historical_rewrite_audit": {"status": "clean"},
                   "latest_nav_date": "2026-09-03", "latest_trade_date": "2026-09-03",
                   "data_freshness_proof": {"expected_latest_date": "2026-09-03",
                                            "expected_latest_rebalance_date": "2026-09-03"}}
        write(tmp_path, f"outputs/{prefix}_summary.json", json.dumps(summary))
        for name in (costed, f"{prefix}_nav.csv", f"{prefix}_performance_nav.csv"):
            write(tmp_path, f"outputs/{name}", daily)
        write(tmp_path, f"outputs/{prefix}_latest_signal.csv",
              f"date,version,member_rebalance_actionable\n2026-09-03,2.{version},False\n")
        for suffix in ("performance_summary.json", "performance_summary.csv", "performance_yearly.csv"):
            write(tmp_path, f"outputs/{prefix}_{suffix}", "{}")
    return tmp_path


def certify(root):
    report = delivery.inspect_outputs(root, "2026-09-03")
    assert report["ok"], report["errors"]
    delivery.write_manifest(root, {**report, "status": "complete"})
    return report


def test_whole_delivery_requires_completed_manifest(workspace):
    assert not delivery.validate_manifest(workspace, delivery.inspect_outputs(workspace, "2026-09-03"))["ok"]
    certify(workspace)
    assert delivery.validate_manifest(workspace, delivery.inspect_outputs(workspace, "2026-09-03"))["ok"]


@pytest.mark.parametrize("version", list(delivery.COSTED))
@pytest.mark.parametrize("suffix", ["costed", "nav.csv", "performance_nav.csv", "latest_signal.csv", "summary.json"])
def test_any_stale_final_blocks_the_group(workspace, version, suffix):
    certify(workspace)
    name = delivery.COSTED[version] if suffix == "costed" else f"microcap_top100_mom16_biweekly_live_v2_{version}_{suffix}"
    path = workspace / "outputs" / name
    path.write_text(path.read_text().replace("2026-09-03", "2026-09-01"))
    assert not delivery.validate_manifest(workspace, delivery.inspect_outputs(workspace, "2026-09-03"))["ok"]


@pytest.mark.parametrize("kind", ["source", "base", "authority", "performance", "signal"])
def test_same_date_content_changes_invalidate_manifest(workspace, kind):
    certify(workspace)
    names = {"source": "microcap_top100_mom16_biweekly_live_v2_0.py",
             "base": "outputs/" + delivery.BASE_PANEL, "authority": delivery.AUTHORITY,
             "performance": "outputs/microcap_top100_mom16_biweekly_live_v2_3_performance_summary.json",
             "signal": "outputs/microcap_top100_mom16_biweekly_live_v2_5_latest_signal.csv"}
    path = workspace / names[kind]
    path.write_text(path.read_text() + "\n")
    assert not delivery.validate_manifest(workspace, delivery.inspect_outputs(workspace, "2026-09-03"))["ok"]


@pytest.mark.parametrize("version", list(delivery.COSTED))
def test_wrong_identity_or_unclean_audit_rejected(workspace, version):
    path = workspace / "outputs" / f"microcap_top100_mom16_biweekly_live_v2_{version}_summary.json"
    value = json.loads(path.read_text())
    value["version"] = "1.0"
    value["historical_rewrite_audit"]["status"] = "audited_exact_hash_lineage_migration"
    path.write_text(json.dumps(value))
    assert not delivery.inspect_outputs(workspace, "2026-09-03")["ok"]


def test_base_success_cannot_mask_failed_child(workspace, monkeypatch):
    certify(workspace)
    monkeypatch.setattr(delivery, "verify_release", lambda root: "release")
    monkeypatch.setattr(delivery.state, "refresh_state", lambda *a, **kw: {"ok": True})
    monkeypatch.setattr(delivery, "independent_target", lambda root: "2026-09-03")
    def fail(*args, **kwargs):
        raise subprocess.CalledProcessError(1, "v2.3")
    monkeypatch.setattr(delivery.subprocess, "run", fail)
    with pytest.raises(subprocess.CalledProcessError):
        delivery.refresh_all(workspace)
    assert not delivery.validate_manifest(workspace, delivery.inspect_outputs(workspace, "2026-09-03"))["ok"]


def test_shared_input_change_during_group_rejected(workspace, monkeypatch):
    monkeypatch.setattr(delivery, "verify_release", lambda root: "release")
    monkeypatch.setattr(delivery.state, "refresh_state", lambda *a, **kw: {"ok": True})
    monkeypatch.setattr(delivery, "independent_target", lambda root: "2026-09-03")
    def mutate(*args, **kwargs):
        path = workspace / "outputs" / delivery.BASE_PANEL
        path.write_text(path.read_text() + "\n")
    monkeypatch.setattr(delivery.subprocess, "run", mutate)
    with pytest.raises(RuntimeError, match="Shared inputs changed"):
        delivery.refresh_all(workspace)


@pytest.mark.parametrize("content", ["", "date\nnot-a-date\n", "date\n2026-09-03\n2026-09-03\n", "date\n2026-09-03\n2026-09-02\n"])
def test_bad_date_streams_fail_closed(tmp_path, content):
    path = tmp_path / "stream.csv"
    path.write_text(content)
    with pytest.raises(ValueError):
        delivery.csv_info(path)


def test_newline_transport_does_not_invalidate_content(workspace):
    # Transport normalization invariant: preserve all bytes except CRLF versus LF.
    certify(workspace)
    for name in delivery.input_hashes(workspace):
        path = workspace / name
        path.write_bytes(path.read_bytes().replace(b"\r\n", b"\n").replace(b"\n", b"\r\n"))
    assert delivery.validate_manifest(workspace, delivery.inspect_outputs(workspace, "2026-09-03"))["ok"]


def test_new_completed_session_invalidates_old_delivery(workspace):
    certify(workspace)
    assert not delivery.validate_manifest(workspace, delivery.inspect_outputs(workspace, "2026-09-04"))["ok"]


def test_parallel_group_refresh_cannot_overwrite_success(workspace):
    certify(workspace)
    write(workspace, delivery.LOCK, "other-process")
    with pytest.raises(RuntimeError, match="Another group refresh"):
        delivery.refresh_all(workspace)
    assert (workspace / delivery.LOCK).read_text() == "other-process"
    assert not delivery.validate_manifest(workspace, delivery.inspect_outputs(workspace, "2026-09-03"))["ok"]


@pytest.mark.parametrize("clock,expected", [
    ("2026-09-04T14:30:00+08:00", "2026-09-03"),
    ("2026-09-04T15:29:59+08:00", "2026-09-03"),
    ("2026-09-04T15:30:00+08:00", "2026-09-04"),
    ("2026-10-08T14:30:00+08:00", "2026-09-30"),
    ("2026-10-08T15:30:00+08:00", "2026-10-08"),
])
def test_independent_target_uses_exchange_sessions_not_market_prices(tmp_path, monkeypatch, clock, expected):
    from datetime import date, datetime
    from scripts import exchange_calendar
    import microcap_top100_mom16_biweekly_live_v2_0 as v2
    current = datetime.fromisoformat(clock)
    class FrozenClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return current.astimezone(tz) if tz is not None else current.replace(tzinfo=None)
    monkeypatch.setattr(exchange_calendar, "datetime", FrozenClock)
    # The provider itself remains real; only its network-backed calendar data is isolated.
    monkeypatch.setattr(exchange_calendar, "sessions_for_day", lambda day: tuple(map(date.fromisoformat,
        ["2026-09-03", "2026-09-04", "2026-09-07", "2026-09-30", "2026-10-08", "2026-10-09"])))
    monkeypatch.setattr(v2.base_mod, "fetch_eastmoney_index_history",
        lambda *a, **kw: pytest.fail("date-only check must not fetch a price history"))
    assert delivery.independent_target(tmp_path) == expected


def test_independent_target_calendar_unknown_fails_closed(tmp_path, monkeypatch):
    from scripts import exchange_calendar
    import microcap_top100_mom16_biweekly_live_v2_0 as v2
    def unknown(day):
        raise RuntimeError("Independent exchange calendar unavailable: year not covered")
    monkeypatch.setattr(exchange_calendar, "sessions_for_day", unknown)
    monkeypatch.setattr(v2.base_mod, "fetch_eastmoney_index_history",
        lambda *a, **kw: pytest.fail("unavailable calendar must not fall back to NAV/price history"))
    with pytest.raises(RuntimeError, match="calendar unavailable"):
        delivery.independent_target(tmp_path)


def test_calendar_target_does_not_authorize_stale_real_streams(workspace, monkeypatch):
    from datetime import date
    from scripts import exchange_calendar
    monkeypatch.setattr(exchange_calendar, "latest_completed_session", lambda: date(2026, 9, 4))
    certify(workspace)  # All real fixture streams end on September 3.
    expected = delivery.independent_target(workspace)
    report = delivery.validate_manifest(workspace, delivery.inspect_outputs(workspace, expected))
    assert not report["ok"]
    assert any("date mismatch" in error for error in report["errors"])
