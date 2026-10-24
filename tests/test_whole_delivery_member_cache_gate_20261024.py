"""Synthetic 100-position state; real member-cache validators and whole gates."""
import json
import pytest

from test_top100_delivery import delivery, workspace, write


def base_structural_report(monkeypatch):
    # Isolate whole-delivery enforcement from the separate base-state fixture.
    # Neither member transport nor inspect_outputs is mocked.
    report = {"ok": True, "scope": "base_state_only", "errors": [],
              "warnings": ["base cache coverage warning"], "anchor_dates": {
                  name: "2026-09-03" for name in delivery.DAILY_STATE_ANCHORS}}
    monkeypatch.setattr(delivery.state, "validate_state", lambda *a, **kw: {
        **report, "errors": list(report["errors"])})
    return report


def break_leg(root, defect):
    symbol = "000017"
    if defect == "missing_raw":
        (root / delivery.state.PRICE_CACHE_DIR / f"{symbol}.csv").unlink()
    elif defect == "stale_raw":
        write(root, f"{delivery.state.PRICE_CACHE_DIR}/{symbol}.csv",
              "date,close_raw\n2026-09-02,10.0\n")
    elif defect == "missing_share":
        (root / delivery.state.SHARE_CACHE_DIR / f"{symbol}.csv").unlink()
    elif defect == "invalid_optional_qfq":
        write(root, f"{delivery.state.ADJUSTED_PRICE_CACHE_DIR}/{symbol}.csv", "date,wrong\n2026-09-03,10\n")


@pytest.mark.parametrize("defect", ["missing_raw", "stale_raw", "missing_share", "invalid_optional_qfq"])
@pytest.mark.parametrize("gate", ["inspect", "aligned_base"])
def test_whole_gate_rejects_incomplete_actual_member_leg(workspace, monkeypatch, defect, gate):
    base = base_structural_report(monkeypatch)
    break_leg(workspace, defect)
    report = (delivery.inspect_outputs(workspace, "2026-09-03") if gate == "inspect"
              else delivery.validate_base_state_for_session(workspace, "2026-09-03"))
    assert not report["ok"]
    assert any("incomplete member cache for transport" in e and "000017" in e for e in report["errors"])
    assert base["ok"] and base["scope"] == "base_state_only" and base["errors"] == []


@pytest.mark.parametrize("gate", ["inspect", "aligned_base"])
def test_complete_raw_and_share_allow_absent_optional_adjusted_cache(workspace, monkeypatch, gate):
    base_structural_report(monkeypatch)
    assert not (workspace / delivery.state.ADJUSTED_PRICE_CACHE_DIR).exists()
    report = (delivery.inspect_outputs(workspace, "2026-09-03") if gate == "inspect"
              else delivery.validate_base_state_for_session(workspace, "2026-09-03"))
    assert report["ok"], report["errors"]


def test_group_refresh_stops_before_version_generation_on_incomplete_raw(workspace, monkeypatch):
    base_structural_report(monkeypatch)
    break_leg(workspace, "stale_raw")
    monkeypatch.setattr(delivery, "verify_release", lambda root: "synthetic-release")
    monkeypatch.setattr(delivery, "independent_target", lambda root: "2026-09-03")
    monkeypatch.setattr(delivery.state, "refresh_state", lambda *a, **kw: {"ok": True})
    attempted = []
    monkeypatch.setattr(delivery.subprocess, "run", lambda *a, **kw: attempted.append(a))
    with pytest.raises(RuntimeError, match="incomplete member cache for transport"):
        delivery._refresh_all_unlocked(workspace)
    assert attempted == []


def test_target_only_cache_cannot_replace_actual_position_cache(workspace, monkeypatch):
    base_structural_report(monkeypatch)
    effective = workspace / delivery.state.PROXY_EFFECTIVE_MEMBERS_REL
    # The newly selected target 000017 is present, but a blocked-exit actual
    # position 999999 is still held and must also have transported price legs.
    effective.write_text(effective.read_text(encoding="utf-8").replace(",000017", ",999999"), encoding="utf-8")
    report = delivery.inspect_outputs(workspace, "2026-09-03")
    assert not report["ok"]
    assert any("999999" in e and "incomplete member cache" in e for e in report["errors"])


def test_check_cli_rejects_stale_member_even_with_prior_complete_manifest(workspace, monkeypatch, capsys):
    base_structural_report(monkeypatch)
    complete = delivery.inspect_outputs(workspace, "2026-09-03")
    assert complete["ok"]
    delivery.write_manifest(workspace, {**complete, "status": "complete"})
    break_leg(workspace, "stale_raw")
    monkeypatch.setattr(delivery, "__file__", str(workspace / "scripts/top100_delivery.py"))
    monkeypatch.setattr(delivery, "verify_release", lambda root: "synthetic-release")
    monkeypatch.setattr(delivery, "independent_target", lambda root: "2026-09-03")
    assert delivery.main(["check"]) == 1
    report = json.loads(capsys.readouterr().out)
    assert not report["ok"]
    assert any("raw-price cache ends at 2026-09-02" in e for e in report["errors"])
