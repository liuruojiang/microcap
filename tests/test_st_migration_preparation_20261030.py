"""Synthetic counterexamples for preparation only; no production data or network writes."""
import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from scripts import audit_v2_st_history_migration as audit
from scripts import audit_v2_3_v2_5_st_history_migration as downstream


def test_computational_success_is_not_user_approval():
    assert audit.preparation_decision(True)["ready_for_review"] is True
    assert audit.preparation_decision(True)["approved"] is False
    assert audit.preparation_decision(False)["ready_for_review"] is False
    assert downstream.preparation_decision(True)["approved"] is False


def test_candidate_uses_current_plain_execution_not_retired_overlays(monkeypatch):
    dates = pd.bdate_range("2024-01-02", periods=80)
    close = pd.DataFrame({"microcap_close": 100 * np.cumprod(1 + .02 * np.sin(np.arange(80))),
                          "hedge_close": np.linspace(100, 101, 80)}, index=dates)
    turnover = pd.DataFrame(columns=["rebalance_date", "two_side_cost_rate"])
    monkeypatch.setattr(audit.v2, "_load_embedded_base_context", lambda: ({}, close, turnover))
    candidate = audit.build_candidate()
    assert not candidate["target_vol_enabled"].any()
    assert not candidate["overheat_enabled"].any()
    assert set(candidate["current_execution_scale"]) <= {0., 1.}
    assert candidate["strategy_revision"].eq(audit.v2.STRATEGY_REVISION).all()
    baseline_gross = audit.v2.base_mod.run_signal(close.rename(columns={"microcap_close": "microcap", "hedge_close": "hedge"})).sort_index()
    baseline_gross = audit.v2.base_mod.apply_momentum_gap_exit_buffer(baseline_gross, audit.v2.V2_0_MOMENTUM_GAP_EXIT_BUFFER)
    expected = audit.v2.overlay_mod.apply_v2_0_execution(baseline_gross, turnover)
    pd.testing.assert_frame_equal(candidate, expected)


def test_report_keeps_initial_capital_loss_and_unavailable_windows():
    frame = pd.DataFrame({"return_net": [-.1, .01, 0.]}, index=pd.bdate_range("2024-01-02", periods=3))
    result = audit.performance_windows(frame)
    assert result["full"]["max_drawdown"] == pytest.approx(-.1)
    for window in ["last_10y", "last_5y", "last_3y", "last_1y"]:
        assert result[window]["annualized_return"] is None
        assert result[window]["max_drawdown"] is None
        assert result[window]["unavailable_reason"]


@pytest.mark.parametrize("bad", [np.nan, np.inf, "not-a-return"])
def test_missing_or_invalid_returns_are_not_silently_zero(bad):
    frame = pd.DataFrame({"return_net": [0., bad]}, index=pd.bdate_range("2024-01-02", periods=2))
    with pytest.raises((ValueError, TypeError)):
        audit.performance_windows(frame)


def test_member_audit_rejects_stale_registry_and_99_member_period(tmp_path, monkeypatch):
    symbols = [str(100000 + n) for n in range(99)]
    members = pd.DataFrame({"rebalance_date": "2026-10-28", "symbol": symbols, "name": "synthetic"})
    path = tmp_path / "members.csv"
    members.to_csv(path, index=False)
    meta = {"st_intervals": [], "st_notice_policy_version": audit.v2.freq_mod.ST_NOTICE_POLICY_VERSION,
            "st_notice_parser_revision": audit.v2.freq_mod.ST_NOTICE_PARSER_REVISION,
            "st_notice_evidence_sha256": "old", "notice_query_status": "ok", "name_history_status": "ok",
            "current_st_history_resolved": True}
    monkeypatch.setattr(audit, "evidence_fingerprint", lambda root: "new")
    monkeypatch.setattr(audit.v2.freq_mod, "resolve_security_meta_path", lambda symbol: tmp_path / (symbol + ".json"))
    monkeypatch.setattr(audit.v2.freq_mod, "_read_security_meta_payload", lambda path, symbol: meta)
    result = audit.member_st_audit(path)
    assert result["bad_policy_count"] == 99
    assert result["invalid_member_period_count"] == 1


def test_missing_member_date_cannot_pass_as_zero_st_overlap(tmp_path):
    path = tmp_path / "members.csv"
    pd.DataFrame({"rebalance_date": [None], "symbol": ["000001"], "name": ["synthetic"]}).to_csv(path, index=False)
    with pytest.raises(ValueError, match="dates"):
        audit.member_st_audit(path)


def test_empty_member_history_cannot_pass_migration(tmp_path):
    path = tmp_path / "empty.csv"
    pd.DataFrame(columns=["rebalance_date", "symbol", "name"]).to_csv(path, index=False)
    with pytest.raises(ValueError, match="Empty member history"):
        audit.member_st_audit(path)
