"""Diagnostic counterexamples for published actions and ranked member lineage."""
from __future__ import annotations

import itertools

import pytest

from scripts import top100_delivery as delivery
from test_top100_delivery import change_csv_cell, workspace  # noqa: F401
from test_adversarial_delivery_state_consistency import ACTIVE, pair


@pytest.mark.parametrize("version", ACTIVE)
@pytest.mark.parametrize("field", ["signal_label", "trade_state", "effective_trade_state",
                                   "holding_trade_state", "momentum_trade_state", "scale_trade_state"])
def test_final_action_cannot_contradict_any_valid_holding_transition(version, field):
    for current, nxt in itertools.product(["cash", ACTIVE[version]], repeat=2):
        signal, nav = pair(version, current, nxt)
        delivery.validate_final_signal(signal, nav, version)
        signal[field] = "contradictory_action"
        with pytest.raises(ValueError, match="mismatch"):
            delivery.validate_final_signal(signal, nav, version)


@pytest.mark.parametrize("version", ACTIVE)
@pytest.mark.parametrize("field", ["signal_label", "trade_state"])
def test_final_action_fields_are_required(version, field):
    signal, nav = pair(version, "cash", "cash")
    signal.pop(field)
    with pytest.raises(ValueError, match="mismatch"):
        delivery.validate_final_signal(signal, nav, version)


@pytest.mark.parametrize("day", ["2026-08-20", "2026-09-03"])
@pytest.mark.parametrize("bad_rank", ["1", "0", "101", "1.5", "nan", ""])
def test_latest_two_member_snapshots_require_complete_unique_ranks(workspace, day, bad_rank):
    path = workspace / "outputs" / delivery.BASE_FILES["proxy_members"]
    payload = path.read_text(encoding="utf-8")
    old = f"{day},100,000100"
    assert old in payload
    path.write_text(payload.replace(old, f"{day},{bad_rank},000100"), encoding="utf-8")
    with pytest.raises(ValueError, match="rank"):
        delivery.canonical_member_rebalance(workspace, "2026-09-03")


def test_full_delivery_rejects_trade_state_conflict(workspace):
    path = workspace / "outputs" / "microcap_top100_mom16_biweekly_live_v2_3_latest_signal.csv"
    change_csv_cell(path, "trade_state", "close")
    report = delivery.inspect_outputs(workspace, "2026-09-03")
    assert not report["ok"]
    assert any("trade_state" in error for error in report["errors"])
