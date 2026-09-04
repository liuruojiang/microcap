"""Synthetic gate counterexamples: agreement alone is not state validity."""
from __future__ import annotations

import itertools

import pytest

from scripts.top100_delivery import validate_final_signal


ACTIVE = {"0": "long_microcap_short_zz1000", "3": "long_microcap_short_zz1000",
          "5": "long_microcap_top100"}


def pair(version, current, nxt):
    signal = {"current_holding": current, "next_holding": nxt,
              "current_execution_scale": "0" if current == "cash" else "1",
              "next_session_actionable_scale": "0" if nxt == "cash" else "1"}
    nav = {**signal, "holding": current}
    return signal, nav


@pytest.mark.parametrize("version", ACTIVE)
def test_all_four_legitimate_state_transitions_are_accepted(version):
    for current, nxt in itertools.product(["cash", ACTIVE[version]], repeat=2):
        validate_final_signal(*pair(version, current, nxt), version)


@pytest.mark.parametrize("version", ACTIVE)
@pytest.mark.parametrize("state_field,nav_field", [("current_holding", "holding"),
                                                  ("next_holding", "next_holding")])
@pytest.mark.parametrize("bad", ["invalid_position", "", "None", "long_wrong_version"])
def test_matching_invalid_holding_is_rejected(version, state_field, nav_field, bad):
    signal, nav = pair(version, ACTIVE[version], ACTIVE[version])
    signal[state_field] = nav[nav_field] = bad
    with pytest.raises(ValueError, match="mismatch"):
        validate_final_signal(signal, nav, version)


@pytest.mark.parametrize("version", ACTIVE)
@pytest.mark.parametrize("key", ["current_execution_scale", "next_session_actionable_scale"])
@pytest.mark.parametrize("bad", ["0", "0.5", "100", "inf", "nan", "-1"])
def test_matching_invalid_active_scale_is_rejected(version, key, bad):
    signal, nav = pair(version, ACTIVE[version], ACTIVE[version])
    signal[key] = nav[key] = bad
    with pytest.raises(ValueError, match="mismatch"):
        validate_final_signal(signal, nav, version)


@pytest.mark.parametrize("version", ACTIVE)
def test_sibling_holding_alias_is_not_allowed(version):
    other_active = ACTIVE["5"] if version in ("0", "3") else ACTIVE["0"]
    with pytest.raises(ValueError, match="mismatch"):
        validate_final_signal(*pair(version, other_active, other_active), version)


@pytest.mark.parametrize("version", ACTIVE)
def test_nav_scale_must_itself_match_fixed_one_state(version):
    signal, nav = pair(version, ACTIVE[version], ACTIVE[version])
    nav["current_execution_scale"] = "1.0000000000001"
    with pytest.raises(ValueError, match="mismatch"):
        validate_final_signal(signal, nav, version)


def test_unknown_version_is_rejected():
    with pytest.raises(ValueError, match="Unsupported"):
        validate_final_signal(*pair("0", "cash", "cash"), "9")
