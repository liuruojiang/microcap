"""Refresh must hand the publication consumer a current, usable base summary.

Small synthetic boundary inputs use the actual embedded summary builder; they
are unit fixtures, not strategy performance or current investment signals.
"""
import copy
import json
from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest

import microcap_top100_mom16_biweekly_live_v2_0 as v2
from scripts import realtime_state_bundle as state


TARGET = date(2026, 9, 17)
SUMMARY_REL = "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_summary.json"
TURNOVER_REL = "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_turnover.csv"


@pytest.fixture
def context():
    holding = "long_microcap_short_zz1000"
    return {
        "close_df": pd.DataFrame(index=pd.DatetimeIndex([TARGET])),
        "result": pd.DataFrame(
            {"holding": [holding], "next_holding": [holding]},
            index=pd.DatetimeIndex([TARGET]),
        ),
        "latest_signal": pd.DataFrame([{
            "signal_label": holding, "microcap_mom": 0.02,
            "hedge_mom": 0.01, "momentum_gap": 0.01,
            "microcap_close": 100.0, "hedge_close": 100.0,
        }]),
        "latest_rebalance": pd.Timestamp(TARGET),
        "prev_rebalance": pd.Timestamp("2026-09-03"),
        "next_rebalance": None,
        "target_members": pd.DataFrame({"symbol": [f"{n:06d}" for n in range(100)]}),
        "changes_df": pd.DataFrame({"action": ["enter", "exit"]}),
        "anchor_freshness": v2.base_mod.assess_history_anchor_freshness(
            latest_trade_date=pd.Timestamp(TARGET), max_stale_days=5,
            trading_dates=pd.DatetimeIndex([TARGET]),
        ),
    }


def build_summary(context):
    return v2.base_mod.build_summary(
        result=context["result"], latest_signal=context["latest_signal"],
        latest_rebalance=context["latest_rebalance"], prev_rebalance=context["prev_rebalance"],
        next_rebalance=context["next_rebalance"], members_df=context["target_members"],
        changes_df=context["changes_df"], capital=None,
        anchor_freshness=context["anchor_freshness"],
    )


def write_state(root, summary):
    path = root / SUMMARY_REL
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary), encoding="utf-8")
    (root / TURNOVER_REL).write_text("rebalance_date\n2026-09-03\n2026-09-17\n", encoding="utf-8")
    return path


def test_current_real_builder_summary_is_accepted(tmp_path, context):
    write_state(tmp_path, build_summary(context))
    assert state.validate_reference_summary(tmp_path, TARGET) == []


@pytest.mark.parametrize("failure", [
    "stale_trade_date", "same_day_wrong_rebalance", "wrong_version",
    "missing_holding", "missing_next_holding", "nan_momentum",
    "infinite_hedge", "missing_gap", "wrong_member_count", "missing_summary",
])
def test_invalid_summary_cannot_be_certified(tmp_path, context, failure):
    summary = build_summary(context)
    if failure == "stale_trade_date":
        summary["latest_trade_date"] = "2026-09-03"
    elif failure == "same_day_wrong_rebalance":
        summary["latest_rebalance_date"] = "2026-09-03"
    elif failure == "wrong_version":
        summary["summary_version_key"] = "hedge_1.0"
    elif failure == "missing_holding":
        summary["latest_signal"]["current_holding"] = ""
    elif failure == "missing_next_holding":
        summary["latest_signal"].pop("next_holding")
    elif failure == "nan_momentum":
        summary["latest_signal"]["microcap_mom"] = float("nan")
    elif failure == "infinite_hedge":
        summary["latest_signal"]["hedge_mom"] = float("inf")
    elif failure == "missing_gap":
        summary["latest_signal"].pop("momentum_gap")
    elif failure == "wrong_member_count":
        summary["target_members"]["count"] = 99
    path = write_state(tmp_path, summary)
    if failure == "missing_summary":
        path.unlink()
    assert state.validate_reference_summary(tmp_path, TARGET)


def test_writer_rebuilds_from_current_context_and_preserves_nav(tmp_path, context):
    old = copy.deepcopy(build_summary(context))
    old["latest_trade_date"] = old["latest_rebalance_date"] = "2026-09-03"
    old["latest_signal"]["current_holding"] = "cash"
    path = write_state(tmp_path, old)
    nav = tmp_path / "outputs/base_costed_nav.csv"
    nav.write_bytes(b"frozen-costed-nav-must-not-be-rebuilt\n")
    paths = {"summary": path, "nav": nav}
    state.persist_reference_summary(v2, SimpleNamespace(capital=None, max_stale_anchor_days=5), paths, context, TARGET)
    actual = json.loads(path.read_text(encoding="utf-8"))
    assert actual == build_summary(context)
    assert state.validate_reference_summary(tmp_path, TARGET) == []
    assert nav.read_bytes() == b"frozen-costed-nav-must-not-be-rebuilt\n"


def test_writer_rejects_context_older_than_requested_target(tmp_path, context):
    path = write_state(tmp_path, build_summary(context))
    before = path.read_bytes()
    context["result"].index = pd.DatetimeIndex(["2026-09-16"])
    with pytest.raises((ValueError, RuntimeError)):
        state.persist_reference_summary(
            v2, SimpleNamespace(capital=None, max_stale_anchor_days=5), {"summary": path}, context, TARGET,
        )
    assert path.read_bytes() == before
