import json
from pathlib import Path

import pandas as pd

import microcap_top100_mom16_biweekly_live_v2_5 as v
from scripts import top100_delivery as delivery


def test_promoted_v25_identity_is_unambiguous() -> None:
    assert v.LOOKBACK == 20
    assert v.HALFLIFE == 3.0
    assert v.ENTRY_THRESHOLD == 0.0
    assert v.EXIT_THRESHOLD == 0.0
    assert v.STRATEGY_REVISION == delivery.V25_STRATEGY_REVISION
    assert v.COSTED_NAV_CSV.name == "microcap_top100_mom16_lb20_hl3_entry0_exit0_no_targetvol_v2_5_costed_nav.csv"
    assert v.PREVIOUS_COSTED_NAV_CSV.name == "microcap_top100_mom16_lb17_hl3_entry46_exit25_no_targetvol_v2_5_costed_nav.csv"


def test_strategy_promotion_requires_exact_evidence(tmp_path, monkeypatch) -> None:
    report = tmp_path / "report.json"
    expected = {"schema_version": 1, "authorization": "user_replace_existing_v2_5"}
    monkeypatch.setattr(v, "strategy_promotion_evidence", lambda *args: expected)
    args = (Path("previous.csv"), pd.DataFrame(), Path("audit.csv"))

    assert not v.strategy_promotion_matches(None, *args)
    report.write_text(json.dumps({**expected, "approved": True}), encoding="utf-8")
    assert v.strategy_promotion_matches(report, *args)

    report.write_text(json.dumps({**expected, "approved": True, "authorization": "wrong"}), encoding="utf-8")
    assert not v.strategy_promotion_matches(report, *args)


def test_delivery_identity_rejects_retired_v25_line() -> None:
    row = {
        "strategy_revision": delivery.V25_STRATEGY_REVISION,
        "target_vol_enabled": "False",
        "cash_day_yield_enabled": "False",
        "financing_enabled": "False",
        "lookback": 20,
        "halflife": 3.0,
        "entry_threshold": 0.0,
        "exit_threshold": 0.0,
        "signal_spread_hedge_ratio": 0.0,
        "execution_hedge_ratio": 0.0,
    }
    assert delivery.plain_v25_identity(row)
    row["lookback"] = 17
    assert not delivery.plain_v25_identity(row)
