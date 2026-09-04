"""V2.3 final identity must reject contradictory active parameter fields."""
import csv
from pathlib import Path

import pytest

from scripts import top100_delivery as delivery


def _identity():
    return dict(strategy_revision=delivery.V23_STRATEGY_REVISION,
                target_vol_enabled="False", r2_gate_enabled="False", r2_entry_gate="0",
                overheat_trigger_threshold=".26", overheat_recovery_threshold=".20",
                signal_spread_hedge_ratio="1", momentum_gap_entry_threshold="0",
                momentum_gap_exit_buffer=".08", cash_day_yield_enabled="False", financing_enabled="False")


@pytest.mark.parametrize("field,bad", [
    ("r2_gate_enabled", "True"), ("signal_spread_hedge_ratio", ".8"),
    ("momentum_gap_entry_threshold", ".9"), ("momentum_gap_exit_buffer", ".09"),
    ("cash_day_yield_enabled", "True"), ("financing_enabled", "True"),
])
def test_missing_and_contradictory_v23_parameter_rejected(field, bad):
    row = _identity()
    assert delivery.plain_v23_identity(row)
    row[field] = bad
    assert not delivery.plain_v23_identity(row)
    row.pop(field)
    assert not delivery.plain_v23_identity(row)


def test_nav_entry_alias_supported_but_contradiction_rejected():
    row = _identity()
    row["entry_threshold"] = row.pop("momentum_gap_entry_threshold")
    assert delivery.plain_v23_identity(row)
    row["momentum_gap_entry_threshold"] = ".9"
    assert not delivery.plain_v23_identity(row)


def test_real_v23_written_nav_and_signal_identity_when_available():
    root = Path(__file__).resolve().parents[1]
    files = [root / "outputs" / f"microcap_top100_mom16_biweekly_live_v2_3_{suffix}.csv"
             for suffix in ("nav", "latest_signal")]
    if not all(path.exists() for path in files):
        pytest.skip("real local v2.3 artifacts unavailable; synthetic identity tests still run")
    for path in files:
        with path.open(encoding="utf-8-sig", newline="") as handle:
            rows = list(csv.DictReader(handle))
        assert rows
        assert all(delivery.plain_v23_identity(row) for row in rows), path
