"""Verify the reviewed source-only transition against the actual archive identity."""
import copy
import json
from pathlib import Path

import pytest

from scripts import realtime_state_bundle as state
from scripts.stock_input_contract import source_fingerprint

ROOT = Path(__file__).resolve().parents[1]
SHARE_READER = "scripts/share_change_source.py"


@pytest.fixture
def identities():
    fixture = ROOT / "tests/fixtures/nov12_whole_archive_stock_fingerprint.json"
    old = json.loads(fixture.read_bytes())
    current = dict(old, code=source_fingerprint(ROOT))
    return old, current


def test_exact_nov12_whole_archive_uses_the_current_share_reader(identities):
    old, current = identities
    original = copy.deepcopy((old, current))
    assert {name for name, digest in old["code"].items() if current["code"][name] != digest} == {SHARE_READER}
    assert state.stock_input_evidence_matches_validated_source(old, current)
    assert (old, current) == original
    assert not state.stock_input_evidence_matches_validated_source(current, old)


@pytest.mark.parametrize("fault", ["changed_data", "other_archive", "unknown_reader", "other_code", "missing_code"])
def test_exact_nov12_review_preserves_all_other_source_gates(identities, fault):
    old, current = identities
    if fault == "changed_data":
        current["sha256"] = "a" * 64
    elif fault == "other_archive":
        old["sha256"] = current["sha256"] = "a" * 64
    elif fault == "unknown_reader":
        current["code"][SHARE_READER] = "a" * 64
    elif fault == "other_code":
        current["code"]["scripts/stock_return_pipeline.py"] = "a" * 64
    else:
        current["code"].pop(SHARE_READER)
    assert not state.stock_input_evidence_matches_validated_source(old, current)
