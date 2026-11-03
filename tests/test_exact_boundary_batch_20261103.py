import copy
import json
import shutil

import pytest

from scripts import st_notice_boundary_review as boundary
from scripts.st_notice_evidence import StNoticeEvidenceError
from scripts.st_notice_refresh import _boundary_precision_drift


@pytest.fixture
def reviewed_pair():
    record = json.loads((boundary.DIRECTORY / "reviewed_20261103/registry.json").read_bytes())
    return record["previous_rows"], record["current_rows"]


def test_real_reviewed_native_pair_preserves_strict_progress_scope(reviewed_pair):
    previous, current = reviewed_pair
    result = _boundary_precision_drift(previous, current)
    assert result["policy"] == "exact_reviewed_native_boundary_batch_20261103_v1"
    assert len(result["reviewed_non_st_added_ids"]) == 449
    assert len(result["reviewed_existing_st_progress_ids"]) == 3
    assert len(result["reviewed_metadata_changed_ids"]) == 10
    assert result["boundary_native_content_unchanged"] is False


@pytest.mark.parametrize("change", ["new_field", "title", "issuer", "metadata", "removed", "new_notice"])
def test_single_unreviewed_native_change_remains_rejected(reviewed_pair, change):
    previous, current = reviewed_pair
    row = current["AN202611021831316700"]
    if change == "new_field":
        row["not_reviewed"] = "new"
    elif change == "title":
        row["title"] += " changed"
    elif change == "issuer":
        row["codes"][0]["stock_code"] = "300245"
    elif change == "metadata":
        current["AN202611021831303677"]["listing_state"] = "8"
    elif change == "removed":
        del current[next(iter(previous))]
    elif change == "new_notice":
        current["AN_UNREVIEWED"] = copy.deepcopy(row)
    with pytest.raises(StNoticeEvidenceError):
        _boundary_precision_drift(previous, current)


def test_only_original_subsecond_precision_rule_is_reused(reviewed_pair):
    previous, current = reviewed_pair
    row = current["AN202611021831316700"]
    stamp = row["display_time"]
    row["display_time"] = stamp[:-1] + ("1" if stamp[-1] == "0" else "0")
    assert _boundary_precision_drift(previous, current)["policy"].startswith("exact_reviewed")
    row["display_time"] = "2026-11-02 14:59:59:000"
    with pytest.raises(StNoticeEvidenceError):
        _boundary_precision_drift(previous, current)


def test_modified_reviewed_original_document_is_rejected(reviewed_pair, tmp_path, monkeypatch):
    target = tmp_path / "reviews"
    shutil.copytree(boundary.DIRECTORY, target)
    monkeypatch.setattr(boundary, "DIRECTORY", target)
    document = target / "reviewed_20261103/AN202611021831316700.pdf"
    document.write_bytes(document.read_bytes() + b" changed")
    with pytest.raises(StNoticeEvidenceError, match="document identity changed"):
        _boundary_precision_drift(*reviewed_pair)
