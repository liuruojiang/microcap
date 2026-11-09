"""Real late Nov5 catalog evidence and rejection of every unreviewed change."""
import copy
import gzip
import hashlib
import json
from pathlib import Path

import pytest

from scripts import st_notice_boundary_review as review
from scripts import st_notice_refresh as refresh
from scripts.st_notice_evidence import StNoticeEvidenceError

FIXTURE = Path(__file__).parent / "fixtures/maintenance_boundary_20261109"
ADDED = {"AN202611051831449198", "AN202611051831449201", "AN202611051831449217"}
METADATA = {"AN202611051831458926", "AN202611051831459677", "AN202611051831461094",
            "AN202611051831461769", "AN202611051831461773"}
TARGET = "AN202611051831461769"


@pytest.fixture(params=["20261106", "20261109"])
def pair(request):
    with gzip.open(FIXTURE / ("native_pair_" + request.param + ".json.gz"), "rb") as stream:
        value = json.load(stream)
    return value["previous"], value["current"]


def test_actual_late_boundary_keeps_raw_observations_and_original_precision(pair, monkeypatch):
    before = copy.deepcopy(pair)
    result = refresh._boundary_precision_drift(*pair)
    assert result["policy"] == "exact_non_st_catalog_boundary_20261109_v1"
    assert set(result["reviewed_non_st_added_ids"]) == ADDED
    assert set(result["reviewed_metadata_changed_ids"]) == METADATA
    assert result["removed_event_ids"] == []
    assert result["risk_projection_unchanged"] is True
    assert pair == before
    assert refresh._boundary_summary_matches(result, refresh._boundary_precision_drift(*pair), *pair)
    monkeypatch.setattr(review, "_reviewed_catalog_boundary_20261109", lambda *args: None)
    with pytest.raises(StNoticeEvidenceError):
        refresh._boundary_precision_drift(*pair)


@pytest.mark.parametrize("field", ["title", "codes", "columns", "listing_state", "notice_date",
                                  "sort_date", "source_type", "eiTime", "language", "product_code"])
def test_new_or_changed_native_content_still_requires_review(pair, field):
    old, new = pair
    new[TARGET][field] = "unreviewed"
    with pytest.raises(StNoticeEvidenceError):
        refresh._boundary_precision_drift(old, new)


@pytest.mark.parametrize("change", ["new_addition", "remove_existing", "remove_reviewed_addition",
                                   "old_row", "old_display", "new_display_second", "new_display_day",
                                   "new_display_close", "added_issuer", "added_risk", "other_row"])
def test_no_generic_catalog_or_clock_waiver(pair, change):
    old, new = pair
    if change == "new_addition": new["unreviewed"] = copy.deepcopy(new[TARGET])
    elif change == "remove_existing": del new[TARGET]
    elif change == "remove_reviewed_addition": del new[sorted(ADDED)[0]]
    elif change == "old_row": old[TARGET]["listing_state"] = "unreviewed"
    elif change == "old_display": old[TARGET]["display_time"] = "2026-11-05 20:00:00"
    elif change == "new_display_second": new[TARGET]["display_time"] = "2026-11-05 20:00:00"
    elif change == "new_display_day": new[TARGET]["display_time"] = "2026-11-06 12:00:00"
    elif change == "new_display_close": new[TARGET]["display_time"] = "2026-11-05 14:59:59.999"
    elif change == "added_issuer": new[sorted(ADDED)[0]]["codes"][0]["stock_code"] = "000001"
    elif change == "added_risk": new[sorted(ADDED)[0]]["title"] += "实施退市风险警示"
    else:
        identity = next(k for k in new if k not in ADDED | METADATA)
        new[identity]["listing_state"] = "unreviewed"
    with pytest.raises(StNoticeEvidenceError):
        refresh._boundary_precision_drift(old, new)


@pytest.mark.parametrize("selector", ["title_action", "native_risk", "investor_equity"])
def test_native_consumer_regressions_cannot_use_the_exact_exception(pair, monkeypatch, selector):
    import microcap_top100_mom16_biweekly_live_v2_0 as base
    from scripts import st_history_scope, st_notice_evidence
    if selector == "title_action":
        monkeypatch.setattr(base.freq_mod, "classify_st_notice_risk_actions", lambda *args: {"other": True})
    elif selector == "native_risk":
        monkeypatch.setattr(st_history_scope, "_daily_notice_row", lambda *args: {"risk": True})
    else:
        monkeypatch.setattr(st_notice_evidence, "is_equity_issuer", lambda *args: True)
    with pytest.raises(StNoticeEvidenceError):
        refresh._boundary_precision_drift(*pair)


def test_eight_actual_issuer_documents_and_reviewed_hashes():
    approved = json.loads((FIXTURE / "document_review.json").read_bytes())
    assert set(approved) == ADDED | METADATA
    for identity, item in approved.items():
        receipt = json.loads((FIXTURE / (identity + ".receipt.json")).read_bytes())
        raw = (FIXTURE / (identity + item["suffix"])).read_bytes()
        text_raw = (FIXTURE / (identity + ".txt")).read_bytes()
        text = text_raw.decode("utf8")
        assert receipt["status"] == 200 and receipt["bytes"] == len(raw)
        assert hashlib.sha256(raw).hexdigest() == receipt["sha256"] == item["document_sha256"]
        assert hashlib.sha256(text_raw).hexdigest() == item["text_sha256"]
        assert receipt["url"] == receipt["actual_url"] == "https://pdf.dfcfw.com/pdf/H2_" + identity + "_1.pdf"
        assert all(term in text for term in item["required_text"])
        assert not any(term in text for term in ["退市风险警示", "其他风险警示", "实施风险警示", "撤销风险警示"])
