"""Frozen actual source pages; mutations are software counterexamples only."""
import copy
from datetime import timedelta
import json
from pathlib import Path
import hashlib
import re

import pytest

from scripts import st_notice_boundary_review as review
from scripts import st_notice_refresh as refresh
from scripts.st_notice_evidence import StNoticeEvidenceError
from scripts.st_notice_incremental import verify_day

FIXTURES = Path(__file__).parent / "fixtures"
ID = "AN202611041831408711"
IDS = (ID, "AN202611041831408713")
DOCS = FIXTURES / "st_notice_columns_20261105"


@pytest.fixture
def columns_review_pair():
    previous = json.loads((FIXTURES / "st_notice_removed_inv_20261104.json").read_bytes())["previous"]
    current = refresh._native(FIXTURES / "st_notice_boundary_20261105", "2026-11-04")
    return previous, current


def test_actual_source_pages_and_original_failure_reproduce(columns_review_pair, monkeypatch):
    old, new = columns_review_pair
    source = verify_day(FIXTURES / "st_notice_boundary_20261105", "2026-11-04")
    assert source["total_hits"] == 1040 and source["pages"] == 11
    assert review.sha(review.canonical(new)) == "01af438221d6323995d23fc7b0b17a93084df79fbc5725ebf5441796731d70b4"
    assert refresh._reviewed_investor_record_removal_20261104(old, new) is None
    from scripts.st_notice_boundary_review import reviewed_additions
    monkeypatch.setattr(review, "_reviewed_catalog_columns_20261105", lambda *args: None)
    with pytest.raises(StNoticeEvidenceError, match="native events changed"):
        reviewed_additions(old, new, refresh._precision_drift_only)


def test_exact_catalog_change_reuses_original_guard_without_mutation(columns_review_pair):
    original = copy.deepcopy(columns_review_pair)
    result = refresh._boundary_precision_drift(*columns_review_pair)
    assert result["policy"] == "exact_non_st_catalog_columns_20261105_v1"
    assert result["removed_event_ids"] == ["AN202611041831417267"]
    assert result["reviewed_metadata_changed_ids"] == list(IDS)
    assert result["risk_projection_unchanged"] is True
    assert refresh._boundary_summary_matches(result, refresh._boundary_precision_drift(*columns_review_pair), *columns_review_pair)
    assert columns_review_pair == original


@pytest.mark.parametrize("mutation", ["add", "extra_delete", "missing_approved_delete", "risk_delete",
    "title", "issuer", "unknown_columns", "one_column_only", "other_field", "old_and_new_title",
    "time_one_second", "time_day", "time_close", "missing_time"])
def test_unknown_content_or_time_changes_still_rejected(columns_review_pair, mutation):
    old, new = columns_review_pair
    common = next(k for k in new if k not in IDS)
    if mutation == "add":
        new["AN999999999999999999"] = copy.deepcopy(new[ID])
    elif mutation == "extra_delete":
        del new[common]
    elif mutation == "missing_approved_delete":
        key = "AN202611041831417267"; new[key] = copy.deepcopy(old[key])
    elif mutation == "risk_delete":
        del new[next(k for k, row in new.items() if "风险警示" in row["title"])]
    elif mutation == "title":
        new[ID]["title"] += "实施其他风险警示"
    elif mutation == "issuer":
        new[ID]["codes"][0]["stock_code"] = "600000"
    elif mutation == "unknown_columns":
        new[ID]["columns"] = [{"column_code": "001002004003", "column_name": "风险警示"}]
    elif mutation == "one_column_only":
        new[ID]["columns"] = copy.deepcopy(old[ID]["columns"])
    elif mutation == "other_field":
        new[common]["listing_state"] = "unexpected"
    elif mutation == "old_and_new_title":
        old[common]["title"] += "changed"; new[common]["title"] += "changed"
    elif mutation == "time_one_second":
        new[common]["display_time"] = (refresh._provider_time(old[common]["display_time"]) + timedelta(seconds=1)).replace(tzinfo=None).isoformat()
    elif mutation == "time_day":
        new[common]["display_time"] = "2026-11-05 12:00:00.000"
    elif mutation == "time_close":
        new[common]["display_time"] = "2026-11-04 14:59:59.999" if refresh._provider_time(old[common]["display_time"]).hour >= 15 else "2026-11-04 15:00:00.001"
    else:
        del new[common]["display_time"]
    with pytest.raises(StNoticeEvidenceError):
        refresh._boundary_precision_drift(old, new)


def test_real_issuer_documents_bind_frozen_review_and_no_st_semantics():
    raw = (DOCS / "review.json").read_bytes()
    assert hashlib.sha256(raw).hexdigest() == "bf2f94482b4f171246d5ca649cdcb2ca275e9dbb9b6b515d9b45fe7eb4543143"
    cert = json.loads(raw)
    for identity in IDS:
        for suffix in (".pdf", ".txt", ".receipt.json"):
            spec = cert["documents"][identity][suffix]
            assert spec["path"] == identity + suffix
            assert hashlib.sha256((DOCS / spec["path"]).read_bytes()).hexdigest() == spec["sha256"]
        pdf = (DOCS / (identity + ".pdf")).read_bytes()
        receipt = json.loads((DOCS / (identity + ".receipt.json")).read_bytes())
        text = re.sub(r"\s+", "", (DOCS / (identity + ".txt")).read_text(encoding="utf8"))
        assert pdf.startswith(b"%PDF-") and "证券代码：301128" in text
        assert "激励对象名单的公示情况说明及核查意见" in text
        assert not re.search("退市风险警示|其他风险警示|实施.{0,12}风险警示|撤销.{0,12}风险警示", text)
        assert receipt["native_row"] == cert["changes"][identity]["current_row"]
        assert receipt["raw_sha256"] == hashlib.sha256(pdf).hexdigest()
        assert receipt["status"] == 200 and receipt["tls_verify_configuration"] is True
        assert receipt["redirects_allowed"] is False
        assert receipt["requested_url"] == receipt["final_url"] == "https://pdf.dfcfw.com/pdf/H2_" + identity + "_1.pdf"


@pytest.mark.parametrize("selector", ["title", "daily", "issuer"])
def test_native_selector_regression_cannot_self_approve(columns_review_pair, monkeypatch, selector):
    import microcap_top100_mom16_biweekly_live_v2_0 as base
    from scripts import st_history_scope, st_notice_evidence
    if selector == "title":
        monkeypatch.setattr(base.freq_mod, "classify_st_notice_risk_actions", lambda title: {"other": True})
    elif selector == "daily":
        monkeypatch.setattr(st_history_scope, "_daily_notice_row", lambda *args: {"unreviewed": True})
    else:
        monkeypatch.setattr(st_notice_evidence, "is_equity_issuer", lambda *args: False)
    with pytest.raises(StNoticeEvidenceError, match="native ST selection"):
        refresh._boundary_precision_drift(*columns_review_pair)


def test_review_does_not_add_production_authority_dependencies():
    from scripts.stock_input_contract import SOURCE_FILES as stock_sources
    assert "scripts/st_notice_boundary_review.py" not in stock_sources
    assert not any("20261105" in p for p in review.SOURCE_FILES)
