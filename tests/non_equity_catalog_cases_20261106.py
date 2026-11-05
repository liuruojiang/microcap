"""Actual failed cloud boundary plus fail-closed metadata counterexamples."""
import copy
import hashlib
import json
from pathlib import Path
import pytest
from scripts import st_notice_refresh as refresh
from scripts import st_notice_boundary_review as review
from scripts.st_notice_evidence import StNoticeEvidenceError

FIXTURE = Path(__file__).parent / "fixtures/non_equity_catalog_20261106"
IDENTITIES = {"AN202611041831414409", "AN202611041831414410", "AN202611041831417416"}
TARGET = "AN202611041831414410"

@pytest.fixture
def non_equity_pair():
    old = json.loads((FIXTURE.parent / "st_notice_removed_inv_20261104.json").read_bytes())["previous"]
    return old, refresh._native(FIXTURE / "2026-11-04", "2026-11-04")

def test_actual_non_equity_catalog_failure_and_repair(non_equity_pair, monkeypatch):
    old, new = non_equity_pair
    original = copy.deepcopy(non_equity_pair)
    result = refresh._boundary_precision_drift(old, new)
    assert result["policy"] == "non_equity_catalog_metadata_with_native_boundary_v1"
    assert set(result["non_equity_catalog_changes"]) == IDENTITIES
    assert result["underlying_boundary_policy"] == "exact_non_st_catalog_columns_20261105_v1"
    assert result["current_stable_native_sha256"] == "02a86b715304a1f75f1cc599888be27697f46408d46af228de15f5e3e73cf02c"
    assert result["risk_projection_unchanged"] is True
    assert original == non_equity_pair
    assert refresh._boundary_summary_matches(result, refresh._boundary_precision_drift(old,new), old,new)
    monkeypatch.setattr(review, "_non_equity_catalog_metadata", lambda *args: None)
    with pytest.raises(StNoticeEvidenceError):
        refresh._boundary_precision_drift(old,new)

@pytest.mark.parametrize("change", ["title", "issuer", "ann_type", "columns", "notice_date", "state", "sort_day", "sort_invalid", "missing_sort", "display_second", "display_day", "display_close", "delete", "add", "other_row"])
def test_non_equity_catalog_changes_never_mask_risk_or_time(non_equity_pair, change):
    old, new = non_equity_pair
    row = new[TARGET]
    if change == "title": row["title"] += "实施退市风险警示"
    elif change == "issuer": row["codes"][0]["stock_code"] = "301288"
    elif change == "ann_type": row["codes"][0]["ann_type"] = "A"
    elif change == "columns": row["columns"] = []
    elif change == "notice_date": row["notice_date"] = "2026-11-05 00:00:00"
    elif change == "state": row["listing_state"] = "unknown"
    elif change == "sort_day": row["sort_date"] = "2026-11-05 12:00:00"
    elif change == "sort_invalid": row["sort_date"] = "2026-11-04 25:00:00"
    elif change == "missing_sort": del row["sort_date"]
    elif change == "display_second": row["display_time"] = "2026-11-04 20:00:00"
    elif change == "display_day": row["display_time"] = "2026-11-05 12:00:00"
    elif change == "display_close": row["display_time"] = "2026-11-04 14:59:59.999"
    elif change == "delete": del new[TARGET]
    elif change == "add": new["unreviewed"] = copy.deepcopy(row)
    else: new["AN202611041831408711"]["listing_state"] = "2"
    with pytest.raises(StNoticeEvidenceError):
        refresh._boundary_precision_drift(old,new)

@pytest.mark.parametrize("selector", ["equity", "risk"])
def test_non_equity_catalog_native_selector_regression(non_equity_pair, monkeypatch, selector):
    from scripts import st_notice_evidence
    import microcap_top100_mom16_biweekly_live_v2_0 as base
    if selector == "equity": monkeypatch.setattr(st_notice_evidence,"is_equity_issuer",lambda *a: True)
    else: monkeypatch.setattr(base.freq_mod,"classify_st_notice_risk_actions",lambda *a: {"other": True})
    with pytest.raises(StNoticeEvidenceError): refresh._boundary_precision_drift(*non_equity_pair)

def test_non_equity_catalog_actual_issuer_documents():
    for identity in IDENTITIES:
        receipt=json.loads((FIXTURE/(identity+".receipt.json")).read_bytes())
        raw=(FIXTURE/(identity+".raw")).read_bytes()
        text=(FIXTURE/(identity+".txt")).read_text(encoding="utf8")
        assert receipt["status"] == 200 and raw.startswith(b"%PDF-")
        assert receipt["bytes"] == len(raw) and receipt["sha256"] == hashlib.sha256(raw).hexdigest()
        assert receipt["url"] == receipt["actual_url"] == "https://pdf.dfcfw.com/pdf/H2_"+identity+"_1.pdf"
        assert "森峰激光" in text and "北京证券交易所" in text
