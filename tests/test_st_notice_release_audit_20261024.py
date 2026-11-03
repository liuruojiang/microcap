"""New synthetic release-audit cases; no metadata, dates or prices are market evidence."""
import pytest

from test_st_notice_state_machine_20261024 import (
    ENTRY_D, ENTRY_O, EXIT_D, funcs, intervals, spans,
)


@pytest.mark.parametrize("joiner", ["及", "以及", "暨", "和", "并"])
@pytest.mark.parametrize("entry,partial", [
    ("关于实施退市风险警示及其他风险警示的公告", "退市风险警示{joiner}部分其他风险警示"),
    ("关于实施退市风险警示及其他风险警示的公告", "其他风险警示{joiner}部分退市风险警示"),
])
def test_elided_withdrawal_keeps_partial_category_active(funcs, joiner, entry, partial):
    title = "关于撤销" + partial.format(joiner=joiner) + "的公告"
    actual = intervals(funcs, [("2024-01-02", entry), ("2024-02-01", title)])
    assert spans(actual) == [("2024-01-02", None)]


@pytest.mark.parametrize("title", [
    "关于拟撤销退市风险警示的公告", "关于拟撤销其他风险警示的公告",
    "关于拟撤销退市风险警示及其他风险警示的公告", "关于拟摘帽的公告",
])
def test_proposed_withdrawal_neither_closes_warning_nor_infers_unknown_history(funcs, title):
    assert funcs["classify_st_notice_risk_actions"](title) == {}
    assert intervals(funcs, [("2024-02-01", title)]) == []
    assert spans(intervals(funcs, [("2024-01-02", ENTRY_D), ("2024-02-01", title)])) == [("2024-01-02", None)]


def test_proposed_exit_does_not_suppress_separate_effective_other_entry(funcs):
    title = "关于拟撤销退市风险警示并实施其他风险警示的公告"
    assert funcs["classify_st_notice_risk_actions"](title) == {"other": True}
    assert spans(intervals(funcs, [("2024-02-01", title)])) == [("2024-02-01", None)]


def test_elided_partial_withdrawal_unknown_history_and_known_nonST(funcs):
    title = "关于撤销退市风险警示及部分其他风险警示的公告"
    assert spans(intervals(funcs, [("2024-02-01", title)])) == [("2010-01-04", None)]
    assert spans(intervals(funcs, [("2009-12-01", EXIT_D), ("2024-02-01", title)])) == [("2024-02-01", None)]


def test_full_elided_withdrawal_still_closes_all_categories(funcs):
    actual = intervals(funcs, [("2024-01-02", "关于实施退市风险警示及其他风险警示的公告"),
                              ("2024-02-01", "关于撤销退市风险警示及其他风险警示的公告")])
    assert spans(actual) == [("2024-01-02", "2024-02-01")]


@pytest.fixture
def exact_late_boundary():
    """Actual preserved all-class response pair, not generated market evidence."""
    import json
    from scripts import st_notice_boundary_review as boundary
    record = json.loads((boundary.DIRECTORY / "reviewed_20261104/registry.json").read_bytes())
    return record["previous_rows"], record["current_rows"]


def test_exact_late_boundary_retains_all_original_risk_and_body_checks(exact_late_boundary):
    from scripts.st_notice_refresh import _boundary_precision_drift
    result = _boundary_precision_drift(*exact_late_boundary)
    assert result["policy"] == "exact_reviewed_late_boundary_transport_20261104_v1"
    assert len(result["reviewed_non_st_added_ids"]) == 82
    assert result["reviewed_existing_st_progress_ids"] == ["AN202611031831376420"]
    assert len(result["reviewed_metadata_changed_ids"]) == 7
    assert result["changed_event_count"] == 878 and result["maximum_delta_microseconds"] == 132000


@pytest.mark.parametrize("change", ["new_event", "removed", "new_effective_title", "issuer", "new_metadata", "day", "large_time"])
def test_exact_late_boundary_unknown_input_still_fails_closed(exact_late_boundary, change):
    import copy
    from scripts.st_notice_refresh import _boundary_precision_drift
    from scripts.st_notice_evidence import StNoticeEvidenceError
    old, new = exact_late_boundary
    row = new["AN202611031831376420"]
    if change == "new_event": new["AN_UNREVIEWED"] = copy.deepcopy(row)
    if change == "removed": new.pop(next(iter(old)))
    if change == "new_effective_title": row["title"] = "关于实施退市风险警示的公告"
    if change == "issuer": row["codes"][0]["stock_code"] = "000001"
    if change == "new_metadata": new["AN202611021831333702"]["codes"][0]["short_name"] = "unreviewed label"
    if change == "day": row["notice_date"] = "2026-11-04 00:00:00"
    if change == "large_time": row["display_time"] = "2026-11-03 18:37:12:440"
    with pytest.raises(StNoticeEvidenceError):
        _boundary_precision_drift(old, new)


@pytest.mark.parametrize("change", ["pdf", "text", "receipt_tls", "future_clock", "progress_effective_date", "scope_count", "source_path"])
def test_exact_late_boundary_document_or_review_tamper_rejected(exact_late_boundary, tmp_path, monkeypatch, change):
    import json
    import shutil
    from scripts import st_notice_boundary_review as boundary
    from scripts.st_notice_refresh import _boundary_precision_drift
    from scripts.st_notice_evidence import StNoticeEvidenceError
    folder = tmp_path / "reviews"
    shutil.copytree(boundary.DIRECTORY, folder)
    monkeypatch.setattr(boundary, "DIRECTORY", folder)
    registry = folder / "reviewed_20261104/registry.json"
    review = json.loads(registry.read_bytes())
    record = review["documents"]["AN202611031831376420"]
    if change in ("pdf", "text"):
        p = folder / record[change]["path"]
        p.write_bytes(p.read_bytes() + b"tampered")
    elif change in ("receipt_tls", "future_clock"):
        p = folder / record["receipt"]["path"]
        receipt = json.loads(p.read_bytes())
        if change == "receipt_tls": receipt["tls_verify_configuration"] = False
        else: receipt["observation_end"] = "2999-01-01T00:00:00+00:00"
        p.write_text(json.dumps(receipt), encoding="utf8")
        record["receipt"]["sha256"] = boundary.sha(p.read_bytes())
    elif change == "progress_effective_date": review["progress"]["AN202611031831376420"]["actual_effective_date"] = "2026-11-03"
    elif change == "scope_count": review["expected_added_count"] = 84
    else: record["text"]["path"] = "../outside.txt"
    registry.write_text(json.dumps(review, ensure_ascii=False), encoding="utf8")
    with pytest.raises(StNoticeEvidenceError):
        _boundary_precision_drift(*exact_late_boundary)


def test_exact_late_boundary_sources_are_registered_in_release_closure():
    from pathlib import Path
    from scripts import st_notice_boundary_review as boundary, top100_delivery, top100_cloud_delivery
    expected = {"data/st_notice_boundary_reviews/reviewed_20261104/registry.json"}
    expected.update("data/st_notice_boundary_reviews/reviewed_20261104/" + identity + suffix
                    for identity in ("AN202611031831376420", "AN202611031831373422")
                    for suffix in (".pdf", ".txt", ".receipt.json"))
    root = Path(__file__).resolve().parents[1]
    assert expected <= set(boundary.SOURCE_FILES)
    assert expected <= set(top100_delivery.RELEASE_FILES) and expected <= set(top100_cloud_delivery.SOURCE_FILES)
    assert expected <= set(top100_delivery.evidence_source_files(root))
    assert all((root / name).is_file() for name in expected)
