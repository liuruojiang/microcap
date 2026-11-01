import copy
from datetime import datetime, timezone
import hashlib
import json

import pytest

from scripts import st_notice_incremental as m


def archive(tmp_path, rows=None):
    rows = rows if rows is not None else [dict(art_code="AN202610300001", notice_date="2026-10-30 00:00:00", title="测试公告", codes=[dict(stock_code="000001", market_code="0", ann_type="A"), dict(stock_code="000001", market_code="1", ann_type="INDEX")])]
    raw = json.dumps(dict(success=1, data=dict(total_hits=len(rows), page_index=1, page_size=100, list=rows))).encode()
    (tmp_path / "page_0001.json").write_bytes(raw)
    receipt = dict(url=m.API, params=m._params("2026-10-30", 1), status=200,
                   collected_at=datetime.now(timezone.utc).isoformat(), sha256=hashlib.sha256(raw).hexdigest())
    m._dump(tmp_path / "page_0001.receipt.json", receipt)
    entry = dict(response="page_0001.json", receipt="page_0001.receipt.json", response_sha256=m._sha(raw), receipt_sha256=m._sha((tmp_path / "page_0001.receipt.json").read_bytes()))
    m._dump(tmp_path / "manifest.json", dict(schema=m.SCHEMA, date="2026-10-30", pages=[entry]))
    return rows


def test_market_and_stock_code_both_bind(tmp_path):
    rows = archive(tmp_path)
    rows.append(dict(art_code="AN202610300002", notice_date="2026-10-30", title="指数公告", codes=[dict(stock_code="600180", market_code="1", ann_type="INDEX")]))
    archive(tmp_path, rows)
    result = m.verify_day(tmp_path, "2026-10-30")
    assert result["total_hits"] == 2
    assert set(result["by_symbol"]) == {"000001"}


def test_rehashed_duplicate_cannot_claim_complete(tmp_path):
    rows = archive(tmp_path)
    archive(tmp_path, rows + copy.deepcopy(rows))
    with pytest.raises(m.StNoticeEvidenceError, match="duplicate"):
        m.verify_day(tmp_path, "2026-10-30")


def test_wrong_notice_date_rejected_even_with_fresh_hash(tmp_path):
    rows = archive(tmp_path)
    rows[0]["notice_date"] = "2026-10-29"
    archive(tmp_path, rows)
    with pytest.raises(m.StNoticeEvidenceError, match="date invalid"):
        m.verify_day(tmp_path, "2026-10-30")


def test_mutated_receipt_rejected(tmp_path):
    archive(tmp_path)
    path = tmp_path / "page_0001.receipt.json"
    receipt = json.loads(path.read_bytes())
    receipt["params"]["f_node"] = "3"
    m._dump(path, receipt)
    with pytest.raises(m.StNoticeEvidenceError, match="binding"):
        m.verify_day(tmp_path, "2026-10-30")


def test_complete_archive_reuse_never_queries_network(tmp_path):
    folder = tmp_path / "2026-10-30"
    folder.mkdir()
    archive(folder)
    class NoNetwork:
        def get(self, *args, **kwargs):
            raise AssertionError("unexpected query")
    assert m.collect_days(tmp_path, "2026-10-30", "2026-10-30", session=NoNetwork(), required_asof=datetime(2026,10,30,7,tzinfo=timezone.utc))[0]["total_hits"] == 1


@pytest.mark.parametrize("mutation", ["missing_codes", "malformed_date", "future_publication"])
def test_source_identity_and_time_counterexamples(tmp_path, mutation):
    rows = archive(tmp_path)
    if mutation == "missing_codes":
        rows[0]["codes"] = []
    elif mutation == "malformed_date":
        rows[0]["notice_date"] = "2026-10-30INVALID"
    else:
        rows[0]["display_time"] = "2099-10-30 19:04:57:417"
    archive(tmp_path, rows)
    with pytest.raises(m.StNoticeEvidenceError):
        m.verify_day(tmp_path, "2026-10-30")


def test_old_snapshot_not_reused_as_fresh(tmp_path):
    folder = tmp_path / "2026-10-30"
    folder.mkdir()
    archive(folder)
    with pytest.raises(m.StNoticeEvidenceError, match="older than required"):
        m.collect_days(tmp_path, "2026-10-30", "2026-10-30")


def test_interrupted_source_preserved(tmp_path):
    folder = tmp_path / "2026-10-30"
    folder.mkdir()
    path = folder / "page_0001.json"
    path.write_bytes(b"partial")
    with pytest.raises(m.StNoticeEvidenceError, match="Interrupted"):
        m.collect_days(tmp_path, "2026-10-30", "2026-10-30")
    assert path.read_bytes() == b"partial"
    assert not (folder / "manifest.json").exists()
