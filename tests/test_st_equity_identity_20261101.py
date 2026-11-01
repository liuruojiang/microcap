"""Real Eastmoney 600593 notice also lists Index 000001; it is not Ping An."""
import hashlib
import json
from pathlib import Path
import pytest
import scripts.st_notice_archive as archive
from scripts.st_notice_evidence import DATE_POLICY, StNoticeEvidenceError, _eastmoney_evidence, is_equity_issuer

REAL = json.loads((Path(__file__).parent / 'fixtures/st_equity_identity_real/notice_row.json').read_text(encoding='utf8'))

def test_real_asset_class_collision():
    assert is_equity_issuer(REAL, '600593')
    assert not is_equity_issuer(REAL, '000001')

def test_native_evidence_rejects_real_index_as_stock(tmp_path):
    art = REAL['art_code']
    raw = json.dumps(dict(success=1, data=dict(list=[REAL]))).encode()
    (tmp_path / 'api.json').write_bytes(raw)
    event = dict(source_provider='eastmoney', announcement_id=art,
                 source_url=f'https://pdf.dfcfw.com/pdf/H2_{art}_1.pdf',
                 body_source_url=f'https://pdf.dfcfw.com/pdf/H2_{art}_1.pdf',
                 symbol='000001', notice_date=REAL['notice_date'][:10],
                 title=REAL['title'], provider_title=REAL['title'], removed_issuer_prefix='',
                 native_evidence=dict(provider_response=dict(relative_path='api.json', sha256=hashlib.sha256(raw).hexdigest(), source_url=archive.API), review={}))
    with pytest.raises(StNoticeEvidenceError, match='issuer/date/title identity'):
        _eastmoney_evidence(tmp_path, event)

def test_archive_rejects_real_index_as_stock(tmp_path, monkeypatch):
    # Controlled one-row query envelope around the unchanged real provider row.
    monkeypatch.setattr(archive, '_registry', lambda root: ({}, 'f' * 64))
    def put(name, obj):
        raw = json.dumps(obj).encode(); (tmp_path / name).write_bytes(raw)
        return dict(path=name, sha256=hashlib.sha256(raw).hexdigest())
    source = put('source.json', dict(success=1, data=dict(page_index=1, page_size=100, total_hits=1, list=[REAL])))
    receipt = put('receipt.json', dict(url=archive.API, status=200, sha256=source['sha256'], params=dict(stock_list='000001', f_node='0', s_node='0', ann_type='A', page_size='100', page_index='1', sr='-1', begin_time='1990-01-01', end_time='2026-10-29')))
    manifest = put('manifest.json', dict(schema_version=2, selection_revision='all-api-pages-risk-title-or-category-v1', date_policy=DATE_POLICY, registry_sha256='f'*64, symbols={'000001':dict(status='complete_symbol_archive', source_files=[source,receipt], partitions=[dict(pages=[dict(response_path='source.json',receipt_path='receipt.json')])])}))
    monkeypatch.setenv('MICROCAP_ST_NOTICE_ARCHIVE', 'manifest.json')
    monkeypatch.setenv('MICROCAP_ST_NOTICE_ARCHIVE_SHA256', manifest['sha256'])
    with pytest.raises(StNoticeEvidenceError, match='Raw API issuer mismatch'):
        archive.load_notice_archive(tmp_path, '000001', '19900101', '20261029')
