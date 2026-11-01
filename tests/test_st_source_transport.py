"""Notice source identity survives transport and cannot silently disappear."""
import hashlib
import json
from pathlib import Path

import pytest

from scripts import realtime_state_bundle as bundle


def seed(root):
    def write(name, body):
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        raw = json.dumps(body).encode()
        path.write_bytes(raw)
        return dict(path=name, sha256=hashlib.sha256(raw).hexdigest())
    source = write('evidence/source.json', {'source': 'test fixture'})
    records = write('evidence/records.json', [])
    manifest = write('evidence/archive.json', {'schema_version': 2, 'symbols': {
        '000001': {'source_files': [source], 'records': records, 'crosswalk': []}}})
    write('.microcap_index_cache/st_notice_archive_pointer.json', {
        'manifest_path': manifest['path'], 'manifest_sha256': manifest['sha256']})
    write('.microcap_index_cache/security_meta/000001.json', {
        'symbol': '000001', 'st_notice_archive_sha256': manifest['sha256']})


def test_notice_closure_survives_new_root(tmp_path, monkeypatch):
    for name in ('MICROCAP_ST_NOTICE_ARCHIVE', 'MICROCAP_ST_NOTICE_ARCHIVE_SHA256'):
        monkeypatch.delenv(name, raising=False)
    first, second = tmp_path / 'first', tmp_path / 'second'
    seed(first)
    files = bundle._iter_bundle_files(first)
    assert {'evidence/source.json', 'evidence/records.json', 'evidence/archive.json',
            '.microcap_index_cache/st_notice_archive_pointer.json'}.issubset(files)
    for relative in files:
        path = second / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes((first / relative).read_bytes())
    assert bundle._st_source_transport_files(second) == bundle._st_source_transport_files(first)


@pytest.mark.parametrize('missing', ['evidence/source.json', '.microcap_index_cache/st_notice_archive_pointer.json'])
def test_missing_notice_authority_is_not_silently_omitted(tmp_path, monkeypatch, missing):
    for name in ('MICROCAP_ST_NOTICE_ARCHIVE', 'MICROCAP_ST_NOTICE_ARCHIVE_SHA256'):
        monkeypatch.delenv(name, raising=False)
    seed(tmp_path)
    (tmp_path / missing).unlink()
    with pytest.raises((ValueError, FileNotFoundError)):
        bundle._iter_bundle_files(tmp_path)


def test_mixed_notice_metadata_rejected(tmp_path):
    seed(tmp_path)
    (tmp_path / '.microcap_index_cache/security_meta/000002.json').write_text('{}')
    with pytest.raises(ValueError, match='missing or mixed'):
        bundle._iter_bundle_files(tmp_path)
