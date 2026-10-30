"""Real reviewed conflict plus adversarial mutations of its identity and bytes."""
import json
import shutil
from pathlib import Path

import pytest

from scripts.st_notice_evidence import (
    StNoticeEvidenceError, evidence_fingerprint, evidence_source_files, load_verified_events,
)

ROOT = Path(__file__).resolve().parents[1]
ID = '1220378276'


@pytest.fixture
def isolated(tmp_path):
    shutil.copytree(ROOT / 'data/st_notice_evidence', tmp_path / 'data/st_notice_evidence')
    path = tmp_path / 'data/st_notice_evidence/registry.json'
    registry = json.loads(path.read_text(encoding='utf-8-sig'))
    return tmp_path, path, registry, next(e for e in registry['events'] if e['announcement_id'] == ID)


def save(path, registry):
    path.write_text(json.dumps(registry, ensure_ascii=False), encoding='utf-8')


def real_provider_rows():
    raw = json.loads((ROOT/'data/st_notice_evidence/000796_20240618_provider_response.json').read_text(encoding='utf-8'))
    return [{'announcement_id':r['announcementId'],'issuer_symbol':r['secCode'],'notice_date':'2024-06-18','title':r['announcementTitle'],'source_url':'https://static.cninfo.com.cn/'+r['adjunctUrl']} for r in raw['announcements']]


def test_real_provider_rows_use_original_identity_and_correct_body():
    events = load_verified_events(ROOT, '000796', '2024-06-18', '2024-06-18', real_provider_rows())
    event = events[ID]
    assert event['source_url'].endswith('/1220378276.PDF')
    assert event['body_source_url'].startswith('https://pdf.dfcfw.com/')
    assert event['risk_actions'] == {'delisting': False, 'other': True}
    files = evidence_source_files(ROOT)
    for spec in event['conflict_evidence'].values():
        assert 'data/st_notice_evidence/' + spec['relative_path'] in files


@pytest.mark.parametrize('field,value', [
    ('source_type','unknown'), ('source_conflict',False), ('body_source_url','https://evil.invalid/a.pdf'),
    ('body_source_url','https://pdf.dfcfw.com/pdf/H2_AN202406171636569663_1.pdf?x=1'),
    ('body_source_url','https://pdf.dfcfw.com@evil.invalid/pdf/H2_AN202406171636569663_1.pdf'),
    ('body_notice_number','2024-057'), ('symbol','000797'), ('risk_actions',{'other':False}),
    ('actual_effective_date','2024-06-18'), ('conflict_evidence',{}),
])
def test_changed_reviewed_identity_rejected(isolated, field, value):
    root,path,registry,event=isolated
    event[field]=value; save(path,registry)
    with pytest.raises(StNoticeEvidenceError): evidence_fingerprint(root)


@pytest.mark.parametrize('artifact',['provider_response','conflicting_pdf','crosscheck','review'])
@pytest.mark.parametrize('damage',['missing','bytes','escape'])
def test_all_auxiliary_evidence_is_mandatory_and_bound(isolated,artifact,damage):
    root,path,registry,event=isolated; spec=event['conflict_evidence'][artifact]
    p=root/'data/st_notice_evidence'/spec['relative_path']
    if damage=='missing': p.unlink()
    elif damage=='bytes': p.write_bytes(p.read_bytes()+b'changed')
    else: spec['relative_path']='../'+p.name; save(path,registry)
    with pytest.raises(StNoticeEvidenceError): evidence_fingerprint(root)


def test_wrong_cninfo_body_cannot_be_relabelled_correct(isolated):
    root,path,registry,event=isolated
    wrong=event['conflict_evidence']['conflicting_pdf']
    event['relative_pdf_path']=wrong['relative_path']; event['sha256']=wrong['sha256']
    save(path,registry)
    with pytest.raises(StNoticeEvidenceError): evidence_fingerprint(root)


def test_mirror_url_cannot_replace_provider_identity(isolated):
    root,path,registry,event=isolated; event['source_url']=event['body_source_url'];save(path,registry)
    with pytest.raises(StNoticeEvidenceError): evidence_fingerprint(root)


def test_ordinary_event_cannot_silently_override_body(isolated):
    root,path,registry,event=isolated
    ordinary=next(e for e in registry['events'] if e['announcement_id']!=ID)
    ordinary['body_source_url']=event['body_source_url'];save(path,registry)
    with pytest.raises(StNoticeEvidenceError): evidence_fingerprint(root)


def test_provider_row_with_mirror_url_is_rejected():
    notices=real_provider_rows()
    for row in notices:
        if row['announcement_id']==ID: row['source_url']='https://pdf.dfcfw.com/pdf/H2_AN202406171636569663_1.pdf'
    with pytest.raises(StNoticeEvidenceError): load_verified_events(ROOT,'000796','2024-06-18','2024-06-18',notices)
