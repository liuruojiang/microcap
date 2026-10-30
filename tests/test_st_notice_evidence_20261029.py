"""Evidence bindings: real reviewed PDFs plus explicit isolated fault cases."""
import copy
import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from scripts.st_notice_evidence import (
    DATE_POLICY, RULES_REVISION, SCHEMA_VERSION, StNoticeEvidenceError,
    evidence_fingerprint, load_verified_events,
)

ROOT = Path(__file__).resolve().parents[1]


def rows(events):
    return [{"announcement_id": e["announcement_id"], "issuer_symbol": e["symbol"],
             "notice_date": e["notice_date"], "title": e["title"], "source_url": e["source_url"]}
            for e in events]


def save(root, registry):
    (root / 'data/st_notice_evidence/registry.json').write_text(json.dumps(registry, ensure_ascii=False), encoding='utf-8')


@pytest.fixture
def isolated(tmp_path):
    folder = tmp_path / 'data/st_notice_evidence'; folder.mkdir(parents=True)
    # Synthetic bytes used only for fault isolation, not certified market data.
    raw = b'%PDF-1.4\nsynthetic-test-only\n'
    (folder / 'fixture.pdf').write_bytes(raw)
    event = {'symbol':'600180','notice_date':'2011-12-14','announcement_id':'60321567',
             'title':'fixture announcement','source_url':'https://static.cninfo.com.cn/finalpage/2011-12-14/60321567.PDF',
             'relative_pdf_path':'fixture.pdf','sha256':hashlib.sha256(raw).hexdigest(),
             'risk_actions':{'delisting':False,'other':True}}
    registry = {'schema_version':SCHEMA_VERSION,'rules_revision':RULES_REVISION,'date_policy':DATE_POLICY,'events':[event]}
    save(tmp_path,registry)
    return tmp_path,registry


def load(root, notices):
    return load_verified_events(root,'600180','2010-01-01','2013-01-01',notices)


@pytest.mark.parametrize('symbol',['600180','002193','002198','002514','000677'])
def test_real_reviewed_pdf_bytes_and_event_actions(symbol):
    registry=json.loads((ROOT/'data/st_notice_evidence/registry.json').read_text(encoding='utf-8-sig'))
    events=[e for e in registry['events'] if e['symbol']==symbol]
    assert events
    result=load_verified_events(ROOT,symbol,'2010-01-01','2026-10-29',pd.DataFrame(rows(events)))
    assert {k:v['risk_actions'] for k,v in result.items()}=={e['announcement_id']:e['risk_actions'] for e in events}
    assert all(v['registry_sha256']==evidence_fingerprint(ROOT) for v in result.values())
    if symbol=='600180':
        assert result['60321567']['risk_actions']=={'delisting':False,'other':True}
    if symbol in {'002198','002514'}:
        assert any(e['risk_actions']=={} for e in events)
    if symbol=='000677':
        assert result['1202069696']['risk_actions']=={'delisting':False,'other':False}
        assert result['1225266382']['risk_actions']=={'other':True}
        assert result['1225266411']['risk_actions']=={}


@pytest.mark.parametrize('missing_id',['1202069696','1225266382','1225266411'])
def test_000677_each_in_range_reviewed_announcement_is_required(missing_id):
    registry=json.loads((ROOT/'data/st_notice_evidence/registry.json').read_text(encoding='utf-8-sig'))
    events=[e for e in registry['events'] if e['symbol']=='000677' and e['announcement_id']!=missing_id]
    with pytest.raises(StNoticeEvidenceError,match=missing_id):
        load_verified_events(ROOT,'000677','2010-01-01','2026-10-29',pd.DataFrame(rows(events)))


def test_duplicates_idempotent_and_unknown_id_cannot_borrow_actions(isolated):
    root,registry=isolated; r=rows(registry['events']); r.append(copy.deepcopy(r[0]))
    unknown={**r[0],'announcement_id':'unknown','risk_actions':{'other':False}}
    assert set(load(root,r+[unknown]))=={'60321567'}
    with pytest.raises(StNoticeEvidenceError,match='missing'):
        load(root,[unknown])


@pytest.mark.parametrize('field,value',[
    ('issuer_symbol','600181'),('notice_date','2011-12-15'),('title','altered'),
    ('source_url','https://static.cninfo.com.cn/other.PDF'),('announcement_id','999'),
])
def test_notice_identity_mismatch_or_missing_is_rejected(isolated,field,value):
    root,registry=isolated; r=rows(registry['events']); r[0][field]=value
    with pytest.raises(StNoticeEvidenceError): load(root,r)


def test_conflicting_duplicate_notice_rejected(isolated):
    root,registry=isolated; r=rows(registry['events']); r.append({**r[0],'title':'contradiction'})
    with pytest.raises(StNoticeEvidenceError): load(root,r)


def test_outside_range_is_not_required_and_boundary_inclusive(isolated):
    root,registry=isolated
    assert load_verified_events(root,'600180','2011-12-15','2013-01-01',[])=={}
    with pytest.raises(StNoticeEvidenceError,match='missing'):
        load_verified_events(root,'600180','2011-12-14','2011-12-14',[])


@pytest.mark.parametrize('field,value',[
    ('schema_version',True),('schema_version',1),('rules_revision','old-v3'),
    ('date_policy','implementation_date'),('events',[]),('events',None),
])
def test_registry_header_fail_closed(isolated,field,value):
    root,registry=isolated; registry[field]=value; save(root,registry)
    with pytest.raises(StNoticeEvidenceError): evidence_fingerprint(root)


@pytest.mark.parametrize('field,value',[
    ('symbol','180'),('notice_date','2011-02-30'),('announcement_id',123),
    ('title',''),('source_url','https://evil.invalid/a.pdf'),
    ('source_url','https://static.cninfo.com.cn@evil.invalid/a.pdf'),
    ('source_url','https://static.cninfo.com.cn/finalpage/2011-12-15/60321567.PDF'),
    ('source_url','https://static.cninfo.com.cn/finalpage/2011-12-14/99999.PDF'),
    ('source_url','https://static.cninfo.com.cn/finalpage/2011-12-14/60321567.PDF?override=1'),
    ('relative_pdf_path','../escape.pdf'),('relative_pdf_path','missing.pdf'),
    ('sha256','0'*64),('risk_actions',None),('risk_actions',{'other':1}),
    ('risk_actions',{'typo':True}),
])
def test_registry_event_damage_fail_closed(isolated,field,value):
    root,registry=isolated; registry['events'][0][field]=value; save(root,registry)
    with pytest.raises(StNoticeEvidenceError): evidence_fingerprint(root)


def test_content_change_and_missing_registry_fail_closed(isolated):
    root,registry=isolated; folder=root/'data/st_notice_evidence'
    (folder/'fixture.pdf').write_bytes(b'%PDF-1.4\nchanged')
    with pytest.raises(StNoticeEvidenceError,match='hash'): load(root,rows(registry['events']))
    with pytest.raises(StNoticeEvidenceError): evidence_fingerprint(root/'absent')


def test_same_id_conflicting_registry_rejected(isolated):
    root,registry=isolated; altered=copy.deepcopy(registry['events'][0]); altered['risk_actions']={}
    registry['events'].append(altered); save(root,registry)
    with pytest.raises(StNoticeEvidenceError,match='Conflicting'): evidence_fingerprint(root)


def test_registry_fingerprint_binds_reviewed_actions_and_empty_is_valid(isolated):
    root,registry=isolated; before=evidence_fingerprint(root)
    registry['events'][0]['risk_actions']={}; save(root,registry)
    assert evidence_fingerprint(root)!=before
    assert load(root,rows(registry['events']))['60321567']['risk_actions']=={}


def test_aware_notice_timestamp_uses_shanghai_calendar_date(isolated):
    root,registry=isolated; r=rows(registry['events'])
    r[0]['notice_date']='2011-12-13T16:00:00+00:00'
    assert set(load(root,r))=={'60321567'}


def test_path_symlink_escape_rejected_when_supported(isolated,tmp_path):
    root,registry=isolated; external=tmp_path/'outside.pdf'; external.write_bytes(b'%PDF-1.4\nexternal')
    link=root/'data/st_notice_evidence/link.pdf'
    try: link.symlink_to(external)
    except (OSError,NotImplementedError): pytest.skip('OS does not permit symlink creation')
    registry['events'][0].update(relative_pdf_path='link.pdf',sha256=hashlib.sha256(external.read_bytes()).hexdigest()); save(root,registry)
    with pytest.raises(StNoticeEvidenceError,match='escapes'): evidence_fingerprint(root)
