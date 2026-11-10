"""Software transaction probes; synthetic archives are not market evidence."""
import copy,json
from contextlib import nullcontext
from types import SimpleNamespace
import pytest
from scripts import st_metadata_refresh as m
from scripts.st_notice_evidence import StNoticeEvidenceError


@pytest.fixture
def mixed(tmp_path,monkeypatch):
    root=tmp_path;cache=root/'.microcap_index_cache';meta=cache/'security_meta';meta.mkdir(parents=True)
    monkeypatch.setattr(m,'MARKER','.microcap_index_cache/local_metadata_scopes/trial.json')
    (root/m.MARKER).parent.mkdir()
    common=dict(schema_version=3,registry_sha256='registry',scope={'path':'scope.json','sha256':'f'*64},symbols={})
    days=[dict(date='2026-10-'+str(d),manifest={'path':str(d)+'.json','sha256':str(d)*32}) for d in (28,29,30)]
    refs=[]
    for name,n in (('cold.json',1),('previous.json',2),('current.json',3)):
        raw=m._bytes(dict(**common,daily_increments=days[:n]));(root/name).write_bytes(raw);refs.append([name,m._sha(raw)])
    cold,previous,current=refs
    (root/m.FRESHNESS).parent.mkdir();(root/m.FRESHNESS).write_bytes(m._bytes(dict(previous_authority=previous)))
    monkeypatch.setattr(m,'resolve_archive_reference',lambda r:tuple(current))
    monkeypatch.setattr(m,'frozen_notice_sources',lambda r:nullcontext())
    observed=[]
    monkeypatch.setattr(m,'source_closure',lambda r,manifest_path=None,expected_sha=None:
        observed.append((manifest_path,expected_sha)) or {})
    for s,ref,end in (('600000',cold,'2026-10-28'),('600001',previous,'2026-10-29')):
        (meta/(s+'.json')).write_bytes(m._bytes(dict(symbol=s,st_notice_archive_manifest_path=ref[0],
            st_notice_archive_sha256=ref[1],notice_query_end_date=end,notice_source_complete=True,
            st_notice_evidence_sha256='registry',st_intervals=[],list_date='2000-01-01',delist_date=None)))
    mod=SimpleNamespace(ROOT=root,SECURITY_META_DIR=meta,
        list_backtest_universe_symbols=lambda:['600000','600001'],_notice_horizon_covers=lambda p,t:True)
    def build(s,as_of):
        value=json.loads((meta/(s+'.json')).read_bytes())
        value.update(st_notice_archive_sha256=current[1],st_notice_archive_manifest_path=current[0],
                     notice_query_end_date=as_of,st_intervals=[dict(start=as_of,end=None)])
        (mod.SECURITY_META_DIR/(s+'.json')).write_bytes(m._bytes(value));return value
    mod.build_security_meta=build
    return root,mod,meta,refs,observed


def run(fixture,scoped=True,consumed='2026-10-28'):
    root,mod,*_=fixture
    return m.refresh_metadata_after_notice(root=root,freq_mod=mod,target='2026-10-30',
        notice_result={'authority_changed':True},consumed_through=consumed,scoped_preparation=scoped)


def test_cold_and_resident_candidates_can_renew_from_different_verified_prefixes(mixed):
    assert run(mixed)['symbols']==2
    assert mixed[4]==[tuple(mixed[3][0])]
    result=json.loads((mixed[0]/m.MARKER).read_bytes())
    assert result['old_boundaries']=={'600000':'2026-10-28'}
    assert result['scoped_preparation'] is True
    assert all(json.loads(p.read_bytes())['st_notice_archive_sha256']==mixed[3][2][1] for p in mixed[2].glob('*.json'))


def test_default_production_route_still_rejects_a_cold_authority(mixed):
    originals={p:p.read_bytes() for p in mixed[2].glob('*.json')}
    with pytest.raises(StNoticeEvidenceError,match='not bound to previous archive'):run(mixed,scoped=False)
    assert all(p.read_bytes()==b for p,b in originals.items())


@pytest.mark.parametrize('fault',['prefix','base','hash'])
def test_other_archives_cannot_be_promoted_by_the_scoped_flag(mixed,fault):
    root,mod,meta,refs,_=mixed
    path=root/refs[0][0];value=json.loads(path.read_bytes())
    if fault=='prefix':value['daily_increments'][0]['manifest']['sha256']='0'*64
    elif fault=='base':value['registry_sha256']='another registry'
    else:
        path.write_bytes(path.read_bytes()+b' ')
    if fault!='hash':
        raw=m._bytes(value);path.write_bytes(raw)
        old=json.loads((meta/'600000.json').read_bytes());old['st_notice_archive_sha256']=m._sha(raw)
        (meta/'600000.json').write_bytes(m._bytes(old))
    with pytest.raises(StNoticeEvidenceError,match='archive'):run(mixed)
    assert not (root/m.MARKER).exists()


def test_scoped_flag_cannot_cover_a_gap_inside_consumed_history(mixed):
    with pytest.raises(StNoticeEvidenceError,match='unverified gap inside consumed'):run(mixed,consumed='2026-10-29')


def test_old_candidate_inclusive_history_remains_protected(mixed):
    root,mod,meta,*_=mixed;build=mod.build_security_meta
    def changed(s,as_of):
        value=build(s,as_of);value['st_intervals'][0]['start']='2026-10-28'
        (mod.SECURITY_META_DIR/(s+'.json')).write_bytes(m._bytes(value));return value
    mod.build_security_meta=changed
    with pytest.raises(StNoticeEvidenceError,match='Historical ST/active'):run(mixed)


@pytest.mark.parametrize('fault',['budget','marker'])
def test_scoped_preparation_cannot_be_used_as_a_whole_market_marker(mixed,monkeypatch,fault):
    if fault=='budget':mixed[1].list_backtest_universe_symbols=lambda:[str(600000+n) for n in range(301)]
    else:monkeypatch.setattr(m,'MARKER','.microcap_index_cache/st_metadata_refresh.json')
    with pytest.raises(StNoticeEvidenceError,match='explicit scoped marker'):run(mixed)


def test_current_scoped_metadata_and_published_transport_match_exact_d5_review():
    from pathlib import Path
    from scripts import realtime_state_bundle as state
    from scripts.stock_input_contract import SOURCE_FILES,source_fingerprint
    root=Path(__file__).resolve().parents[1]
    old=json.loads((root/'tests/fixtures/maintenance_d5_source_identity_20261109.json').read_bytes())['stock_input_evidence_fingerprint']
    current=copy.deepcopy(old);current['code']=source_fingerprint(root)
    review=state.TOTAL_NEUTRAL_SHARE_D5_SOURCE_REVIEW
    changed={n for n in old['code'] if old['code'][n]!=current['code'][n]}
    assert changed==set(review['changes'])&set(SOURCE_FILES)
    assert all((old['code'][n],current['code'][n])==tuple(review['changes'][n]) for n in changed)
    assert state.stock_input_evidence_matches_validated_source(old,current)
    for previous_review in (state.SCOPED_METADATA_D5_SOURCE_REVIEW,
            state.RECOVERABLE_PREPARATION_D5_SOURCE_REVIEW,
            state.EXACT_FUTURE_STRUCTURE_D5_SOURCE_REVIEW,
            state.ARCHIVED_REFRESH_RECEIPT_D5_SOURCE_REVIEW):
        prior=copy.deepcopy(old)
        prior['code'].update({n:p[1] for n,p in previous_review['changes'].items() if n in prior['code']})
        assert state.stock_input_evidence_matches_validated_source(old,prior)
    for name in ('scripts/st_metadata_refresh.py','scripts/stock_daily_alternative.py'):
        unknown=copy.deepcopy(current);unknown['code'][name]='f'*64
        assert not state.stock_input_evidence_matches_validated_source(old,unknown)
        partial=copy.deepcopy(current);partial['code'][name]=old['code'][name]
        assert not state.stock_input_evidence_matches_validated_source(old,partial)
