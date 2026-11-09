"""Synthetic metadata transactions; no current market certification."""
import json
from types import SimpleNamespace
import pytest
from scripts import scoped_metadata_consumer as m


@pytest.fixture
def renewal(tmp_path):
    root=tmp_path/'receiver';source=tmp_path/'producer'
    old_dir=root/'.microcap_index_cache/security_meta';new_dir=source/'.microcap_index_cache/security_meta'
    old_dir.mkdir(parents=True);new_dir.mkdir(parents=True)
    symbols=[str(600000+n) for n in range(300)];universe=symbols+['600999']
    old_records={}
    for s in universe:
        old=dict(symbol=s,notice_query_end_date='2026-11-05',notice_source_complete=True,
            current_st_history_resolved=True,st_notice_archive_manifest_path='old.json',
            st_notice_archive_sha256='a'*64,st_intervals=[],list_date='2000-01-01',delist_date=None,
            last_trade_date='2026-11-05',st_notice_evidence_sha256='fixed-registry')
        raw=json.dumps(old).encode();(old_dir/(s+'.json')).write_bytes(raw);old_records[s]=raw
        if s in symbols:
            new={**old,'notice_query_end_date':'2026-11-09','last_trade_date':'2026-11-09',
                'st_notice_archive_manifest_path':'new.json','st_notice_archive_sha256':'b'*64}
            (new_dir/(s+'.json')).write_bytes(json.dumps(new).encode())
    options=dict(symbols=symbols,target='2026-11-09',previous_fingerprint=m.metadata_fingerprint(old_records),
        current_authority=['new.json','b'*64],consumed={s:'2026-11-05' for s in universe},universe=universe,
        native_freq=SimpleNamespace(_notice_horizon_covers=lambda p,t:p['notice_query_end_date']>=str(t.date())))
    return root,source,options,old_records


def test_scoped_binding_preserves_whole_historical_universe_and_all_cold_bytes(renewal):
    root,source,options,old=renewal
    result=m.plan_metadata_renewal(root,source,**options)
    assert result['historical_universe_count']==301 and result['cold_count']==1
    assert set(result['new_records'])==set(options['symbols'])
    assert result['previous_fingerprint']!=result['fingerprint']
    assert all((root/'.microcap_index_cache/security_meta'/(s+'.json')).read_bytes()==raw for s,raw in old.items())
    assert not result['final_strategy_delivery']


@pytest.mark.parametrize('fault',['historical_st','lifecycle','unknown_field','unresolved','wrong_archive','gap','old_fingerprint'])
def test_scoped_metadata_never_relabels_history_or_unknown_source_as_new(renewal,fault):
    root,source,options,old=renewal
    path=source/'.microcap_index_cache/security_meta/600000.json';new=json.loads(path.read_bytes())
    if fault=='historical_st':new['st_intervals']=[dict(start='2026-11-05',end=None)]
    elif fault=='lifecycle':new['list_date']='2001-01-01'
    elif fault=='unknown_field':new['new_evidence_override']=True
    elif fault=='unresolved':new['current_st_history_resolved']=False
    elif fault=='wrong_archive':new['st_notice_archive_sha256']='c'*64
    elif fault=='gap':options['consumed']['600000']='2026-11-06'
    else:options['previous_fingerprint']['sha256']='0'*64
    path.write_bytes(json.dumps(new).encode())
    with pytest.raises(ValueError):m.plan_metadata_renewal(root,source,**options)
    assert all((root/'.microcap_index_cache/security_meta'/(s+'.json')).read_bytes()==raw for s,raw in old.items())


def test_genuine_post_consumption_st_entry_does_not_rewrite_old_days(renewal):
    root,source,options,*_=renewal
    path=source/'.microcap_index_cache/security_meta/600000.json';new=json.loads(path.read_bytes())
    new['st_intervals']=[dict(start='2026-11-06',end=None)];path.write_bytes(json.dumps(new).encode())
    result=m.plan_metadata_renewal(root,source,**options)
    assert result['changes']['600000']['protected_through']=='2026-11-05'
