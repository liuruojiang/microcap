"""Synthetic replay/lineage counterexamples; no live-data acceptance claims."""
import copy
import base64
import gzip
import json
from pathlib import Path

import pandas as pd
import pytest

from test_scoped_forward_consumption import forward
from scripts import scoped_same_anchor as m
from scripts import scoped_source_consumer as sources
from scripts import scoped_forward_consumption as consumption


@pytest.fixture
def same(forward):
    root,meta,proof,data,requested,base=forward
    prior=copy.deepcopy(meta);prior['core_params']={'native_earned_fixture':'old'}
    raw=sources.encode(prior);path=root/consumption.PREFIX/'same/prior.json'
    sources.atomic(path,raw)
    marker=dict(original_proxy=dict(path=path.relative_to(root).as_posix(),sha256=sources.sha(raw)),source_bundle_sha256='a'*64)
    proxy=copy.deepcopy(prior);proxy['core_params']={'native_earned_fixture':'new'}
    receipt=dict(schema='native_same_anchor_replay_v1',status='passed',target=proxy['end_date'],
        earned_core_params=proxy['core_params'],previous_proxy_sha256=sources.sha(raw),
        source_bundle_sha256='a'*64,economic_csv_bytes_changed=False,
        scope=proof['segments'][0]['symbols'],native_inputs=copy.deepcopy(data))
    path=root/consumption.PREFIX/'same/replay.json'
    body=sources.encode(receipt);sources.atomic(path,body)
    proxy['same_anchor_native_revalidation']=dict(path=path.relative_to(root).as_posix(),sha256=sources.sha(body))
    paths={key:root/consumption.FILES[role] for key,role in (
        ('proxy_effective_members','effective'),('proxy_members','members'),('proxy_turnover','turnover'))}
    return root,base,proxy,marker,paths,receipt


def test_same_close_new_observation_requires_a_real_native_replay_receipt(same):
    root,base,proxy,marker,paths,receipt=same
    files=m.validate(root,base,proxy,marker,paths)
    assert files=={proxy['same_anchor_native_revalidation']['path']:proxy['same_anchor_native_revalidation']['sha256']}


@pytest.mark.parametrize('fault',['receipt_bytes','producer','prior','core','dates','initial','target','cap','terminal'])
def test_same_anchor_cannot_relabel_old_evidence_or_change_consumed_state(same,fault):
    root,base,proxy,marker,paths,receipt=same
    spec=proxy['same_anchor_native_revalidation'];path=root/spec['path']
    if fault=='receipt_bytes':path.write_bytes(b'corrupt')
    else:
        receipt=copy.deepcopy(receipt)
        if fault=='producer':receipt['source_bundle_sha256']='b'*64
        elif fault=='prior':receipt['previous_proxy_sha256']='b'*64
        elif fault=='core':receipt['earned_core_params']={}
        elif fault=='dates':receipt['native_inputs']['dates']=receipt['native_inputs']['dates'][1:]
        elif fault=='initial':receipt['native_inputs']['initial'][0]='000300'
        elif fault=='target':receipt['native_inputs']['targets']['2026-10-29'][0]='000300'
        elif fault=='cap':receipt['native_inputs']['caps']['2026-10-29']['000200']+=10000
        elif fault=='terminal':receipt['native_inputs']['terminal'][0]='000300'
        raw=sources.encode(receipt);path.write_bytes(raw);spec['sha256']=sources.sha(raw)
    with pytest.raises((ValueError,AssertionError)):
        m.validate(root,base,proxy,marker,paths)


def test_no_tail_never_silently_grants_a_same_anchor_full_rebuild(tmp_path):
    with pytest.raises(ValueError,match='not an implicit full rebuild'):
        m.window(tmp_path,{'end_date':'2026-10-29','return_basis_consumption':{
            'revision':'stock_return_consumption_v1','full_rebuild_anchor':'2026-10-29','tail_extensions':[]}},[])


@pytest.fixture
def actual_advance(tmp_path,monkeypatch):
    """Original Nov9 state and failed Nov10 native calculation; no formal gate."""
    import microcap_top100_mom16_biweekly_live_v2_0 as native
    import scripts.exchange_calendar as calendar
    packet=json.loads(gzip.decompress((Path(__file__).parent/'fixtures/same_anchor_advance_20261110/native-state.json.gz').read_bytes()))
    for name,encoded in packet['files'].items():
        raw=base64.b64decode(encoded)
        assert sources.sha(raw)==packet['sha256'][name]
        sources.atomic(sources.bound(tmp_path,name),raw)
    proxy=json.loads(sources.bound(tmp_path,packet['meta']).read_bytes())
    owner_raw=sources.bound(tmp_path,packet['owner']['path']).read_bytes();owner=json.loads(owner_raw)
    archived='.microcap_index_cache/scoped_native_metadata/next-day/original_active_marker.json'
    sources.atomic(sources.bound(tmp_path,archived),owner_raw)
    owner_spec=dict(path=archived,sha256=sources.sha(owner_raw))
    future_raw=base64.b64decode(packet['failed_next_day_meta'])
    assert sources.sha(future_raw)==packet['failed_next_day_meta_sha256']
    future=json.loads(future_raw)
    # These are the exact exchange sessions captured by the original native
    # calculation. No network, contemporary source refresh or PIT claim.
    monkeypatch.setattr(calendar,'sessions_for_day',lambda *a:tuple(pd.Timestamp(d).date() for d in packet['dates']))
    paths={key:tmp_path/consumption.FILES[role] for key,role in (
        ('proxy_effective_members','effective'),('proxy_members','members'),('proxy_turnover','turnover'))}
    return tmp_path,native.base_mod,proxy,owner,owner_spec,future,paths


def test_real_same_anchor_to_next_day_retires_only_activity_and_keeps_direct_archive_edge(actual_advance):
    root,base,prior,owner,owner_spec,future,paths=actual_advance
    original_spec=copy.deepcopy(prior['same_anchor_native_revalidation'])
    assert m.validate(root,base,prior,owner,paths)=={original_spec['path']:original_spec['sha256']}
    assert prior['end_date']=='2026-11-09' and future['end_date']=='2026-11-10'
    assert future['same_anchor_native_revalidation']==original_spec
    # The exact bug: the next day's actual native candidate inherited Nov9's
    # active replay. Strict validation must continue to reject that binding.
    with pytest.raises(ValueError,match='binding differs'):
        m.validate(root,base,future,owner,paths)
    other_fields={k:v for k,v in future.items() if k!='same_anchor_native_revalidation'}
    m.retire_on_advance(prior,future,future['end_date'])
    assert future==other_fields and prior['same_anchor_native_revalidation']==original_spec
    assert m.validate(root,base,future,owner,paths)=={}
    files=m.archived_receipt_files(root,prior,owner_spec)
    assert files[original_spec['path']]==original_spec['sha256']
    assert files[owner_spec['path']]==owner_spec['sha256']
    assert files[owner['original_proxy']['path']]==owner['original_proxy']['sha256']
    # A later period's holdings/turnover cannot be substituted into the old
    # validation. Archival acceptance binds old bytes and its original owner.
    paths['proxy_effective_members'].write_bytes(b'later-period members are not the old receipt input')
    paths['proxy_turnover'].write_bytes(b'later-period turnover is not the old receipt input')
    assert m.archived_receipt_files(root,prior,owner_spec)==files


@pytest.mark.parametrize('fault',['receipt_bytes','owner_bytes','owner_role','target','core','bundle','prior'])
def test_archival_replay_cannot_relabel_old_native_target_core_or_source(actual_advance,fault):
    root,base,proxy,owner,owner_spec,future,paths=actual_advance
    spec=proxy['same_anchor_native_revalidation'];path=sources.bound(root,spec['path'])
    if fault=='receipt_bytes':path.write_bytes(path.read_bytes()+b' ')
    elif fault=='owner_bytes':sources.bound(root,owner_spec['path']).write_bytes(b'changed old owner')
    elif fault=='owner_role':proxy['scoped_metadata_adoption']['path']='outputs/unrelated-marker.json'
    else:
        receipt=json.loads(path.read_bytes())
        if fault=='target':receipt['target']='2026-11-10'
        elif fault=='core':receipt['earned_core_params']={}
        elif fault=='bundle':receipt['source_bundle_sha256']='0'*64
        else:receipt['previous_proxy_sha256']='0'*64
        raw=sources.encode(receipt);path.write_bytes(raw);spec['sha256']=sources.sha(raw)
    with pytest.raises(ValueError):m.archived_receipt_files(root,proxy,owner_spec)


def test_same_day_replay_is_not_retired_and_unexplained_new_spec_is_rejected(actual_advance):
    root,base,proxy,owner,owner_spec,future,paths=actual_advance
    before=copy.deepcopy(proxy)
    m.retire_on_advance(proxy,proxy,proxy['end_date'])
    assert proxy==before
    future['same_anchor_native_revalidation']['sha256']='0'*64
    with pytest.raises(ValueError,match='unexplained'):
        m.retire_on_advance(proxy,future,future['end_date'])
