"""Synthetic replay/lineage counterexamples; no live-data acceptance claims."""
import copy
import json

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
