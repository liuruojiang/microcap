"""Explicit source boundary and candidate-scope counterexamples; synthetic inputs."""
from contextlib import ExitStack
from pathlib import Path
import json
from types import SimpleNamespace

import pytest

from scripts import scoped_native_consumer as m
from scripts import scoped_source_consumer as s
from scripts import top100_delivery as delivery
from test_st_notice_refresh import seed, Session
from scripts import st_notice_refresh as notice


def put(path, raw):
    path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(raw)


def test_default_native_refresh_has_no_scoped_adoption_or_hidden_side_effect(tmp_path):
    assert m._CONTEXT is None
    assert m.reuse_verified_sources(tmp_path,['600000'],'2026-11-09') is False
    assert not list(tmp_path.iterdir())


@pytest.fixture
def lazy(tmp_path,monkeypatch):
    root=tmp_path/'native';stage=tmp_path/'staged';names=s.mutable_files('600000')
    old={name:b'old' for name in names};files={}
    for name in names:
        put(root/name,b'old');put(stage/name,b'new');files[name]=s.sha(b'new')
    plan=dict(root=root,staged_root=stage,originals=old,mutable=set(names),files=files,
        target='2026-11-09',symbols=['600000'],consumed={'600000':'2026-11-05'})
    with ExitStack() as stack:
        context=dict(root=root,target='2026-11-09',plan=plan,stack=stack,adopted=False)
        monkeypatch.setattr(m,'_CONTEXT',context)
        yield context


def test_sources_are_adopted_only_when_original_native_refresh_requests_them(lazy):
    root=lazy['root']
    assert not lazy['adopted']
    assert all((root/name).read_bytes()==b'old' for name in lazy['plan']['files'])
    assert m.reuse_verified_sources(root,['600000'],'2026-11-09')
    assert lazy['adopted']
    assert all((root/name).read_bytes()==b'new' for name in lazy['plan']['files'])


@pytest.mark.parametrize('fault',['root','date','issuer'])
def test_wrong_native_request_is_rejected_before_any_adoption(lazy,fault,tmp_path):
    root=lazy['root'];symbols=['600000'];target='2026-11-09'
    if fault=='root':root=tmp_path/'other'
    elif fault=='date':target='2026-11-10'
    else:symbols=['600001']
    with pytest.raises(ValueError,match='outside the authenticated scope'):
        m.reuse_verified_sources(root,symbols,target)
    assert not lazy['adopted']
    assert all((lazy['root']/name).read_bytes()==b'old' for name in lazy['plan']['files'])


def test_original_notice_chain_is_located_without_rewriting_receipts(seed):
    root,pointer,raw=seed;old=json.loads(raw)
    notice.refresh_notice_archive(root=root,target='2026-10-30',session=Session())
    notice.refresh_notice_archive(root=root,target='2026-10-31',session=Session())
    before={p:p.read_bytes() for p in (root/notice.PREFIX).glob('refresh_*/proof.json')}
    result,specs=m.original_notice_chain(root,[old['manifest_path'],old['manifest_sha256']],'2026-10-31')
    assert result['receipt_count']==len(specs)==2
    assert result['original_clocks_retained']
    assert all(p.read_bytes()==b for p,b in before.items())


def test_forged_mixed_metadata_marker_cannot_grant_transport(tmp_path):
    put(tmp_path/m.ACTIVE,b'{}')
    put(tmp_path/'outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_meta.json',b'{}')
    with pytest.raises(ValueError,match='binding is absent or changed'):
        m.scoped_transport_files(tmp_path)


def test_old_native_core_gate_rejection_precedes_any_source_planning(tmp_path):
    proxy=tmp_path/'proxy.json';proxy.write_bytes(b'{}')
    base=SimpleNamespace(proxy_meta_matches_execution_model=lambda _:False)
    with pytest.raises(ValueError,match='before adoption'):
        s.consumed_boundaries(tmp_path,base,SimpleNamespace(),{'proxy_meta':proxy})
    assert list(tmp_path.iterdir())==[proxy]


def test_candidate_identity_does_not_call_or_satisfy_live_main_gate(tmp_path,monkeypatch):
    candidate='a'*40
    monkeypatch.setattr(delivery.subprocess,'check_output',lambda *a,**k:candidate+'\n')
    seen=[]
    monkeypatch.setattr(delivery,'_verify_release_files',lambda root,ref:seen.append(ref) or ref)
    assert delivery.verify_candidate_release(tmp_path,candidate)==candidate
    assert seen==[candidate]
    with pytest.raises(ValueError,match='immutable full commit'):
        delivery.verify_candidate_release(tmp_path,'main')
    with pytest.raises(ValueError,match='requested commit'):
        delivery.verify_candidate_release(tmp_path,'b'*40)


def test_scoped_config_cannot_enter_default_formal_release(tmp_path,monkeypatch):
    monkeypatch.setattr(delivery,'verify_release',lambda _:pytest.fail('explicit candidate required before any source operation'))
    with pytest.raises(ValueError,match='explicit immutable candidate'):
        delivery._refresh_all_unlocked(tmp_path,scoped_sources={})
