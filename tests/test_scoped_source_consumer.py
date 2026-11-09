"""Synthetic transaction and consumed-history counterexamples, not market evidence."""
import json
from pathlib import Path

import pytest

from scripts import scoped_source_consumer as m


def put(path, raw):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)


@pytest.mark.parametrize('old,new,key,column,carried,allowed', [
    ('date,close_raw\n2026-10-29,10\n', 'date,close_raw\n2026-10-29,11\n', 'date','close_raw',False,False),
    ('date,close_raw\n2026-10-29,10\n', 'date,close_raw\n2026-10-29,10\n2026-11-09,11\n', 'date','close_raw',False,True),
    ('date,close_raw\n2026-10-29,10\n', 'date,close_raw\n2026-10-28,9\n2026-10-29,10\n', 'date','close_raw',False,False),
    ('change_date,total_shares_10k\n2020-01-01,100\n', 'change_date,total_shares_10k\n2020-01-01,100\n2026-11-06,200\n', 'change_date','total_shares_10k',True,True),
    ('change_date,total_shares_10k\n2020-01-01,100\n', 'change_date,total_shares_10k\n2020-01-01,100\n2026-10-29,200\n', 'change_date','total_shares_10k',True,False),
    ('date,close_raw\n2026-10-29,10\n', 'date,close_raw\n2026-10-29,NaN\n', 'date','close_raw',False,False),
    ('date,close_raw\n2026-10-29,10\n', 'date,close_raw\n2026-10-29,10\n2026-10-29,10\n', 'date','close_raw',False,False),
])
def test_raw_and_carried_share_history_protect_actual_consumption(old,new,key,column,carried,allowed):
    run=lambda:m.preserve_csv_history(old.encode(),new.encode(),key,column,'2026-11-05',carried=carried)
    if allowed:run()
    else:
        with pytest.raises(ValueError):run()


def test_consumed_return_parity_accepts_only_constant_level_normalization():
    old=b'date,close_adj\n2026-10-29,10\n2026-11-05,11\n'
    same=b'date,close_adj\n2026-10-29,20\n2026-11-05,22\n2026-11-09,25\n'
    m.preserve_return_history(old,same,'2026-11-05')
    changed=same.replace(b'22\n',b'23\n')
    with pytest.raises(AssertionError,match='consumed stock returns'):
        m.preserve_return_history(old,changed,'2026-11-05')
    with pytest.raises(ValueError,match='coverage'):
        m.preserve_return_history(old,same,'2026-11-09')


@pytest.fixture
def candidate(tmp_path):
    root=tmp_path/'consumer';stage=tmp_path/'stage';root.mkdir();stage.mkdir()
    master=m.CACHE+'security_master.csv'
    put(root/master,b'fixture-lifecycle-identity\n')
    symbols=[str(600000+n) for n in range(300)]
    files={};originals={}
    for s in symbols:
        names=m.mutable_files(s)
        bodies=[b'date,close_raw\n2026-10-29,10\n2026-11-05,11\n',
            b'change_date,total_shares_10k\n2020-01-01,100\n',
            b'{"query_end":"2026-10-29"}',b'{}',b'{}',
            b'date,close_adj\n2026-10-29,10\n2026-11-05,11\n',b'old-q',b'old-h',b'{}',b'{}']
        for name,raw in zip(names,bodies):
            put(root/name,raw);originals[name]=raw
            new=raw+b' ' if name.endswith('.js') else raw
            put(stage/name,new);files[name]=m.sha(new)
    immutable=m.CACHE+'return_basis/bundles/600000/'+'a'*64+'/raw.csv'
    put(stage/immutable,b'fixture immutable raw evidence');files[immutable]=m.sha((stage/immutable).read_bytes())
    manifest=dict(schema=m.SCHEMA,source_inputs_only=True,native_ST_refresh_required_at_consumer=True,
        final_strategy_delivery=False,symbols=symbols,files=files,target='2026-11-09',master_sha256=m.sha((root/master).read_bytes()))
    observed_path=root/'outputs/old-proxy-meta.json';put(observed_path,b'original native consumption authority')
    return root,stage,manifest,{s:'2026-10-29' for s in symbols},{observed_path:observed_path.read_bytes()},originals,immutable


def plan(candidate):
    root,stage,manifest,consumed,observed,*_=candidate
    return m.plan_adoption(root,stage,manifest,consumed=consumed,observed=observed)


@pytest.fixture
def transaction(tmp_path):
    root=tmp_path/'consumer';stage=tmp_path/'stage'
    files={};originals={};mutable=set()
    for n in range(2):
        name=m.CACHE+'trade_status/'+str(600000+n)+'.json'
        put(root/name,b'old');put(stage/name,b'new')
        files[name]=m.sha(b'new');originals[name]=b'old';mutable.add(name)
    evidence=m.CACHE+'trade_status/sources/fixture.raw'
    put(stage/evidence,b'raw evidence');files[evidence]=m.sha(b'raw evidence')
    authority='outputs/proxy.json';put(root/authority,b'old native gate');originals[authority]=b'old native gate'
    return dict(root=root,staged_root=stage,files=files,originals=originals,mutable=mutable,
        symbols=['600000','600001'],target='2026-11-09',consumed={'600000':'2026-11-05','600001':'2026-10-29'})


def test_verified_late_adoption_commits_sources_without_claiming_strategy_delivery(transaction):
    p=transaction
    assert all((p['root']/name).read_bytes()==raw for name,raw in p['originals'].items())
    with m.adoption_transaction(p) as work:
        assert all(m.sha((p['root']/name).read_bytes())==digest for name,digest in p['files'].items())
    receipt=json.loads((work/'receipt.json').read_bytes())
    assert receipt['status']=='native_sources_adopted' and receipt['final_strategy_delivery'] is False
    assert not (p['root']/m.CACHE/'scoped_native_adoption.lock').exists()


def test_native_readback_failure_restores_all_old_mutable_bytes_and_keeps_evidence(transaction):
    p=transaction
    with pytest.raises(RuntimeError,match='native downstream rejection'):
        with m.adoption_transaction(p) as work:
            raise RuntimeError('native downstream rejection')
    assert all((p['root']/name).read_bytes()==raw for name,raw in p['originals'].items())
    assert (p['root']/(m.CACHE+'trade_status/sources/fixture.raw')).is_file()
    assert json.loads((work/'failure_receipt.json').read_bytes())['status']=='rolled_back'


def test_changed_old_gate_authority_cannot_adopt_even_first_pointer(transaction):
    p=transaction;authority=p['root']/'outputs/proxy.json';authority.write_bytes(b'other writer')
    with pytest.raises(ValueError,match='Old native state changed'):
        with m.adoption_transaction(p):pytest.fail('must reject before native stage')
    assert all((p['root']/name).read_bytes()==p['originals'][name] for name in p['mutable'])


def test_staged_source_changed_after_planning_rolls_back_any_prior_installs(transaction):
    p=transaction;name=list(p['files'])[1];(p['staged_root']/name).write_bytes(b'replaced')
    with pytest.raises(ValueError,match='staged source changed'):
        with m.adoption_transaction(p):pytest.fail('must reject')
    assert all((p['root']/name).read_bytes()==p['originals'][name] for name in p['mutable'])


def test_complete_300_issuer_plan_is_read_only_and_preserves_cold_candidate_consumption(candidate):
    p=plan(candidate)
    assert len(p['symbols'])==300
    assert all((p['root']/name).read_bytes()==raw for name,raw in candidate[5].items())


@pytest.mark.parametrize('fault',['outside','missing','tamper','newer','share_consumed'])
def test_source_plan_rejects_unauthorized_or_history_changing_inputs(candidate,fault):
    root,stage,manifest,consumed,observed,*_=candidate
    if fault=='outside':
        name=m.CACHE+'trade_status/600999.json';m.atomic(stage/name,b'{}');manifest['files'][name]=m.sha(b'{}')
    elif fault=='missing':del manifest['files'][m.mutable_files('600000')[0]]
    elif fault=='tamper':(stage/m.mutable_files('600000')[0]).write_bytes(b'changed')
    elif fault=='newer':
        (root/m.mutable_files('600000')[2]).write_bytes(b'{"query_end":"2026-11-10"}')
    else:
        consumed['600000']='2026-11-05';name=m.mutable_files('600000')[1]
        raw=(stage/name).read_bytes()+b'2026-11-03,200\n';(stage/name).write_bytes(raw);manifest['files'][name]=m.sha(raw)
    with pytest.raises(ValueError):plan(candidate)


@pytest.mark.parametrize('name',['../escape','/absolute','C:/escape','a\\b','a/../b'])
def test_source_paths_are_portable_and_bounded(tmp_path,name):
    with pytest.raises(ValueError):m.bound(tmp_path,name)
