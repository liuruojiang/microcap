"""Frozen native fixture equivalence and tampering; not current market evidence."""
from pathlib import Path
from datetime import datetime,timezone,timedelta
import gzip,json,shutil
import copy
import pytest

from scripts import verified_d5_basis_cache as m
from scripts import stock_return_pipeline as pipeline
from scripts.stock_return_pipeline import ReturnBasisError
from test_stock_return_basis_transport import prepare


def test_new_cache_reader_never_expands_the_frozen_d4_authorization():
    from scripts import realtime_state_bundle as state
    from scripts.stock_input_contract import source_fingerprint
    root=Path(__file__).resolve().parents[1]
    old=json.loads((root/'tests/fixtures/validation_source_identity_20261104.json').read_bytes())['stock_input_evidence_fingerprint']
    prior=copy.deepcopy(old)
    prior['code'].update({n:pair[1] for n,pair in state.SEMANTIC_TAIL_SOURCE_REVIEW['changes'].items()})
    assert state.stock_input_evidence_matches_validated_source(old,prior)
    prior['code']['scripts/stock_return_pipeline.py']=source_fingerprint(root)['scripts/stock_return_pipeline.py']
    assert not state.stock_input_evidence_matches_validated_source(old,prior)


@pytest.fixture
def reference(tmp_path,monkeypatch):
    anchor,cache=prepare(tmp_path,monkeypatch)
    native=pipeline.verify_return_basis_transport(tmp_path,anchor,['000001'])
    assert native['verified_count']==1 and native['read_only']
    manifest=json.loads((cache/'return_basis/manifests/000001.json').read_bytes())
    inputs={p.relative_to(tmp_path).as_posix():m.sha(p.read_bytes()) for p in cache.rglob('*') if p.is_file()}
    trade={n[len('.microcap_index_cache/'):]:h for n,h in inputs.items() if n.startswith('.microcap_index_cache/trade_status/')}
    for name in ('scripts/verified_d5_basis_cache.py','scripts/stock_return_pipeline.py'):
        path=tmp_path/name;path.parent.mkdir(parents=True,exist_ok=True)
        shutil.copyfile(Path(__file__).resolve().parents[1]/name,path)
    payload=dict(schema='verified_d5_basis_source_equivalence_v1',source_bundle_sha256='synthetic-frozen-fixture',
        runtime=m.runtime(),master_sha256=m.sha((cache/'security_master.csv').read_bytes()),math_files={},
        native_verified_at=datetime.now(timezone.utc).isoformat(),
        records={'000001':dict(manifest=manifest,inputs=inputs,trade_status_files=trade)})
    path=tmp_path/m.REFERENCE;path.parent.mkdir(parents=True,exist_ok=True)
    def save():
        raw=gzip.compress(json.dumps(payload).encode(),mtime=0);path.write_bytes(raw)
        monkeypatch.setattr(m,'REFERENCE_SHA256',m.sha(raw))
    monkeypatch.setattr(m,'_EXPECTED_RECORDS',1)
    monkeypatch.setattr(m,'SEED_SHA256','synthetic-frozen-fixture')
    save()
    return tmp_path,cache,anchor,payload,path,save,native


def test_cache_reuses_only_exact_native_verified_inputs_without_reconstruction(reference,monkeypatch):
    root,cache,anchor,payload,path,save,native=reference
    ref=m.load_reference(root,(cache/'security_master.csv').read_bytes())
    sessions=[d.isoformat() for d in pipeline.sessions_for_day(None)]
    result=ref.try_symbol('000001',payload['records']['000001']['manifest'],sessions,'1991-04-03',None)
    assert result and result['verification_method']=='sealed_native_verified_d5_source_equivalence'
    assert all(result[k]==native['symbols'][0][k] for k in native['symbols'][0])
    ref.finish()
    report=pipeline.verify_return_basis_transport(root,anchor,['000001'])
    assert report['sealed_source_equivalence_count']==1 and report['freshly_reconstructed_count']==0


@pytest.mark.parametrize('kind',['raw','source','candidate','status','manifest'])
def test_changed_input_never_inherits_the_old_verdict(reference,kind):
    root,cache,anchor,payload,path,save,*_=reference
    names=dict(raw='prices_raw/000001.csv',source='return_basis/sources/000001/qfq.js',
        candidate='return_basis/prices_adj/000001.csv',status='trade_status/query.json',
        manifest='return_basis/manifests/000001.json')
    target=cache/names[kind]
    if kind=='manifest':
        changed=json.loads(target.read_bytes());changed['rows']+=1;target.write_bytes(json.dumps(changed).encode())
    else:target.write_bytes(target.read_bytes()+b' ')
    ref=m.load_reference(root,(cache/'security_master.csv').read_bytes())
    sessions=[d.isoformat() for d in pipeline.sessions_for_day(None)]
    assert ref.try_symbol('000001',payload['records']['000001']['manifest'],sessions,'1991-04-03',None) is None
    with pytest.raises(ReturnBasisError):pipeline.verify_return_basis_transport(root,anchor,['000001'])


def test_changed_calendar_cannot_reuse_a_previous_lookup_for_the_same_cutoff(reference):
    root,cache,anchor,payload,path,save,*_=reference
    ref=m.load_reference(root,(cache/'security_master.csv').read_bytes())
    sessions=[d.isoformat() for d in pipeline.sessions_for_day(None)];manifest=payload['records']['000001']['manifest']
    assert ref.try_symbol('000001',manifest,sessions,'1991-04-03',None)
    assert ref.try_symbol('000001',manifest,sessions[1:],'1991-04-03',None) is None


@pytest.mark.parametrize('kind',['runtime','master','future_clock','math'])
def test_incompatible_environment_retains_the_full_native_route(reference,monkeypatch,kind):
    root,cache,anchor,payload,path,save,*_=reference
    if kind=='runtime':payload['runtime']['python']='different'
    elif kind=='master':payload['master_sha256']='0'*64
    elif kind=='future_clock':payload['native_verified_at']=(datetime.now(timezone.utc)+timedelta(days=1)).isoformat()
    else:
        p=root/'math.py';p.write_bytes(b'current math');payload['math_files']={'math.py':'0'*64}
    save()
    assert m.load_reference(root,(cache/'security_master.csv').read_bytes()) is None
    result=pipeline.verify_return_basis_transport(root,anchor,['000001'])
    assert result['sealed_source_equivalence_count']==0 and result['freshly_reconstructed_count']==1


def test_tampered_sealed_reference_is_an_error_even_if_other_inputs_are_unchanged(reference):
    root,cache,anchor,payload,path,save,*_=reference
    path.write_bytes(path.read_bytes()+b' ')
    with pytest.raises(ValueError,match='reference bytes changed'):
        m.load_reference(root,(cache/'security_master.csv').read_bytes())


def test_sources_or_reader_changed_during_reuse_are_not_certified(reference):
    root,cache,anchor,payload,path,save,*_=reference
    ref=m.load_reference(root,(cache/'security_master.csv').read_bytes())
    sessions=[d.isoformat() for d in pipeline.sessions_for_day(None)]
    assert ref.try_symbol('000001',payload['records']['000001']['manifest'],sessions,'1991-04-03',None)
    (cache/'prices_raw/000001.csv').write_bytes(b'changed after initial hash')
    with pytest.raises(ValueError,match='inputs changed'):ref.finish()


def test_runtime_replacement_cannot_inherit_the_reference(reference,monkeypatch):
    root,cache,anchor,*_=reference
    monkeypatch.setattr(pipeline,'build_ratio_return_basis',lambda **k:(_ for _ in ()).throw(ValueError('injected current builder')))
    assert m.load_reference(root,(cache/'security_master.csv').read_bytes()) is None
    with pytest.raises(ReturnBasisError,match='injected current builder'):
        pipeline.verify_return_basis_transport(root,anchor,['000001'])
