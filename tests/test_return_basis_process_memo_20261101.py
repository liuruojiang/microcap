"""Real small-source replay plus cache invalidation fault injection; no full universe."""
from pathlib import Path
import json
import pytest
from scripts import realtime_state_bundle as state
from scripts import stock_return_pipeline as pipeline
from test_stock_return_basis_transport import prepare

@pytest.fixture
def real(tmp_path,monkeypatch):
    state._RETURN_BASIS_SUCCESS_MEMO.clear()
    anchor,cache=prepare(tmp_path,monkeypatch)
    original=pipeline.verify_return_basis_transport;calls=[]
    def counted(root,day,symbols):
        calls.append(tuple(symbols));return original(root,day,symbols)
    monkeypatch.setattr(pipeline,'verify_return_basis_transport',counted)
    return tmp_path,cache,anchor,calls

def run(real):return state._verify_return_basis_transport_memoized(real[0],real[2],['000001'])

def test_real_identical_bytes_reuse_only_in_process_and_report_is_not_mutable(real):
    result=run(real);result['verified_count']=999
    again=run(real)
    assert again['verified_count']==1 and len(real[3])==1
    state._RETURN_BASIS_SUCCESS_MEMO.clear();run(real)
    assert len(real[3])==2

@pytest.mark.parametrize('name', ['raw','manifest','candidate','factor','nested_status','master','authority'])
def test_any_actual_dependency_change_rechecks_and_failure_is_not_cached(real,name):
    run(real);root,cache,anchor,calls=real
    paths={'raw':cache/'prices_raw/000001.csv','manifest':cache/'return_basis/manifests/000001.json','candidate':cache/'return_basis/prices_adj/000001.csv','factor':cache/'return_basis/sources/000001/qfq.js','nested_status':cache/'trade_status/query.json','master':cache/'security_master.csv','authority':root/'outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_meta.json'}
    path=paths[name];path.parent.mkdir(parents=True,exist_ok=True)
    if name=='authority':path.write_text('{"return_basis_consumption":{}}')
    elif name=='raw':path.write_text(path.read_text().replace('close_raw','invalid_column'))
    elif name=='master':path.write_text(path.read_text().replace('000001','000002'))
    else:path.write_bytes(path.read_bytes()+b'corrupt')
    with pytest.raises((ValueError,KeyError)):run(real)
    assert len(calls)==2 and not state._RETURN_BASIS_SUCCESS_MEMO


def test_added_deleted_file_each_invalidates_even_if_not_used_by_numeric_rule(real):
    run(real);path=real[1]/'return_basis/extra.json';path.write_bytes(b'unchanged-rule-extra')
    run(real);path.unlink();run(real)
    assert len(real[3])==3


def test_symbol_lock_forces_full_verifier_and_never_caches_failure(real):
    run(real);lock=real[1]/'return_basis/locks/000001.lock';lock.write_bytes(b'active')
    with pytest.raises(ValueError):run(real)
    assert len(real[3])==2 and not state._RETURN_BASIS_SUCCESS_MEMO
    lock.unlink();run(real);assert len(real[3])==3


def test_calendar_change_forces_real_reconstruction(real,monkeypatch):
    run(real);previous=pipeline.sessions_for_day
    monkeypatch.setattr(pipeline,'sessions_for_day',lambda day:previous(day)[:-1])
    with pytest.raises(ValueError):run(real)
    assert len(real[3])==2 and not state._RETURN_BASIS_SUCCESS_MEMO


def test_source_module_bytes_change_invalidates(real,monkeypatch):
    path=real[0]/'pipeline_source.py';path.write_bytes(Path(pipeline.__file__).read_bytes())
    monkeypatch.setattr(pipeline,'__file__',str(path));run(real)
    path.write_bytes(path.read_bytes()+b'\n# source changed\n');run(real)
    assert len(real[3])==2


def test_mutation_during_success_never_enters_cache(real,monkeypatch):
    original=pipeline.verify_return_basis_transport
    def mutate(root,anchor,symbols):
        result=original(root,anchor,symbols)
        (real[1]/'return_basis/appeared.json').write_text('{}')
        return result
    monkeypatch.setattr(pipeline,'verify_return_basis_transport',mutate)
    with pytest.raises(RuntimeError,match='changed during full verification'):run(real)
    assert not state._RETURN_BASIS_SUCCESS_MEMO


def test_duplicate_requested_symbols_cannot_reuse_success(real):
    run(real)
    with pytest.raises(ValueError):state._verify_return_basis_transport_memoized(real[0],real[2],['000001','000001'])
    assert len(real[3])==2 and not state._RETURN_BASIS_SUCCESS_MEMO


def test_change_between_reuse_hashes_forces_full_verifier(real,monkeypatch):
    run(real);original=state._return_basis_input_fingerprint;count=0
    def change_after_hash(*args):
        nonlocal count
        result=original(*args);count+=1
        if count==1:(real[1]/'return_basis/changed_during_hash.json').write_text('{}')
        return result
    monkeypatch.setattr(state,'_return_basis_input_fingerprint',change_after_hash)
    run(real)
    assert len(real[3])==2
