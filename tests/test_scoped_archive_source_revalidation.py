"""Exact real M3 metadata/data, with isolated software transaction boundaries.

The transaction's large native gate is spied here, never represented as an
actual 5,438-stock production run. Real pack/readback runs remain deployment
acceptance. All original seven CSV bytes and the full 170,713-object manifest
are frozen from the published SHA-pinned M3 bundle.
"""
import base64
import copy
from datetime import date
import gzip
import io
import json
from pathlib import Path
import socket
import zipfile

import pytest
from scripts import scoped_archive_source_revalidation as a
from scripts import realtime_state_bundle as state, top100_delivery as delivery, top100_cloud_delivery as cloud

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    def blocked(*args,**kwargs):raise AssertionError('This test must never collect market data')
    monkeypatch.setattr(socket.socket,'connect',blocked)
    monkeypatch.setattr(socket,'create_connection',blocked)


@pytest.fixture(scope='module')
def frozen():
    path=Path(__file__).parent/'fixtures/archive_source_revalidation_m3_20261110.json.gz'
    packet=json.loads(gzip.decompress(path.read_bytes()))
    assert packet['metadata']==a.BASELINE
    def raw(name):
        obj=packet['objects'][name];result=base64.b64decode(obj['base64'],validate=True)
        assert a.digest(result)==obj['sha256'];return result
    packet['raw']=raw
    packet['entries']=json.loads(raw('state-integrity-manifest.json'))['files']
    return packet


def next_fingerprint():
    fp=copy.deepcopy(a.BASELINE['stock_fingerprint'])
    for name,(_,after) in a.CHANGES.items():fp['code'][name]=after
    return fp


def current_report(original):
    current=copy.deepcopy(original)
    for name,(_,after) in a.input_changes().items():current['inputs'][name]=after
    current['inputs'][a.MODULE]=delivery.sha(ROOT/a.MODULE)
    return current


@pytest.fixture
def transaction(tmp_path,monkeypatch,frozen):
    root=tmp_path/'native';root.mkdir()
    for name in (*a.BASELINE['economic_csvs'],a.PROXY):
        raw=frozen['raw']('proxy.json' if name==a.PROXY else name)
        path=root/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(raw)
    manifest=frozen['raw']('bundle-manifest.json');(root/delivery.MANIFEST).write_bytes(manifest)
    for name in (*a.input_changes(),a.MODULE):
        path=root/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes((ROOT/name).read_bytes())
    original=json.loads(manifest);report=current_report(original);calls=[]
    def inspect(where,day,**options):
        assert Path(where)==root and day==a.DATE
        saved=json.loads((root/delivery.MANIFEST).read_bytes())
        result=copy.deepcopy(report);a.annotate(root,result,saved,**options);return result
    def native_gate(where,session):
        calls.append(('archive',Path(where),session))
        assert Path(where)==root and session==date(2026,11,10)
        assert state.stock_input_evidence_matches_validated_source(a.BASELINE['stock_fingerprint'],next_fingerprint(),root=root)
        return {'ok':True,'errors':[],'archive_session':a.DATE,'current_signal_ready':False}
    def raw_identity(where,current):
        paths=[*a.BASELINE['economic_csvs'],a.PROXY,*a.input_changes(),a.MODULE]
        return {name:a.digest((root/name).read_bytes()) for name in paths}
    monkeypatch.setattr(delivery,'inspect_outputs',inspect)
    monkeypatch.setattr(delivery,'verify_release',lambda where:'f'*40)
    monkeypatch.setattr(delivery,'_recertification_raw_identity',raw_identity)
    monkeypatch.setattr(state,'validate_archive_state',native_gate)
    monkeypatch.setattr(a,'data_inventory',lambda where,current:frozen['entries'])
    return root,calls,report


def test_two_actual_manifest_forms_differ_only_in_original_pack_time(frozen):
    packed=a.original_manifest(frozen['raw']('bundle-manifest.json'))
    local=a.original_manifest(frozen['raw']('local-manifest.json'))
    assert [key for key in packed if packed[key]!=local[key]]==['verified_at']
    assert packed['release_sha']==local['release_sha']==a.RELEASE
    assert a.digest(a.encoded(a.BASELINE['stock_fingerprint']))==a.STOCK_IDENTITY


def test_real_complete_inventory_and_seven_csv_bytes(frozen):
    a.inventory_matches(frozen['entries'])
    entries={e['path']:e for e in frozen['entries']}
    assert len(entries)==170714
    for name,expected in a.BASELINE['economic_csvs'].items():
        raw=frozen['raw'](name)
        assert len(raw)==entries[name]['bytes'] and a.digest(raw)==expected==entries[name]['sha256']


@pytest.mark.parametrize('case',['remove','extra','raw_history','cold_share','clock','size','order'])
def test_even_self_consistent_whole_inventory_mutations_are_rejected(frozen,case):
    entries=list(frozen['entries'])
    index=next(i for i,e in enumerate(entries) if e['path'].startswith('.microcap_index_cache/prices_raw/'))
    if case=='remove':entries.pop(index)
    if case=='extra':entries.append(dict(path='.microcap_index_cache/fake',bytes=1,sha256='a'*64));entries.sort(key=lambda e:e['path'])
    if case in ('raw_history','size'):
        entries[index]=dict(entries[index]);entries[index]['sha256' if case=='raw_history' else 'bytes']='b'*64 if case=='raw_history' else 1
    if case=='cold_share':
        index=next(i for i,e in enumerate(entries) if e['path'].startswith('.microcap_index_cache/share_change/'))
        entries[index]={**entries[index],'sha256':'a'*64}
    if case=='clock':
        index=next(i for i,e in enumerate(entries) if 'receipt' in e['path'])
        entries[index]={**entries[index],'sha256':'c'*64}
    if case=='order':entries[0],entries[1]=entries[1],entries[0]
    with pytest.raises(ValueError,match='complete M3'):a.inventory_matches(entries)


@pytest.mark.parametrize('case',['valid','different_data','different_count','unknown_source','extra_source','wrong_date','changed_proxy'])
def test_source_exception_is_only_real_unchanged_m3_anchor(tmp_path,frozen,case):
    path=tmp_path/a.PROXY;path.parent.mkdir(parents=True);raw=frozen['raw']('proxy.json');path.write_bytes(raw)
    old=copy.deepcopy(a.BASELINE['stock_fingerprint']);new=next_fingerprint()
    if case=='different_data':new['sha256']='a'*64
    if case=='different_count':new['file_count']+=1
    if case=='unknown_source':new['code']['scripts/share_change_source.py']='a'*64
    if case=='extra_source':new['code']['scripts/stock_daily_inputs.py']='a'*64
    if case=='wrong_date':
        obj=json.loads(raw);obj['end_date']='2026-11-11';path.write_bytes(json.dumps(obj).encode())
    if case=='changed_proxy':path.write_bytes(raw+b' ')
    assert state.stock_input_evidence_matches_validated_source(old,new,root=tmp_path) is (case=='valid')


@pytest.mark.parametrize('case',['history','clock','extra_path','missing','wrong_code','added_code','date','scope'])
def test_source_only_transition_rejects_unreviewed_changes(frozen,case):
    original=json.loads(frozen['raw']('bundle-manifest.json'));current=current_report(original)
    if case=='history':current['artifacts'][next(iter(current['artifacts']))]='a'*64
    if case=='clock':current['streams'][next(iter(current['streams']))]['latest_date']='2026-11-11'
    if case=='extra_path':current['inputs']['scripts/unknown.py']='a'*64
    if case=='missing':current['inputs'].pop(next(iter(current['inputs'])))
    if case=='wrong_code':current['inputs']['scripts/share_change_source.py']='a'*64
    if case=='added_code':current['inputs'][a.MODULE]='not-a-sha'
    if case=='date':current['expected_date']='2026-11-11'
    if case=='scope':current['scope']='candidate'
    with pytest.raises(ValueError):a.require_transition(original,current)


def test_archive_source_transaction_keeps_all_bytes_and_real_producer(transaction,frozen):
    root,calls,current=transaction
    before={name:(root/name).read_bytes() for name in (*a.BASELINE['economic_csvs'],a.PROXY)}
    result=a.recertify(root,a.DATE)
    assert calls==[('archive',root,date(2026,11,10))]
    assert result['release_sha']=='f'*40 and result[a.ACTIVE]['original_release_sha']==a.RELEASE
    assert result['signal_ready'] is result['current_signal_ready'] is False
    assert result[a.ACTIVE]['original_manifest_utf8'].encode()==frozen['raw']('bundle-manifest.json')
    assert all((root/name).read_bytes()==raw for name,raw in before.items())
    assert not (root/delivery.LOCK).exists()
    assert a.recertify(root,a.DATE)['recertification_action']=='already_complete_revalidated'


@pytest.mark.parametrize('case',['native_failure','data_race','manifest_race','wrong_day','source_dirty','original_dirty'])
def test_failed_recertification_never_marks_success(transaction,monkeypatch,case):
    root,calls,current=transaction;original=(root/delivery.MANIFEST).read_bytes()
    if case=='native_failure':monkeypatch.setattr(state,'validate_archive_state',lambda *args:dict(ok=False,errors=['native failure']))
    if case=='data_race':
        original_gate=state.validate_archive_state
        def gate(*args):
            result=original_gate(*args);(root/next(iter(a.BASELINE['economic_csvs']))).write_bytes(b'changed');return result
        monkeypatch.setattr(state,'validate_archive_state',gate)
    if case=='manifest_race':
        original_gate=state.validate_archive_state
        def gate(*args):
            result=original_gate(*args);(root/delivery.MANIFEST).write_bytes(original+b' ');return result
        monkeypatch.setattr(state,'validate_archive_state',gate)
    if case=='source_dirty':(root/'scripts/share_change_source.py').write_bytes(b'unknown')
    if case=='original_dirty':(root/delivery.MANIFEST).write_bytes(original+b' ')
    with pytest.raises(ValueError):a.recertify(root,'2026-11-11' if case=='wrong_day' else a.DATE)
    if case!='manifest_race' and case!='original_dirty':assert (root/delivery.MANIFEST).read_bytes()==original
    assert not (root/delivery.LOCK).exists()


def test_manifest_commit_failure_restores_exact_original(transaction,monkeypatch):
    root,_,_=transaction;original=(root/delivery.MANIFEST).read_bytes()
    def broken(root,report):
        (root/delivery.MANIFEST).write_bytes(b'partial');raise OSError('write failed')
    monkeypatch.setattr(delivery,'write_manifest',broken)
    with pytest.raises(OSError):a.recertify(root,a.DATE)
    assert (root/delivery.MANIFEST).read_bytes()==original and not (root/delivery.LOCK).exists()


@pytest.mark.parametrize('field',['original_manifest_sha256','stock_fingerprint_sha256','data_inventory_sha256',
                                 'original_proxy_sha256','source_bundle_sha256','source_changes','validator_inputs'])
def test_receipt_tampering_is_not_accepted_after_manifest_rehash(transaction,field):
    root,_,_=transaction;result=a.recertify(root,a.DATE);receipt=copy.deepcopy(result[a.ACTIVE])
    receipt[field]='a'*64 if not isinstance(receipt[field],dict) else {}
    with pytest.raises(ValueError):a.checked_receipt(receipt)


def test_new_native_day_retires_constraints_preserving_flat_ancestry(transaction,monkeypatch):
    root,_,current=transaction;saved=a.recertify(root,a.DATE)
    future=copy.deepcopy(current);future['expected_date']='2026-11-11'
    future['inputs']['scripts/share_change_source.py']='b'*64
    future['artifacts'][next(iter(future['artifacts']))]='c'*64
    with pytest.raises(ValueError):a.annotate(root,copy.deepcopy(future),saved)
    a.annotate(root,future,saved,new_generation=True)
    assert a.ACTIVE not in future and future[a.PREVIOUS]==saved[a.ACTIVE]
    assert 'signal_ready' not in future or future['signal_ready'] is not False
    again=copy.deepcopy(future);a.annotate(root,again,future,new_generation=True)
    assert again[a.PREVIOUS]==saved[a.ACTIVE]
    assert a.ACTIVE not in again[a.PREVIOUS] and a.PREVIOUS not in again[a.PREVIOUS]
    assert a.pending_ancestry(saved)=={a.PREVIOUS:saved[a.ACTIVE]}


def test_normal_refresh_preserves_ancestry_when_native_refresh_fails(transaction,monkeypatch):
    root,_,current=transaction;saved=a.recertify(root,a.DATE)
    def fail(*args,**kwargs):
        status=json.loads((root/delivery.MANIFEST).read_bytes())
        assert status['status']=='refreshing' and status[a.PREVIOUS]==saved[a.ACTIVE]
        raise RuntimeError('no current source available')
    monkeypatch.setattr(state,'refresh_state',fail)
    with pytest.raises(RuntimeError):delivery.refresh_all(root)
    blocked=json.loads((root/delivery.MANIFEST).read_bytes())
    assert blocked['status']=='blocked' and blocked[a.PREVIOUS]==saved[a.ACTIVE]


def test_repacked_partial_archive_cannot_claim_the_full_m3_certificate(transaction,tmp_path):
    root,_,_=transaction;saved=a.recertify(root,a.DATE)
    manifest=json.dumps(saved).encode()
    content={'outputs/top100_delivery_manifest.json':manifest,'outputs/only-one.csv':b'not full data'}
    entries=[dict(path=k,bytes=len(v),sha256=a.digest(v)) for k,v in sorted(content.items())]
    bundle=tmp_path/'partial.zip'
    with zipfile.ZipFile(bundle,'w') as archive:
        for name,raw in content.items():archive.writestr(name,raw)
        archive.writestr(state.MANIFEST_NAME,json.dumps(dict(files=entries)))
    with zipfile.ZipFile(bundle) as archive,pytest.raises(ValueError,match='complete M3'):
        state._verify_bundle_manifest(archive)


def test_source_revalidation_module_is_transported_as_released_code():
    assert a.MODULE in delivery.RELEASE_FILES and a.MODULE in cloud.SOURCE_FILES
    from scripts.stock_input_contract import SOURCE_FILES
    assert len(SOURCE_FILES)==18 and a.MODULE not in SOURCE_FILES


@pytest.mark.parametrize('field,value',[('signal_ready',True),('current_signal_ready',True),
                                        ('archive_session','2026-11-11'),('expected_date','2026-11-11')])
def test_archive_certificate_cannot_be_relabelled_current(transaction,field,value):
    root,_,_=transaction;saved=a.recertify(root,a.DATE);saved[field]=value
    with pytest.raises(ValueError):a.checked_document(saved)


@pytest.mark.parametrize('producer', ['be89e87a0da728b27be0308d7c4c9e57a212b815',
                                    '706af7ea9186bdb9b3948738deb5a5afda7a331e',
                                    'f6970d665d5c7a39b37c866cf13dc3d4cb8fdcc5'])
def test_original_source_identity_is_preserved_under_exact_new_reader_pairs(monkeypatch,producer):
    from scripts import scoped_source_consumer as reader, stock_input_contract as contract
    current=next_fingerprint()['code'];old=copy.deepcopy(current)
    # Predecessor packages retain their actual P1 helper/parser fingerprints;
    # they cannot inherit the new M7 helper just by copying current code.
    for name,(before,_) in reader.LATER_QUOTE_READER.items():old[name]=before
    if producer!='f6970d665d5c7a39b37c866cf13dc3d4cb8fdcc5':
        old['scripts/st_notice_refresh.py']=reader.ARCHIVED_PROOF_READER[0]
        old['scripts/stock_return_pipeline.py']=reader.FORWARD_CONSUMPTION_READER[0]
        old['scripts/share_change_source.py']=(reader.FUTURE_PLAN_SHARE_READER[0]
            if producer.startswith('be89') else reader.TOTAL_NEUTRAL_SHARE_READER[0])
        old['data/share_source_future_plan_reviews.json']=reader.FUTURE_PLAN_REGISTRY_READER[0]
    monkeypatch.setattr(contract,'source_fingerprint',lambda root:current)
    manifest=dict(runtime_sha=producer,producer_context=dict(runtime_sha=producer,native_sources=old))
    before=copy.deepcopy(manifest)
    assert reader.validate_source_contract(ROOT,manifest)==current
    assert manifest==before
    changed=copy.deepcopy(old);changed['scripts/share_change_source.py']='a'*64
    manifest['producer_context']['native_sources']=changed
    with pytest.raises(ValueError):reader.validate_source_contract(ROOT,manifest)
    manifest=copy.deepcopy(before);manifest['runtime_sha']='f'*40;manifest['producer_context']['runtime_sha']='f'*40
    with pytest.raises(ValueError):reader.validate_source_contract(ROOT,manifest)


def test_reviewed_source_change_set_exactly_matches_actual_input_paths(frozen):
    original=json.loads(frozen['raw']('bundle-manifest.json'))
    names=[f'outputs/{n}' for n in (*delivery.BASE_FILES.values(),delivery.BASE_PANEL)]
    names += [delivery.AUTHORITY]+[f'microcap_top100_mom16_biweekly_live_v2_{v}.py' for v in delivery.COSTED]
    names += ['scripts/top100_data_contracts.py']+delivery.evidence_source_files(ROOT)+delivery.RETURN_BASIS_FILES
    expected=set(original['inputs'])|{a.MODULE}|{n for n,pair in a.input_changes().items() if pair[0] is None}
    assert set(names)==expected
    for name,(before,after) in a.input_changes().items():
        assert original['inputs'].get(name)==before
        assert delivery.sha(ROOT/name)==after
    from scripts.stock_input_contract import SOURCE_FILES
    assert {n for n in a.input_changes() if n in SOURCE_FILES}==set(a.CHANGES)


def test_boundary_review_is_exact_nonstock_source_change(frozen):
    from scripts.stock_input_contract import SOURCE_FILES
    name='scripts/scoped_cap_boundary.py'
    original=json.loads(frozen['raw']('bundle-manifest.json'))
    before,after=a.ARCHIVE_INPUT_CHANGES[name]
    assert name in delivery.RELEASE_FILES and name not in SOURCE_FILES and name not in a.CHANGES
    assert before==original['inputs'][name] and after==delivery.sha(ROOT/name)
    assert before!=after
    assert a.LEGACY_REVISION=='exact_m3_20261110_boundary_coverage_source_only_v2'
    assert a.REVISION=='exact_m3_20261110_latequote_st_observation_source_only_v3'
    a.require_transition(original,current_report(original))


@pytest.mark.parametrize('fault',['old_boundary','different_boundary','unreviewed_companion'])
def test_archive_boundary_review_does_not_accept_other_source_bytes(frozen,fault):
    name='scripts/scoped_cap_boundary.py'
    original=json.loads(frozen['raw']('bundle-manifest.json'))
    current=current_report(original)
    if fault=='old_boundary':current['inputs'][name]=a.ARCHIVE_INPUT_CHANGES[name][0]
    elif fault=='different_boundary':current['inputs'][name]='a'*64
    else:current['inputs']['scripts/scoped_forward_consumption.py']='a'*64
    with pytest.raises(ValueError,match='unreviewed input bytes'):
        a.require_transition(original,current)


def test_old_active_receipt_does_not_gain_the_new_boundary_review(transaction):
    root,_,_=transaction
    saved=a.recertify(root,a.DATE)
    saved[a.ACTIVE]['revision']='exact_m3_20261110_share_observation_source_only_v1'
    with pytest.raises(ValueError,match='Unknown exact archive source validation revision'):
        a.checked_receipt(saved[a.ACTIVE])
