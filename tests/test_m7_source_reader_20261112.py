"""M7 source-only metadata/contracts; no full archive or current strategy claim.

The original M6 receipt and M3 manifest/proxy are frozen real artifacts. The v3
receipt constructor below is explicitly a software fixture, never an earned
recertification. Full 170713-object validation must run before publication.
"""
import ast
import copy
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
import socket
import sys
import types

import pytest

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from scripts import scoped_archive_source_revalidation as archive
from scripts import scoped_source_consumer as consumer
from scripts.stock_input_contract import source_fingerprint

P2='3fe2e15ee9e3f2ee7e25bbc328479f1fd9910d36'
FIXTURE=ROOT/'tests/fixtures/m7_source_reader_20261112'
M6_MANIFEST=FIXTURE/'M6-earned-manifest.json'
M3_MANIFEST=FIXTURE/'M3-original-manifest.json'


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    def blocked(*args,**kwargs):raise AssertionError('M7 metadata regression forbids transport')
    monkeypatch.setattr(socket.socket,'connect',blocked)


@pytest.fixture
def legacy():
    raw=M6_MANIFEST.read_bytes()
    assert hashlib.sha256(raw).hexdigest()=='a891e3ce2dbb111b9e6e80316e528ec8973180a0ab243ea12ee7a0a0097d22e5'
    return json.loads(raw)


def software_receipt():
    raw=M3_MANIFEST.read_bytes();original=archive.original_manifest(raw)
    inputs=dict(original['inputs'])
    for name,(_,after) in archive.input_changes().items():inputs[name]=after
    inputs[archive.MODULE]=hashlib.sha256((ROOT/archive.MODULE).read_bytes().replace(b'\r\n',b'\n')).hexdigest()
    return dict(revision=archive.REVISION,archive_session=archive.DATE,original_manifest_utf8=raw.decode(),
        original_manifest_sha256=archive.digest(raw),original_release_sha=archive.RELEASE,
        original_proxy_sha256=archive.BASELINE['proxy_sha256'],stock_fingerprint_sha256=archive.STOCK_IDENTITY,
        source_changes={name:list(pair) for name,pair in archive.input_changes().items()},validator_inputs=inputs,
        validation_release_sha='f'*40,validated_at=datetime.now(timezone.utc).isoformat(),
        source_bundle_sha256=archive.BASELINE['source_bundle_sha256'],source_state_manifest_sha256=archive.BASELINE['state_manifest_sha256'],
        data_inventory_sha256=archive.BASELINE['state_inventory_sha256'],economic_csvs=copy.deepcopy(archive.BASELINE['economic_csvs']))


def real_sha_dependency(monkeypatch):
    # Execute the actual unchanged native sha definition, without importing its
    # unrelated 146MB static notice registry just to test source-map integrity.
    module=types.ModuleType('scripts.top100_delivery')
    tree=ast.parse((ROOT/'scripts/top100_delivery.py').read_text(encoding='utf8'))
    node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='sha')
    module.__dict__.update(hashlib=hashlib,Path=Path)
    exec(compile(ast.Module(body=[node],type_ignores=[]),'native-sha-definition','exec'),module.__dict__)
    monkeypatch.setitem(sys.modules,'scripts.top100_delivery',module)
    return module


def test_public_new_revision_and_actual_legacy_M6_receipt_are_separate(legacy):
    assert archive.REVISION=='exact_m3_20261110_latequote_st_observation_source_only_v3'
    assert legacy[archive.ACTIVE]['revision']==archive.LEGACY_REVISION!=archive.REVISION
    before=copy.deepcopy(legacy)
    original=archive.checked_document(legacy)
    assert original==archive.original_manifest(M3_MANIFEST.read_bytes())
    assert legacy==before
    assert archive.pending_ancestry(legacy)=={archive.PREVIOUS:legacy[archive.ACTIVE]}


def test_current_v3_source_closure_is_exact_and_legacy_active_is_not_current(legacy,monkeypatch):
    real_sha_dependency(monkeypatch)
    archive.checked_source(ROOT)
    with pytest.raises(ValueError,match='unreviewed reader'):
        archive.checked_source(ROOT,revision=archive.LEGACY_REVISION)
    report=copy.deepcopy(legacy);report['inputs']=software_receipt()['validator_inputs']
    with pytest.raises(ValueError,match='unreviewed input bytes|inputs changed|input path set'):
        archive.validate_active(ROOT,report,legacy)


def test_relabelled_real_M6_v2_receipt_cannot_become_v3(legacy):
    receipt=copy.deepcopy(legacy[archive.ACTIVE]);receipt['revision']=archive.REVISION
    with pytest.raises(ValueError,match='exact M3 review|validator inputs'):
        archive.checked_receipt(receipt)


@pytest.mark.parametrize('fault',['unknown_revision','source_pair','validator_source','data_inventory','economic_csv','original_manifest'])
def test_software_v3_schema_rejects_unreviewed_metadata_changes(fault):
    receipt=software_receipt();archive.checked_receipt(receipt)
    if fault=='unknown_revision':receipt['revision']+='unknown'
    elif fault=='source_pair':receipt['source_changes']['scripts/share_change_source.py'][1]='0'*64
    elif fault=='validator_source':receipt['validator_inputs']['scripts/share_unchanged_tail.py']='0'*64
    elif fault=='data_inventory':receipt['data_inventory_sha256']='0'*64
    elif fault=='economic_csv':receipt['economic_csvs'][next(iter(receipt['economic_csvs']))]='0'*64
    elif fault=='original_manifest':receipt['original_manifest_utf8']+=' '
    with pytest.raises(ValueError):archive.checked_receipt(receipt)


def test_exact_M3_stock_identity_only_allows_reviewed_source_successor(tmp_path):
    original=copy.deepcopy(archive.BASELINE['stock_fingerprint'])
    current=copy.deepcopy(original);current['code']=source_fingerprint(ROOT)
    path=tmp_path/archive.PROXY;path.parent.mkdir(parents=True)
    raw=(FIXTURE/'M3-proxy-meta.json').read_bytes()
    assert archive.digest(raw)==archive.BASELINE['proxy_sha256'];path.write_bytes(raw)
    assert archive.stock_compatible(original,current,tmp_path)
    unrelated=copy.deepcopy(original);unrelated['file_count']+=1
    assert not archive.stock_compatible(unrelated,current,tmp_path)
    unknown=copy.deepcopy(current);unknown['code']['scripts/share_unchanged_tail.py']='0'*64
    assert not archive.stock_compatible(original,unknown,tmp_path)
    path.write_bytes(raw+b' ')
    assert not archive.stock_compatible(original,current,tmp_path)


def test_new_P2_and_old_P1_are_exact_reviewed_source_roles():
    current=source_fingerprint(ROOT)
    p2=dict(runtime_sha=P2,producer_context=dict(runtime_sha=P2,native_sources=current))
    assert consumer.validate_source_contract(ROOT,p2)==current
    old=copy.deepcopy(current)
    for name,(before,_) in consumer.LATER_QUOTE_READER.items():old[name]=before
    p1='f6970d665d5c7a39b37c866cf13dc3d4cb8fdcc5'
    assert consumer.validate_source_contract(ROOT,dict(runtime_sha=p1,producer_context=dict(runtime_sha=p1,native_sources=old)))==current
    fake=copy.deepcopy(p2);fake['runtime_sha']='1'*40;fake['producer_context']['runtime_sha']='1'*40
    with pytest.raises(ValueError,match='producer/code contract'):consumer.validate_source_contract(ROOT,fake)
    fake=copy.deepcopy(p2);fake['producer_context']['native_sources']['scripts/share_unchanged_tail.py']='0'*64
    with pytest.raises(ValueError,match='Unreviewed scoped source consumer'):consumer.validate_source_contract(ROOT,fake)
