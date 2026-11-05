"""Sealed past certificates never grant current-source execution permission."""
import copy,gzip,hashlib,json
from pathlib import Path
from datetime import datetime,timezone
import pytest
from scripts import realtime_state_bundle as state
from scripts import top100_delivery as delivery


def actual_source_ancestor():
    raw=gzip.decompress((Path(__file__).parent/'fixtures/source_validation_ancestry/certified_manifest_20261104.json.gz').read_bytes())
    assert hashlib.sha256(raw).hexdigest()=='04826756a450a0bb5898859132bd0d6c01b3f2e32845b2ae8acd6bd6b8760b1b'
    return {'manifest_sha256':hashlib.sha256(raw).hexdigest(),'manifest_utf8':raw.decode('utf8')}


def test_source_ancestry_real_old_manifest_needs_no_current_source_files(tmp_path):
    previous=actual_source_ancestor()
    assert delivery.validate_previous_source_validation(tmp_path,{'expected_date':'2026-11-05'},previous)==previous
    assert not list(tmp_path.iterdir())  # No old files are materialized or rewritten.


@pytest.mark.parametrize('field',['inputs','artifacts','streams','expected_date','source_changes','original_bytes','revision','clock'])
def test_source_ancestry_rehashed_old_manifest_mutation_is_rejected(tmp_path,field):
    previous=actual_source_ancestor();saved=json.loads(previous['manifest_utf8'])
    if field in ('inputs','artifacts'):
        saved[field][next(iter(saved[field]))]='0'*64
    elif field=='streams':saved['streams']={'forged':True}
    elif field=='expected_date':saved[field]='2026-11-03'
    else:
        cert=saved['source_validation_compatibility']
        if field=='source_changes':cert[field][next(iter(cert[field]))][1]='0'*64
        elif field=='original_bytes':cert['original_manifest_utf8']+=' '
        elif field=='revision':cert[field]='unknown_review'
        else:cert['validated_at']='2999-01-01T00:00:00+00:00'
    raw=json.dumps(saved);previous.update(manifest_utf8=raw,manifest_sha256=hashlib.sha256(raw.encode()).hexdigest())
    with pytest.raises((ValueError,KeyError)):
        delivery.validate_previous_source_validation(tmp_path,{'expected_date':'2026-11-05'},previous)


@pytest.mark.parametrize('fault',['raw_hash','nested','rollback'])
def test_source_ancestry_original_bytes_structure_and_date_stay_strict(tmp_path,fault):
    previous=actual_source_ancestor();day='2026-11-05'
    if fault=='raw_hash':previous['manifest_utf8']+=' '
    elif fault=='nested':
        saved=json.loads(previous['manifest_utf8']);saved['previous_source_validation']={}
        raw=json.dumps(saved);previous.update(manifest_utf8=raw,manifest_sha256=hashlib.sha256(raw.encode()).hexdigest())
    else:day='2026-11-03'
    with pytest.raises(ValueError):delivery.validate_previous_source_validation(tmp_path,{'expected_date':day},previous)


@pytest.fixture
def isolated_source_certificate(tmp_path,monkeypatch):
    # Explicit synthetic record to exercise current-source authority separation.
    path='microcap_top100_mom16_biweekly_live_v2_0.py';source=tmp_path/path
    before=b'old validator\n';after=b'reviewed validator\n';source.write_bytes(after)
    digest=lambda raw:hashlib.sha256(raw).hexdigest()
    original=dict(status='complete',ok=True,errors=[],scope='whole_workspace_delivery',expected_date='2026-11-04',
                  inputs={path:digest(before),'prices.csv':'a'*64},artifacts={'final.csv':'b'*64},streams={'0':{'count':100}})
    raw=json.dumps(original).encode()
    review=dict(revision='isolated_source_certificate_v1',expected_date='2026-11-04',manifest_sha256=digest(raw),
                stock_fingerprint_sha256='c'*64,changes={path:(digest(before),digest(after))})
    monkeypatch.setattr(state,'VALIDATION_SOURCE_REVIEW',review)
    saved=copy.deepcopy(original);saved['inputs'][path]=digest(after)
    saved['source_validation_compatibility']=dict(revision=review['revision'],expected_date=review['expected_date'],
        original_manifest_sha256=review['manifest_sha256'],original_manifest_utf8=raw.decode(),
        stock_fingerprint_sha256=review['stock_fingerprint_sha256'],source_changes={path:list(review['changes'][path])},
        validated_at='2026-11-04T16:00:00+08:00',validation_release_sha='d'*40)
    (tmp_path/'outputs').mkdir();(tmp_path/delivery.MANIFEST).write_text(json.dumps(saved))
    previous_raw=json.dumps(saved)
    previous={'manifest_sha256':digest(previous_raw.encode()),'manifest_utf8':previous_raw}
    return tmp_path,source,saved,previous


def test_source_ancestry_future_code_fix_does_not_break_sealed_certificate(isolated_source_certificate):
    root,source,saved,previous=isolated_source_certificate
    assert delivery.validate_source_compatibility(root,saved,saved)==saved['source_validation_compatibility']
    source.write_bytes(source.read_bytes()+b'# later actual source fix\n')
    assert delivery.validate_previous_source_validation(root,{'expected_date':'2026-11-05'},previous)==previous
    with pytest.raises(ValueError,match='Unreviewed validation source'):
        delivery.validate_source_compatibility(root,saved,saved)


def test_source_ancestry_cannot_authorize_new_recertification(isolated_source_certificate):
    root,source,saved,previous=isolated_source_certificate
    source.write_bytes(source.read_bytes()+b'# later source change\n');before=(root/delivery.MANIFEST).read_bytes()
    with pytest.raises(ValueError,match='Unreviewed validation source'):
        delivery.recertify_validation_source(root,'2026-11-04')
    assert (root/delivery.MANIFEST).read_bytes()==before


def test_source_ancestry_metadata_is_a_deep_copy_without_execution_authority(isolated_source_certificate):
    root,source,saved,previous=isolated_source_certificate
    review=state.validation_source_review_metadata();review['changes'].clear()
    assert state.VALIDATION_SOURCE_REVIEW['changes']
    source.write_bytes(b'unreviewed current source')
    assert state.validation_source_review_metadata()['changes']
    with pytest.raises(ValueError,match='Unreviewed validation source'):state.validation_source_review(root)


def test_source_ancestry_history_never_calls_current_source_permission(tmp_path,monkeypatch):
    monkeypatch.setattr(state,'validation_source_review',lambda *a:pytest.fail('historical record must not reauthorize current source'))
    previous=actual_source_ancestor()
    assert delivery.validate_previous_source_validation(tmp_path,{'expected_date':'2026-11-05'},previous)==previous
