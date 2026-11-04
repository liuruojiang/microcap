"""Independent bounded lifecycle checks; never use production state or sources."""
import hashlib
import json
from pathlib import Path
import shutil
import zipfile

import pytest

from test_normal_source_receipt_transition import certified, normal_refresh_and_generate
from test_top100_delivery import workspace
from scripts import top100_delivery as delivery
from scripts import top100_cloud_delivery as cloud


def next_manifest(certified, monkeypatch):
    root, prior = certified
    normal_refresh_and_generate(root, '2026-09-04', monkeypatch)
    bundle=root.parent/(root.name+'-independent1.zip')
    cloud.pack({v:root for v in delivery.COSTED},bundle,'2026-09-04')
    with zipfile.ZipFile(bundle) as archive:
        raw=archive.read(delivery.MANIFEST)
    (root/delivery.MANIFEST).write_bytes(raw)
    return root,json.loads(raw),prior


def test_second_pack_after_manifest_readback_keeps_exact_ancestry(certified,monkeypatch):
    root,first,prior=next_manifest(certified,monkeypatch)
    assert delivery.validate_manifest(root,delivery.inspect_outputs(root,'2026-09-04'))['ok']
    again=root.parent/(root.name+'-independent2.zip')
    cloud.pack({v:root for v in delivery.COSTED},again,'2026-09-04')
    with zipfile.ZipFile(again) as archive:
        second=json.loads(archive.read(delivery.MANIFEST))
    assert first['previous_source_validation']==second['previous_source_validation']
    previous=second['previous_source_validation']
    assert previous['manifest_utf8'].encode()==prior
    assert 'previous_source_validation' not in json.loads(previous['manifest_utf8'])
    assert 'source_validation_compatibility' not in second
    for field in ('expected_date','inputs','artifacts','streams'):
        assert first[field]==second[field]


@pytest.mark.parametrize('field',['artifacts','inputs','expected_date'])
def test_rehashed_tampered_ancestor_cannot_authenticate(certified,monkeypatch,field):
    root,current,prior=next_manifest(certified,monkeypatch)
    previous=current['previous_source_validation']
    claimed=json.loads(previous['manifest_utf8'])
    if field=='expected_date':
        claimed[field]='2026-09-02'
    else:
        claimed[field][next(iter(claimed[field]))]='0'*64
    forged=json.dumps(claimed)
    previous.update(manifest_utf8=forged,manifest_sha256=hashlib.sha256(forged.encode()).hexdigest())
    (root/delivery.MANIFEST).write_text(json.dumps(current),encoding='utf8')
    assert not delivery.inspect_outputs(root,'2026-09-04')['ok']
    output=root.parent/(root.name+'-forged.zip')
    with pytest.raises((ValueError,RuntimeError)):
        cloud.pack({v:root for v in delivery.COSTED},output,'2026-09-04')
    assert not output.exists()


def test_previous_provenance_does_not_waive_regular_manifest_artifact_match(certified,monkeypatch):
    root,current,prior=next_manifest(certified,monkeypatch)
    summary=root/'outputs/microcap_top100_mom16_biweekly_live_v2_0_summary.json'
    summary.write_text(json.dumps(json.loads(summary.read_bytes()),indent=3),encoding='utf8')
    inspected=delivery.inspect_outputs(root,'2026-09-04')
    assert inspected['ok'],inspected['errors']
    checked=delivery.validate_manifest(root,inspected)
    assert not checked['ok']
    assert any('manifest mismatch: artifacts' in item for item in checked['errors'])


def test_ancestry_rejects_date_rollback_without_historical_replay(certified,monkeypatch):
    root,current,prior=next_manifest(certified,monkeypatch)
    previous=current['previous_source_validation']
    with pytest.raises(ValueError,match='after the current delivery'):
        delivery.validate_previous_source_validation(root,{'expected_date':'2026-09-02'},previous)


def test_ancestry_verification_does_not_run_native_history_scans(certified,monkeypatch):
    root,current,prior=next_manifest(certified,monkeypatch)
    previous=current['previous_source_validation']
    def forbidden(*args,**kwargs):
        pytest.fail('ancestry verification must authenticate hashes, not replay history')
    monkeypatch.setattr(delivery.state,'validate_state',forbidden)
    monkeypatch.setattr(delivery.state,'validate_member_cache_transport',forbidden)
    monkeypatch.setattr(delivery,'input_hashes',forbidden)
    assert delivery.validate_previous_source_validation(root,current,previous)==previous


def test_multi_root_pack_carries_primary_original_receipt(certified,monkeypatch):
    # Only copies this bounded synthetic fixture, never a production/history tree.
    root,prior=certified
    normal_refresh_and_generate(root,'2026-09-04',monkeypatch)
    assert len([p for p in root.rglob('*') if p.is_file()]) < 500
    roots={'0':root}
    for version in ('3','5'):
        target=root.parent/(root.name+'-v'+version)
        shutil.copytree(root,target)
        roots[version]=target
    bundle=root.parent/(root.name+'-multi.zip')
    cloud.pack(roots,bundle,'2026-09-04')
    with zipfile.ZipFile(bundle) as archive:
        report=json.loads(archive.read(delivery.MANIFEST))
    assert report['previous_source_validation']['manifest_utf8'].encode()==prior
    assert report['previous_source_validation']['manifest_sha256']==hashlib.sha256(prior).hexdigest()


def test_official_restore_then_repack_preserves_ancestry(certified,monkeypatch):
    root,first,prior=next_manifest(certified,monkeypatch)
    bundle=root.parent/(root.name+'-independent1.zip')
    target=root.parent/(root.name+'-restored'); target.mkdir()
    for name in cloud.SOURCE_FILES:
        cloud.copy_file(root/name,target/name)
    result=cloud.restore(target,bundle,'2026-09-04')
    assert result['ok'] and result['signal_ready'] is False
    restored=json.loads((target/delivery.MANIFEST).read_bytes())
    assert restored['previous_source_validation']==first['previous_source_validation']
    again=target.parent/(target.name+'-repack.zip')
    cloud.pack({v:target for v in delivery.COSTED},again,'2026-09-04')
    with zipfile.ZipFile(again) as archive:
        second=json.loads(archive.read(delivery.MANIFEST))
    assert second['previous_source_validation']==first['previous_source_validation']
