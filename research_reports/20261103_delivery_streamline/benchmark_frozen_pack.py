from pathlib import Path
import sys,time,json,hashlib,zipfile,datetime
root=Path.cwd();sys.path.insert(0,str(root))
from scripts import top100_cloud_delivery as cloud,top100_delivery as delivery,realtime_state_bundle as state
out=root/'research_reports/20261103_delivery_streamline'
target=out/'shared-whole-delivery-20261102.zip'
assert not target.exists()
baseline=root/'research_reports/20261102_deferred_repairs/microcap-tencent-whole-delivery-20261102.zip'
assert hashlib.sha256(baseline.read_bytes()).hexdigest()=='9338d5536584b131d0ee0a78fcf7a604f85970c21c30a33b77c35be6902c0864'
counts={'base_validations':0,'workspace_file_copies':0}
validate=delivery.validate_base_state_for_session;copy=cloud.copy_file
def measured_validate(*args,**kwargs):
    counts['base_validations']+=1
    return validate(*args,**kwargs)
def measured_copy(*args,**kwargs):
    counts['workspace_file_copies']+=1
    return copy(*args,**kwargs)
delivery.validate_base_state_for_session=measured_validate;cloud.copy_file=measured_copy
started=datetime.datetime.now(datetime.timezone.utc).isoformat();t=time.perf_counter()
result=cloud.pack({v:root for v in delivery.COSTED},target,'2026-11-02')
elapsed=time.perf_counter()-t
assert result['ok'] and counts=={'base_validations':1,'workspace_file_copies':0}
with zipfile.ZipFile(baseline) as old,zipfile.ZipFile(target) as new:
    a=json.loads(old.read(state.MANIFEST_NAME));b=state._verify_bundle_manifest(new)
    old_files={x['path']:(x['bytes'],x['sha256']) for x in a['files']}
    new_files={x['path']:(x['bytes'],x['sha256']) for x in b['files']}
    assert old_files.keys()==new_files.keys()
    differences=[p for p in old_files if old_files[p]!=new_files[p]]
    assert set(differences)<={delivery.MANIFEST},differences
    old_delivery=json.loads(old.read(delivery.MANIFEST));new_delivery=json.loads(new.read(delivery.MANIFEST))
    for key in ('inputs','artifacts','expected_date'):assert old_delivery[key]==new_delivery[key],key
result.update(counts=counts,elapsed_seconds=elapsed,started_at=started,completed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),fixed_date_diagnostic=True,all_payloads_identical_except_delivery_record=True,changed_payloads=differences,sha256=hashlib.sha256(target.read_bytes()).hexdigest(),bytes=target.stat().st_size)
(out/'frozen_pack_receipt.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
print(json.dumps(result,ensure_ascii=False,indent=2),flush=True)
