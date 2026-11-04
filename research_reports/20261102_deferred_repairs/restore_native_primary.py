"""Restore the already fully verified whole candidate, then run both original gates.
No model/history waiver: require exact seed parity, source identity, backup and per-file hashes.
"""
from pathlib import Path
import datetime, hashlib, io, json, os, subprocess, sys, uuid, zipfile
EVIDENCE=Path(__file__).resolve().parent
ROOT=EVIDENCE.parents[1]
assert ROOT==Path("D:/动量策略/微盘股对冲策略").resolve()
os.chdir(ROOT);sys.path.insert(0,str(ROOT))
import pandas as pd
from scripts import top100_delivery as delivery, top100_cloud_delivery as cloud, realtime_state_bundle as state

def sha(path):
    h=hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda:f.read(1024*1024),b""): h.update(chunk)
    return h.hexdigest()

def save(name,value): (EVIDENCE/name).write_text(json.dumps(value,ensure_ascii=False,indent=2),encoding="utf-8")
review_path=EVIDENCE/"native_candidate_review.json"
review=json.loads(review_path.read_bytes())
assert review["status"]=="PASS_ISOLATED_CANDIDATE_PENDING_EXACT_HASH_APPROVAL"
assert review["native_reconstruction"]["verified_symbols"]==5438
assert len(review["historical_parity"])==12 and all(x["identical_values"] is True for x in review["historical_parity"])
assert review["seed_sha256"]=="8d6af3322aeb693f0bcee2e61281d6389c21b496b8305f3e1f1c27834fdc8664"
bundle=Path(review["whole_bundle"]["path"]).resolve();assert bundle.parent==EVIDENCE
assert sha(bundle)==review["whole_bundle"]["sha256"]
release=delivery.verify_release(ROOT)
subprocess.run(["git","merge-base","--is-ancestor",review["source_sha"],"HEAD"],check=True)
# The existing task authorizes bug fixes and synchronization, not a changed return history.
# Exact all-history parity is mandatory here; any changed history cannot use this path.
seed=EVIDENCE.parent/"20261029_st_history_repair/final_delivery_20261101/microcap-whole-delivery-20261030.zip"
assert sha(seed)==review["seed_sha256"]
with zipfile.ZipFile(seed) as old:
    for item in review["historical_parity"]:
        actual=pd.read_csv(ROOT/item["path"])
        baseline=pd.read_csv(io.BytesIO(old.read(item["path"])))
        pd.testing.assert_frame_equal(actual,baseline,check_exact=True,check_dtype=False)
acceptance={"scope":"code_only_cache_recertification","historical_values_changed":False,"strategy_parameters_changed":False,"history_rewrite_migration_option_used":False,"authorization":"existing user request to fix deferred issues and synchronize Git/local/cloud daily reports; no new financial assumption or historical rewrite","candidate_report_sha256":sha(review_path),"source_sha":review["source_sha"],"remote_main_sha":release,"bundle_sha256":sha(bundle),"old_meta_sha256":review["old_meta_sha256"],"new_meta_sha256":review["new_meta_sha256"]}
save("native_code_recertification_acceptance.json",acceptance)
lock=ROOT/delivery.LOCK
fd=os.open(lock,os.O_CREAT|os.O_EXCL|os.O_WRONLY);os.write(fd,str(os.getpid()).encode());os.close(fd)
backup=ROOT/".codex_backups"/("native_recertification_"+datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ"))
assert backup.resolve().is_relative_to(ROOT)
started=False
try:
    with zipfile.ZipFile(bundle) as archive:
        manifest=state._verify_bundle_manifest(archive)
        incoming=json.loads(archive.read(delivery.MANIFEST));target=incoming["expected_date"]
        cloud._validate_no_date_rollback(ROOT,target)
        for name in cloud.SOURCE_FILES:
            actual=sha(ROOT/name) if name.startswith("data/st_notice_evidence/") else delivery.sha(ROOT/name)
            assert actual==incoming["inputs"][name],("source mismatch",name)
        entries=manifest["files"];names=[x["path"] for x in entries]
        assert len(names)==len({name.casefold() for name in names})
        for name in names:
            rel=Path(name);dest=(ROOT/rel).resolve()
            assert rel.parts[0] in ("outputs",".microcap_index_cache") and name not in cloud.SOURCE_FILES
            assert ".." not in rel.parts and not rel.is_absolute() and ":" not in name and "\\" not in name
            assert dest.is_relative_to(ROOT)
        try: existing=json.loads((ROOT/delivery.MANIFEST).read_bytes())
        except (OSError,ValueError): existing={}
        if existing.get("status")=="complete":
            local=delivery.inspect_outputs(ROOT,target)
            if local["ok"]:
                assert all(local.get(k)==incoming.get(k) for k in ("expected_date","inputs","artifacts")),"different complete state"
        backup.mkdir(parents=True);changes=[]
        print("backing up every replaced primary file",flush=True)
        for name in names:
            dest=ROOT/name;existed=dest.is_file();before=None
            if existed:
                before=sha(dest);cloud.copy_file(dest,backup/name)
                assert sha(backup/name)==before and sha(dest)==before,("backup mismatch",name)
            changes.append({"path":name,"existed":existed,"before_sha256":before})
        (backup/"manifest.json").write_text(json.dumps({"bundle_sha256":acceptance["bundle_sha256"],"files":changes},indent=2),encoding="utf-8")
        delivery.write_manifest(ROOT,{"status":"restoring_verified_native_whole_state","errors":["whole restore in progress"]})
        started=True
        print("restoring verified whole-state bytes",flush=True)
        for entry in entries:
            name=entry["path"];raw=archive.read(name)
            assert hashlib.sha256(raw).hexdigest()==entry["sha256"]
            dest=state._extended(ROOT/name);dest.parent.mkdir(parents=True,exist_ok=True)
            temp=dest.with_name(dest.name+"."+uuid.uuid4().hex+".tmp")
            temp.write_bytes(raw);temp.replace(dest)
            assert sha(dest)==entry["sha256"],("write readback mismatch",name)
        delivery.write_manifest(ROOT,{"status":"restored_pending_refresh","expected_date":target,"errors":["original refresh-all/check required"]})
        acceptance.update(restored_files=len(entries),backup=str(backup),restored_at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
        save("native_primary_restore_receipt.json",acceptance)
except BaseException as exc:
    if started: delivery.write_manifest(ROOT,{"status":"blocked","errors":[str(exc)],"backup":str(backup)})
    raise
finally:
    if lock.exists() and lock.read_text().strip()==str(os.getpid()):lock.unlink()
print("whole restore complete; running original refresh-all and check",flush=True)
for command in ("refresh-all","check"):
    rc=delivery.main([command]);assert rc==0,(command,rc)
acceptance.update(local_delivery_pass=True,completed_at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),delivery_manifest_sha256=sha(ROOT/delivery.MANIFEST))
save("native_primary_delivery_receipt.json",acceptance)
print("PRIMARY_NATIVE_DELIVERY_PASS",flush=True)
