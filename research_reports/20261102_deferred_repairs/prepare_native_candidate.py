"""Isolated code-identity recertification; never writes primary or authorizes promotion."""
from pathlib import Path
import datetime, hashlib, io, json, os, subprocess, sys, zipfile
ROOT = Path("D:/Codex/workspaces/microcap-native-regression-20261102")
EVIDENCE = Path(__file__).resolve().parent
SEED = EVIDENCE.parent / "20261029_st_history_repair/final_delivery_20261101/microcap-whole-delivery-20261030.zip"
EXPECTED_SEED = "8d6af3322aeb693f0bcee2e61281d6389c21b496b8305f3e1f1c27834fdc8664"
NEW_SHA = "75dd463e0a724af0b55e0f029f0468de1feed3c2"
META = "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_meta.json"
def sha(raw): return hashlib.sha256(raw).hexdigest()
def save(name, value): (EVIDENCE/name).write_text(json.dumps(value,ensure_ascii=False,indent=2),encoding="utf-8")
assert subprocess.check_output(["git","-C",str(ROOT),"rev-parse","HEAD"],text=True).strip()==NEW_SHA
assert not os.environ.get("MICROCAP_CSV_LINE_TERMINATOR") and not os.environ.get("MICROCAP_REVIEWED_NON_ST_BOUNDARY")
assert sha(SEED.read_bytes())==EXPECTED_SEED
os.chdir(ROOT);sys.path.insert(0,str(ROOT))
import pandas as pd
from scripts import top100_delivery as delivery, realtime_state_bundle as state
from scripts.stock_input_contract import source_fingerprint
with zipfile.ZipFile(SEED) as archive:
    original=archive.read(META)
assert (ROOT/META).read_bytes()==original
meta=json.loads(original); old_code=meta["core_params"]["stock_input_evidence_fingerprint"]["code"]
new_code=source_fingerprint(ROOT)
changed={k for k in set(old_code)|set(new_code) if old_code.get(k)!=new_code.get(k)}
assert changed=={"scripts/stock_return_basis.py","scripts/stock_return_basis_store.py","scripts/st_notice_refresh.py"}, changed
report={"status":"diagnostic_candidate_only","approved":False,"primary_written":False,"source_sha":NEW_SHA,"seed_sha256":EXPECTED_SEED,"old_meta_sha256":sha(original),"source_changes":{k:{"before":old_code[k],"after":new_code[k]} for k in sorted(changed)}}
(EVIDENCE/"candidate_original_meta.json").write_bytes(original)
# Only this isolated candidate's code-binding field changes. All inputs remain the restored approved bytes.
meta["core_params"]["stock_input_evidence_fingerprint"]["code"]=new_code
(ROOT/META).write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")
report["candidate_meta_sha256"]=sha((ROOT/META).read_bytes());save("native_candidate_review.json",report)
print("candidate code binding prepared; beginning whole-universe read-only native reconstruction",flush=True)
state.validate_security_metadata(ROOT)
symbols=state._transport_universe_symbols(ROOT)
assert len(symbols)==5438
proof=state._verify_return_basis_transport_memoized(ROOT,"2026-10-30",symbols)
assert proof["verified_count"]==5438 and proof["read_only"] is True
save("native_all_basis_reconstruction.json",proof)
report["native_reconstruction"]={"verified_symbols":5438,"rows":sum(x["rows"] for x in proof["symbols"]),"proof_sha256":sha((EVIDENCE/"native_all_basis_reconstruction.json").read_bytes())}
save("native_candidate_review.json",report)
print("native reconstruction passed; running unchanged refresh-all and check",flush=True)
for command in ("refresh-all","check"):
    rc=delivery.main([command]);assert rc==0,(command,rc)
parity=[]
with zipfile.ZipFile(SEED) as archive:
    for version,costed in delivery.COSTED.items():
        names=["outputs/"+costed]+[f"outputs/microcap_top100_mom16_biweekly_live_v2_{version}_{suffix}" for suffix in ("nav.csv","performance_nav.csv","latest_signal.csv")]
        for name in names:
            old=archive.read(name);new=(ROOT/name).read_bytes()
            a=pd.read_csv(io.BytesIO(old));b=pd.read_csv(io.BytesIO(new))
            pd.testing.assert_frame_equal(a,b,check_exact=True,check_dtype=False)
            parity.append({"path":name,"rows":len(a),"identical_values":True,"old_sha256":sha(old),"new_sha256":sha(new)})
print("historical parity passed; packing the whole candidate for exact-hash review",flush=True)
from scripts import top100_cloud_delivery as cloud
import tempfile
temp_root=EVIDENCE/"pack_tmp";temp_root.mkdir(exist_ok=True);tempfile.tempdir=str(temp_root)
bundle=EVIDENCE/"microcap-native-whole-delivery-20261030.zip"
packed=cloud.pack({version:ROOT for version in delivery.COSTED},bundle,"2026-10-30")
assert packed["ok"] is True
save("native_candidate_pack.json",packed)
report["whole_bundle"]={"path":str(bundle),"sha256":sha(bundle.read_bytes()),"bytes":bundle.stat().st_size}
report.update(status="PASS_ISOLATED_CANDIDATE_PENDING_EXACT_HASH_APPROVAL",historical_parity=parity,new_meta_sha256=sha((ROOT/META).read_bytes()),delivery_manifest_sha256=sha((ROOT/delivery.MANIFEST).read_bytes()),completed_at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
save("native_candidate_review.json",report)
print(json.dumps({"status":report["status"],"verified_symbols":5438,"streams":len(parity),"new_meta_sha256":report["new_meta_sha256"]}),flush=True)
