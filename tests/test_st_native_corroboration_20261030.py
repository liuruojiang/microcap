import json,shutil,hashlib
from pathlib import Path
import pytest
from scripts.st_notice_evidence import evidence_fingerprint,evidence_source_files,StNoticeEvidenceError
ROOT=Path(__file__).resolve().parents[1]
ID="AN201208060005411940"
@pytest.fixture
def case(tmp_path):
    source=ROOT/"data/st_notice_evidence";dest=tmp_path/"data/st_notice_evidence";dest.mkdir(parents=True)
    registry=json.loads((source/"registry.json").read_text(encoding="utf-8"));e=next(x for x in registry["events"] if x["announcement_id"]==ID);registry["events"]=[e]
    for name in [e["relative_pdf_path"]]+[spec["relative_path"] for spec in e["native_evidence"].values()]:shutil.copy2(source/name,dest/name)
    p=dest/"registry.json";p.write_text(json.dumps(registry),encoding="utf-8")
    return tmp_path,p,registry,e

def test_real_issuer_corroboration_is_bound_and_transported(case):
    root,p,registry,e=case
    assert evidence_fingerprint(root)
    assert e["notice_date"]=="2012-08-07" and e["actual_effective_date"]=="2012-08-08"
    assert not any(e["risk_actions"].values())
    assert "data/st_notice_evidence/"+e["native_evidence"]["issuer_corroboration"]["relative_path"] in evidence_source_files(root)

@pytest.mark.parametrize("change",["bytes","host","review_binding"])
def test_corroboration_mutations_fail_closed(case,change):
    root,p,registry,e=case;spec=e["native_evidence"]["issuer_corroboration"];folder=p.parent
    if change=="bytes":
        artifact=folder/spec["relative_path"];artifact.write_bytes(artifact.read_bytes()+b"tampered")
    elif change=="host":
        spec["source_url"]=spec["source_url"].replace("static.cninfo.com.cn","example.com")
    else:
        rp=folder/e["native_evidence"]["review"]["relative_path"];review=json.loads(rp.read_text(encoding="utf-8"));review["evidence"].pop("issuer_corroboration");rp.write_text(json.dumps(review),encoding="utf-8");e["native_evidence"]["review"]["sha256"]=hashlib.sha256(rp.read_bytes()).hexdigest()
    p.write_text(json.dumps(registry),encoding="utf-8")
    with pytest.raises(StNoticeEvidenceError):evidence_fingerprint(root)
