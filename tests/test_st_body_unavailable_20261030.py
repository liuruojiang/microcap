from st_evidence_fixture import copy_real_evidence_subset
"""Actual missing-original event and adversarial identity/transport checks."""
import json
import shutil
from pathlib import Path
import pytest
from scripts.st_notice_evidence import evidence_fingerprint, evidence_source_files, load_verified_events, StNoticeEvidenceError
ROOT=Path(__file__).resolve().parents[1]
ID="1200827048"
@pytest.fixture
def case(tmp_path):
    copy_real_evidence_subset(ROOT, tmp_path, [ID])
    path=tmp_path/"data/st_notice_evidence/registry.json"
    reg=json.loads(path.read_text(encoding="utf-8"))
    return tmp_path,path,reg,next(e for e in reg["events"] if e["announcement_id"]==ID)
def save(path,reg):path.write_text(json.dumps(reg,ensure_ascii=False),encoding="utf-8")
def test_real_identity_and_transport(case):
    root,path,reg,event=case
    row=dict(announcement_id=ID,issuer_symbol="000968",notice_date="2015-04-14",title=event["title"],source_url=event["source_url"])
    verified=load_verified_events(root,"000968","2015-04-14","2015-04-14",[row])[ID]
    assert verified["risk_actions"]=={} and verified["security_scope"]=="bond_only"
    assert "conflict_evidence" not in verified
    files=evidence_source_files(root)
    for spec in event["unavailable_evidence"].values():
        assert "data/st_notice_evidence/"+spec["relative_path"] in files
@pytest.mark.parametrize("field,value",[("source_type","unknown"),("body_unavailable",False),("source_conflict",True),("body_announcement_id","AN999"),("body_notice_number","2015-024"),("body_provider_title","wrong"),("symbol","000969"),("notice_date","2015-04-15"),("risk_actions",{"other":True}),("body_source_url","https://evil.invalid/a.pdf"),("unavailable_evidence",{})])
def test_identity_changes_fail(case,field,value):
    root,path,reg,event=case;event[field]=value;save(path,reg)
    with pytest.raises(StNoticeEvidenceError):evidence_fingerprint(root)
@pytest.mark.parametrize("artifact",["provider_response","mirror_response","crosscheck","failure_record","review"])
@pytest.mark.parametrize("damage",["missing","bytes","escape"])
def test_auxiliary_is_required(case,artifact,damage):
    root,path,reg,event=case;spec=event["unavailable_evidence"][artifact];p=root/"data/st_notice_evidence"/spec["relative_path"]
    if damage=="missing":p.unlink()
    elif damage=="bytes":p.write_bytes(p.read_bytes()+b"changed")
    else:spec["relative_path"]="../"+p.name;save(path,reg)
    with pytest.raises(StNoticeEvidenceError):evidence_fingerprint(root)

@pytest.mark.parametrize("artifact,field,value",[("provider_response","secCode","000969"),("provider_response","announcementTime",1429027200000),("provider_response","announcementTitle","wrong"),("mirror_response","notice_date","2015-04-15 00:00:00"),("mirror_response","title","wrong"),("failure_record","original_pdf_obtained",True)])
def test_rehashed_bad_provider_claim_is_still_rejected(case,artifact,field,value):
    import hashlib
    root,path,reg,event=case;spec=event["unavailable_evidence"][artifact];p=root/"data/st_notice_evidence"/spec["relative_path"]
    payload=json.loads(p.read_text(encoding="utf-8"))
    if artifact=="provider_response":row=next(r for r in payload["announcements"] if str(r["announcementId"])==ID)
    elif artifact=="mirror_response":row=next(r for r in payload["data"]["list"] if r["art_code"]==event["body_announcement_id"])
    else:row=payload
    row[field]=value;p.write_text(json.dumps(payload),encoding="utf-8");spec["sha256"]=hashlib.sha256(p.read_bytes()).hexdigest()
    review=event["unavailable_evidence"]["review"];rp=root/"data/st_notice_evidence"/review["relative_path"];rv=json.loads(rp.read_text(encoding="utf-8"));rv["evidence"][artifact]=spec;rp.write_text(json.dumps(rv),encoding="utf-8");review["sha256"]=hashlib.sha256(rp.read_bytes()).hexdigest();save(path,reg)
    with pytest.raises(StNoticeEvidenceError):evidence_fingerprint(root)
