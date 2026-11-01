from st_evidence_fixture import copy_real_evidence_subset
import json,shutil
from pathlib import Path
import pytest
from scripts.st_notice_evidence import evidence_fingerprint,evidence_source_files,load_verified_events,StNoticeEvidenceError
ROOT=Path(__file__).resolve().parents[1]
ID="AN201203010004601977"
@pytest.fixture
def case(tmp_path):
    copy_real_evidence_subset(ROOT, tmp_path, [ID])
    p=tmp_path/"data/st_notice_evidence/registry.json";reg=json.loads(p.read_text(encoding="utf-8"));return tmp_path,p,reg,next(e for e in reg["events"] if e["announcement_id"]==ID)
def test_native_real_provider_identity(case):
    root,p,reg,e=case
    row=dict(source_provider="eastmoney",issuer_symbol=e["symbol"],announcement_id=ID,notice_date=e["notice_date"],title=e["title"],source_url=e["source_url"])
    result=load_verified_events(root,"002072","2010-05-05","2010-05-05",[row])
    assert result[ID]["risk_actions"]=={} and result[ID]["notice_date"]=="2010-05-05"
    for spec in e["native_evidence"].values():assert "data/st_notice_evidence/"+spec["relative_path"] in evidence_source_files(root)
@pytest.mark.parametrize("field,value",[("source_provider","cninfo"),("announcement_id","201203010004601977"),("notice_date","2012-03-01"),("symbol","002073"),("source_url","https://static.cninfo.com.cn/finalpage/2010-05-05/123.PDF"),("removed_issuer_prefix",""),("risk_actions",{"delisting":False}),("native_evidence",{}),("body_unavailable",True)])
def test_native_changes_fail(case,field,value):
    root,p,reg,e=case;e[field]=value;p.write_text(json.dumps(reg),encoding="utf-8")
    with pytest.raises(StNoticeEvidenceError):evidence_fingerprint(root)
@pytest.mark.parametrize("artifact",["provider_response","review"])
def test_native_artifact_bytes_bound(case,artifact):
    root,p,reg,e=case;a=root/"data/st_notice_evidence"/e["native_evidence"][artifact]["relative_path"];a.write_bytes(a.read_bytes()+b"changed")
    with pytest.raises(StNoticeEvidenceError):evidence_fingerprint(root)
