"""Archive rows must agree with actual response rows even after rehashing."""
import hashlib,json
import pytest
import scripts.st_notice_archive as archive_module
from scripts.st_notice_evidence import StNoticeEvidenceError,DATE_POLICY
@pytest.fixture
def archive(tmp_path,monkeypatch):
    monkeypatch.setattr(archive_module,"_registry",lambda root:({},"f"*64))
    def put(name,obj):
        raw=json.dumps(obj).encode();(tmp_path/name).write_bytes(raw);return dict(path=name,sha256=hashlib.sha256(raw).hexdigest())
    art="AN201203010004601977";title="关于其他风险警示的公告"
    api=put("source.json",dict(success=1,data=dict(page_index=1,page_size=100,total_hits=1,list=[dict(art_code=art,notice_date="2010-05-05 00:00:00",title="issuer:"+title,codes=[dict(stock_code="002072",ann_type="A,SZA",market_code="0")],columns=[])])))
    receipt=put("receipt.json",dict(url=archive_module.API,status=200,sha256=api["sha256"],params=dict(stock_list="002072",f_node="0",s_node="0",ann_type="A",page_size="100",page_index="1",sr="-1",begin_time="1990-01-01",end_time="2026-10-29")))
    row=dict(source_row_binding=dict(path="source.json",index=0),source_provider="eastmoney",announcement_id=art,issuer_symbol="002072",notice_date="2010-05-05",title=title,provider_title="issuer:"+title,title_transform=dict(removed_prefix="issuer:"),source_url=f"https://pdf.dfcfw.com/pdf/H2_{art}_1.pdf")
    records=put("rows.json",[row]);payload=dict(schema_version=2,selection_revision="all-api-pages-risk-title-or-category-v1",date_policy=DATE_POLICY,registry_sha256="f"*64,symbols={"002072":dict(status="complete_symbol_archive",source_files=[api,receipt],records=records,partitions=[dict(pages=[dict(response_path="source.json",receipt_path="receipt.json")])])})
    spec=put("manifest.json",payload);monkeypatch.setenv("MICROCAP_ST_NOTICE_ARCHIVE","manifest.json");monkeypatch.setenv("MICROCAP_ST_NOTICE_ARCHIVE_SHA256",spec["sha256"])
    return tmp_path,monkeypatch

def call(root,end="20261029"):return archive_module.load_notice_archive(root,"002072","19900101",end)
def test_native_identity_is_preserved(archive):
    root,_=archive;assert call(root)["rows"][0]["announcement_id"].startswith("AN")
@pytest.mark.parametrize("target",["source.json","rows.json","manifest.json"])
def test_bound_bytes_change_blocks(archive,target):
    root,_=archive;p=root/target;p.write_bytes(p.read_bytes()+b"changed")
    with pytest.raises(StNoticeEvidenceError):call(root)
@pytest.mark.parametrize("attack",["title","date","remove","provider"])
def test_rehashed_forgery_still_rejected(archive,attack):
    root,monkeypatch=archive;p=root/"rows.json";rows=json.loads(p.read_bytes())
    if attack=="title":rows[0]["title"]="关于撤销其他风险警示的公告"
    elif attack=="date":rows[0]["notice_date"]="2010-05-06"
    elif attack=="provider":rows[0]["source_provider"]="cninfo"
    else:rows=[]
    p.write_text(json.dumps(rows),encoding="utf-8");manifest=json.loads((root/"manifest.json").read_bytes());manifest["symbols"]["002072"]["records"]["sha256"]=hashlib.sha256(p.read_bytes()).hexdigest();m=root/"manifest.json";m.write_text(json.dumps(manifest),encoding="utf-8");monkeypatch.setenv("MICROCAP_ST_NOTICE_ARCHIVE_SHA256",hashlib.sha256(m.read_bytes()).hexdigest())
    with pytest.raises((StNoticeEvidenceError,KeyError)):call(root)
def test_actual_request_horizon_enforced(archive):
    with pytest.raises(StNoticeEvidenceError):call(archive[0],"20261030")
def test_partial_activation_blocks(archive):
    root,monkeypatch=archive;monkeypatch.delenv("MICROCAP_ST_NOTICE_ARCHIVE_SHA256")
    with pytest.raises(StNoticeEvidenceError):call(root)
