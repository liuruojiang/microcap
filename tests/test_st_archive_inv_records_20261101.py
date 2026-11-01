import copy,hashlib,json
from pathlib import Path
import pytest
from test_st_notice_archive_20261030 import archive,call
from scripts.st_notice_archive import _nonrisk_investor_record
from scripts.st_notice_evidence import StNoticeEvidenceError
ROOT=Path(__file__).resolve().parents[1]
REAL=ROOT/'tests/fixtures/st_archive_inv_real/rows.json'
def test_real_18_INV_rows_remain_non_equity():
 rows=json.loads(REAL.read_bytes());assert len(rows)==18
 assert all(_nonrisk_investor_record(r['row'],r['symbol']) for r in rows)
def inject(root,monkeypatch,item,normalized=False):
 p=root/'source.json';d=json.loads(p.read_bytes());d['data']['list'].append(item);d['data']['total_hits']=2;p.write_text(json.dumps(d),encoding='utf8')
 r=root/'receipt.json';d=json.loads(r.read_bytes());d['sha256']=hashlib.sha256(p.read_bytes()).hexdigest();r.write_text(json.dumps(d),encoding='utf8')
 m=root/'manifest.json';d=json.loads(m.read_bytes())
 for spec in d['symbols']['002072']['source_files']:spec['sha256']=hashlib.sha256((root/spec['path']).read_bytes()).hexdigest()
 m.write_text(json.dumps(d),encoding='utf8');monkeypatch.setenv('MICROCAP_ST_NOTICE_ARCHIVE_SHA256',hashlib.sha256(m.read_bytes()).hexdigest())
def real():return copy.deepcopy(json.loads(REAL.read_bytes())[0]['row'])
def test_complete_page_keeps_INV_out_of_candidates(archive):
 root,mp=archive;inject(root,mp,real());r=call(root);assert len(r['rows'])==1;assert r['excluded_investor_records'][0]['announcement_id']=='AN202607091826849731'
@pytest.mark.parametrize('attack',['index','wrong_code','wrong_venue','risk_title','risk_category','empty_codes','unknown_type'])
def test_INV_scope_never_hides_identity_or_risk(archive,attack):
 root,mp=archive;r=real()
 if attack=='index':r['codes'][0]['ann_type']='Index'
 if attack=='unknown_type':r['codes'][0]['ann_type']='BOGUS'
 if attack=='wrong_code':r['codes'][0]['stock_code']='000001'
 if attack=='wrong_venue':r['codes'][0]['market_code']='1'
 if attack=='risk_title':r['title']='关于实施其他风险警示的公告'
 if attack=='risk_category':r['columns'][0]['column_code']='001002004003'
 if attack=='empty_codes':r['codes']=[]
 inject(root,mp,r)
 with pytest.raises(StNoticeEvidenceError):call(root)

@pytest.mark.parametrize('code,name',[('050001','媒体采访'),('050002','路演活动'),('050003','调研活动'),('050004','管理制度'),('050005','其他投资者关系')])
def test_each_observed_investor_category_is_excluded(archive,code,name):
 root,mp=archive;r=real();r['columns']=[dict(column_code=code,column_name=name)];inject(root,mp,r);assert len(call(root)['rows'])==1

def test_unknown_or_mislabeled_category_blocks(archive):
 root,mp=archive;r=real();r['columns']=[dict(column_code='050003',column_name='风险警示')];inject(root,mp,r)
 with pytest.raises(StNoticeEvidenceError):call(root)
