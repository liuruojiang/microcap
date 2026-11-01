import copy,json
from pathlib import Path
import pytest
from scripts.st_history_scope import _daily_notice_row
from scripts.st_notice_evidence import StNoticeEvidenceError,load_verified_events
ROOT=Path(__file__).resolve().parents[1]
@pytest.fixture
def actual():
 reg=json.loads((ROOT/'data/st_notice_evidence/registry.json').read_bytes());e=next(e for e in reg['events'] if e['announcement_id']=='AN201203030004915150')
 p=ROOT/'tests/fixtures/st_recovery_listing_real/notice_row.json';r=json.loads(p.read_bytes())
 return r,e
def test_real_recovery_without_review_blocks(actual):
 r,e=actual
 with pytest.raises(StNoticeEvidenceError,match='exact reviewed'):_daily_notice_row(r,'600633','2011-09-23',{})
def test_real_reviewed_recovery_retains_notice_identity(actual):
 r,e=actual;row=_daily_notice_row(r,'600633','2011-09-23',{e['announcement_id']:e});v=load_verified_events(ROOT,'600633','2011-09-23','2011-09-23',[row]);assert v[e['announcement_id']]['risk_actions']==e['risk_actions']
@pytest.mark.parametrize('field,value',[('symbol','600634'),('notice_date','2011-09-24'),('title','其他公告'),('source_provider','cninfo'),('announcement_id','OTHER')])
def test_wrong_review_identity_blocks(actual,field,value):
 r,e=actual;e[field]=value
 with pytest.raises(StNoticeEvidenceError):_daily_notice_row(r,'600633','2011-09-23',{'AN201203030004915150':e})
def test_wrong_raw_issuer_blocks(actual):
 r,e=actual;r['codes'][0]['stock_code']='600634'
 with pytest.raises(StNoticeEvidenceError):_daily_notice_row(r,'600633','2011-09-23',{e['announcement_id']:e})
def test_category_alone_requires_review(actual):
 r,e=actual;r['title']='关于公司情况的公告'
 with pytest.raises(StNoticeEvidenceError):_daily_notice_row(r,'600633','2011-09-23',{})
def test_malformed_category_blocks(actual):
 r,e=actual;r['columns']=[{}]
 with pytest.raises(StNoticeEvidenceError):_daily_notice_row(r,'600633','2011-09-23',{e['announcement_id']:e})
def test_reviewed_nonexit_is_not_guessed_as_clear(actual):
 r,e=actual;e['risk_actions']={};before=copy.deepcopy(e);row=_daily_notice_row(r,'600633','2011-09-23',{e['announcement_id']:e});assert e==before and 'risk_actions' not in row

def test_raw_date_cannot_be_replaced_with_query_date(actual):
 r,e=actual;r['notice_date']='2011-09-24 00:00:00'
 with pytest.raises(StNoticeEvidenceError):_daily_notice_row(r,'600633','2011-09-23',{e['announcement_id']:e})
