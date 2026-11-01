import copy,json,hashlib
from pathlib import Path
import pytest
from scripts.st_notice_evidence import evidence_fingerprint,load_verified_events,StNoticeEvidenceError
ROOT=Path(__file__).resolve().parents[1]
@pytest.fixture
def exchange_root(tmp_path):
 source=ROOT/'data/st_notice_evidence';payload=json.loads((source/'registry.json').read_text(encoding='utf8'));event=copy.deepcopy(next(e for e in payload['events'] if e.get('source_type')=='reviewed_exchange_html'));payload['events']=[event];target=tmp_path/'data/st_notice_evidence';target.mkdir(parents=True)
 for name in [event['relative_html_path']]+[v['relative_path'] for v in event['exchange_evidence'].values()]:(target/name).write_bytes((source/name).read_bytes())
 (target/'registry.json').write_text(json.dumps(payload,ensure_ascii=False),encoding='utf8')
 return tmp_path,target,payload,event

def test_real_exchange_html_and_issuer_binding(exchange_root):
 root,_,_,event=exchange_root
 row=dict(announcement_id=event['announcement_id'],issuer_symbol=event['symbol'],notice_date=event['notice_date'],title=event['title'],source_url=event['source_url'],source_provider='sse')
 assert load_verified_events(root,'600556','2010-01-01','2026-10-29',[row])[event['announcement_id']]['risk_actions']=={'other':True}
 row['source_provider']='cninfo'
 with pytest.raises(StNoticeEvidenceError,match='provider'):load_verified_events(root,'600556','2010-01-01','2026-10-29',[row])

def test_exchange_html_changed_bytes_block(exchange_root):
 root,target,_,event=exchange_root;(target/event['relative_html_path']).write_bytes(b'changed')
 with pytest.raises(StNoticeEvidenceError,match='hash mismatch'):evidence_fingerprint(root)

def test_exchange_issuer_deleted_even_rehashed_blocks(exchange_root):
 root,target,payload,event=exchange_root;spec=event['exchange_evidence']['provider_response'];path=target/spec['relative_path'];api=json.loads(path.read_bytes())
 for row in api['data']['list']:row['codes']=[]
 raw=json.dumps(api).encode();path.write_bytes(raw);spec['sha256']=hashlib.sha256(raw).hexdigest();review_spec=event['exchange_evidence']['review'];rp=target/review_spec['relative_path'];review=json.loads(rp.read_bytes());review['evidence']['provider_response']=spec;raw=json.dumps(review).encode();rp.write_bytes(raw);review_spec['sha256']=hashlib.sha256(raw).hexdigest();(target/'registry.json').write_text(json.dumps(payload),encoding='utf8')
 with pytest.raises(StNoticeEvidenceError,match='equity issuer'):evidence_fingerprint(root)

def test_exchange_url_date_disagrees_blocks(exchange_root):
 root,target,payload,event=exchange_root;event['notice_date']='2016-09-10';event['announcement_id']='SSE_20160910_600556';(target/'registry.json').write_text(json.dumps(payload),encoding='utf8')
 with pytest.raises(StNoticeEvidenceError,match='URL/publication'):evidence_fingerprint(root)
