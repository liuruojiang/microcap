import hashlib,json,copy
from pathlib import Path
import pytest
from scripts.st_history_scope import validate_scope,resolve_archive_reference,source_closure
from scripts.st_notice_evidence import StNoticeEvidenceError
ROOT=Path(__file__).resolve().parents[1]
@pytest.fixture
def scope_case(tmp_path):
 row=json.loads((ROOT/'tests/fixtures/st_equity_identity_real/notice_row.json').read_text(encoding='utf8'))
 day=row['notice_date'][:10];files=[]
 def save(path,obj):
  raw=json.dumps(obj,ensure_ascii=False).encode();(tmp_path/path).write_bytes(raw);files.append(dict(path=path,sha256=hashlib.sha256(raw).hexdigest()));return raw
 names=dict(success=True,result=dict(pages=1,count=1,data=[dict(SECURITY_CODE='000001',CHANGE_DATE='1991-04-03 00:00:00',CHANGE_AFTER_FN='深发展A',TRADE_MARKET_TYPE='2',RANK=1,SECUCODE='000001.SZ')]))
 raw=save('names.json',names);save('names.receipt.json',dict(status=200,url='https://datacenter-web.eastmoney.com/api/data/v1/get',sha256=hashlib.sha256(raw).hexdigest(),params=dict(reportName='RPT_IPO_ABSTOCK',pageSize=1000,pageNumber=1,sortColumns='SECUCODE,CHANGE_DATE,RANK')))
 risk=dict(success=1,data=dict(list=[row],total_hits=1,page_index=1,page_size=100));raw=save('risk.json',risk);save('risk.receipt.json',dict(status=200,url='https://np-anotice-stock.eastmoney.com/api/security/ann',sha256=hashlib.sha256(raw).hexdigest(),params=dict(stock_list='',f_node=3,s_node=0,ann_type='A',page_size=100,sr=-1,page_index=1,begin_time=day,end_time=day)))
 scope=dict(start_date=day,end_date=day,name_pages=[dict(response_path='names.json',receipt_path='names.receipt.json')],risk_partitions=[dict(pages=[dict(response_path='risk.json',receipt_path='risk.receipt.json')])],source_files=files)
 def mutate(path,fn):
  obj=json.loads((tmp_path/path).read_bytes());fn(obj);raw=json.dumps(obj,ensure_ascii=False).encode();(tmp_path/path).write_bytes(raw)
  for spec in files:
   if spec['path']==path:spec['sha256']=hashlib.sha256(raw).hexdigest()
  receipt=path.replace('.json','.receipt.json')
  if not path.endswith('.receipt.json'):
   obj=json.loads((tmp_path/receipt).read_bytes());obj['sha256']=hashlib.sha256(raw).hexdigest();mutate(receipt,lambda d:d.update(obj))
 return tmp_path,scope,mutate

def test_actual_index_collision_stays_out(scope_case):
 root,scope,_=scope_case;data=validate_scope(root,scope,force=True)
 assert '600593' in data['notices'] and '000001' not in data['notices']
 assert data['absolute_history_complete'] is False

@pytest.mark.parametrize('change',[
 lambda p:p['data']['list'][0].update(codes=[]),
 lambda p:p['data']['list'][0].update(notice_date=p['data']['list'][0]['notice_date']+'INVALID'),
 lambda p:p['data'].update(total_hits=101),
])
def test_corrupt_risk_cannot_certify_empty(scope_case,change):
 root,scope,mutate=scope_case;mutate('risk.json',change)
 with pytest.raises(StNoticeEvidenceError):validate_scope(root,scope,force=True)

@pytest.mark.parametrize('change',[
 lambda r:r.update(CHANGE_DATE='1991-04-03 garbage'),
 lambda r:r.update(SECURITY_CODE='1'),
 lambda r:r.update(TRADE_MARKET_TYPE='1'),
 lambda r:r.update(RANK=2),
])
def test_corrupt_name_chain_blocks(scope_case,change):
 root,scope,mutate=scope_case;mutate('names.json',lambda p:change(p['result']['data'][0]))
 with pytest.raises(StNoticeEvidenceError):validate_scope(root,scope,force=True)

def test_cache_changed_source_is_revalidated(scope_case):
 root,scope,_=scope_case;validate_scope(root,scope);(root/'risk.json').write_text('{}')
 with pytest.raises(StNoticeEvidenceError,match='hash mismatch'):validate_scope(root,scope)

def test_missing_pointer_for_bound_metadata_blocks(tmp_path,monkeypatch):
 for k in ('MICROCAP_ST_NOTICE_ARCHIVE','MICROCAP_ST_NOTICE_ARCHIVE_SHA256'):monkeypatch.delenv(k,raising=False)
 p=tmp_path/'.microcap_index_cache/security_meta';p.mkdir(parents=True);(p/'000001.json').write_text(json.dumps(dict(st_notice_archive_sha256='a'*64)))
 with pytest.raises(StNoticeEvidenceError,match='no portable'):source_closure(tmp_path)

def test_pointer_conflicting_environment_blocks(tmp_path,monkeypatch):
 p=tmp_path/'.microcap_index_cache';p.mkdir();(p/'st_notice_archive_pointer.json').write_text(json.dumps(dict(manifest_path='scope.json',manifest_sha256='a'*64)))
 monkeypatch.setenv('MICROCAP_ST_NOTICE_ARCHIVE','other.json');monkeypatch.setenv('MICROCAP_ST_NOTICE_ARCHIVE_SHA256','a'*64)
 with pytest.raises(StNoticeEvidenceError,match='conflicts'):resolve_archive_reference(tmp_path)

def test_portable_install_preserves_raw_and_rebinds_paths(scope_case,monkeypatch):
 from scripts.st_history_scope import install_source_archive,read_bound
 root,scope,_=scope_case
 for k in ('MICROCAP_ST_NOTICE_ARCHIVE','MICROCAP_ST_NOTICE_ARCHIVE_SHA256'):monkeypatch.delenv(k,raising=False)
 raw=json.dumps(scope).encode();(root/'scope.json').write_bytes(raw)
 manifest=dict(schema_version=3,scope=dict(path='scope.json',sha256=hashlib.sha256(raw).hexdigest()),symbols={},daily_increments=[])
 raw=json.dumps(manifest).encode();(root/'archive.json').write_bytes(raw);digest=hashlib.sha256(raw).hexdigest()
 installed=install_source_archive(root,root/'isolated','archive.json',digest)
 destination=root/'isolated';new_manifest=json.loads((destination/installed['manifest_path']).read_bytes());new_scope=json.loads(read_bound(destination,new_manifest['scope']))
 assert validate_scope(destination,new_scope,force=True)['names']['000001'][0]['CHANGE_AFTER_FN']=='深发展A'
 for before,after in zip(scope['source_files'],new_scope['source_files']):
  assert (root/before['path']).read_bytes()==(destination/after['path']).read_bytes()
 assert all(p.startswith('.microcap_index_cache/') for p in installed['installed_source_files'])

def test_staged_batch_defers_mutation_detection_until_exit(scope_case,monkeypatch):
 from scripts.st_history_scope import validated_source_batch,validate_scope
 from scripts import st_notice_evidence as ev
 root,scope,_=scope_case
 for k in ('MICROCAP_ST_NOTICE_ARCHIVE','MICROCAP_ST_NOTICE_ARCHIVE_SHA256'):monkeypatch.delenv(k,raising=False)
 source=ROOT/'data/st_notice_evidence';payload=json.loads((source/'registry.json').read_bytes());event=next(e for e in payload['events'] if e.get('source_type')=='reviewed_exchange_html');payload['events']=[event];folder=root/'data/st_notice_evidence';folder.mkdir(parents=True);raw=json.dumps(payload).encode();(folder/'registry.json').write_bytes(raw)
 for name in [event['relative_html_path']]+[x['relative_path'] for x in event['exchange_evidence'].values()]:(folder/name).write_bytes((source/name).read_bytes())
 for name in ('scripts/st_notice_evidence.py','scripts/st_notice_archive.py'):
  p=root/name;p.parent.mkdir(exist_ok=True);p.write_bytes((ROOT/name).read_bytes())
 raw=json.dumps(scope).encode();(root/'scope.json').write_bytes(raw);manifest=dict(schema_version=3,scope=dict(path='scope.json',sha256=hashlib.sha256(raw).hexdigest()),symbols={},daily_increments=[])
 raw=json.dumps(manifest).encode();(root/'archive.json').write_bytes(raw);p=root/'.microcap_index_cache';p.mkdir();(p/'st_notice_archive_pointer.json').write_text(json.dumps(dict(manifest_path='archive.json',manifest_sha256=hashlib.sha256(raw).hexdigest())))
 with validated_source_batch(root):
  with pytest.raises(StNoticeEvidenceError,match='Nested'):
   with validated_source_batch(root):pass
  assert ev.evidence_fingerprint(root)
 original=ev._registry
 with pytest.raises(StNoticeEvidenceError,match='hash mismatch'):
  with validated_source_batch(root):
   (root/'risk.json').write_text('{}')
 assert ev._registry is original
