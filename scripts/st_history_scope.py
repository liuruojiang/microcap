"""Authenticated, source-bounded historical names plus market risk-query coverage.

This proves the provider's returned query scope, not an absolute absence of ST.
Raw pages remain authority. Process cache is invalidated by every source file's
size/mtime/ctime; audit callers can request force=True for full byte readback.
"""
from pathlib import Path
from datetime import date,timedelta,datetime
from collections import defaultdict
import hashlib,json,re
from scripts.st_notice_evidence import StNoticeEvidenceError,is_equity_issuer
API='https://np-anotice-stock.eastmoney.com/api/security/ann'
NAME_API='https://datacenter-web.eastmoney.com/api/data/v1/get'
_CACHE={}
def fail(message):raise StNoticeEvidenceError(message)
def _path(root,relative):
 p=Path(relative);target=(root/p).resolve()
 if p.is_absolute() or not target.is_relative_to(root):fail('ST scope source escapes root')
 return target
def read_bound(root,spec):
 raw=_path(root,spec['path']).read_bytes()
 if hashlib.sha256(raw).hexdigest()!=spec['sha256']:fail('ST scope source hash mismatch: '+spec['path'])
 return raw
def _signature(root,specs):
 return tuple((s['path'],s['sha256'],*(getattr(_path(root,s['path']).stat(),k) for k in ('st_size','st_mtime_ns','st_ctime_ns'))) for s in specs)
def validate_scope(root,scope,*,force=False):
 root=Path(root).resolve();specs=scope['source_files'];keys=[s['path'] for s in specs]
 if len(keys)!=len(set(keys)):fail('Duplicate ST scope source path')
 signature=_signature(root,specs);key=(str(root),hashlib.sha256(json.dumps(scope,sort_keys=True).encode()).hexdigest())
 if not force and key in _CACHE and _CACHE[key][0]==signature:return _CACHE[key][1]
 blobs={s['path']:read_bound(root,s) for s in specs};names=defaultdict(list);native=defaultdict(dict);page_numbers=set();count=None;pages=None;all_names=[]
 for item in scope['name_pages']:
  raw=blobs[item['response_path']];receipt=json.loads(blobs[item['receipt_path']]);p=receipt['params'];payload=json.loads(raw)
  if receipt.get('status')!=200 or receipt.get('url')!=NAME_API or receipt.get('sha256')!=hashlib.sha256(raw).hexdigest():fail('Name page receipt mismatch')
  if p.get('reportName')!='RPT_IPO_ABSTOCK' or p.get('filter') or str(p.get('pageSize'))!='1000' or p.get('sortColumns')!='SECUCODE,CHANGE_DATE,RANK':fail('Name query scope changed')
  if payload.get('success') is not True:fail('Name query unsuccessful')
  data=payload['result'];n=int(p['pageNumber'])
  if n in page_numbers:fail('Duplicate name page')
  page_numbers.add(n)
  if count is None:count=int(data['count']);pages=int(data['pages'])
  if count!=int(data['count']) or pages!=int(data['pages']):fail('Name query totals changed')
  if len(data['data'])!=min(500,max(0,count-(n-1)*500)):fail('Name page truncated')
  all_names.extend(data['data'])
 if page_numbers!=set(range(1,pages+1)) or pages!=(count+499)//500 or len(all_names)!=count:fail('Name pagination incomplete')
 for row in all_names:
  s=row['SECURITY_CODE']
  if not isinstance(s,str) or re.fullmatch(r'\d{6}',s) is None:fail('Invalid name issuer code')
  if not isinstance(row.get('RANK'),int) or isinstance(row['RANK'],bool) or row['RANK']<1:fail('Invalid name rank')
  if re.fullmatch(r'\d{4}-\d{2}-\d{2} 00:00:00',str(row.get('CHANGE_DATE',''))) is None:fail('Invalid name change date')
  datetime.strptime(row['CHANGE_DATE'],'%Y-%m-%d %H:%M:%S')
  venue='SH' if s.startswith(('6','900')) else 'SZ' if s.startswith(('0','3','2')) else None
  expected_market={'SH':'1','SZ':'2'}.get(venue)
  if venue is None:
   if not (row.get('TRADE_MARKET_TYPE')=='2' and row.get('SECUCODE')==s+'.BJ'):fail('Unknown name venue')
   continue
  if str(row.get('TRADE_MARKET_TYPE'))!=expected_market:fail('Name market identity mismatch')
  if row['SECUCODE']!=s+'.'+venue:fail('Name issuer/venue mismatch')
  if s.startswith(('900','2')):continue  # B shares are outside this A-share source scope.
  date.fromisoformat(row['CHANGE_DATE'][:10]);names[s].append(row)
 for s,rows in names.items():
  if sorted(int(x['RANK']) for x in rows)!=list(range(1,len(rows)+1)):fail('Name history ranks incomplete')
  rows.sort(key=lambda x:(x['CHANGE_DATE'],-int(x['RANK'])))
 ranges=[]
 for partition in scope['risk_partitions']:
  seen=set();total=None;query=None;ids=set()
  for page in partition['pages']:
   raw=blobs[page['response_path']];receipt=json.loads(blobs[page['receipt_path']]);p=receipt['params'];payload=json.loads(raw)
   if receipt.get('status')!=200 or receipt.get('url')!=API or receipt.get('sha256')!=hashlib.sha256(raw).hexdigest():fail('Risk page receipt mismatch')
   if any(str(p.get(k))!=v for k,v in {'stock_list':'','f_node':'3','s_node':'0','ann_type':'A','page_size':'100','sr':'-1'}.items()):fail('Risk query scope changed')
   q=(p['begin_time'],p['end_time'])
   if query is None:query=q
   if q!=query:fail('Risk partition dates changed')
   if payload.get('success')!=1:fail('Risk query unsuccessful')
   data=payload['data'];n=int(data['page_index']);t=int(data['total_hits'])
   if total is None:total=t
   if t!=total or n in seen or int(p['page_index'])!=n or int(data['page_size'])!=100:fail('Risk pagination identity changed')
   seen.add(n)
   if len(data['list'])!=max(0,min(100,total-(n-1)*100)):fail('Risk page truncated')
   for index,row in enumerate(data['list']):
    from scripts.st_notice_incremental import _provider_time
    notice_day=_provider_time(row.get('notice_date')).date().isoformat()
    codes=row.get('codes')
    if not isinstance(codes,list) or not codes or any(not isinstance(c,dict) or any(not isinstance(c.get(k),str) or not c[k] for k in ('stock_code','market_code','ann_type')) for c in codes):fail('Risk issuer identity unresolved')
    if not q[0]<=notice_day<=q[1]:fail('Risk notice outside query')
    if not isinstance(row.get('art_code'),str) or re.fullmatch(r'AN\d+',row['art_code']) is None or not isinstance(row.get('title'),str):fail('Risk notice identity invalid')
    art=row['art_code']
    if art in ids:fail('Duplicate risk notice ID')
    ids.add(art)
    for code in row.get('codes',[]):
     s=code.get('stock_code','')
     if not is_equity_issuer(row,s):continue
     value=dict(raw=row,binding=dict(path=page['response_path'],index=index))
     if art in native[s] and native[s][art]['raw']!=row:fail('Conflicting market notice identity')
     native[s][art]=value
  if query is None or seen!=set(range(1,max(1,(total+99)//100)+1)) or len(ids)!=total:fail('Risk pagination incomplete')
  ranges.append(query)
 cursor=date.fromisoformat(scope['start_date']);end=date.fromisoformat(scope['end_date'])
 for a,b in sorted(ranges):
  a=date.fromisoformat(a);b=date.fromisoformat(b)
  if a>cursor:break
  if b>=cursor:cursor=b+timedelta(days=1)
 if cursor<=end:fail('Global risk query date gap')
 if _signature(root,specs)!=signature:fail('ST source changed during validation')
 result=dict(names=dict(names),notices=dict(native),start_date=scope['start_date'],end_date=scope['end_date'],source_boundary='complete_returned_provider_name_chain_plus_global_risk_category_query',absolute_history_complete=False)
 _CACHE[key]=(signature,result);return result

def symbol_name_changes(scope_data,symbol):
 rows=scope_data['names'].get(symbol)
 if not rows:fail('Name history missing for issuer')
 out=[];old=''
 for r in rows:
  out.append(dict(change_date=r['CHANGE_DATE'][:10],old_name=old,new_name=r['CHANGE_AFTER_FN'],source='eastmoney_RPT_IPO_ABSTOCK'));old=r['CHANGE_AFTER_FN']
 return out

def normalized_risk_rows(scope_data,symbol):
 out=[]
 for art,item in scope_data['notices'].get(symbol,{}).items():
  row=item['raw'];title=row['title']
  if not any(word in title for word in ('风险警示','特别处理','摘帽')) and not any(c.get('column_code','').startswith('001002004003') for c in row.get('columns',[])):continue
  clean=re.split('[:：]',title,maxsplit=1)[-1]
  out.append(dict(source_row_binding=item['binding'],source_provider='eastmoney',announcement_id=art,issuer_symbol=symbol,notice_date=row['notice_date'][:10],provider_notice_date=row['notice_date'],provider_title=title,title=clean,title_transform=dict(operation='strip_issuer_prefix' if clean!=title else 'identity',removed_prefix=title[:-len(clean)] if clean else title),source_url=f'https://pdf.dfcfw.com/pdf/H2_{art}_1.pdf'))
 return sorted(out,key=lambda r:(r['notice_date'],r['announcement_id']))


def _daily_notice_row(item,symbol,day,registry):
 from scripts.st_notice_evidence import is_equity_issuer
 from scripts.st_notice_incremental import _provider_time
 if not is_equity_issuer(item,symbol):fail('Daily notice issuer mismatch')
 if _provider_time(item.get('notice_date')).date().isoformat()!=day:fail('Daily notice date mismatch')
 title=item['title'];columns=item.get('columns',[])
 if not isinstance(columns,list) or any(not isinstance(c,dict) or not isinstance(c.get('column_code'),str) for c in columns):fail('Daily notice category malformed')
 recovery='恢复上市' in title or any(c['column_code'].startswith('001002004005') for c in columns)
 if not recovery and not any(w in title for w in ('风险警示','特别处理','摘帽')) and not any(c['column_code'].startswith('001002004003') for c in columns):return None
 art=item['art_code'];clean=re.split('[:：]',title,maxsplit=1)[-1]
 row=dict(source_provider='eastmoney',announcement_id=art,issuer_symbol=symbol,notice_date=day,provider_title=title,title=clean,title_transform=dict(removed_prefix=title[:-len(clean)] if clean else title),source_url=f'https://pdf.dfcfw.com/pdf/H2_{art}_1.pdf')
 if recovery:
  event=registry.get(art)
  if not event or any(event.get(k)!=v for k,v in dict(symbol=symbol,announcement_id=art,notice_date=day,title=clean,source_provider='eastmoney',provider_title=title,source_url=row['source_url']).items()):fail('Daily recovery listing requires exact reviewed issuer/body evidence: '+art)
 return row

def load_scope_archive(root,symbol,start_date,end_date,manifest,fingerprint):
 from scripts.st_notice_evidence import _registry
 from scripts.st_notice_archive import _load_notice_archive
 if manifest.get('date_policy')!='notice_date_exclusion_inclusive_exit_unchanged' or manifest.get('selection_revision')!='names-plus-global-risk-source-bounded-v1':fail('Unsupported source-bounded ST policy')
 registry,registry_sha=_registry(root)
 if manifest.get('registry_sha256')!=registry_sha:fail('ST scope registry differs')
 entry=manifest.get('symbols',{}).get(symbol)
 if not entry or entry.get('status') not in {'normal_name_chain','complete_symbol_notice_chain','source_bounded_market_notice_chain'}:fail('ST source coverage remains unresolved for '+symbol)
 scope=json.loads(read_bound(root,manifest['scope']));data=validate_scope(root,scope)
 parse=lambda x:datetime.strptime(str(x),'%Y%m%d').date() if '-' not in str(x) else date.fromisoformat(str(x))
 start,end=parse(start_date),parse(end_date);baseline=date.fromisoformat(data['end_date']);changes=symbol_name_changes(data,symbol)
 if start<date.fromisoformat(data['start_date']):fail('ST source starts after requested history')
 if entry['status']=='complete_symbol_notice_chain':
  legacy_raw=read_bound(root,manifest['legacy_archive']);legacy=json.loads(legacy_raw)
  result=_load_notice_archive(root,symbol,start_date,min(end,baseline).isoformat(),_manifest=legacy,_expected=manifest['legacy_archive']['sha256'])
  rows=result['rows']
 else:
  # No-ST certificate is source bounded; an unreviewed risk name is never empty.
  first=date.fromisoformat(entry['first_sample_date']);relevant=[r for r in changes if date.fromisoformat(r['change_date'])<=end]
  if not relevant or date.fromisoformat(relevant[0]['change_date'])>first:fail('Ordinary name chain has unknown prefix')
  for i,r in enumerate(relevant):
   nextday=date.fromisoformat(relevant[i+1]['change_date']) if i+1<len(relevant) else end+timedelta(days=1)
   if entry['status']=='normal_name_chain' and nextday>first and re.match(r'^\s*(?:[SG]\s*)?\*?\s*(?:ST|PT)(?![A-Z])',r['new_name'].upper()):fail('Risk name cannot use ordinary-source certificate')
  rows=[r for r in normalized_risk_rows(data,symbol) if start<=date.fromisoformat(r['notice_date'])<=min(end,baseline)]
  # Reviewed identities omitted by the category index must remain actual evidence.
  for art,e in registry.items():
   if e['symbol']!=symbol or not start<=date.fromisoformat(e['notice_date'])<=min(end,baseline) or any(r['announcement_id']==art for r in rows):continue
   row=dict(issuer_symbol=symbol,announcement_id=art,notice_date=e['notice_date'],title=e['title'],source_url=e['source_url'],source_provider=e.get('source_provider','cninfo'))
   if row['source_provider']=='eastmoney':row.update(provider_title=e['provider_title'],title_transform=dict(removed_prefix=e['removed_issuer_prefix']),body_source_url=e['body_source_url'])
   rows.append(row)
 cursor=baseline+timedelta(days=1);tail_specs={x['date']:x for x in manifest.get('daily_increments',[])}
 from scripts.st_notice_incremental import verify_day
 while cursor<=end:
  day=cursor.isoformat();spec=tail_specs.get(day)
  if spec is None:fail('Daily notice increment missing for '+day)
  read_bound(root,spec['manifest']);folder=_path(root,spec['manifest']['path']).parent;verified=verify_day(folder,day)
  for item in verified['by_symbol'].get(symbol,[]):
   row=_daily_notice_row(item,symbol,day,registry)
   if row is not None:rows.append(row)
  cursor+=timedelta(days=1)
 return dict(rows=sorted(rows,key=lambda r:(r['notice_date'],r['announcement_id'])),manifest_sha256=fingerprint,source_providers=['eastmoney','cninfo_reviewed'],notice_query_start_date=data['start_date'],notice_query_end_date=end.isoformat(),name_changes=changes,name_history_source='eastmoney_RPT_IPO_ABSTOCK',source_boundary=data['source_boundary'],absolute_history_complete=False)

POINTER_PATH='.microcap_index_cache/st_notice_archive_pointer.json'
def resolve_archive_reference(root):
 import os
 root=Path(root).resolve();configured=os.environ.get('MICROCAP_ST_NOTICE_ARCHIVE');expected=os.environ.get('MICROCAP_ST_NOTICE_ARCHIVE_SHA256');pointer=root/POINTER_PATH
 if pointer.exists():
  data=json.loads(pointer.read_bytes());relative=data.get('manifest_path');digest=data.get('manifest_sha256')
  if not isinstance(relative,str) or not isinstance(digest,str) or re.fullmatch(r'[0-9a-f]{64}',digest) is None:fail('Invalid ST archive pointer')
  target=_path(root,relative)
  if configured or expected:
   supplied=Path(configured or '');supplied=supplied.resolve() if supplied.is_absolute() else _path(root,str(supplied))
   if not configured or expected!=digest or supplied!=target:fail('ST archive environment conflicts with portable pointer')
  configured=relative;expected=digest
 if bool(configured)!=bool(expected):fail('ST archive reference is incomplete')
 return configured,expected

def source_closure(root,manifest_path=None,expected_sha=None):
 """Return verified root-relative path -> SHA256 for portable source transport."""
 import os
 root=Path(root).resolve()
 if manifest_path is None and expected_sha is None:manifest_path,expected_sha=resolve_archive_reference(root)
 if not manifest_path and not expected_sha:
  for p in (root/'.microcap_index_cache/security_meta').glob('*.json'):
   if json.loads(p.read_bytes()).get('st_notice_archive_sha256'):fail('Bound ST metadata has no portable archive reference')
  return {}
 if not manifest_path or not expected_sha:fail('ST source closure requires manifest path and hash')
 p=Path(manifest_path);p=p.resolve() if p.is_absolute() else _path(root,str(p))
 if not p.is_relative_to(root):fail('ST source manifest escapes root')
 result={}
 pointer=root/POINTER_PATH
 if pointer.exists():result[POINTER_PATH]=hashlib.sha256(pointer.read_bytes()).hexdigest()
 def add(spec):
  if spec['path'] in result and result[spec['path']]!=spec['sha256']:fail('Conflicting ST closure identity')
  result[spec['path']]=spec['sha256']
 spec=dict(path=p.relative_to(root).as_posix(),sha256=expected_sha);add(spec);manifest=json.loads(read_bound(root,spec))
 def legacy(m):
  for entry in m.get('symbols',{}).values():
   for x in entry.get('source_files',[]):add(x)
   add(entry['records'])
   for link in entry.get('crosswalk',[]):
    if link.get('mirror_pdf'):add(link['mirror_pdf'])
 if manifest.get('schema_version')==2:legacy(manifest)
 elif manifest.get('schema_version')==3:
  add(manifest['scope']);scope=json.loads(read_bound(root,manifest['scope']))
  for x in scope['source_files']:add(x)
  if manifest.get('legacy_archive'):
   add(manifest['legacy_archive']);legacy(json.loads(read_bound(root,manifest['legacy_archive'])))
  from scripts.st_notice_incremental import verify_day
  for day in manifest.get('daily_increments',[]):
   add(day['manifest']);read_bound(root,day['manifest'])
   folder=_path(root,day['manifest']['path']).parent;verified=verify_day(folder,day['date'])
   if verified.get('manifest_sha256')!=day['manifest']['sha256']:
    fail('ST daily manifest changed during verification')
   for name,digest in verified['source_files'].items():add(dict(path=(folder/name).relative_to(root).as_posix(),sha256=digest))
 else:fail('Unsupported ST closure schema')
 # Every unique declared source is still read and hashed on every call. Bound
 # concurrency removes serial disk/path overhead, without a stat-only cache or
 # persistence of a previous validation result. Dynamic manifests above are
 # authenticated before parsing and read back again here to detect replacement.
 from collections import deque
 from concurrent.futures import ThreadPoolExecutor
 def verify(item):
  relative,digest=item
  read_bound(root,dict(path=relative,sha256=digest))
 entries=iter(result.items());pending=deque()
 with ThreadPoolExecutor(max_workers=4) as readers:
  for _ in range(8):
   item=next(entries,None)
   if item is not None:pending.append(readers.submit(verify,item))
  while pending:
   pending.popleft().result()
   item=next(entries,None)
   if item is not None:pending.append(readers.submit(verify,item))
 return result


def install_source_archive(source_root,destination_root,manifest_path,expected_sha):
 """Install only into an explicitly supplied replay/runtime root; raw bytes stay exact."""
 import shutil
 source_root=Path(source_root).resolve();destination_root=Path(destination_root).resolve()
 if source_root==destination_root:fail('ST source installation requires an isolated destination root')
 closure=source_closure(source_root,manifest_path,expected_sha);p=Path(manifest_path);p=p.resolve() if p.is_absolute() else _path(source_root,str(p));manifest_relative=p.relative_to(source_root).as_posix();manifest=json.loads(p.read_bytes())
 prefix='.microcap_index_cache/st_notice_sources';derived={manifest_relative};daily_dirs={}
 if manifest.get('schema_version')==3:
  derived.add(manifest['scope']['path'])
  if manifest.get('legacy_archive'):
   derived.add(manifest['legacy_archive']['path']);legacy=json.loads(read_bound(source_root,manifest['legacy_archive']))
  else:legacy={}
  for item in manifest.get('daily_increments',[]):
   folder=Path(item['manifest']['path']).parent.as_posix();daily_dirs[folder]=prefix+'/daily/'+item['date']
 else:legacy=manifest
 for entry in legacy.get('symbols',{}).values():derived.add(entry['records']['path'])
 mapping={}
 for relative in closure:
  if relative==POINTER_PATH:continue
  parent=Path(relative).parent.as_posix()
  if parent in daily_dirs:mapping[relative]=daily_dirs[parent]+'/'+Path(relative).name
  else:mapping[relative]=prefix+'/objects/'+hashlib.sha256(relative.encode()).hexdigest()[:20]+Path(relative).suffix
 written={};inflight=set()
 def materialize(relative):
  if relative in written:return written[relative]
  if relative in inflight:fail('Cyclic derived ST source references')
  inflight.add(relative);raw=_path(source_root,relative).read_bytes()
  if hashlib.sha256(raw).hexdigest()!=closure[relative]:fail('ST source changed during portable installation')
  if relative in derived:
   def rewrite(obj):
    if isinstance(obj,list):return [rewrite(x) for x in obj]
    if isinstance(obj,dict):
     original_path=obj.get('path');value={k:rewrite(v) for k,v in obj.items()}
     if original_path in mapping and 'sha256' in value:value['sha256']=materialize(original_path)['sha256']
     return value
    if isinstance(obj,str) and obj in mapping:return mapping[obj]
    return obj
   raw=(json.dumps(rewrite(json.loads(raw)),ensure_ascii=False,indent=2)+'\n').encode('utf8')
  target=_path(destination_root,mapping[relative]);target.parent.mkdir(parents=True,exist_ok=True)
  if target.exists() and target.read_bytes()!=raw:fail('Portable ST target already contains different evidence')
  target.write_bytes(raw);written[relative]=dict(path=mapping[relative],sha256=hashlib.sha256(raw).hexdigest());inflight.remove(relative);return written[relative]
 for relative in mapping:materialize(relative)
 final=written[manifest_relative];pointer=destination_root/POINTER_PATH;pointer.parent.mkdir(parents=True,exist_ok=True);payload=dict(manifest_path=final['path'],manifest_sha256=final['sha256'])
 if pointer.exists() and json.loads(pointer.read_bytes())!=payload:fail('Destination ST pointer already identifies different history')
 pointer.write_text(json.dumps(payload,indent=2)+'\n',encoding='utf8')
 installed=source_closure(destination_root,final['path'],final['sha256'])
 if not all(path==POINTER_PATH or path.startswith(prefix+'/') for path in installed):fail('Portable ST source escaped cache prefix')
 if closure!=source_closure(source_root,manifest_path,expected_sha):fail('Source ST archive changed during installation')
 return dict(payload,source_manifest_sha256=expected_sha,installed_source_files=installed,raw_bytes_preserved=True,derived_paths_rebound=True)

from contextlib import contextmanager
_ACTIVE_BATCH=False
@contextmanager
def validated_source_batch(root):
 """Authenticate once for staged metadata work; reject the entire batch on any change.

Callers must publish no metadata until this context exits successfully. Single
lookups outside this explicit context retain all ordinary per-source checks.
"""
 global _ACTIVE_BATCH
 if _ACTIVE_BATCH:fail('Nested ST source batches are forbidden')
 import scripts.st_notice_evidence as ev
 import scripts.st_notice_archive as archive
 import scripts.st_notice_incremental as daily
 root=Path(root).resolve();configured,expected=resolve_archive_reference(root)
 if not configured:fail('ST batch requires a bound portable archive')
 path=Path(configured);path=path if path.is_absolute() else root/path;manifest=json.loads(path.read_bytes());before=source_closure(root,configured,expected)
 real_registry=ev._registry;real_archive_registry=archive._registry;real_validate=globals()['validate_scope'];real_day=daily.verify_day
 records,registry_sha=real_registry(root);evidence_files=ev.evidence_source_files(root)
 extra=['scripts/st_history_scope.py','scripts/st_notice_incremental.py','scripts/st_notice_archive.py','scripts/st_notice_evidence.py','scripts/st_metadata_refresh.py','scripts/st_notice_refresh.py','analyze_top100_rebalance_frequency.py','microcap_top100_mom16_biweekly_live_v2_0.py']
 evidence_files=sorted(set(evidence_files+[p for p in extra if (root/p).exists()]));evidence_before={p:hashlib.sha256((root/p).read_bytes()).hexdigest() for p in evidence_files}
 scope=None;scope_data=None;scope_key=None
 digest=lambda obj:hashlib.sha256(json.dumps(obj,sort_keys=True,ensure_ascii=False).encode()).hexdigest()
 if manifest.get('schema_version')==3:
  scope=json.loads(read_bound(root,manifest['scope']));scope_key=digest(scope);scope_data=real_validate(root,scope,force=True)
 days={}
 for item in manifest.get('daily_increments',[]):
  folder=_path(root,item['manifest']['path']).parent;days[(str(folder.resolve()),item['date'])]=real_day(folder,item['date'])
 cache_registry_digest=digest(records);cache_scope_digest=digest(scope_data);cache_days_digest=digest(days) if not days else hashlib.sha256(repr(days).encode()).hexdigest()
 def registry_at(request_root):
  if Path(request_root).resolve()!=root:fail('ST batch registry root mismatch')
  return records,registry_sha
 def scope_at(request_root,request_scope,*,force=False):
  if Path(request_root).resolve()!=root or digest(request_scope)!=scope_key:fail('ST batch scope identity changed')
  return scope_data
 def day_at(folder,day,*,required_asof=None):
  result=days.get((str(Path(folder).resolve()),str(day)))
  if result is None:fail('ST batch daily identity changed')
  if required_asof is not None:
   observed=datetime.fromisoformat(result['observation_start'])
   if required_asof.tzinfo is None or observed<required_asof:fail('ST batch daily observation is too old')
  return result
 _ACTIVE_BATCH=True;ev._registry=registry_at;archive._registry=registry_at;globals()['validate_scope']=scope_at;daily.verify_day=day_at
 try:
  yield dict(manifest_sha256=expected,registry_sha256=registry_sha,scope='staged_batch_only_no_publication_before_exit')
 finally:
  ev._registry=real_registry;archive._registry=real_archive_registry;globals()['validate_scope']=real_validate;daily.verify_day=real_day;_ACTIVE_BATCH=False
  if digest(records)!=cache_registry_digest or digest(scope_data)!=cache_scope_digest or (hashlib.sha256(repr(days).encode()).hexdigest() if days else digest(days))!=cache_days_digest:fail('ST validated in-memory source was modified during batch')
  if resolve_archive_reference(root)!=(configured,expected):fail('ST archive reference changed during staged batch')
  if before!=source_closure(root,configured,expected) or evidence_before!={p:hashlib.sha256((root/p).read_bytes()).hexdigest() for p in evidence_files}:fail('ST source/code changed during staged batch')
  if real_registry(root)[1]!=registry_sha:fail('ST registry changed during staged batch')
  if scope is not None:real_validate(root,scope,force=True)
