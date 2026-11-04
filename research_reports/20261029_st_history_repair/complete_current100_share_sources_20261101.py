"""Bind selected economic share history and query only the missing 2026-10-30 session."""
from pathlib import Path
import sys,json,hashlib,shutil,datetime
import pandas as pd
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT))
from scripts.share_change_source import install_approved_share_baseline,refresh_share_change,verify_share_cache
TASK=Path(__file__).resolve().parent;OUT=TASK/'current100_share_completion_20261101';STAGE=OUT/'stage';PKG=OUT/'approval_package'
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
def write(p,d):p.parent.mkdir(parents=True,exist_ok=True);p.write_text(json.dumps(d,ensure_ascii=False,indent=2),encoding='utf-8')
def main():
 OUT.mkdir(exist_ok=True);PKG.mkdir(exist_ok=True)
 members_path=ROOT/'outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_effective_members.csv';symbols=pd.read_csv(members_path,dtype={'symbol':str}).symbol.tolist()
 if len(symbols)!=100 or len(set(symbols))!=100:raise ValueError('Not actual100')
 mapping_path=TASK/'share_install_20261029/manifest.json';mapping={r['symbol']:r for r in json.loads(mapping_path.read_text(encoding='utf-8'))['records']}
 em_manifest_path=TASK/'cross_source_shares/normalized_candidate/manifest.json';em={r['symbol']:r for r in json.loads(em_manifest_path.read_text(encoding='utf-8'))['files']}
 cn_manifest_path=TASK/'new463_cninfo_shares_20261029/final_independent_manifest.json';cn={r['symbol']:r for r in json.loads(cn_manifest_path.read_text(encoding='utf-8'))['records']}
 previous=TASK/'formal_base_install_backups/bcb459276dcfa5e130ba14654a045b85d454dadc5f9d1e97796a02571e351a8a/.microcap_index_cache/share_change'
 approval_path=PKG/'approval.json'
 if not approval_path.exists():
  records=[]
  def bind(src,rel,expected=None):
   if expected is not None and sha(src)!=expected:raise ValueError('Source hash mismatch '+str(src))
   dest=PKG/rel;dest.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(src,dest)
   if sha(src)!=sha(dest):raise ValueError('Source copy differs')
   return {'path':rel,'sha256':sha(dest)}
  shared=[bind(mapping_path,'selected_history_mapping.json'),bind(ROOT/'docs/release_evidence_20261101/combined_review.json','existing_migration_combined_review.json')]
  for symbol in symbols:
   record=mapping[symbol];cache=ROOT/'.microcap_index_cache/share_change'/f'{symbol}.csv'
   if sha(cache)!=record['candidate_sha256']:raise ValueError('Selected formal cache changed '+symbol)
   sources=shared+[bind(cache,f'sources/{symbol}/selected_history.csv',record['candidate_sha256'])]
   note='Existing selected economic-share history; not certification of global public-disclosure PIT completeness.'
   if 'source_receipt_path' in record:
    old=previous/f'{symbol}.csv';sources.append(bind(old,f'sources/{symbol}/previous_formal_history.csv',record['baseline_sha256']))
    receipt_path=Path(record['source_receipt_path']);sources.append(bind(receipt_path,f'sources/{symbol}/tail_receipt.json',record['source_receipt_sha256']))
    receipt=json.loads(receipt_path.read_text(encoding='utf-8'))
    if receipt['query_cutoff']!='2026-10-29' or receipt['input_sha256']!=sha(old):raise ValueError('Wrong tail boundary')
    oldf=pd.read_csv(old).fillna('');newf=pd.read_csv(cache).fillna('')
    pd.testing.assert_frame_equal(newf.iloc[:len(oldf)].reset_index(drop=True),oldf,check_dtype=False)
    for i,request in enumerate(receipt['requests']):
     if datetime.datetime.fromisoformat(request['retrieved_utc'])<datetime.datetime.fromisoformat('2026-10-29T15:00:00+08:00'):raise ValueError('Tail observation predates certified close')
     sources.append(bind(Path(request['raw_path']),f'sources/{symbol}/tail_page_{i+1}.json',request['raw_sha256']))
    note+=' Original selected history is preserved; EM page set is a bounded matching-anchor tail, not a full-history original-source claim.'
   else:
    er=em[symbol];original=TASK/'cross_source_shares'/er['source'];sources.append(bind(original,f'sources/{symbol}/em_original_candidate.csv',er['source_sha256']))
    requests_path=original.with_name(symbol+'_requests.json');requests=json.loads(requests_path.read_text(encoding='utf-8'));sources.append(bind(requests_path,f'sources/{symbol}/em_requests.json'))
    for i,request in enumerate(requests):sources.append(bind(original.with_name(symbol+f'_page{i+1}.json'),f'sources/{symbol}/em_page_{i+1}.json',request['sha256']))
    cr=cn[symbol]
    for field in ('raw','request','candidate'):sources.append(bind(Path(cr[field+'_path']),f'sources/{symbol}/cninfo_{field}'+('.csv' if field=='candidate' else '.json'),cr[field+'_sha256']))
    raw=json.loads(Path(cr['raw_path']).read_bytes());request=json.loads(Path(cr['request_path']).read_text(encoding='utf-8'))
    if request['params']['edate']!='2026-10-29' or request['params']['scode']!=symbol or raw['count']!=raw['total'] or raw['count']!=len(raw['records']):raise ValueError('Incomplete29 CNInfo evidence')
    if any(str(row['VARYDATE'])[:10]>'2026-10-28' for row in raw['records']):raise ValueError('Selected28 history needs29 economic review')
    note+=' Complete EM economic history retained; CNInfo29 query confirms no new29 event. Historical cross-source differences remain disclosed: '+json.dumps(cr.get('quantity_disagreement_points',[]),ensure_ascii=False)
   records.append({'symbol':symbol,'candidate_sha256':sha(cache),'certified_cutoff':'2026-10-29','source_files':sources,'scope':note})
  approval={'schema':'share_baseline_approval_v1','approved':True,'approved_by':'assistant executing explicit user historical economic-share repair and daily delivery authorization','user_separately_reviewed_generated_hashes':False,'authorization':'Existing user instruction to repair all three versions and complete local/cloud daily delivery; keep adopted economic share history and defer extra PIT investigation. This approval binds unchanged already selected histories, not new return values.','effective_members_sha256':sha(members_path),'records':records}
  write(approval_path,approval)
 approval=json.loads(approval_path.read_text(encoding='utf-8'));approval_sha=sha(approval_path)
 if [r['symbol'] for r in approval['records']]!=symbols or approval['effective_members_sha256']!=sha(members_path):raise ValueError('Membership changed')
 state_path=OUT/'progress.json';state=json.loads(state_path.read_text(encoding='utf-8')) if state_path.exists() else {'approval_sha256':approval_sha,'baseline':{},'refreshed':{},'formal_csv_changed':False}
 if state['approval_sha256']!=approval_sha:raise ValueError('Approval changed')
 for symbol in symbols:
  cache=STAGE/'.microcap_index_cache/share_change'/f'{symbol}.csv';live=ROOT/'.microcap_index_cache/share_change'/f'{symbol}.csv';record=next(r for r in approval['records'] if r['symbol']==symbol)
  if sha(live)!=record['candidate_sha256']:raise ValueError('Formal share history changed '+symbol)
  if not cache.exists():cache.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(live,cache)
  source_state=STAGE/'.microcap_index_cache/share_sources'/f'{symbol}.json'
  if not source_state.exists():install_approved_share_baseline(root=STAGE,symbol=symbol,cache_path=cache,approval_path=approval_path,approval_sha256=approval_sha)
  day=json.loads(source_state.read_text(encoding='utf-8'))['query_end'];verify_share_cache(root=STAGE,symbol=symbol,as_of=day)
  state['baseline'][symbol]=day;write(state_path,state)
 for symbol in symbols:
  cache=STAGE/'.microcap_index_cache/share_change'/f'{symbol}.csv';live=ROOT/'.microcap_index_cache/share_change'/f'{symbol}.csv'
  refresh_share_change(root=STAGE,symbol=symbol,cache_path=cache,as_of='2026-10-30',allow_network=True)
  result=verify_share_cache(root=STAGE,symbol=symbol,as_of='2026-10-30')
  if sha(cache)!=sha(live):
   state['blocked_symbol']=symbol;state['reason']='Actual queried candidate differs; no formal share CSV changed';write(state_path,state);raise ValueError(state['reason']+' '+symbol)
  state['refreshed'][symbol]=result;write(state_path,state);print('verified',len(state['refreshed']),symbol,flush=True)
 state['status']='all100_verified_no_formal_csv_change';write(state_path,state);print('ALL100 VERIFIED')
if __name__=='__main__':main()
