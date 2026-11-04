"""Assemble transportable trade-status evidence in a research-only tree."""
from pathlib import Path
import hashlib,json,shutil,sys
import pandas as pd
RUN=Path(__file__).resolve().parent
ROOT=RUN.parents[1]
sys.path.insert(0,str(ROOT))
from scripts.tradeability_evidence import enrich_price_with_trade_status

def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def save(p,x):p.write_text(json.dumps(x,ensure_ascii=False,indent=2),encoding='utf-8')

def main():
 out=RUN/'trade_status_candidate';base=out/'.microcap_index_cache/trade_status'
 for sub in ['raw','documents','reviews']:(base/sub).mkdir(parents=True,exist_ok=True)
 rawroot=RUN/'raw_tail_20261029_baostock'
 installpath=RUN/'raw_install_normalized_20261029/install_manifest.json'
 install=json.loads(installpath.read_text());mapping={r['symbol']:r for r in install['records']}
 masterpath=RUN/'cross_source_official/security_master_combined_candidate.csv'
 master=pd.read_csv(masterpath,dtype=str).fillna('')
 sessions=pd.DatetimeIndex(json.loads((rawroot/'calendar.json').read_text())['sessions'])
 sessions=sessions[(sessions>='2010-01-04')&(sessions<='2026-10-29')]
 reviewroot=RUN/'cross_source_prices/missing_session_review'
 notices=json.loads((reviewroot/'trade_state_candidate.json').read_text())
 wind=json.loads((reviewroot/'wind_three_completion.json').read_text())
 for entry in wind:
  entry=dict(entry);entry['file']=str(ROOT/entry['response_path']);entry['sha256']=entry['response_sha256'];entry['status']='official_suspension';notices.append(entry)
 results=[]
 for f in sorted((rawroot/'receipts').glob('*.json')):
  receipt=json.loads(f.read_text());sym=receipt['symbol']
  if receipt['status']!='candidate_tail_verified':
   results.append({'symbol':sym,'status':'blocked_raw_receipt','reason':receipt.get('error')});continue
  source=Path(mapping[sym]['candidate_path']);raw=Path(receipt['raw_path'])
  assert sha(source)==mapping[sym]['candidate_sha256'] and sha(raw)==receipt['raw_sha256']
  copied=base/'raw'/f'{sym}.json';shutil.copyfile(raw,copied)
  m=master.loc[master.symbol==sym].iloc[0]
  record={'schema_version':1,'symbol':sym,'raw_price_sha256':sha(source),'lifecycle':{'list_date':m.list_date,'delist_date':m.delist_date},'provider_queries':[{'path':f'raw/{sym}.json','sha256':sha(copied)}],'reviewed_suspensions':[]}
  if receipt.get('provider_status_review_bundle'):
   bundle=Path(receipt['provider_status_review_bundle']);assert sha(bundle)==receipt['provider_status_review_bundle_sha256']
   reviews=json.loads(bundle.read_text());record['provider_status_reviews']=reviews
   for review in reviews:
    for key in ['wind_receipt','wind_response','document','issuer_review']:
     if key not in review:continue
     sourcefile=bundle.parent/review[key]['path'];assert sha(sourcefile)==review[key]['sha256']
     review[key]['path']='corroboration/'+review[key]['path']
     destination=base/review[key]['path'];destination.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(sourcefile,destination)
  for entry in notices:
   if entry['symbol']!=sym or entry.get('status')!='official_suspension':continue
   document=Path(entry['file']);document=document if document.is_absolute() else reviewroot/document
   assert sha(document)==entry['sha256']
   ds=entry['date'];docrel=f'documents/{sym}_{ds}{document.suffix}';reviewrel=f'reviews/{sym}_{ds}.json'
   shutil.copyfile(document,base/docrel)
   review={'symbol':sym,'date':ds,'status':'official_suspension','document_sha256':sha(document),'source_url':entry['source_url'],'rationale':'Individually reviewed issuer suspension for this date; no inferred date interval. Source audit retained in missing_session_review.','formal_approved':False}
   save(base/reviewrel,review)
   record['reviewed_suspensions'].append({'date':ds,'document':{'path':docrel,'sha256':sha(base/docrel)},'review':{'path':reviewrel,'sha256':sha(base/reviewrel)}})
  save(base/f'{sym}.json',record)
  try:
   price=pd.read_csv(source,parse_dates=['date'])
   enriched=enrich_price_with_trade_status(root=out,symbol=sym,raw_path=source,price=price,master=master,trading_dates=sessions,require_evidence=True)
   results.append({'symbol':sym,'status':'candidate_evidence_validated','raw_path':str(source),'raw_sha256':sha(source),'sidecar_sha256':sha(base/f'{sym}.json'),'raw_rows':len(price),'valuation_rows':len(enriched),'explicit_suspended_rows':int(enriched.tradestatus.eq(0).sum()),'reviewed_notice_dates':len(record['reviewed_suspensions'])})
  except Exception as exc:results.append({'symbol':sym,'status':'blocked_evidence_validation','error':str(exc)})
 source_files={str(p.relative_to(base)).replace('\\','/'):sha(p) for p in sorted(base.rglob('*')) if p.is_file()}
 set_hash=hashlib.sha256(json.dumps(source_files,sort_keys=True,separators=(',',':')).encode()).hexdigest()
 save(out/'manifest.json',{'approved':False,'formal_written':False,'master_path':str(masterpath),'master_sha256':sha(masterpath),'install_manifest_path':str(installpath),'install_manifest_sha256':sha(installpath),'target':'2026-10-29','sidecar_count':sum(p.stem.isdigit() and len(p.stem)==6 for p in base.glob('*.json')),'source_files':source_files,'source_files_set_sha256':set_hash,'records':results,'coverage_note':'Explicit provider tail and individually reviewed historical suspensions only; unknown historical raw tradeability is not certified.'})
 print({s:sum(r['status']==s for r in results) for s in {r['status'] for r in results}})
if __name__=='__main__':main()
