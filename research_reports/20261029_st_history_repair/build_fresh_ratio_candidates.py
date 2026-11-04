"""Offline normalized-raw candidates; missing factors remain pending, no network."""
from pathlib import Path
import sys,json,hashlib,datetime
import pandas as pd
B=Path(__file__).resolve().parent;ROOT=B.parents[1];sys.path.insert(0,str(ROOT))
from scripts.stock_return_basis import build_ratio_return_basis
OUT=B/'fresh_ratio_candidate_20261029';SRC=B/'return_basis_candidate_20261030/sources'
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def dump(p,x):
 p.parent.mkdir(parents=True,exist_ok=True);t=p.with_suffix('.tmp');t.write_text(json.dumps(x,ensure_ascii=False,indent=2),encoding='utf-8');t.replace(p)
def main():
 mapping=B/'raw_install_normalized_20261029/install_manifest.json';masterpath=B/'cross_source_official/security_master_combined_candidate.csv';cal=B/'cross_source_prices/missing_session_review/exchange_sessions_20261028.json'
 assert sha(mapping)=='3f7f8f3aab8b97dddd219f3da140295efa70af2cc5cb0a12a128b470714e3416'
 assert sha(masterpath)=='9c6195aaf52972733e6583eb67753b22579fc3c4c15ed9d635e88cbdcbb0d874'
 data=json.loads(mapping.read_text(encoding='utf-8'));master=pd.read_csv(masterpath,dtype={'symbol':str}).set_index('symbol');sessions=json.loads(cal.read_text());records=[]
 for i,r in enumerate(data['records']):
  s=r['symbol'];result={'symbol':s,'status':'pending_source'};folder=SRC/s
  if not all((folder/(k+ext)).exists() for k in ['qfq','hfq'] for ext in ['.js','.json']):records.append(result);continue
  try:
   prior=OUT/'results'/f'{s}.json';mp=OUT/'manifests'/f'{s}.json';cp=OUT/'prices_adj'/f'{s}.csv'
   if prior.exists() and mp.exists() and cp.exists():
    saved=json.loads(prior.read_text(encoding='utf-8'));m0=json.loads(mp.read_text(encoding='utf-8'))
    if saved.get('status')=='candidate_generated_not_certified' and saved.get('source_timestamps_present_and_fresh') and saved['raw_sha256']==r['candidate_sha256'] and sha(r['candidate_path'])==saved['raw_sha256'] and sha(cp)==m0['candidate_sha256'] and all(sha(folder/(k+'.js'))==m0[k+'_sha256'] for k in ['qfq','hfq']):records.append(saved);continue
   raw=Path(r['candidate_path']);assert sha(raw)==r['candidate_sha256'];m=master.loc[s];receipts={k:json.loads((folder/(k+'.json')).read_text()) for k in ['qfq','hfq']}
   frame,manifest=build_ratio_return_basis(symbol=s,raw_csv=raw.read_bytes(),qfq_source=(folder/'qfq.js').read_bytes(),hfq_source=(folder/'hfq.js').read_bytes(),qfq_receipt=receipts['qfq'],hfq_receipt=receipts['hfq'],as_of='2026-10-29',sessions=sessions,list_date=str(m.list_date),delist_date=None if pd.isna(m.delist_date) else str(m.delist_date))
   p=OUT/'prices_adj'/f'{s}.csv';p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(frame.to_csv(index=False).encode());assert sha(p)==manifest['candidate_sha256']
   dump(OUT/'manifests'/f'{s}.json',manifest)
   fresh=all(x.get('retrieved_at_utc') and datetime.datetime.fromisoformat(x['retrieved_at_utc'].replace('Z','+00:00'))>=datetime.datetime.fromisoformat('2026-10-29T15:00:00+08:00') for x in receipts.values())
   result.update(status='candidate_generated_not_certified',raw_path=str(raw),raw_sha256=sha(raw),candidate_sha256=sha(p),rows=len(frame),last=manifest['last'],source_timestamps_present_and_fresh=fresh)
  except Exception as exc:result.update(status='blocked',error=repr(exc))
  dump(OUT/'results'/f'{s}.json',result);records.append(result)
  if (i+1)%500==0:print(i+1,flush=True)
 assert sha(mapping)=='3f7f8f3aab8b97dddd219f3da140295efa70af2cc5cb0a12a128b470714e3416'
 assert sha(masterpath)==data['master_sha256']
 dump(OUT/'summary.json',dict(formal_written=False,source_mapping_sha256=sha(mapping),master_sha256=sha(masterpath),calendar_sha256=sha(cal),total=len(records),records=records))
 print({k:sum(r['status']==k for r in records) for k in sorted({r['status'] for r in records})})
if __name__=='__main__':main()
