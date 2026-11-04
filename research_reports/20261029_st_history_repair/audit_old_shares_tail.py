"""Independent readback of bounded latest-share queries; never grants approval."""
from pathlib import Path
import csv,hashlib,json
RUN=Path(__file__).resolve().parent
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def rows(p):return list(csv.DictReader(p.open(encoding='utf-8-sig',newline='')))
def main():
 out=RUN/'old4975_shares_tail_20261029'
 expected={r['symbol'] for r in rows(RUN/'resume_audit/delivery_old4975_input_coverage.csv')}
 records=[json.loads(p.read_text()) for p in sorted((out/'receipts').glob('*.json'))]
 assert {r['symbol'] for r in records}==expected and len(records)==4975,'Source acquisition not complete'
 errors=[];total_new=0;conflicts=[]
 for r in records:
  assert r['query_cutoff']=='2026-10-29'
  source=Path(r['input_path']);assert sha(source)==r['input_sha256']
  for query in r['requests']:
   raw=Path(query['raw_path']);assert sha(raw)==query['raw_sha256']
   payload=json.loads(raw.read_bytes());assert payload['success'] is True
  if r['status']=='blocked_anchor_quantity_conflict':conflicts.append(r);continue
  assert r['status']=='candidate_tail_source_queried',r
  candidate=Path(r['candidate_path']);assert sha(candidate)==r['candidate_sha256']
  old=rows(source);new=rows(candidate);assert new[:len(old)]==old
  assert len(new)-len(old)==r['new_provider_events']
  assert all(r['anchor_date']<v['change_date']<='2026-10-29' for v in new[len(old):])
  total_new+=r['new_provider_events']
 summary={'approved':False,'formal_written':False,'queried_symbols':4975,'candidate_count':len(records)-len(conflicts),'blocked_anchor_conflicts':len(conflicts),'new_provider_events':total_new,'old_sources_unchanged':True,'all_query_raw_hashes_readback_verified':True,'candidate_old_rows_unchanged':True,'query_cutoff':'2026-10-29','date_semantics':'EM END_DATE does not independently prove registration/listing/publication timing; no historical source replacement','conflicts':conflicts,'records':records}
 (out/'final_independent_manifest.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding='utf-8')
 print({k:v for k,v in summary.items() if k not in ['records','conflicts']})
if __name__=='__main__':main()
