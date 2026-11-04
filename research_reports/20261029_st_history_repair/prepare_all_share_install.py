from pathlib import Path
import json,hashlib,csv
from decimal import Decimal
B=Path(__file__).resolve().parent
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
D=B/'new463_cninfo_shares_20261029/issuer_latest_review'
review=[]
for sym,qty,date,claim in [('002592',284095157,'2026-09-30','Issuer Q3 consolidated balance sheet records total share capital at quarter end; report also states cumulative option exercise. Exact individual exercise dates remain unproved.'),('003004',81864843,'2026-10-15','Issuer announcement 2026-086 explicitly states total shares as of 2026-10-15; individual convertible conversion dates are not established.')]:
 text=(D/f'{sym}.txt').read_text(encoding='utf-8');assert format(qty,',') in text
 receipt=json.loads((D/f'{sym}_receipt.json').read_text());assert receipt['sha256']==sha(D/f'{sym}.html')
 review.append(dict(symbol=sym,resolution='later issuer observation supports EM latest quantity; CNInfo latest observation is earlier',observed_date=date,total_shares=qty,source_url=receipt['url'],source_path=str((D/f'{sym}.html').resolve()),source_sha256=sha(D/f'{sym}.html'),source_text_sha256=sha(D/f'{sym}.txt'),scope=claim))
(D/'review.json').write_text(json.dumps(dict(approved=False,formal_written=False,records=review),ensure_ascii=False,indent=2),encoding='utf-8')
rawp=B/'raw_install_normalized_20261029/install_manifest.json';oldp=B/'old4975_shares_tail_20261029/install_manifest.json';emp=B/'cross_source_shares/normalized_candidate/manifest.json';cnp=B/'new463_cninfo_shares_20261029/final_independent_manifest.json'
raw=json.loads(rawp.read_text()); old=json.loads(oldp.read_text()); em=json.loads(emp.read_text());cn=json.loads(cnp.read_text()); cm={r['symbol']:r for r in cn['records']}
records={r['symbol']:dict(r,scope='preserved baseline + separately reviewed corrections/tail') for r in old['records']}
for r in records.values():assert sha(Path(r['candidate_path']))==r['candidate_sha256']
needed={r['symbol'] for r in raw['records']}
for row in em['files']:
 sym=row['symbol']
 if sym not in needed or sym in records:continue
 p=B/'cross_source_shares/normalized_candidate'/f'{sym}.csv';assert sha(p)==row['normalized_sha256']
 r=dict(symbol=sym,candidate_path=str(p.resolve()),candidate_sha256=sha(p),scope='EM provider END_DATE historical candidate; not globally PIT adjudicated',cross_source='retired_source_limited',requires_historical_adjudication=False)
 if sym in cm:
  cr=cm[sym];cp=Path(cr['candidate_path']);assert sha(cp)==cr['candidate_sha256']
  r.update(cninfo_candidate_path=str(cp),cninfo_candidate_sha256=sha(cp),cross_source=cr['cross_source_status'],quantity_disagreement_points=cr['quantity_disagreement_points'],requires_historical_adjudication=bool(cr['quantity_disagreement_points']))
 if sym in ['002592','003004']:r['latest_issuer_review']=next(x for x in review if x['symbol']==sym)
 records[sym]=r
out=B/'share_install_20261029';out.mkdir(exist_ok=True)
src=B/'incremental_688287/shares/688287_candidate.csv';rows=list(csv.DictReader(src.open(encoding='utf-8-sig')));dest=out/'688287.csv'
with dest.open('w',newline='',encoding='utf-8') as f:
 w=csv.DictWriter(f,fieldnames=['change_date','total_shares_10k','reason'],lineterminator='\n');w.writeheader();w.writerows(dict(change_date=r['END_DATE'][:10],total_shares_10k=str(Decimal(r['TOTAL_SHARES'])/10000),reason=r['CHANGE_REASON']) for r in rows if r['END_DATE'][:10]<='2026-10-29')
records['688287']=dict(symbol='688287',candidate_path=str(dest.resolve()),candidate_sha256=sha(dest),source_path=str(src.resolve()),source_sha256=sha(src),scope='retired EM provider historical candidate',requires_historical_adjudication=False)
assert set(records)==needed and len(records)==5438
manifest=dict(approved=False,formal_written=False,target='2026-10-29',scope='complete candidate path/hash mapping; not full historical cross-source certification',symbol_count=len(records),source_manifests={str(p.relative_to(B)):sha(p) for p in [rawp,oldp,emp,cnp]},latest_quantity_conflicts_resolved=review,historical_cross_source_unresolved=[s for s,r in records.items() if r.get('requires_historical_adjudication')],records=[records[s] for s in sorted(records)])
(out/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding='utf-8');print(dict(symbols=len(records),unresolved=len(manifest['historical_cross_source_unresolved']),manifest_sha256=sha(out/'manifest.json')))
