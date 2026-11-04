"""Prepare explicit share installation mapping; approved remains false."""
from pathlib import Path
from decimal import Decimal
import csv,json,hashlib
RUN=Path(__file__).resolve().parent
OUT=RUN/'old4975_shares_tail_20261029'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def read(p):return list(csv.DictReader(p.open(encoding='utf-8-sig',newline='')))
def main():
 source=OUT/'final_independent_manifest.json';original=json.loads(source.read_text())
 reviewpath=OUT/'issuer_anchor_review.json';reviews={r['symbol']:r for r in json.loads(reviewpath.read_text())['records']}
 cnpath=OUT/'current_cninfo_anchor_reconciliation.json';cn={r['symbol']:r for r in json.loads(cnpath.read_text())['records']}
 destination=OUT/'final_install_candidate';destination.mkdir(exist_ok=True)
 records=[];new_events=[];historical=[]
 for r in original['records']:
  sym=r['symbol'];old=Path(r['input_path']);assert sha(old)==r['input_sha256'];baseline=read(old)
  if r['status']=='candidate_tail_source_queried':
   candidate=Path(r['candidate_path']);assert sha(candidate)==r['candidate_sha256'];merged=read(candidate);assert merged[:len(baseline)]==baseline
   records.append({'symbol':sym,'candidate_path':str(candidate),'candidate_sha256':sha(candidate),'baseline_sha256':sha(old),'source':'EM tail with matching old quantity anchor','source_receipt_path':str(OUT/'receipts'/f'{sym}.json'),'source_receipt_sha256':sha(OUT/'receipts'/f'{sym}.json'),'new_rows':len(merged)-len(baseline),'historical_changes':[]})
   for row in merged[len(baseline):]:new_events.append({'symbol':sym,**row,'source_kind':'EM END_DATE','source_receipt_sha256':records[-1]['source_receipt_sha256']})
   continue
  assert r['status']=='blocked_anchor_quantity_conflict' and sym in reviews and sym in cn
  base=baseline;changes=[]
  if sym in {'002869','605006'}:
   diffpath=OUT/'issuer_corrected_anchors'/f'{sym}_migration_diff.json';diff=json.loads(diffpath.read_text());cp=Path(diff['candidate_path']);assert diff['input_sha256']==sha(old) and diff['candidate_sha256']==sha(cp)
   evidence=Path(reviews[sym]['source_path']);assert sha(evidence)==diff['source_sha256']
   base=read(cp);changes=diff['changes'];historical.append({'symbol':sym,'diff_path':str(diffpath),'diff_sha256':sha(diffpath),'issuer_source_path':str(evidence),'issuer_source_sha256':sha(evidence),'changes':changes})
   allowed={x['date']:Decimal(str(x['new_shares']))/10000 for x in changes}
   before={x['change_date']:x for x in baseline};after={x['change_date']:x for x in base}
   assert set(after)-set(before)=={x['date'] for x in changes if x.get('old_event_absent')}
   for date,row in before.items():
    assert date in after
    if date in allowed:assert Decimal(after[date]['total_shares_10k'])==allowed[date]
    else:assert after[date]==row
  receiptpath=Path(cn[sym]['source_receipt']);receipt=json.loads(receiptpath.read_text());rawpath=receiptpath.with_name(receiptpath.name.replace('_receipt.json','_response.json'))
  assert receipt['status']==200 and receipt['params']['scode']==sym and receipt['params']['edate']=='2026-10-29'
  assert sha(rawpath)==receipt['sha256']==cn[sym]['raw_sha256']
  payload=json.loads(rawpath.read_bytes());rawrows=payload['records'];assert payload['resultcode']==200 and len(rawrows)==payload['count']==payload['total']
  end=max(x['change_date'] for x in base);bydate={}
  for row in rawrows:
   assert row['SECCODE']==sym;date=row['VARYDATE'][:10];qty=Decimal(str(row['F003N']));assert qty.is_finite() and qty>0 and date<='2026-10-29'
   if date<=end:continue
   item={'change_date':date,'total_shares_10k':str(qty),'reason':row.get('F002V') or ''}
   if date in bydate:assert Decimal(bydate[date]['total_shares_10k'])==qty,'contradictory new same-date quantity'
   bydate[date]=item
   new_events.append({'symbol':sym,**item,'declare_date':row.get('DECLAREDATE'),'source_kind':'CNInfo original VARYDATE','source_raw_path':str(rawpath),'source_raw_sha256':sha(rawpath),'source_receipt_sha256':sha(receiptpath)})
  additions=[bydate[d] for d in sorted(bydate)];merged=base+additions;candidate=destination/f'{sym}.csv'
  with candidate.open('w',encoding='utf-8',newline='') as stream:
   writer=csv.DictWriter(stream,fieldnames=['change_date','total_shares_10k','reason'],lineterminator='\n');writer.writeheader();writer.writerows(merged)
  assert read(candidate)==merged and sha(old)==r['input_sha256']
  if not changes:assert merged[:len(baseline)]==baseline
  assert all(x['change_date']<='2026-10-29' for x in merged)
  records.append({'symbol':sym,'candidate_path':str(candidate),'candidate_sha256':sha(candidate),'baseline_sha256':sha(old),'source':'Original CNInfo tail with preserved baseline plus explicit issuer corrections only','source_raw_path':str(rawpath),'source_raw_sha256':sha(rawpath),'source_receipt_path':str(receiptpath),'source_receipt_sha256':sha(receiptpath),'new_rows':len(additions),'historical_changes':changes,'issuer_disposition':reviews[sym]})
 assert len(records)==4975 and {r['symbol'] for r in historical}=={'002869','605006'}
 report={'approved':False,'formal_written':False,'query_cutoff':'2026-10-29','symbol_count':4975,'source_manifest_sha256':sha(source),'issuer_review_sha256':sha(reviewpath),'cninfo_reconciliation_sha256':sha(cnpath),'historical_corrections':historical,'all_new_events':new_events,'records':records,'boundaries':{s:reviews[s] for s in ['000422','002851','603501','603535']},'semantics':'Only two issuer-backed historical corrections; other baseline dates and quantities preserved. Event/disclosure timing not globally PIT certified.'}
 f=OUT/'install_manifest.json';f.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8');print({'symbols':len(records),'historically_changed':len(historical),'new_events':len(new_events),'manifest_sha256':sha(f)})
if __name__=='__main__':main()
