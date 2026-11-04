"""Isolate seven evidenced non-session rows; never overwrite source/tail candidates."""
from pathlib import Path
import csv,json,hashlib
RUN=Path(__file__).resolve().parent
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def read(p):return list(csv.DictReader(p.open(encoding='utf-8-sig',newline='')))
def main():
 source=RUN/'raw_tail_20261029_baostock/final_independent_manifest.json'
 manifest=json.loads(source.read_text());calendar=RUN/'raw_tail_20261029_baostock/calendar.json'
 sessions=set(json.loads(calendar.read_text())['sessions'])
 masterpath=RUN/'cross_source_official/security_master_combined_candidate.csv'
 master={r['symbol']:r for r in read(masterpath)}
 auditpath=RUN/'cross_source_prices/missing_session_review/raw_calendar_audit.json'
 audit=json.loads(auditpath.read_text());invalid={r['symbol']:r for r in audit['invalid_date_records']}
 assert set(invalid)=={'300028','300090','300104','300156','300216','300372','300431'}
 out=RUN/'raw_install_normalized_20261029';(out/'normalized_raw').mkdir(parents=True,exist_ok=True)
 changes=[]
 for rec in manifest['records']:
  sym=rec['symbol']
  if sym not in invalid:continue
  p=Path(rec['candidate_path']);assert sha(p)==rec['candidate_sha256'];rows=read(p);event=invalid[sym]
  assert sha(Path(event['raw_file']))==event['raw_sha256']
  removed=[r for r in rows if r['date']==event['date']]
  assert len(removed)==1 and event['date'] not in sessions
  assert master[sym]['delist_date'] and event['date']>=master[sym]['delist_date']
  kept=[r for r in rows if r['date']!=event['date']];dest=out/'normalized_raw'/f'{sym}.csv'
  with dest.open('w',encoding='utf-8',newline='') as stream:
   writer=csv.DictWriter(stream,fieldnames=['date','close_raw'],lineterminator='\n');writer.writeheader();writer.writerows(kept)
  assert read(dest)==kept and sha(p)==rec['candidate_sha256']
  changes.append({'symbol':sym,'source_path':str(p),'source_sha256':sha(p),'candidate_path':str(dest),'candidate_sha256':sha(dest),'removed_rows':removed,'reason':'2021-06-27 is a non-exchange Sunday and after the issuer delisting date; not a suspended valuation','official_master_delist_date':master[sym]['delist_date'],'calendar_sha256':sha(calendar),'master_sha256':sha(masterpath),'prior_raw_audit_sha256':sha(auditpath)})
  rec.update(candidate_path=str(dest),candidate_sha256=sha(dest),candidate_rows=len(kept),last_actual_price_date=kept[-1]['date'],normalization_reason='removed_exact_evidenced_non_session_row',normalization_source_sha256=sha(p))
 assert len(changes)==7
 result={'approved':False,'formal_written':False,'target':'2026-10-29','total':5438,'source_manifest_sha256':sha(source),'calendar_sha256':sha(calendar),'master_sha256':sha(masterpath),'historical_changes':changes,'records':manifest['records'],'scope':'Final runtime raw mapping; raw tail acquisition evidence remains immutable. Seven exclusions require historical migration review.'}
 (out/'install_manifest.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
 print({'total':5438,'changed_symbols':7,'removed_rows':7,'manifest_sha256':sha(out/'install_manifest.json')})
if __name__=='__main__':main()
