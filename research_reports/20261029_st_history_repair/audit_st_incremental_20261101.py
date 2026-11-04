import sys,json,hashlib,copy,inspect,ast
from pathlib import Path
sys.path.insert(0,str(Path.cwd()))
from scripts import st_notice_incremental as m
B=Path('research_reports/20261029_st_history_repair');S=B/'source_recovery/daily_all_market_20261101/2026-10-30';O=B/'resume_audit/st_incremental_independent_20261101';O.mkdir(exist_ok=True)
rows=json.loads((S/'page_0001.json').read_bytes())['data']['list'];r=next(x for x in rows if any(c.get('ann_type','').startswith('A') for c in x['codes']))
def fixture(name,row,collected='2026-10-30T08:00:00+00:00'):
 root=O/name;d=root/'2026-10-30';d.mkdir(parents=True,exist_ok=True)
 raw=json.dumps(dict(success=1,data=dict(total_hits=1,page_index=1,page_size=100,list=[row])),ensure_ascii=False).encode();(d/'page_0001.json').write_bytes(raw)
 receipt=dict(url=m.API,params=m._params('2026-10-30',1),status=200,collected_at=collected,sha256=m._sha(raw));m._dump(d/'page_0001.receipt.json',receipt)
 m._dump(d/'manifest.json',dict(schema=m.SCHEMA,date='2026-10-30',pages=[dict(response='page_0001.json',receipt='page_0001.receipt.json',response_sha256=m._sha(raw),receipt_sha256=m._sha((d/'page_0001.receipt.json').read_bytes()))]))
 return root,d
result={'scope':'mutated real-source isolated counterexamples, not production evidence','source':str(S/'page_0001.json'),'source_sha256':m._sha((S/'page_0001.json').read_bytes()),'module_sha256':m._sha(Path('scripts/st_notice_incremental.py').read_bytes()),'cases':[]}
root,d=fixture('late_announcement_reuse',r)
class NewData:
 calls=0
 def get(self,*a,**kw):self.calls+=1;raise AssertionError('A new collection would observe later rows')
sess=NewData();v=m.collect_days(root,'2026-10-30','2026-10-30',session=sess);result['cases'].append(dict(case='permanent_day_reuse_without_asof_requirement',accepted=True,network_calls=sess.calls,total_hits=v[0]['total_hits'],note='A 16:00 archive is forever reused despite later same-day notices.'))
bad=copy.deepcopy(r);bad['codes']=[];root,d=fixture('missing_issuer',bad);v=m.verify_day(d,'2026-10-30');result['cases'].append(dict(case='missing_issuer_silently_dropped',accepted=True,by_symbol=v['by_symbol']))
bad=copy.deepcopy(r);bad['notice_date']='2026-10-30INVALID';root,d=fixture('bad_date_suffix',bad);v=m.verify_day(d,'2026-10-30');result['cases'].append(dict(case='malformed_date_suffix',accepted=True))
root,d=fixture('row_after_receipt',r);v=m.verify_day(d,'2026-10-30');result['cases'].append(dict(case='row_publication_after_receipt',accepted=True,receipt='2026-10-30T08:00:00+00:00',row_display_time=r.get('display_time'),row_eiTime=r.get('eiTime')))
try:v=m.verify_day(S,'2026-10-30');result['real_archive']={k:v[k] for k in ['total_hits','pages','manifest_sha256']}
except Exception as e:result['real_archive_error']=str(e)
(O/'results.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8');print(json.dumps(result,ensure_ascii=False,indent=2))
