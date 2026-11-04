import pathlib,json,re,hashlib,datetime
B=pathlib.Path(__file__).resolve().parent; R=B.parents[1]; O=B/'source_recovery/round2_price14_pdf_20261101'
a=json.loads((O/'fetch_manifest.json').read_text(encoding='utf-8'))
# Manually reviewed event-specific decisions. Only explicitly supported risk categories are changed.
rows='''0|O|2025-04-17|ST张家界|实施其他风险警示的起始日：2025年4月17日
1|N|||预计公司2021年年度财务报表披露后触及
2|DO|2025-11-17|*ST美谷|公司股票交易将于2025年11月17日被叠加实施退市风险警示
3|O||*ST恒立|本次被叠加实施“其他风险警示”公司股票不停牌
4|DO|2025-09-25|*ST炼石|自2025年9月25日开市起复牌
5|O|2022-05-06|ST泰禾|实施其他风险警示的起始日：2022年5月6日
6|DO|2022-05-06|*ST银河|实施退市风险警示的起始日：2022年5月6日
7|DO|2025-10-27|*ST交投|公司股票交易将自2025年10月27日开市起被叠加实施“退市风险警示”
8|O|2025-04-25|ST宏达|实施其他风险警示的起始日：2025年4月25日
9|O|2025-12-23|ST百灵|实施其他风险警示的起始日：2025年12月23日
10|DO|2026-04-30|*ST瑞和|实施退市风险警示并继续被叠加实施其他风险警示的起始日：2026年4月30日
11|PENDING|||自该规定实施后，公司股票交易将被实施其他风险警示（ST）
12|DO|2026-04-30|*ST香雪|实施退市风险警示的起始日：2026年4月30日
13|O|2020-09-15|ST天龙|实行其他风险警示的起始日：2020年9月15日开市起'''
actions={'DO':{'delisting':True,'other':True},'O':{'other':True},'D':{'delisting':True},'CLEAR':{'delisting':False,'other':False},'WITHDRAW_D_KEEP_O':{'delisting':False,'other':True},'N':{},'C':{},'BOND':{},'PENDING':{}}
notes={1:'Forecast new annual trigger plus history of already-existing two risk categories; no new transition as of this notice.',3:'Definite additional other risk but no explicit implementation date; null, not prior-day inferred.',11:'Same pre-rule notice remains pending, no current transition. Separate actual later notice AN202009131411572897 establishes announcement2020-09-14 and execution2020-09-15; do not backdate.',13:'Actual subsequent implementation notice, independent f_node0 bounded full page; not risk-category index. Replaces missing implementation evidence, does not alter earlier warning classification.'}
def sha(x):return hashlib.sha256(x).hexdigest()
def flat(x):return re.sub(r'\s+','',x)
result=[]
for line in rows.splitlines():
 idx,kind,day,name,q=line.split('|'); i=int(idx);m=a[i];e=m['event'];t=(R/m['text_path']).read_text(encoding='utf-8');pgs=re.split(r'\[PAGE \d+\]',t)[1:];hits=[j+1 for j,p in enumerate(pgs) if flat(q) in flat(p)];assert hits,(i,q)
 assert m['symbol'] in t,(i,'issuer'); assert not name or flat(name) in flat(t)
 pdf=R/m['pdf_path']; assert sha(pdf.read_bytes())==m['pdf_sha256']; rec=O/(e['announcement_id']+'.receipt.json'); rc=json.loads(rec.read_text());assert rc['sha256']==m['pdf_sha256'] and rc['url']==e['source_url']
 b=e['source_row_binding'];raw=(R/b['path']).read_bytes();assert sha(raw)==m['api_response_sha256'];rr=json.loads(raw)['data']['list'][b['index']];assert rr['art_code']==e['announcement_id'] and rr['notice_date']==e['provider_notice_date'] and rr['title']==e['provider_title']
 result.append(dict(symbol=m['symbol'],announcement_id=e['announcement_id'],source_provider='eastmoney',notice_date=e['notice_date'],effective_date=day or None,new_security_name=name or None,risk_actions=actions[kind],decision=kind,complete_risk_state_after_event=len(actions[kind])==2,no_action=kind in ['N','C','BOND'],requires_date_crosscheck=i in [3,11],review_note=notes.get(i,'Only current issuer-confirmed action; conditional/rule examples not treated as transitions.'),source_url=e['source_url'],source_row_binding=dict(b,sha256=m['api_response_sha256']),pdf_path=m['pdf_path'],pdf_sha256=m['pdf_sha256'],receipt_path=str(rec.relative_to(R)),receipt_sha256=sha(rec.read_bytes()),text_path=m['text_path'],text_sha256=sha((R/m['text_path']).read_bytes()),evidence=[dict(page=hits[0],quote=q)],review_method='manual issuer body action/date/condition review; literal quote and issuer/API/hash checked',formal_approved=False))
out=B/'source_recovery/reviewed_round2_price14_candidates_20261101.json';out.write_text(json.dumps(dict(records=result,reviewed_count=len(result),formal_written=False,registry_written=False,reviewed_at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),input_manifest_sha256=sha((O/'fetch_manifest.json').read_bytes())),ensure_ascii=False,indent=2),encoding='utf-8');print(len(result),sha(out.read_bytes()))
