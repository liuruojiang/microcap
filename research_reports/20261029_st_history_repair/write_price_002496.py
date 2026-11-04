import pathlib,json,re,hashlib,datetime
B=pathlib.Path(__file__).resolve().parent; R=B.parents[1]; O=B/'source_recovery/price_002496_pdf_20261101'
a=json.loads((O/'fetch_manifest.json').read_text(encoding='utf-8'))
# Manually reviewed event-specific decisions. Only explicitly supported risk categories are changed.
rows='''0|O|2018-08-14|ST辉丰|公司股票自2018年8月14日开市起被实施其他风险警示'''
actions={'DO':{'delisting':True,'other':True},'O':{'other':True},'D':{'delisting':True},'CLEAR':{'delisting':False,'other':False},'WITHDRAW_D_KEEP_O':{'delisting':False,'other':True},'N':{},'C':{},'BOND':{},'PENDING':{}}
notes={0:'Actual issuer implementation; PDF board signature2018-08-10 is not public notice_date2018-08-13. Complete bounded f_node0 pages captured, no IPO anchor modified.'}
def sha(x):return hashlib.sha256(x).hexdigest()
def flat(x):return re.sub(r'\s+','',x)
result=[]
for line in rows.splitlines():
 idx,kind,day,name,q=line.split('|'); i=int(idx);m=a[i];e=m['event'];t=(R/m['text_path']).read_text(encoding='utf-8');pgs=re.split(r'\[PAGE \d+\]',t)[1:];hits=[j+1 for j,p in enumerate(pgs) if flat(q) in flat(p)];assert hits,(i,q)
 assert m['symbol'] in t,(i,'issuer'); assert not name or flat(name) in flat(t)
 pdf=R/m['pdf_path']; assert sha(pdf.read_bytes())==m['pdf_sha256']; rec=O/(e['announcement_id']+'.receipt.json'); rc=json.loads(rec.read_text());assert rc['sha256']==m['pdf_sha256'] and rc['url']==e['source_url']
 b=e['source_row_binding'];raw=(R/b['path']).read_bytes();assert sha(raw)==m['api_response_sha256'];rr=json.loads(raw)['data']['list'][b['index']];assert rr['art_code']==e['announcement_id'] and rr['notice_date']==e['provider_notice_date'] and rr['title']==e['provider_title']
 result.append(dict(symbol=m['symbol'],announcement_id=e['announcement_id'],source_provider='eastmoney',notice_date=e['notice_date'],effective_date=day or None,new_security_name=name or None,risk_actions=actions[kind],decision=kind,complete_risk_state_after_event=len(actions[kind])==2,no_action=kind in ['N','C','BOND'],requires_date_crosscheck=False,review_note=notes.get(i,'Only current issuer-confirmed action; conditional/rule examples not treated as transitions.'),source_url=e['source_url'],source_row_binding=dict(b,sha256=m['api_response_sha256']),pdf_path=m['pdf_path'],pdf_sha256=m['pdf_sha256'],receipt_path=str(rec.relative_to(R)),receipt_sha256=sha(rec.read_bytes()),text_path=m['text_path'],text_sha256=sha((R/m['text_path']).read_bytes()),evidence=[dict(page=hits[0],quote=q)],review_method='manual issuer body action/date/condition review; literal quote and issuer/API/hash checked',formal_approved=False))
out=B/'source_recovery/reviewed_price_002496_actual_entry_20261101.json';out.write_text(json.dumps(dict(records=result,reviewed_count=len(result),formal_written=False,registry_written=False,reviewed_at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),input_manifest_sha256=sha((O/'fetch_manifest.json').read_bytes())),ensure_ascii=False,indent=2),encoding='utf-8');print(len(result),sha(out.read_bytes()))
