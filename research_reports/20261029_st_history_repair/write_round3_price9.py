import pathlib,json,re,hashlib,datetime
B=pathlib.Path(__file__).resolve().parent; R=B.parents[1]; O=B/'source_recovery/round3_price9_pdf_20261101'
a=json.loads((O/'fetch_manifest.json').read_text(encoding='utf-8'))
# Manually reviewed event-specific decisions. Only explicitly supported risk categories are changed.
rows='''0|D|2011-05-04|*ST大地|实施“退市风险警示”特别处理的起始日：2011年5月4日开市时起
1|CLEAR|2011-05-30|福建三农|本公司股票交易自2011年5月30日起撤销其他特别处理
2|D|2012-04-26|*ST银河|公司股票将于2012年4月25日停牌一天，4月26日起实行“退市风险警示”特别处理
3|D|2013-02-08|*ST上控|公司股票自2013年2月8日起实施“退市风险警示”特别处理
4|O|2019-03-28|ST高升|公司股票自2019年3月28日开市起被实行其他风险警示
5|O|2019-03-29|ST银河|实行其他风险警示的起始日：2019年3月29日开市起
6|O|2020-01-13|ST摩登|实行其他风险警示的起始日：2020年1月13日开市起
7|CLEAR|2014-03-13|中冠A|自2014年3月13日起撤销对公司股票交易实施的退市风险警示以及其他风险警示的特别处理
8|CLEAR|2014-03-11|天一科技|公司股票交易自2014年3月11日起撤销退市风险警示及其他风险警示
9|CLEAR|2015-03-31|南华生物|亦不存在上市规则第13.3.1条规定的应实施其它风险警示的情形
10|CLEAR|2017-05-12|南华生物|公司股票现已不存在被实行退市风险警示的情形，也不存在被实行其他风险警示的情形'''
actions={'DO':{'delisting':True,'other':True},'O':{'other':True},'D':{'delisting':True},'CLEAR':{'delisting':False,'other':False},'WITHDRAW_D_KEEP_O':{'delisting':False,'other':True},'N':{},'C':{},'BOND':{},'PENDING':{}}
notes={1:'Actual ordinary name plus restored10% limit after explicit other withdrawal; both risks absent. Subsequent normal renamings do not require fake exit events.',7:'Explicit withdrawal of both categories; A and B codes identified, target000018 is A-share.',9:'Actual approval and next-session normal name plus explicit no other risk.',10:'Actual approval and normal name plus explicit absence of both categories.'}
def sha(x):return hashlib.sha256(x).hexdigest()
def flat(x):return re.sub(r'\s+','',x)
result=[]
for line in rows.splitlines():
 idx,kind,day,name,q=line.split('|'); i=int(idx);m=a[i];e=m['event'];t=(R/m['text_path']).read_text(encoding='utf-8');pgs=re.split(r'\[PAGE \d+\]',t)[1:];hits=[j+1 for j,p in enumerate(pgs) if flat(q) in flat(p)];assert hits,(i,q)
 assert m['symbol'] in t,(i,'issuer'); assert not name or flat(name) in flat(t)
 pdf=R/m['pdf_path']; assert sha(pdf.read_bytes())==m['pdf_sha256']; rec=O/(e['announcement_id']+'.receipt.json'); rc=json.loads(rec.read_text());assert rc['sha256']==m['pdf_sha256'] and rc['url']==e['source_url']
 b=e['source_row_binding'];raw=(R/b['path']).read_bytes();assert sha(raw)==m['api_response_sha256'];rr=json.loads(raw)['data']['list'][b['index']];assert rr['art_code']==e['announcement_id'] and rr['notice_date']==e['provider_notice_date'] and rr['title']==e['provider_title']
 result.append(dict(symbol=m['symbol'],announcement_id=e['announcement_id'],source_provider='eastmoney',notice_date=e['notice_date'],effective_date=day or None,new_security_name=name or None,risk_actions=actions[kind],decision=kind,complete_risk_state_after_event=len(actions[kind])==2,no_action=kind in ['N','C','BOND'],requires_date_crosscheck=False,review_note=notes.get(i,'Only current issuer-confirmed action; conditional/rule examples not treated as transitions.'),source_url=e['source_url'],source_row_binding=dict(b,sha256=m['api_response_sha256']),pdf_path=m['pdf_path'],pdf_sha256=m['pdf_sha256'],receipt_path=str(rec.relative_to(R)),receipt_sha256=sha(rec.read_bytes()),text_path=m['text_path'],text_sha256=sha((R/m['text_path']).read_bytes()),evidence=[dict(page=hits[0],quote=q)],review_method='manual issuer body action/date/condition review; literal quote and issuer/API/hash checked',formal_approved=False))
out=B/'source_recovery/reviewed_round3_price9_11events_20261101.json';out.write_text(json.dumps(dict(records=result,reviewed_count=len(result),formal_written=False,registry_written=False,reviewed_at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),input_manifest_sha256=sha((O/'fetch_manifest.json').read_bytes())),ensure_ascii=False,indent=2),encoding='utf-8');print(len(result),sha(out.read_bytes()))
