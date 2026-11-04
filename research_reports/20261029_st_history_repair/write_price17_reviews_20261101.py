import pathlib,json,re,hashlib,datetime
B=pathlib.Path(__file__).resolve().parent; R=B.parents[1]; O=B/'source_recovery/price17_pdf_review_20261101'
a=json.loads((O/'fetch_manifest.json').read_text(encoding='utf-8'))
# Manually reviewed event-specific decisions. Only explicitly supported risk categories are changed.
rows='''0|CLEAR|2010-03-17|科苑集团|且不存在13.3.1条规定的关于股票交易实行其他特别处理的情形
1|D|2010-04-27|*ST博盈|实行退市风险警示的起始日：2010年4月27日
2|D|2011-04-20|*ST西轴|2011年4月20日起实行退市风险警示的特别处理
3|D|2012-04-05|*ST东电|实行退市风险警示的起始日：自2012年4月5日起实行
4|D|2012-04-23|*ST索芙|自2012年4月23日起实行退市风险警示的特别处理
5|D|2012-05-02|*ST中基|自2012年5月2日起实行退市风险警示的特别处理
6|O|2016-05-04|ST亚太|实施其他风险警示的起始日：2016年5月4日
7|O|2018-01-19|ST龙力|实行其他风险警示的起始日：2018年1月19日开市起
8|O|2018-07-26|ST长生|公司股票自2018年7月26日开市起被实施其他风险警示
9|O|2019-01-23|ST康得新|自2019年1月23日开市起，公司股票交易实施其他风险警示
10|O|2020-07-27|ST圣莱|自2020年7月27日起，公司股票交易被实施其他风险警示
11|O|2022-06-06|ST美盛|于2022年6月6日开市起复牌，复牌后公司股票被实行其他风险警示
12|O|2023-05-05|ST阳光城|公司股票自2023年5月5日开市起被实施其他风险警示
13|WITHDRAW_D_KEEP_O|2011-06-22|ST当代|公司股票交易自2011年6月22日开市时起撤销退市风险警示并实行其他特别处理
14|D|2013-05-02|*ST国恒|2013年5月2日起实行退市风险警示特别处理
15|N|||公司股票将可能在2019年年度报告披露后被实施退市风险警示'''
actions={'DO':{'delisting':True,'other':True},'O':{'other':True},'D':{'delisting':True},'CLEAR':{'delisting':False,'other':False},'WITHDRAW_D_KEEP_O':{'delisting':False,'other':True},'N':{},'C':{},'BOND':{},'PENDING':{}}
notes={0:'Explicit absence of other treatment, actual normal name and 10% limit; clear both. Date missing 日 in original literal sentence corroborated by explicit resumption date.',14:'Title/application wording does not negate explicit scheduled 2013-05-02 implementation in body.',15:'Conditional audited-loss forecast; no current ST action. Must remove erroneous transition, not force entire issuer normal.'}
def sha(x):return hashlib.sha256(x).hexdigest()
def flat(x):return re.sub(r'\s+','',x)
result=[]
for line in rows.splitlines():
 idx,kind,day,name,q=line.split('|'); i=int(idx);m=a[i];e=m['event'];t=(R/m['text_path']).read_text(encoding='utf-8');pgs=re.split(r'\[PAGE \d+\]',t)[1:];hits=[j+1 for j,p in enumerate(pgs) if flat(q) in flat(p)];assert hits,(i,q)
 assert m['symbol'] in t,(i,'issuer'); assert not name or flat(name) in flat(t)
 pdf=R/m['pdf_path']; assert sha(pdf.read_bytes())==m['pdf_sha256']; rec=O/(e['announcement_id']+'.receipt.json'); rc=json.loads(rec.read_text());assert rc['sha256']==m['pdf_sha256'] and rc['url']==e['source_url']
 b=e['source_row_binding'];raw=(R/b['path']).read_bytes();assert sha(raw)==m['api_response_sha256'];rr=json.loads(raw)['data']['list'][b['index']];assert rr['art_code']==e['announcement_id'] and rr['notice_date']==e['provider_notice_date'] and rr['title']==e['provider_title']
 result.append(dict(symbol=m['symbol'],announcement_id=e['announcement_id'],source_provider='eastmoney',notice_date=e['notice_date'],effective_date=day or None,new_security_name=name or None,risk_actions=actions[kind],decision=kind,complete_risk_state_after_event=len(actions[kind])==2,no_action=kind in ['N','C','BOND'],requires_date_crosscheck=False,review_note=notes.get(i,'Only current issuer-confirmed action; conditional/rule examples not treated as transitions.'),source_url=e['source_url'],source_row_binding=dict(b,sha256=m['api_response_sha256']),pdf_path=m['pdf_path'],pdf_sha256=m['pdf_sha256'],receipt_path=str(rec.relative_to(R)),receipt_sha256=sha(rec.read_bytes()),text_path=m['text_path'],text_sha256=sha((R/m['text_path']).read_bytes()),evidence=[dict(page=hits[0],quote=q)],review_method='manual issuer body action/date/condition review; literal quote and issuer/API/hash checked',formal_approved=False))
out=B/'source_recovery/reviewed_long34_price17_candidates_20261101.json';out.write_text(json.dumps(dict(records=result,reviewed_count=len(result),formal_written=False,registry_written=False,reviewed_at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),input_manifest_sha256=sha((O/'fetch_manifest.json').read_bytes())),ensure_ascii=False,indent=2),encoding='utf-8');print(len(result),sha(out.read_bytes()))
