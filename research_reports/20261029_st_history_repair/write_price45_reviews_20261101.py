import pathlib,json,re,hashlib,datetime
B=pathlib.Path(__file__).resolve().parent; R=B.parents[1]; O=B/'source_recovery/global399_price45_review_20261101'
a=json.loads((O/'fetch_manifest.json').read_text(encoding='utf-8'))
# Manually reviewed event-specific decisions. Only explicitly supported risk categories are changed.
rows='''0|DO|2025-03-07|*ST旭蓝|实施退市风险警示的起始日：2025年3月7日
1|N|||申请能否获得深圳证券交易所核准仍存在较大不确定性
2|C|||股票仍被深圳证券交易所实施退市风险警示的处理
3|O||ST美谷|本次被叠加实施“其他风险警示”公司股票不停牌
4|CLEAR|2013-01-14|领先科技|自2013年1月14日起撤销本公司股票交易的退市风险警示
5|O|2026-05-06|ST海龙|其他风险警示实施起始日为2026年5月6日
6|O|2025-10-10|ST沈化|实施其他风险警示的起始日：2025年10月10日
7|DO|2026-04-28|*ST西发|并自2026年4月28日复牌交易起被实施退市风险警示
8|DO|2026-03-02|*ST华闻|公司股票交易将于2026年3月2日被实施退市风险警示
9|N|||提交了关于撤销公司股票交易退市风险警示的申请
10|O|2026-01-20|ST京机|实施其他风险警示的起始日：2026年1月20日
11|DO|2025-10-23|*ST景峰|公司股票交易将于2025年10月23日被实施退市风险警示
12|WITHDRAW_D_KEEP_O|2010-04-12|ST琼花|公司股票交易自2010年4月12日开市时起撤销退市风险警示并实行其他特别处理
13|CLEAR|2014-01-13|成霖股份|公司股票交易自2014年1月13日开市时起撤销退市风险警示
14|O|2026-01-06|ST得润|实施其他风险警示的起始日：2026年1月6日
15|WITHDRAW_D_KEEP_O|2012-05-22|ST天润|公司股票交易自2012年5月22日开市时起撤销退市风险警示并实行其他特别处理
16|DO|2023-05-05|*ST紫鑫|公司股票自2023年5月5日同时被实施“退市风险警示”和“其他风险警示”
17|D|2023-07-21|*ST正邦|公司股票交易将于2023年7月21日起被叠加实施退市风险警示
18|CLEAR|2014-03-17|斯米克|公司也不存在其他涉及退市风险警示及其他风险警示的情形
19|O|2026-05-06|ST如意|实施其他风险警示的起始日：2026年5月6日
20|O|2026-05-06|ST嘉应|公司股票自2026年5月6日起被实施“其他风险警示”
21|CLEAR|2014-03-05|绿大地|公司不存在《深圳证券交易所股票上市规则》（2012年修订）规定的其他需要实行退市风险警示或其他风险警示的情形
22|O|2022-05-06|ST宏达|公司股票自2022年5月6日起被实施“其他风险警示”
23|N|||若公司2019年度经审计的净利润仍为负值
24|WITHDRAW_D_KEEP_O|2019-12-04|ST百特|撤销退市风险警示及继续实行其他风险警示的起始日为：2019年12月4日
25|DO|2024-05-06|*ST洪涛|实施退市风险警示和其他风险警示的起始日：2024年5月6日
26|O|2024-05-06|ST新纶|实施其他风险警示的起始日：2024年5月6日
27|O|2024-05-06|ST百灵|实施其他风险警示的起始日：2024年5月6日
28|BOND|||公司债券（第一期）（债券简称：12希努01，债券代码：112146）停牌一天
29|O|2026-03-03|ST达华|实施其他风险警示的起始日：2026年3月3日
30|O|2024-05-06|ST英飞拓|实施其他风险警示的起始日：2024年5月6日
31|O|2024-06-03|ST爱康|本次叠加其他风险警示的起始日为2024年6月3日
32|O|2025-04-30|ST瑞和|实施其他风险警示的起始日：2025年4月30日
33|D|2025-01-03|*ST仁东|实施退市风险警示的起始日：2025年1月3日
34|DO|2025-01-14|*ST摩登|实施退市风险警示的起始日为2025年1月14日
35|O|2025-12-02|ST远智|实施其他风险警示的起始日：2025年12月2日
36|O|2022-05-05|ST爱迪尔|公司股票将于2022年5月5日开市起被实施其他风险警示
37|DO|2025-08-11|*ST三圣|公司股票交易将于2025年8月11日被叠加实施退市风险警示
38|DO|2022-05-06|*ST奇信|实施退市风险警示的起始日：2022年5月6日
39|DO|2025-08-21|*ST中装|自2025年8月21日开市起复牌
40|PENDING|||自该规定实施后，公司股票交易将被实施其他风险警示（ST）
41|N|||如果公司控股股东及实际控制人未能在2021年1月25日前解决其非经营性占用公司资金问题
42|O|2025-03-18|ST中青宝|实施其他风险警示的起始日：2025年3月18日
43|O|2024-12-12|ST智云|实施其他风险警示的起始日：2024年12月12日
44|DO||*ST保力|自2024年4月30日复牌之日起，深圳证券交易所对公司股票交易实施退市风险警示和其他风险警示'''
actions={'DO':{'delisting':True,'other':True},'O':{'other':True},'D':{'delisting':True},'CLEAR':{'delisting':False,'other':False},'WITHDRAW_D_KEEP_O':{'delisting':False,'other':True},'N':{},'C':{},'BOND':{},'PENDING':{}}
notes={2:'Continuation of existing delisting risk; no new transition or inferred start. Current known delisting=True.',3:'Definite additional other risk, no explicit effective date; leave null.',16:'One body sentence misprints 2022-05-05; repeated special notice and explicit start field both 2023-05-05. Preserve discrepancy; source owner must crosscheck.',17:'Additional delisting reason; no opposite-risk reset inferred.',33:'Only delisting action established; no other=False inferred from ordinary previous name.',40:'Rule commencement 2020-09-12 is not a confirmed stock execution date; planned future treatment, retain pending.',44:'Contradiction: page1 says 2024-04-29 start; page2 says 2024-04-30 on resumption. Actions certain but effective_date unresolved; do not autopromote.'}
def sha(x):return hashlib.sha256(x).hexdigest()
def flat(x):return re.sub(r'\s+','',x)
result=[]
for line in rows.splitlines():
 idx,kind,day,name,q=line.split('|'); i=int(idx);m=a[i];e=m['event'];t=(R/m['text_path']).read_text(encoding='utf-8');pgs=re.split(r'\[PAGE \d+\]',t)[1:];hits=[j+1 for j,p in enumerate(pgs) if flat(q) in flat(p)];assert hits,(i,q)
 assert m['symbol'] in t,(i,'issuer'); assert not name or flat(name) in flat(t)
 pdf=R/m['pdf_path']; assert sha(pdf.read_bytes())==m['pdf_sha256']; rec=O/(e['announcement_id']+'.receipt.json'); rc=json.loads(rec.read_text());assert rc['sha256']==m['pdf_sha256'] and rc['url']==e['source_url']
 b=e['source_row_binding'];raw=(R/b['path']).read_bytes();assert sha(raw)==m['api_response_sha256'];rr=json.loads(raw)['data']['list'][b['index']];assert rr['art_code']==e['announcement_id'] and rr['notice_date']==e['provider_notice_date'] and rr['title']==e['provider_title']
 result.append(dict(symbol=m['symbol'],announcement_id=e['announcement_id'],source_provider='eastmoney',notice_date=e['notice_date'],effective_date=day or None,new_security_name=name or None,risk_actions=actions[kind],decision=kind,complete_risk_state_after_event=len(actions[kind])==2,no_action=kind in ['N','C','BOND'],requires_date_crosscheck=i in [3,16,40,44],review_note=notes.get(i,'Only current issuer-confirmed action; conditional/rule examples not treated as transitions.'),source_url=e['source_url'],source_row_binding=dict(b,sha256=m['api_response_sha256']),pdf_path=m['pdf_path'],pdf_sha256=m['pdf_sha256'],receipt_path=str(rec.relative_to(R)),receipt_sha256=sha(rec.read_bytes()),text_path=m['text_path'],text_sha256=sha((R/m['text_path']).read_bytes()),evidence=[dict(page=hits[0],quote=q)],review_method='manual issuer body action/date/condition review; literal quote and issuer/API/hash checked',formal_approved=False))
out=B/'source_recovery/reviewed_global399_price45_candidates_20261101.json';out.write_text(json.dumps(dict(records=result,reviewed_count=len(result),formal_written=False,registry_written=False,reviewed_at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),input_manifest_sha256=sha((O/'fetch_manifest.json').read_bytes())),ensure_ascii=False,indent=2),encoding='utf-8');print(len(result),sha(out.read_bytes()))
