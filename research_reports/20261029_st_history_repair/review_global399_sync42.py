import json,re,hashlib
from pathlib import Path
O=Path('research_reports/20261029_st_history_repair/source_recovery/global399_sync42_bodies_20261101');src=json.loads((O/'sources.json').read_text(encoding='utf-8'))
# Manual issuer-body adjudications; D/O/B describe only affirmatively stated actions, not all risk state.
raw='''300117|2024-04-29|*ST嘉寓|B
300125|2025-11-19|*ST聆达|B
300147|2025-03-25|ST香雪|O
300159|2025-03-20|*ST新研|D
300167|2023-04-28|ST迪威迅|O
300173|2025-12-23|ST福能|O
300175|2025-03-25|ST朗源|O
300178|2021-05-06|*ST腾邦|D
300198|2025-04-30|ST纳川|O
300208|2024-04-30|*ST中程|B
300211|2025-05-06|*ST亿通|D
300262|2024-05-06|*ST巴安|B
300268|2023-04-10|*ST佳沃|B
300297|2023-07-10|蓝盾退|N
300301||*ST长方|N
300309|2022-04-29|*ST吉艾|B
300311|2025-06-24|ST任子行|O
300326||凯利泰|N
300336|2022-04-29|*ST新文|B
300338|2022-05-05|ST开元|O
300344|2025-12-02|*ST立方|D
300356|2022-04-29|*ST光一|B
300367||东方网力|N
300506|2024-04-29|*ST名家|B
300526|2022-04-25|*ST中潜|B
300527|2025-08-01|ST应急|O
300630|2025-01-07|*ST普利|D
300742|2023-05-04|*ST越博|B
300799|2023-05-04|*ST左江|D
301288|2025-04-30|*ST清研|D
600069||银鸽投资|N
600136||*ST明诚|B
600265||ST景谷|N
600306||*ST商城|B
600321||正源股份|N
600423|2017-05-03|*ST柳化|D
600634|2018-05-03|*ST富控|D
600696|2018-02-09|ST匹凸|W
601258|2020-01-15|ST庞大|W
603843||*ST正平|N
688086|2022-05-06|*ST紫晶|D
688555|2022-11-22|*ST泽达|D'''
M={s:(d or None,n,k) for s,d,n,k in (line.split('|') for line in raw.splitlines())}
notes={'300297':'退市整理期首日及可转债安排，不是新的ST类别实施/撤销；不得由名称蓝盾退推断解除历史ST。日期为退市整理开始。','300301':'月度进展无当前新增动作；正文确认other自2022-05-06实施，仅可另作历史旁证，不能回填为本公告日新实施。','300326':'与审计机构沟通预告，年度报告披露后将实施；尚无具体实施日，actions为空。','300367':'2020-09-08预告，提及规则9/12生效和预计10/12前不能解决，不是确定股票实施日期；actions为空。','600069':'2014业绩初步测算/未经审计，年报披露后预期实施；公司债券暂停风险不是股票ST行动。','600265':'预告/初测2025业绩，明确实际实施前还将另行公告；不在本公告日新增退市风险。','600321':'会计调整将导致连续亏损，年报披露后才实施；无当前确定实施日。','603843':'暂不申请撤销，退市风险与其他风险继续存在；无新增或撤销动作，actions={}不是false。','600136':'法院已受理重整，确定叠加delisting并继续other；正文未给具体叠加日期，effective_date=None，不自行推算。','600306':'法院受理重整确定叠加delisting/继续other；正文写下一个交易日，需独立交易日历才能填具体日。','300125':'叠加退市风险11/19；正文确认其他风险仍存在，不清除旧类别。','300344':'本公告确定新增delisting；未明示其他风险解除，不写other=False。','300338':'只确定other实施；对未来净资产导致退市风险的条件预告不作delisting=True。'}
res=[]
for x in src:
 s=x['symbol'];d,name,k=M[s];t=re.sub(r'\s+','',Path(x['text_path']).read_text(encoding='utf-8'));assert s in t[:500],s;assert hashlib.sha256(Path(x['pdf_path']).read_bytes()).hexdigest()==x['sha256'];receipt=json.loads(Path(x['receipt_path']).read_text(encoding='utf-8'));assert receipt['sha256']==x['sha256']
 actions={'D':{'delisting':True},'O':{'other':True},'B':{'delisting':True,'other':True},'W':{'delisting':False,'other':True},'N':{}}[k]
 snippets=[t[:min(800,len(t))]]
 for match in re.finditer(r'(?:实施.{0,16}起始日|公司股票自|公司股票将于下一个交易日|继续实施其他风险警示)',t):
  snippets.append(t[max(0,match.start()-60):match.end()+150])
  if len(snippets)>=4:break
 res.append(dict(**x,notice_date=x['event']['notice_date'],effective_date=d,new_short_name=name,risk_actions=actions,review_status='no_current_action' if k=='N' else ('body_confirmed_effective_date_unresolved' if d is None else 'body_confirmed'),review_note=notes.get(s,'正文确认股票实施类别、起始日和简称；未涉及类别不赋false，不自动套用同标题其他日期。'),critical_excerpts=snippets,formal_registry_modified=False))
report=dict(scope='independent manual issuer-body review; exact event only; no registry mutation',count=len(res),records=res);(O/'adjudication.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8');print('count',len(res),'actions',sum(bool(x['risk_actions']) for x in res),'no_action',sum(not x['risk_actions'] for x in res));print(hashlib.sha256((O/'adjudication.json').read_bytes()).hexdigest())
