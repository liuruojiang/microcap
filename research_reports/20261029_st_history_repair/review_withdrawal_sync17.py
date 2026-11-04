import json,hashlib,re
from pathlib import Path
O=Path('research_reports/20261029_st_history_repair/source_recovery/withdrawal_sync17_20261101');src=json.loads((O/'sources.json').read_text())
M={'600645':('2012-03-19','中源协和',False),'600699':('2011-08-25','ST得亨',True),'600706':('2012-09-04','长安信息',False),'600711':(None,'雄震矿业',False),'600714':('2011-04-29','金瑞矿业',False),'600715':('2012-05-28','ST松辽',True),'600722':('2011-05-06','ST金化',True),'600728':('2010-09-13','ST新太',True),'600757':('2011-08-22','ST源发',True),'600771':('2013-04-26','东盛科技',False),'600793':(None,'ST宜纸',True),'600817':('2018-05-09','ST宏盛',True),'600876':('2011-04-19','洛陽玻璃',False),'600984':('2011-11-15','ST建机',True),'600988':('2011-09-06','ST宝龙',True),'600800':('2012-08-31','天津磁卡',False),'600868':('2012-08-22','梅雁水电',False)}
rows=[]
for x in src:
 s=x['symbol'];day,name,other=M[s];text=Path(x['text_path']).read_text();compact=re.sub(r'\s+','',text);p=Path(x['pdf_path']);assert hashlib.sha256(p.read_bytes()).hexdigest()==x['sha256'];rec=json.loads(Path(x['receipt_path']).read_text());assert rec['sha256']==x['sha256']
 actions={'delisting':False,'other':other,'unspecified':False}
 note='正文明确实施日及新简称；无ST新简称及10%幅度支持完全撤销，ST简称及5%幅度支持退市风险撤销但其他风险继续。'
 status='body_review_complete'
 if s=='600711':status='effective_date_conflict_pending';note='原PDF落款2011-03-02/公告2011-03-03，却正文两次写2010-03-03停牌/2010-03-04撤销；不擅改年份。动作/新简称明确，实际实施日须独立佐证。'
 if s=='600793':status='no_action_continues_other';actions={};note='不能撤销是无动作；现状other=True，不能用分类撤销清除任何风险。实际撤销另见补充2016公告。'
 idx=compact.rfind('股票简称');idx=idx if idx>=0 else compact.find('不能撤销');quote=compact[max(0,idx-260):idx+300]
 rows.append(dict(**x,review_status=status,effective_date=day,new_short_name=name,risk_actions=actions,current_other_risk=other,review_note=note,critical_excerpt=quote,formal_registry_modified=False))
p=O/'AN201611170107306580.pdf';t=(O/'600793_actual_withdrawal.txt').read_text();rows.append(dict(symbol='600793',announcement_id='AN201611170107306580',notice_date='2016-11-18',pdf_path=str(p),receipt_path=str(p.with_suffix('.receipt.json')),sha256=hashlib.sha256(p.read_bytes()).hexdigest(),text_path=str(O/'600793_actual_withdrawal.txt'),review_status='body_review_complete',effective_date='2016-11-21',new_short_name='宜宾纸业',risk_actions={'delisting':False,'other':False,'unspecified':False},critical_excerpt='撤销其他风险警示的起始日：2016年11月21日；公司的股票简称将从“ST宜纸”变更为“宜宾纸业”；股票价格的日涨跌幅限制从5%变为10%。',formal_registry_modified=False))
(O/'adjudication.json').write_text(json.dumps(dict(scope='independent issuer-body review recommendation; no registry write',records=rows),ensure_ascii=False,indent=2),encoding='utf-8');print('records',len(rows));print('sha',hashlib.sha256((O/'adjudication.json').read_bytes()).hexdigest())
