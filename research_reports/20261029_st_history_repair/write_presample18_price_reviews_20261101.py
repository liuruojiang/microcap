"""Persist manually read issuer-body decisions, verifying every quoted excerpt."""
from pathlib import Path
import datetime,hashlib,json,re
B=Path(__file__).resolve().parent;ROOT=B.parents[1]
O=B/'source_recovery/presample18_price_review_20261101'
manifest=json.loads((O/'fetch_manifest.json').read_text())
# symbol: effective date, new name, other-risk remains, page, short literal evidence
decisions={
'600076':('2010-06-09','ST华光',True,1,'自2010年6月9日起撤销对本公司股票交易实行退市风险警示特别处理，仍实行其他特别处理'),
'600084':('2011-12-22','ST中葡',True,1,'经上海证券交易所审核批准，同意撤销对公司股票交易的退市风险警示，股票交易仍实行其他特别处理'),
'600115':('2010-05-28','東方航空',False,2,'公司的A股股票簡稱自2010年5月28日起由「ST東航」變更為「東方航空」'),
'600149':('2012-03-27','ST廊发展',True,1,'经上海证券交易所批准，公司股票撤销退市风险警示同时实施其他特别处理'),
'600155':('2014-03-10','宝硕股份',False,2,'公司也不存在触及其他风险警示的情形'),
'600187':('2010-08-26','国中水务',False,2,'公司股票简称由“ST国中”变更为“国中水务”，公司股票交易日涨跌幅限制由5%恢复为10%'),
'600207':('2012-08-22','安彩高科',False,2,'本公司也不存在触及其他风险警示的情形'),
'600217':('2011-07-22','ST秦岭',True,2,'经上海证券交易所批准同意撤销退市风险警示同时对本公司股票实施其他特别处理'),
'600223':('2010-03-11','鲁商置业',False,1,'公司股票简称将由“ST鲁置业”变更为“鲁商置业”，公司股票的日涨跌幅限制将由5%恢复为10%'),
'600234':('2010-06-09','ST天龙',True,1,'自2010年6月9日起撤销本公司股票交易实行的退市风险警示，同时继续对公司股票实行其他特别处理'),
'600340':('2010-07-01','ST国祥',True,1,'自2010年7月1日起撤销对本公司股票交易实行退市风险警示特别处理，仍实行其他特别处理'),
'600419':('2012-08-28','新疆天宏',False,2,'本公司也不存在触及其他风险警示的情形'),
'600506':('2010-11-11','ST香梨',True,1,'本公司股票交易自2010年11月11日起撤销退市风险警示，但仍实行其他特别处理'),
'600515':('2011-05-06','ST筑信',True,1,'同意撤销对本公司股票退市风险警示，实行其他特别处理'),
'600556':('2014-05-08','北生药业',False,2,'公司也不存在触及其他风险警示的情形'),
'600579':('2014-04-29','天华院',False,1,'公司也不存在触及其他风险警示的情形'),
'600603':('2014-03-10','大洲兴业',False,2,'公司也不存在触及其他风险警示的情形'),
'600617':('2011-12-22','ST联华',True,1,'经上海证券交易所批准，同意本公司股票交易撤销退市风险警示同时对公司股票实施其他特别处理'),
}
def sha(raw):return hashlib.sha256(raw).hexdigest()
def flat(text):return re.sub(r'\s+','',text)
reviews=[]
for item in manifest:
    assert item['status']=='body_ready_for_review'
    s=item['symbol'];effective,new_name,other,page,quote=decisions[s]
    text=Path(item['text_path']).read_text();pages=re.split(r'\[PAGE \d+\]',text)[1:]
    assert flat(quote) in flat(pages[page-1]),(s,'quote not literal')
    assert flat(new_name) in flat(text),(s,'new name missing')
    assert s in text,(s,'issuer code missing')
    pdf=ROOT/item['pdf_path'];assert sha(pdf.read_bytes())==item['pdf_sha256']
    event=item['event'];binding=event['source_row_binding'];api=ROOT/binding['path'];api_raw=api.read_bytes();assert sha(api_raw)==item['api_response_sha256']
    rawrow=json.loads(api_raw)['data']['list'][binding['index']]
    assert rawrow['art_code']==event['announcement_id'] and rawrow['title']==event['provider_title'] and rawrow['notice_date']==event['provider_notice_date']
    receipt=O/(event['announcement_id']+'.receipt.json');rc=json.loads(receipt.read_text());assert rc['status']==200 and rc['sha256']==item['pdf_sha256'] and rc['url']==event['source_url']
    reviews.append(dict(symbol=s,announcement_id=event['announcement_id'],source_provider='eastmoney',
        notice_date=event['notice_date'],effective_date=effective,new_security_name=new_name,
        risk_actions={'delisting':False,'other':other},complete_risk_state_after_event=True,
        decision='withdraw_delisting_keep_other' if other else 'withdraw_all_risk_normal_name',
        source_url=event['source_url'],source_row_binding=dict(binding,sha256=item['api_response_sha256']),
        pdf_path=item['pdf_path'],pdf_sha256=item['pdf_sha256'],receipt_path=str(receipt.relative_to(ROOT)),receipt_sha256=sha(receipt.read_bytes()),
        text_path=item['text_path'],text_sha256=sha(Path(item['text_path']).read_bytes()),
        evidence=[dict(page=page,quote=quote)],
        review_method='manual full PDF text review; quote/issuer/API row/hash independently read back',
        actual_publication_date_not_signature_date=True,formal_approved=False,
        review_note=('HK overseas regulatory issuer notice explicitly identifies A-share 600115; page 2 visually checked. Normal name and restored 10% limit prove no remaining ST treatment.' if s=='600115' else
                    'Header ordinary name is stale; body explicitly retains other ST, page 1 visually checked.' if s=='600506' else
                    'Actual issuer approval and dated new name read in full body; no prior-day announcement date inferred.')))
report=dict(formal_written=False,registry_written=False,reviewed_at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
    reviewed_count=len(reviews),remaining_unknown=[],retains_other_count=sum(r['risk_actions']['other'] for r in reviews),
    input_manifest_sha256=sha((O/'fetch_manifest.json').read_bytes()),records=reviews)
p=B/'source_recovery/reviewed_presample18_price_candidates.json';p.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
print(len(reviews),'complete evidence candidates',sha(p.read_bytes()))
