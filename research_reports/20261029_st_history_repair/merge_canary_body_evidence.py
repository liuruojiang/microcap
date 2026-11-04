"""Apply the five explicitly reviewed canary events to the review branch only."""
from pathlib import Path
import json, hashlib, shutil
import pandas as pd
ROOT=Path(__file__).resolve().parents[2]
BASE=Path(__file__).resolve().parent
REG=ROOT/'data/st_notice_evidence/registry.json'
backup=ROOT/'.codex_backups/st_canary_body_evidence_20261029'
backup.mkdir(exist_ok=True)
if not (backup/'registry.json').exists(): shutil.copy2(REG,backup/'registry.json')
payload=json.loads(REG.read_text(encoding='utf-8-sig'))
additions=[]
for symbol in ['000010','000037']:
    proposal=json.loads((BASE/'source_review'/symbol/'proposed_event.json').read_text(encoding='utf-8'))
    keys=['symbol','announcement_id','notice_date','title','source_url','sha256','relative_pdf_path','risk_actions','actual_effective_date','pages','reason']
    event={k:proposal[k] for k in keys}
    src=BASE/'source_review'/symbol/event['relative_pdf_path']
    additions.append((event,src))
for proposal in json.loads((BASE/'source_review/000035/verified_events.json').read_text(encoding='utf-8')):
    aid=Path(proposal['source_url']).stem
    notices=pd.read_csv(BASE/'st_recert_runtime/canary10/notices/000035.csv',dtype=str)
    row=notices.loc[notices.announcement_id.eq(aid)]
    assert len(row)==1
    event={'symbol':'000035','announcement_id':aid,'notice_date':proposal['notice_date'],'title':row.iloc[0]['title'],
           'source_url':proposal['source_url'],'sha256':proposal['source_sha256'],
           'relative_pdf_path':f"000035_{proposal['notice_date'].replace('-','')}_{aid}.pdf",
           'risk_actions':proposal['risk_actions'],'actual_effective_date':proposal['actual_effective_date'],
           'pages':proposal['pdf_pages'],'reason':proposal['reason']}
    additions.append((event,BASE/'source_review/000035'/f"{proposal['notice_date']}.pdf"))
for event,src in additions:
    assert hashlib.sha256(src.read_bytes()).hexdigest()==event['sha256']
    old=[e for e in payload['events'] if e['announcement_id']==event['announcement_id']]
    if old:
        assert old==[event], 'conflicting reviewed event'
        continue
    shutil.copy2(src,REG.parent/event['relative_pdf_path'])
    payload['events'].append(event)
payload['events'].sort(key=lambda e:(e['symbol'],e['notice_date'],e['announcement_id']))
REG.write_text(json.dumps(payload,ensure_ascii=False,indent=2)+'\n',encoding='utf-8',newline='')
print('reviewed events',len(payload['events']),'registry sha256',hashlib.sha256(REG.read_bytes()).hexdigest())
