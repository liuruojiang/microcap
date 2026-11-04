from pathlib import Path
import json,hashlib,shutil,argparse
ROOT=Path(__file__).resolve().parents[2];BASE=Path(__file__).resolve().parent
path=ROOT/'data/st_notice_evidence/registry.json'
data=json.loads(path.read_text(encoding='utf-8-sig'))
parser=argparse.ArgumentParser();parser.add_argument('--symbols',nargs='+',default=['000059','000068','000576']);args=parser.parse_args()
for symbol in args.symbols:
    folder=BASE/'source_review'/symbol
    for p in json.loads((folder/'verified_events.json').read_text(encoding='utf-8')):
        aid=p['announcement_id'];date=p['notice_date'];name=f"{symbol}_{date.replace('-','')}_{aid}.pdf"
        src=folder/f'{date}.pdf';assert hashlib.sha256(src.read_bytes()).hexdigest()==p['source_sha256']
        e={k:p[k] for k in ['symbol','announcement_id','notice_date','title','source_url','risk_actions','actual_effective_date','reason']}
        e.update(relative_pdf_path=name,sha256=p['source_sha256'],pages=p['pdf_pages'])
        if 'security_scope' in p:e['security_scope']=p['security_scope']
        old=[r for r in data['events'] if r['announcement_id']==aid]
        if old:assert old==[e]
        else:
            shutil.copy2(src,path.parent/name);data['events'].append(e)
data['events'].sort(key=lambda e:(e['symbol'],e['notice_date'],e['announcement_id']))
path.write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n',encoding='utf-8',newline='')
print('reviewed events',len(data['events']))
