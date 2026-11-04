"""Install observed current names in the isolated runtime, never historical ST."""
from pathlib import Path
import csv, hashlib, json, re

ROOT=Path(__file__).resolve().parents[2]
RESEARCH=Path(__file__).resolve().parent
SOURCE=RESEARCH/'resume_audit/current_names_tencent_20261101'
DEST=RESEARCH/'replay/runtime_20261029/.microcap_index_cache'
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def read(p): return list(csv.DictReader(p.open(encoding='utf-8-sig',newline='')))
def write(p,fields,rows):
    with p.open('w',encoding='utf-8',newline='') as f:
        w=csv.DictWriter(f,fieldnames=fields);w.writeheader();w.writerows(rows)

def main():
    master=DEST/'security_master.csv'
    snapshot=json.loads((SOURCE/'snapshot.json').read_text(encoding='utf-8'))
    assert sha(master)==snapshot['master_sha256']
    eligible={r['symbol'] for r in read(master) if r['symbol'].startswith(('0','3','6')) and r['list_date']<='2026-10-30' and (not r['delist_date'] or r['delist_date']>'2026-10-30')}
    observed={}; evidence={}
    for receipt in sorted(SOURCE.glob('batch_*.receipt.json')):
        r=json.loads(receipt.read_text(encoding='utf-8'));raw=receipt.with_name(receipt.name.replace('.receipt.json','.gbk'))
        assert r['status']==200 and sha(raw)==r['sha256']
        assert r['url']=='https://qt.gtimg.cn/q='+','.join(r['requested'])
        got=set()
        for line in raw.read_bytes().decode('gbk',errors='strict').splitlines():
            match=re.fullmatch(r'v_(\w+)="(.*)";',line.strip())
            if not match: continue
            key,body=match.groups();fields=body.split('~')
            assert key in r['requested'] and key not in got
            code=fields[2];name=fields[1];stamp=fields[30]
            assert key[-6:]==code and len(stamp)==14 and stamp[:8]=='20261030' and stamp[8:]>='150000'
            assert code not in observed and name
            got.add(key);observed[code]=(key,name,stamp)
        assert got==set(r['requested'])
        evidence[str(raw.relative_to(ROOT))]=sha(raw);evidence[str(receipt.relative_to(ROOT))]=sha(receipt)
    assert set(observed)==eligible and len(observed)==5168
    prior=read(SOURCE/'current_name_snapshot.csv')
    assert {r['symbol']:(r['quote_key'],r['name'],r['quote_time']) for r in prior}==observed
    risk=lambda name: re.sub(r'\s+','',name).upper().startswith(('*ST','ST','PT'))
    st=[{'code':c,'name':n} for c,(k,n,t) in sorted(observed.items()) if risk(n)]
    assert len(st)==191
    outputs={'current_st.csv':(['code','name'],st),'active_universe.csv':(['symbol','name','code'],[{'symbol':k,'name':n,'code':c} for c,(k,n,t) in sorted(observed.items())])}
    backup=RESEARCH/'replay/current_names_before_20261101';backup.mkdir(exist_ok=True)
    before={}
    for name,(fields,rows) in outputs.items():
        target=DEST/name
        if target.exists():
            before[name]=sha(target);saved=backup/name
            if saved.exists(): assert saved.read_bytes()==target.read_bytes(),'Do not overwrite an earlier backup'
            else: saved.write_bytes(target.read_bytes())
        write(target,fields,rows);assert read(target)==rows
    report={'scope':'isolated current-name filter only; no historical ST projection','target_close':'2026-10-30','observed_on':'2026-11-01','current_names':5168,'current_risk_names':191,'master_sha256':sha(master),'source_files':evidence,'prior_files':before,'installed':{n:sha(DEST/n) for n in outputs},'formal_written':False}
    out=RESEARCH/'replay/current_names_install_20261101.json';out.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps({'rows':5168,'st':191,'report_sha256':sha(out)}))
if __name__=='__main__':main()
