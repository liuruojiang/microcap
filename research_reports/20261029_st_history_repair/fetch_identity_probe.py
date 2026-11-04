from pathlib import Path
import sys,json
from concurrent.futures import ThreadPoolExecutor, as_completed
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT))
import microcap_top100_mom16_biweekly_live_v2_0 as v
from scripts.st_notice_evidence import load_verified_events
import pandas as pd
v._sync_embedded_base_config(); f=v.base_mod.freq_mod
out=Path(__file__).with_name(sys.argv[1] if len(sys.argv)>1 else 'provider_identity');out.mkdir(exist_ok=True)
def run(code):
    frame=f.fetch_cninfo_st_notices(code,'20100104','20261029')
    frame.to_csv(out/f'{code}.csv',index=False)
    try:
        records=load_verified_events(ROOT,code,pd.Timestamp('2010-01-04'),pd.Timestamp('2026-10-29'),frame)
        return {'symbol':code,'ok':True,'rows':len(frame),'verified_ids':list(records)}
    except Exception as e:return {'symbol':code,'ok':False,'rows':len(frame),'error':str(e)}
rows=[]
with ThreadPoolExecutor(max_workers=4) as pool:
    for future in as_completed([pool.submit(run,c) for c in ['600180','002193','002198','002514']]):
        try:r=future.result()
        except Exception as e:r={'ok':False,'error':str(e)}
        rows.append(r);print(json.dumps(r,ensure_ascii=False),flush=True)
(out/'result.json').write_text(json.dumps(rows,ensure_ascii=False,indent=2),encoding='utf-8')
