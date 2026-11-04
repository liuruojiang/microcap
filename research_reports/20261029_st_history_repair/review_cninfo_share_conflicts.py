from pathlib import Path
import json,hashlib
from decimal import Decimal
import pandas as pd
B=Path(__file__).resolve().parent;D=B/'old4975_shares_tail_20261029';records=[]
for r in json.loads((D/'final_independent_manifest.json').read_text(encoding='utf-8'))['conflicts']:
 s=r['symbol'];folder=B/('cninfo_share_api_probe_20261030' if s in ['000422','002869'] else 'cninfo_share_conflicts_probe_20261030');p=folder/(s+'_parsed.csv');receipt=folder/(s+'_receipt.json')
 if not p.exists():records.append(dict(symbol=s,status='pending_original_provider'));continue
 f=pd.read_csv(p,dtype=str);matches=f[f['变动日期'].eq(r['anchor_date'])];amounts=[str(Decimal(x)*10000) for x in matches['总股本']]
 e=json.loads(receipt.read_text(encoding='utf-8'));raw=folder/(s+'_response.json');assert hashlib.sha256(raw.read_bytes()).hexdigest()==e['sha256']
 records.append(dict(symbol=s,anchor_date=r['anchor_date'],old_shares=r['old_anchor_shares'],current_original_provider_shares=amounts,old_anchor_current_provider_supported=any(Decimal(x)==Decimal(r['old_anchor_shares']) for x in amounts),row_count=len(f),raw_sha256=e['sha256'],source_receipt=str(receipt),status='original_provider_reconfirmed_not_independent_issuer_certification',known_official_correction_required=s=='002869',later_events=f[f['变动日期'].gt(r['anchor_date'])][['变动日期','公告日期','总股本','变动原因']].to_dict('records')))
(D/'current_cninfo_anchor_reconciliation.json').write_text(json.dumps(dict(formal_written=False,records=records),ensure_ascii=False,indent=2),encoding='utf-8')
print([(r['symbol'],r.get('old_anchor_current_provider_supported')) for r in records])
