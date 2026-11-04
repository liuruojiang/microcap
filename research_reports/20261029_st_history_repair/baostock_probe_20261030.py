"""Small read-only provider probe; no production/cache writes."""
from pathlib import Path
import json, hashlib, socket
from datetime import datetime,timezone
import baostock as bs

socket.setdefaulttimeout(15)
out=Path(__file__).resolve().parent/'baostock_probe_20261030'
out.mkdir(exist_ok=True)
login=bs.login()
summary={'provider':'baostock','retrieved_at_utc':datetime.now(timezone.utc).isoformat(),
         'login_error_code':login.error_code,'login_error_msg':login.error_msg,'approved':False,'queries':[]}
if login.error_code!='0':
    (out/'summary.json').write_text(json.dumps(summary,indent=2),encoding='utf-8')
    raise SystemExit('login failed')
try:
    for label,code,start,end,flag in [
        ('000418_cash_raw','sz.000418','2012-06-08','2012-06-12','3'),
        ('000418_cash_qfq','sz.000418','2012-06-08','2012-06-12','2'),
        ('000007_tail_raw','sz.000007','2026-10-01','2026-10-29','3'),
        ('002072_tail_raw','sz.002072','2026-10-01','2026-10-29','3'),
    ]:
        rs=bs.query_history_k_data_plus(code,'date,code,close,preclose,volume,tradestatus,isST,adjustflag',
               start_date=start,end_date=end,frequency='d',adjustflag=flag)
        rows=[]
        while rs.error_code=='0' and rs.next(): rows.append(rs.get_row_data())
        record={'code':code,'start':start,'end':end,'adjustflag':flag,'error_code':rs.error_code,
                'error_msg':rs.error_msg,'fields':rs.fields,'rows':rows}
        raw=json.dumps(record,ensure_ascii=False,indent=2).encode('utf-8'); path=out/(label+'.json');path.write_bytes(raw)
        summary['queries'].append({'label':label,'rows':len(rows),'error_code':rs.error_code,'sha256':hashlib.sha256(raw).hexdigest()})
        print(label,len(rows),rs.error_code,flush=True)
        if rs.error_code!='0': break
finally:
    bs.logout()
    (out/'summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding='utf-8')
