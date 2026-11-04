from pathlib import Path
import sys,json,datetime
root=Path.cwd();sys.path.insert(0,str(root))
from scripts import top100_delivery as d
from scripts.exchange_calendar import latest_completed_session
print('Independent completed session:',latest_completed_session(),flush=True)
for command in ('refresh-all','check'):
 rc=d.main([command])
 if rc:raise SystemExit(rc)
p=root/'research_reports/20261102_deferred_repairs/native_primary_delivery_receipt.json'
r=json.loads((p.parent/'native_primary_restore_receipt.json').read_bytes());r.update(local_delivery_pass=True,expected_date=json.loads((root/d.MANIFEST).read_bytes())['expected_date'],completed_at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
p.write_text(json.dumps(r,ensure_ascii=False,indent=2),encoding='utf-8')
print('PRIMARY_NATIVE_DELIVERY_PASS',flush=True)
