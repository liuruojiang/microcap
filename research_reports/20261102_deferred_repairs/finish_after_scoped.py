from pathlib import Path
from datetime import datetime, timezone
import ctypes,os,subprocess,sys
root=Path.cwd();e=root/'research_reports/20261102_deferred_repairs'
k=ctypes.windll.kernel32;k.OpenProcess.restype=ctypes.c_void_p;k.WaitForSingleObject.argtypes=[ctypes.c_void_p,ctypes.c_ulong];k.CloseHandle.argtypes=[ctypes.c_void_p]
h=k.OpenProcess(0x00100000,False,17320)
print('WAIT_SCOPED_REPLAY',datetime.now(timezone.utc).isoformat(),flush=True)
if h:
 while k.WaitForSingleObject(h,1000)==258:pass
 k.CloseHandle(h)
log=(e/'scoped-replay-native-2.log').read_text(encoding='utf-8-sig')
if 'SCOPED_REPLAY_PASS' not in log:raise SystemExit('Scoped replay did not pass; no delivery or pack started')
os.environ['MICROCAP_DAILY_STOCK_PROVIDER']='tencent';os.environ['PYTHONDONTWRITEBYTECODE']='1'
for name in ('finish_current_primary.py','finish_current_pack.py'):
 print('STAGE_START',name,datetime.now(timezone.utc).isoformat(),flush=True)
 rc=subprocess.run([sys.executable,'-B','-X','utf8',str(e/name)],cwd=root).returncode
 print('STAGE_END',name,rc,datetime.now(timezone.utc).isoformat(),flush=True)
 if rc:raise SystemExit(rc)
print('LOCAL_DELIVERY_AND_PACK_PASS',flush=True)
