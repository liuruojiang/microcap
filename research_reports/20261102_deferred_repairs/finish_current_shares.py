"""Finish only the blocked source stage, then use the original delivery gates."""
from pathlib import Path
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import runpy
import sys

root = Path.cwd()
sys.path.insert(0, str(root))
os.environ['MICROCAP_DAILY_STOCK_PROVIDER'] = 'tencent'
from scripts import top100_delivery as delivery
from scripts.share_change_source import refresh_share_change
from scripts.realtime_state_bundle import _transport_universe_symbols

symbols = _transport_universe_symbols(root)
lock = root / delivery.LOCK
with os.fdopen(os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY), 'w') as stream:
    stream.write(str(os.getpid()))
print('SHARE_SOURCE_START', datetime.now(timezone.utc).isoformat(), flush=True)
try:
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {pool.submit(refresh_share_change, root=root, symbol=symbol,
                   cache_path=root / '.microcap_index_cache/share_change' / (symbol + '.csv'),
                   as_of='2026-11-02', allow_network=True): symbol for symbol in symbols}
        try:
            for i, future in enumerate(as_completed(futures), 1):
                try:
                    future.result()
                except Exception as exc:
                    print('SHARE_FAILED', futures[future], type(exc).__name__, str(exc), flush=True)
                    raise
                if i % 100 == 0:
                    print('SHARE_PROGRESS', i, flush=True)
        except BaseException:
            for future in futures:
                future.cancel()
            raise
finally:
    lock.unlink()
print('SHARE_SOURCE_PASS', len(symbols), datetime.now(timezone.utc).isoformat(), flush=True)
runpy.run_path('research_reports/20261102_deferred_repairs/finish_current_primary.py', run_name='__main__')
print('PACK_START', datetime.now(timezone.utc).isoformat(), flush=True)
runpy.run_path('research_reports/20261102_deferred_repairs/finish_current_pack.py', run_name='__main__')
print('PACK_COMPLETE', datetime.now(timezone.utc).isoformat(), flush=True)
