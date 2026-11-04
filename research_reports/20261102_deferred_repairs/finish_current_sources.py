"""Resume exact input pairs, then run original delivery gates and package once."""
from pathlib import Path
from datetime import datetime, timezone
import os
import runpy
import sys

root = Path.cwd()
sys.path.insert(0, str(root))
os.environ['MICROCAP_DAILY_STOCK_PROVIDER'] = 'tencent'
from scripts.stock_daily_inputs import refresh_daily_inputs
from scripts.realtime_state_bundle import _transport_universe_symbols

print('SOURCE_REFRESH_START', datetime.now(timezone.utc).isoformat(), flush=True)
symbols = _transport_universe_symbols(root)
results = refresh_daily_inputs(root=root, symbols=symbols, as_of='2026-11-02')
print('SOURCE_PAIRS_PASS', len(results), datetime.now(timezone.utc).isoformat(), flush=True)
runpy.run_path('research_reports/20261102_deferred_repairs/finish_current_primary.py', run_name='__main__')
print('PACK_START', datetime.now(timezone.utc).isoformat(), flush=True)
runpy.run_path('research_reports/20261102_deferred_repairs/finish_current_pack.py', run_name='__main__')
print('PACK_COMPLETE', datetime.now(timezone.utc).isoformat(), flush=True)
