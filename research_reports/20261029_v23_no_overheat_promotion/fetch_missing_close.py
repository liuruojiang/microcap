"""Fetch real secondary-source tail bars into staging; never writes formal caches."""
from pathlib import Path
import sys, json, hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
import microcap_top100_mom16_biweekly_live_v2_0 as v

v._sync_embedded_base_config()
b = v.base_mod
f = b.freq_mod.fetch_mod.fetch_price_history.__globals__
target = pd.Timestamp('2026-10-29')
symbols = b.price_cache_refresh_preflight(target, b.freq_mod.load_universe(), False)['stale_or_missing_symbols']
stage = Path(__file__).with_name('secondary_close_staging')
stage.mkdir(exist_ok=True)

def fetch(code):
    path = Path(f['PRICE_CACHE_DIR']) / f'{code}.csv'
    before = path.read_bytes()
    old = pd.read_csv(path, parse_dates=['date'])
    last = old.date.max()
    data = f['_fetch_price_history_eastmoney'](code, last, target)
    assert data.date.notna().all() and not data.date.duplicated().any()
    assert (data.date <= target).all() and (data.close_raw > 0).all()
    tail = data.loc[data.date > last].copy()
    tail.to_csv(stage / f'{code}.csv', index=False)
    return dict(symbol=code, cache=str(path), before_sha256=hashlib.sha256(before).hexdigest(),
                previous_date=str(last.date()), new_rows=len(tail),
                latest_source_date=str(data.date.max().date()) if len(data) else None,
                staged_sha256=hashlib.sha256((stage/f'{code}.csv').read_bytes()).hexdigest())

rows=[]; errors=[]
with ThreadPoolExecutor(max_workers=8) as pool:
    futures={pool.submit(fetch, str(code).zfill(6)):str(code).zfill(6) for code in symbols}
    for future in as_completed(futures):
        try: rows.append(future.result())
        except Exception as exc: errors.append(dict(symbol=futures[future],error=str(exc)))
        if (len(rows)+len(errors))%50==0: print(len(rows),len(errors),flush=True)
report=dict(source='existing _fetch_price_history_eastmoney',target=str(target.date()),
            rows=rows,errors=errors,formal_cache_written=False)
(stage/'manifest.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
print(json.dumps(dict(success=len(rows),errors=len(errors),current=sum(x['latest_source_date']=='2026-10-29' for x in rows))))
