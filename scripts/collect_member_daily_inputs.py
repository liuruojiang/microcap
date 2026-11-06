"""Collect only the fixed basket and index; never refresh global metadata."""
from __future__ import annotations
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
import hashlib
import json
import math
from pathlib import Path
import sys
import threading
from zoneinfo import ZoneInfo

CN = ZoneInfo('Asia/Shanghai')

def read_bound(root, spec):
    p = (root / spec['path']).resolve()
    if not p.is_relative_to(root.resolve()) or hashlib.sha256(p.read_bytes()).hexdigest() != spec['sha256']:
        raise ValueError('Input reference escaped root or changed')
    return json.loads(p.read_bytes())

def adjusted_pair(symbol, previous_date, day, previous_raw, current_raw, bodies, receipts):
    from scripts.stock_return_basis import _factor, RETURN_BASIS_REVISION
    values = {}
    for kind in ('qfq', 'hfq'):
        frame = _factor(symbol, kind, bodies[kind], receipts[kind])
        selected = []
        for date in (previous_date, day):
            eligible = frame.loc[frame.date.le(date), kind]
            if eligible.empty:
                raise ValueError('Missing adjustment coverage at ' + date)
            selected.append(float(eligible.iloc[-1]))
        values[kind] = selected
    q, h = values['qfq'], values['hfq']
    backward = (previous_raw * h[0], current_raw * h[1])
    forward = (previous_raw / q[0], current_raw / q[1])
    if not all(math.isfinite(v) and v > 0 for v in (*backward, *forward)):
        raise ValueError('Invalid adjusted member prices')
    if abs(backward[1] / backward[0] - forward[1] / forward[0]) > 1e-9:
        raise ValueError('qfq/hfq return basis disagreement')
    return dict(previous_close_adj=backward[0], close_adj=backward[1],
                return_basis_revision=RETURN_BASIS_REVISION)

def collect_hedge(base, day, mode, out):
    try:
        if mode == 'close_confirmed':
            from scripts.index_history_preflight import fetch_delivery_history
            frame = fetch_delivery_history(date.fromisoformat(day), date.fromisoformat(day))
            frame.to_csv(Path(out)/'hedge_daily_bar.csv',index=False)
            return dict(market_date=day, close=float(frame.close.iloc[-1]),
                        source=frame.attrs['independent_history_source'],collected_at=datetime.now(CN).isoformat(),
                        close_confirmed=True,source_attempts=frame.attrs['independent_history_attempts'])
        if datetime.now(CN).hour >= 15:
            raise ValueError('Intraday hedge collection cutoff reached')
        raw = base.fetch_hedge_realtime_quote_fast()
        value, source, quote_day = base.normalize_hedge_realtime_quote_result(raw)
        if quote_day != day:
            raise ValueError('Wrong-date hedge quote')
        return dict(market_date=day,close=float(value),source=source,collected_at=datetime.now(CN).isoformat(),close_confirmed=False)
    except Exception as exc:
        return dict(market_date=day,error=f'{type(exc).__name__}: {exc}')

def collect(*, root, checkpoint, day, mode, out, workers=8):
    import requests
    root = Path(root).resolve()
    sys.path.insert(0, str(root))
    import microcap_top100_mom16_biweekly_live_v2_0 as v20
    base = v20.realtime_core.base_mod
    cp_path = Path(checkpoint).resolve()
    cp = json.loads(cp_path.read_bytes())
    period = read_bound(cp_path.parent, cp['period'])
    members = period['effective_members']
    price_basis = read_bound(cp_path.parent, cp['prices'])
    price_rows = {str(r['symbol']).zfill(6):r for r in price_basis['rows']}
    symbols = [str(m['symbol']).zfill(6) for m in members]
    if len(symbols) != 100 or len(set(symbols)) != 100:
        raise ValueError('Fixed basket must contain 100 unique members')
    now = datetime.now(CN)
    if day != now.date().isoformat() or (mode == 'close_confirmed' and now.hour < 15):
        raise ValueError('Collection requires actual current session and eligible clock')
    if mode == 'intraday' and now.hour >= 15:
        raise ValueError('Intraday collection cutoff reached')
    from scripts.exchange_calendar import sessions_for_day
    sessions = {x.isoformat() for x in sessions_for_day(date.fromisoformat(day))}
    if day not in sessions:
        raise ValueError('Market date is not a verified session')
    later = sorted(d for d in sessions if cp['as_of_date'] < d <= day)
    if later != [day]:
        raise ValueError('Checkpoint requires missing-session replay before current collection')
    out = Path(out).resolve()
    out.mkdir(parents=True, exist_ok=False)
    import pandas as pd
    raw = base.fetch_member_realtime_quotes(symbols) if mode == 'intraday' else pd.DataFrame()
    raw.to_csv(out / 'raw_quotes.csv', index=False)
    by_symbol = {str(r['code']).zfill(6): r for r in raw.to_dict('records')}
    errors = {}
    source_stopped = threading.Event()
    def one(member):
        symbol = str(member['symbol']).zfill(6)
        if mode == 'intraday' and datetime.now(CN).hour >= 15:
            raise ValueError('Intraday source completion cutoff reached')
        basis = price_rows[symbol]
        previous_date = str(basis['previous_raw_date'])
        if previous_date != cp['as_of_date']:
            raise ValueError('Member price basis differs from checkpoint')
        folder = out / 'factor_sources' / symbol
        folder.mkdir(parents=True)
        if mode == 'close_confirmed':
            from scripts.stock_daily_alternative import query, verify
            daily = query(symbol,day,day,[day],float(basis['previous_raw_close']))
            verify(daily)
            (folder/'daily_raw.json').write_text(json.dumps(daily,ensure_ascii=False),encoding='utf8')
            row = dict(rt_price=float(daily['rows'][0][2]),trade_date=day,
                       quote_time=day+'T15:00:00+08:00',source=daily['provider'])
        else:
            row = by_symbol.get(symbol)
            if row is None or str(row.get('trade_date')) != day:
                raise ValueError('Missing current-date member quote')
        bodies, receipts = {}, {}
        market = ('sh' if symbol.startswith('6') else 'sz') + symbol
        for kind in ('qfq', 'hfq'):
            if source_stopped.is_set():
                raise ValueError('Factor source stopped after HTTP403/429')
            if mode == 'intraday' and datetime.now(CN).hour >= 15:
                raise ValueError('Intraday request cutoff reached')
            url = f'https://finance.sina.com.cn/realstock/company/{market}/{kind}.js'
            start = datetime.now(CN).isoformat()
            response = requests.get(url, timeout=15)
            body = response.content
            receipt = dict(url=url, status=response.status_code, sha256=hashlib.sha256(body).hexdigest(),
                           observation_start=start, observation_end=datetime.now(CN).isoformat())
            (folder / (kind + '.js')).write_bytes(body)
            (folder / (kind + '.json')).write_text(json.dumps(receipt), encoding='utf8')
            if response.status_code in (403, 429):
                source_stopped.set()
            response.raise_for_status()
            bodies[kind], receipts[kind] = body, receipt
        pair = adjusted_pair(symbol, previous_date, day, float(basis['previous_raw_close']),
                             float(row['rt_price']), bodies, receipts)
        completed = datetime.now(CN)
        if mode == 'intraday' and completed.hour >= 15:
            raise ValueError('Intraday source completion cutoff reached')
        return dict(symbol=symbol, trade_date=day, previous_date=previous_date,
                    raw_close=float(row['rt_price']), previous_raw_close=float(basis['previous_raw_close']),
                    source=str(row.get('source') or raw.attrs.get('quote_source', 'native_member_quote')) + '+sina_ratio',
                    quote_time=str(row.get('quote_time', '')), collected_at=completed.isoformat(), **pair)
    rows = []
    with ThreadPoolExecutor(max_workers=max(1, min(workers, 8))) as pool:
        futures = {pool.submit(one, m): str(m['symbol']) for m in members}
        for future in as_completed(futures):
            try:
                rows.append(future.result())
            except Exception as exc:
                errors[futures[future]] = f'{type(exc).__name__}: {exc}'
    quotes = dict(market_date=day, collected_at=datetime.now(CN).isoformat(), rows=sorted(rows,key=lambda r:r['symbol']), errors=errors,
                  close_confirmed=mode=='close_confirmed')
    (out/'quotes.json').write_text(json.dumps(quotes,ensure_ascii=False,indent=2,allow_nan=False),encoding='utf8')
    hedge = collect_hedge(base,day,mode,out)
    (out/'hedge.json').write_text(json.dumps(hedge,indent=2,allow_nan=False),encoding='utf8')
    return quotes

def main():
    p=argparse.ArgumentParser(description=__doc__)
    for key in ('root','checkpoint','date','mode','out'):
        p.add_argument('--'+key,required=True)
    args=p.parse_args()
    report=collect(root=args.root,checkpoint=args.checkpoint,day=args.date,mode=args.mode,out=args.out)
    print(json.dumps({'member_rows':len(report['rows']),'errors':report['errors']}))

if __name__=='__main__':
    main()
