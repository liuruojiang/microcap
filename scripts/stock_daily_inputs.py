"""Bounded raw-price/trade-state continuation. No inferred suspension or rewrite.

Requires an installed, approved lineage. An attempt prepares both files before
publication; interrupted publication is detectably inconsistent and fails closed.
"""
from __future__ import annotations

import csv
import datetime as dt
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import socket
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd

from scripts.exchange_calendar import sessions_for_day
from scripts.stock_return_basis_store import _extended
from scripts.tradeability_evidence import enrich_price_with_trade_status

FIELDS = 'date,code,close,preclose,volume,tradestatus,isST,adjustflag'
_PROVIDER_MUTEX = threading.Lock()


def _parallel_alternative(symbols, task):
    """Independent issuer files; join all active writes before releasing group lock."""
    executor = ThreadPoolExecutor(max_workers=4)
    futures = {executor.submit(task, symbol): symbol for symbol in symbols}
    results = {}
    try:
        for future in as_completed(futures):
            results[futures[future]] = future.result()
    except BaseException:
        for future in futures:
            future.cancel()
        raise
    finally:
        executor.shutdown(wait=True, cancel_futures=True)
    return [results[symbol] for symbol in symbols]


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _write(path, raw):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    tmp.write_bytes(raw)
    tmp.replace(path)


def _json_bytes(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False).encode('utf-8')


def _one(root, symbol, target, sessions, master, get_provider, allow_network, force_source_query):
    cache = root / '.microcap_index_cache'
    raw_path = cache / 'prices_raw' / f'{symbol}.csv'
    base = cache / 'trade_status'; sidecar_path = base / f'{symbol}.json'
    original = raw_path.read_bytes(); before_sidecar = sidecar_path.read_bytes()
    record = json.loads(before_sidecar)
    price = pd.read_csv(io.BytesIO(original), parse_dates=['date'])
    if set(price.columns) != {'date', 'close_raw'} or price.empty or price.date.duplicated().any() or not price.date.is_monotonic_increasing:
        raise ValueError('Daily raw input must be sorted unique two-column complete history')
    validated = enrich_price_with_trade_status(root=root, symbol=symbol, raw_path=raw_path,
                                  price=price, master=master, trading_dates=sessions,
                                  require_evidence=True)
    def unchanged():
        return (raw_path.read_bytes() == original and sidecar_path.read_bytes() == before_sidecar
                and _sha((cache / 'security_master.csv').read_bytes()) == master.attrs['source_sha256'])
    if not unchanged():
        raise ValueError('Daily installed inputs changed during validation')
    lifecycle = record['lifecycle']
    end = min(target, (pd.Timestamp(lifecycle['delist_date']) - pd.Timedelta(days=1)).strftime('%Y-%m-%d')) if lifecycle['delist_date'] else target
    if price.date.max() > pd.Timestamp(target):
        raise ValueError('Daily raw input extends beyond requested close')
    if price.date.max() > pd.Timestamp(end):
        raise ValueError('Daily raw input extends beyond verified issuer lifecycle')
    required_tail = {d.strftime('%Y-%m-%d') for d in sessions if price.date.max() <= d <= pd.Timestamp(end)}
    verified = set(validated.attrs.get('verified_trade_status_dates', []))
    if not force_source_query and required_tail <= verified and any(json.loads((base / query['path']).read_bytes())['request']['end'] >= end
           for query in record.get('provider_queries', [])):
        return {'symbol': symbol, 'status': 'existing_evidence_verified', 'target': target,
                'raw_sha256': _sha(original), 'sidecar_sha256': _sha(before_sidecar)}
    if not allow_network:
        raise ValueError('Daily trade status requires a real source refresh')
    start = price.date.iloc[max(0, len(price) - 10)].strftime('%Y-%m-%d')
    code = ('sh.' if symbol.startswith('6') else 'sz.') + symbol
    attempt_id = dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%S%fZ') + '_' + symbol
    source_attempt = base / 'daily' / attempt_id
    # Only referenced market evidence belongs to the portable sidecar tree.
    # Prepared pairs/backups/staging must not inflate each cloud state bundle.
    attempt = cache / 'daily_input_attempts' / attempt_id
    source_mode = os.environ.get('MICROCAP_DAILY_STOCK_PROVIDER', 'baostock')
    if source_mode == 'tencent':
        from scripts.stock_daily_alternative import query
        expected_days = [d.strftime('%Y-%m-%d') for d in sessions
                         if start <= d.strftime('%Y-%m-%d') <= end]
        prior = price.loc[price.date < pd.Timestamp(start), 'close_raw']
        raw = query(symbol, start, end, expected_days,
                    float(prior.iloc[-1] if not prior.empty else price.close_raw.iloc[0]))
        values = raw['rows']
    elif source_mode == 'baostock':
        result = get_provider().query_history_k_data_plus(code, FIELDS, start_date=start,
                                                   end_date=end, frequency='d', adjustflag='3')
        values = []
        while result.error_code == '0' and result.next():
            values.append(result.get_row_data())
        raw = {'request': {'code': code, 'fields': FIELDS, 'start': start, 'end': end,
                           'frequency': 'd', 'adjustflag': '3'}, 'error_code': result.error_code,
               'error_msg': result.error_msg, 'fields': result.fields, 'rows': values}
    else:
        raise ValueError('Unknown explicit daily stock provider')
    response = _json_bytes(raw)
    _write(source_attempt / 'response.json', response)
    _write(source_attempt / 'receipt.json', _json_bytes({'provider': raw.get('provider', 'baostock'), 'retrieved_at_utc': dt.datetime.now(dt.timezone.utc).isoformat(),
           'request': raw['request'], 'response_sha256': _sha(response), 'target': target,
           'old_raw_sha256': _sha(original), 'old_sidecar_sha256': _sha(before_sidecar)}))
    required_fields = FIELDS.split(',') if source_mode == 'baostock' else ['date', 'code', 'close', 'volume', 'tradestatus', 'adjustflag']
    if raw['error_code'] != '0' or not values or raw['fields'] != required_fields:
        raise ValueError('Daily provider query failed, empty, or wrong schema; group stopped')
    rows = [dict(zip(raw['fields'], r, strict=True)) for r in values]
    dates = [r['date'] for r in rows]
    if dates != sorted(set(dates)):
        raise ValueError('Daily provider dates not sorted and unique')
    expected = {d.strftime('%Y-%m-%d') for d in sessions if start <= d.strftime('%Y-%m-%d') <= end}
    if set(dates) != expected:
        raise ValueError('Daily provider omitted/added exchange sessions; suspension cannot be inferred')
    old = price.set_index('date').close_raw
    overlap = 0
    for row in rows:
        if row['code'] != code or row['adjustflag'] != '3' or row['tradestatus'] not in ('0', '1') or (source_mode == 'baostock' and row['isST'] not in ('0', '1')):
            raise ValueError('Daily provider row identity/status mismatch')
        close = float(row['close'])
        checked_prices = [close, float(row['preclose'])] if source_mode == 'baostock' else [close]
        if not np.isfinite(checked_prices).all() or min(checked_prices) <= 0:
            raise ValueError('Daily provider invalid close/preclose')
        volume = float(row['volume']) if row['volume'] else None
        if (row['tradestatus'] == '1' and (volume is None or not np.isfinite(volume) or volume <= 0)) or (row['tradestatus'] == '0' and volume not in (None, 0)):
            raise ValueError('Daily provider contradictory trading status/volume')
        day = pd.Timestamp(row['date'])
        if day in old.index:
            if abs(float(old.loc[day]) - close) > 1e-8:
                raise ValueError('Daily provider historical raw-price rewrite rejected')
            overlap += row['tradestatus'] == '1'
    if overlap < 5:
        raise ValueError('Daily provider fewer than five real overlap sessions')
    additions = [{'date': row['date'], 'close_raw': row['close']} for row in rows
                 if pd.Timestamp(row['date']) > price.date.max() and row['tradestatus'] == '1']
    if additions:
        original_rows = list(csv.DictReader(io.StringIO(original.decode('utf-8-sig'))))
        stream = io.StringIO(newline=''); writer = csv.DictWriter(stream, fieldnames=['date', 'close_raw'], lineterminator='\n')
        writer.writeheader(); writer.writerows(original_rows + additions); candidate = stream.getvalue().encode()
    else:
        candidate = original
    new_record = dict(record)
    new_record['raw_price_sha256'] = _sha(candidate)
    new_record['provider_queries'] = list(record.get('provider_queries', [])) + [
        {'path': f'daily/{attempt_id}/response.json', 'sha256': _sha(response)}]
    prepared_raw = attempt / 'candidate_raw.csv'
    prepared_sidecar = _json_bytes(new_record)
    _write(prepared_raw, candidate); _write(attempt / 'candidate_sidecar.json', prepared_sidecar)
    _write(attempt / 'before_raw.csv', original); _write(attempt / 'before_sidecar.json', before_sidecar)
    staging = attempt / 'validation'
    staging_base = staging / '.microcap_index_cache/trade_status'
    def copy_bound(value):
        if isinstance(value, dict):
            if 'path' in value and 'sha256' in value:
                source = (base / value['path']).resolve()
                if not source.is_relative_to(base.resolve()) or _sha(source.read_bytes()) != value['sha256']:
                    raise ValueError('Daily prepared evidence binding failed')
                destination = staging_base / value['path']; destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, destination)
            else:
                for nested in value.values(): copy_bound(nested)
        elif isinstance(value, list):
            for nested in value: copy_bound(nested)
    copy_bound(new_record)
    _write(staging_base / f'{symbol}.json', prepared_sidecar)
    enrich_price_with_trade_status(root=staging, symbol=symbol, raw_path=prepared_raw,
                    price=pd.read_csv(io.BytesIO(candidate), parse_dates=['date']), master=master,
                    trading_dates=sessions, require_evidence=True)
    # Both exact files have passed before publication. A process crash between
    # replaces fails the raw/sidecar SHA gate; ordinary exceptions roll back.
    if not unchanged():
        raise ValueError('Daily inputs changed during source query; no publication')
    try:
        _write(raw_path, candidate); _write(sidecar_path, prepared_sidecar)
        enriched = enrich_price_with_trade_status(root=root, symbol=symbol, raw_path=raw_path,
                         price=pd.read_csv(io.BytesIO(candidate), parse_dates=['date']), master=master,
                         trading_dates=sessions, require_evidence=True)
    except BaseException:
        _write(raw_path, original); _write(sidecar_path, before_sidecar)
        raise
    summary = {'symbol': symbol, 'status': 'daily_pair_verified', 'target': target,
               'evidence_through': end, 'raw_sha256': _sha(candidate), 'sidecar_sha256': _sha(prepared_sidecar),
               'added_trade_rows': len(additions), 'overlap_trade_days': overlap,
               'suspended_valuation_rows': int(enriched.tradestatus.eq(0).sum()), 'attempt': str(attempt)}
    _write(attempt / 'result.json', _json_bytes(summary))
    return summary


def refresh_daily_inputs(*, root, symbols, as_of, allow_network=True, force_source_query=False):
    """Update a bounded symbol group; first data/provider failure stops the group."""
    root = _extended(root); target = pd.Timestamp(as_of).strftime('%Y-%m-%d')
    close = dt.datetime.fromisoformat(target + 'T15:00:00+08:00')
    if close > dt.datetime.now(dt.timezone.utc):
        raise ValueError('Daily close is not yet eligible')
    symbols = list(symbols)
    if not symbols or len(symbols) > 6000 or len(set(symbols)) != len(symbols) or any(not isinstance(s, str) or len(s) != 6 or not s.isascii() or not s.isdigit() for s in symbols):
        raise ValueError('Daily refresh requires bounded unique explicit six-digit symbols')
    required = [root / '.microcap_index_cache/security_master.csv']
    for symbol in symbols:
        required.extend([root / '.microcap_index_cache/prices_raw' / f'{symbol}.csv',
                         root / '.microcap_index_cache/trade_status' / f'{symbol}.json'])
    missing = [str(path.relative_to(root)) for path in required if not path.is_file()]
    if missing:
        raise ValueError('Daily installed input seed is incomplete; audited migration required: ' + ', '.join(missing[:8]))
    sessions = pd.DatetimeIndex(sessions_for_day(pd.Timestamp(target).date()))
    sessions = sessions[sessions <= pd.Timestamp(target)]
    if pd.Timestamp(target) not in sessions:
        raise ValueError('Daily target is not an exchange session')
    master_raw = (root / '.microcap_index_cache/security_master.csv').read_bytes()
    master = pd.read_csv(io.BytesIO(master_raw), dtype={'symbol': str})
    master.attrs['source_sha256'] = _sha(master_raw)
    with _PROVIDER_MUTEX:
        lock = root / '.microcap_index_cache/trade_status/DAILY_PROVIDER.lock'
        fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, str(os.getpid()).encode()); os.close(fd)
        provider = None
        previous_timeout = socket.getdefaulttimeout()
        def get_provider():
            nonlocal provider
            if provider is None:
                import baostock as loaded
                provider = loaded
                socket.setdefaulttimeout(15)
                login = provider.login()
                if login.error_code != '0':
                    raise ValueError('Daily BaoStock login failed; no retry')
            return provider
        try:
            if os.environ.get('MICROCAP_DAILY_STOCK_PROVIDER', 'baostock') == 'tencent':
                return _parallel_alternative(symbols, lambda sym: _one(
                    root, sym, target, sessions, master, get_provider, allow_network, force_source_query))
            return [_one(root, sym, target, sessions, master, get_provider, allow_network, force_source_query) for sym in symbols]
        finally:
            try:
                if provider is not None:
                    provider.logout()
            finally:
                socket.setdefaulttimeout(previous_timeout)
                lock.unlink()
