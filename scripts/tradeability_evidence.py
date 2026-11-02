"""Bind explicit trade-status observations to immutable price and issuer evidence.

No missing-session inference and no mutation of the raw-price cache. Sidecars are
lineage inputs, not approval records; installing them requires the normal migration.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd


def _read_bound(base: Path, item: dict) -> bytes:
    path = (base / item['path']).resolve()
    if not path.is_relative_to(base.resolve()):
        raise ValueError('trade status evidence path escapes its bundle')
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != item['sha256']:
        raise ValueError('trade status evidence hash mismatch')
    return raw


def reviewed_provider_status(base: Path, reviews: list[dict], symbol: str) -> dict[str, dict]:
    """Validate explicit row corrections against a separately bound live Wind call."""
    out = {}
    for review in reviews:
        if review.get('symbol') != symbol or review.get('original_status') not in (0, 1):
            raise ValueError('provider status review issuer/original status mismatch')
        dt = review['date']; corrected = review['corrected_status']
        if corrected not in (0, 1) or corrected == review['original_status'] or dt in out:
            raise ValueError('invalid or duplicate status correction')
        receipt = json.loads(_read_bound(base, review['wind_receipt']))
        response_raw = _read_bound(base, review['wind_response'])
        response_text = response_raw.decode('utf-8')
        if response_text.lstrip().startswith('event:'):
            messages = [line[5:].strip() for line in response_text.splitlines() if line.startswith('data:')]
            if len(messages) != 1:
                raise ValueError('independent Wind SSE response must have one data event')
            response = json.loads(messages[0])
        else:
            response = json.loads(response_text)
        args = receipt['arguments']
        expected = symbol + ('.SH' if symbol.startswith('6') else '.SZ')
        if (receipt.get('provider') != 'wind' or receipt.get('status') != 200 or
                receipt.get('tool') != 'get_stock_kline' or args.get('windcode') != expected or
                args.get('aftype') != '2' or args.get('period') != '10' or
                not args['begin_date'] <= dt.replace('-', '') <= args['end_date'] or
                receipt['sha256'] != hashlib.sha256(response_raw).hexdigest() or
                response.get('id') != receipt['request_id'] or response.get('result', {}).get('isError')):
            raise ValueError('independent status-review Wind request/response binding failed')
        payloads = [json.loads(item['text']) for item in response['result']['content'] if item.get('type') == 'text']
        if len(payloads) != 1 or payloads[0].get('error') is not None:
            raise ValueError('independent status-review Wind payload invalid')
        data = payloads[0]['data']; fields = [c['name'] for c in data['columns']]
        matches = [dict(zip(fields, values, strict=True)) for values in data['rows'] if str(values[fields.index('TIME')])[:10] == dt]
        if len(matches) != 1:
            raise ValueError('independent status review requires one exact date')
        row = matches[0]
        independent_close = float(row['MATCH'])
        if not np.isfinite(independent_close) or independent_close <= 0:
            raise ValueError('independent source has invalid close')
        if corrected == 1:
            if row.get('VOLUME') is None or not np.isfinite(float(row['VOLUME'])) or float(row['VOLUME']) <= 0:
                raise ValueError('independent source does not prove actual trading')
        else:
            if row.get('VOLUME') not in (None, '', '0', 0):
                raise ValueError('independent source conflicts with suspended correction')
            _read_bound(base, review['document'])
            issuer = json.loads(_read_bound(base, review['issuer_review']))
            if (issuer.get('symbol') != symbol or issuer.get('date') != dt or
                    issuer.get('status') != 'official_suspension' or
                    issuer.get('document_sha256') != review['document']['sha256'] or
                    not issuer.get('rationale') or not issuer.get('source_url')):
                raise ValueError('nontrading correction lacks bound issuer review')
        out[dt] = dict(review, independently_observed_close=independent_close,
                       independently_observed_volume=None if row.get('VOLUME') in (None, '') else float(row['VOLUME']))
    return out


def enrich_price_with_trade_status(*, root: Path, symbol: str, raw_path: Path,
                                  price: pd.DataFrame, master: pd.DataFrame,
                                  trading_dates: pd.DatetimeIndex,
                                  require_evidence: bool = False) -> pd.DataFrame:
    """Add only evidenced suspended valuations; keep legacy behavior if absent."""
    base = Path(root) / '.microcap_index_cache' / 'trade_status'
    path = base / f'{symbol}.json'
    if 'tradestatus' in price.columns:
        raise ValueError('raw-cache trading flags require a bound sidecar, not inline assertions')
    if not path.exists():
        if require_evidence:
            raise ValueError('verified return-basis pipeline requires trade status evidence')
        return price
    record = json.loads(path.read_bytes())
    if record.get('schema_version') != 1 or record.get('symbol') != symbol:
        raise ValueError('trade status sidecar identity/schema mismatch')
    if hashlib.sha256(Path(raw_path).read_bytes()).hexdigest() != record['raw_price_sha256']:
        raise ValueError('trade status sidecar belongs to different raw prices')
    matches = master.loc[master['symbol'].astype(str).str.zfill(6) == symbol]
    if len(matches) != 1:
        raise ValueError('trade status requires unique security master identity')
    lifecycle = matches.iloc[0]
    def day(value):
        parsed = pd.to_datetime(value, errors='coerce')
        return '' if pd.isna(parsed) else parsed.strftime('%Y-%m-%d')
    for field in ['list_date', 'delist_date']:
        if day(lifecycle.get(field)) != record['lifecycle'][field]:
            raise ValueError('trade status lifecycle mismatch')
    if not record['lifecycle']['list_date']:
        raise ValueError('trade status requires a verified listing date')
    if record['lifecycle']['delist_date'] and record['lifecycle']['delist_date'] <= record['lifecycle']['list_date']:
        raise ValueError('trade status invalid lifecycle interval')
    observed: dict[str, tuple[int, float | None]] = {}
    corrections = reviewed_provider_status(base, record.get('provider_status_reviews', []), symbol)
    used_corrections = set()
    for source in record.get('provider_queries', []):
        payload = json.loads(_read_bound(base, source))
        if 'provider' in payload or 'native_evidence' in payload:
            from scripts.stock_daily_alternative import verify
            verify(payload)
        request = payload['request']
        code = ('sh.' if symbol.startswith('6') else 'sz.') + symbol
        if request.get('code') != code or request.get('adjustflag') != '3' or request.get('frequency') != 'd' or payload['error_code'] != '0':
            raise ValueError('trade status provider query identity failed')
        fields = payload['fields']
        required = {'date', 'code', 'close', 'volume', 'tradestatus', 'adjustflag'}
        if not required <= set(fields) or len(set(fields)) != len(fields):
            raise ValueError('trade status provider fields malformed')
        seen = set()
        for values in payload['rows']:
            row = dict(zip(fields, values, strict=True)); dt = row['date']
            if dt in seen or not request['start'] <= dt <= request['end']:
                raise ValueError('trade status duplicate/out-of-query observation')
            seen.add(dt)
            if row['code'] != code or row['adjustflag'] != '3' or row['tradestatus'] not in ('0', '1'):
                raise ValueError('trade status observation identity failed')
            status = int(row['tradestatus']); close = float(row['close'])
            if dt in corrections:
                review = corrections[dt]
                if (review['provider_raw_sha256'] != source['sha256'] or review['original_status'] != status
                        or abs(review['independently_observed_close'] - close) > 1e-8
                        or (review['corrected_status'] == 1 and
                            abs(float(row['volume']) - review['independently_observed_volume']) > 1e-8)):
                    raise ValueError('reviewed correction does not bind original provider row')
                status = review['corrected_status']; used_corrections.add(dt)
            if not np.isfinite(close) or close <= 0:
                raise ValueError('trade status nonpositive/nonfinite valuation')
            volume = float(row['volume']) if row['volume'] else None
            if (status == 1 and (volume is None or not np.isfinite(volume) or volume <= 0)) or (status == 0 and volume not in (None, 0)):
                raise ValueError('trade status inconsistent traded volume')
            if dt in observed and observed[dt] != (status, close):
                raise ValueError('trade status conflicting provider observations')
            observed[dt] = (status, close)
        days = trading_dates.strftime('%Y-%m-%d')
        in_scope = ((days >= request['start']) & (days <= request['end']) &
                    (days >= record['lifecycle']['list_date']))
        if record['lifecycle']['delist_date']:
            in_scope &= days < record['lifecycle']['delist_date']
        expected = set(days[in_scope])
        if expected - seen:
            raise ValueError('trade status provider query omitted active exchange sessions')
    if used_corrections != set(corrections):
        raise ValueError('unused provider status correction')
    for notice in record.get('reviewed_suspensions', []):
        _read_bound(base, notice['document'])
        review = json.loads(_read_bound(base, notice['review']))
        dt = notice['date']
        if (review.get('symbol') != symbol or review.get('date') != dt or
                review.get('status') != 'official_suspension' or
                review.get('document_sha256') != notice['document']['sha256'] or
                not review.get('source_url') or not review.get('rationale')):
            raise ValueError('suspension review does not bind issuer/date/document')
        if dt in observed and observed[dt][0] != 0:
            raise ValueError('issuer suspension conflicts with provider trade')
        observed[dt] = (0, observed.get(dt, (0, None))[1])
    if not observed:
        raise ValueError('trade status sidecar has no observations')
    result = price.copy().set_index('date').sort_index()
    if result.index.has_duplicates:
        raise ValueError('duplicate input price dates')
    result['tradestatus'] = 1
    sessions = set(pd.DatetimeIndex(trading_dates))
    verified_dates = []
    for ds, (status, close) in sorted(observed.items()):
        dt = pd.Timestamp(ds)
        if dt < trading_dates.min() or dt > trading_dates.max():
            continue
        if dt not in sessions:
            raise ValueError('trade status observation is not an exchange session')
        if ds < record['lifecycle']['list_date'] or (record['lifecycle']['delist_date'] and ds >= record['lifecycle']['delist_date']):
            raise ValueError('trade status observation outside issuer lifecycle')
        verified_dates.append(ds)
        if dt in result.index:
            if close is not None and abs(float(result.at[dt, 'close_raw']) - close) > 1e-8:
                raise ValueError('trade status source disagrees with raw close')
        elif status == 1:
            raise ValueError('evidenced trade absent from raw price candidate')
        else:
            prior = result.loc[result.index < dt, 'close_raw']
            if prior.empty:
                raise ValueError('suspension has no previous observed valuation')
            if close is not None and abs(float(prior.iloc[-1]) - close) > 1e-8:
                raise ValueError('suspended valuation differs from previous raw close')
            result.loc[dt, 'close_raw'] = float(prior.iloc[-1])
            result = result.sort_index()
        result.loc[dt, 'tradestatus'] = status
    output = result.rename_axis('date').reset_index()
    output.attrs['verified_trade_status_dates'] = sorted(set(verified_dates))
    return output
