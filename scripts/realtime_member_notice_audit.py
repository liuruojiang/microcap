"""Read-only, source-bound intraday notice audit for the actual member union.

This never advances a production archive or mutates metadata/NAV. Existing
completed-session collectors keep their >=15:00 contract. These observations
describe only pages returned during this run, never future publication coverage.
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path, PurePosixPath
import hashlib
import json
import math
import re
import uuid
from zoneinfo import ZoneInfo

import requests

from scripts.st_notice_incremental import API, _params, _provider_time
from scripts.st_notice_evidence import StNoticeEvidenceError, _registry, is_equity_issuer, load_verified_events
from scripts.st_notice_archive import _nonrisk_investor_record
from scripts.st_history_scope import _daily_notice_row

PREFIX = '.microcap_index_cache/realtime_notice_audits'
POLICY = 'member_notice_intraday_readonly_20261103_v1'
SCHEMA = 'eastmoney_member_notice_intraday_v1'
MAX_PAGES = 100
CN = ZoneInfo('Asia/Shanghai')


def _now():
    return datetime.now(timezone.utc)


def _fail(message):
    raise StNoticeEvidenceError('Intraday member notice audit: ' + message)


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _json(raw):
    return json.loads(raw.decode('utf-8-sig'))


def _write(path, raw):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('xb') as stream:
        stream.write(raw)


def _dump(path, value):
    _write(path, (json.dumps(value, ensure_ascii=False, indent=2) + '\n').encode('utf8'))


def _bound(root, relative):
    if (not isinstance(relative, str) or not relative or '\\' in relative or ':' in relative
            or PurePosixPath(relative).is_absolute() or any(p in ('', '.', '..') for p in relative.split('/'))):
        _fail('unsafe evidence path')
    root = Path(root).resolve()
    path = (root / relative).resolve()
    if not path.is_relative_to(root):
        _fail('evidence path escapes root')
    return path


def _stamp(value):
    try:
        result = datetime.fromisoformat(value)
    except (ValueError, TypeError):
        _fail('invalid observation clock')
    if result.tzinfo is None:
        _fail('observation clock has no timezone')
    return result


def _days(signal_day, previous_session, before_request=None):
    signal, previous = date.fromisoformat(str(signal_day)), date.fromisoformat(str(previous_session))
    from scripts.exchange_calendar import sessions_for_day
    sessions = set(sessions_for_day(signal, before_request=before_request)) if before_request is not None else set(sessions_for_day(signal))
    earlier = [day for day in sessions if day < signal]
    if signal not in sessions or not earlier or max(earlier) != previous:
        _fail('signal/previous session differs from independent calendar')
    return signal, previous


def _symbols(values):
    values = list(values)
    if (not 100 <= len(values) <= 200 or len(values) != len(set(values))
            or any(not isinstance(v, str) or not re.fullmatch(r'[036]\d{5}', v) for v in values)):
        _fail('member union must contain 100..200 unique equity codes')
    return sorted(values)


def _guard(signal, before_request):
    now = _now()
    if now.astimezone(CN).date() != signal or now >= datetime.combine(signal, time(15), CN):
        _fail('current intraday observation/publication must be before 15:00 on signal day')
    if before_request is not None:
        before_request()
        now = _now()
        if now.astimezone(CN).date() != signal or now >= datetime.combine(signal, time(15), CN):
            _fail('intraday callback completed outside publication window')
    return now


def _calendar_guard(signal, original):
    def guarded():
        return _guard(signal, original)
    deadline = datetime.combine(signal, time(15), CN)
    supplied = getattr(original, 'intraday_deadline', None)
    if supplied is not None:
        if not isinstance(supplied, datetime) or supplied.tzinfo is None:
            _fail('calendar deadline is not an aware timestamp')
        deadline = min(deadline, supplied)
    guarded.intraday_deadline = deadline
    return guarded


def _integer(value):
    if isinstance(value, bool):
        _fail('boolean pagination value')
    if isinstance(value, int) or (isinstance(value, float) and math.isfinite(value) and value.is_integer()):
        return int(value)
    if isinstance(value, str) and re.fullmatch(r'[0-9]+', value):
        return int(value)
    _fail('pagination value is not an exact integer')


def strict_native_pagination(root, manifest_spec, day):
    """Supplement only an explicitly declared, bound native closed-day manifest.

    The original full closed-day verification still runs. This does not inspect
    arbitrary JSON, CNINFO responses, or historical issuer archive schemas.
    """
    from scripts.st_notice_incremental import verify_day, SCHEMA as DAILY_SCHEMA
    if (not isinstance(manifest_spec, dict) or not isinstance(manifest_spec.get('sha256'), str)
            or re.fullmatch(r'[0-9a-f]{64}', manifest_spec['sha256']) is None):
        _fail('native day manifest binding missing')
    path = _bound(root, manifest_spec['path'])
    raw = path.read_bytes()
    if _sha(raw) != manifest_spec['sha256']:
        _fail('native day manifest bytes changed')
    manifest = _json(raw)
    if manifest.get('schema') != DAILY_SCHEMA or manifest.get('date') != str(day):
        _fail('declared native day schema/date differs')
    verified = verify_day(path.parent, str(day))
    for entry in manifest['pages']:
        body = (path.parent/entry['response']).read_bytes()
        if _sha(body) != verified['source_files'].get(entry['response']):
            _fail('native response changed after original verification')
        data = _json(body)['data']
        for field in ('total_hits', 'page_index', 'page_size'):
            _integer(data[field])
    for name, digest in verified['source_files'].items():
        if _sha((path.parent/name).read_bytes()) != digest:
            _fail('native day evidence changed during strict validation')
    if path.read_bytes() != raw:
        _fail('native day manifest changed during strict validation')
    return dict(date=verified['date'],pages=verified['pages'],total_hits=verified['total_hits'],
                manifest_sha256=manifest_spec['sha256'])


def _page(raw, receipt, day, number, started, completed, signal):
    if (receipt.get('url') != API or receipt.get('params') != _params(day, number)
            or receipt.get('status') != 200 or receipt.get('sha256') != _sha(raw)):
        _fail('page HTTP/source identity differs')
    begin, end = _stamp(receipt.get('collected_at')), _stamp(receipt.get('completed_at'))
    cutoff = datetime.combine(signal, time(15), CN)
    if (not started <= begin <= end <= completed <= _now() or end >= cutoff
            or begin.astimezone(CN).date() != signal or end.astimezone(CN).date() != signal):
        _fail('page observation interval/date/cutoff differs')
    payload = _json(raw)
    if payload.get('success') != 1 or not isinstance(payload.get('data'), dict):
        _fail('provider query unsuccessful')
    data = payload['data']
    try:
        count, index, size = (_integer(data[k]) for k in ('total_hits', 'page_index', 'page_size'))
    except (KeyError, TypeError, ValueError):
        _fail('invalid pagination identity')
    if (count < 0 or count > MAX_PAGES * 100 or index != number or size != 100
            or not isinstance(data.get('list'), list)
            or len(data['list']) != max(0, min(100, count - (number - 1) * 100))):
        _fail('page count/size/inventory invalid')
    for row in data['list']:
        if (not isinstance(row, dict) or not re.fullmatch(r'AN\d+', row.get('art_code', ''))
                or not isinstance(row.get('title'), str) or _provider_time(row.get('notice_date')).date().isoformat() != day):
            _fail('native row date/ID/title invalid')
        codes = row.get('codes')
        if (not isinstance(codes, list) or not codes or any(not isinstance(c, dict)
                or not all(isinstance(c.get(k), str) and c[k] for k in ('stock_code','market_code','ann_type')) for c in codes)):
            _fail('native issuer identity unresolved')
        for key in ('display_time', 'eiTime'):
            if row.get(key) and _provider_time(row[key]) > end:
                _fail('native publication timestamp is later than completed HTTP observation')
    return count, data['list']


def _adjudicate(root, symbols, previous, signal, rows, registry):
    grouped = {symbol: [] for symbol in symbols}
    for row in rows:
        touched = set(c['stock_code'] for c in row['codes']).intersection(grouped)
        for symbol in touched:
            if not is_equity_issuer(row, symbol):
                if row['art_code'] not in registry and _nonrisk_investor_record(row, symbol):
                    continue
                _fail('member issuer/type unresolved: ' + symbol + '/' + row['art_code'])
            normal = _daily_notice_row(row, symbol, row['notice_date'][:10], registry)
            if normal is None and row['art_code'] in registry:
                title = re.split('[:：]', row['title'], maxsplit=1)[-1]
                normal = dict(source_provider='eastmoney', announcement_id=row['art_code'],
                    issuer_symbol=symbol, notice_date=row['notice_date'][:10], title=title,
                    source_url=f"https://pdf.dfcfw.com/pdf/H2_{row['art_code']}_1.pdf")
            if normal is not None:
                grouped[symbol].append(normal)
    for symbol, notices in grouped.items():
        required = {k for k,v in registry.items() if v['symbol'] == symbol and previous.isoformat() <= v['notice_date'] <= signal.isoformat()}
        if not notices and not required:
            continue
        verified = load_verified_events(root, symbol, previous, signal, notices)
        for row in notices:
            event = verified.get(row['announcement_id'])
            if event is None:
                _fail('member risk notice has no authenticated issuer/body decision: ' + symbol + '/' + row['announcement_id'])
            if any(event['risk_actions'].values()):
                _fail('member has authenticated announcement-day risk exclusion: ' + symbol + '/' + row['announcement_id'])


def _verify(root, reference, *, intraday, symbols=None, signal_day=None, previous_session=None, before_request=None):
    root = Path(root).resolve()
    if not isinstance(reference, dict) or reference.get('policy_revision') != POLICY:
        _fail('missing current member notice policy evidence')
    audit_id = reference.get('audit_id')
    if not isinstance(audit_id, str) or not re.fullmatch(r'[0-9a-f]{32}', audit_id):
        _fail('invalid audit identity')
    relative = f'{PREFIX}/{audit_id}/manifest.json'
    if reference.get('manifest_path') != relative:
        _fail('manifest path/identity differs')
    raw = _bound(root, relative).read_bytes()
    if _sha(raw) != reference.get('manifest_sha256'):
        _fail('manifest bytes changed')
    manifest = _json(raw)
    if (manifest.get('schema') != SCHEMA or manifest.get('policy_revision') != POLICY
            or manifest.get('audit_id') != audit_id or manifest.get('status') != 'observation_only_not_signal_permission'):
        _fail('manifest schema/identity differs')
    if intraday:
        _guard(date.fromisoformat(manifest['signal_day']), before_request)
    signal, previous = _days(manifest['signal_day'], manifest['previous_session'],
        before_request=_calendar_guard(date.fromisoformat(manifest['signal_day']), before_request) if intraday else None)
    actual_symbols = _symbols(manifest['symbols'])
    if actual_symbols != manifest['symbols']:
        _fail('manifest member union is not canonical')
    if symbols is not None and _symbols(symbols) != actual_symbols:
        _fail('actual member union differs from audited union')
    if ((signal_day is not None and str(signal_day) != signal.isoformat())
            or (previous_session is not None and str(previous_session) != previous.isoformat())):
        _fail('audit signal/anchor session differs')
    started, completed = _stamp(manifest['observation_start']), _stamp(manifest['observation_end'])
    if not started <= completed <= _now() or started.astimezone(CN).date() != signal or completed >= datetime.combine(signal,time(15),CN):
        _fail('manifest observation clock/date/cutoff invalid')
    registry, fingerprint = _registry(root)
    if fingerprint != manifest['registry_sha256']:
        _fail('approved registry changed after notice observation')
    expected_days = [(previous + timedelta(days=n)).isoformat() for n in range((signal-previous).days+1)]
    if [d.get('date') for d in manifest['days']] != expected_days:
        _fail('D..T observation calendar coverage incomplete')
    files, rows = {}, []
    for day in manifest['days']:
        total, ids, count = None, set(), 0
        for number, entry in enumerate(day['pages'],1):
            prefix = f"{PREFIX}/{audit_id}/{day['date']}/page_{number:04}"
            if entry.get('response') != prefix+'.json' or entry.get('receipt') != prefix+'.receipt.json':
                _fail('page path/sequence differs')
            body = _bound(root,entry['response']).read_bytes()
            receipt_raw = _bound(root,entry['receipt']).read_bytes()
            for key,data in [('response',body),('receipt',receipt_raw)]:
                digest = _sha(data)
                if entry.get(key+'_sha256') != digest:
                    _fail('page/receipt bytes changed')
                files[entry[key]] = digest
            page_total, page_rows = _page(body,_json(receipt_raw),day['date'],number,started,completed,signal)
            if total is not None and page_total != total:
                _fail('pagination total changed during observation')
            total = page_total
            for row in page_rows:
                if row['art_code'] in ids:
                    _fail('duplicate/missing native announcement identity')
                ids.add(row['art_code'])
            rows.extend(page_rows);count += len(page_rows)
        if total is None or len(day['pages']) != max(1,(total+99)//100) or count != total or len(ids) != total:
            _fail('missing notice page or response inventory')
    if manifest.get('source_files') != files:
        _fail('manifest source inventory differs')
    files[relative] = _sha(raw)
    result = dict(audit_id=audit_id,policy_revision=POLICY,manifest_path=relative,manifest_sha256=_sha(raw),
                  observation_start=manifest['observation_start'],observation_end=manifest['observation_end'],
                  symbols=actual_symbols,signal_day=signal.isoformat(),previous_session=previous.isoformat(),source_files=files)
    if result != reference:
        _fail('audit reference summary or closure differs')
    _adjudicate(root,actual_symbols,previous,signal,rows,registry)
    if _registry(root)[1] != fingerprint:
        _fail('approved registry changed during notice audit')
    if intraday:
        _guard(signal,before_request)
    return result


def verify_current_member_notice_audit(root, reference, symbols, signal_day, previous_session, before_request=None):
    return _verify(root,reference,intraday=True,symbols=symbols,signal_day=signal_day,previous_session=previous_session,before_request=before_request)


def current_member_notice_audit_files(root, reference):
    return _verify(root,reference,intraday=False)['source_files']


def audit_current_member_notices(root, symbols, signal_day, previous_session, before_request, *, session=None, reuse=None, required_asof=None):
    root = Path(root).resolve()
    started = _guard(date.fromisoformat(str(signal_day)),before_request)
    signal, previous = _days(signal_day,previous_session,
        before_request=_calendar_guard(date.fromisoformat(str(signal_day)),before_request))
    symbols = _symbols(symbols)
    required = started if required_asof is None else _stamp(required_asof.isoformat() if isinstance(required_asof,datetime) else required_asof)
    if required > started:
        _fail('required observation time is in the future')
    if reuse is not None:
        result = verify_current_member_notice_audit(root,reuse,symbols,signal,previous,before_request)
        if _stamp(result['observation_start']) < required:
            _fail('reused notice observation predates this run')
        return result
    _, fingerprint = _registry(root)
    audit_id = uuid.uuid4().hex
    folder = _bound(root,f'{PREFIX}/{audit_id}')
    folder.mkdir(parents=True,exist_ok=False)
    own = session is None
    session = session or requests.Session()
    days,files = [],{}
    try:
        day = previous
        while day <= signal:
            entries,total,number = [],None,1
            while total is None or number <= max(1,(total+99)//100):
                if number > MAX_PAGES:
                    _fail('bounded notice pagination budget exceeded')
                params = _params(day.isoformat(),number)
                begin = _guard(signal,before_request)
                receipt = dict(url=API,params=params,collected_at=begin.isoformat())
                response_raw = None
                try:
                    remaining = (datetime.combine(signal,time(15),CN)-_now()).total_seconds()
                    response = session.get(API,params=params,timeout=min(20,max(0.001,remaining)))
                    response_raw = response.content
                    receipt.update(status=response.status_code,sha256=_sha(response_raw),completed_at=_now().isoformat())
                    _guard(signal,before_request)
                    response.raise_for_status()
                except BaseException as error:
                    receipt.update(error=type(error).__name__,detail=str(error))
                    if 'completed_at' not in receipt:
                        receipt['completed_at'] = _now().isoformat()
                    raise
                finally:
                    prefix = f'{PREFIX}/{audit_id}/{day.isoformat()}/page_{number:04}'
                    if response_raw is not None:
                        _write(_bound(root,prefix+'.json'),response_raw)
                    _dump(_bound(root,prefix+'.receipt.json'),receipt)
                completed = _now()
                page_total,_ = _page(response_raw,receipt,day.isoformat(),number,started,completed,signal)
                if total is not None and page_total != total:
                    _fail('pagination total changed during collection')
                total = page_total
                raw_receipt = _bound(root,prefix+'.receipt.json').read_bytes()
                entry=dict(response=prefix+'.json',receipt=prefix+'.receipt.json',response_sha256=_sha(response_raw),receipt_sha256=_sha(raw_receipt))
                entries.append(entry)
                files[entry['response']]=entry['response_sha256'];files[entry['receipt']]=entry['receipt_sha256']
                number += 1
            days.append(dict(date=day.isoformat(),pages=entries));day += timedelta(days=1)
        completed = _guard(signal,before_request)
        manifest=dict(schema=SCHEMA,policy_revision=POLICY,audit_id=audit_id,status='observation_only_not_signal_permission',
                      signal_day=signal.isoformat(),previous_session=previous.isoformat(),symbols=symbols,
                      observation_start=started.isoformat(),observation_end=completed.isoformat(),
                      registry_sha256=fingerprint,days=days,source_files=files)
        path=f'{PREFIX}/{audit_id}/manifest.json'
        _dump(_bound(root,path),manifest)
        files=dict(files,**{path:_sha(_bound(root,path).read_bytes())})
        reference=dict(audit_id=audit_id,policy_revision=POLICY,manifest_path=path,manifest_sha256=files[path],
                       observation_start=started.isoformat(),observation_end=completed.isoformat(),symbols=symbols,
                       signal_day=signal.isoformat(),previous_session=previous.isoformat(),source_files=files)
        return verify_current_member_notice_audit(root,reference,symbols,signal,previous,before_request)
    finally:
        if own:
            session.close()
