"""Evidence-bound EM/Sina ZZ1000 representation bridge, never a price tolerance.

The earned history is immutable.  Only separately evidenced, exact decimal
quantization differences may explain why a formal export has another prefix.
Proof lives in period.json, which the unchanged daily reader carries forward.
"""
from __future__ import annotations

import base64
import copy
import csv
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import hashlib
import io
import json
import re
from zoneinfo import ZoneInfo

KEY = 'hedge_precision_bridge_v1'
SCHEMA = 'member_hedge_precision_bridge_v1'
WITNESS_SCHEMA = 'member_hedge_precision_witness_v1'
PANEL = 'outputs/microcap_top100_mom16_biweekly_live_v2_0_base_panel_refreshed.csv'
CONTRACT = dict(instrument='000852', earned_provider='eastmoney',
                earned_instrument='1.000852', earned_fqt=0,
                formal_provider='sina_legacy', formal_instrument='sh000852',
                earned_quantum='0.01', formal_quantum='0.001', rounding='ROUND_HALF_UP')


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _hash(value):
    if not isinstance(value, str) or re.fullmatch('[0-9a-f]{64}', value) is None:
        raise ValueError('Hedge precision proof requires an exact SHA256')
    return value


def blob(raw):
    """Encode immutable original evidence for transport inside period.json."""
    return dict(sha256=_sha(raw), base64=base64.b64encode(raw).decode('ascii'))


def _decimal(value):
    if isinstance(value, bool):
        raise ValueError('Invalid hedge precision decimal')
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError('Invalid hedge precision decimal') from exc
    if not number.is_finite() or number <= 0:
        raise ValueError('Invalid hedge precision decimal')
    return number


def _blob(spec):
    if not isinstance(spec, dict) or set(spec) != {'sha256', 'base64'}:
        raise ValueError('Hedge precision source bytes and SHA256 are required')
    if not isinstance(spec['base64'], str) or len(spec['base64']) > 12_000_000:
        raise ValueError('Hedge precision witness is too large')
    try:
        raw = base64.b64decode(spec['base64'], validate=True)
    except (ValueError, TypeError) as exc:
        raise ValueError('Invalid hedge precision source encoding') from exc
    if _sha(raw) != _hash(spec['sha256']):
        raise ValueError('Hedge precision source hash differs')
    return raw


def _json(raw):
    return json.loads(raw, parse_float=Decimal)


def _history(raw):
    reader = csv.DictReader(io.StringIO(raw.decode('utf-8-sig')))
    if reader.fieldnames != ['date', 'microcap', 'hedge']:
        raise ValueError('Hedge precision bridge requires the exact history columns')
    result = list(reader)
    dates = [row['date'] for row in result]
    if not result or dates != sorted(set(dates)):
        raise ValueError('Hedge precision history dates differ')
    for row in result:
        if datetime.fromisoformat(row['date']).date().isoformat() != row['date']:
            raise ValueError('Hedge precision history requires daily dates')
        _decimal(row['microcap']); _decimal(row['hedge'])
    return result


def _formal_source(raw, day):
    text = raw.decode('utf-8-sig')
    try:
        documents = [_json(text)]
    except (ValueError, TypeError):
        documents = []
        for line in text.splitlines():
            if '[index-history] ' in line:
                documents.append(_json(line.split('[index-history] ', 1)[1]))
    matches = []
    for attrs in documents:
        if not isinstance(attrs, dict):
            continue
        if (attrs.get('independent_history_source') == 'sina_legacy'
                and attrs.get('independent_history_start', '') <= day <= attrs.get('independent_history_end', '')
                and attrs.get('independent_history_attempts', [])
                and attrs['independent_history_attempts'][-1].get('source') == 'sina_legacy'
                and attrs['independent_history_attempts'][-1].get('ok') is True):
            matches.append(attrs)
    if not matches:
        raise ValueError('Hedge precision bridge lacks the actual Sina source receipt')


def checked_record(record):
    """Validate archived original source bytes; no network or inferred sources."""
    if not isinstance(record, dict) or set(record) != {'date', 'earned', 'formal'}:
        raise ValueError('Invalid hedge precision witness record')
    day = record['date']
    if not isinstance(day, str) or datetime.fromisoformat(day).date().isoformat() != day:
        raise ValueError('Invalid hedge precision witness date')
    earned, formal = record['earned'], record['formal']
    if (not isinstance(earned, dict) or not isinstance(formal, dict)
            or earned.get('provider') != 'eastmoney' or earned.get('instrument') != '1.000852'
            or formal.get('provider') != 'sina_legacy' or formal.get('instrument') != 'sh000852'):
        raise ValueError('Unapproved hedge precision source or instrument')
    quote = _json(_blob(earned['hedge_json']))
    if (quote.get('source') != 'eastmoney' or quote.get('market_date') != day
            or quote.get('close_confirmed') is not True
            or not quote.get('source_attempts')
            or quote['source_attempts'][-1].get('source') != 'eastmoney'
            or quote['source_attempts'][-1].get('ok') is not True):
        raise ValueError('Hedge precision earned close lacks the actual EM receipt')
    stamp = datetime.fromisoformat(quote['collected_at'])
    if stamp.tzinfo is None:
        raise ValueError('Hedge precision receipt lacks its source clock')
    stamp = stamp.astimezone(ZoneInfo('Asia/Shanghai'))
    if stamp.date().isoformat() != day or (stamp.hour, stamp.minute) < (15, 0):
        raise ValueError('Hedge precision receipt is not a same-day confirmed close')
    bars = list(csv.DictReader(io.StringIO(_blob(earned['bar_csv']).decode('utf-8-sig'))))
    if len(bars) != 1 or bars[0].get('date') != day:
        raise ValueError('Hedge precision EM bar date differs')
    earned_close, formal_close = _decimal(quote['close']), _decimal(formal['close'])
    if _decimal(bars[0]['close']) != earned_close:
        raise ValueError('Hedge precision EM source values disagree')
    if (earned_close.quantize(Decimal('0.01')) != earned_close
            or formal_close.quantize(Decimal('0.001')) != formal_close
            or earned_close == formal_close
            or formal_close.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) != earned_close):
        raise ValueError('Hedge precision values do not satisfy the exact EM/Sina contract')
    _hash(formal['manifest_sha256']); _hash(formal['panel_sha256'])
    _formal_source(_blob(formal['source_log']), day)
    return day, earned_close, formal_close


def _compare(old, new, records):
    used = set()
    if len(old) != len(new):
        raise ValueError('Hedge precision bridge cannot skip or remove earned sessions')
    for earned, formal in zip(old, new):
        if earned['date'] != formal['date'] or _decimal(earned['microcap']) != _decimal(formal['microcap']):
            raise ValueError('Hedge precision bridge cannot revise dates or microcap history')
        if _decimal(earned['hedge']) == _decimal(formal['hedge']):
            continue
        day = earned['date']
        if day not in records:
            raise ValueError('Hedge precision difference lacks its original source witness: ' + day)
        _, actual_earned, actual_formal = checked_record(records[day])
        if actual_earned != _decimal(earned['hedge']) or actual_formal != _decimal(formal['hedge']):
            raise ValueError('Hedge precision witness differs from actual earned/formal history')
        used.add(day)
    return used


def _formal_binding(record, period):
    proof = record['formal']
    if (proof['manifest_sha256'] != period.get('source_manifest_sha256')
            or proof['panel_sha256'] != period.get('source_files', {}).get(PANEL)):
        raise ValueError('Hedge precision witness formal source identity differs')


def _bound(document, key, raw):
    if document.get(key, {}).get('sha256') != _sha(raw):
        raise ValueError('Hedge precision original checkpoint binding differs: ' + key)


def _append(current_raw, last):
    header = next(csv.reader(io.StringIO(current_raw.decode('utf-8-sig'))))
    newline = '\r\n' if b'\r\n' in current_raw else '\n'
    stream = io.StringIO(newline='')
    csv.DictWriter(stream, fieldnames=header, lineterminator=newline).writerow(last)
    separator = b'' if current_raw.endswith((b'\r', b'\n')) else newline.encode()
    return current_raw + separator + stream.getvalue().encode('utf-8')


def checked_bridge(period, history_raw, checkpoint_day, *, version_state_raw=None):
    """Recheck immutable anchor prefixes, including after the old daily appends."""
    proof = period.get(KEY)
    if proof is None:
        return None
    if (not isinstance(proof, dict) or proof.get('schema') != SCHEMA
            or proof.get('contract') != CONTRACT or set(proof) != {'schema', 'contract', 'records', 'anchors'}):
        raise ValueError('Invalid hedge precision bridge contract')
    history = _history(history_raw)
    by_day = {row['date']: row for row in history}
    dates = []
    for record in proof['records']:
        day, earned, _ = checked_record(record)
        if day not in by_day or _decimal(by_day[day]['hedge']) != earned:
            raise ValueError('Hedge precision witnessed earned history changed')
        dates.append(day)
    if not dates or dates != sorted(set(dates)):
        raise ValueError('Hedge precision witness records are not unique and ordered')
    records = {record['date']: record for record in proof['records']}
    anchors = proof['anchors']
    if not anchors or [item['date'] for item in anchors] != sorted({item['date'] for item in anchors}):
        raise ValueError('Hedge precision anchors are not unique and ordered')
    available = {}
    for anchor in anchors:
        count, length = anchor['history_rows'], anchor['history_bytes']
        if (type(count) is not int or type(length) is not int or count <= 0 or length <= 0
                or count > len(history) or length > len(history_raw) or anchor['date'] > checkpoint_day):
            raise ValueError('Invalid hedge precision history anchor')
        prefix = history_raw[:length]
        if (_sha(prefix) != _hash(anchor['history_sha256']) or len(_history(prefix)) != count
                or history[count-1]['date'] != anchor['date']):
            raise ValueError('Hedge precision earned anchor prefix changed')
        original_current_raw = _blob(anchor['current_checkpoint'])
        original_candidate_raw = _blob(anchor['candidate_checkpoint'])
        original_current = json.loads(original_current_raw)
        original_candidate = json.loads(original_candidate_raw)
        formal_period_raw = _blob(anchor['candidate_period'])
        formal_period = json.loads(formal_period_raw)
        candidate_history = _blob(anchor['candidate_history'])
        candidate_state = _blob(anchor['candidate_version_state'])
        current_state = _blob(anchor['current_version_state'])
        derived_state = _blob(anchor['derived_version_state'])
        if (original_current.get('schema') != 'member_daily_checkpoint_v1'
                or original_candidate.get('schema') != 'member_daily_checkpoint_v1'
                or original_candidate.get('bootstrap_source') != 'existing_formal_exports'
                or KEY in formal_period):
            raise ValueError('Hedge precision original formal checkpoint identity differs')
        _bound(original_candidate, 'period', formal_period_raw)
        _bound(original_candidate, 'history', candidate_history)
        _bound(original_candidate, 'version_state', candidate_state)
        _bound(original_current, 'version_state', current_state)
        current_length = anchor['current_history_bytes']
        if type(current_length) is not int or current_length <= 0 or current_length > length:
            raise ValueError('Invalid hedge precision original history boundary')
        current_prefix = prefix[:current_length]
        _bound(original_current, 'history', current_prefix)
        old, new = _history(current_prefix), _history(candidate_history)
        if (original_current.get('as_of_date') != old[-1]['date']
                or original_candidate.get('as_of_date') != new[-1]['date']
                or new[-1]['date'] != anchor['date']):
            raise ValueError('Hedge precision original checkpoint date differs')
        new_dates = anchor['new_record_dates']
        if not isinstance(new_dates, list) or new_dates != sorted(set(new_dates)):
            raise ValueError('Invalid hedge precision new witness dates')
        for day in new_dates:
            if day in available or day not in records:
                raise ValueError('Hedge precision witness origin is not unique')
            _formal_binding(records[day], formal_period)
            available[day] = records[day]
        if anchor['mode'] == 'extends_previous':
            if (len(new) != len(old)+1 or original_current.get('next_trade_date') != anchor['date']
                    or prefix != _append(current_prefix, new[-1])):
                raise ValueError('Hedge precision derived history does not extend its original prefix')
            used = _compare(old, new[:-1], available)
        elif anchor['mode'] == 'same_close':
            if prefix != current_prefix or derived_state != current_state:
                raise ValueError('Hedge precision same-close must preserve all earned bytes and state')
            used = _compare(old, new, available)
        else:
            raise ValueError('Invalid hedge precision handoff mode')
        if set(new_dates) - used:
            raise ValueError('Hedge precision archived witness was not used by its source checkpoint')
        if anchor['derived_version_state_sha256'] != _sha(derived_state):
            raise ValueError('Hedge precision derived native state source differs')
        formal_states = json.loads(candidate_state)['versions']
        derived_states = json.loads(derived_state)['versions']
        for version in ('v2.0', 'v2.3', 'v2.5'):
            for field in ('current_holding', 'next_holding', 'strategy_revision'):
                if str(formal_states[version].get(field)) != str(derived_states[version].get(field)):
                    raise ValueError('Hedge precision archived native state is not equivalent')
        if (anchor['date'] == checkpoint_day and version_state_raw is not None
                and _sha(version_state_raw) != anchor['derived_version_state_sha256']):
            raise ValueError('Hedge precision derived native state changed')
    if dates[-1] > anchors[-1]['date']:
        raise ValueError('Hedge precision witness is newer than its bound anchor')
    if set(available) != set(records):
        raise ValueError('Hedge precision witness lacks its original formal source binding')
    return proof


def bridge_history(*, current, candidate, current_raw, candidate_raw, current_period,
                   candidate_period, current_checkpoint_raw, candidate_checkpoint_raw,
                   witness=None):
    """Return a derived immutable-prefix history and a flat reusable proof."""
    old = _history(current_raw); new = _history(candidate_raw)
    extends = current['as_of_date'] < candidate['as_of_date']
    if len(new) != len(old) + int(extends):
        raise ValueError('Hedge precision bridge cannot skip or remove earned sessions')
    inherited = checked_bridge(current_period, current_raw, current['as_of_date'])
    records = {item['date']: copy.deepcopy(item) for item in inherited['records']} if inherited else {}
    supplied = set()
    if witness is not None:
        if (not isinstance(witness, dict) or witness.get('schema') != WITNESS_SCHEMA
                or witness.get('current_checkpoint_sha256') != _sha(current_checkpoint_raw)
                or witness.get('candidate_checkpoint_sha256') != _sha(candidate_checkpoint_raw)):
            raise ValueError('Hedge precision witness checkpoint identity differs')
        for record in witness['records']:
            day, _, _ = checked_record(record)
            if day in supplied or (day in records and records[day] != record):
                raise ValueError('Hedge precision witness duplicates or revises an archived source')
            supplied.add(day); records[day] = copy.deepcopy(record)
    used = _compare(old, new[:-1] if extends else new, records)
    for day in supplied:
        _formal_binding(records[day], candidate_period)
    if supplied - used:
        raise ValueError('Hedge precision witness contains an unrelated date')
    if not used and not inherited:
        raise ValueError('Hedge precision bridge has no evidenced precision difference')
    # Keep every earned byte, including newline convention and original spelling.
    derived = _append(current_raw, new[-1]) if extends else current_raw
    proof = dict(schema=SCHEMA, contract=copy.deepcopy(CONTRACT),
                 records=[records[day] for day in sorted(records)],
                 anchors=copy.deepcopy(inherited['anchors']) if inherited else [])
    return derived, proof, sorted(supplied - set(item['date'] for item in inherited['records']) if inherited else supplied)


def finish(proof, *, history, current_checkpoint_raw, candidate_checkpoint_raw,
           current_history_raw, candidate_history_raw, candidate_period_raw,
           current_state_raw, candidate_state_raw, derived_state_raw, new_record_dates):
    proof = copy.deepcopy(proof)
    rows = _history(history)
    if proof['anchors'] and proof['anchors'][-1]['date'] == rows[-1]['date']:
        if new_record_dates:
            raise ValueError('Hedge precision cannot revise an already bound same-close proof')
        return proof
    proof['anchors'].append(dict(date=rows[-1]['date'], history_rows=len(rows), history_bytes=len(history),
                                 history_sha256=_sha(history),
                                 mode='same_close' if history == current_history_raw else 'extends_previous',
                                 current_history_bytes=len(current_history_raw),
                                 new_record_dates=new_record_dates,
                                 current_checkpoint=blob(current_checkpoint_raw),
                                 candidate_checkpoint=blob(candidate_checkpoint_raw),
                                 candidate_period=blob(candidate_period_raw),
                                 candidate_history=blob(candidate_history_raw),
                                 current_version_state=blob(current_state_raw),
                                 candidate_version_state=blob(candidate_state_raw),
                                 derived_version_state=blob(derived_state_raw),
                                 derived_version_state_sha256=_sha(derived_state_raw)))
    return proof
