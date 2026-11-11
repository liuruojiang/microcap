"""Small immutable member-period handoff and daily checkpoint transport.

Bootstrap reads existing formal exports only. It never refreshes, re-ranks,
changes authority, or labels an unfinished maintenance operation successful.
"""
from __future__ import annotations
import argparse
import csv
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import hashlib
import io
import json
import math
import os
from pathlib import Path
import subprocess
import uuid

PREFIX = 'microcap_top100_mom16_biweekly_live_v2_0_base'
VERSIONS = ('v2.0', 'v2.3', 'v2.5')
SCHEMA = 'member_daily_checkpoint_v1'
REVISIONS = {'v2.0': 'plain_mom16_fixed1_20260904', 'v2.3': 'plain_lb25_hl2p5_r2off_no_overheat_20261029', 'v2.5': 'plain_lb20_hl3_entry0_exit0_20260905'}


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def encode(value):
    return (json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + '\n').encode('utf-8')


def rows(path):
    return list(csv.DictReader(io.StringIO(Path(path).read_text(encoding='utf-8-sig'))))


def day(value):
    text = str(value)
    parsed = date.fromisoformat(text[:10])
    if len(text) > 10:
        stamp = datetime.fromisoformat(text)
        if stamp.tzinfo is not None or any((stamp.hour, stamp.minute, stamp.second, stamp.microsecond)):
            raise ValueError('Expected a daily session date')
    return parsed.isoformat()


def read_bound(checkpoint, key):
    spec = checkpoint[key]
    path = (checkpoint['_directory'] / spec['path']).resolve()
    if Path(spec['path']).is_absolute() or not path.is_relative_to(checkpoint['_directory']):
        raise ValueError('Checkpoint reference escapes directory')
    raw = path.read_bytes()
    if digest(raw) != spec['sha256']:
        raise ValueError('Checkpoint file hash differs: ' + key)
    return raw


def _history_values(raw):
    """Compare exact decimal values, without CSV presentation or float drift."""
    result = []
    for row in csv.DictReader(io.StringIO(raw.decode('utf-8-sig'))):
        values = {key: Decimal(row[key]) for key in ('microcap', 'hedge')}
        if any(not value.is_finite() or value <= 0 for value in values.values()):
            raise ValueError('Invalid earned history value')
        result.append(dict(date=day(row['date']), **values))
    return result


def _turnover_values(raw):
    result = []
    date_keys = {'rebalance_date', 'constraint_trade_date', 'execution_date', 'effective_date', 'return_start_date'}
    for row in csv.DictReader(io.StringIO(raw.decode('utf-8-sig'))):
        normalized = {}
        for key, value in row.items():
            if key in date_keys:
                normalized[key] = day(value) if value else None
            else:
                try:
                    normalized[key] = Decimal(value) if value else None
                except InvalidOperation:
                    normalized[key] = value
        result.append(normalized)
    return result


def verify(path):
    path = Path(path).resolve()
    checkpoint = json.loads(path.read_bytes())
    if checkpoint.get('schema') != SCHEMA:
        raise ValueError('Unsupported checkpoint schema')
    checkpoint['_directory'] = path.parent
    anchor = day(checkpoint['as_of_date'])
    for key in ('period', 'history', 'turnover', 'version_state', 'prices'):
        read_bound(checkpoint, key)
    period = json.loads(read_bound(checkpoint, 'period'))
    members = period['effective_members']
    symbols = [item['symbol'] for item in members]
    if (period.get('schema') != 'member_period_v1' or len(symbols) != 100
            or len(set(symbols)) != 100 or any(len(s) != 6 or not s.isascii() or not s.isdigit() for s in symbols)
            or [int(item['rank']) for item in members] != list(range(1, 101))):
        raise ValueError('Period requires 100 unique ranked effective members')
    history = list(csv.DictReader(io.StringIO(read_bound(checkpoint, 'history').decode('utf-8-sig'))))
    dates = [day(item['date']) for item in history]
    if not dates or dates != sorted(set(dates)) or dates[-1] != anchor:
        raise ValueError('History anchor/order differs from checkpoint')
    for item in history:
        for field in ('microcap', 'hedge'):
            if not math.isfinite(float(item[field])) or float(item[field]) <= 0:
                raise ValueError('Invalid history level')
    prices = json.loads(read_bound(checkpoint, 'prices'))
    if prices.get('schema') != 'member_price_basis_v1' or prices['as_of_date'] != anchor:
        raise ValueError('Price basis anchor differs')
    if sorted(item['symbol'] for item in prices['rows']) != sorted(symbols):
        raise ValueError('Price basis member closure differs')
    if any(item['previous_raw_date'] != anchor or not math.isfinite(float(item['previous_raw_close']))
           or float(item['previous_raw_close']) <= 0 for item in prices['rows']):
        raise ValueError('Missing or invalid member close at checkpoint anchor')
    state = json.loads(read_bound(checkpoint, 'version_state'))
    if set(state['versions']) != set(VERSIONS):
        raise ValueError('Three version states required')
    if any(day(item['date']) != anchor for item in state['versions'].values()):
        raise ValueError('Version state date differs from checkpoint')
    if any(item.get('strategy_revision') != REVISIONS[version] for version, item in state['versions'].items()):
        raise ValueError('Version revision differs from approved strategy')
    if 'hedge_precision_bridge_v1' in period:
        from scripts.member_hedge_precision_bridge import checked_bridge
        checked_bridge(period, read_bound(checkpoint, 'history'), anchor,
                       version_state_raw=read_bound(checkpoint, 'version_state'))
    return checkpoint


def _write_package(out, files, metadata):
    out = Path(out).resolve()
    out.mkdir(parents=True, exist_ok=False)
    specs = {}
    for key, (name, raw) in files.items():
        (out / name).write_bytes(raw)
        specs[key] = {'path': name, 'sha256': digest(raw)}
    checkpoint = dict(schema=SCHEMA, **metadata, **specs)
    (out / 'checkpoint.json').write_bytes(encode(checkpoint))
    verify(out / 'checkpoint.json')
    return checkpoint


def calendar_links(anchor):
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from scripts.exchange_calendar import sessions_for_day
    import pandas as pd
    from microcap_top100_mom16_biweekly_live_v2_0 import base_mod
    build_biweekly_rebalance_dates = base_mod.build_biweekly_rebalance_dates
    sessions = pd.DatetimeIndex(sessions_for_day(date.fromisoformat(anchor))).normalize()
    if pd.Timestamp(anchor) not in sessions:
        raise ValueError('Checkpoint anchor is not an exchange session')
    future = sessions[sessions > pd.Timestamp(anchor)]
    rebalances = build_biweekly_rebalance_dates(sessions)
    later = rebalances[rebalances > pd.Timestamp(anchor)]
    if not len(future) or not len(later):
        raise ValueError('Calendar does not cover next session and rebalance')
    return str(future[0].date()), str(later[0].date())


def bootstrap(root, out):
    root = Path(root).resolve()
    output = root / 'outputs'
    paths = {
        'effective': output / (PREFIX + '_proxy_effective_members.csv'),
        'members': output / (PREFIX + '_proxy_members.csv'),
        'proxy': output / 'wind_microcap_top_100_biweekly_thursday_16y_cached.csv',
        'panel': output / (PREFIX + '_panel_refreshed.csv'),
        'turnover': output / (PREFIX + '_proxy_turnover.csv'),
    }
    source_files = {}
    manifest_path = output / 'top100_delivery_manifest.json'
    manifest_raw = manifest_path.read_bytes()
    manifest = json.loads(manifest_raw)
    if manifest.get('ok') is not True or manifest.get('status') != 'complete' or manifest.get('scope') != 'whole_workspace_delivery':
        raise ValueError('Bootstrap requires an existing completed formal delivery manifest')
    formal_hashes = {**manifest['inputs'], **manifest['artifacts']}
    transport_line_endings = []
    def source(path):
        raw = path.read_bytes()
        relative = path.relative_to(root).as_posix()
        source_files[relative] = digest(raw)
        if relative.startswith('outputs/'):
            expected = formal_hashes.get(relative)
            if expected != digest(raw):
                if expected is not None and expected == digest(raw.replace(b'\r\n', b'\n')):
                    transport_line_endings.append(relative)
                else:
                    raise ValueError('Bootstrap formal export hash differs: ' + relative)
        return raw
    for path in paths.values():
        source(path)
    proxy = rows(paths['proxy'])
    anchor = day(proxy[-1]['date'])
    if manifest['expected_date'] != anchor:
        raise ValueError('Bootstrap manifest anchor differs')
    effective = sorted(rows(paths['effective']), key=lambda x: int(x['rank']))
    seed_dates = {day(item['as_of_date']) for item in effective}
    if len(seed_dates) != 1 or next(iter(seed_dates)) > anchor:
        raise ValueError('Effective seed has no single valid bridge date')
    seed_date = next(iter(seed_dates))
    turnover = rows(paths['turnover'])
    if any(seed_date < day(item['rebalance_date']) <= anchor for item in turnover):
        raise ValueError('Effective member seed does not cover bridge')
    name_map = {item['symbol'].zfill(6): item.get('name', '') for item in rows(paths['members'])}
    members, basis = [], []
    for item in effective:
        symbol = item['symbol'].zfill(6)
        path = root / '.microcap_index_cache' / 'prices_raw' / (symbol + '.csv')
        source(path)
        closes = [r for r in rows(path) if day(r['date']) == anchor]
        if len(closes) != 1:
            raise ValueError('Member raw close missing at bridge: ' + symbol)
        members.append(dict(rank=int(item['rank']), symbol=symbol, name=name_map.get(symbol, ''), target_weight=.01))
        basis.append(dict(symbol=symbol, previous_raw_date=anchor, previous_raw_close=float(closes[0]['close_raw'])))
    panel = {day(item['date']): item for item in rows(paths['panel'])}
    history = io.StringIO(newline='')
    writer = csv.DictWriter(history, fieldnames=['date', 'microcap', 'hedge'])
    writer.writeheader()
    for item in proxy:
        dt = day(item['date'])
        if dt not in panel or not panel[dt].get('1.000852'):
            raise ValueError('Missing aligned hedge history: ' + dt)
        writer.writerow(dict(date=dt, microcap=item['close'], hedge=panel[dt]['1.000852']))
    versions = {}
    for version in VERSIONS:
        path = output / ('microcap_top100_mom16_biweekly_live_' + version.replace('.', '_') + '_latest_signal.csv')
        source(path)
        values = rows(path)
        if len(values) != 1 or day(values[0]['date']) != anchor:
            raise ValueError('Version date/row differs: ' + version)
        versions[version] = values[0]
    events = [dict(r) for r in turnover if day(r['rebalance_date']) == seed_date]
    # A native close-at-final-row event has no next return row yet. Resolve
    # that model clock from the independent calendar without rewriting the
    # native turnover CSV or claiming any next-session trade occurred.
    for event in events:
        if event.get('execution_timing') == 'close' and not event.get('return_start_date'):
            event['derived_return_start_date'] = calendar_links(seed_date)[0]
            event['return_start_date_source'] = 'independent_calendar_next_session_after_native_close'
    return_start = (events[-1].get('return_start_date') or events[-1].get('derived_return_start_date')) if events else None
    effective_from = day(return_start) if return_start else seed_date
    targets = [item for item in rows(paths['members']) if day(item['rebalance_date']) == seed_date]
    if len(targets) != 100 or len({item['symbol'] for item in targets}) != 100:
        raise ValueError('Formal target member snapshot incomplete')
    identity = digest(encode(dict(seed_date=seed_date, members=members, events=events)))
    next_trade_date, next_rebalance_date = calendar_links(anchor)
    period = dict(schema='member_period_v1', package_id=identity, anchor_day=seed_date, member_as_of=seed_date, next_rebalance_date=next_rebalance_date,
                  effective_from=effective_from, effective_members=members, target_members=targets, rebalance_events=events,
                  source_files=source_files, source_manifest_sha256=digest(manifest_raw),
                  transport_line_endings_lf_equivalent=transport_line_endings)
    strategy_sha = subprocess.run(['git', '-C', str(root), 'rev-parse', 'HEAD'], check=True, capture_output=True, text=True).stdout.strip()
    for relative, expected in source_files.items():
        if digest((root / relative).read_bytes()) != expected:
            raise ValueError('Bootstrap source changed during read: ' + relative)
    return _write_package(out, {
        'period': ('period.json', encode(period)),
        'history': ('history.csv', history.getvalue().encode('utf-8')),
        'turnover': ('turnover.csv', paths['turnover'].read_bytes()),
        'version_state': ('version_state.json', encode(dict(schema='member_version_state_v1', versions=versions))),
        'prices': ('member_prices.json', encode(dict(schema='member_price_basis_v1', as_of_date=anchor, rows=basis))),
    }, dict(as_of_date=anchor, next_trade_date=next_trade_date, strategy_sha=strategy_sha, bootstrap_source='existing_formal_exports'))


def handoff(current_path, candidate_path, session, next_session, out, *, hedge_precision_witness=None):
    current, candidate = verify(current_path), verify(candidate_path)
    session, next_session = day(session), day(next_session)
    if candidate['as_of_date'] != session or current['as_of_date'] > session or next_session <= session:
        raise ValueError('Handoff requires candidate close anchor and a later return-start session')
    extends_previous = current['as_of_date'] < session
    if extends_previous and current.get('next_trade_date') != session:
        raise ValueError('Handoff cannot skip a checkpoint exchange session')
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from scripts.exchange_calendar import sessions_for_day
    import pandas as pd
    from microcap_top100_mom16_biweekly_live_v2_0 import base_mod
    build_biweekly_rebalance_dates = base_mod.build_biweekly_rebalance_dates
    sessions = pd.DatetimeIndex(sessions_for_day(pd.Timestamp(next_session).date())).normalize()
    if extends_previous:
        previous = sessions[sessions < pd.Timestamp(session)]
        if not len(previous) or str(previous[-1].date()) != current['as_of_date']:
            raise ValueError('Handoff prior checkpoint is not the immediately preceding exchange session')
    if pd.Timestamp(session) not in build_biweekly_rebalance_dates(sessions):
        raise ValueError('Not a formal biweekly rebalance session')
    future = sessions[sessions > pd.Timestamp(session)]
    if not len(future) or future[0] != pd.Timestamp(next_session):
        raise ValueError('Return start is not the next actual exchange session')
    period = json.loads(read_bound(candidate, 'period'))
    current_period = json.loads(read_bound(current, 'period'))
    derived_history = derived_state = None
    bridge = None
    needs_bridge = hedge_precision_witness is not None or 'hedge_precision_bridge_v1' in current_period
    if extends_previous or needs_bridge:
        # The 15:10 native maintenance producer finishes R before the 16:00
        # report. Require its existing full-delivery acceptance and an exact
        # old-history prefix; do not manufacture a prior R daily run.
        if (candidate.get('bootstrap_source') != 'existing_formal_exports'
                or len(period.get('source_manifest_sha256', '')) != 64
                or not period.get('source_files')):
            raise ValueError('Prior-day handoff requires native formal-export source acceptance')
        previous_rows = _history_values(read_bound(current, 'history'))
        candidate_rows = _history_values(read_bound(candidate, 'history'))
        if needs_bridge:
            from scripts import member_hedge_precision_bridge as precision
            witness = json.loads(Path(hedge_precision_witness).read_bytes()) if hedge_precision_witness is not None else None
            derived_history, bridge, new_record_dates = precision.bridge_history(
                current=current, candidate=candidate, current_raw=read_bound(current, 'history'),
                candidate_raw=read_bound(candidate, 'history'), current_period=current_period,
                candidate_period=period, current_checkpoint_raw=Path(current_path).read_bytes(),
                candidate_checkpoint_raw=Path(candidate_path).read_bytes(), witness=witness)
            candidate_rows = _history_values(derived_history)
        expected_prefix = candidate_rows[:-1] if extends_previous else candidate_rows
        if (len(candidate_rows) != len(previous_rows) + int(extends_previous) or expected_prefix != previous_rows
                or day(candidate_rows[-1]['date']) != session):
            raise ValueError('Handoff cannot revise or skip the earned history prefix')
        from scripts.member_daily_signal import calculate_native, native_modules
        previous_frame = pd.read_csv(io.BytesIO(read_bound(current, 'history'))).set_index('date')
        previous_frame.index = pd.to_datetime(previous_frame.index)
        previous_turnover = pd.read_csv(io.BytesIO(read_bound(current, 'turnover')))
        previous_states = json.loads(read_bound(current, 'version_state'))['versions']
        for version, module in native_modules().items():
            original, _ = calculate_native(version, module, previous_frame, previous_turnover)
            for field in ('current_holding', 'next_holding', 'strategy_revision'):
                if str(previous_states[version].get(field)) != str(original.iloc[0][field]):
                    raise ValueError('Handoff prior model state does not match its native history: ' + version)
        if bridge is not None:
            from scripts.member_daily_signal import clean
            frame = pd.read_csv(io.BytesIO(derived_history)).set_index('date')
            frame.index = pd.to_datetime(frame.index)
            turnover = pd.read_csv(io.BytesIO(read_bound(candidate, 'turnover')))
            formal_states = json.loads(read_bound(candidate, 'version_state'))['versions']
            states = {}
            for version, module in native_modules().items():
                calculated, _ = calculate_native(version, module, frame, turnover)
                states[version] = clean(calculated.iloc[0].to_dict())
                for field in ('current_holding', 'next_holding', 'strategy_revision'):
                    if str(states[version].get(field)) != str(formal_states[version].get(field)):
                        raise ValueError('Hedge precision bridge changes native model state: ' + version)
            # A late same-close recovery keeps its already delivered state bytes.
            # The native recalculation above still guards all three identities.
            derived_state = (encode(dict(schema='member_version_state_v1', versions=states))
                             if extends_previous else read_bound(current, 'version_state'))
            for version in VERSIONS:
                for field in ('current_holding', 'next_holding', 'strategy_revision'):
                    actual = json.loads(derived_state)['versions'][version].get(field)
                    if str(actual) != str(states[version].get(field)):
                        raise ValueError('Hedge precision bridge changes earned same-close state: ' + version)
            period[precision.KEY] = precision.finish(
                bridge, history=derived_history, current_checkpoint_raw=Path(current_path).read_bytes(),
                candidate_checkpoint_raw=Path(candidate_path).read_bytes(),
                current_history_raw=read_bound(current, 'history'),
                candidate_history_raw=read_bound(candidate, 'history'),
                candidate_period_raw=read_bound(candidate, 'period'),
                current_state_raw=read_bound(current, 'version_state'),
                candidate_state_raw=read_bound(candidate, 'version_state'), derived_state_raw=derived_state,
                new_record_dates=new_record_dates)
    elif _history_values(read_bound(current, 'history')) != _history_values(read_bound(candidate, 'history')):
        raise ValueError('Handoff cannot revise same-day earned return history')
    old_events = _turnover_values(read_bound(current, 'turnover'))
    candidate_events = _turnover_values(read_bound(candidate, 'turnover'))
    old_prefix = [event for event in old_events if event['rebalance_date'] < session]
    candidate_prefix = [event for event in candidate_events if event['rebalance_date'] < session]
    if (old_prefix != candidate_prefix or any(event['rebalance_date'] > session for event in candidate_events)
            or any(event not in candidate_events for event in old_events if event['rebalance_date'] == session)):
        raise ValueError('Handoff cannot revise existing turnover/cost event history')
    events = period['rebalance_events']
    if (period['anchor_day'] != session or period['effective_from'] != next_session or not events
            or any(e.get('execution_timing') != 'close' or day(e['execution_date']) != session
                   or day(e['effective_date']) != session or day(e.get('return_start_date') or e.get('derived_return_start_date')) != next_session
                   or (not e.get('return_start_date') and e.get('return_start_date_source') != 'independent_calendar_next_session_after_native_close') for e in events)):
        raise ValueError('Candidate does not bind original close execution and next-session returns')
    files = {}
    for key in ('period', 'history', 'turnover', 'version_state', 'prices'):
        owner = candidate if extends_previous or key in ('period', 'turnover', 'prices') else current
        spec = owner[key]
        files[key] = (spec['path'], read_bound(owner, key))
    if bridge is not None:
        files['period'] = (candidate['period']['path'], encode(period))
        files['history'] = (candidate['history']['path'], derived_history)
        files['version_state'] = (candidate['version_state']['path'], derived_state)
    elif not extends_previous and 'hedge_precision_bridge_v1' in current_period:
        # Same-close period switches also retain the old daily's immutable proof.
        period['hedge_precision_bridge_v1'] = current_period['hedge_precision_bridge_v1']
        files['period'] = (candidate['period']['path'], encode(period))
    return _write_package(out, files, dict(as_of_date=session, next_trade_date=next_session, period_status='current', strategy_sha=candidate['strategy_sha'] if extends_previous else current['strategy_sha'],
                          handoff_mode='native_close_extends_previous' if extends_previous else 'same_close_period_switch',
                          handoff_from=json.loads(read_bound(current, 'period'))['package_id']))


def publish(checkpoint_path, pointer, expected_current_sha=None):
    checkpoint_path, pointer = Path(checkpoint_path).resolve(), Path(pointer).resolve()
    verify(checkpoint_path)
    pointer.parent.mkdir(parents=True, exist_ok=True)
    lock = pointer.with_name(pointer.name + '.lock')
    fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    try:
        os.close(fd)
        old = pointer.read_bytes() if pointer.exists() else None
        if old is not None and (expected_current_sha is None or digest(old) != expected_current_sha):
            raise ValueError('Published pointer changed or replacement was not explicitly bound')
        value = encode(dict(checkpoint=str(checkpoint_path), sha256=digest(checkpoint_path.read_bytes())))
        temporary = pointer.with_name(pointer.name + '.' + uuid.uuid4().hex + '.tmp')
        temporary.write_bytes(value)
        os.replace(temporary, pointer)
    finally:
        lock.unlink()


def schedule(root, session):
    import sys
    sys.path.insert(0, str(Path(root).resolve()))
    from scripts.exchange_calendar import sessions_for_day
    from microcap_top100_mom16_biweekly_live_v2_0 import base_mod
    import pandas as pd
    session = day(session)
    sessions = pd.DatetimeIndex(sessions_for_day(date.fromisoformat(session))).normalize()
    after = sessions[sessions > pd.Timestamp(session)]
    if not len(after):
        raise ValueError('Independent calendar does not cover next session')
    rebalances = base_mod.build_biweekly_rebalance_dates(sessions)
    return dict(date=session, is_trading_day=pd.Timestamp(session) in sessions,
                is_rebalance=pd.Timestamp(session) in rebalances, next_session=str(after[0].date()),
                source='scripts.exchange_calendar', anchor='2016-01-07', execution_timing='close')


def main(argv=None):
    # CLI is also invoked from the separate automation checkout.
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    p = sub.add_parser('bootstrap'); p.add_argument('--root', type=Path, required=True); p.add_argument('--out', type=Path, required=True)
    p = sub.add_parser('schedule'); p.add_argument('--root', type=Path, required=True); p.add_argument('--date', required=True); p.add_argument('--out', type=Path, required=True)
    p = sub.add_parser('verify'); p.add_argument('--checkpoint', type=Path, required=True)
    p = sub.add_parser('handoff')
    for name in ('current', 'candidate', 'out'):
        p.add_argument('--' + name, type=Path, required=True)
    p.add_argument('--session', required=True); p.add_argument('--next-session', required=True)
    p.add_argument('--hedge-precision-witness', type=Path)
    p = sub.add_parser('publish'); p.add_argument('--checkpoint', type=Path, required=True); p.add_argument('--pointer', type=Path, required=True); p.add_argument('--expected-current-sha')
    args = parser.parse_args(argv)
    if args.command == 'bootstrap': result = bootstrap(args.root, args.out)
    elif args.command == 'schedule':
        result = schedule(args.root, args.date); args.out.parent.mkdir(parents=True, exist_ok=True); args.out.write_bytes(encode(result))
    elif args.command == 'verify': result = verify(args.checkpoint); result.pop('_directory')
    elif args.command == 'handoff': result = handoff(args.current, args.candidate, args.session, args.next_session, args.out,
                                                  hedge_precision_witness=args.hedge_precision_witness)
    else: publish(args.checkpoint, args.pointer, args.expected_current_sha); result = {'published': True}
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
