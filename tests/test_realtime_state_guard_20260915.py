"""Regression: a validated transport must not trigger an implicit realtime rebuild."""
import hashlib
import json
from types import SimpleNamespace

import pytest
import pandas as pd
import microcap_top100_mom16_biweekly_live_v2_0 as v2
from scripts import realtime_state_bundle as state


@pytest.fixture
def intraday_notice_case(tmp_path, monkeypatch):
    """Authentic body registry; fake HTTP pages are isolated control inputs."""
    import copy
    from datetime import date, datetime, timezone
    from pathlib import Path
    from st_evidence_fixture import copy_real_evidence_subset
    from scripts import exchange_calendar
    from scripts import realtime_member_notice_audit as audit
    root = tmp_path / "audit_root"
    ids = ["AN202611021831333702", "AN202611021831331618"]
    copy_real_evidence_subset(Path(__file__).resolve().parents[1], root, ids)
    registry = json.loads((root / "data/st_notice_evidence/registry.json").read_bytes())
    events = {e['announcement_id']: e for e in registry['events']}
    def native(event_id):
        artifact = events[event_id]['native_evidence']['provider_response']['relative_path']
        payload = json.loads((root / 'data/st_notice_evidence' / artifact).read_bytes())
        return copy.deepcopy(next(r for r in payload['data']['list'] if r['art_code'] == event_id))
    clock = [datetime(2026, 11, 3, 6, 30, tzinfo=timezone.utc)]
    monkeypatch.setattr(audit, '_now', lambda: clock[0])
    monkeypatch.setattr(exchange_calendar, 'sessions_for_day', lambda d, before_request=None: (date(2026,11,2), date(2026,11,3)))
    symbols = [f'{n:06}' for n in range(1,101)]
    class Session:
        def __init__(self, rows=(), cross=False, future=False):
            self.rows = list(rows); self.calls = []; self.cross = cross; self.future = future
        def get(self, url, params, timeout):
            self.calls.append(dict(params))
            rows = copy.deepcopy(self.rows) if params['begin_time'] == '2026-11-03' else []
            if self.future and rows:
                rows[0]['display_time'] = '2026-11-03 14:31:00'
            body = json.dumps(dict(success=1, data=dict(total_hits=len(rows), page_index=int(params['page_index']),page_size=100,list=rows))).encode()
            owner = self
            class Response:
                status_code = 200
                @property
                def content(self):
                    if owner.cross:
                        clock[0] = datetime(2026,11,3,7,0,tzinfo=timezone.utc)
                    return body
                def raise_for_status(self):
                    return None
            return Response()
    def collect(rows=(), members=None, **kwargs):
        return audit.audit_current_member_notices(root, members or symbols, '2026-11-03', '2026-11-02',
                                                 lambda: None, session=Session(rows, **kwargs))
    return SimpleNamespace(root=root, audit=audit, symbols=symbols, Session=Session, native=native,
                           clock=clock, collect=collect)


@pytest.fixture
def state_only_notice_case(intraday_notice_case, monkeypatch):
    """Actual notice audit and state-only consumer, isolated transport fixtures."""
    from datetime import date
    from zoneinfo import ZoneInfo
    from test_realtime_publication_deadline import meta
    c = intraday_notice_case
    reference = c.collect()
    output = c.root/'outputs'; output.mkdir()
    members = output/'microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv'
    members.write_text('rebalance_date,symbol,name\n'+''.join('2026-11-02,'+symbol+',SYNTHETIC\n' for symbol in c.symbols))
    (c.root/state.PROXY_EFFECTIVE_MEMBERS_REL).write_text('as_of_date,symbol\n'+''.join(
        '2026-11-02,'+symbol+'\n' for symbol in c.symbols))
    monkeypatch.setattr(state, '_preflight_now', lambda: c.clock[0].astimezone(ZoneInfo('Asia/Shanghai')))
    base = v2.base_mod
    ns = base.load_realtime_refresh_proof.__globals__
    original_clock = ns['_cn_timestamp']
    monkeypatch.setitem(ns, '_cn_timestamp', lambda value=None: pd.Timestamp(c.clock[0]).tz_convert('Asia/Shanghai') if value is None else original_clock(value))
    monkeypatch.setitem(ns, '_cn_local_day', lambda value=None: pd.Timestamp(c.clock[0].astimezone(ZoneInfo('Asia/Shanghai')).date()))
    path = c.root/state.REFRESH_PROOF_REL; path.parent.mkdir(parents=True)
    monkeypatch.setitem(ns, 'REALTIME_REFRESH_PROOF_PATH', path)
    proof = base.build_realtime_refresh_proof_fields('2026-11-02')
    proof['preflight_evidence'] = {'current_member_notice_audit': reference}
    path.write_text(json.dumps(proof))
    summary = state.verify_intraday_notice_refresh_proof(c.root, proof)
    metadata = meta('2026-11-03T14:30:00+08:00')
    for field in ('member_quote_trade_date_min','member_quote_trade_date_max','hedge_quote_trade_date',
                  'quote_trade_date','expected_latest_completed_trade_date_verified_on'):
        metadata[field] = '2026-11-03'
    metadata['latest_anchor_trade_date'] = metadata['expected_latest_completed_trade_date'] = '2026-11-02'
    metadata.update(summary)
    monkeypatch.setenv('TOP100_REALTIME_REQUIRE_STATE', '1')
    row = pd.DataFrame({key:[v2.overlay_mod._csv_safe_value(value)] for key,value in metadata.items()})
    return SimpleNamespace(**vars(c), reference=reference, proof=proof, proof_path=path,
                           members_path=members, metadata=metadata, row=row, base=base)


@pytest.mark.parametrize('version', ['2_0','2_3','2_5'])
@pytest.mark.parametrize('fault', ['none','old_proof','raw_changed','union_changed','different_valid_audit',
                                   'missing_meta','missing_csv','all_notice_meta_removed','cutoff'])
def test_three_state_only_consumers_revalidate_same_notice_observation(state_only_notice_case, monkeypatch, version, fault):
    import importlib
    from datetime import datetime, timezone
    c = state_only_notice_case
    module = importlib.import_module('microcap_top100_mom16_biweekly_live_v'+version)
    v20 = module if version == '2_0' else module.v2_0
    guard = v20.overlay_mod._assert_realtime_signal_ready_to_commit
    assert guard is v2.overlay_mod._assert_realtime_signal_ready_to_commit
    if fault == 'old_proof':
        c.proof.pop('preflight_evidence');c.proof_path.write_text(json.dumps(c.proof))
    elif fault == 'raw_changed':
        page = c.root/next(k for k in c.reference['source_files'] if k.endswith('page_0001.json'))
        page.write_bytes(page.read_bytes()+b' ')
    elif fault == 'union_changed':
        c.members_path.write_text(c.members_path.read_text().replace('000100','000972'))
    elif fault == 'different_valid_audit':
        c.proof['preflight_evidence']['current_member_notice_audit'] = c.collect()
        c.proof_path.write_text(json.dumps(c.proof))
    elif fault == 'missing_meta':
        c.metadata.pop('notice_audit_id')
    elif fault == 'missing_csv':
        c.row.drop(columns=['notice_audit_id'],inplace=True)
    elif fault == 'all_notice_meta_removed':
        for field in ('notice_audit_required', *c.base.NOTICE_AUDIT_META_FIELDS):
            c.metadata.pop(field,None)
        c.row.drop(columns=[field for field in c.row if field.startswith('notice_')],inplace=True)
    elif fault == 'cutoff':
        c.clock[0] = datetime(2026,11,3,7,0,tzinfo=timezone.utc)
    if fault == 'none':
        v20.run_realtime_query_with_fresh_state(lambda: guard(c.row,c.metadata))
        for field in c.base.NOTICE_AUDIT_META_FIELDS:
            assert c.row.iloc[0][field] == c.metadata[field]
    else:
        with pytest.raises(RuntimeError):
            v20.run_realtime_query_with_fresh_state(lambda: guard(c.row,c.metadata))


def test_notice_proof_closed_transport_carries_only_bound_active_audit(state_only_notice_case):
    c = state_only_notice_case
    assert state._current_notice_audit_transport_files(c.root) == sorted(c.reference['source_files'])


def test_previous_day_notice_audit_restores_as_archive_not_todays_live_proof(state_only_notice_case):
    from datetime import datetime, timezone
    c = state_only_notice_case
    original = {name:(c.root/name).read_bytes() for name in c.reference['source_files']}
    c.clock[0] = datetime(2026,11,4,6,30,tzinfo=timezone.utc)
    assert state._current_notice_audit_transport_files(c.root) == sorted(c.reference['source_files'])
    assert c.audit.current_member_notice_audit_files(c.root,c.reference) == c.reference['source_files']
    with pytest.raises(RuntimeError, match='notice audit'):
        state.verify_intraday_notice_refresh_proof(c.root,c.proof)
    assert original == {name:(c.root/name).read_bytes() for name in original}


def test_close_proof_is_not_relabelled_as_intraday_notice_observation(state_only_notice_case):
    from datetime import datetime, timezone
    c = state_only_notice_case
    c.proof.pop('preflight_evidence');c.proof_path.write_text(json.dumps(c.proof))
    c.clock[0] = datetime(2026,11,3,12,0,tzinfo=timezone.utc)
    assert 'current_member_notice_audit_summary' not in c.base.load_realtime_refresh_proof('2026-11-02')


@pytest.mark.parametrize('version', ['v2.0','v2.3','v2.5'])
def test_three_real_csv_commits_keep_shared_notice_identity(state_only_notice_case, monkeypatch, tmp_path, version):
    import importlib
    from test_realtime_member_action_contract import _fake_realtime_base, _patch_native_realtime_builder
    c = state_only_notice_case
    module = importlib.import_module('microcap_top100_mom16_biweekly_live_v'+version[1:].replace('.','_'))
    builder = (module._build_realtime_v2_0_outputs_unlocked if version == 'v2.0'
               else getattr(module, '_build_realtime_v'+version[1:].replace('.','_')+'_outputs_unlocked'))
    native_writer = builder.__globals__['_atomic_write_text']
    payload = _fake_realtime_base(latest_anchor_trade_date='2026-11-02',quote_trade_date='2026-11-03',official_rebalance=False)
    payload.meta.update(c.metadata)
    _patch_native_realtime_builder(monkeypatch,version,payload)
    namespace = builder.__globals__
    monkeypatch.setitem(namespace,'_atomic_write_text',native_writer)
    monkeypatch.setitem(namespace,'ensure_output_dir',lambda:None)
    destination = tmp_path/'signal.csv'
    monkeypatch.setitem(namespace,'REALTIME_SIGNAL_CSV',destination)
    v2.run_realtime_query_with_fresh_state(builder)
    row = pd.read_csv(destination).iloc[0]
    for field in c.base.NOTICE_AUDIT_META_FIELDS:
        assert row[field] == c.metadata[field]


def test_intraday_notice_unrelated_risk_does_not_block_and_transports_exact_closure(intraday_notice_case):
    c = intraday_notice_case
    row = c.native('AN202611021831333702')
    row['art_code'] = 'AN202611039999999999'
    row['codes'][0].update(stock_code='600999', market_code='1')
    proof = c.collect([row])
    files = c.audit.current_member_notice_audit_files(c.root, proof)
    assert files == proof['source_files'] and len(files) == 5
    assert c.audit.verify_current_member_notice_audit(c.root, proof, c.symbols, '2026-11-03', '2026-11-02') == proof


@pytest.mark.parametrize('fault', ['registered_entry', 'unreviewed_member_risk', 'certified_event_missing',
                                   'wrong_issuer_type', 'future_publication', 'HTTP_completion_at_1500'])
def test_intraday_notice_related_event_or_clock_cannot_pass(intraday_notice_case, fault):
    c = intraday_notice_case
    row = c.native('AN202611021831333702')
    symbols = c.symbols[:-1] + ['000972']
    kwargs = {}
    if fault == 'unreviewed_member_risk':
        row['art_code'] = 'AN202611039999999999'
    if fault == 'wrong_issuer_type':
        row['codes'][0]['market_code'] = '1'
    if fault == 'future_publication':
        kwargs['future'] = True
    if fault == 'HTTP_completion_at_1500':
        kwargs['cross'] = True
    with pytest.raises(ValueError):
        c.collect([] if fault == 'certified_event_missing' else [row], symbols, **kwargs)
    assert not list((c.root / '.microcap_index_cache').glob('st_notice_archive_pointer.json'))
    assert not (c.root / '.microcap_index_cache/security_meta').exists()


def test_intraday_notice_certified_progress_is_not_new_entry(intraday_notice_case):
    c = intraday_notice_case
    symbols = c.symbols[:-1] + ['002592']
    proof = c.collect([c.native('AN202611021831331618')], symbols)
    assert proof['symbols'] == sorted(symbols)


@pytest.mark.parametrize('fault', ['raw_bytes', 'registry_bytes', 'union', 'date', 'missing_policy', 'reference_summary'])
def test_intraday_notice_consumer_rejects_changed_evidence(intraday_notice_case, fault):
    c = intraday_notice_case
    proof = c.collect()
    symbols = c.symbols
    signal = '2026-11-03'
    if fault == 'raw_bytes':
        path = c.root / next(p for p in proof['source_files'] if 'page_0001.json' in p)
        path.write_bytes(path.read_bytes() + b' ')
    if fault == 'registry_bytes':
        path = c.root / 'data/st_notice_evidence/registry.json'
        path.write_bytes(path.read_bytes() + b' ')
    if fault == 'union':
        symbols = symbols[:-1] + ['000972']
    if fault == 'date':
        signal = '2026-11-04'
    if fault == 'missing_policy':
        proof.pop('policy_revision')
    if fault == 'reference_summary':
        proof['observation_end'] = '2026-11-03T06:31:00+00:00'
    with pytest.raises(ValueError):
        c.audit.verify_current_member_notice_audit(c.root, proof, symbols, signal, '2026-11-02')


def test_intraday_notice_old_proof_archives_but_cannot_publish_after_cutoff(intraday_notice_case):
    from datetime import datetime, timezone
    c = intraday_notice_case
    proof = c.collect()
    c.clock[0] = datetime(2026,11,3,7,0,tzinfo=timezone.utc)
    assert c.audit.current_member_notice_audit_files(c.root, proof) == proof['source_files']
    with pytest.raises(ValueError):
        c.audit.verify_current_member_notice_audit(c.root, proof, c.symbols, '2026-11-03', '2026-11-02')


def test_intraday_notice_cutoff_before_HTTP_does_not_start(intraday_notice_case):
    from datetime import datetime, timezone
    c = intraday_notice_case
    c.clock[0] = datetime(2026,11,3,7,0,tzinfo=timezone.utc)
    session = c.Session()
    with pytest.raises(ValueError):
        c.audit.audit_current_member_notices(c.root,c.symbols,'2026-11-03','2026-11-02',lambda: None,session=session)
    assert session.calls == []


def test_intraday_notice_reuse_and_callback_are_real(intraday_notice_case):
    from datetime import timedelta
    c = intraday_notice_case
    proof = c.collect()
    session = c.Session();calls = []
    reused = c.audit.audit_current_member_notices(c.root,c.symbols,'2026-11-03','2026-11-02',lambda: calls.append(1),
                session=session,reuse=proof,required_asof=c.clock[0])
    assert reused == proof and calls and session.calls == []
    with pytest.raises(ValueError):
        c.audit.audit_current_member_notices(c.root,c.symbols,'2026-11-03','2026-11-02',lambda: None,
                session=session,reuse=proof,required_asof=c.clock[0]+timedelta(seconds=1))


@pytest.mark.parametrize('fault', ['missing_page','wrong_params','missing_timezone','changed_page_date',
                                   'future_completion','duplicate_row','missing_issuer','category_shape'])
def test_intraday_notice_even_rebound_artifacts_keep_raw_contract(intraday_notice_case, fault):
    c = intraday_notice_case
    # An unrelated risk row is legal. Rebind hashes after deliberate corruption
    # to exercise actual page/receipt semantics, not just checksum rejection.
    row = c.native('AN202611021831333702')
    row['art_code'] = 'AN202611039999999999'
    row['codes'][0].update(stock_code='600999', market_code='1')
    if fault == 'category_shape':
        row['codes'][0].update(stock_code='000050', market_code='0')
        row['title'] = '隔离普通公告'
        row['columns'] = None
        with pytest.raises(ValueError):
            c.collect([row])
        return
    proof = c.collect([row])
    path = c.root / proof['manifest_path']
    manifest = json.loads(path.read_bytes())
    if fault == 'missing_page':
        manifest['days'][-1]['pages'] = []
    else:
        entry = manifest['days'][-1]['pages'][0]
        key = 'receipt' if fault in ('wrong_params','missing_timezone','future_completion') else 'response'
        artifact = c.root / entry[key]
        value = json.loads(artifact.read_bytes())
        if fault == 'wrong_params':value['params']['end_time'] = '2026-11-04'
        if fault == 'missing_timezone':value['completed_at'] = '2026-11-03T14:30:00'
        if fault == 'future_completion':value['completed_at'] = '2026-11-03T06:31:00+00:00'
        if fault == 'changed_page_date':value['data']['list'][0]['notice_date'] = '2026-11-02 00:00:00'
        if fault == 'missing_issuer':value['data']['list'][0]['codes'] = []
        if fault == 'duplicate_row':
            value['data']['list'].append(value['data']['list'][0]);value['data']['total_hits'] = 2
        data = json.dumps(value).encode();artifact.write_bytes(data)
        entry[key+'_sha256'] = hashlib.sha256(data).hexdigest()
        manifest['source_files'][entry[key]] = entry[key+'_sha256']
        if key == 'response':
            receipt_path = c.root / entry['receipt'];receipt = json.loads(receipt_path.read_bytes())
            receipt['sha256'] = entry['response_sha256']
            receipt_raw = json.dumps(receipt).encode();receipt_path.write_bytes(receipt_raw)
            entry['receipt_sha256'] = hashlib.sha256(receipt_raw).hexdigest()
            manifest['source_files'][entry['receipt']] = entry['receipt_sha256']
    raw = json.dumps(manifest).encode();path.write_bytes(raw)
    proof['manifest_sha256'] = hashlib.sha256(raw).hexdigest()
    proof['source_files'] = dict(manifest['source_files'], **{proof['manifest_path']:proof['manifest_sha256']})
    with pytest.raises(ValueError):
        c.audit.verify_current_member_notice_audit(c.root,proof,c.symbols,'2026-11-03','2026-11-02')


def test_intraday_notice_callback_cannot_finish_at_cutoff(intraday_notice_case):
    from datetime import datetime, timezone
    c = intraday_notice_case
    session = c.Session()
    def crosses():c.clock[0] = datetime(2026,11,3,7,0,tzinfo=timezone.utc)
    with pytest.raises(ValueError):
        c.audit.audit_current_member_notices(c.root,c.symbols,'2026-11-03','2026-11-02',crosses,session=session)
    assert session.calls == []


def test_intraday_notice_calendar_transport_receives_actual_guard(intraday_notice_case, monkeypatch):
    from datetime import date
    from scripts import exchange_calendar
    c = intraday_notice_case
    observed = []
    def calendar(day, before_request=None):
        assert callable(before_request)
        assert before_request.intraday_deadline.isoformat() == '2026-11-03T15:00:00+08:00'
        before_request();observed.append(day)
        return (date(2026,11,2), date(2026,11,3))
    monkeypatch.setattr(exchange_calendar, 'sessions_for_day', calendar)
    c.collect()
    assert observed == [date(2026,11,3), date(2026,11,3)]


@pytest.mark.parametrize('field,value', [('total_hits',0.5),('page_index',1.5),('page_size',100.5),
                                       ('total_hits',False),('page_index',True),('page_size',True)])
def test_intraday_notice_pagination_never_truncates_fraction_or_boolean(intraday_notice_case, field, value):
    c = intraday_notice_case
    proof = c.collect()
    path = c.root / proof['manifest_path'];manifest = json.loads(path.read_bytes())
    entry = manifest['days'][-1]['pages'][0]
    response_path = c.root/entry['response'];payload = json.loads(response_path.read_bytes())
    payload['data'][field] = value
    raw = json.dumps(payload).encode();response_path.write_bytes(raw)
    entry['response_sha256'] = hashlib.sha256(raw).hexdigest()
    receipt_path = c.root/entry['receipt'];receipt = json.loads(receipt_path.read_bytes())
    receipt['sha256'] = entry['response_sha256'];receipt_raw = json.dumps(receipt).encode();receipt_path.write_bytes(receipt_raw)
    entry['receipt_sha256'] = hashlib.sha256(receipt_raw).hexdigest()
    for key in ('response','receipt'):manifest['source_files'][entry[key]] = entry[key+'_sha256']
    raw = json.dumps(manifest).encode();path.write_bytes(raw)
    proof['manifest_sha256'] = hashlib.sha256(raw).hexdigest()
    proof['source_files'] = dict(manifest['source_files'], **{proof['manifest_path']:proof['manifest_sha256']})
    with pytest.raises(ValueError):c.audit.verify_current_member_notice_audit(c.root,proof,c.symbols,'2026-11-03','2026-11-02')


@pytest.mark.parametrize('field,value', [('total_hits',0.5),('page_index',1.5),('page_size',100.5),
                                       ('total_hits',False),('page_index',True)])
def test_declared_closed_native_protocol_rejects_original_integer_truncation(tmp_path, field, value):
    from scripts import realtime_member_notice_audit as audit
    from scripts.st_notice_incremental import verify_day, API, SCHEMA, _params
    folder = tmp_path/'.microcap_index_cache/native_closed_fault/2026-11-03'
    folder.mkdir(parents=True)
    data = dict(total_hits=0,page_index=1,page_size=100,list=[]);data[field]=value
    raw = json.dumps(dict(success=1,data=data)).encode();(folder/'page_0001.json').write_bytes(raw)
    receipt = dict(status=200,url=API,params=_params('2026-11-03',1),sha256=hashlib.sha256(raw).hexdigest(),
                   collected_at='2026-11-03T08:00:00+00:00',completed_at='2026-11-03T08:00:01+00:00')
    r = json.dumps(receipt).encode();(folder/'page_0001.receipt.json').write_bytes(r)
    manifest = dict(schema=SCHEMA,date='2026-11-03',pages=[dict(response='page_0001.json',receipt='page_0001.receipt.json',
                    response_sha256=hashlib.sha256(raw).hexdigest(),receipt_sha256=hashlib.sha256(r).hexdigest())])
    raw_manifest = json.dumps(manifest).encode();(folder/'manifest.json').write_bytes(raw_manifest)
    # Demonstrate the real existing gap, then require the new supplemental gate.
    assert verify_day(folder,'2026-11-03')['total_hits'] == 0
    spec = dict(path=(folder/'manifest.json').relative_to(tmp_path).as_posix(),sha256=hashlib.sha256(raw_manifest).hexdigest())
    with pytest.raises(ValueError):audit.strict_native_pagination(tmp_path,spec,'2026-11-03')
    # Exercise the real state transport integration, not a patched validator.
    # This minimal archive is an inventory fixture, never member eligibility.
    scope_path = tmp_path/'scope.json';scope_path.write_text(json.dumps({'source_files':[]}))
    archive = dict(schema_version=3,scope=dict(path='scope.json',sha256=hashlib.sha256(scope_path.read_bytes()).hexdigest()),
                   daily_increments=[dict(date='2026-11-03',manifest=spec)])
    archive_path = tmp_path/'archive.json';archive_path.write_text(json.dumps(archive))
    digest = hashlib.sha256(archive_path.read_bytes()).hexdigest()
    metadata = tmp_path/'.microcap_index_cache/security_meta/000001.json';metadata.parent.mkdir(parents=True)
    metadata.write_text(json.dumps({'st_notice_archive_sha256':digest}))
    pointer = tmp_path/'.microcap_index_cache/st_notice_archive_pointer.json'
    pointer.write_text(json.dumps(dict(manifest_path='archive.json',manifest_sha256=digest)))
    from scripts.st_history_scope import source_closure
    assert spec['path'] in source_closure(tmp_path)  # Old full closure accepts its int truncation.
    with pytest.raises(ValueError):state._st_source_transport_files(tmp_path)


def test_declared_closed_native_protocol_preserves_original_full_checks(tmp_path):
    from scripts import realtime_member_notice_audit as audit
    from scripts.st_notice_incremental import API, SCHEMA, _params
    folder = tmp_path/'native/2026-11-03';folder.mkdir(parents=True)
    raw = json.dumps(dict(success=1,data=dict(total_hits='0',page_index='1',page_size='100',list=[]))).encode()
    (folder/'page_0001.json').write_bytes(raw)
    receipt = dict(status=200,url=API,params=_params('2026-11-03',1),sha256=hashlib.sha256(raw).hexdigest(),
                   collected_at='2026-11-03T08:00:00+00:00',completed_at='2026-11-03T08:00:01+00:00')
    r = json.dumps(receipt).encode();(folder/'page_0001.receipt.json').write_bytes(r)
    manifest = dict(schema=SCHEMA,date='2026-11-03',pages=[dict(response='page_0001.json',receipt='page_0001.receipt.json',
                   response_sha256=hashlib.sha256(raw).hexdigest(),receipt_sha256=hashlib.sha256(r).hexdigest())])
    m = json.dumps(manifest).encode();(folder/'manifest.json').write_bytes(m)
    spec = dict(path='native/2026-11-03/manifest.json',sha256=hashlib.sha256(m).hexdigest())
    assert audit.strict_native_pagination(tmp_path,spec,'2026-11-03')['total_hits'] == 0
    # A matching strict numeric tuple cannot excuse a broken original receipt.
    receipt['params']['page_index'] = '2';r=json.dumps(receipt).encode();(folder/'page_0001.receipt.json').write_bytes(r)
    manifest['pages'][0]['receipt_sha256']=hashlib.sha256(r).hexdigest();m=json.dumps(manifest).encode();(folder/'manifest.json').write_bytes(m)
    spec['sha256']=hashlib.sha256(m).hexdigest()
    with pytest.raises(ValueError):audit.strict_native_pagination(tmp_path,spec,'2026-11-03')


@pytest.mark.parametrize("change", ["new_st_label", "updated_st_label", "unknown_field",
                                   "changed_interval", "unresolved_history", "tampered_bytes"])
def test_cached_replay_snapshot_label_preserves_semantic_guards(tmp_path, monkeypatch, change):
    """A real approved 000972 baseline; derived labels are isolated guard inputs."""
    from contextlib import nullcontext
    import base64
    import copy
    from pathlib import Path
    from scripts import st_metadata_refresh as metadata

    fixture = Path(__file__).parent / "fixtures/st_snapshot_label_replay_20261103"
    container = json.loads((fixture / "000972.json").read_bytes())
    assert container["schema"] == "exact_native_metadata_bytes_v1"
    raw = base64.b64decode(container["raw_utf8_base64"], validate=True)
    provenance = json.loads((fixture / "provenance.json").read_bytes())
    assert hashlib.sha256(raw).hexdigest() == provenance["sha256"]
    old = json.loads(raw)
    assert old["symbol"] == "000972" and old["current_st_snapshot_name"] is None
    assert old["current_st_history_resolved"] is True
    if change == "updated_st_label":
        old["current_st_snapshot_name"] = "*ST中基"
    if change == "unresolved_history":
        old["current_st_history_resolved"] = False
    new = copy.deepcopy(old)
    new["current_st_snapshot_name"] = "ST中基" if change == "updated_st_label" else "*ST中基"
    if change == "unknown_field":
        new["board"] = "unapproved board change"
    if change == "changed_interval":
        new["st_intervals"][-1]["start"] = "2026-11-04"
    semantic_match = metadata._history_equal(old, new, "2026-11-04")
    assert semantic_match is (change != "changed_interval")
    cache = tmp_path / ".microcap_index_cache"
    (cache / "backup").mkdir(parents=True)
    (cache / "security_meta").mkdir()
    oldraw, newraw = json.dumps(old).encode(), json.dumps(new).encode()
    (cache / "backup/000972.json").write_bytes(oldraw)
    current = cache / "security_meta/000972.json"
    current.write_bytes(newraw + (b" " if change == "tampered_bytes" else b""))
    marker = dict(status="complete", target="2026-11-04", work=".microcap_index_cache",
                  old_hashes={"000972": metadata._sha(oldraw)},
                  new_hashes={"000972": metadata._sha(newraw)})
    (tmp_path / metadata.MARKER).write_text(json.dumps(marker))
    paths = {k: tmp_path / (k + ".csv") for k in
             ("proxy_meta", "proxy_members", "proxy_turnover", "proxy_effective_members")}
    for path in paths.values():
        path.write_text("date\n2026-11-03\n")
    paths["proxy_meta"].write_text(json.dumps({"return_basis_consumption": {
        "revision": "stock_return_consumption_v1", "full_rebuild_anchor": "2026-11-03"}}))
    panel = tmp_path / "panel.csv"
    panel.write_text("date\n2026-11-03\n2026-11-04\n")
    args = SimpleNamespace(index_csv=tmp_path / "index.csv", costed_nav_csv=tmp_path / "nav.csv",
                           allow_full_rebuild=False)
    args.index_csv.write_text("date\n2026-11-03\n")
    args.costed_nav_csv.write_text("date\n2026-11-03\n")

    class ReachedNativeBuilder(BaseException):
        pass

    def native_boundary(staged_args, dates):
        assert staged_args.allow_full_rebuild is False
        assert list(dates) == [pd.Timestamp("2026-11-03")]
        raise ReachedNativeBuilder()

    base = SimpleNamespace(proxy_meta_matches_execution_model=lambda meta: False,
                           build_biweekly_rebalance_dates=lambda dates: pd.DatetimeIndex([]),
                           build_local_proxy_bundle=native_boundary)
    monkeypatch.setattr(metadata, "frozen_notice_sources", lambda root: nullcontext())
    kwargs = dict(root=tmp_path, base=base, args=args, paths=paths, panel_path=panel, target="2026-11-04")
    if change in ("new_st_label", "updated_st_label"):
        with pytest.raises(ReachedNativeBuilder):
            metadata.try_cached_history_rebuild(**kwargs)
    elif change == "tampered_bytes":
        with pytest.raises(ValueError, match="Changed cached replay metadata"):
            metadata.try_cached_history_rebuild(**kwargs)
    else:
        assert metadata.try_cached_history_rebuild(**kwargs) is False
        assert not (cache / "scoped_proxy_rebuild").exists()
    assert args.allow_full_rebuild is False


@pytest.mark.parametrize("cached_replay", [False, True])
@pytest.mark.parametrize("explicit_full_rebuild", [False, True])
def test_metadata_refresh_never_grants_implicit_full_rebuild(
    tmp_path, monkeypatch, cached_replay, explicit_full_rebuild
):
    """Exercise the real refresh driver through its downstream strategy call."""
    import sys
    from scripts import st_metadata_refresh, st_notice_refresh

    for name in ("MICROCAP_ST_NOTICE_ARCHIVE", "MICROCAP_ST_NOTICE_ARCHIVE_SHA256"):
        monkeypatch.delenv(name, raising=False)
    pointer = tmp_path / ".microcap_index_cache/st_notice_archive_pointer.json"
    pointer.parent.mkdir()
    pointer.write_text("{}")
    calls = []

    class ReachedStrategyBoundary(BaseException):
        pass

    def replay(**kwargs):
        calls.append(("cached_replay", kwargs["args"].allow_full_rebuild))
        return cached_replay

    def strategy(args, *unused):
        calls.append(("strategy", args.allow_full_rebuild))
        assert args.allow_full_rebuild is explicit_full_rebuild
        raise ReachedStrategyBoundary()

    base = SimpleNamespace(
        DEFAULT_OUTPUT_PREFIX="isolated", freq_mod=object(),
        build_output_paths=lambda prefix: {},
        build_refreshed_panel_shadow=lambda *args: (tmp_path / "panel.csv", "2026-11-03"),
        read_csv_last_date=lambda path: "2026-11-02", ensure_strategy_files=strategy,
    )
    runtime = SimpleNamespace(
        _sync_embedded_base_config=lambda: None,
        _build_base_args=lambda **kwargs: SimpleNamespace(index_csv=tmp_path / "index.csv"),
        base_mod=base,
    )
    monkeypatch.setitem(sys.modules, "microcap_top100_mom16_biweekly_live_v2_0", runtime)
    monkeypatch.setitem(sys.modules, "run_top100_v1_6_v1_8_realtime_signals",
                        SimpleNamespace(ensure_static_realtime_inputs=lambda **kwargs: None))
    monkeypatch.setattr(state, "_reuse_same_day_notice_proof", lambda *args: None)
    monkeypatch.setattr(st_notice_refresh, "refresh_notice_archive", lambda **kwargs: {})
    monkeypatch.setattr(st_metadata_refresh, "refresh_metadata_after_notice",
                        lambda **kwargs: {"requires_full_rebuild": True})
    monkeypatch.setattr(st_metadata_refresh, "try_cached_history_rebuild", replay)
    with pytest.raises(ReachedStrategyBoundary):
        state.refresh_state(tmp_path, allow_full_rebuild=explicit_full_rebuild)
    assert calls == [("cached_replay", explicit_full_rebuild),
                     ("strategy", explicit_full_rebuild)]


@pytest.mark.parametrize("failure", ["missing", "hedge", "metadata", "none"])
def test_real_base_guard_preserves_files_and_never_refreshes(tmp_path, monkeypatch, failure):
    ns = v2.realtime_core.load_realtime_base.__globals__
    paths = {key: tmp_path / key for key in ("proxy_meta", "proxy_members", "proxy_turnover")}
    resolved = SimpleNamespace(index_csv=tmp_path / "index", costed_nav_csv=tmp_path / "nav", output_paths=paths)
    for path in [resolved.index_csv, resolved.costed_nav_csv, *paths.values()]:
        path.write_bytes(b"preserve-me")
    if failure == "missing":
        paths["proxy_members"].unlink()
    before = {p.name: p.read_bytes() for p in tmp_path.iterdir()}
    monkeypatch.setitem(ns, "_sync_embedded_base_config", lambda: None)
    monkeypatch.setitem(ns, "_build_base_args", lambda: None)
    monkeypatch.setitem(ns, "_resolve_base_paths", lambda *a: resolved)
    monkeypatch.setitem(ns, "_base_costed_nav_matches_current_hedge_ratio", lambda *a: failure != "hedge")
    monkeypatch.setitem(ns, "_proxy_meta_matches_execution_model", lambda *a: failure != "metadata")
    monkeypatch.setitem(ns, "_seed_proxy_bundle", lambda *a: pytest.fail("seed write"))
    monkeypatch.setattr(ns["base_mod"], "build_refreshed_panel_shadow", lambda *a: pytest.fail("refresh write"))
    if failure == "none":
        ns["_ensure_base_outputs_unlocked"](state_only=True)
    else:
        with pytest.raises(RuntimeError, match="state-only"):
            ns["_ensure_base_outputs_unlocked"](state_only=True)
    assert before == {p.name: p.read_bytes() for p in tmp_path.iterdir()}


def test_real_realtime_call_passes_guard_before_data_access(monkeypatch):
    ns = v2.realtime_core.load_realtime_base.__globals__
    from contextlib import nullcontext
    monkeypatch.setenv("TOP100_REALTIME_REQUIRE_STATE", "1")
    monkeypatch.setitem(ns, "_v2_base_build_lock", nullcontext)
    def guard(*, state_only=False):
        assert state_only is True
        raise RuntimeError("checked-before-refresh")
    monkeypatch.setitem(ns, "_ensure_base_outputs_unlocked", guard)
    with pytest.raises(RuntimeError, match="checked-before-refresh"):
        ns["load_realtime_context"]()


def test_close_confirmed_generation_uses_validated_state_without_refresh(monkeypatch):
    ns = v2.embedded_context._load_embedded_base_context.__globals__
    from contextlib import nullcontext
    monkeypatch.setenv("TOP100_REALTIME_REQUIRE_STATE", "1")
    monkeypatch.setitem(ns, "_v2_base_build_lock", nullcontext)
    monkeypatch.setitem(ns, "_ensure_base_outputs_unlocked", lambda *, state_only: (
        None if state_only else pytest.fail("close-confirmed delivery rebuilt base state")
    ))
    resolved = SimpleNamespace(
        index_csv=pytest.fail,
        costed_nav_csv=pytest.fail,
        output_paths={"panel_shadow": pytest.fail},
    )
    # The guard is asserted before any live history refresh; no data fixture is
    # needed to prove that the unsafe branch is unavailable.
    monkeypatch.setitem(ns, "_build_base_args", lambda: None)
    monkeypatch.setitem(ns, "_resolve_base_paths", lambda *_: resolved)
    monkeypatch.setattr(ns["base_mod"], "read_csv_last_date", lambda *_: None)
    with pytest.raises(RuntimeError, match="state-only panel"):
        ns["_load_embedded_base_context"]()


def test_missing_reference_summary_never_rebuilds(tmp_path, monkeypatch):
    ns = v2.realtime_core.load_realtime_base.__globals__
    monkeypatch.setitem(ns, "_read_current_reference_summary", lambda *a: None)
    monkeypatch.setitem(ns, "_resolved_base_summary_json", lambda: tmp_path / "missing.json")
    monkeypatch.setitem(ns, "_ensure_base_outputs_unlocked", lambda: pytest.fail("rebuild"))
    with pytest.raises(RuntimeError, match="reference summary"):
        ns["_load_reference_summary_unlocked"](state_only=True)


def test_certified_dated_summary_is_accepted_after_delivery_only_code_change(tmp_path, monkeypatch):
    ns = v2.realtime_core.load_realtime_base.__globals__
    summary = tmp_path / "summary.json"
    summary.write_text(json.dumps({"latest_trade_date": "2026-09-16", "summary_version_key": "old"}))
    monkeypatch.setitem(ns, "_read_current_reference_summary", lambda *a: None)
    monkeypatch.setitem(ns, "_resolved_base_summary_json", lambda: summary)
    assert ns["_load_reference_summary_unlocked"](pd.Timestamp("2026-09-16"), state_only=True)["summary_version_key"] == "old"


def test_state_only_summary_can_carry_exactly_one_no_rebalance_session(tmp_path, monkeypatch):
    ns = v2.realtime_core.load_realtime_base.__globals__
    summary = tmp_path / "summary.json"
    summary.write_text(json.dumps({"latest_trade_date": "2026-09-15", "summary_version_key": "old", "latest_signal": {"current_holding": "microcap"}}))
    monkeypatch.setitem(ns, "_read_current_reference_summary", lambda *a: None)
    monkeypatch.setitem(ns, "_resolved_base_summary_json", lambda: summary)
    carried = ns["_load_reference_summary_unlocked"](
        pd.Timestamp("2026-09-16"), state_only=True,
        available_dates=pd.DatetimeIndex(["2026-09-15", "2026-09-16"]),
        turnover_df=pd.DataFrame({"rebalance_date": ["2026-09-03"]}),
    )
    assert carried["latest_signal"] == {}
    assert carried["state_only_summary_carry_forward"]["source_latest_trade_date"] == "2026-09-15"


def test_state_only_summary_can_carry_long_fresh_tail_without_rebalance(tmp_path, monkeypatch):
    ns = v2.realtime_core.load_realtime_base.__globals__
    summary = tmp_path / "summary.json"
    summary.write_text(json.dumps({"latest_trade_date": "2026-09-03", "summary_version_key": "old"}))
    monkeypatch.setitem(ns, "_read_current_reference_summary", lambda *a: None)
    monkeypatch.setitem(ns, "_resolved_base_summary_json", lambda: summary)
    carried = ns["_load_reference_summary_unlocked"](
        pd.Timestamp("2026-09-16"), state_only=True,
        available_dates=pd.DatetimeIndex(["2026-09-03", "2026-09-15", "2026-09-16"]),
        turnover_df=pd.DataFrame({"rebalance_date": ["2026-09-03"]}),
    )
    assert carried["state_only_summary_carry_forward"]["target_latest_trade_date"] == "2026-09-16"


def test_state_only_summary_rejects_rebalance_tail(tmp_path, monkeypatch):
    ns = v2.realtime_core.load_realtime_base.__globals__
    summary = tmp_path / "summary.json"
    summary.write_text(json.dumps({"latest_trade_date": "2026-09-15", "summary_version_key": "old"}))
    monkeypatch.setitem(ns, "_read_current_reference_summary", lambda *a: None)
    monkeypatch.setitem(ns, "_resolved_base_summary_json", lambda: summary)
    with pytest.raises(RuntimeError, match="reference summary"):
        ns["_load_reference_summary_unlocked"](
            pd.Timestamp("2026-09-16"), state_only=True,
            available_dates=pd.DatetimeIndex(["2026-09-15", "2026-09-16"]),
            turnover_df=pd.DataFrame({"rebalance_date": ["2026-09-16"]}),
        )


def test_metadata_transport_and_changed_bytes_are_checked(tmp_path):
    meta = tmp_path / ".microcap_index_cache/security_meta/000001.json"
    meta.parent.mkdir(parents=True)
    meta.write_bytes(b'{"st_intervals": []}\n')
    expected = {"present_count": 1, "missing_count": 0,
                "sha256": hashlib.sha256(b"000001\0" + meta.read_bytes() + b"\0").hexdigest()}
    proxy = tmp_path / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_meta.json"
    proxy.parent.mkdir()
    proxy.write_text(json.dumps({"core_params": {"security_meta_cache_fingerprint": expected}}))
    assert len(state.validate_security_metadata(tmp_path)) == 1
    assert meta.relative_to(tmp_path).as_posix() in state._iter_bundle_files(tmp_path)
    meta.write_bytes(b'{"st_intervals": ["changed"]}\n')
    with pytest.raises(ValueError, match="fingerprint mismatch"):
        state.validate_security_metadata(tmp_path)
    meta.unlink()
    with pytest.raises(ValueError, match="fingerprint mismatch"):
        state.validate_security_metadata(tmp_path)
