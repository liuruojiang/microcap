"""Synthetic transport fixtures; not market evidence or production approval."""
import hashlib
import json
from pathlib import Path

import pytest

from scripts import st_notice_refresh as m
from scripts.st_notice_incremental import collect_days
from scripts.st_notice_evidence import StNoticeEvidenceError


class Session:
    def __init__(self, *, changed=False, fail_day=None, display_fraction="100"):
        self.changed, self.fail_day, self.calls = changed, fail_day, []
        self.display_fraction = display_fraction

    def get(self, url, *, params, timeout):
        day = params['begin_time']
        self.calls.append(day)
        if day == self.fail_day:
            raise ConnectionError('fixture provider unavailable')
        rows = [dict(art_code='AN' + day.replace('-', '') + '01', notice_date=day,
                     title='fixture ordinary notice', display_time=day+' 12:00:00.'+self.display_fraction, codes=[dict(stock_code='600000', market_code='1', ann_type='A')])]
        if self.changed and day == '2026-10-29':
            rows.append(dict(rows[0], art_code='AN2026102902', title='fixture late risk notice'))
        raw = json.dumps(dict(success=1, data=dict(page_index=1, page_size=100,
                         total_hits=len(rows), list=rows))).encode()
        class Response:
            status_code = 200
            content = raw
            def raise_for_status(self):
                pass
        return Response()


@pytest.fixture
def seed(tmp_path):
    parent = tmp_path / m.PREFIX
    parent.mkdir(parents=True)
    daily = parent / 'seed'
    collect_days(daily, '2026-10-29', '2026-10-29', session=Session())
    scope = parent / 'scope.json'
    scope.write_text(json.dumps(dict(end_date='2026-10-28', source_files=[])))
    spec = lambda p: dict(path=p.relative_to(tmp_path).as_posix(), sha256=hashlib.sha256(p.read_bytes()).hexdigest())
    archive = parent / 'archive.json'
    archive.write_text(json.dumps(dict(schema_version=3, scope=spec(scope),
        daily_increments=[dict(date='2026-10-29', manifest=spec(daily / '2026-10-29/manifest.json'))])))
    pointer = tmp_path / m.POINTER_PATH
    pointer.write_text(json.dumps(dict(manifest_path=spec(archive)['path'], manifest_sha256=spec(archive)['sha256'])))
    return tmp_path, pointer, pointer.read_bytes()


def test_failure_does_not_advance_pointer(seed):
    root, pointer, before = seed
    with pytest.raises(ConnectionError):
        m.refresh_notice_archive(root=root, target='2026-10-30', session=Session(fail_day='2026-10-30'))
    assert pointer.read_bytes() == before
    assert not (root / m.FRESHNESS).exists()


def test_late_boundary_notice_blocks_history_rewrite(seed):
    root, pointer, before = seed
    with pytest.raises(StNoticeEvidenceError, match='boundary native events changed'):
        m.refresh_notice_archive(root=root, target='2026-10-30', session=Session(changed=True))
    assert pointer.read_bytes() == before


def test_same_target_requeries_without_changing_authority(seed):
    root, pointer, before = seed
    session = Session()
    result = m.refresh_notice_archive(root=root, target='2026-10-29', session=session)
    assert session.calls == ['2026-10-29']
    assert result['authority_changed'] is False
    assert pointer.read_bytes() == before
    assert m.freshness_source_files(root)
    proof = json.loads((root / m.FRESHNESS).read_bytes())
    response = next(p for p in proof['source_files'] if p.endswith('page_0001.json'))
    (root / response).write_bytes(b'changed')
    with pytest.raises(StNoticeEvidenceError, match='freshness evidence changed'):
        m.freshness_source_files(root)


def test_new_day_is_contiguous_and_backed_up(seed):
    root, pointer, before = seed
    session = Session()
    result = m.refresh_notice_archive(root=root, target='2026-10-30', session=session)
    assert session.calls == ['2026-10-29', '2026-10-30']
    assert result['authority_changed'] is True
    manifest = json.loads((root / result['manifest_path']).read_bytes())
    assert [r['date'] for r in manifest['daily_increments']] == ['2026-10-29', '2026-10-30']
    assert (root / result['manifest_path']).with_name('previous_pointer.json').read_bytes() == before


def test_archived_original_proof_uses_original_path_and_keeps_current_authority(seed):
    root, pointer, _ = seed
    first = m.refresh_notice_archive(root=root, target='2026-10-30', session=Session())
    authority = [first['manifest_path'], first['manifest_sha256']]
    second = m.refresh_notice_archive(root=root, target='2026-10-31', session=Session())
    proof_path = (root / second['manifest_path']).with_name('previous_freshness.json')
    relative = proof_path.relative_to(root).as_posix()
    digest = hashlib.sha256(proof_path.read_bytes()).hexdigest()
    before = pointer.read_bytes()
    verified = m.archived_freshness_source_files(root, relative, digest, authority)
    assert verified[relative] == digest
    assert m.FRESHNESS not in verified
    assert pointer.read_bytes() == before
    assert m.freshness_source_files(root)[m.FRESHNESS] != digest
    with pytest.raises(StNoticeEvidenceError, match='another authority'):
        m.archived_freshness_source_files(root, relative, digest, m.resolve_archive_reference(root))
    with pytest.raises(StNoticeEvidenceError, match='bytes changed'):
        m.archived_freshness_source_files(root, relative, '0'*64, authority)


def test_original_multistep_notice_chain_is_contiguous_and_not_reclocked(seed):
    from scripts.scoped_source_consumer import verify_notice_chain
    root, pointer, original = seed
    previous = json.loads(original)
    first = m.refresh_notice_archive(root=root, target='2026-10-30', session=Session())
    second = m.refresh_notice_archive(root=root, target='2026-10-31', session=Session())
    old_path = (root / second['manifest_path']).with_name('previous_freshness.json')
    current = root / m.FRESHNESS
    specs = [dict(path=p.relative_to(root).as_posix(), sha256=hashlib.sha256(p.read_bytes()).hexdigest())
             for p in (old_path, current)]
    before = {p:p.read_bytes() for p in (pointer, old_path, current)}
    result = verify_notice_chain(root, [previous['manifest_path'], previous['manifest_sha256']], specs, '2026-10-31')
    assert result['receipt_count'] == 2 and result['original_clocks_retained']
    assert result['authority'] == [second['manifest_path'], second['manifest_sha256']]
    assert all(p.read_bytes() == raw for p,raw in before.items())
    with pytest.raises(ValueError, match='missing or reordered'):
        verify_notice_chain(root, [previous['manifest_path'], previous['manifest_sha256']], specs[::-1], '2026-10-31')
    with pytest.raises(ValueError, match='requested target'):
        verify_notice_chain(root, [previous['manifest_path'], previous['manifest_sha256']], specs, '2026-11-02')


def test_explicit_unpromoted_recovery_reuses_original_observation_clocks(seed, monkeypatch):
    from datetime import datetime, timezone
    root, pointer, before = seed
    original = m._boundary_precision_drift
    monkeypatch.setattr(m, '_boundary_precision_drift', lambda *args: (_ for _ in ()).throw(StNoticeEvidenceError('review needed')))
    session = Session()
    with pytest.raises(StNoticeEvidenceError, match='review needed'):
        m.refresh_notice_archive(root=root, target='2026-10-30', session=session)
    generation = next((root / m.PREFIX).glob('refresh_*'))
    receipts = {p: p.read_bytes() for p in generation.rglob('*.receipt.json')}
    first_clock = min(datetime.fromisoformat(json.loads(raw)['collected_at']) for raw in receipts.values())
    monkeypatch.setattr(m, '_boundary_precision_drift', original)
    no_requests = Session(fail_day='2026-10-29')
    m.refresh_notice_archive(root=root, target='2026-10-30', session=no_requests,
                             resume_generation=generation, required_asof=datetime(2026, 10, 30, 7, tzinfo=timezone.utc))
    assert no_requests.calls == []
    assert {p: p.read_bytes() for p in receipts} == receipts
    assert json.loads((root / m.FRESHNESS).read_bytes())['required_asof'] == first_clock.isoformat()
    assert pointer.read_bytes() != before
    with pytest.raises(StNoticeEvidenceError, match='Invalid unpromoted'):
        m.refresh_notice_archive(root=root, target='2026-10-30', resume_generation=generation,
                                 required_asof=datetime(2026, 10, 30, 7, tzinfo=timezone.utc))


def test_recovery_rejects_stale_sources_and_other_directories(seed, monkeypatch):
    from datetime import datetime, timezone, timedelta
    root, pointer, before = seed
    generation = root / m.PREFIX / 'refresh_prepared'
    collect_days(generation, '2026-10-29', '2026-10-30', session=Session())
    for folder, floor in [(generation, datetime.now(timezone.utc)),
                          (generation.parent.parent, datetime(2026, 10, 30, 7, tzinfo=timezone.utc))]:
        with pytest.raises(StNoticeEvidenceError):
            m.refresh_notice_archive(root=root, target='2026-10-30', resume_generation=folder, required_asof=floor)
    assert pointer.read_bytes() == before


def test_baseline_without_all_class_boundary_is_rejected(seed):
    root, pointer, before = seed
    reference = json.loads(before)
    path = root / reference['manifest_path']
    manifest = json.loads(path.read_bytes())
    manifest['daily_increments'] = []
    path.write_text(json.dumps(manifest))
    reference['manifest_sha256'] = m._sha(path.read_bytes())
    pointer.write_text(json.dumps(reference))
    before = pointer.read_bytes()
    session = Session()
    with pytest.raises(StNoticeEvidenceError, match='all-class daily boundary'):
        m.refresh_notice_archive(root=root, target='2026-10-30', session=session)
    assert not session.calls
    assert pointer.read_bytes() == before


def test_existing_source_concurrent_change_is_not_committed(seed):
    root, pointer, before = seed
    class ChangedDuringQuery(Session):
        def get(self, *args, **kwargs):
            response = super().get(*args, **kwargs)
            (root / m.PREFIX / 'scope.json').write_text('{}')
            return response
    with pytest.raises(StNoticeEvidenceError, match='hash mismatch'):
        m.refresh_notice_archive(root=root, target='2026-10-29', session=ChangedDuringQuery())
    assert pointer.read_bytes() == before


def test_missing_page_response_never_commits(seed):
    root, pointer, before = seed
    class MissingPage(Session):
        def get(self, *args, **kwargs):
            response = super().get(*args, **kwargs)
            payload = json.loads(response.content)
            payload['data']['total_hits'] = 101
            response.content = json.dumps(payload).encode()
            return response
    with pytest.raises(StNoticeEvidenceError, match='incomplete'):
        m.refresh_notice_archive(root=root, target='2026-10-29', session=MissingPage())
    assert pointer.read_bytes() == before


@pytest.mark.parametrize('mutation', ['empty', 'missing_boundary', 'target', 'schema', 'rehash_boundary'])
def test_freshness_reconstructs_actual_evidence(seed, mutation):
    root, pointer, before = seed
    m.refresh_notice_archive(root=root, target='2026-10-30', session=Session())
    path = root / m.FRESHNESS
    proof = json.loads(path.read_bytes())
    if mutation == 'empty':
        proof['queried_days'], proof['source_files'] = [], {}
    elif mutation == 'missing_boundary':
        proof['queried_days'] = proof['queried_days'][1:]
    elif mutation == 'target':
        proof['target'] = '2026-10-29'
    elif mutation == 'schema':
        proof['schema'] = 'wrong'
    else:
        folder = (root / proof['queried_days'][0]['manifest_path']).parent
        response = folder / 'page_0001.json'
        payload = json.loads(response.read_bytes())
        payload['data']['list'][0]['title'] = 'changed content, same id'
        response.write_bytes(m._bytes(payload))
        receipt = folder / 'page_0001.receipt.json'
        info = json.loads(receipt.read_bytes())
        info['sha256'] = m._sha(response.read_bytes())
        receipt.write_bytes(m._bytes(info))
        manifest = folder / 'manifest.json'
        data = json.loads(manifest.read_bytes())
        data['pages'][0]['response_sha256'] = m._sha(response.read_bytes())
        data['pages'][0]['receipt_sha256'] = m._sha(receipt.read_bytes())
        manifest.write_bytes(m._bytes(data))
        for p in (response, receipt, manifest):
            proof['source_files'][p.relative_to(root).as_posix()] = m._sha(p.read_bytes())
    path.write_bytes(m._bytes(proof))
    with pytest.raises(StNoticeEvidenceError):
        m.freshness_source_files(root)


@pytest.mark.parametrize('after_write', [False, True])
def test_publication_failure_rolls_back_only_owned_bytes(seed, monkeypatch, after_write):
    root, pointer, before = seed
    m.refresh_notice_archive(root=root, target='2026-10-29', session=Session())
    old_proof = (root / m.FRESHNESS).read_bytes()
    original = m._atomic
    failed = False
    def failing(path, raw):
        nonlocal failed
        if path == root / m.FRESHNESS and not failed:
            failed = True
            if after_write:
                original(path, raw)
            raise OSError('fixture publish failure')
        original(path, raw)
    monkeypatch.setattr(m, '_atomic', failing)
    with pytest.raises(OSError, match='publish failure'):
        m.refresh_notice_archive(root=root, target='2026-10-30', session=Session())
    assert pointer.read_bytes() == before
    assert (root / m.FRESHNESS).read_bytes() == old_proof
    assert m.freshness_source_files(root)


@pytest.mark.parametrize('configured,notice_fails', [(True, False), (True, True), (False, False)])
def test_real_refresh_state_orders_notice_between_panel_and_strategy(tmp_path, monkeypatch, configured, notice_fails):
    import sys
    from types import SimpleNamespace
    import pandas as pd
    from scripts import realtime_state_bundle as bundle
    for name in ('MICROCAP_ST_NOTICE_ARCHIVE', 'MICROCAP_ST_NOTICE_ARCHIVE_SHA256'):
        monkeypatch.delenv(name, raising=False)
    if configured:
        pointer = tmp_path / m.POINTER_PATH
        pointer.parent.mkdir(parents=True)
        pointer.write_text('{}')  # Hook mocked here; seed verification is covered above.
    calls = []
    def panel(*args):
        calls.append('panel')
        return tmp_path / 'panel.csv', pd.Timestamp('2026-10-30')
    def notice(**kwargs):
        calls.append('notice')
        assert kwargs['target'] == '2026-10-30'
        if notice_fails:
            raise StNoticeEvidenceError('fixture late announcement')
        return {'authority_changed': False}
    from scripts import st_metadata_refresh
    monkeypatch.setattr(st_metadata_refresh, 'refresh_metadata_after_notice', lambda **kw: {'requires_full_rebuild': False})
    def strategy(*args):
        calls.append('strategy')
        raise RuntimeError('stop after verified integration boundary')
    fake = SimpleNamespace(_sync_embedded_base_config=lambda: None,
        _build_base_args=lambda **kw: SimpleNamespace(index_csv=tmp_path/'index.csv'),
        base_mod=SimpleNamespace(read_csv_last_date=lambda p: '2026-10-29', freq_mod=SimpleNamespace(), DEFAULT_OUTPUT_PREFIX='fixture', build_output_paths=lambda p: {},
                                 build_refreshed_panel_shadow=panel, ensure_strategy_files=strategy))
    monkeypatch.setitem(sys.modules, 'microcap_top100_mom16_biweekly_live_v2_0', fake)
    monkeypatch.setitem(sys.modules, 'run_top100_v1_6_v1_8_realtime_signals',
                        SimpleNamespace(ensure_static_realtime_inputs=lambda **kw: None))
    monkeypatch.setattr(m, 'refresh_notice_archive', notice)
    monkeypatch.setattr(bundle, '_write_refresh_proof', lambda *a: None)
    monkeypatch.setattr(bundle, 'validate_refreshed_state', lambda *a: {'ok': True})
    monkeypatch.setattr(bundle, 'enforce_anchor_target', lambda *a: None)
    if notice_fails:
        with pytest.raises(RuntimeError, match='current ST notice refresh did not complete'):
            bundle.refresh_state(tmp_path)
        assert calls == ['panel', 'notice']  # No fallback despite otherwise reusable state.
    else:
        bundle.refresh_state(tmp_path)
        assert calls == (['panel', 'notice', 'strategy'] if configured else ['panel', 'strategy'])


def test_freshness_bundle_transport_survives_other_machine_root(seed, tmp_path):
    import shutil
    from scripts import realtime_state_bundle as bundle
    root, pointer, before = seed
    result = m.refresh_notice_archive(root=root, target='2026-10-30', session=Session())
    meta = root / '.microcap_index_cache/security_meta/600000.json'
    meta.parent.mkdir()
    meta.write_text(json.dumps(dict(st_notice_archive_sha256=result['manifest_sha256'])))
    files = bundle._st_source_transport_files(root)
    assert m.FRESHNESS in files
    other = tmp_path / 'other_machine'
    for relative in files:
        dst = other / relative
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(root / relative, dst)
    target_meta = other / meta.relative_to(root)
    target_meta.parent.mkdir(parents=True)
    shutil.copyfile(meta, target_meta)
    assert bundle._st_source_transport_files(other) == files
    proof = json.loads((other / m.FRESHNESS).read_bytes())
    missing = next(p for p in proof['source_files'] if p.endswith('page_0001.receipt.json'))
    (other / missing).unlink()
    with pytest.raises((FileNotFoundError, StNoticeEvidenceError)):
        bundle._st_source_transport_files(other)


def test_refresh_module_is_in_both_release_code_contracts():
    from scripts.stock_input_contract import SOURCE_FILES
    from scripts.top100_delivery import RETURN_BASIS_FILES
    assert 'scripts/st_notice_refresh.py' in SOURCE_FILES
    assert 'scripts/st_notice_refresh.py' in RETURN_BASIS_FILES


@pytest.mark.parametrize('before,after,count', [
    ('2026-10-30 12:00:00.100','2026-10-30 12:00:00.100',0),
    ('2026-10-30 12:00:00.100','2026-10-30 12:00:00.339',1),
    ('2026-10-30 12:00:00.999','2026-10-30 12:00:01.010',1),
])
def test_only_provider_subsecond_display_drift_is_accepted(before,after,count):
    old={'AN1':dict(art_code='AN1',title='unchanged',codes=[{'stock_code':'600000'}],display_time=before)}
    new={'AN1':dict(old['AN1'],display_time=after)}
    report=m._boundary_precision_drift(old,new)
    assert report['changed_event_count']==count
    assert old['AN1']['display_time']==before

@pytest.mark.parametrize('fault', ['day','second','over_second','close','title','issuer','added','deleted','missing_time','malformed_time'])
def test_boundary_drift_never_masks_material_or_boundary_changes(fault):
    import copy
    old={'AN1':dict(art_code='AN1',title='unchanged',codes=[{'stock_code':'600000'}],display_time='2026-10-30 23:59:59.900')}
    new=copy.deepcopy(old)
    if fault=='day':new['AN1']['display_time']='2026-10-31 00:00:00.100'
    elif fault=='second':new['AN1']['display_time']='2026-10-30 23:59:58.900'
    elif fault=='over_second':new['AN1']['display_time']='2026-10-30 23:59:58.899'
    elif fault=='close':
        old['AN1']['display_time']='2026-10-30 14:59:59.999';new['AN1']['display_time']='2026-10-30 15:00:00.001'
    elif fault=='title':new['AN1']['title']='different title'
    elif fault=='issuer':new['AN1']['codes'][0]['stock_code']='600001'
    elif fault=='added':new['AN2']=dict(new['AN1'],art_code='AN2')
    elif fault=='deleted':new={}
    elif fault=='missing_time':new['AN1'].pop('display_time')
    elif fault=='malformed_time':new['AN1']['display_time']='unknown'
    with pytest.raises(StNoticeEvidenceError):m._boundary_precision_drift(old,new)


def test_freshness_records_and_reverifies_precision_drift_without_authority_rewrite(seed):
    root,pointer,before=seed
    result=m.refresh_notice_archive(root=root,target='2026-10-29',session=Session(display_fraction='339'))
    assert result['authority_changed'] is False and pointer.read_bytes()==before
    path=root/m.FRESHNESS;proof=json.loads(path.read_bytes())
    assert proof['boundary_native_content_unchanged'] is False
    assert proof['boundary_display_time_precision_drift']['changed_event_count']==1
    assert proof['boundary_display_time_precision_drift']['maximum_delta_microseconds']==239000
    assert m.freshness_source_files(root)
    proof['boundary_display_time_precision_drift']['changed_event_count']=0
    path.write_bytes(m._bytes(proof))
    with pytest.raises(StNoticeEvidenceError,match='precision summary'):
        m.freshness_source_files(root)
