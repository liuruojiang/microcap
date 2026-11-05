"""Real 300237 exit-day regression; archive adapters are isolated, not live IO."""
import ast
import copy
import json
from pathlib import Path
import re
import socket
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
REAL = json.loads((ROOT / 'tests/fixtures/st_300237_inclusive_exit_20261105.json').read_text(encoding='utf8'))

@pytest.fixture(params=['standalone', 'embedded'])
def inclusive_exit_builder(request, tmp_path, monkeypatch):
    import scripts.st_notice_archive as archive
    import scripts.st_notice_evidence as evidence
    def no_network(*args, **kwargs):
        raise AssertionError('offline regression attempted network')
    monkeypatch.setattr(socket.socket, 'connect', no_network)
    name = 'analyze_top100_rebalance_frequency.py' if request.param == 'standalone' else 'microcap_top100_mom16_biweekly_live_v2_0.py'
    tree = ast.parse((ROOT / name).read_text(encoding='utf8'))
    if request.param == 'embedded':
        tree = ast.parse(ast.literal_eval(next(n.value for n in tree.body if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'FREQ_SOURCE' for t in n.targets))))
    required = {'build_security_meta', '_parse_st_notice_risk_transition', 'classify_st_notice_risk_actions', 'build_st_intervals_from_notices', 'merge_st_intervals', 'build_st_status_series', 'build_st_presample_name_anchor', '_require_resolved_current_st_history', '_security_meta_source_failures'}
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in required]
    price = tmp_path / 'price.csv'
    price.write_text('date\n2011-06-29\n2026-11-02\n', encoding='utf8')
    ns = dict(pd=pd, re=re, json=json, ROOT=tmp_path, START_DATE='2011-06-29', PRICE_DIR=tmp_path, SHARED_PRICE_DIR=None, SECURITY_META_DIR=tmp_path/'meta', SECURITY_META_VERSION=2, ST_NOTICE_POLICY_VERSION='unchanged', ST_NOTICE_PARSER_REVISION='unchanged', SecurityMetaHistoryEvidenceUnresolved=evidence.StNoticeEvidenceError, resolve_cache_path=lambda *a:price, resolve_security_meta_path=lambda *a:None, load_security_master=lambda:pd.DataFrame(), load_current_st_name_map=lambda:{'300237':'ST美晨'})
    exec(compile(ast.Module(body=nodes, type_ignores=[]), name, 'exec'), ns)
    state = copy.deepcopy(REAL)
    def load(*args):
        return dict(rows=state['rows'], name_changes=state['name_changes'], manifest_sha256='fixture', source_providers=['eastmoney'], notice_query_end_date=args[-1])
    monkeypatch.setattr(archive, 'load_notice_archive', load)
    monkeypatch.setattr(evidence, 'load_verified_events', lambda *a:{})
    monkeypatch.setattr(evidence, 'evidence_fingerprint', lambda *a:'fixture')
    return ns, state, tmp_path

def test_inclusive_exit_real_300237_builder_accepts_exit_day(inclusive_exit_builder):
    ns, state, root = inclusive_exit_builder
    result = ns['build_security_meta']('300237', as_of='2026-11-05')
    assert result['notice_count'] == 4
    assert result['last_trade_date'] == '2026-11-02'
    assert result['current_st_history_resolved'] is True
    assert result['st_intervals'] == [dict(start='2025-09-27',end='2026-11-05',source='archived_mixed_provider_notice')]
    dates = pd.DatetimeIndex(['2025-09-26','2025-09-27','2026-11-04','2026-11-05','2026-11-06'])
    assert ns['build_st_status_series'](result, dates).tolist() == [False,True,True,True,False]
    from scripts.st_metadata_refresh import _history_equal
    assert _history_equal({'st_intervals':[dict(start='2025-09-27',end=None)]}, result, '2026-11-04')
    assert (root/'meta/300237.json').exists()

def test_inclusive_exit_expired_interval_cannot_use_old_price_day(inclusive_exit_builder):
    ns, state, root = inclusive_exit_builder
    with pytest.raises(ValueError, match='no verified historical interval'):
        ns['build_security_meta']('300237', as_of='2026-11-06')
    assert not (root/'meta/300237.json').exists()

@pytest.mark.parametrize('case', ['missing', 'future_entry', 'conflict', 'source_error'])
def test_inclusive_exit_invalid_evidence_still_rejected(inclusive_exit_builder, case):
    ns, state, root = inclusive_exit_builder
    if case == 'missing': state['rows']=[]
    elif case == 'future_entry':
        state['rows']=[dict(state['rows'][0],notice_date='2026-11-06')]
    elif case == 'conflict':
        state['rows'].append(dict(state['rows'][0],notice_date='2026-11-05'))
    else:
        def fail(*args, **kwargs): raise RuntimeError('source unavailable')
        ns['build_st_intervals_from_notices']=fail
    with pytest.raises(ValueError): ns['build_security_meta']('300237', as_of='2026-11-05')
    assert not (root/'meta/300237.json').exists()

def test_inclusive_exit_future_exit_does_not_clear_today(inclusive_exit_builder):
    ns,state,root=inclusive_exit_builder
    state['rows'][-1]['notice_date']='2026-11-06'
    result=ns['build_security_meta']('300237', as_of='2026-11-05')
    assert result['st_intervals'][0]['end'] is None
    assert result['current_st_history_resolved'] is True
