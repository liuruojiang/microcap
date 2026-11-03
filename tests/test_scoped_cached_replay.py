import json
from pathlib import Path
from types import SimpleNamespace
import pandas as pd
import pytest
from scripts.st_metadata_refresh import try_cached_history_rebuild, MARKER, _sha

@pytest.mark.parametrize('change',['new_rebalance','economic_change','unknown_field','tampered_metadata'])
def test_scoped_replay_cannot_assume_equivalence(tmp_path,change):
    cache=tmp_path/'.microcap_index_cache';(cache/'security_meta').mkdir(parents=True);(cache/'backup').mkdir()
    old={'list_date':'2000-01-01','st_intervals':[]};new=dict(old,notice_query_end_date='2026-11-02')
    if change=='economic_change':new['st_intervals']=[{'start':'2026-11-02','end':None}]
    if change=='unknown_field':new['name']='Changed'
    a=json.dumps(old).encode();b=json.dumps(new).encode();(cache/'backup/000001.json').write_bytes(a);(cache/'security_meta/000001.json').write_bytes(b)
    marker=dict(status='complete',target='2026-11-02',work='.microcap_index_cache',old_hashes={'000001':_sha(a)},new_hashes={'000001':_sha(b)})
    (tmp_path/MARKER).write_text(json.dumps(marker))
    if change=='tampered_metadata':(cache/'security_meta/000001.json').write_bytes(b+b' ')
    meta=tmp_path/'meta.json';meta.write_text(json.dumps({'return_basis_consumption':{'revision':'stock_return_consumption_v1','full_rebuild_anchor':'2026-10-29'}}))
    panel=tmp_path/'panel.csv';panel.write_text('date\n2026-10-29\n2026-10-30\n2026-11-02\n')
    base=SimpleNamespace(proxy_meta_matches_execution_model=lambda p:False,build_biweekly_rebalance_dates=lambda d:pd.DatetimeIndex(['2026-10-30'] if change=='new_rebalance' else ['2026-10-29']))
    kw=dict(root=tmp_path,base=base,args=None,paths={'proxy_meta':meta},panel_path=panel,target='2026-11-02')
    if change=='tampered_metadata':
        with pytest.raises(ValueError):try_cached_history_rebuild(**kw)
    else:assert try_cached_history_rebuild(**kw) is False
    assert not (cache/'scoped_proxy_rebuild').exists()

@pytest.mark.parametrize('bad',[False,True])
def test_terminal_close_date_uses_next_exchange_session(monkeypatch,bad):
    from datetime import date
    from scripts.st_metadata_refresh import complete_terminal_return_dates
    from scripts import exchange_calendar
    monkeypatch.setattr(exchange_calendar,'sessions_for_day',lambda d:(date(2026,10,29),date(2026,10,30),date(2026,11,2)))
    f=pd.DataFrame([dict(rebalance_date='2026-10-29',execution_date='2026-10-29',effective_date='2026-10-28' if bad else '2026-10-29',execution_timing='close',return_start_date=pd.NaT)])
    if bad:
        with pytest.raises(ValueError):complete_terminal_return_dates(f,pd.to_datetime(['2026-10-29','2026-10-30','2026-11-02']),'2026-10-29')
    else:
        out=complete_terminal_return_dates(f,pd.to_datetime(['2026-10-29','2026-10-30','2026-11-02']),'2026-10-29')
        assert out.return_start_date.iloc[0]==pd.Timestamp('2026-10-30')
        assert pd.isna(f.return_start_date.iloc[0])

@pytest.mark.parametrize('changed_return',[False,True])
def test_proxy_replay_preserves_bytes_but_rejects_return_change(tmp_path,changed_return):
    import numpy as np
    import microcap_top100_mom16_biweekly_live_v2_0 as v
    from scripts.st_metadata_refresh import preserve_verified_replay_prefix
    old=b'date,close,daily_return\r\n2026-10-29,100.12345678901234,0.01\r\n'
    p=tmp_path/'index.csv';f=pd.read_csv(__import__('io').BytesIO(old));f.loc[0,'close']=np.nextafter(f.loc[0,'close'],np.inf)
    if changed_return:f.loc[0,'daily_return']+=1e-8
    f.to_csv(p,index=False)
    if changed_return:
        with pytest.raises(AssertionError):preserve_verified_replay_prefix(v.base_mod,old,p,proxy=True)
    else:
        preserve_verified_replay_prefix(v.base_mod,old,p,proxy=True);assert p.read_bytes()==old


@pytest.mark.parametrize('representation', ['string', 'timestamp', 'numpy64'])
def test_turnover_constraint_date_uses_same_strict_daily_normalisation(representation):
    import numpy as np
    from scripts.top100_data_contracts import normalise_turnover_table
    old = pd.DataFrame([dict(rebalance_date='2026-10-29', execution_timing='close',
                            constraint_trade_date='2026-10-29', two_side_cost_rate=0.0003)])
    new = old.copy()
    value = {'string': '2026-10-29T00:00:00.000000000',
             'timestamp': pd.Timestamp('2026-10-29'),
             'numpy64': np.datetime64('2026-10-29', 'ns')}[representation]
    new['constraint_trade_date'] = [value]
    pd.testing.assert_frame_equal(normalise_turnover_table(old), normalise_turnover_table(new),
                                  check_exact=True, check_dtype=False)


@pytest.mark.parametrize('value', ['not-a-date', '2026-10-29T00:00:01', '2026-10-29T00:00:00+08:00'])
def test_turnover_constraint_date_does_not_truncate_or_drop_invalid_values(value):
    from scripts.top100_data_contracts import normalise_turnover_table
    frame = pd.DataFrame([dict(rebalance_date='2026-10-29', execution_timing='close',
                              constraint_trade_date=value, two_side_cost_rate=0.0003)])
    with pytest.raises(ValueError):
        normalise_turnover_table(frame)


@pytest.mark.parametrize('field,value', [('constraint_trade_date', '2026-10-30'),
                                       ('two_side_cost_rate', 0.000300000001), ('entry_count', 6)])
def test_turnover_date_format_compatibility_keeps_values_exact(field, value):
    from scripts.top100_data_contracts import normalise_turnover_table
    old = pd.DataFrame([dict(rebalance_date='2026-10-29', execution_timing='close',
                            constraint_trade_date='2026-10-29', two_side_cost_rate=0.0003,
                            entry_count=5)])
    new = old.copy()
    new.loc[0, field] = value
    with pytest.raises(AssertionError):
        pd.testing.assert_frame_equal(normalise_turnover_table(old), normalise_turnover_table(new),
                                      check_exact=True, check_dtype=False)
