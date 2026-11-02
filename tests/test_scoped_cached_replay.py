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
