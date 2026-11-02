import json,shutil
from pathlib import Path
import pandas as pd
import pytest
from scripts.stock_return_pipeline import load_return_prices, ReturnBasisError
F=Path(__file__).parent/'fixtures/stock_return_basis_real'

def test_dated_replay_keeps_verified_cache_and_rejects_corruption(tmp_path):
    folder=tmp_path/'.microcap_index_cache/return_basis/sources/000418';folder.mkdir(parents=True)
    for kind in ('qfq','hfq'):
        shutil.copyfile(F/(kind+'.js'),folder/(kind+'.js'))
        receipt=json.loads((F/(kind+'.json')).read_bytes());receipt['retrieved_at_utc']='2012-06-12T08:00:00+00:00'
        (folder/(kind+'.json')).write_text(json.dumps(receipt))
    kw=dict(root=tmp_path,symbol='000418',raw_path=F/'000418_raw.csv',master=pd.DataFrame([dict(symbol='000418',list_date='1993-11-29',delist_date=None)]))
    full=load_return_prices(**kw,as_of='2012-06-12')
    candidate=tmp_path/'.microcap_index_cache/return_basis/prices_adj/000418.csv';before=candidate.read_bytes()
    historical=load_return_prices(**kw,as_of='2012-06-11')
    assert pd.to_datetime(historical.date).max()==pd.Timestamp('2012-06-11')
    assert historical.attrs['return_basis_manifest']['as_of']=='2012-06-12'
    pd.testing.assert_frame_equal(historical,full.loc[pd.to_datetime(full.date)<=pd.Timestamp('2012-06-11')])
    assert candidate.read_bytes()==before
    candidate.write_bytes(before+b'\n')
    with pytest.raises(ReturnBasisError):load_return_prices(**kw,as_of='2012-06-11')
