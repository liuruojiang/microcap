import hashlib
import json
import pytest
from scripts.collect_member_daily_inputs import adjusted_pair, read_bound

def factor(symbol, kind, records):
    market='sh'+symbol
    raw=(f'var {market}{kind}='+json.dumps({'total':len(records),'data':records})).encode()
    return raw,dict(status=200,url=f'https://finance.sina.com.cn/realstock/company/{market}/{kind}.js',sha256=hashlib.sha256(raw).hexdigest())

def pair(qtoday=2,htoday=.5):
    bodies, receipts={},{}
    for kind, current in [('qfq',qtoday),('hfq',htoday)]:
        bodies[kind],receipts[kind]=factor('600001',kind,[{'d':'2026-11-05','f':1},{'d':'2026-11-06','f':current}])
    return bodies,receipts

def test_ratio_uses_corporate_action_factors_not_raw_price_move():
    bodies,receipts=pair(.5,2)
    got=adjusted_pair('600001','2026-11-05','2026-11-06',100,50,bodies,receipts)
    assert got['close_adj']/got['previous_close_adj']==1

def test_disagreeing_factor_axes_are_not_a_valid_signal():
    bodies,receipts=pair(2,2)
    with pytest.raises(ValueError,match='disagreement'):
        adjusted_pair('600001','2026-11-05','2026-11-06',100,50,bodies,receipts)

def test_factor_source_hash_is_verified():
    bodies,receipts=pair()
    bodies['hfq']+=b' '
    with pytest.raises(ValueError,match='identity/hash'):
        adjusted_pair('600001','2026-11-05','2026-11-06',100,50,bodies,receipts)

def test_bound_input_cannot_escape_checkpoint(tmp_path):
    with pytest.raises(ValueError,match='escaped'):
        read_bound(tmp_path,{'path':'../outside','sha256':'0'*64})

def test_factor_pair_matches_native_whole_history_builder():
    from scripts.stock_return_basis import build_ratio_return_basis
    bodies,receipts=pair(.5,2)
    frame,_=build_ratio_return_basis(symbol='600001',raw_csv=b'date,close_raw\n2026-11-05,100\n2026-11-06,50\n',
        qfq_source=bodies['qfq'],hfq_source=bodies['hfq'],qfq_receipt=receipts['qfq'],hfq_receipt=receipts['hfq'],
        as_of='2026-11-06',sessions=['2026-11-05','2026-11-06'],list_date='2026-11-05')
    got=adjusted_pair('600001','2026-11-05','2026-11-06',100,50,bodies,receipts)
    assert got['close_adj']/got['previous_close_adj']==pytest.approx(frame.close_adj.iloc[-1]/frame.close_adj.iloc[0],abs=1e-15)
