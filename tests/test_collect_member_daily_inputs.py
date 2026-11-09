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


def install_tail_transport(monkeypatch,*,bad_second_axis=False):
    import pandas as pd
    import requests
    from types import SimpleNamespace
    from scripts import stock_daily_alternative as alt,index_history_preflight as index
    calls=[]
    def get(url,params=None,timeout=None):
        calls.append((url,params))
        if url==alt.PRICE_URL:
            code=params['param'].split(',')[0]
            body=json.dumps(dict(code=0,data={code:{'day':[['2026-11-05','100','100','100','100','10'],['2026-11-06','50','50','50','50','10']]}})).encode()
        else:
            market=url.split('/company/')[1].split('/')[0]
            kind=url.rsplit('/',1)[1].split('.')[0]
            final=.5 if kind=='qfq' else 2.
            if bad_second_axis and kind=='hfq':final=3.
            body=('var '+market+kind+'='+json.dumps({'total':3,'data':[{'d':'2026-11-04','f':1.},{'d':'2026-11-05','f':1.},{'d':'2026-11-06','f':final}]})).encode()
        return SimpleNamespace(content=body,status_code=200,raise_for_status=lambda:None)
    monkeypatch.setattr(requests,'get',get)
    index_calls=[]
    def fetch(start,end):
        index_calls.append((start.isoformat(),end.isoformat()))
        frame=pd.DataFrame({'date':pd.to_datetime(['2026-11-05','2026-11-06']),'close':[2000.,2010.]})
        frame.attrs.update(independent_history_source='offline native transport fixture',independent_history_attempts=[])
        return frame
    monkeypatch.setattr(index,'fetch_delivery_history',fetch)
    return calls,index_calls


def test_two_day_tail_reuses_native_query_and_factor_bodies(tmp_path,monkeypatch):
    from scripts.collect_member_daily_inputs import _collect_close_tail
    calls,index_calls=install_tail_transport(monkeypatch)
    result=_collect_close_tail(cp={'as_of_date':'2026-11-04'},members=[{'symbol':'600001'}],
        price_rows={'600001':{'previous_raw_date':'2026-11-04','previous_raw_close':100.}},
        days=['2026-11-05','2026-11-06'],out=tmp_path,workers=1)
    manifest=json.loads((tmp_path/'replay_manifest.json').read_bytes())
    assert [row['date'] for row in manifest['days']]==['2026-11-05','2026-11-06']
    first=json.loads((tmp_path/manifest['days'][0]['quotes']).read_bytes())
    assert first['retrospective_replay'] is True and first['rows'][0]['previous_date']=='2026-11-04'
    assert result['rows'][0]['previous_date']=='2026-11-05'
    assert result['rows'][0]['previous_raw_close']==100 and result['rows'][0]['raw_close']==50
    assert result['rows'][0]['close_adj']/result['rows'][0]['previous_close_adj']==1
    assert len(calls)==3 and index_calls==[('2026-11-05','2026-11-06')]
    assert json.loads((tmp_path/'hedge_by_day/2026-11-05.json').read_bytes())['close']==2000.
    assert json.loads((tmp_path/'hedge.json').read_bytes())['close']==2010.


def test_tail_factor_failure_retains_successful_prior_day(tmp_path,monkeypatch):
    from scripts.collect_member_daily_inputs import _collect_close_tail
    install_tail_transport(monkeypatch,bad_second_axis=True)
    result=_collect_close_tail(cp={'as_of_date':'2026-11-04'},members=[{'symbol':'600001'}],
        price_rows={'600001':{'previous_raw_date':'2026-11-04','previous_raw_close':100.}},
        days=['2026-11-05','2026-11-06'],out=tmp_path,workers=1)
    first=json.loads((tmp_path/'quotes_by_day/2026-11-05.json').read_bytes())
    assert len(first['rows'])==1 and first['errors']=={}
    assert result['rows']==[] and 'disagreement' in result['errors']['600001']
    assert json.loads((tmp_path/'hedge.json').read_bytes())['close']==2010.


def test_intraday_gap_keeps_observations_without_false_one_day_return(tmp_path,monkeypatch):
    import pandas as pd
    from types import SimpleNamespace
    from scripts import collect_member_daily_inputs as c
    monkeypatch.setattr(c,'collect_hedge',lambda *args:{'market_date':'2026-11-06','close':2010.})
    base=SimpleNamespace(fetch_member_realtime_quotes=lambda symbols:pd.DataFrame([{'code':'600001','rt_price':50.,'trade_date':'2026-11-06'}]))
    result=c._collect_intraday_gap(base,['600001'],{'as_of_date':'2026-11-04'},['2026-11-05','2026-11-06'],'2026-11-06',tmp_path)
    assert result['rows']==[] and result['observed_raw_quotes'][0]['rt_price']==50.
    assert '2026-11-05' in result['errors']['missing_sessions']
