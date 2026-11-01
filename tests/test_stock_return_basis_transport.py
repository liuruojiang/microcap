import datetime as dt
import json
from pathlib import Path
import shutil
import hashlib

import pandas as pd
import pytest

from scripts.stock_return_basis_store import load_or_build, _extended
from scripts.stock_return_pipeline import verify_return_basis_transport, ReturnBasisError
import scripts.stock_return_pipeline as pipeline

F=Path(__file__).parent/"fixtures/stock_return_basis_transport_real/000001"


def prepare(root,monkeypatch):
    cache=root/".microcap_index_cache";raw=cache/"prices_raw/000001.csv";raw.parent.mkdir(parents=True)
    shutil.copy2(F/"raw.csv",raw)
    folder=cache/"return_basis/sources/000001";shutil.copytree(F,folder)
    dates=pd.read_csv(raw).date.tolist();sessions=[dt.date.fromisoformat(s) for s in dates]
    monkeypatch.setattr(pipeline,"sessions_for_day",lambda _:tuple(sessions))
    pd.DataFrame([dict(symbol="000001",list_date="1991-04-03",delist_date=None)]).to_csv(cache/"security_master.csv",index=False)
    status=cache/'trade_status';status.mkdir();source=status/'query.json';shutil.copy2(F/'trade_status_original_query.json',source)
    (status/'000001.json').write_text(json.dumps(dict(schema_version=1,symbol='000001',raw_price_sha256=hashlib.sha256(raw.read_bytes()).hexdigest(),lifecycle=dict(list_date='1991-04-03',delist_date=''),provider_queries=[dict(path='query.json',sha256=hashlib.sha256(source.read_bytes()).hexdigest())],reviewed_suspensions=[])))
    load_or_build(basis_root=cache/"return_basis",symbol="000001",raw_path=raw,as_of=dates[-1],sessions=dates,list_date="1991-04-03")
    return dates[-1],cache


def snapshot(root):
    root=_extended(root)
    return {str(p.relative_to(root)):p.read_bytes() for p in root.rglob('*') if p.is_file()}


def test_real_transport_reconstruction_is_read_only(tmp_path,monkeypatch):
    anchor,cache=prepare(tmp_path,monkeypatch);before=snapshot(tmp_path)
    report=verify_return_basis_transport(tmp_path,anchor,["000001"])
    assert report["verified_count"]==1 and report["read_only"]
    assert snapshot(tmp_path)==before


@pytest.mark.parametrize('fault',['symbols','missing_session','empty_turnover','new_execution','new_effective','unexecuted_first_return','return_before_execution','stale_effective_authority'])
def test_consumption_proof_requires_independent_complete_authorities(tmp_path,monkeypatch,fault):
    symbols=[str(i).zfill(6) for i in range(1,101)]
    monkeypatch.setattr(pipeline,'sessions_for_day',lambda _:tuple(dt.date.fromisoformat(s) for s in ['2026-10-22','2026-10-23','2026-10-26','2026-10-27','2026-10-28']))
    out=tmp_path/'outputs';out.mkdir();prefix=out/'microcap_top100_mom16_biweekly_live_v2_0_base_proxy'
    pd.DataFrame(dict(as_of_date=['2026-10-23']*100,rank=range(1,101),symbol=symbols)).to_csv(str(prefix)+'_effective_members.csv',index=False)
    pd.DataFrame(dict(rebalance_date=['2026-10-23']*100,rank=range(1,101),symbol=symbols)).to_csv(str(prefix)+'_members.csv',index=False)
    # Real strategy convention: close execution/effectiveness on one session,
    # followed by the first return on the next exchange session.
    pd.DataFrame([dict(execution_timing='close',execution_date='2026-10-23',effective_date='2026-10-23',return_start_date='2026-10-26')]).to_csv(str(prefix)+'_turnover.csv',index=False)
    pd.DataFrame(dict(date=['2026-10-23','2026-10-26','2026-10-27'])).to_csv(out/'wind_microcap_top_100_biweekly_thursday_16y_cached.csv',index=False)
    tail=dict(start_exclusive='2026-10-23',end='2026-10-27',symbols=symbols)
    for name,key in [('effective_members','effective_members_sha256'),('members','proxy_members_sha256'),('turnover','proxy_turnover_sha256')]:tail[key]=hashlib.sha256(Path(str(prefix)+'_'+name+'.csv').read_bytes()).hexdigest()
    meta=dict(return_basis_consumption=dict(revision='stock_return_consumption_v1',full_rebuild_anchor='2026-10-23',tail_extensions=[tail]))
    p=Path(str(prefix)+'_meta.json');p.write_text(json.dumps(meta))
    full,live,_=pipeline._consumption_requirements(tmp_path,'2026-10-27',set(symbols+['999999']))
    assert full=='2026-10-23' and live==set(symbols)
    if fault=='symbols':tail['symbols'][0]='999999'
    elif fault=='missing_session':pd.DataFrame(dict(date=['2026-10-23','2026-10-27'])).to_csv(out/'wind_microcap_top_100_biweekly_thursday_16y_cached.csv',index=False)
    elif fault=='stale_effective_authority':
        authority=Path(str(prefix)+'_effective_members.csv');data=pd.read_csv(authority,dtype={'symbol':str});data['as_of_date']='2026-10-22';data.to_csv(authority,index=False)
        tail['effective_members_sha256']=hashlib.sha256(authority.read_bytes()).hexdigest()
    else:
        turnover=Path(str(prefix)+'_turnover.csv')
        data=pd.DataFrame(columns=['execution_date','effective_date','return_start_date']) if fault=='empty_turnover' else pd.DataFrame([dict(execution_date='2026-10-26',effective_date='2026-10-26',return_start_date='2026-10-27')])
        if fault=='new_effective':data['execution_date']='2026-10-23'
        if fault=='unexecuted_first_return':
            data['execution_date']='2026-10-28';data['effective_date']='2026-10-28'
        if fault=='return_before_execution':
            data['execution_date']='2026-10-23';data['effective_date']='2026-10-23';data['return_start_date']='2026-10-22'
        data.to_csv(turnover,index=False);tail['proxy_turnover_sha256']=hashlib.sha256(turnover.read_bytes()).hexdigest()
    p.write_text(json.dumps(meta))
    with pytest.raises(ReturnBasisError):pipeline._consumption_requirements(tmp_path,'2026-10-27',set(symbols+['999999']))


@pytest.mark.parametrize("change",["raw","source","candidate","master","timestamp","missing_symbol","missing_status_source","tampered_status_source"])
def test_transport_fails_closed_without_repairs(tmp_path,monkeypatch,change):
    anchor,cache=prepare(tmp_path,monkeypatch)
    paths=dict(raw=cache/"prices_raw/000001.csv",source=cache/"return_basis/sources/000001/qfq.js",candidate=cache/"return_basis/prices_adj/000001.csv",master=cache/"security_master.csv")
    if change in paths:paths[change].write_bytes(paths[change].read_bytes()+b"\n")
    if change=="master":
        p=paths["master"];p.write_text(p.read_text().replace("1991-04-03","1991-04-04"))
    if change=="timestamp":
        p=cache/"return_basis/sources/000001/qfq.json";r=json.loads(p.read_text());r['retrieved_at_utc']='2010-01-01T00:00:00+00:00';p.write_text(json.dumps(r))
    if change=='missing_status_source':(cache/'trade_status/query.json').unlink()
    if change=='tampered_status_source':
        p=cache/'trade_status/query.json';p.write_bytes(p.read_bytes()+b' ')
    before=snapshot(tmp_path)
    symbols=["000001","000559"] if change=="missing_symbol" else ["000001"]
    with pytest.raises(ReturnBasisError):verify_return_basis_transport(tmp_path,anchor,symbols)
    assert snapshot(tmp_path)==before
