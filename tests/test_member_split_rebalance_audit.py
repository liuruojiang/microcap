"""Scoped adversarial fixtures; never current market or production evidence."""
from datetime import date
import importlib.util
import json
from pathlib import Path
import pytest
from scripts import member_period_handoff as handoff

_spec=importlib.util.spec_from_file_location('rebalance_audit_fixture',Path(__file__).with_name('test_member_period_handoff.py'))
fixture=importlib.util.module_from_spec(_spec);_spec.loader.exec_module(fixture)

@pytest.fixture
def same_close(tmp_path,monkeypatch):
    from scripts import exchange_calendar
    monkeypatch.setattr(exchange_calendar,'sessions_for_day',lambda _:tuple(date.fromisoformat(x) for x in ('2026-10-28','2026-10-29','2026-10-30')))
    old=fixture.package(tmp_path,'old',history=b'date,microcap,hedge\n2026-10-29,1000.0,2000.0\n')
    new=fixture.package(tmp_path,'new',history=b'date,microcap,hedge\n2026-10-29,1e3,2000\n')
    return old,new

def test_equivalent_csv_number_format_does_not_block_period_handoff(same_close,tmp_path):
    old,new=same_close
    handoff.handoff(old,new,'2026-10-29','2026-10-30',tmp_path/'adopted')
    assert handoff.read_bound(handoff.verify(tmp_path/'adopted/checkpoint.json'),'history')==handoff.read_bound(handoff.verify(old),'history')

def test_real_earned_return_delta_remains_rejected(same_close,tmp_path):
    old,new=same_close
    cp=json.loads(new.read_bytes());path=new.parent/cp['history']['path'];raw=b'date,microcap,hedge\n2026-10-29,1000.000000000000000001,2000\n';path.write_bytes(raw);cp['history']['sha256']=handoff.digest(raw);new.write_bytes(handoff.encode(cp))
    with pytest.raises(ValueError,match='earned'):handoff.handoff(old,new,'2026-10-29','2026-10-30',tmp_path/'bad')

def test_same_close_cannot_rewrite_existing_turnover_cost_event(same_close,tmp_path):
    old,new=same_close
    for path,rate in ((old,'0.001'),(new,'0.009')):
        cp=json.loads(path.read_bytes());raw=f'rebalance_date,execution_timing,two_side_cost_rate\n2026-10-15,close,{rate}\n'.encode();(path.parent/'turnover.csv').write_bytes(raw);cp['turnover']['sha256']=handoff.digest(raw)
        # Keep history byte-identical so the turnover defect is independently tested.
        if path==new:
            history=handoff.read_bound(handoff.verify(old),'history');(path.parent/'history.csv').write_bytes(history);cp['history']['sha256']=handoff.digest(history)
        path.write_bytes(handoff.encode(cp))
    with pytest.raises(ValueError,match='turnover|cost|event'):handoff.handoff(old,new,'2026-10-29','2026-10-30',tmp_path/'bad_turnover')

def test_native_final_close_event_has_no_return_row_and_bootstrap_derives_only_its_clock(tmp_path,monkeypatch):
    import numpy as np
    import pandas as pd
    from microcap_top100_mom16_biweekly_live_v2_0 import freq_mod
    dates=pd.DatetimeIndex(['2026-10-28','2026-10-29'])
    old=[f'{i:06d}' for i in range(1,101)];new=[f'{i:06d}' for i in range(2,102)]
    names=sorted(set(old+new));returns=pd.DataFrame(0.,index=dates,columns=names)
    allowed=pd.DataFrame(True,index=dates,columns=names)
    _,turnover,_=freq_mod.simulate_rebalance_path(dates,returns,{dates[-1]:new},pd.DatetimeIndex([dates[-1]]),allowed,allowed,.003,execution_timing='close',initial_members=old)
    assert pd.isna(turnover.iloc[-1]['return_start_date'])
    root=fixture.formal_root(tmp_path)
    # Reuse the original formal-source fixture, with this authentic final-row
    # shape. No current source, production state or network is involved.
    output=root/'outputs';event=output/(handoff.PREFIX+'_proxy_turnover.csv')
    event.write_text('rebalance_date,execution_timing,execution_date,effective_date,return_start_date\n2026-10-29,close,2026-10-29,2026-10-29,\n',encoding='utf8')
    manifest_path=output/'top100_delivery_manifest.json';manifest=json.loads(manifest_path.read_bytes())
    manifest['inputs'][event.relative_to(root).as_posix()]=handoff.digest(event.read_bytes());manifest_path.write_bytes(handoff.encode(manifest))
    monkeypatch.setattr(handoff,'calendar_links',lambda d:('2026-10-30','2026-11-12') if d=='2026-10-29' else ('2026-11-06','2026-11-12'))
    class Result:stdout='a'*40
    monkeypatch.setattr(handoff.subprocess,'run',lambda *a,**k:Result())
    source=event.read_bytes();handoff.bootstrap(root,tmp_path/'export')
    period=json.loads(handoff.read_bound(handoff.verify(tmp_path/'export/checkpoint.json'),'period'))
    assert period['effective_from']=='2026-10-30'
    assert period['rebalance_events'][0]['return_start_date']==''
    assert period['rebalance_events'][0]['derived_return_start_date']=='2026-10-30'
    assert event.read_bytes()==source
