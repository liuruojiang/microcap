"""A finite restart-safe cooldown, never a content or TLS bypass."""
from datetime import datetime, timedelta, timezone
import json

import pytest
from scripts import share_change_source as source
from scripts import share_unchanged_tail as native


@pytest.fixture
def exhausted(tmp_path):
    folder=tmp_path/'providers'/'fixture';folder.mkdir(parents=True)
    clock=datetime(2026,11,9,8,tzinfo=timezone.utc)
    params=native.quote_params('002566')
    for number in range(1,5):
        (folder/f'quote_attempt{number}.transport.json').write_text(json.dumps(dict(
            url=native.QUOTE_URL,params=params,http_start=(clock-timedelta(minutes=5-number)).isoformat(),
            http_complete=clock.isoformat(),error_type='ReadTimeout',status='transport_failure_no_response')))
    (folder/'quote.exhausted.json').write_text(json.dumps(dict(error='SHARE_REQUEST_RETRY_EXHAUSTED: quote',recorded_at=clock.isoformat())))
    return tmp_path,folder,clock,params


def recover(case,monkeypatch,minutes=31):
    root,folder,clock,params=case
    return source._dated_public_observation(directory=folder,source=root,url=native.QUOTE_URL,
        params=params,purpose='quote',end='2026-11-06',now=clock+timedelta(minutes=minutes))


def test_one_cooled_recovery_preserves_failures_and_cannot_reset_after_restart(exhausted,monkeypatch):
    root,folder,_,_=exhausted
    before={p:p.read_bytes() for p in folder.iterdir()};calls=[]
    def timeout(**kwargs):
        calls.append(kwargs['directory'])
        raise RuntimeError('SHARE_REQUEST_RETRY_EXHAUSTED: quote')
    monkeypatch.setattr(native,'_save_public_observation',timeout)
    with pytest.raises(ValueError,match='cooling'):recover(exhausted,monkeypatch,29)
    assert calls==[]
    with pytest.raises(RuntimeError,match='EXHAUSTED'):recover(exhausted,monkeypatch)
    assert len(calls)==1 and calls[0]!=folder
    assert all(p.read_bytes()==raw for p,raw in before.items())
    with pytest.raises(FileExistsError):recover(exhausted,monkeypatch,120)
    assert len(calls)==1
    marker=json.loads((calls[0]/'started.json').read_bytes())
    assert marker['max_attempts']==4 and len(marker['initial_attempts'])==4


@pytest.mark.parametrize('fault',['http','tls','params','future_clock','missing'])
def test_recovery_cannot_consume_unproved_failure_evidence(exhausted,monkeypatch,fault):
    _,folder,_,_=exhausted
    p=folder/'quote_attempt1.transport.json';r=json.loads(p.read_bytes())
    if fault=='missing':p.unlink()
    else:
        if fault=='http':r['status']='http_error'
        elif fault=='tls':r['error_type']='SSLError'
        elif fault=='params':r['params']['secid']='0.000001'
        else:r['http_complete']='2099-01-01T00:00:00+00:00'
        p.write_text(json.dumps(r))
    monkeypatch.setattr(native,'_save_public_observation',lambda **kwargs:pytest.fail('network must remain blocked'))
    with pytest.raises((ValueError,FileNotFoundError)):recover(exhausted,monkeypatch)
