"""Scoped split audit: offline counterexamples, never market evidence."""
from datetime import datetime
from pathlib import Path
import importlib.util
import json

import pandas as pd
import pytest
from scripts import member_daily_signal as daily

_spec = importlib.util.spec_from_file_location('_split_native_fixture', Path(__file__).with_name('test_member_daily_signal.py'))
_fixture = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_fixture)
bundle = _fixture.bundle


def rebind(root, key, name):
    cp = json.loads((root/'checkpoint.json').read_bytes())
    cp[key]['sha256'] = daily.digest((root/name).read_bytes())
    daily.write_json(root/'checkpoint.json', cp)


def test_missing_interior_session_cannot_be_consumed_as_complete_history(bundle, tmp_path):
    root, history, quotes, hedge, day, sessions = bundle
    # Simulate a hash-consistent producer that accidentally drops one recent
    # session. A verified tail must retain the calendar's actual observations.
    damaged = history.drop(history.index[-10])
    damaged.rename_axis('date').to_csv(root/'history.csv')
    rebind(root, 'history', 'history.csv')
    result = daily.build(Path(__file__).parents[1], root/'checkpoint.json', quotes, hedge,
                         day, 'close_confirmed', tmp_path/'gap',
                         now=datetime.fromisoformat(day+'T16:01:00+08:00'), calendar_sessions=sessions)
    assert result['status'] == 'PARTIAL', 'A missing confirmed historical session changed the input windows yet remained verified'
    assert result['checkpoint'] is None


def test_replay_partial_v25_does_not_publish_shared_checkpoint(bundle, tmp_path):
    manifest_path, day, sessions = _fixture.replay_inputs(bundle, tmp_path)
    manifest=json.loads(manifest_path.read_bytes())
    manifest['days'][0].pop('hedge')
    daily.write_json(manifest_path,manifest)
    result=daily.replay_from_manifest(Path(__file__).parents[1],bundle[0]/'checkpoint.json',manifest_path,
           day,'close_confirmed',tmp_path/'component',now=datetime.fromisoformat(day+'T16:01:00+08:00'),calendar_sessions=sessions)
    assert result['versions'][2]['signal'] is not None
    assert result['checkpoint'] is None
    from scripts.member_period_handoff import verify
    with pytest.raises(ValueError,match='Unsupported'):
        verify(tmp_path/'component'/result['component_observation'])


def test_replay_missing_latest_payload_still_preserves_earlier_receipts(bundle,tmp_path):
    manifest_path,day,sessions=_fixture.replay_inputs(bundle,tmp_path)
    # A source-stage failure may save an error response without the rows key.
    # Earlier real observations and today's explicit source error must remain
    # reportable rather than turning the entire replay into a CLI exception.
    latest=manifest_path.parent/'q2.json'
    daily.write_json(latest,dict(market_date=day,collected_at=day+'T16:00:00+08:00',close_confirmed=True,
                               errors={'source':'offline source timeout'}))
    result=daily.replay_from_manifest(Path(__file__).parents[1],bundle[0]/'checkpoint.json',manifest_path,
           day,'close_confirmed',tmp_path/'latest_partial',now=datetime.fromisoformat(day+'T16:01:00+08:00'),calendar_sessions=sessions)
    assert result['status']=='PARTIAL' and result['replay']['observations']


def test_daily_append_keeps_confirmed_history_bytes_exact(bundle,tmp_path):
    root,history,quotes,hedge,day,sessions=bundle
    original=(root/'history.csv').read_bytes()
    result=daily.build(Path(__file__).parents[1],root/'checkpoint.json',quotes,hedge,
          day,'close_confirmed',tmp_path/'appended',now=datetime.fromisoformat(day+'T16:01:00+08:00'),calendar_sessions=sessions)
    assert result['status']=='OK'
    assert (tmp_path/'appended/history.csv').read_bytes().startswith(original)
