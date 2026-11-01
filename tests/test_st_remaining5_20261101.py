"""Real final-five body evidence: cancellation, continuance and actual decisions."""
import json
from pathlib import Path
import pytest
from scripts.st_notice_evidence import load_verified_events, StNoticeEvidenceError

ROOT = Path(__file__).resolve().parents[1]

@pytest.mark.parametrize('aid,actions', [
    ('AN202201141540405788', {}),
    ('AN202311131609933741', {'delisting': True, 'other': True}),
    ('AN201911121370672979', {'other': False}),
    ('AN202604241821558018', {'other': True}),
    ('AN202304141585466764', {}),
    ('AN202304141585466770', {}),
])
def test_verified_real_body_decision_and_wrong_issuer(aid, actions):
    payload = json.loads((ROOT / 'data/st_notice_evidence/registry.json').read_text(encoding='utf8'))
    event = next(e for e in payload['events'] if e['announcement_id'] == aid)
    # Supply the complete same-day issuer set required by the real binding contract.
    same_day = [e for e in payload['events'] if e['symbol'] == event['symbol'] and e['notice_date'] == event['notice_date']]
    rows = [dict(issuer_symbol=e['symbol'], **{k: e[k] for k in ('announcement_id', 'notice_date', 'title', 'source_url', 'source_provider')}) for e in same_day]
    actual = load_verified_events(ROOT, event['symbol'], event['notice_date'], event['notice_date'], rows)
    assert actual[aid]['risk_actions'] == actions
    next(r for r in rows if r['announcement_id'] == aid)['issuer_symbol'] = '000001'
    with pytest.raises(StNoticeEvidenceError, match='identity mismatch'):
        load_verified_events(ROOT, event['symbol'], event['notice_date'], event['notice_date'], rows)
