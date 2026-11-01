"""000509's actual application advisory must not end the ST interval."""
import json
from pathlib import Path
import pandas as pd
import pytest
import analyze_top100_rebalance_frequency as standalone
import microcap_top100_mom16_biweekly_live_v2_0 as embedded
from scripts.st_notice_evidence import StNoticeEvidenceError, load_verified_events

ROOT = Path(__file__).resolve().parents[1]

@pytest.mark.parametrize('module', [standalone, embedded.freq_mod])
def test_real_000980_partial_reason_removal_retains_delisting(module):
    title = '公司关于撤销对公司股票交易因重整而被实施退市风险警示情形但仍处于退市风险警示状态的公告'
    assert module.classify_st_notice_risk_actions(title) == {'delisting': True}

@pytest.mark.parametrize('module', [standalone, embedded.freq_mod])
def test_real_advisory_waits_for_body_and_approval(module):
    registry = json.loads((ROOT / 'data/st_notice_evidence/registry.json').read_text(encoding='utf-8'))
    ids = ['AN201404290005601263', 'AN201405150005819522', '63951214']
    events = [next(e for e in registry['events'] if e['announcement_id'] == aid) for aid in ids]
    rows = [{k: e[k] for k in ['announcement_id', 'notice_date', 'title', 'source_url']} | {'issuer_symbol': '000509', 'source_provider': e.get('source_provider', 'cninfo')} for e in events]
    with pytest.raises(StNoticeEvidenceError):
        module.classify_st_notice_risk_actions(rows[0]['title'])
    verified = load_verified_events(ROOT, '000509', '2014-01-01', '2014-05-30', rows)
    assert verified[ids[0]]['risk_actions'] == {}
    # The initial state and dates are a controlled test boundary; notices are real.
    anchor = dict(date='2013-12-31', risk_state={'delisting': True}, name='*ST华塑')
    intervals = module.build_st_intervals_from_notices(pd.Timestamp('2014-01-01'), pd.Timestamp('2014-05-30'), pd.DataFrame(rows), verified_events=verified, initial_name_anchor=anchor)
    assert [(i['start'], i['end']) for i in intervals] == [('2014-01-01', '2014-05-16')]
