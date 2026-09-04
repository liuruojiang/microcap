import json

import pytest

import microcap_top100_mom16_biweekly_live_v2_3 as v
from scripts import top100_delivery as delivery


def test_selected_parameters_and_identity():
    assert (v.LOOKBACK, v.HALFLIFE, v.R2_ENTRY_GATE) == (25, 2.5, 0.)
    assert (v.OVERHEAT_TRIGGER_THRESHOLD, v.OVERHEAT_RECOVERY_THRESHOLD) == (.26, .20)
    assert v.OVERHEAT_RECOVERY_RATIO == pytest.approx(.20 / .26)
    assert v.STRATEGY_REVISION == delivery.V23_STRATEGY_REVISION
    assert v.COSTED_NAV_CSV.name == delivery.COSTED['3']


@pytest.mark.parametrize('field,bad', [('strategy_revision', 'spread_nav_log_wls_lb25_vol10_overheat'),
    ('r2_entry_gate', '.08'), ('r2_gate_enabled', 'True'), ('overheat_recovery_threshold', '.195'),
    ('target_vol_enabled', 'True')])
def test_old_or_contradictory_final_identity_rejected(field, bad):
    row = dict(strategy_revision=v.STRATEGY_REVISION, target_vol_enabled='False',
               r2_gate_enabled='False', r2_entry_gate='0', overheat_trigger_threshold='.26',
               overheat_recovery_threshold='.20')
    assert delivery.plain_v23_identity(row)
    row[field] = bad
    assert not delivery.plain_v23_identity(row)


def test_promotion_requires_exact_evidence(tmp_path, monkeypatch):
    expected = dict(authorization='user_replace_existing_v2_3', source_sha256_lf='source',
                    unchanged_base_inputs={'proxy': 'digest'}, candidate_frame_sha256='candidate')
    monkeypatch.setattr(v, 'strategy_promotion_evidence', lambda *args: expected)
    report = tmp_path / 'approval.json'
    assert not v.strategy_promotion_matches(None, None, None, None)
    report.write_text(json.dumps(dict(expected, approved=True)))
    assert v.strategy_promotion_matches(report, None, None, None)
    for field in expected:
        changed = dict(expected, approved=True)
        changed[field] = 'different'
        report.write_text(json.dumps(changed))
        assert not v.strategy_promotion_matches(report, None, None, None)
