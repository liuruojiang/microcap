"""Resume keeps the original authenticated snapshot; no history or clock waiver."""
from datetime import date, datetime
import json

import pytest

from scripts import realtime_state_bundle as state, st_notice_refresh as notice


def setup_proof(root, **changes):
    proof = dict(target='2026-11-02', required_asof='2026-11-02T07:05:00+00:00',
                 completed_at='2026-11-02T07:10:00+00:00', authority=['archive.json', 'a'*64])
    proof.update(changes)
    path = root / notice.FRESHNESS
    path.parent.mkdir(parents=True); path.write_text(json.dumps(proof))
    return path


def test_resume_revalidates_bound_sources_without_rewriting_observation(tmp_path, monkeypatch):
    path = setup_proof(tmp_path); before = path.read_bytes(); calls = []
    monkeypatch.setattr(notice, 'freshness_source_files', lambda root: calls.append(root) or {'file': 'hash'})
    result = state._reuse_same_day_notice_proof(tmp_path, date(2026, 11, 2), datetime.fromisoformat('2026-11-02T09:00:00+00:00'))
    assert result['reused_same_day_close_proof'] and result['authority_changed'] is False
    assert calls == [tmp_path] and path.read_bytes() == before


@pytest.mark.parametrize('changes,now', [
    ({'target': '2026-10-30'}, '2026-11-02T09:00:00+00:00'),
    ({'required_asof': '2026-11-02T06:59:59+00:00'}, '2026-11-02T09:00:00+00:00'),
    ({'completed_at': '2026-11-02T10:00:00+00:00'}, '2026-11-02T09:00:00+00:00'),
    ({}, '2026-11-03T09:00:00+00:00'),
    ({}, '2026-11-02T06:59:59+00:00'),
])
def test_old_preclose_future_and_wrong_day_proofs_do_not_qualify(tmp_path, monkeypatch, changes, now):
    setup_proof(tmp_path, **changes)
    monkeypatch.setattr(notice, 'freshness_source_files', lambda root: pytest.fail('not eligible'))
    assert state._reuse_same_day_notice_proof(tmp_path, date(2026, 11, 2), datetime.fromisoformat(now)) is None


def test_altered_bound_evidence_blocks_resume(tmp_path, monkeypatch):
    setup_proof(tmp_path)
    def altered(root): raise ValueError('source hash changed')
    monkeypatch.setattr(notice, 'freshness_source_files', altered)
    with pytest.raises(ValueError, match='source hash changed'):
        state._reuse_same_day_notice_proof(tmp_path, date(2026, 11, 2), datetime.fromisoformat('2026-11-02T09:00:00+00:00'))
