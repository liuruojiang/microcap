from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_realtime_workflow_consumes_state_refresh_bundle_only() -> None:
    workflow = (ROOT / ".github/workflows/top100_realtime_signals.yml").read_text(encoding="utf-8")

    assert '--workflow "Top100 State Refresh"' in workflow
    assert '--workflow "Top100 Realtime Signals"' not in workflow
    assert "actions/cache" not in workflow
    assert "Pack validated realtime state bundle" not in workflow
    assert "Upload validated realtime state bundle" not in workflow


def test_state_refresh_workflow_owns_state_bundle_production() -> None:
    workflow = (ROOT / ".github/workflows/top100_state_refresh.yml").read_text(encoding="utf-8")

    assert "name: Top100 State Refresh" in workflow
    assert "python scripts/realtime_state_bundle.py refresh" in workflow
    assert "Previous state bundle ${run_id} is incomplete under current validation" in workflow
    assert "Pack validated realtime state bundle" in workflow
    assert "Upload validated realtime state bundle" in workflow
    assert "top100-realtime-state-bundle" in workflow
