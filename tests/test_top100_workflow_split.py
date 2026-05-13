from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_realtime_workflow_consumes_state_refresh_bundle_only() -> None:
    workflow = (ROOT / ".github/workflows/top100_realtime_signals.yml").read_text(encoding="utf-8")

    assert '--workflow "Top100 State Refresh"' in workflow
    assert '--workflow "Top100 Realtime Signals"' not in workflow
    assert "actions/cache" not in workflow
    assert "Pack validated realtime state bundle" not in workflow
    assert "Upload validated realtime state bundle" not in workflow
    assert "Fail job when realtime signal script failed" not in workflow
    assert "Script exit code:" in workflow


def test_realtime_workflow_reports_only_v1_6() -> None:
    workflow = (ROOT / ".github/workflows/top100_realtime_signals.yml").read_text(encoding="utf-8")

    assert "Run v1.6 realtime signal" in workflow
    assert "run_top100_v1_6_v1_8_realtime_signals.py --versions v1.6" in workflow
    assert "Top100 v1.6 realtime signal" in workflow
    assert "microcap_top100_mom16_biweekly_live_v1_4.py" not in workflow
    assert "v1.4" not in workflow
    assert "v1.8" not in workflow


def test_state_refresh_workflow_owns_state_bundle_production() -> None:
    workflow = (ROOT / ".github/workflows/top100_state_refresh.yml").read_text(encoding="utf-8")

    assert "name: Top100 State Refresh" in workflow
    assert "python scripts/realtime_state_bundle.py refresh" in workflow
    assert "Previous state bundle ${run_id} is incomplete under current validation" in workflow
    assert "Pack validated realtime state bundle" in workflow
    assert "Upload validated realtime state bundle" in workflow
    assert "top100-realtime-state-bundle" in workflow


def test_state_refresh_cache_does_not_restore_state_bundle_outputs() -> None:
    workflow = (ROOT / ".github/workflows/top100_state_refresh.yml").read_text(encoding="utf-8")
    match = re.search(
        r"- name: Restore generated strategy cache(?P<body>.*?)- name: Restore previous state bundle",
        workflow,
        flags=re.S,
    )

    assert match is not None
    cache_step = match.group("body")
    assert "outputs" not in cache_step
    assert ".microcap_index_cache" not in cache_step
    assert ".microcap_ohlc_cache" in cache_step
