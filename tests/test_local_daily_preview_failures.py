"""Failure evidence and time limits; no strategy refresh, GitHub, or SMTP calls."""
import base64
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
HELPER = ROOT / 'research_reports/20261024_release_audit/delivery/prepare_local_daily.py'


@pytest.fixture
def helper():
    spec = importlib.util.spec_from_file_location('local_daily_preview', HELPER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def execute(helper, tmp_path, code, timeout=5):
    return helper.run_logged([sys.executable, '-B', '-u', '-c', code], 'attempt',
                             root=tmp_path, out=tmp_path, records=[], timeout=timeout)


def test_real_success_preserves_stdout_and_stderr(helper, tmp_path):
    result = execute(helper, tmp_path, 'import sys; print("result"); print("diagnostic", file=sys.stderr)')
    assert result == 'result\n'
    assert (tmp_path/'attempt.log').read_text(encoding='utf-8') == 'result\ndiagnostic\n'
    record, = json.loads((tmp_path/'commands.json').read_text(encoding='utf-8'))
    assert record['status'] == 'success' and record['exit_code'] == 0


def test_real_nonzero_records_failure_before_raising(helper, tmp_path):
    with pytest.raises(AssertionError, match='attempt failed'):
        execute(helper, tmp_path, 'import sys; print("before failure"); sys.exit(7)')
    record, = json.loads((tmp_path/'commands.json').read_text(encoding='utf-8'))
    assert record['status'] == 'failed' and record['exit_code'] == 7
    assert 'before failure' in (tmp_path/'attempt.log').read_text(encoding='utf-8')


def test_real_timeout_keeps_partial_output(helper, tmp_path):
    with pytest.raises(subprocess.TimeoutExpired):
        execute(helper, tmp_path, 'import time,sys; print("partial output",flush=True); print("partial error",file=sys.stderr,flush=True); time.sleep(30)', timeout=2)
    record, = json.loads((tmp_path/'commands.json').read_text(encoding='utf-8'))
    assert record['status'] == 'timeout' and record['exit_code'] is None
    assert record['timeout_seconds'] == 2
    assert (tmp_path/'attempt.log').read_text(encoding='utf-8') == 'partial output\npartial error\n'


def test_launch_failure_keeps_attempt(helper, tmp_path):
    with pytest.raises(OSError):
        helper.run_logged([str(tmp_path/'missing_executable')], 'launch', root=tmp_path,
                          out=tmp_path, records=[], timeout=5)
    record, = json.loads((tmp_path/'commands.json').read_text(encoding='utf-8'))
    assert record['status'] == 'launch_failed' and record['exit_code'] is None
    assert (tmp_path/'launch.log').read_text(encoding='utf-8')


@pytest.mark.parametrize('partial', [None, b'\xe8\xb6\x85\xe6\x97\xb6', '超时'])
def test_timeout_supports_bytes_text_and_empty_output(helper, tmp_path, monkeypatch, partial):
    def fail(*args, **kwargs):
        raise subprocess.TimeoutExpired(args[0], kwargs['timeout'], output=partial)
    monkeypatch.setattr(helper.subprocess, 'run', fail)
    with pytest.raises(subprocess.TimeoutExpired):
        execute(helper, tmp_path, '')
    assert (tmp_path/'attempt.log').read_text(encoding='utf-8') == ('' if partial is None else '超时')


@pytest.mark.parametrize('stage', ['automation_main', 'consumer_fetch', 'whole_check_before'])
def test_main_remote_reads_are_bounded_and_fail_closed(helper, tmp_path, monkeypatch, stage):
    root = tmp_path/'root'; out = tmp_path/'report'
    (root/'outputs').mkdir(parents=True)
    (root/'outputs/microcap_top100_mom16_biweekly_live_v2_0_base_frozen_tail_authority.json').write_text(json.dumps({
        'data_tail_revision':helper.DATA_TAIL_REVISION, 'lineage_revision':'2026-10-24-600241-st-category'}))
    rel = Path('research_reports/20261024_release_audit/tail_correction_review/approval_package.json')
    (root/rel).parent.mkdir(parents=True)
    shutil.copy2(ROOT/rel, root/rel)
    auto = '48403ff025c158466f197d6b53a10ac7510d8df7'
    calls = []
    def run(command, **kwargs):
        label = ('automation_main' if '/commits/main' in command[2] else 'consumer_fetch') if command[0] == 'gh' else 'whole_check_before'
        calls.append((label, kwargs['timeout'], kwargs['env']))
        if label == stage:
            raise subprocess.TimeoutExpired(command, kwargs['timeout'], output=b'partial response', stderr=b'failure detail')
        stdout = auto+'\n' if label == 'automation_main' else json.dumps({'sha':'source-blob', 'content':base64.b64encode(b'# isolated, never executed').decode()})
        return subprocess.CompletedProcess(command, 0, stdout, '')
    monkeypatch.setattr(helper.subprocess, 'run', run)
    monkeypatch.setattr(sys, 'argv', [str(HELPER), '--root', str(root), '--out-dir', str(out),
                                    '--strategy-sha', '5'*40, '--automation-sha', auto])
    with pytest.raises(subprocess.TimeoutExpired):
        helper.main()
    assert all(timeout == 60 and env is None for name, timeout, env in calls if name in ('automation_main','consumer_fetch'))
    if stage == 'whole_check_before':
        assert calls[-1][1] == 180 and calls[-1][2]['PYTHONDONTWRITEBYTECODE'] == '1'
    records = json.loads((out/'commands.json').read_text(encoding='utf-8'))
    assert len(records) == len(calls) and records[-1]['status'] == 'timeout'
    assert (out/(stage+'.log')).read_text(encoding='utf-8') == 'partial responsefailure detail'
    assert not (out/'local_daily_readback.json').exists() and not (out/'preview').exists()
