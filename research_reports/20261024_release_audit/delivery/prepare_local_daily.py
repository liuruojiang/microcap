"""Generate a no-send local close-confirmed preview after formal activation.

Run only after the coordinator has published and verified both exact commits.
The normal group refresh reseals freshness summaries after official queries.
No migration, seed adoption, SMTP, or delivery-marker operation exists.
"""
from __future__ import annotations
import argparse
import base64
import csv
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import shutil
from datetime import datetime
from zoneinfo import ZoneInfo

APPROVAL_PACKAGE_SHA = '0e79f12ebcb3215c203f29f0627ccda356356a22bf02d87d08be95a603c906f9'
DATA_TAIL_REVISION = '2026-10-24-complete-20261023-member-prices'
AUTO_REPOSITORY = 'liuruojiang/codex-daily-automation-probe'
CONSUMER_REL = 'scripts/build_microcap_realtime_digest.py'
REMOTE_QUERY_TIMEOUT = 60

def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()

def read_json(path: Path):
    return json.loads(path.read_text(encoding='utf-8-sig'))

def only_row(path: Path) -> dict:
    with path.open(encoding='utf-8-sig', newline='') as stream:
        rows = list(csv.DictReader(stream))
    assert len(rows) == 1, f'Final signal must have exactly one row: {path}'
    assert None not in rows[0], f'Final CSV width mismatch: {path}'
    return rows[0]

def run_logged(command: list[str], label: str, *, root: Path, out: Path,
               records: list[dict], timeout: int, env: dict | None = None) -> str:
    """Keep failed-attempt evidence before propagating an execution failure."""
    log = out / f'{label}.log'
    record = {'command': command, 'cwd': str(root), 'exit_code': None,
              'timeout_seconds': timeout, 'log': str(log)}

    def persist(stdout, stderr) -> None:
        def text(value):
            return value.decode('utf-8', errors='replace') if isinstance(value, bytes) else (value or '')
        log.write_text(text(stdout) + text(stderr), encoding='utf-8')
        records.append(record)
        (out/'commands.json').write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding='utf-8')

    try:
        process = subprocess.run(command, cwd=root, env=env, capture_output=True,
                                 text=True, encoding='utf-8', timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        record.update(status='timeout', error_type=type(exc).__name__)
        persist(exc.stdout, exc.stderr)
        raise
    except OSError as exc:
        record.update(status='launch_failed', error_type=type(exc).__name__)
        persist('', str(exc))
        raise
    record.update(exit_code=process.returncode,
                  status='success' if process.returncode == 0 else 'failed')
    persist(process.stdout, process.stderr)
    assert process.returncode == 0, f'{label} failed: inspect its log; do not render a partial daily report'
    return process.stdout

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--out-dir', type=Path, required=True)
    parser.add_argument('--strategy-sha', required=True)
    parser.add_argument('--automation-sha', required=True)
    parser.add_argument('--reuse-verified-signal-logs', type=Path)
    args = parser.parse_args()
    for value in (args.strategy_sha, args.automation_sha):
        assert re.fullmatch('[0-9a-f]{40}', value), 'Supply a verified immutable commit, not a placeholder'
    root = args.root.resolve(); out = args.out_dir.resolve()
    assert not out.exists(), 'Use a new evidence directory; never overwrite earlier evidence'
    out.mkdir(parents=True)
    authority = read_json(root/'outputs/microcap_top100_mom16_biweekly_live_v2_0_base_frozen_tail_authority.json')
    assert authority['data_tail_revision'] == DATA_TAIL_REVISION, 'Expected approved tail revision is not active'
    assert authority['lineage_revision'] == '2026-10-24-600241-st-category'
    package = root/'research_reports/20261024_release_audit/tail_correction_review/approval_package.json'
    assert sha(package) == APPROVAL_PACKAGE_SHA, 'Preserve the exact reviewed approval package'

    command_records = []
    # GitHub retains the configured credential environment. Each read is bounded
    # and uses the same failure evidence contract as the strategy subprocesses.
    def remote(command: list[str], label: str) -> str:
        return run_logged(command, label, root=root, out=out, records=command_records,
                          timeout=REMOTE_QUERY_TIMEOUT)
    # Resolve the live automation main, then fetch the immutable consumer bytes.
    auto_main = remote(['gh','api',f'repos/{AUTO_REPOSITORY}/commits/main','--jq','.sha'], 'automation_main').strip()
    assert auto_main == args.automation_sha, 'Automation main changed: review the current pin before adopting it'
    payload = json.loads(remote(['gh','api',f'repos/{AUTO_REPOSITORY}/contents/{CONSUMER_REL}?ref={auto_main}'], 'consumer_fetch'))
    consumer = out/'build_microcap_realtime_digest.py'
    consumer.write_bytes(base64.b64decode(payload['content']))
    provenance = {'automation_repository':AUTO_REPOSITORY,'automation_sha':auto_main,'consumer_git_blob_sha':payload['sha'],'consumer_raw_sha256':sha(consumer)}
    (out/'consumer_provenance.json').write_text(json.dumps(provenance,indent=2),encoding='utf-8')
    allowed = {'systemroot','windir','path','pathext','comspec','userprofile','appdata','localappdata','programfiles','programfiles(x86)','programdata','temp','tmp'}
    env = {k:v for k,v in os.environ.items() if k.lower() in allowed}
    env.update(PYTHONDONTWRITEBYTECODE='1',TOP100_REALTIME_REQUIRE_STATE='1')
    def run(command: list[str], label: str, timeout: int = 180) -> str:
        return run_logged(command, label, root=root, out=out, records=command_records,
                          timeout=timeout, env=env)
    check_cmd = [sys.executable,'-B','-X','utf8','scripts/top100_delivery.py','check']
    if args.reuse_verified_signal_logs:
        prior = args.reuse_verified_signal_logs.resolve()
        before = read_json(prior/'whole_check_before.log')
        commands = read_json(prior/'commands.json')
        # A prior successful check and real queries may be reused only while
        # every input and every CSV still matches their authenticated snapshot.
        for name, digest in {**before['inputs'], **before['artifacts']}.items():
            if name in before['inputs'] or name.endswith('.csv'):
                assert hashlib.sha256((root/name).read_bytes().replace(b'\r\n',b'\n')).hexdigest() == digest, name
        for version in ('0','3','5'):
            label=f'signal_v2_{version}'
            matching=[c for c in commands if Path(c['log']).name == label+'.log']
            assert len(matching)==1 and matching[0]['exit_code']==0
            shutil.copy2(prior/(label+'.log'),out/(label+'.log'))
        (out/'reused_query_provenance.json').write_text(json.dumps({'source':str(prior),'authenticated_csv_and_inputs_unchanged':True},indent=2),encoding='utf-8')
    else:
        before = json.loads(run(check_cmd,'whole_check_before'))
    assert before['ok'] and before['scope'] == 'whole_workspace_delivery' and before['release_sha'] == args.strategy_sha
    expected = before['expected_date']
    if not args.reuse_verified_signal_logs:
        for version in ('0','3','5'):
            run([sys.executable,'-B','-X','utf8',f'microcap_top100_mom16_biweekly_live_v2_{version}.py','信号'],f'signal_v2_{version}')
    # Official queries may update data_freshness_proof in summaries even when
    # every final CSV and input remains identical. Reseal via the actual group
    # refresh, never by editing a manifest or restoring an older summary.
    run([sys.executable,'-B','-X','utf8','scripts/top100_delivery.py','refresh-all'],
        'whole_refresh_after_signals',timeout=900)
    proof = json.loads(run(check_cmd,'whole_check_after_signals'))
    assert proof['ok'] and proof['expected_date'] == expected and proof['release_sha'] == args.strategy_sha
    protected = set(proof['inputs'])|set(proof['artifacts'])|{'outputs/top100_delivery_manifest.json'}
    before_render = {name:sha(root/name) for name in protected}
    final_rows = {}
    command = [sys.executable,'-B','-X','utf8',str(consumer),'--out-dir',str(out/'preview'),'--publication-mode','close_confirmed','--expected-signal-date',expected,'--strategy-sha',args.strategy_sha,'--planned','local release preview; no email','--started',datetime.now(ZoneInfo('Asia/Shanghai')).isoformat()]
    for version in ('0','3','5'):
        path = root/f'outputs/microcap_top100_mom16_biweekly_live_v2_{version}_latest_signal.csv'
        row = only_row(path)
        assert row['version'] == f'2.{version}' and row['date'] == expected
        assert row['signal_timing'] == 'close_confirmed' and row['official_close_confirmed_signal'] == 'True'
        final_rows[f'v2.{version}'] = {'path':str(path),'sha256':sha(path),'row':row}
        command += ['--result',f'v2.{version}={out/f"signal_v2_{version}.log"}','--signal-csv',f'v2.{version}={path}','--exit-code',f'v2.{version}=0']
    run(command,'preview_build')
    metadata = read_json(out/'preview/metadata.json')
    assert metadata['status'] == 'OK' and metadata['publication_mode'] == 'close_confirmed' and metadata['signal_date'] == expected
    assert not metadata.get('delivery_test_id'), 'Local preview must not impersonate the cloud SMTP acceptance'
    assert all(sha(root/name)==digest for name,digest in before_render.items()), 'Consumer modified formal state'
    report = {'status':'local_close_preview_verified_no_send','strategy_sha':args.strategy_sha,**provenance,'expected_date':expected,'authority_data_tail_revision':authority['data_tail_revision'],'approved_package_sha256':APPROVAL_PACKAGE_SHA,'whole_check_ok':True,'final_csv_rows':final_rows,'metadata':metadata,'formal_files_unchanged_by_consumer':True,'smtp_called':False,'delivery_markers_touched':False}
    (out/'local_daily_readback.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps({k:v for k,v in report.items() if k not in ('final_csv_rows','metadata')},ensure_ascii=False))

if __name__ == '__main__':
    main()
