"""Copy a checked, isolated code/evidence snapshot; never touch formal data."""
import hashlib
import os
import datetime
import json
from pathlib import Path
import sys
import argparse

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from scripts.st_notice_evidence import evidence_source_files
from scripts.top100_delivery import RETURN_BASIS_FILES
from scripts.stock_return_basis_store import _extended

RESEARCH = ROOT / 'research_reports/20261029_st_history_repair'
TARGET = RESEARCH / 'replay/runtime_20261029'

def safe_relative(root, name):
    path = (root / name).resolve()
    if Path(name).is_absolute() or not path.is_relative_to(root.resolve()) or path == root.resolve():
        raise RuntimeError('Snapshot path escapes root: ' + str(name))
    return path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--replace-incomplete-snapshot-sha256')
    parser.add_argument('--supersede-rejected-result-sha256')
    args = parser.parse_args()
    if not TARGET.resolve().is_relative_to((RESEARCH / 'replay').resolve()):
        raise RuntimeError('Snapshot target escapes research replay')
    old_manifest_path = TARGET / 'source_manifest.json'
    old_manifest = None
    previous_generation = None
    if old_manifest_path.exists():
        old_raw = old_manifest_path.read_bytes()
        old_hash = hashlib.sha256(old_raw).hexdigest()
        if args.replace_incomplete_snapshot_sha256 != old_hash:
            raise RuntimeError('Existing snapshot requires its exact hash for explicit incomplete-candidate replacement')
        rejected = args.supersede_rejected_result_sha256
        if (TARGET / 'diagnostic_replay_20261029').exists():
            result_path = TARGET / 'diagnostic_replay_20261029/diagnostic_result.json'
            result = json.loads(result_path.read_bytes())
            if not rejected or hashlib.sha256(result_path.read_bytes()).hexdigest() != rejected or result.get('source_manifest_sha256') != old_hash or result.get('approved') is not False or result.get('formal_written') is not False:
                raise RuntimeError('Completed candidate requires exact rejected-result identity')
        failed_reports = {}
        for report_path in TARGET.glob('st_metadata_build_20261101*.json'):
            report_raw = report_path.read_bytes()
            report = json.loads(report_raw)
            if (report.get('status') not in (('failed_not_approved', 'installed_verified') if rejected else ('failed_not_approved',))
                    or report.get('approved') is not False
                    or report.get('formal_written') is not False
                    or (report.get('source_manifest_sha256') != old_hash and not (rejected and report.get('status') == 'failed_not_approved' and (TARGET / 'source_snapshot_generations' / report.get('source_manifest_sha256', '') / 'source_manifest.json').is_file()))):
                raise RuntimeError('Metadata report is not a failed uninstalled batch for this snapshot')
            failed_reports[report_path.name] = report_raw
        if any((TARGET / '.microcap_index_cache/security_meta').glob('*.json')):
            if not rejected: raise RuntimeError('Installed metadata requires explicit rejected-candidate supersession')
            verified = [json.loads(body) for body in failed_reports.values() if json.loads(body).get('status') == 'installed_verified']
            if len(verified) != 1 or len(verified[0]['records']) != 5438: raise RuntimeError('Verified full metadata report required')
            for row in verified[0]['records']:
                meta_path = TARGET / '.microcap_index_cache/security_meta' / (row['symbol'] + '.json')
                if hashlib.sha256(meta_path.read_bytes()).hexdigest() != row['sha256']: raise RuntimeError('Metadata bytes changed: ' + row['symbol'])
        old_manifest = json.loads(old_raw)
        for name, expected in old_manifest['files'].items():
            if hashlib.sha256(_extended(safe_relative(TARGET, name)).read_bytes()).hexdigest() != expected:
                raise RuntimeError('Prior snapshot already changed: ' + name)
        previous_generation = TARGET / 'source_snapshot_generations' / old_hash
        previous_generation.mkdir(parents=True, exist_ok=False)
        (previous_generation / 'source_manifest.json').write_bytes(old_raw)
        for name, body in failed_reports.items():
            (previous_generation / name).write_bytes(body)
    elif args.replace_incomplete_snapshot_sha256:
        raise RuntimeError('There is no prior snapshot to replace')
    if not (TARGET / 'base_input_assembly.json').is_file():
        raise RuntimeError('Prepare and verify the isolated input layer first')
    template = json.loads((RESEARCH / 'st_recert_runtime/source_manifest.json').read_text())
    paths = {p.replace('\\', '/') for p in template['files'] if p.endswith('.py')}
    paths.update(RETURN_BASIS_FILES)
    paths.update(evidence_source_files(ROOT))
    paths.update(['scripts/top100_delivery.py', 'scripts/top100_cloud_delivery.py'])
    snapshot = {name: _extended(safe_relative(ROOT, name)).read_bytes() for name in sorted(paths)}
    if old_manifest is not None:
        if set(old_manifest['files']) - set(snapshot):
            raise RuntimeError('Replacement must retain prior source paths')
        for name, old_hash in old_manifest['files'].items():
            if hashlib.sha256(snapshot[name]).hexdigest() != old_hash:
                archived = _extended(safe_relative(previous_generation, name))
                archived.parent.mkdir(parents=True, exist_ok=True)
                archived.write_bytes(_extended(TARGET / name).read_bytes())
                if hashlib.sha256(archived.read_bytes()).hexdigest() != old_hash:
                    raise RuntimeError('Prior changed-source backup differs: ' + name)
    for name, body in snapshot.items():
        if name.endswith('.py'):
            compile(body, name, 'exec')
    for name, body in snapshot.items():
        if old_manifest is not None and old_manifest['files'].get(name) == hashlib.sha256(body).hexdigest():
            continue
        target = _extended(safe_relative(TARGET, name))
        target.parent.mkdir(parents=True, exist_ok=True)
        temp = target.with_name(target.name + ".snapshot-tmp")
        temp.write_bytes(body)
        os.replace(temp, target)
        if target.read_bytes() != body:
            raise RuntimeError('Code snapshot copy differs: ' + name)
    if any(_extended(ROOT / name).read_bytes() != body for name, body in snapshot.items()):
        raise RuntimeError('Code/evidence changed during snapshot; no successful manifest')
    manifest = {'scope': 'isolated candidate code only', 'approved': False,
        'formal_written': False, 'created_at_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'source_root': str(ROOT), 'target_root': str(TARGET.resolve()), 'files': {name: hashlib.sha256(body).hexdigest() for name, body in snapshot.items()}}
    if previous_generation is not None:
        manifest['previous_generation'] = previous_generation.relative_to(TARGET).as_posix()
        manifest['replacement_reason'] = 'Explicit rejected diagnostic supersession for approved conservative delisting model; old outputs and source generation retained' if args.supersede_rejected_result_sha256 else 'Explicit incomplete diagnostic snapshot repair; raw input/source archive bytes retained'
        if args.supersede_rejected_result_sha256: manifest['superseded_rejected_result_sha256'] = args.supersede_rejected_result_sha256
    path = TARGET / 'source_manifest.json'
    temp = path.with_suffix('.tmp')
    temp.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8')
    os.replace(temp, path)
    print(json.dumps({'files': len(snapshot), 'manifest': str(path),
        'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}))

if __name__ == '__main__':
    main()
