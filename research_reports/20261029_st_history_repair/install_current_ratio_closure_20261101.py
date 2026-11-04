"""Copy only independently audited current ratio bundles to isolated replay."""
from pathlib import Path
import hashlib
import json
import shutil
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor

B = Path(__file__).resolve().parent
sys.path.insert(0, str(B.parents[1]))
from scripts.stock_return_basis_store import _extended

source = _extended(B / 'installable_return_basis_20261029/return_basis')
target = _extended(B / 'replay/runtime_20261029/.microcap_index_cache/return_basis')
assert target.is_relative_to(_extended(B / 'replay/runtime_20261029'))
audit_path = B / 'installable_return_basis_20261029/final_independent_audit.json'
audit_bytes = audit_path.read_bytes()
audit = json.loads(audit_bytes)
assert audit['total'] == audit['passed'] == 5438 and not audit['failures'] and not audit['locks']
assert not list((source / 'locks').glob('*.lock'))
def copy_entry(entry):
    symbol = entry['symbol']
    copied = 0
    for relative, expected in entry['file_hashes'].items():
        src = source / relative
        dst = target / relative
        assert src.resolve().is_relative_to(source.resolve()) and dst.resolve().is_relative_to(target.resolve())
        payload = src.read_bytes()
        assert hashlib.sha256(payload).hexdigest() == expected, f'source changed: {relative}'
        if dst.exists():
            old_digest = hashlib.sha256(dst.read_bytes()).hexdigest()
            if old_digest != expected:
                archive = _extended(B / 'replay/runtime_20261029_ratio_interrupted_copy') / relative
                archive = archive.with_name(archive.name + '.' + old_digest)
                archive.parent.mkdir(parents=True,exist_ok=True)
                assert not archive.exists(), f'interrupted backup collision: {relative}'
                dst.replace(archive)
        if not dst.exists():
            dst.parent.mkdir(parents=True, exist_ok=True)
            temporary = dst.with_name(dst.name + '.' + uuid.uuid4().hex + '.copying')
            temporary.write_bytes(payload)
            temporary.replace(dst)
        assert hashlib.sha256(dst.read_bytes()).hexdigest() == expected, f'copy mismatch: {relative}'
        copied += 1
    return dict(symbol=symbol,files=copied,manifest_sha256=entry['manifest_sha256'])
records = []
with ThreadPoolExecutor(max_workers=4) as pool:
    for row in pool.map(copy_entry,audit['records'],buffersize=8):
        records.append(row)
        if len(records) % 500 == 0:
            print(len(records), flush=True)
assert audit_path.read_bytes() == audit_bytes
report = dict(formal_written=False,approved=False,network_requests=0,total=len(records),
              copied_files=sum(r['files'] for r in records),source=str(source),target=str(target),
              source_audit_sha256=hashlib.sha256(audit_bytes).hexdigest(),records=records)
(B / 'replay/runtime_20261029_ratio_install.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
print('copied',report['total'],'symbols',report['copied_files'],'files',flush=True)
