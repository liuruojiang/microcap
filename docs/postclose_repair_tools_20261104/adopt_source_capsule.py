"""Copy native-produced source successors only after exact-prefix verification."""
import argparse
from contextlib import ExitStack, contextmanager
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import sys
import tempfile
import uuid
import zipfile

RUNTIME = '14b896a0915f700e5b999ff6c47676e6f8acaca1'
ARCHIVE_SHA = '2713c657b527b9b5c7c79daba0d7aceb210bee6ed00d7b3f0514d1fb3b573562'
MANIFEST_SHA = '60975486aee8ff9b8016457116c21d39e1c66cdef2cedec72ae6f8105f058eb5'
REVIEW_SHA = '8ecabf6af932b1ab0c158675098087414a951936c4b30a07a359aad86998bb7a'


def sha(raw): return hashlib.sha256(raw).hexdigest()


def read_capsule(archive):
    if sha(archive.read_bytes()) != ARCHIVE_SHA:
        raise RuntimeError('Share capsule archive identity differs')
    with zipfile.ZipFile(archive) as z:
        names = z.namelist()
        if len(names) != len(set(names)):
            raise RuntimeError('Duplicate capsule member')
        raw = z.read('manifest.json')
        if sha(raw) != MANIFEST_SHA:
            raise RuntimeError('Share capsule manifest identity differs')
        manifest = json.loads(raw)
        if (manifest['schema'] != 'native_share_source_extension_capsule_v1'
                or manifest['validator_runtime_sha'] != RUNTIME or len(manifest['states']) != 31
                or manifest['source_authority_review_sha256'] != REVIEW_SHA
                or manifest['not_a_whole_state_bundle'] is not True
                or manifest['requires_identical_previous_state_and_unchanged_csv'] is not True):
            raise RuntimeError('Share capsule scope differs')
        if set(names) != {'manifest.json'} | {'sources/'+n for n in manifest['files']}:
            raise RuntimeError('Share capsule closed inventory differs')
        objects = {}
        for name, spec in manifest['files'].items():
            p = PurePosixPath(name)
            if p.is_absolute() or '..' in p.parts or '\\' in name:
                raise RuntimeError('Unsafe source object name')
            raw = z.read('sources/'+name)
            if len(raw) != spec['bytes'] or sha(raw) != spec['sha256']:
                raise RuntimeError('Share capsule object differs: '+name)
            objects[name] = raw
    return manifest, objects


def validate_prefix(root, manifest, objects):
    directory = root/'.microcap_index_cache/share_sources'
    before, csvs = {}, {}
    for symbol, spec in manifest['states'].items():
        if (len(symbol) != 6 or not symbol.isascii() or not symbol.isdigit()
                or spec['previous_session'] != '2026-11-03' or spec['query_end'] != '2026-11-04'
                or spec['native_verified'] is not True):
            raise RuntimeError('Unexpected source successor identity')
        raw = (directory/(symbol+'.json')).read_bytes()
        csv = (root/'.microcap_index_cache/share_change'/(symbol+'.csv')).read_bytes()
        new = objects[symbol+'.json']; state = json.loads(new)
        if (sha(new) != spec['new_state_sha256'] or sha(csv) != spec['candidate_sha256']
                or state['previous_state']['sha256'] != spec['previous_state_sha256']
                or state['candidate_sha256'] != spec['candidate_sha256']
                or state['query_end'] != '2026-11-04' or state['symbol'] != symbol):
            raise RuntimeError('Native source successor/unchanged history differs: '+symbol)
        if sha(raw) not in (spec['previous_state_sha256'],spec['new_state_sha256']):
            raise RuntimeError('Source is newer or on another branch: '+symbol)
        before[symbol] = raw; csvs[symbol] = csv
    return before, csvs


@contextmanager
def owned_lock(path):
    token = str(os.getpid())+':'+uuid.uuid4().hex
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('x',encoding='utf8') as stream: stream.write(token)
    try: yield
    finally:
        if path.read_text(encoding='utf8') != token:
            raise RuntimeError('Source lock ownership changed; preserved')
        path.unlink()


def atomic(path, raw):
    temp = path.with_name(path.name+'.repair-'+uuid.uuid4().hex)
    try:
        with temp.open('xb') as stream:
            stream.write(raw);stream.flush();os.fsync(stream.fileno())
        os.replace(temp,path)
    finally:
        if temp.exists(): temp.unlink()


def adopt(root, archive, backup):
    root = root.resolve()
    if subprocess.check_output(['git','-C',str(root),'rev-parse','HEAD']).decode().strip() != RUNTIME:
        raise RuntimeError('Wrong native source validator')
    manifest, objects = read_capsule(archive)
    sys.path.insert(0,str(root))
    from scripts.share_change_source import verify_share_cache
    from scripts.realtime_state_bundle import _verify_share_source_json_protocol, current_notice_member_symbols
    if not set(manifest['states']).issubset(set(current_notice_member_symbols(root))):
        raise RuntimeError('Capsule is outside current native member scope')
    directory = root/'.microcap_index_cache/share_sources'
    if backup.resolve().is_relative_to(root):
        raise RuntimeError('Backup must be outside production state')
    with ExitStack() as stack:
        for symbol in sorted(manifest['states']):
            stack.enter_context(owned_lock(directory/'locks'/(symbol+'.lock')))
        before, csvs = validate_prefix(root,manifest,objects)
        with tempfile.TemporaryDirectory(prefix='native-source-stage-') as tmp:
            stage = Path(tmp)
            for name, raw in objects.items():
                path = stage/'.microcap_index_cache/share_sources'/name
                path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(raw)
            for symbol, raw in csvs.items():
                path = stage/'.microcap_index_cache/share_change'/(symbol+'.csv')
                path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(raw)
            (stage/'data').mkdir()
            shutil.copy2(root/'data/share_source_future_plan_reviews.json',stage/'data')
            shutil.copytree(root/'data/share_source_sse_review_20261104',stage/'data/share_source_sse_review_20261104')
            for symbol in manifest['states']:
                proof = verify_share_cache(root=stage,symbol=symbol,as_of='2026-11-04')
                _verify_share_source_json_protocol(stage,symbol,proof)
        active = {s+'.json' for s in manifest['states']}
        for name, raw in objects.items():
            path = directory/name
            if name in active: continue
            if path.exists() and path.read_bytes() != raw:
                raise RuntimeError('Immutable source object conflict: '+name)
        backup.mkdir(parents=True,exist_ok=False)
        for symbol, raw in before.items(): (backup/(symbol+'.json')).write_bytes(raw)
        changed = []
        try:
            for name, raw in objects.items():
                path = directory/name
                if name in active or path.exists(): continue
                path.parent.mkdir(parents=True,exist_ok=True)
                with path.open('xb') as stream: stream.write(raw)
            if validate_prefix(root,manifest,objects) != (before,csvs):
                raise RuntimeError('Source inputs changed before publication')
            for symbol in sorted(manifest['states']):
                path = directory/(symbol+'.json');new = objects[symbol+'.json']
                if before[symbol] != new:
                    if path.read_bytes() != before[symbol]: raise RuntimeError('Source changed during adoption')
                    atomic(path,new);changed.append(symbol)
                proof = verify_share_cache(root=root,symbol=symbol,as_of='2026-11-04')
                _verify_share_source_json_protocol(root,symbol,proof)
            if any((root/'.microcap_index_cache/share_change'/(s+'.csv')).read_bytes()!=raw for s,raw in csvs.items()):
                raise RuntimeError('Share history changed during source adoption')
        except BaseException:
            for symbol in reversed(changed):
                path = directory/(symbol+'.json')
                if path.read_bytes() != objects[symbol+'.json']:
                    raise RuntimeError('Source changed outside owned transaction; rollback stopped')
                atomic(path,before[symbol])
            raise
    return {'ok':True,'archive_sha256':ARCHIVE_SHA,'native_verified':31,'adopted':len(changed),
            'already_identical':31-len(changed),'history_csv_bytes_unchanged':True,
            'exact_previous_prefix_verified':True,'not_a_whole_state_bundle':True,'SMTP':False}


if __name__ == '__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--archive',type=Path,required=True)
    p.add_argument('--backup',type=Path,required=True);p.add_argument('--audit',type=Path,required=True);a=p.parse_args()
    result=adopt(a.root,a.archive,a.backup);a.audit.write_text(json.dumps(result,indent=2),encoding='utf8');print(json.dumps(result))
