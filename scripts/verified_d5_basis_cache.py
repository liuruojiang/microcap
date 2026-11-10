"""Reuse only byte-identical, already native-verified D5 factor inputs.

This is a sealed historical reference, not a verdict loaded from an arbitrary
receipt. Changed inputs, runtime, math or calendar retain the full native path.
"""
from __future__ import annotations

import gzip
import hashlib
import json
from pathlib import Path, PurePosixPath
import platform
from datetime import datetime, timezone

import numpy as np
import pandas as pd

REFERENCE = 'data/verified_d5_basis_source_index_20261109.json.gz'
REFERENCE_SHA256 = 'aa66c2cbf724fe80748d53f8f13373e2ecceecd7c5475e2c270918ccbc6a7d36'
SEED_SHA256 = 'b2a241bf6a87017104989c203b6262d921c48c32530a9685e6b226f3cfa84060'
_EXPECTED_RECORDS = 5438


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def runtime():
    return dict(python=platform.python_version(), pandas=pd.__version__, numpy=np.__version__)


def _path(root, name):
    p = PurePosixPath(name)
    if (not isinstance(name,str) or p.is_absolute() or '..' in p.parts
            or '\\' in name or ':' in name or p.as_posix()!=name):
        raise ValueError('Invalid sealed factor-reference path')
    path = root / name
    if not path.resolve().is_relative_to(root):
        raise ValueError('Sealed factor-reference path escapes runtime')
    return path


class VerifiedReference:
    def __init__(self, root, payload, compressed):
        self.root = root
        self.payload = payload
        self.signature = sha(json.dumps(payload,sort_keys=True,ensure_ascii=False,separators=(',',':')).encode())
        self.observed = {REFERENCE:sha(compressed)}
        self.calendar_hashes = {}
        self.files = {}
        self.reused = 0
        for name in (*payload['math_files'], 'scripts/verified_d5_basis_cache.py', 'scripts/stock_return_pipeline.py'):
            path = _path(root,name)
            self.observed[name] = sha(path.read_bytes().replace(b'\r\n',b'\n'))

    def finish(self):
        if sha(json.dumps(self.payload,sort_keys=True,ensure_ascii=False,separators=(',',':')).encode()) != self.signature:
            raise ValueError('Validated factor reference was mutated in memory')
        for name,digest in self.observed.items():
            raw = _path(self.root,name).read_bytes()
            if name.endswith('.py'):raw=raw.replace(b'\r\n',b'\n')
            if sha(raw)!=digest:
                raise ValueError('Factor reference or reader code changed during verification')
        for name,digest in self.files.items():
            if sha(_path(self.root,name).read_bytes())!=digest:
                raise ValueError('Equivalent factor inputs changed during verification')

    def try_symbol(self, symbol, manifest, sessions, listed, delisted):
        record = self.payload['records'].get(symbol)
        if not record or manifest != record['manifest']:
            return None
        if manifest['list_date']!=listed or manifest['delist_date']!=delisted:
            return None
        certified=manifest['as_of']
        calendar_key=(certified,tuple(sessions))
        if calendar_key not in self.calendar_hashes:
            from scripts.stock_return_basis import _dates
            days=_dates(sessions).sort_values().unique()
            self.calendar_hashes[calendar_key]=sha('\n'.join(days[days<=pd.Timestamp(certified)].strftime('%Y-%m-%d')).encode())
        if self.calendar_hashes[calendar_key]!=manifest['sessions_sha256']:
            return None
        for name,expected in record['inputs'].items():
            path=_path(self.root,name)
            # No mtime-only trust: hash each exact dependency even on a hit.
            try:actual=sha(path.read_bytes())
            except FileNotFoundError:return None
            if actual!=expected:return None
            self.files[name]=expected
        self.reused+=1
        return dict(symbol=symbol,
            manifest_sha256=record['inputs']['.microcap_index_cache/return_basis/manifests/'+symbol+'.json'],
            candidate_sha256=manifest['candidate_sha256'],raw_sha256=manifest['raw_sha256'],
            rows=manifest['rows'],last=manifest['last'],trade_status_files=record['trade_status_files'],
            verification_method='sealed_native_verified_d5_source_equivalence')


def load_reference(root, master_bytes):
    root=Path(root).resolve();path=_path(root,REFERENCE)
    if not path.exists():
        return None  # Small isolated fixtures retain the original full validator.
    raw=path.read_bytes()
    if sha(raw)!=REFERENCE_SHA256:
        raise ValueError('Sealed D5 factor reference bytes changed')
    payload=json.loads(gzip.decompress(raw))
    if (payload.get('schema')!='verified_d5_basis_source_equivalence_v1'
            or payload.get('source_bundle_sha256')!=SEED_SHA256
            or len(payload.get('records',{}))!=_EXPECTED_RECORDS):
        raise ValueError('Invalid sealed D5 source equivalence reference')
    if payload['runtime']!=runtime() or payload['master_sha256']!=sha(master_bytes):
        return None
    verified_at=datetime.fromisoformat(payload['native_verified_at'])
    if verified_at.tzinfo is None or verified_at>datetime.now(timezone.utc):
        return None  # Do not borrow an audit performed after the actual clock.
    if any(sha(_path(root,n).read_bytes().replace(b'\r\n',b'\n'))!=digest for n,digest in payload['math_files'].items()):
        return None
    from scripts import stock_return_pipeline as pipeline, stock_return_basis as basis
    if (pipeline.build_ratio_return_basis is not basis.build_ratio_return_basis
            or pipeline.validate_cached_candidate is not basis.validate_cached_candidate):
        return None  # Runtime replacements never inherit a historical cache hit.
    return VerifiedReference(root,payload,raw)
