"""Explicit ratio-basis source/cache adapter. No legacy or raw-return fallback."""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import uuid

import pandas as pd
import numpy as np
import requests

from scripts.stock_return_basis import build_ratio_return_basis, validate_cached_candidate, RETURN_BASIS_REVISION, CSV_LINE_TERMINATOR


def _hash(data):
    return hashlib.sha256(data).hexdigest()


def _extended(path):
    path=Path(path).resolve()
    if os.name=="nt" and not str(path).startswith("\\\\?\\"):
        return Path("\\\\?\\"+str(path))
    return path


def _write(path: Path, data: bytes):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp=path.with_name(path.name+"."+uuid.uuid4().hex+".tmp")
    tmp.write_bytes(data);tmp.replace(path)


def _json(path, data):
    _write(path,json.dumps(data,ensure_ascii=False,sort_keys=True,indent=2,allow_nan=False).encode())


def _load_or_build(*, basis_root, symbol, raw_path, as_of, sessions, list_date,
                  delist_date=None, anchor_date=None, source_dir=None,
                  allow_network=False, source_verified_through=None,
                  allow_research_attestation=False):
    """Build from complete raw history and separately evidenced factor sources.

    `source_verified_through` is an explicit caller attestation for an imported
    package whose original receipts lack timestamps; it is recorded, never
    inferred from file mtimes. With timestamped receipts, both must have been
    fetched after the requested China close. Network refresh is opt-in, bounded,
    and never retries a failed request. Return (frame, provenance manifest).
    """
    root=_extended(basis_root);raw_path=_extended(raw_path);folder=_extended(source_dir) if source_dir else root/"sources"/symbol
    if source_verified_through is not None and not allow_research_attestation:
        raise ValueError("Caller freshness attestation is research-only; production requires timestamped source receipts")
    candidate=root/"prices_adj"/f"{symbol}.csv";mp=root/"manifests"/f"{symbol}.json"
    old=json.loads(mp.read_text(encoding="utf-8")) if mp.exists() else None
    if old is not None and old.get("return_basis_revision")!=RETURN_BASIS_REVISION:
        raise ValueError("Legacy cache must be migrated explicitly")
    if candidate.exists() and old is None:
        raise ValueError("Unmanifested legacy cache rejected")
    if old and anchor_date is None:anchor_date=old["anchor_date"]
    raw_bytes=raw_path.read_bytes()
    if old and _hash(raw_bytes)!=old["raw_sha256"]:
        previous_bundle=root/"bundles"/symbol/_hash(json.dumps(old,sort_keys=True).encode())
        previous_raw=previous_bundle/"raw.csv"
        if not previous_raw.exists() or _hash(previous_raw.read_bytes())!=old["raw_sha256"]:
            raise ValueError("Previous complete raw snapshot unavailable; explicit new-root migration required")
        prior=pd.read_csv(previous_raw).set_index("date")["close_raw"]
        current=pd.read_csv(raw_path).set_index("date")["close_raw"]
        prior=prior.loc[prior.index<=old["as_of"]]
        if not prior.index.is_unique or not current.index.is_unique or not prior.index.isin(current.index).all():
            raise ValueError("Raw history deletion rejected; use explicit new-root migration")
        if not (prior.to_numpy()==current.reindex(prior.index).to_numpy()).all():
            raise ValueError("Historical raw-price rewrite rejected; use explicit new-root migration")
    close=dt.datetime.fromisoformat(as_of+"T15:00:00+08:00")
    bodies={};receipts={};fresh=True
    for kind in ("qfq","hfq"):
        path=folder/f"{kind}.js";rp=folder/f"{kind}.json"
        if not(path.exists() and rp.exists()):fresh=False;continue
        bodies[kind]=path.read_bytes();receipts[kind]=json.loads(rp.read_text(encoding="utf-8"))
        receipt=receipts[kind]
        if receipt.get("status")!=200 or receipt.get("sha256")!=_hash(bodies[kind]):
            raise ValueError("Existing factor receipt is failed or corrupted; no silent retry")
        stamp=receipt.get("retrieved_at_utc")
        if stamp:
            timestamp=dt.datetime.fromisoformat(stamp.replace("Z","+00:00"))
            if timestamp.tzinfo is not None and timestamp>dt.datetime.now(dt.timezone.utc):
                raise ValueError("Future factor receipt timestamp rejected")
            if timestamp.tzinfo is None or timestamp<close:fresh=False
        elif not source_verified_through or source_verified_through<as_of:fresh=False
    if source_verified_through and source_verified_through<as_of:
        raise ValueError("Imported source package is stale")
    if not fresh:
        if not allow_network:raise ValueError("Factor source freshness unavailable; explicit source refresh required")
        if dt.datetime.now(dt.timezone.utc)<close:raise ValueError("Requested close is not yet eligible for source refresh")
        if (root/"RATE_LIMIT_STOP.json").exists():raise ValueError("Factor source group stopped after HTTP rate limit")
        # Immutable attempt directory: partial/failed pairs never become active.
        folder=root/"source_attempts"/symbol/dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        market=("sh" if symbol.startswith("6") else "sz")+symbol
        for kind in ("qfq","hfq"):
            url=f"https://finance.sina.com.cn/realstock/company/{market}/{kind}.js"
            try:
                response=requests.get(url,timeout=15)
            except requests.RequestException as exc:
                _json(folder/f"{kind}.json",dict(url=url,status="transport_error",error=str(exc)))
                raise
            body=response.content;receipt=dict(url=url,status=response.status_code,sha256=_hash(body),retrieved_at_utc=dt.datetime.now(dt.timezone.utc).isoformat())
            _write(folder/f"{kind}.js",body);_json(folder/f"{kind}.json",receipt)
            if response.status_code in (403,429):_json(root/"RATE_LIMIT_STOP.json",receipt)
            response.raise_for_status();bodies[kind]=body;receipts[kind]=receipt
    frame,manifest=build_ratio_return_basis(symbol=symbol,raw_csv=raw_bytes,
        qfq_source=bodies["qfq"],hfq_source=bodies["hfq"],qfq_receipt=receipts["qfq"],hfq_receipt=receipts["hfq"],
        as_of=as_of,sessions=sessions,list_date=list_date,delist_date=delist_date,anchor_date=anchor_date)
    manifest["source_package_verified_through"]=source_verified_through
    manifest["source_receipts_sha256"]={kind:_hash(json.dumps(receipts[kind],sort_keys=True,ensure_ascii=False).encode()) for kind in receipts}
    if old == manifest:
        validate_cached_candidate(candidate.read_bytes(),old,expected_manifest=manifest)
        return frame,manifest
    if old and candidate.exists():
        # Reject corruption even when legitimate current-input changes require a rebuild.
        validate_cached_candidate(candidate.read_bytes(),old,expected_manifest=old)
        previous=pd.read_csv(candidate).set_index("date")["close_adj"]
        current=frame.assign(date=frame.date.astype(str)).set_index("date")["close_adj"]
        if manifest["anchor_date"]!=old["anchor_date"] or not previous.index.isin(current.index).all():
            raise ValueError("Adjusted history deletion/anchor rewrite requires explicit new-root migration")
        # Permit only floating serialization noise, never a changed historical return.
        if not np.allclose(previous.to_numpy(),current.reindex(previous.index).to_numpy(),rtol=1e-12,atol=0):
            raise ValueError("Historical adjusted-price/factor rewrite requires explicit new-root migration")
    elif old:
        raise ValueError("Previous adjusted candidate missing; explicit recovery required")
    payload=frame.to_csv(index=False, lineterminator=CSV_LINE_TERMINATOR).encode("utf-8")
    # Publish content-addressed complete bundle first, then the current pointers.
    bundle=root/"bundles"/symbol/_hash(json.dumps(manifest,sort_keys=True).encode())
    _write(bundle/"candidate.csv",payload);_json(bundle/"manifest.json",manifest)
    _write(bundle/"raw.csv",raw_bytes)
    for kind in receipts:
        _write(bundle/f"{kind}.js",bodies[kind]);_json(bundle/f"{kind}.json",receipts[kind])
    if allow_network and source_dir is None:
        for kind in receipts:
            _write(root/"sources"/symbol/f"{kind}.js",bodies[kind])
            _json(root/"sources"/symbol/f"{kind}.json",receipts[kind])
    _write(candidate,payload);_json(mp,manifest)
    return frame,manifest


def load_or_build(**kwargs):
    """Serialize all accesses for one symbol; incomplete writes fail closed.

    Lock files are never stolen automatically. A crashed writer requires explicit
    recovery after process verification. Readers must use this API as well.
    """
    root=_extended(kwargs["basis_root"]);symbol=kwargs["symbol"]
    if not isinstance(symbol,str) or len(symbol)!=6 or not symbol.isascii() or not symbol.isdigit():
        raise ValueError("Invalid symbol")
    lock=root/"locks"/f"{symbol}.lock";lock.parent.mkdir(parents=True,exist_ok=True)
    try:fd=os.open(lock,os.O_CREAT|os.O_EXCL|os.O_WRONLY)
    except FileExistsError as exc:raise RuntimeError("Return basis symbol is locked; no concurrent cache access") from exc
    try:
        os.write(fd,str(os.getpid()).encode());os.close(fd)
        return _load_or_build(**kwargs)
    finally:
        lock.unlink()
