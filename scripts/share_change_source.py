"""Bounded CNInfo share-change refresh with immutable source receipts."""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import uuid
import time
import threading
import ipaddress
import subprocess
from urllib.parse import urlencode

import numpy as np
import pandas as pd
import requests

from scripts.stock_return_basis_store import _extended

_ENCODER_LOCK = threading.Lock()
_DNS_LOCK = threading.Lock()
_DNS_CACHE = {}
# Observed through Google/Cloudflare/Ali DNS and verified against the real
# CNInfo HTTPS API on 2026-11-02. This is a route, never a TLS-identity override.
_KNOWN_CNINFO_IPV4 = '120.48.196.100'


def _post_with_dns_fallback(url, *, params, headers, timeout):
    """Keep the original HTTPS origin/TLS identity when the OS resolver fails."""
    deadline = time.monotonic() + timeout
    try:
        return requests.post(url, params=params, headers=headers, timeout=timeout)
    except requests.ConnectionError as exc:
        if 'getaddrinfo failed' not in str(exc) and 'NameResolutionError' not in str(exc):
            raise
    host = 'webapi.cninfo.com.cn'
    if url != f'https://{host}/api/stock/p_stock2215':
        raise ValueError('DNS fallback is restricted to the CNInfo share endpoint')
    with _DNS_LOCK:
        if _DNS_CACHE.get('expires', 0) <= time.monotonic():
            records = []
            for resolver in ('https://cloudflare-dns.com/dns-query', 'https://dns.google/resolve'):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise requests.Timeout('CNInfo DNS fallback deadline exhausted')
                try:
                    answer = requests.get(resolver, params={'name': host, 'type': 'A'},
                                          headers={'Accept': 'application/dns-json'}, timeout=min(5, remaining))
                    answer.raise_for_status()
                except requests.RequestException:
                    continue
                payload = answer.json()
                if not any(q.get('name', '').rstrip('.') == host and q.get('type') == 1 for q in payload.get('Question', [])):
                    raise ValueError('CNInfo DNS response identity failed')
                if payload.get('Status') != 0:
                    continue
                records = [r for r in payload.get('Answer', []) if r.get('name', '').rstrip('.') == host and r.get('type') == 1]
                if records:
                    break
            if not records:
                _DNS_CACHE.update(address=_KNOWN_CNINFO_IPV4, expires=time.monotonic() + 60,
                                  kind='previously_observed_route_not_fresh_dns')
            else:
                if not ipaddress.ip_address(records[0]['data']).is_global or int(records[0]['TTL']) < 0:
                    raise ValueError('CNInfo DNS response has no valid public address')
                _DNS_CACHE.update(address=records[0]['data'], expires=time.monotonic() + min(300, int(records[0]['TTL'])),
                                  kind='https_dns_response')
        address = _DNS_CACHE['address']
        route_kind = _DNS_CACHE.get('kind', 'previously_observed_route_not_fresh_dns')
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise requests.Timeout('CNInfo DNS fallback deadline exhausted')
    command = ['curl', '--silent', '--show-error', '--connect-timeout', str(min(5, remaining)),
               '--max-time', str(remaining), '--resolve', f'{host}:443:{address}',
               '--request', 'POST', '--write-out', '\n%{http_code}']
    for key, value in headers.items():
        command.extend(['--header', f'{key}: {value}'])
    command.append(url + '?' + urlencode(params))
    try:
        result = subprocess.run(command, capture_output=True, timeout=remaining + 1, check=False)
    except subprocess.TimeoutExpired as exc:
        raise requests.Timeout('CNInfo DNS fallback transport timed out') from exc
    if result.returncode:
        raise requests.ConnectionError('CNInfo DNS fallback transport failed: ' + result.stderr.decode(errors='replace')[:300])
    body, status = result.stdout.rsplit(b'\n', 1)
    response = requests.Response()
    response.status_code = int(status)
    response._content = body
    response.url = url
    response._microcap_transport = dict(kind=route_kind, address=address, origin_hostname=host,
                                       https_certificate_verification=True)
    return response


def _cninfo_request_key():
    # MiniRacer's process-wide native initialization is not concurrency safe.
    with _ENCODER_LOCK:
        import akshare as ak
        namespace = ak.stock_share_change_cninfo.__globals__
        encoder = namespace["py_mini_racer"].MiniRacer()
        encoder.eval(namespace["_get_file_content_cninfo"]("cninfo.js"))
        return encoder.call("getResCode1")


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _atomic(path, raw):
    path = _extended(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    temporary.write_bytes(raw)
    temporary.replace(path)


def parse_source(raw, *, symbol, start, end):
    payload = json.loads(raw)
    rows = payload.get("records")
    if (payload.get("resultcode") != 200 or not isinstance(rows, list)
            or payload.get("total") != len(rows) or payload.get("count") != len(rows)):
        raise ValueError("Incomplete CNInfo share-change response")
    records = []
    for row in rows:
        day = str(row["VARYDATE"])[:10]
        if str(row["SECCODE"]).zfill(6) != symbol or not start <= day <= end:
            raise ValueError("CNInfo share-change issuer/date mismatch")
        value = float(row["F003N"])
        if not np.isfinite(value) or value <= 0:
            raise ValueError("Invalid CNInfo total shares")
        records.append(dict(change_date=pd.Timestamp(day), total_shares_10k=value,
                            reason=str(row.get("F002V", ""))))
    result = pd.DataFrame(records, columns=["change_date", "total_shares_10k", "reason"])
    if not result.empty:
        if result.groupby("change_date").total_shares_10k.nunique().gt(1).any():
            raise ValueError("Conflicting same-day CNInfo share counts")
        result = result.drop_duplicates("change_date", keep="last").sort_values("change_date")
    return result


def merge_preserving_history(old, incoming):
    """New reporting rows may repeat a count; they may not rewrite past counts."""
    if old.empty:
        return incoming
    if incoming.empty:
        return old.copy()
    combined = pd.concat([old, incoming], ignore_index=True).drop_duplicates("change_date", keep="last").sort_values("change_date")
    if not incoming.empty:
        dates = pd.DatetimeIndex(sorted(set(old.change_date) | set(incoming.loc[incoming.change_date.le(old.change_date.max()), "change_date"])))
        before = old.set_index("change_date").total_shares_10k.reindex(dates).ffill()
        after = combined.set_index("change_date").total_shares_10k.reindex(dates).ffill()
        if not (before.eq(after) | (before.isna() & after.isna())).all():
            raise ValueError("Share-count history changed; exact-hash migration required")
    return combined


def _refresh_share_change(*, root, symbol, cache_path, as_of, allow_network=False):
    root, cache_path = _extended(root), _extended(cache_path)
    end = pd.Timestamp(as_of).strftime("%Y-%m-%d")
    symbol = str(symbol).zfill(6)
    source_root = root / ".microcap_index_cache/share_sources"
    failure_marker = source_root / "incomplete" / f"{symbol}.json"
    if failure_marker.exists():
        raise ValueError("Incomplete share-source publication requires verified recovery")
    state_path = source_root / f"{symbol}.json"
    old_bytes = cache_path.read_bytes() if cache_path.exists() else b""
    old = pd.read_csv(cache_path, parse_dates=["change_date"]) if old_bytes else pd.DataFrame(columns=["change_date", "total_shares_10k", "reason"])
    if not old.empty:
        old = old.sort_values("change_date")
        if (old.change_date.isna().any() or old.change_date.duplicated().any()
                or not np.isfinite(old.total_shares_10k).all() or old.total_shares_10k.le(0).any()
                or old.change_date.gt(pd.Timestamp(end)).any()):
            raise ValueError("Invalid existing share-change cache")
    original_state_bytes = state_path.read_bytes() if state_path.exists() else None
    state = None
    if state_path.exists():
        state = json.loads(state_path.read_bytes())
        verify_share_cache(root=root, symbol=symbol, as_of=state["query_end"])
        if state["candidate_sha256"] != _sha(old_bytes):
            raise ValueError("Share source/cache identity mismatch")
        if state["query_end"] > end:
            raise ValueError("Share certified cutoff is after requested close")
        if state["query_end"] == end:
            return old
    elif not old.empty:
        raise ValueError("Existing share history requires an approved exact-hash baseline")
    if not allow_network:
        raise ValueError("Share source freshness requires explicit refresh")
    if datetime.now(timezone.utc) < datetime.fromisoformat(end + "T15:00:00+08:00"):
        raise ValueError("Share source close is not eligible")
    if (source_root / "RATE_LIMIT_STOP.json").exists() or (state and state.get("kind") in ("dual_source_unchanged_tail_v1","sse_dated_unchanged_tail_20261104_v1","sse_dated_transition_20261105_v1")):
        if not state:
            raise ValueError("Unchanged-tail fallback requires an existing verified share chain")
        if (cache_path.resolve() != (root / ".microcap_index_cache/share_change" / (symbol + ".csv")).resolve()
                or not source_root.resolve().is_relative_to(root.resolve())):
            raise ValueError("Unchanged-tail source/cache path differs")
        from scripts.share_unchanged_tail import collect_manifest
        manifest = collect_manifest(root=root, symbol=symbol, previous_day=state["query_end"],
                                    end=end, candidate=old_bytes)
        if state_path.read_bytes() != original_state_bytes or cache_path.read_bytes() != old_bytes:
            raise ValueError("Share inputs changed during unchanged-tail collection")
        previous_path = source_root / "states" / (_sha(original_state_bytes) + ".json")
        for immutable, raw in ((previous_path, original_state_bytes),
                               (source_root / "snapshots" / symbol / (_sha(old_bytes) + ".csv"), old_bytes)):
            if immutable.exists() and immutable.read_bytes() != raw:
                raise ValueError("Conflicting immutable unchanged-tail evidence")
            if not immutable.exists():
                _atomic(immutable, raw)
        manifest_kind=json.loads((source_root/manifest["path"]).read_bytes())["schema"]
        if manifest_kind not in ("dual_source_unchanged_tail_v1","sse_dated_unchanged_tail_20261104_v1","sse_dated_transition_20261105_v1"):
            raise ValueError("Unknown collected share-source schema")
        candidate = old_bytes
        if manifest_kind == "sse_dated_transition_20261105_v1":
            from scripts.sse_share_daily_unchanged_20261104 import approved_transition
            candidate, _ = approved_transition(root, symbol=symbol, previous_day=state["query_end"],
                                               end=end, previous_candidate=old_bytes)
            _atomic(source_root / "snapshots" / symbol / (_sha(candidate) + ".csv"), candidate)
        receipt = dict(schema_version=2, kind=manifest_kind, symbol=symbol,
                       query_start=(pd.Timestamp(state["query_end"]) + pd.Timedelta(days=1)).strftime("%Y-%m-%d"),
                       query_end=end, candidate_sha256=_sha(candidate), evidence_manifest=manifest,
                       previous_state=dict(path=previous_path.relative_to(source_root).as_posix(), sha256=_sha(original_state_bytes)),
                       primary_source_health="unknown_original_denial_preserved")
        published = json.dumps(receipt, ensure_ascii=False, indent=2).encode()
        try:
            if candidate != old_bytes:
                _atomic(cache_path, candidate)
            _atomic(state_path, published)
            verify_share_cache(root=root, symbol=symbol, as_of=end)
        except BaseException:
            if state_path.read_bytes() != published:
                _atomic(failure_marker, json.dumps(dict(status="unknown_concurrent_state", original_sha256=_sha(original_state_bytes),
                                                       published_sha256=_sha(published))).encode())
                raise RuntimeError("Unchanged-tail publication state changed concurrently; owner recovery required")
            if cache_path.read_bytes() != candidate:
                _atomic(failure_marker, b'{"status":"concurrent_share_candidate_change"}')
                raise RuntimeError("Share candidate changed concurrently; owner recovery required")
            if candidate != old_bytes:
                _atomic(cache_path, old_bytes)
            _atomic(state_path, original_state_bytes)
            raise
        return pd.read_csv(cache_path, parse_dates=["change_date"]) if candidate != old_bytes else old

    start = (pd.Timestamp(state["query_end"]) + pd.Timedelta(days=1)).strftime("%Y-%m-%d") if state else "1990-01-01"
    # Use the installed, pinned AkShare public CNInfo request encoder.
    url = "https://webapi.cninfo.com.cn/api/stock/p_stock2215"
    params = dict(scode=symbol, sdate=start, edate=end)
    headers = {"Accept-Enckey": _cninfo_request_key(), "Origin": "https://webapi.cninfo.com.cn",
               "Referer": "https://webapi.cninfo.com.cn/", "User-Agent": "Mozilla/5.0"}
    try:
        deadline = time.monotonic() + 45
        for attempt_index in range(3):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise requests.Timeout('Share source query exceeded its total deadline')
            try:
                response = _post_with_dns_fallback(url, params=params, headers=headers, timeout=min(15, remaining))
                break
            except (requests.Timeout, requests.ConnectionError):
                if attempt_index == 2:
                    raise
                print(f'[share-source] transient network failure symbol={symbol}; retry={attempt_index + 1}/2', flush=True)
                time.sleep(min(attempt_index + 1, max(0, deadline - time.monotonic())))
    except requests.RequestException as exc:
        failed = dict(symbol=symbol, url=url, params=params,
                      retrieved_at_utc=datetime.now(timezone.utc).isoformat(),
                      error_type=type(exc).__name__, status="transport_failure_no_response")
        failure_path = source_root / "failures" / symbol / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + ".json")
        _atomic(failure_path, json.dumps(failed, ensure_ascii=False, indent=2).encode())
        raise

    raw = response.content
    attempt = source_root / "responses" / symbol / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + ".json")
    _atomic(attempt, raw)
    receipt = dict(schema_version=1, symbol=symbol, url=url, params=params, status=response.status_code,
                   retrieved_at_utc=datetime.now(timezone.utc).isoformat(), response_sha256=_sha(raw),
                   response_path=attempt.relative_to(source_root).as_posix(), query_end=end)
    receipt['transport'] = getattr(response, '_microcap_transport', {'kind': 'system_dns_https'})
    _atomic(attempt.with_suffix(".receipt.json"), json.dumps(receipt, ensure_ascii=False, indent=2).encode())
    if response.status_code in (403, 429):
        _atomic(source_root / "RATE_LIMIT_STOP.json", json.dumps(receipt).encode())
        if state is not None:
            try:
                response.raise_for_status()
            except requests.HTTPError as primary_error:
                try:
                    # The real stop now exists, so this private re-entry can
                    # only take the qualified fallback, never query CNINFO or
                    # re-enter the public per-symbol lock.
                    return _refresh_share_change(root=root, symbol=symbol, cache_path=cache_path,
                                                 as_of=as_of, allow_network=allow_network)
                except Exception as fallback_error:
                    diagnostic = dict(status="primary_denial_and_fallback_failure", symbol=symbol,
                                      query_end=end, primary_status=response.status_code,
                                      primary_error_type=type(primary_error).__name__,
                                      primary_error=str(primary_error), fallback_error_type=type(fallback_error).__name__,
                                      fallback_error=str(fallback_error),
                                      stop_sha256=_sha((source_root / "RATE_LIMIT_STOP.json").read_bytes()),
                                      response_sha256=receipt["response_sha256"],
                                      recorded_at_utc=datetime.now(timezone.utc).isoformat())
                    failed_path = source_root / "failures" / symbol / (uuid.uuid4().hex + ".json")
                    _atomic(failed_path, json.dumps(diagnostic, ensure_ascii=False, indent=2).encode())
                    raise primary_error from fallback_error
    response.raise_for_status()
    incoming = parse_source(raw, symbol=symbol, start=start, end=end)
    combined = merge_preserving_history(old, incoming)
    if combined.empty:
        raise ValueError("No verified share-count history")
    candidate = old_bytes if incoming.empty and old_bytes else combined.to_csv(index=False).encode()
    receipt["candidate_sha256"] = _sha(candidate)
    if state:
        previous_bytes = state_path.read_bytes()
        previous_path = source_root / "states" / (_sha(previous_bytes) + ".json")
        _atomic(previous_path, previous_bytes)
        receipt["previous_state"] = {"path": previous_path.relative_to(source_root).as_posix(), "sha256": _sha(previous_bytes)}
        receipt["query_start"] = start
        receipt["historical_revisions"] = "Not applied; provider corrections before certified cutoff require separate exact-hash migration"

    previous_state = state_path.read_bytes() if state_path.exists() else None
    if previous_state != original_state_bytes or (cache_path.read_bytes() if cache_path.exists() else b"") != old_bytes:
        raise ValueError("Share inputs changed during source query; no publication")
    if old_bytes:
        _atomic(source_root / "snapshots" / symbol / (_sha(old_bytes) + ".csv"), old_bytes)
    _atomic(source_root / "snapshots" / symbol / (_sha(candidate) + ".csv"), candidate)
    try:
        _atomic(cache_path, candidate)
        _atomic(state_path, json.dumps(receipt, ensure_ascii=False, indent=2).encode())
        verify_share_cache(root=root, symbol=symbol, as_of=end)
    except BaseException:
        _atomic(failure_marker, json.dumps({"old_sha256": _sha(old_bytes), "candidate_sha256": _sha(candidate)}).encode())
        if old_bytes:
            _atomic(cache_path, old_bytes)
        if previous_state is not None:
            _atomic(state_path, previous_state)
        raise
    return combined


def refresh_share_change(*, root, symbol, cache_path, as_of, allow_network=False):
    symbol = str(symbol).zfill(6)
    if len(symbol) != 6 or not symbol.isascii() or not symbol.isdigit():
        raise ValueError("Invalid share-change symbol")
    lock = Path(root) / ".microcap_index_cache/share_sources/locks" / (symbol + ".lock")
    lock.parent.mkdir(parents=True, exist_ok=True)
    with lock.open("x", encoding="utf-8") as handle:
        handle.write(str(datetime.now(timezone.utc)))
    try:
        return _refresh_share_change(root=root, symbol=symbol, cache_path=cache_path,
                                     as_of=as_of, allow_network=allow_network)
    finally:
        lock.unlink()


def install_approved_share_baseline(*, root, symbol, cache_path, approval_path, approval_sha256):
    """Install only a user-approved, hash-bound historical share authority.

    The approval document uses schema share_baseline_approval_v1, approved=true,
    records[{symbol,candidate_sha256,certified_cutoff,source_files[{path,sha256}]}].
    Source paths are bounded relative to that document; bytes are copied intact.
    Preparing this document is not approval and approved=false is rejected.
    """
    root, cache_path = _extended(root), _extended(cache_path)
    approval_path = _extended(approval_path).resolve()
    raw = approval_path.read_bytes()
    if _sha(raw) != approval_sha256:
        raise ValueError("Share baseline approval hash mismatch")
    approval = json.loads(raw)
    if approval.get("schema") != "share_baseline_approval_v1" or approval.get("approved") is not True:
        raise ValueError("Share baseline exact-hash approval required")
    matches = [r for r in approval["records"] if r["symbol"] == symbol]
    if len(matches) != 1 or not matches[0].get("source_files"):
        raise ValueError("Share baseline lacks unique symbol/source closure")
    entry = matches[0]; candidate = Path(cache_path).read_bytes()
    if _sha(candidate) != entry["candidate_sha256"]:
        raise ValueError("Share baseline candidate hash mismatch")
    directory = _extended(root) / ".microcap_index_cache/share_sources"
    state_path = directory / f"{symbol}.json"
    if state_path.exists():
        raise ValueError("Share baseline already installed; explicit migration required")
    bucket = directory / "baselines" / approval_sha256
    for item in entry["source_files"]:
        source = (approval_path.parent / item["path"]).resolve()
        destination = (bucket / item["path"]).resolve()
        if not source.is_relative_to(approval_path.parent) or not destination.is_relative_to(bucket.resolve()):
            raise ValueError("Share baseline source escapes authority root")
        body = source.read_bytes()
        if _sha(body) != item["sha256"]:
            raise ValueError("Share baseline source hash mismatch")
        _atomic(destination, body)
    _atomic(bucket / "approval.json", raw)
    _atomic(directory / "snapshots" / symbol / (_sha(candidate) + ".csv"), candidate)
    state = dict(schema_version=1, kind="approved_baseline", symbol=symbol,
                 query_end=entry["certified_cutoff"], candidate_sha256=_sha(candidate),
                 approval_path=(bucket / "approval.json").relative_to(directory).as_posix(),
                 approval_sha256=approval_sha256)
    _atomic(state_path, json.dumps(state, ensure_ascii=False, indent=2).encode())
    try:
        verify_share_cache(root=root, symbol=symbol, as_of=state["query_end"])
    except BaseException:
        state_path.unlink()
        raise
    return state


def verify_share_cache(*, root, symbol, as_of):
    """Verify the complete baseline + contiguous query chain without network."""
    directory = _extended(root) / ".microcap_index_cache/share_sources"
    state_path = directory / f"{symbol}.json"
    cache_path = _extended(root) / ".microcap_index_cache/share_change" / f"{symbol}.csv"
    readbacks = {}; active = set()
    def read(path):
        path = path.resolve()
        if not path.is_relative_to(directory.resolve()):
            raise ValueError("Share response path escapes source root")
        body = path.read_bytes(); readbacks[path] = body
        return body
    def bound(relative, expected):
        body = read(directory / relative)
        if _sha(body) != expected:
            raise ValueError("Share source response hash mismatch")
        return body
    def frame(raw, day):
        import io
        value = pd.read_csv(io.BytesIO(raw), parse_dates=["change_date"])
        if (value.empty or value.change_date.isna().any() or value.change_date.duplicated().any()
                or not value.change_date.is_monotonic_increasing or value.change_date.gt(day).any()
                or not np.isfinite(value.total_shares_10k).all() or value.total_shares_10k.le(0).any()):
            raise ValueError("Invalid transported share-change history")
        return value
    def verify(state, candidate, depth=0):
        ident = _sha(json.dumps(state, sort_keys=True).encode())
        if depth > 10000 or ident in active:
            raise ValueError("Share source history cycle or excessive depth")
        active.add(ident)
        day = state["query_end"]
        if datetime.fromisoformat(day + "T15:00:00+08:00") > datetime.now(timezone.utc):
            raise ValueError("Share certified cutoff is not close-confirmed")
        if (state.get("schema_version") not in (1, 2) or state.get("symbol") != symbol
                or state.get("candidate_sha256") != _sha(candidate)):
            raise ValueError("Share source/cache identity or cutoff mismatch")
        actual = frame(candidate, day)
        if state.get("schema_version") == 2:
            if state.get("kind") not in ("dual_source_unchanged_tail_v1","sse_dated_unchanged_tail_20261104_v1","sse_dated_transition_20261105_v1"):
                raise ValueError("Unknown share provider/schema")
            previous = state["previous_state"]
            previous_state = json.loads(bound(previous["path"], previous["sha256"]))
            previous_raw = bound(f"snapshots/{symbol}/{previous_state['candidate_sha256']}.csv", previous_state['candidate_sha256'])
            verify(previous_state, previous_raw, depth+1)
            expected = (pd.Timestamp(previous_state["query_end"]) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
            transition = state.get("kind") == "sse_dated_transition_20261105_v1"
            if state.get("query_start") != expected or day < expected or (not transition and candidate != previous_raw):
                raise ValueError("Unchanged share tail changed history or has a query gap")
            from scripts.share_unchanged_tail import verify_manifest
            reference = state["evidence_manifest"]
            manifest = json.loads(bound(reference["path"], reference["sha256"]))
            if manifest.get("schema") != state["kind"]:
                raise ValueError("Share state/provider manifest kind differs")
            if transition:
                from scripts.sse_share_daily_unchanged_20261104 import verify_transition
                verify_transition(manifest, symbol=symbol, previous_day=previous_state["query_end"],
                                  end=day, candidate=candidate, previous_candidate=previous_raw, bound=bound, root=root)
            else:
                verify_manifest(manifest, symbol=symbol, previous_day=previous_state["query_end"],
                                end=day, candidate=candidate, bound=bound, root=root)
        elif state.get("kind") == "approved_baseline":
            raw = bound(state["approval_path"], state["approval_sha256"])
            approval = json.loads(raw)
            if approval.get("schema") != "share_baseline_approval_v1" or approval.get("approved") is not True:
                raise ValueError("Share baseline exact-hash approval required")
            matches = [r for r in approval["records"] if r["symbol"] == symbol]
            if len(matches) != 1 or not matches[0].get("source_files"):
                raise ValueError("Share baseline lacks unique symbol/source closure")
            entry = matches[0]
            if entry["candidate_sha256"] != _sha(candidate) or entry["certified_cutoff"] != day:
                raise ValueError("Share baseline authority mismatch")
            base = (directory / state["approval_path"]).parent.resolve()
            for item in entry["source_files"]:
                p = (base / item["path"]).resolve()
                if not p.is_relative_to(base) or _sha(read(p)) != item["sha256"]:
                    raise ValueError("Share baseline source hash mismatch")
        else:
            if (state.get("status") != 200 or state.get("url") != "https://webapi.cninfo.com.cn/api/stock/p_stock2215"
                    or state["params"].get("scode") != symbol or state["params"].get("edate") != day):
                raise ValueError("Share source receipt identity/freshness mismatch")
            raw = bound(state["response_path"], state["response_sha256"])
            stamp = datetime.fromisoformat(state["retrieved_at_utc"])
            if stamp.tzinfo is None or not datetime.fromisoformat(day + "T15:00:00+08:00") <= stamp <= datetime.now(timezone.utc):
                raise ValueError("Share source receipt identity/freshness mismatch")
            incoming = parse_source(raw, symbol=symbol, start=state["params"]["sdate"], end=day)
            previous = state.get("previous_state")
            if previous:
                previous_state = json.loads(bound(previous["path"], previous["sha256"]))
                previous_raw = bound(f"snapshots/{symbol}/{previous_state['candidate_sha256']}.csv", previous_state['candidate_sha256'])
                old = verify(previous_state, previous_raw, depth+1)
                expected = (pd.Timestamp(previous_state["query_end"]) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
                if state["params"]["sdate"] != expected or state.get("query_start") != expected or day < expected:
                    raise ValueError("Share source query chain has a gap/overlap")
                expected_frame = merge_preserving_history(old, incoming)
            else:
                expected_frame = incoming
            columns = ["change_date", "total_shares_10k", "reason"]
            try:
                pd.testing.assert_frame_equal(actual[columns].reset_index(drop=True).fillna(""),
                    expected_frame[columns].reset_index(drop=True).fillna(""), check_dtype=False, check_exact=True)
            except AssertionError as exc:
                raise ValueError("Share candidate does not reproduce source chain") from exc
        active.remove(ident)
        return actual
    state_bytes = read(state_path); state = json.loads(state_bytes)
    cache_bytes = cache_path.read_bytes()
    if state["query_end"] != pd.Timestamp(as_of).strftime("%Y-%m-%d"):
        raise ValueError("Share source/cache identity or cutoff mismatch")
    verify(state, cache_bytes)
    if cache_path.read_bytes() != cache_bytes or any(path.read_bytes() != body for path, body in readbacks.items()):
        raise ValueError("Share inputs changed during read-only validation")
    return dict(symbol=symbol, query_end=state["query_end"], candidate_sha256=_sha(cache_bytes),
                source_files={p.relative_to(directory.resolve()).as_posix(): _sha(b) for p,b in readbacks.items()})
