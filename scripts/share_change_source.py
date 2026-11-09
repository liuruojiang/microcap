"""Bounded CNInfo share-change refresh with immutable source receipts."""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
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
DATED_WINDOW_KIND = 'dual_source_dated_window_v1'
# Exact forward structural transfer, independently checked against issuer PDF
# pages 1 and 8. No legal-effective-date or historical migration is authorized.
STRUCTURE_ONLY_EVENT_20261105 = dict(symbol='300743', date='2026-11-05',
    announcement_id='AN202611051831456433', total_shares=151204323,
    em_row_sha256='8e60250d80578a9788804873854a6055b55a9dd9221eb0f3e3b10f73468f66e5',
    pdf_url='https://pdf.dfcfw.com/pdf/H2_AN202611051831456433_1.pdf',
    pdf_sha256='0e3d22e681f7e10c7192cb951e0ac1c44f912b9023773c72fc1e31e5223f80cf',
    pdf_bytes=164308, limited_before=23653195, limited_after=24432195,
    unlimited_before=127551128, unlimited_after=126772128, transferred=779000)
EXACT_FUTURE_PLAN_688092 = dict(symbol='688092', date='2026-11-12',
    provider_notice_date='2026-11-10', announcement_id='AN202611091831555510',
    before_total_shares=82663740, after_total_shares=82148940,
    em_row_sha256='e2171ef595b37ad2d474a27ba397852cab22a883c7442363d1b543896d4ed9d2',
    pdf_url='https://pdf.dfcfw.com/pdf/H2_AN202611091831555510_1.pdf',
    pdf_sha256='77b5ee10304392ffed474c5e569a932b40bea55b9297bcd98eb2bbb5c1c4d72d',
    pdf_bytes=125835)
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
    if (source_root / "RATE_LIMIT_STOP.json").exists() or (state and state.get("kind") in ("dual_source_unchanged_tail_v1","sse_dated_unchanged_tail_20261104_v1","sse_dated_transition_20261105_v1",DATED_WINDOW_KIND)):
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
        # A cloud seed can precede several completed sessions. Preserve the
        # unchanged-tail contract's adjacency: qualify/publish each session in
        # order under this same symbol lock, never skip an unproved date. A
        # later failure leaves the last independently verified state reusable.
        state_path = Path(root) / ".microcap_index_cache/share_sources" / (symbol + ".json")
        if allow_network and state_path.exists():
            state = json.loads(state_path.read_bytes())
            source_root = state_path.parent
            fallback = (source_root / "RATE_LIMIT_STOP.json").exists() or state.get("kind") in (
                "dual_source_unchanged_tail_v1", "sse_dated_unchanged_tail_20261104_v1", "sse_dated_transition_20261105_v1",DATED_WINDOW_KIND)
            target = pd.Timestamp(as_of).date()
            previous = pd.Timestamp(state["query_end"]).date()
            if fallback and previous < target:
                # These two issuers retain their exact approved dated routes.
                # Other issuers can certify every session of a bounded window
                # once, including genuine forward total-share changes.
                exact_legacy_window = symbol in ('301288','603028') and previous < datetime.fromisoformat('2026-11-06').date()
                if os.environ.get('MICROCAP_SHARE_DATED_WINDOW') == '1' and not exact_legacy_window:
                    return _refresh_dated_window(root=root,symbol=symbol,cache_path=cache_path,
                                                 as_of=target.isoformat())
                if datetime.now(timezone.utc) < datetime.fromisoformat(target.isoformat() + "T15:00:00+08:00"):
                    raise ValueError("Share source close is not eligible")
                from scripts.exchange_calendar import sessions_for_day
                sessions = sessions_for_day(target)
                if previous not in sessions or target not in sessions:
                    raise ValueError("Share continuation requires verified exchange sessions")
                pending = [day for day in sessions if previous < day <= target]
                if len(pending) > 1:
                    for day in pending:
                        result = _refresh_share_change(root=root, symbol=symbol, cache_path=cache_path,
                                                       as_of=day.isoformat(), allow_network=True)
                    return result
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
            if state.get("kind") not in ("dual_source_unchanged_tail_v1","sse_dated_unchanged_tail_20261104_v1","sse_dated_transition_20261105_v1",DATED_WINDOW_KIND):
                raise ValueError("Unknown share provider/schema")
            previous = state["previous_state"]
            previous_state = json.loads(bound(previous["path"], previous["sha256"]))
            previous_raw = bound(f"snapshots/{symbol}/{previous_state['candidate_sha256']}.csv", previous_state['candidate_sha256'])
            verify(previous_state, previous_raw, depth+1)
            expected = (pd.Timestamp(previous_state["query_end"]) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
            transition = state.get("kind") in ("sse_dated_transition_20261105_v1",DATED_WINDOW_KIND)
            if state.get("query_start") != expected or day < expected or (not transition and candidate != previous_raw):
                raise ValueError("Unchanged share tail changed history or has a query gap")
            from scripts.share_unchanged_tail import verify_manifest
            reference = state["evidence_manifest"]
            manifest = json.loads(bound(reference["path"], reference["sha256"]))
            if manifest.get("schema") != state["kind"]:
                raise ValueError("Share state/provider manifest kind differs")
            if state.get('kind') == DATED_WINDOW_KIND:
                expected_candidate,_ = verify_dated_window(manifest,symbol=symbol,previous_day=previous_state['query_end'],
                    end=day,previous_candidate=previous_raw,bound=bound,root=root)
                if candidate != expected_candidate:
                    raise ValueError('Dated share window candidate does not reproduce original sources')
            elif transition:
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


def verify_dated_window(manifest, *, symbol, previous_day, end, previous_candidate, bound, root, now=None):
    """Reproduce forward shares from three independent native observations.

    A complete dated market-value row is required for *every* exchange session,
    including the previously certified boundary. Historical/provider dates are
    not relabeled as observation times or asserted to be legal effect dates.
    Existing approvals and future-plan/zero-change guards remain applicable.
    """
    import csv
    import io
    from decimal import Decimal
    from scripts import share_unchanged_tail as n
    from scripts.exchange_calendar import sessions_for_day
    now=now or datetime.now(timezone.utc)
    need=lambda ok,message:n.require(ok,'dated window: '+message)
    d,t=n.day(previous_day),n.day(end)
    need(d<t and (t-d).days<=366,'invalid or excessive window')
    need(manifest.get('schema')==DATED_WINDOW_KIND and manifest.get('symbol')==symbol
         and manifest.get('previous_day')==previous_day and manifest.get('query_end')==end,
         'manifest identity/window differs')
    sessions=[x.isoformat() for x in sessions_for_day(t) if d<=x<=t]
    need(sessions and sessions[0]==previous_day and sessions[-1]==end,'boundary is not an exchange session')
    old=list(csv.DictReader(io.StringIO(previous_candidate.decode('utf-8-sig'))))
    need(old and list(old[0])==['change_date','total_shares_10k','reason']
         and [r['change_date'][:10] for r in old]==sorted(set(r['change_date'][:10] for r in old))
         and max(r['change_date'][:10] for r in old)<=previous_day,'invalid original history')
    before=n.amount(old[-1]['total_shares_10k'])
    files=manifest['files']
    raw,range_receipt=n._artifact(files['dated_window'],bound,url=n.HISTORICAL_URL,
                       params=n.historical_params(symbol,previous_day,end),end=end,now=now)
    payload=json.loads(raw,parse_float=Decimal)
    result=payload.get('result')
    need(payload.get('success') is True and isinstance(result,dict),'dated source query failed')
    need(type(result.get('count')) is int and type(result.get('pages')) is int
         and result['pages']==1 and result['count']==len(sessions)
         and isinstance(result.get('data'),list) and len(result['data'])==len(sessions),
         'dated source truncated or wrong total')
    observed={}
    days=[]
    for row in result['data']:
        text=row.get('TRADE_DATE');need(isinstance(text,str) and text.endswith(' 00:00:00'),'dated source clock is not daily')
        ds=text[:10];days.append(ds)
        need(row.get('SECURITY_CODE')==symbol and row.get('SECUCODE')==n.em_symbol(symbol)
             and ds in sessions and ds not in observed,'dated source issuer/session differs')
        shares=n.amount(row.get('TOTAL_SHARES'))
        close,cap=n.amount(row.get('CLOSE_PRICE')),n.amount(row.get('TOTAL_MARKET_CAP'))
        need(shares==shares.to_integral_value() and abs(shares*close-cap)<=Decimal('0.01'),
             'dated source share units or market-cap identity differs')
        observed[ds]=shares/10000
    need(days==list(reversed(sessions)),'dated source missing/duplicate/out-of-order sessions')
    need(observed[previous_day]==before,'previously certified boundary conflicts; exact historical migration required')
    later_review=None;later_plan=None
    review=EXACT_FUTURE_PLAN_688092
    if symbol==review['symbol'] and end<review['date']:
        docs=files.get('later_future_documents',[])
        need(len(docs)==1 and docs[0].get('announcement_id')==review['announcement_id'],
             'exact later future plan lacks original issuer document')
        pdf,pdf_receipt=n._artifact(docs[0],bound,url=review['pdf_url'],params={},end=end,now=now)
        need(_sha(pdf)==review['pdf_sha256'] and len(pdf)==review['pdf_bytes']
             and pdf.startswith(b'%PDF-') and b'%%EOF' in pdf[-1024:],
             'exact later future plan issuer document differs')
        if end<review['provider_notice_date']:
            need(n.clock(pdf_receipt['http_start'])>datetime.fromisoformat(end+'T15:00:00+08:00'),
                 'later plan observation is not actually later than target')
        rows=[]
        for entry in files['unfiltered']:
            body,_=n._artifact(entry,bound,url=n.EM_URL,
                params=n.em_params(symbol,end,False,len(rows)+1),end=end,now=now)
            # The inventory validator below still authenticates every page and
            # count. This pass only identifies the one exact reviewed raw row.
            rows.append(json.loads(body,parse_float=Decimal)['result']['data'])
        candidates=[r for page in rows for r in page if _sha(json.dumps(r,sort_keys=True,
            ensure_ascii=False,separators=(',',':'),default=str).encode())==review['em_row_sha256']]
        need(len(candidates)==1,'exact later future plan raw row differs')
        row=candidates[0]
        need(row['SECURITY_CODE']==symbol and row['INFO_CODE']==review['announcement_id']
             and row['END_DATE'][:10]==row['LISTING_DATE'][:10]==review['date']
             and row['NOTICE_DATE'][:10]==review['provider_notice_date']
             and n.amount(row['TOTAL_SHARES'])==review['after_total_shares']
             and observed[end]*10000==review['before_total_shares'],
             'exact later future plan date/quantity differs')
        # This internal argument is only an exact raw-row identity filter. No
        # user approval record is created or modified. The late row is future
        # to the target and is never used in its candidate or daily totals.
        later_review=dict(row=row)
        later_plan=dict(date=review['date'],total_shares_10k=str(Decimal(review['after_total_shares'])/10000))
    unfiltered=n._em_inventory(files['unfiltered'],bound,symbol,end,False,now,approved_event=later_review)
    bounded=n._em_inventory(files['bounded'],bound,symbol,end,True,now)
    need([r for r in unfiltered if n._em_date(r['END_DATE'])<=t]==bounded,'bounded/full event inventory differs')
    future=[r for r in unfiltered if n._em_date(r['END_DATE'])>t
            and (later_review is None or r!=later_review['row'])]
    plans,_=n._reviewed_future_plans(manifest,future,symbol=symbol,end=end,boundary=observed[end],
                                  bound=bound,now=now,root=root)
    sina_raw,_=n._artifact(files['sina'],bound,url=n.SINA_URL.format(symbol=symbol),params={},end=end,now=now)
    sina_later_present=False
    try:
        sina=n.parse_sina_total_history(sina_raw,symbol=symbol,observed_session=end,allowed_future_plans=plans)
    except ValueError as error:
        if later_plan is None or str(error)!='Sina TotalStock: future event/report date without exact reviewed plan':
            raise
        # The other vendor need not publish a still-unexecuted plan at the same
        # time. If present, all three Sina layers must exactly match that one
        # independently reviewed future date and quantity; unknown futures fail.
        sina=n.parse_sina_total_history(sina_raw,symbol=symbol,observed_session=end,
            allowed_future_plans=plans+[later_plan])
        sina_later_present=True
    records=sina['records'];prior=[r for r in records if r['provider_date']<=previous_day]
    need(prior and n.amount(prior[-1]['total_shares_10k'])==before,'Sina boundary differs from certified history')
    tail=[r for r in records if previous_day<r['provider_date']<=end]
    latest=before;events=[];structure_events=[]
    for event in tail:
        value=n.amount(event['total_shares_10k'])
        matches=[row for row in bounded if n._em_date(row['END_DATE']).isoformat()==event['provider_date']
                 and n._em_date(row['LISTING_DATE']).isoformat()==event['provider_date']
                 and n.amount(row['TOTAL_SHARES'])==value*10000]
        need(len(matches)==1 and matches[0].get('INFO_CODE'),'Sina change lacks unique matching EM issuer event')
        if value==latest:
            review=STRUCTURE_ONLY_EVENT_20261105;row=matches[0]
            row_sha=_sha(json.dumps(row,sort_keys=True,ensure_ascii=False,separators=(',',':'),default=str).encode())
            need((symbol,event['provider_date'],row_sha,value*10000)==
                 (review['symbol'],review['date'],review['em_row_sha256'],review['total_shares']),
                 'new zero-change event requires its exact reviewed route')
            docs=files.get('structure_documents',[])
            need(len(docs)==1 and docs[0].get('announcement_id')==review['announcement_id'],
                 'exact structural event lacks issuer document')
            pdf,_=n._artifact(docs[0],bound,url=review['pdf_url'],params={},end=end,now=now)
            need(len(pdf)==review['pdf_bytes'] and _sha(pdf)==review['pdf_sha256']
                 and pdf.startswith(b'%PDF-') and b'%%EOF' in pdf[-1024:],
                 'exact structural event issuer document differs')
            need(n.integer(row['TOTAL_SHARES_CHANGE'])==0
                 and n.amount(row['LIMITED_SHARES'])==review['limited_after']
                 and n.amount(row['UNLIMITED_SHARES'])==review['unlimited_after']
                 and n.amount(row['LIMITED_SHARES_CHANGE'])==review['transferred']
                 and row['UNLIMITED_SHARES_CHANGE']==-review['transferred']
                 and review['limited_before']+review['unlimited_before']==review['total_shares']
                 and review['limited_after']+review['unlimited_after']==review['total_shares'],
                 'exact structural event is not total-neutral')
            structure_events.append(dict(date=event['provider_date'],total_shares_10k=str(value),
                announcement_id=review['announcement_id'],issuer_document_sha256=review['pdf_sha256'],
                old_total_unchanged=True))
            continue
        events.append(dict(date=event['provider_date'],total_shares_10k=str(value),announcement_id=matches[0]['INFO_CODE']))
        latest=value
    tails=[row for row in bounded if max(n._em_date(row[k]) for k in n.DATE_FIELDS)>d]
    reobservations=[];new_tails=[]
    for row in tails:
        old_dates=[n._em_date(row[k]).isoformat() for k in ('END_DATE','LISTING_DATE')]
        # A late quarterly disclosure can repeat an already certified older
        # quantity. It is not a new zero-change capital event. Both provider
        # capital dates must precede the boundary and exactly reproduce the
        # protected old curve; a changed historical total still fails closed.
        if all(ds<=previous_day for ds in old_dates):
            for ds in old_dates:
                prior_rows=[r for r in old if r['change_date'][:10]<=ds]
                need(prior_rows and n.amount(prior_rows[-1]['total_shares_10k'])*10000==n.amount(row['TOTAL_SHARES']),
                     'late historical report conflicts with certified share history')
            reobservations.append(dict(announcement_id=row.get('INFO_CODE'),provider_dates=old_dates))
        else:new_tails.append(row)
    proven_events=events+structure_events
    need(len(new_tails)==len(proven_events) and all(any(n._em_date(row['END_DATE']).isoformat()==e['date']
         and n._em_date(row['LISTING_DATE']).isoformat()==e['date']
         and n.amount(row['TOTAL_SHARES'])==n.amount(e['total_shares_10k'])*10000 for e in proven_events) for row in new_tails),
         'new/ambiguous EM event not reproduced by independent dated changes')
    for ds in sessions:
        expected=before
        for e in events:
            if e['date']<=ds:expected=n.amount(e['total_shares_10k'])
        need(observed[ds]==expected,'daily totals do not reproduce complete event history')
    quote_raw,quote_receipt=n._artifact(files['quote'],bound,url=n.QUOTE_URL,
                                      params=n.quote_params(symbol),end=end,now=now)
    quote=json.loads(quote_raw);data=quote.get('data')
    need(quote.get('rc')==0 and isinstance(data,dict) and data.get('f57')==symbol,'quote issuer differs')
    stamp=datetime.fromtimestamp(n.integer(data.get('f86')),timezone.utc)
    frame_day=stamp.astimezone(timezone(n.timedelta(hours=8))).date()
    complete=n.clock(quote_receipt['http_complete'])
    need(t<=frame_day and datetime.fromisoformat(end+'T15:00:00+08:00')<=stamp<=complete,
         'quote is not close eligible or has a future clock')
    if frame_day>t:
        need(frame_day==complete.astimezone(timezone(n.timedelta(hours=8))).date(),'later quote is not from observation day')
    if data.get('f124') not in (None,0):
        extra=datetime.fromtimestamp(n.integer(data['f124']),timezone.utc)
        need(extra.astimezone(timezone(n.timedelta(hours=8))).date()==frame_day and extra<=complete,
             'quote secondary clock conflicts')
    need(n.amount(data.get('f84'))/10000==observed[end],'quote total differs from dated terminal total')
    candidate=previous_candidate
    if events:
        need(previous_candidate.endswith(b'\n'),'original CSV has no final line boundary')
        stream=io.StringIO(newline='');writer=csv.writer(stream,lineterminator='\n')
        for event in events:
            writer.writerow([event['date'],event['total_shares_10k'],
                             'Dated EM/Sina provider event; legal effective date not asserted'])
        candidate+=stream.getvalue().encode('utf8')
    facts=dict(symbol=symbol,previous_day=previous_day,query_end=end,session_count=len(sessions),
                          changes=events,historical_reobservations=reobservations,
                          old_prefix_preserved=True,legal_effective_date_not_asserted=True)
    if structure_events:facts['structure_events']=structure_events
    if later_plan is not None:
        later=EXACT_FUTURE_PLAN_688092
        facts['future_plan_not_consumed']=dict(**later_plan,
            announcement_id=later['announcement_id'],provider_notice_date=later['provider_notice_date'],
            observed_after_target=later['provider_notice_date']>end,
            present_in_sina_future_inventory=sina_later_present,
            issuer_document_sha256=later['pdf_sha256'],execution_or_legal_effect_not_asserted=True)
    return candidate,facts


def collect_dated_window(*, root, symbol, previous_day, end, candidate):
    from scripts import share_unchanged_tail as n
    source=Path(root)/'.microcap_index_cache/share_sources'
    identity=_sha(json.dumps([symbol,previous_day,end,_sha(candidate)]).encode())
    directory=source/'providers'/DATED_WINDOW_KIND/symbol/identity
    directory.mkdir(parents=True,exist_ok=True)
    def bound(relative,digest):
        path=(source/relative).resolve()
        n.require(path.is_relative_to(source.resolve()),'dated window source escaped root')
        raw=path.read_bytes();n.require(_sha(raw)==digest,'dated window source hash differs');return raw
    def save(url,params,purpose):
        path,rp=directory/(purpose+'.raw'),directory/(purpose+'.receipt.json')
        if path.exists() and rp.exists():
            entry=dict(raw_path=path.relative_to(source).as_posix(),raw_sha256=_sha(path.read_bytes()),
                       receipt_path=rp.relative_to(source).as_posix(),receipt_sha256=_sha(rp.read_bytes()))
            raw,_=n._artifact(entry,bound,url=url,params=params,end=end,now=datetime.now(timezone.utc))
            return entry,raw
        exhausted=directory/(purpose+'.exhausted.json')
        if exhausted.exists():raise RuntimeError('SHARE_REQUEST_RETRY_EXHAUSTED: preserved '+purpose)
        try:return n._save_public_observation(directory=directory,source=source,url=url,params=params,purpose=purpose,end=end)
        except RuntimeError as error:
            if str(error).startswith('SHARE_REQUEST_RETRY_EXHAUSTED:'):
                _atomic(exhausted,json.dumps(dict(error=str(error),recorded_at=datetime.now(timezone.utc).isoformat())).encode())
            raise
    files={}
    # A full window avoids one set of identical issuer queries per missing day.
    # Verification still requires each intervening exchange-session row.
    files['dated_window'],_=save(n.HISTORICAL_URL,n.historical_params(symbol,previous_day,end),'dated_window')
    for bounded,name in ((False,'unfiltered'),(True,'bounded')):
        entry,raw=save(n.EM_URL,n.em_params(symbol,end,bounded),name+'_1')
        pages=n.integer(json.loads(raw)['result']['pages']);n.require(1<=pages<=100,'dated window page budget exceeded')
        files[name]=[entry]
        for page in range(2,pages+1):files[name].append(save(n.EM_URL,n.em_params(symbol,end,bounded,page),name+'_'+str(page))[0])
    documents=[];reviews=n.plan_reviews(root)
    for entry in files['unfiltered']:
        for row in json.loads((source/entry['raw_path']).read_bytes())['result']['data']:
            if n._em_date(row['END_DATE'])>n.day(end):
                later=EXACT_FUTURE_PLAN_688092
                if (symbol,row.get('INFO_CODE'),row['END_DATE'][:10])==(later['symbol'],later['announcement_id'],later['date']):
                    ref,_=save(later['pdf_url'],{},'later_future_'+later['announcement_id'])
                    files['later_future_documents']=[{**ref,'announcement_id':later['announcement_id']}]
                    continue
                matching=[r for r in reviews if r['symbol']==symbol and r['planned_date']==row['END_DATE'][:10]]
                n.require(len(matching)==1,'unreviewed future event; no dated window promotion')
                review=matching[0];ref,_=save(review['pdf_url'],{},'pdf_'+review['announcement_id'])
                documents.append({**ref,'announcement_id':review['announcement_id']})
    files['sina'],_=save(n.SINA_URL.format(symbol=symbol),{},'sina')
    files['quote'],_=save(n.QUOTE_URL,n.quote_params(symbol),'quote')
    review=STRUCTURE_ONLY_EVENT_20261105
    if symbol==review['symbol'] and previous_day<review['date']<=end:
        ref,_=save(review['pdf_url'],{},'structure_'+review['announcement_id'])
        files['structure_documents']=[{**ref,'announcement_id':review['announcement_id']}]
    manifest=dict(schema=DATED_WINDOW_KIND,symbol=symbol,previous_day=previous_day,query_end=end,
                  files=files,future_plan_documents=documents,previous_candidate_sha256=_sha(candidate))
    _atomic(directory/'collection.json',json.dumps(manifest,ensure_ascii=False,indent=2).encode('utf8'))
    proposed,facts=verify_dated_window(manifest,symbol=symbol,previous_day=previous_day,end=end,
                                      previous_candidate=candidate,bound=bound,root=root)
    manifest['verified_facts']=facts
    raw=json.dumps(manifest,ensure_ascii=False,indent=2).encode('utf8');path=directory/'manifest.json';_atomic(path,raw)
    return proposed,dict(path=path.relative_to(source).as_posix(),sha256=_sha(raw))


def _refresh_dated_window(*, root, symbol, cache_path, as_of):
    root,cache_path=_extended(root),_extended(cache_path)
    source=root/'.microcap_index_cache/share_sources';state_path=source/(symbol+'.json')
    if (cache_path.resolve()!=(root/'.microcap_index_cache/share_change'/(symbol+'.csv')).resolve()
            or not source.resolve().is_relative_to(root.resolve())):
        raise ValueError('Dated share window source/cache path differs')
    if (source/'incomplete'/(symbol+'.json')).exists():raise ValueError('Incomplete share-source publication requires verified recovery')
    old_state=state_path.read_bytes();state=json.loads(old_state);old=cache_path.read_bytes()
    verify_share_cache(root=root,symbol=symbol,as_of=state['query_end'])
    proposed,reference=collect_dated_window(root=root,symbol=symbol,previous_day=state['query_end'],end=as_of,candidate=old)
    if state_path.read_bytes()!=old_state or cache_path.read_bytes()!=old:raise ValueError('Concurrent share source change before dated window publication')
    previous_path=source/'states'/(_sha(old_state)+'.json')
    for path,raw in ((previous_path,old_state),(source/'snapshots'/symbol/(_sha(old)+'.csv'),old),
                     (source/'snapshots'/symbol/(_sha(proposed)+'.csv'),proposed)):
        if path.exists() and path.read_bytes()!=raw:raise ValueError('Conflicting immutable share-source object')
        if not path.exists():_atomic(path,raw)
    result=dict(schema_version=2,kind=DATED_WINDOW_KIND,symbol=symbol,
                query_start=(pd.Timestamp(state['query_end'])+pd.Timedelta(days=1)).strftime('%Y-%m-%d'),query_end=as_of,
                candidate_sha256=_sha(proposed),previous_state=dict(path=previous_path.relative_to(source).as_posix(),sha256=_sha(old_state)),
                evidence_manifest=reference)
    new_state=json.dumps(result,ensure_ascii=False,indent=2).encode('utf8')
    try:
        _atomic(cache_path,proposed);_atomic(state_path,new_state)
        verify_share_cache(root=root,symbol=symbol,as_of=as_of)
    except BaseException:
        if cache_path.read_bytes() not in (old,proposed) or state_path.read_bytes() not in (old_state,new_state):
            _atomic(source/'incomplete'/(symbol+'.json'),json.dumps(dict(status='concurrent_change',
                    previous_state_sha256=_sha(old_state),candidate_sha256=_sha(proposed))).encode())
            raise ValueError('Concurrent share publication prevents rollback')
        _atomic(cache_path,old);_atomic(state_path,old_state)
        raise
    import io
    return pd.read_csv(io.BytesIO(proposed),parse_dates=['change_date'])
