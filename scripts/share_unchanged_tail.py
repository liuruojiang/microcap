"""Provider-labelled, strictly unchanged share tail; never selects event dates.

This contract observes complete provider inventories, not a guarantee against
late disclosure.  A quote frame timestamp does not date its fundamental field.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import io
import base64
import json
import os
from pathlib import Path
import re
import time
import uuid
import zipfile
from urllib.parse import parse_qs, urlparse

import pandas as pd
import requests

from scripts.sina_share_history import parse_sina_total_history

KIND = "dual_source_unchanged_tail_v1"
QUOTE_CORROBORATION_KIND = "exact_reviewed_plan_later_quote_v1"
QUOTE_CORROBORATION_ENV = "MICROCAP_SHARE_LATER_QUOTE_CORROBORATION"
EM_URL = "https://datacenter.eastmoney.com/securities/api/data/v1/get"
QUOTE_URL = "https://push2.eastmoney.com/api/qt/stock/get"
TYPED_QUOTE = "tencent_sina_total_shares_v1"
_QUOTE_ENDPOINT_FAILURES = {}  # This process/date only; immutable HTTP evidence remains authoritative.
HISTORICAL_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
SINA_URL = "https://money.finance.sina.com.cn/corp/go.php/vCI_StockStructureHistory/stockid/{symbol}/stocktype/TotalStock.phtml"
DATE_FIELDS = ("END_DATE", "LISTING_DATE", "NOTICE_DATE")
SAFE_HEADERS = frozenset({"date", "last-modified", "content-type", "content-length", "cache-control", "etag", "content-encoding"})


def require(ok, message):
    if not ok:
        raise ValueError("Unchanged share tail: " + message)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def day(value):
    require(isinstance(value, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", value), "invalid date")
    return date.fromisoformat(value)


def clock(value):
    require(isinstance(value, str), "missing source clock")
    stamp = datetime.fromisoformat(value)
    require(stamp.tzinfo is not None, "source clock needs timezone")
    return stamp


def integer(value):
    require(not isinstance(value, bool), "boolean integer")
    try:
        result = Decimal(str(value))
    except InvalidOperation as error:
        raise ValueError("Unchanged share tail: invalid integer") from error
    require(result.is_finite() and result == result.to_integral_value() and result >= 0, "noninteger count")
    return int(result)


def amount(value):
    require(not isinstance(value, bool), "boolean share amount")
    try:
        result = Decimal(str(value))
    except InvalidOperation as error:
        raise ValueError("Unchanged share tail: invalid share amount") from error
    require(result.is_finite() and result > 0, "invalid share amount")
    return result


def em_symbol(symbol):
    require(isinstance(symbol, str) and re.fullmatch(r"[0-9]{6}", symbol), "invalid symbol")
    return symbol + (".SH" if symbol.startswith(("5", "6", "9")) else ".BJ" if symbol.startswith(("4", "8")) else ".SZ")


def em_params(symbol, target, bounded, page=1):
    return dict(reportName="RPT_F10_EH_EQUITY", columns="ALL",
                filter=f'(SECUCODE="{em_symbol(symbol)}")' + (f"(END_DATE<='{target}')" if bounded else ""),
                pageNumber=str(page), pageSize="500", sortTypes="-1", sortColumns="END_DATE", source="HSF10", client="PC")


def quote_params(symbol):
    return dict(fltt="2", invt="2", fields="f57,f58,f84,f85,f124,f86,f43,f59,f152,f189",
                secid=("1." if symbol.startswith(("5", "6", "9")) else "0.") + symbol)


def historical_params(symbol, previous_day, end):
    em_symbol(symbol)
    day(previous_day), day(end)
    return dict(sortColumns="TRADE_DATE", sortTypes="-1", pageSize="5000", pageNumber="1",
                reportName="RPT_VALUEANALYSIS_DET", columns="ALL", quoteColumns="", source="WEB", client="WEB",
                filter=f'(SECURITY_CODE="{symbol}")(TRADE_DATE>=\'{previous_day}\')(TRADE_DATE<=\'{end}\')')


def _historical_close(entry, bound, *, symbol, previous_day, end, boundary, now):
    """Read dated raw total shares, never reinterpret a later quote as a close.

    Both adjacent sessions must independently match the authenticated boundary.
    This supplements, rather than replaces, the complete EM/Sina event checks.
    """
    raw, _ = _artifact(entry, bound, url=HISTORICAL_URL,
                       params=historical_params(symbol, previous_day, end), end=end, now=now)
    payload = json.loads(raw, parse_float=Decimal)
    result = payload.get("result")
    require(payload.get("success") is True and type(payload.get("code")) is int and payload["code"] == 0
            and isinstance(result, dict), "historical close provider failed")
    rows = result.get("data")
    require(isinstance(rows, list) and len(rows) == 2 and integer(result.get("pages")) == 1
            and integer(result.get("count")) == 2, "historical close inventory incomplete")
    require([_em_date(row.get("TRADE_DATE")).isoformat() for row in rows] == [end, previous_day],
            "historical close session inventory differs")
    for row in rows:
        require(row.get("SECURITY_CODE") == symbol and row.get("SECUCODE") == em_symbol(symbol),
                "historical close issuer differs")
        total = integer(row.get("TOTAL_SHARES"))
        require(total > 0 and Decimal(total) / 10000 == boundary, "historical close raw total differs")
        # Provider values are in shares and currency units, not ten-thousand
        # shares. The independent field relationship also detects unit drift.
        cap, close = amount(row.get("TOTAL_MARKET_CAP")), amount(row.get("CLOSE_PRICE"))
        require(abs(cap - Decimal(total) * close) <= Decimal("0.01"), "historical close units/price differ")
    return dict(source="eastmoney_RPT_VALUEANALYSIS_DET", sessions=[previous_day, end],
                total_shares=str(boundary * 10000), raw_sha256=sha(raw),
                basis="provider_trade_date_labelled_raw_total_shares",
                provider_revision_or_latency_guaranteed=False)


def require_adjacent(previous_day, end):
    from scripts.exchange_calendar import sessions_for_day
    previous, target = day(previous_day), day(end)
    sessions = sessions_for_day(target)
    before = [value for value in sessions if value < target]
    require(target in sessions and before and before[-1] == previous, "unchanged-tail requires adjacent exchange sessions")


def _artifact(entry, bound, *, url, params, end, now):
    raw = bound(entry["raw_path"], entry["raw_sha256"])
    receipt_raw = bound(entry["receipt_path"], entry["receipt_sha256"])
    receipt = json.loads(receipt_raw)
    require("request_headers" not in receipt and "headers" not in receipt, "private/unknown header inventory")
    headers = receipt.get("response_headers", {})
    require(isinstance(headers, dict) and all(isinstance(k, str) and k.lower() in SAFE_HEADERS
            and isinstance(v, str) and "\r" not in v and "\n" not in v for k, v in headers.items()), "unsafe response headers")
    original_schema = receipt.get("schema")
    if original_schema == "share_source_public_receipt_v1":
        derivation = receipt.get("derivation")
        require(isinstance(derivation, dict) and set(derivation) == {"method", "original_receipt_sha256"}
                and derivation["method"] == "allowlisted_headers_only_v1"
                and isinstance(derivation["original_receipt_sha256"], str)
                and re.fullmatch(r"[0-9a-f]{64}", derivation["original_receipt_sha256"]), "unknown receipt derivation")
        original_schema = receipt.get("original_receipt_schema")
    legacy_sina = original_schema == "public_share_source_probe_v1"
    if legacy_sina:
        require(url.startswith("https://money.finance.sina.com.cn/") and receipt.get("request_method") == "GET", "unknown legacy receipt")
        receipt = {**receipt, "actual_url": receipt["final_url"], "http_status": receipt["status"],
                   "http_start": receipt["observation_start"], "http_complete": receipt["observation_end"],
                   "bytes": receipt["raw_bytes"]}
    require(receipt.get("url") == url and receipt.get("params") == params, "request endpoint/parameters differ")
    require(receipt.get("http_status") == 200, "source HTTP failed")
    # Archived Sina v1 receipts predate the explicit TLS-config field. Their
    # preserved collector uses requests default verify=True; do not fabricate
    # a TLS-handshake assertion in the original receipt.
    require(receipt.get("tls_verification") is True or (legacy_sina
            and receipt.get("tls_verification") is None
            and receipt.get("source_identity") == "Sina native AkShare TotalStock URL"), "source TLS configuration failed")
    require(receipt.get("raw_sha256") == sha(raw) and integer(receipt.get("bytes")) == len(raw), "raw receipt mismatch")
    start, complete = clock(receipt["http_start"]), clock(receipt["http_complete"])
    close = datetime.fromisoformat(end + "T15:00:00+08:00")
    require(close <= start <= complete <= now, "source collection outside close/clock bounds")
    actual = urlparse(receipt["actual_url"])
    expected = urlparse(url)
    allowed_hosts = {expected.netloc}
    if url == QUOTE_URL:
        allowed_hosts.add("push2delay.eastmoney.com")
    require(actual.scheme == expected.scheme and actual.netloc in allowed_hosts and actual.path == expected.path, "actual endpoint differs")
    require(parse_qs(actual.query, keep_blank_values=True) == {k: [str(v)] for k, v in params.items()}, "actual query differs")
    return raw, receipt


def _quote_symbols(symbols):
    require(isinstance(symbols, list) and 0 < len(symbols) <= 300
            and len(symbols) == len(set(symbols))
            and all(isinstance(s, str) and re.fullmatch(r"[036][0-9]{5}", s) for s in symbols),
            "invalid typed quote issuer scope")
    return [(("sh" if s.startswith("6") else "sz") + s) for s in symbols]


def typed_quote_urls(symbols):
    tickers = _quote_symbols(symbols)
    return ("https://qt.gtimg.cn/q=" + ",".join(tickers),
            "https://hq.sinajs.cn/list=" + ",".join(x for ticker in tickers for x in (ticker, ticker + "_i")))


def _quote_variables(raw, prefix, separator):
    text = raw.decode("gb18030").strip(); values = {}
    pattern = re.compile(r'(?:var\s+)?' + prefix + r'([a-z0-9_]+)="([^"\r\n]*)";')
    for match in pattern.finditer(text):
        require(match[1] not in values, "duplicate typed quote variable")
        values[match[1]] = match[2].split(separator)
    require(values and not pattern.sub("", text).strip(), "typed quote wrapper differs")
    return values


def parse_typed_quote(tencent_raw, sina_raw, *, symbol, symbols, end, receipts):
    """Provider-native exact capital fields, never a fabricated EastMoney f84."""
    tickers = _quote_symbols(symbols); require(symbol in symbols, "typed quote issuer absent")
    ticker = tickers[symbols.index(symbol)]
    tq = _quote_variables(tencent_raw, "v_", "~")
    sq = _quote_variables(sina_raw, "hq_str_", ",")
    require(set(tq) == set(tickers) and set(sq) == {x for t in tickers for x in (t, t + "_i")},
            "typed quote requested variable set differs")
    t, s, info = tq[ticker], sq[ticker], sq[ticker + "_i"]
    require(len(t) >= 74 and len(s) >= 33 and len(info) >= 23
            and t[2] == symbol and info[0] == "A",
            "typed quote issuer or asset class differs")
    require(re.fullmatch(r"[0-9]+", t[73]) is not None, "Tencent total shares is not an integer")
    total = amount(t[73]); sina_total = amount(info[7]) * 10000
    require(sina_total == sina_total.to_integral_value() and total == sina_total,
            "typed quote total shares disagree")
    require(re.fullmatch(r"[0-9]{14}", t[30]) is not None
            and re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", s[30]) is not None
            and re.fullmatch(r"[0-9]{2}:[0-9]{2}:[0-9]{2}", s[31]) is not None,
            "typed quote source clock malformed")
    bj = timezone(timedelta(hours=8))
    stamps = [datetime.strptime(t[30], "%Y%m%d%H%M%S").replace(tzinfo=bj),
              datetime.fromisoformat(s[30] + "T" + s[31]).replace(tzinfo=bj)]
    for stamp, receipt in zip(stamps, receipts):
        complete = clock(receipt["http_complete"])
        require(datetime.fromisoformat(end + "T15:00:00+08:00") <= stamp <= complete,
                "typed quote is not a close-confirmed target frame")
        if stamp.date() > day(end):
            require(stamp.date() == complete.astimezone(bj).date(), "later typed quote is not from observation day")
    return dict(kind=TYPED_QUOTE, symbol=symbol, total_shares=str(total),
                source_market_dates=dict(tencent=stamps[0].date().isoformat(), sina=stamps[1].date().isoformat()),
                source_market_dates_agree=stamps[0].date() == stamps[1].date(),
                tencent_frame_time=stamps[0].isoformat(), sina_frame_time=stamps[1].isoformat(),
                tencent_total_field=73, tencent_unit="shares", sina_total_field=7, sina_unit="10000_shares",
                source_names=dict(tencent=t[1], sina=s[0], sina_information=info[22]),
                source_names_agree=t[1] == s[0] == info[22],
                independent_capital_asof_guarantee_proven=False)


def _failure_files(directory, source, *, symbol, end):
    """Authenticate this issuer/date's retained one/four failed main requests."""
    entries = []
    for number in range(1, 5):
        receipt = directory / f"quote_attempt{number}.receipt.json"
        transport = directory / f"quote_attempt{number}.transport.json"
        require(not (receipt.exists() and transport.exists()), "ambiguous primary failure observation")
        path = receipt if receipt.exists() else transport
        if not path.exists():
            break
        item = dict(receipt=dict(path=path.relative_to(source).as_posix(), sha256=sha(path.read_bytes())))
        if receipt.exists():
            raw = directory / f"quote_attempt{number}.raw"
            item["raw"] = dict(path=raw.relative_to(source).as_posix(), sha256=sha(raw.read_bytes()))
        entries.append(item)
    return entries


def _verify_primary_failure(record, *, symbol, end, bound, now):
    if record.get("schema") == "quote_primary_endpoint_failure_v1":
        require(record.get("symbol") == symbol and record.get("target") == end,
                "endpoint failure target differs")
        spec = record["original_failure"]
        original = json.loads(bound(spec["path"], spec["sha256"]))
        require(original.get("schema") == "quote_primary_transport_failure_v1", "endpoint failure nesting forbidden")
        previous = _verify_primary_failure(original, symbol=original["symbol"], end=end, bound=bound, now=now)
        selected = clock(record["selected_at"])
        require(previous["completed_at"] <= selected <= previous["completed_at"] + timedelta(minutes=30)
                and selected.astimezone(timezone(timedelta(hours=8))).date()
                == previous["completed_at"].astimezone(timezone(timedelta(hours=8))).date(),
                "endpoint failure cooling/date scope expired")
        return dict(completed_at=previous["completed_at"], selected_at=selected, origin=original["symbol"])
    require(record.get("schema") == "quote_primary_transport_failure_v1"
            and record.get("symbol") == symbol and record.get("target") == end
            and isinstance(record.get("attempts"), list) and 1 <= len(record["attempts"]) <= 4,
            "primary failure budget differs")
    completed = []
    for item in record["attempts"]:
        receipt = json.loads(bound(item["receipt"]["path"], item["receipt"]["sha256"]))
        require(receipt.get("url") == QUOTE_URL and receipt.get("params") == quote_params(symbol),
                "primary failure endpoint or issuer differs")
        start, complete = clock(receipt["http_start"]), clock(receipt["http_complete"])
        require(datetime.fromisoformat(end + "T15:00:00+08:00") <= start <= complete <= now,
                "primary failure target or clock differs")
        completed.append(complete)
        if "raw" in item:
            raw = bound(item["raw"]["path"], item["raw"]["sha256"])
            require(receipt.get("http_status") in (500, 502, 503, 504)
                    and receipt.get("tls_verification") is True
                    and receipt.get("raw_sha256") == sha(raw) and receipt.get("bytes") == len(raw),
                    "primary failure is not an authenticated HTTP 5xx")
        else:
            require(receipt.get("status") == "transport_failure_no_response"
                    and receipt.get("error_type") in ("ReadTimeout", "ConnectTimeout", "ConnectionError"),
                    "primary failure is not a response-free transport error")
    return dict(completed_at=max(completed), selected_at=None, origin=symbol)


def verify_typed_quote(manifest, *, symbol, end, bound, now):
    require(manifest.get("quote_source") == TYPED_QUOTE and "quote" not in manifest["files"]
            and "quote_dated_corroboration" not in manifest and "quote_dated_corroboration" not in manifest["files"],
            "ambiguous typed quote selection")
    failure = manifest["quote_primary_failure"]
    cause = _verify_primary_failure(json.loads(bound(failure["path"], failure["sha256"])),
                                    symbol=symbol, end=end, bound=bound, now=now)
    urls = typed_quote_urls(manifest["quote_symbols"])
    pairs = [_artifact(manifest["files"][key], bound, url=url, params={}, end=end, now=now)
             for key, url in zip(("tencent_quote", "sina_quote"), urls)]
    for key, url in zip(("tencent_quote", "sina_quote"), urls):
        entry = manifest["files"][key]
        if "attempt_budget" in entry:
            _verify_backup_budget(entry, bound=bound, url=url, purpose=key, end=end, now=now)
    if cause["selected_at"] is not None:
        require(all(cause["selected_at"] <= clock(pair[1]["http_start"])
                    and ("attempt_budget" in manifest["files"][key]
                         or clock(pair[1]["http_start"]) <= cause["selected_at"] + timedelta(seconds=90))
                    for key, pair in zip(("tencent_quote", "sina_quote"), pairs)),
                "typed fallback observations do not belong to the selected endpoint outage")
    return parse_typed_quote(pairs[0][0], pairs[1][0], symbol=symbol, symbols=manifest["quote_symbols"],
                             end=end, receipts=[pair[1] for pair in pairs])


BACKUP_COOLDOWN = timedelta(minutes=30)


class QuoteRecoveryPending(RuntimeError):
    def __init__(self, directory, retry_after):
        self.state = "quote_backup_recovery_pending"
        self.directory = str(directory); self.retry_after = retry_after
        super().__init__(self.state + "; retry_after=" + retry_after.isoformat() + "; evidence=" + str(directory))


class QuoteRecoveryExhausted(RuntimeError):
    state = "quote_backup_recovery_exhausted"


def _backup_query_directory(source, *, url, end, purpose):
    key = sha(json.dumps([url, {}, end], sort_keys=True, separators=(",", ":")).encode())
    return Path(source) / "quote_failover" / "backup" / end / (purpose + "-" + key)


def _backup_state(read, *, url, purpose, end, now):
    """Reconstruct an original attempt from bytes, including an interrupted GET."""
    raw = read("intent.json")
    if raw is None:
        return None
    intent = json.loads(raw); created = clock(intent["created_at"])
    require(intent.get("schema") == "quote_backup_attempt_v1" and intent.get("url") == url
            and intent.get("params") == {} and intent.get("target") == end
            and intent.get("purpose") == purpose and intent.get("max_attempts") == 1
            and created <= now, "backup attempt identity or clock differs")
    if read("terminal.json") is not None:
        return dict(status="blocked", cooling_from=created)
    body, receipt_raw = read(purpose + ".raw"), read(purpose + ".receipt.json")
    if body is not None and receipt_raw is not None:
        values = {"body": body, "receipt": receipt_raw}
        entry = dict(raw_path="body", raw_sha256=sha(body), receipt_path="receipt", receipt_sha256=sha(receipt_raw))
        _, receipt = _artifact(entry, lambda path, digest: values[path] if sha(values[path]) == digest else None,
                               url=url, params={}, end=end, now=now)
        require(created <= clock(receipt["http_start"]), "backup success precedes its durable intent")
        return dict(status="complete", cooling_from=created)
    if body is not None or receipt_raw is not None:
        return dict(status="unknown", cooling_from=created)
    failed = read(purpose + "_attempt1.receipt.json")
    transport = read(purpose + "_attempt1.transport.json")
    require(not (failed is not None and transport is not None), "backup failure observation ambiguous")
    if failed is None and transport is None:
        return dict(status="unknown", cooling_from=created)
    record = json.loads(failed if failed is not None else transport)
    start, complete = clock(record["http_start"]), clock(record["http_complete"])
    require(record.get("url") == url and record.get("params") == {}
            and created <= start and datetime.fromisoformat(end + "T15:00:00+08:00") <= start <= complete <= now,
            "backup failure endpoint or clock differs")
    if failed is not None:
        data = read(purpose + "_attempt1.raw")
        require(data is not None and record.get("raw_sha256") == sha(data)
                and record.get("bytes") == len(data) and record.get("tls_verification") is True,
                "backup failed HTTP raw differs")
        retryable = record.get("http_status") in (500, 502, 503, 504)
    else:
        retryable = record.get("status") == "transport_failure_no_response" and record.get("error_type") in (
            "ReadTimeout", "ConnectTimeout", "ConnectionError")
    return dict(status="retryable" if retryable else "blocked", cooling_from=complete)


def _backup_read(folder):
    return lambda name: (folder / name).read_bytes() if (folder / name).is_file() else None


def _backup_once(path, body):
    if path.exists():
        require(path.read_bytes() == body, "preserved backup observation changed")
        return False
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("xb") as stream:
            stream.write(body); stream.flush(); os.fsync(stream.fileno())
        return True


def _verify_backup_budget(entry, *, bound, url, purpose, end, now):
    spec = entry["attempt_budget"]; raw = bound(spec["path"], spec["sha256"]); budget = json.loads(raw)
    require(budget.get("schema") == "quote_backup_single_recovery_v1" and budget.get("url") == url
            and budget.get("target") == end and budget.get("purpose") == purpose
            and budget.get("selected") in ("initial", "recovery") and 0 < len(budget.get("files", {})) <= 20,
            "backup budget identity differs")
    values = {path: bound(path, digest) for path, digest in budget["files"].items()}
    prefix = spec["path"].rsplit("/", 1)[0] + "/"
    require(all(path.startswith(prefix) for path in values), "backup budget object escaped query scope")
    state = {}
    for attempt in ("initial", "recovery"):
        state[attempt] = _backup_state(lambda name: values.get(prefix + attempt + "/" + name),
            url=url, purpose=purpose, end=end, now=now)
    chosen = state[budget["selected"]]
    require(chosen is not None and chosen["status"] == "complete", "backup selected attempt is incomplete")
    if budget["selected"] == "recovery":
        old = state["initial"]; intent = json.loads(values[prefix + "recovery/intent.json"])
        require(old is not None and old["status"] in ("unknown", "retryable")
                and clock(intent["created_at"]) >= old["cooling_from"] + BACKUP_COOLDOWN,
                "backup recovery precedes cooling or repeats a blocked source")
    actual = {key: entry[key] for key in ("raw_path", "raw_sha256", "receipt_path", "receipt_sha256")}
    selected = prefix + budget["selected"] + "/" + purpose
    require(budget.get("success") == actual and actual["raw_path"] == selected + ".raw"
            and actual["receipt_path"] == selected + ".receipt.json"
            and budget["files"].get(actual["raw_path"]) == actual["raw_sha256"]
            and budget["files"].get(actual["receipt_path"]) == actual["receipt_sha256"],
            "backup success entry differs from selected attempt closure")


def _collect_backup(*, directory, source, url, purpose, end):
    now = datetime.now(timezone.utc)
    def entry(folder):
        body, receipt = folder / (purpose + ".raw"), folder / (purpose + ".receipt.json")
        return dict(raw_path=body.relative_to(source).as_posix(), raw_sha256=sha(body.read_bytes()),
            receipt_path=receipt.relative_to(source).as_posix(), receipt_sha256=sha(receipt.read_bytes()))
    # Preserve and reuse a successful d2d-era typed observation at its old ABI.
    if (directory / (purpose + ".raw")).is_file() and (directory / (purpose + ".receipt.json")).is_file():
        return entry(directory)
    budget = _backup_query_directory(source, url=url, end=end, purpose=purpose)
    initial, recovery = budget / "initial", budget / "recovery"
    initial.mkdir(parents=True, exist_ok=True)
    state = _backup_state(_backup_read(initial), url=url, purpose=purpose, end=end, now=now)
    chosen = initial
    if state is not None and state["status"] != "complete":
        if state["status"] == "blocked":
            raise ValueError("Backup source refusal/TLS is terminal; original evidence preserved")
        old = _backup_state(_backup_read(recovery), url=url, purpose=purpose, end=end, now=now)
        if old is not None:
            if old["status"] != "complete":
                raise QuoteRecoveryExhausted("Backup recovery already started or failed; original attempts preserved")
            chosen = recovery; state = old
        else:
            retry_after = state["cooling_from"] + BACKUP_COOLDOWN
            if now < retry_after:
                raise QuoteRecoveryPending(budget, retry_after)
            chosen = recovery; state = None
    if state is None:
        chosen.mkdir(parents=True, exist_ok=True)
        intent = dict(schema="quote_backup_attempt_v1", url=url, params={}, target=end, purpose=purpose,
                      created_at=now.isoformat(), max_attempts=1)
        if not _backup_once(chosen / "intent.json", json.dumps(intent, sort_keys=True).encode()):
            raise QuoteRecoveryPending(budget, now + BACKUP_COOLDOWN)
        try:
            _save_public_observation(directory=chosen, source=source, url=url, params={}, purpose=purpose,
                end=end, max_attempts=1, referer="https://finance.sina.com.cn/" if purpose == "sina_quote" else None)
        except (requests.exceptions.SSLError, requests.HTTPError) as exc:
            _backup_once(chosen / "terminal.json", json.dumps(dict(error_type=type(exc).__name__,
                recorded_at=datetime.now(timezone.utc).isoformat(), no_retry=True)).encode())
            raise
        except RuntimeError:
            observed = _backup_state(_backup_read(chosen), url=url, purpose=purpose, end=end, now=datetime.now(timezone.utc))
            if chosen == recovery:
                raise QuoteRecoveryExhausted("Backup recovery failed; original attempts preserved")
            raise QuoteRecoveryPending(budget, observed["cooling_from"] + BACKUP_COOLDOWN)
    result = entry(chosen)
    allowed = {"intent.json", "terminal.json", purpose + ".raw", purpose + ".receipt.json",
        purpose + "_attempt1.raw", purpose + "_attempt1.receipt.json", purpose + "_attempt1.transport.json"}
    files = {path.relative_to(source).as_posix(): sha(path.read_bytes())
        for folder in (initial, recovery) if folder.exists() for path in folder.iterdir() if path.name in allowed}
    body = json.dumps(dict(schema="quote_backup_single_recovery_v1", url=url, target=end, purpose=purpose,
        selected=chosen.name, success=result, files=files), sort_keys=True).encode()
    manifest = budget / "success.json"; _backup_once(manifest, body)
    result["attempt_budget"] = dict(path=manifest.relative_to(source).as_posix(), sha256=sha(body))
    return result


def collect_dual_quote(*, directory, source, symbols, end):
    """Two provider-native observations with durable, bounded source recovery."""
    return {key: _collect_backup(directory=Path(directory), source=Path(source), url=url, purpose=key, end=end)
        for key, url in zip(("tencent_quote", "sina_quote"), typed_quote_urls(symbols))}


def collect_quote_inputs(*, directory, source, symbol, end):
    """One main attempt, or retained exhaustion; never reset a failed budget."""
    directory, source = Path(directory), Path(source)
    now = datetime.now(timezone.utc)
    def bound(relative, digest):
        path = (source / relative).resolve(); require(path.is_relative_to(source.resolve()), "quote source escaped root")
        raw = path.read_bytes(); require(sha(raw) == digest, "quote source hash differs"); return raw
    legacy, legacy_receipt = directory / "quote.raw", directory / "quote.receipt.json"
    if legacy.exists() or legacy_receipt.exists():
        require(legacy.exists() and legacy_receipt.exists(), "incomplete primary observation preserved")
        entry = dict(raw_path=legacy.relative_to(source).as_posix(), raw_sha256=sha(legacy.read_bytes()),
                     receipt_path=legacy_receipt.relative_to(source).as_posix(), receipt_sha256=sha(legacy_receipt.read_bytes()))
        _artifact(entry, bound, url=QUOTE_URL, params=quote_params(symbol), end=end, now=now)
        return {}, {"quote": entry}
    ledger = source / "quote_failover" / symbol / end / "primary_failure.json"
    ledger.parent.mkdir(parents=True, exist_ok=True)
    if not ledger.exists():
        attempts = _failure_files(directory, source, symbol=symbol, end=end)
        if not attempts:
            intent = ledger.with_name("primary_started.json")
            require(not intent.exists(), "primary request outcome unknown; budget preserved")
            cached = _QUOTE_ENDPOINT_FAILURES.get((str(source.resolve()), end))
            if cached and now <= cached["expires_at"] and now.astimezone(timezone(timedelta(hours=8))).date() == cached["observation_day"]:
                record = dict(schema="quote_primary_endpoint_failure_v1", symbol=symbol, target=end,
                    original_failure=cached["reference"], selected_at=now.isoformat())
            else:
                with intent.open("xb") as stream:
                    stream.write(json.dumps(dict(symbol=symbol, target=end, started_at=now.isoformat(), max_attempts=1)).encode())
                try:
                    entry, _ = _save_public_observation(directory=directory, source=source, url=QUOTE_URL,
                        params=quote_params(symbol), purpose="quote", end=end, max_attempts=1)
                    return {}, {"quote": entry}
                except RuntimeError as exc:
                    if not str(exc).startswith("SHARE_REQUEST_RETRY_EXHAUSTED: quote"):
                        raise
                attempts = _failure_files(directory, source, symbol=symbol, end=end)
                record = dict(schema="quote_primary_transport_failure_v1", symbol=symbol, target=end, attempts=attempts)
        else:
            record = dict(schema="quote_primary_transport_failure_v1", symbol=symbol, target=end, attempts=attempts)
        record["observation_directory"] = directory.relative_to(source).as_posix()
        _verify_primary_failure(record, symbol=symbol, end=end, bound=bound, now=datetime.now(timezone.utc))
        with ledger.open("xb") as stream:
            stream.write(json.dumps(record, ensure_ascii=False, indent=2).encode())
    reference = dict(path=ledger.relative_to(source).as_posix(), sha256=sha(ledger.read_bytes()))
    record = json.loads(bound(reference["path"], reference["sha256"]))
    cause = _verify_primary_failure(record, symbol=symbol, end=end, bound=bound, now=datetime.now(timezone.utc))
    if cause["selected_at"] is None:
        _QUOTE_ENDPOINT_FAILURES[str(source.resolve()), end] = dict(reference=reference,
            expires_at=cause["completed_at"] + timedelta(minutes=30),
            observation_day=cause["completed_at"].astimezone(timezone(timedelta(hours=8))).date())
    observation = (source / record["observation_directory"]).resolve()
    require(observation.is_relative_to(source.resolve()) and observation.is_dir(), "typed quote observation directory escaped")
    files = collect_dual_quote(directory=observation, source=source, symbols=[symbol], end=end)
    return dict(quote_source=TYPED_QUOTE, quote_symbols=[symbol], quote_primary_failure=reference), files


def _em_date(value):
    require(isinstance(value, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}(?: 00:00:00)?", value), "missing/ambiguous EM date")
    return day(value[:10])


def _em_inventory(entries, bound, symbol, end, bounded, now, approved_event=None, reviewed_future_rows=()):
    require(isinstance(entries, list) and entries, "missing complete EM inventory")
    rows, counts, versions = [], set(), set()
    for index, entry in enumerate(entries, 1):
        raw, _ = _artifact(entry, bound, url=EM_URL, params=em_params(symbol, end, bounded, index), end=end, now=now)
        payload = json.loads(raw, parse_float=Decimal)
        require(payload.get("success") is True and payload.get("code") == 0 and isinstance(payload.get("result"), dict), "EM not a successful counted inventory")
        result = payload["result"]
        require(isinstance(result.get("data"), list), "EM data not list")
        pages, count = integer(result.get("pages")), integer(result.get("count"))
        require(pages == len(entries) and pages > 0 and count > 0, "EM page inventory incomplete")
        counts.add(count)
        versions.add(payload.get("version"))
        rows.extend(result["data"])
    require(len(counts) == 1 and len(rows) == next(iter(counts)), "EM count drift/truncated inventory")
    # Version may be null in real responses. Equal counts plus exact independent
    # bounded/unfiltered inventories below provide the cross-query comparison.
    require(len(versions) == 1, "EM version drift")
    require(isinstance(reviewed_future_rows,(list,tuple)) and len(reviewed_future_rows)<=2
            and all(isinstance(row,dict) and sum(row==item for item in rows)==1
                    and _em_date(row['END_DATE'])>day(end) for row in reviewed_future_rows),
            'reviewed future row inventory differs')
    identities, dates = set(), []
    for row in rows:
        require(row.get("SECUCODE") == em_symbol(symbol) and row.get("SECURITY_CODE") == symbol, "EM issuer differs")
        ds = tuple(_em_date(row.get(key)) for key in DATE_FIELDS)
        require((ds[2] <= day(end) or _matches_approved_event(row, approved_event)
                 or any(row==item for item in reviewed_future_rows))
                and (not bounded or ds[0] <= day(end)), "future announcement or wrong bounded inventory")
        shares = amount(row.get("TOTAL_SHARES"))
        require(shares == shares.to_integral_value(), "EM total not whole shares")
        identity = ds[0]
        require(identity not in identities, "duplicate EM event date")
        identities.add(identity)
        dates.append(ds[0])
    require(dates == sorted(dates, reverse=True), "EM event order differs")
    return rows


def plan_reviews(root=None):
    root = Path(root) if root is not None else Path(__file__).resolve().parents[1]
    registry = json.loads((root / "data/share_source_future_plan_reviews.json").read_bytes())
    require(set(registry) in ({"schema", "records"}, {"schema", "records", "unchanged_event_approval"}, {"schema", "records", "unchanged_event_approval", "sse_transition_approval"}) and registry["schema"] == "share_future_plan_reviews_v1"
            and isinstance(registry["records"], list), "future plan registry schema differs")
    required = {"symbol", "announcement_id", "notice_number", "provider_notice_date", "document_signature_date",
                "planned_date", "before_total_shares", "after_total_shares", "change_total_shares", "status",
                "pdf_url", "pdf_alternate_urls", "pdf_sha256", "pdf_bytes"}
    identities = set()
    for item in registry["records"]:
        require(isinstance(item, dict) and set(item) == required, "future plan registry fields differ")
        em_symbol(item["symbol"])
        ident = item["symbol"], item["planned_date"]
        require(ident not in identities, "duplicate future plan review")
        identities.add(ident)
        require(item["status"] == "verified_future_plan_not_executed"
                and day(item["document_signature_date"]) <= day(item["provider_notice_date"]) < day(item["planned_date"]), "invalid reviewed plan dates/status")
        before, after = integer(item["before_total_shares"]), integer(item["after_total_shares"])
        require(before > 0 and after > 0 and before != after
                and isinstance(item["change_total_shares"], int) and not isinstance(item["change_total_shares"], bool)
                and after - before == item["change_total_shares"], "reviewed plan count identity differs")
        require(re.fullmatch(r"AN[0-9]+", item["announcement_id"])
                and re.fullmatch(r"[0-9]{4}-[0-9]+", item["notice_number"])
                and item["pdf_url"] == "https://pdf.dfcfw.com/pdf/H2_" + item["announcement_id"] + "_1.pdf"
                and re.fullmatch(r"[0-9a-f]{64}", item["pdf_sha256"])
                and integer(item["pdf_bytes"]) > 0, "reviewed PDF identity differs")
        alternatives = item["pdf_alternate_urls"]
        require(isinstance(alternatives, list) and len(alternatives) == len(set(alternatives)), "reviewed alternate PDF URLs differ")
        for url in alternatives:
            parsed = urlparse(url)
            require(parsed.scheme == "https" and parsed.netloc and parsed.username is None and parsed.password is None
                    and not parsed.query and not parsed.fragment and url != item["pdf_url"], "unsafe reviewed PDF URL")
    return registry["records"]


def _approved_unchanged_event(*, root, symbol, previous_day, end, candidate, now):
    """One user-approved late-disclosure exception; never a generic date waiver.

    The sealed ZIP is the exact reviewed public-source package, not a source of
    current quotes. Every normal live inventory/quote gate still runs below.
    """
    identity = (symbol, previous_day, end)
    reobserved = identity == ("301288", "2026-11-05", "2026-11-06")
    if identity != ("301288", "2026-11-04", "2026-11-05") and not reobserved:
        return None
    root = Path(root) if root is not None else Path(__file__).resolve().parents[1]
    record = json.loads((root / "data/share_source_future_plan_reviews.json").read_bytes()).get("unchanged_event_approval")
    require(isinstance(record, dict) and set(record) == {"approval", "package_base64"}, "missing exact event approval")
    approval = record["approval"]
    package = base64.b64decode(record["package_base64"], validate=True)
    digest = "3af53c7fea86b3334321112981747560e9fcea9dba5ee7667c9f011d8a70f4ff"
    review_sha = "b716606578531dbb67ed5f1ea20d91f25330cac284e5f96818d4ae65bfa4cfc9"
    require(sha(package) == digest and approval.get("decision") == "approved"
            and approval.get("package_sha256") == digest and approval.get("review_sha256") == review_sha,
            "exact event approval/package differs")
    require(datetime.fromisoformat("2026-11-06T00:00:00+08:00") <= clock(approval["recorded_at"]) <= now,
            "event approval clock differs")
    with zipfile.ZipFile(io.BytesIO(package)) as archive:
        review_raw = archive.read("review.json")
        require(sha(review_raw) == review_sha, "event review differs")
        review = json.loads(review_raw)
        require(review["approved"] is False and sha(candidate) == review["history_candidate_sha256_unchanged"],
                "event old history differs")
        pdf = archive.read("issuer_notice.pdf")
        require(sha(pdf) == review["issuer_pdf_sha256"] and pdf.startswith(b"%PDF-"), "event issuer PDF differs")
        receipt = json.loads(archive.read("issuer_notice_pdf.receipt.json"))
        require(clock(receipt["http_start"]) <= clock(receipt["http_complete"]) <= now, "event evidence clock differs")
        row = json.loads(archive.read("bounded.raw"), parse_float=Decimal)["result"]["data"][0]
    require(review["before_total_shares"] == review["after_total_shares"] == 108010000
            and review["share_delta"] == 0, "event changes total shares")
    result = dict(row=row, package_sha256=digest, review_sha256=review_sha,
                supplemental_verification_day="2026-11-06", signal_day=end,
                disclosure="301288：11/06补充核验11/05公告事件，总股本108010000股不变；不声称11/05收盘前已取得该公告。")
    if reobserved:
        result.update(signal_day="2026-11-05", qualification_day=end,
                      reobserved_existing_approved_event=True,
                      disclosure="301288：11/06资料维护重新观测已精确批准的11/05零总股本变动事件；原批准日期、公告原行及历史CSV保持不变。")
    return result


def _matches_approved_event(row, event):
    return event is not None and row == event["row"]


def _later_quote_plan(*, symbol, end, boundary, data, receipt, plans, root):
    """Select one already reviewed bridge, never infer a date from quote f84."""
    require(isinstance(plans, list) and len(plans) == 1, "later quote needs one exact reviewed plan")
    plan = plans[0]
    stamp = datetime.fromtimestamp(integer(data.get("f86")), timezone.utc)
    frame = stamp.astimezone(timezone(timedelta(hours=8))).date()
    start, complete = clock(receipt["http_start"]), clock(receipt["http_complete"])
    require(day(end) < frame == day(plan["date"])
            and start.astimezone(timezone(timedelta(hours=8))).date() == frame
            and complete.astimezone(timezone(timedelta(hours=8))).date() == frame
            and stamp <= complete, "later quote and reviewed plan observation dates differ")
    matches = [r for r in plan_reviews(root) if r["symbol"] == symbol and r["planned_date"] == plan["date"]]
    require(len(matches) == 1, "later quote reviewed plan is missing or ambiguous")
    review = matches[0]
    before, after = Decimal(review["before_total_shares"]), Decimal(review["after_total_shares"])
    delta = review["change_total_shares"]
    require(type(delta) is int and delta != 0 and after - before == delta
            and before == boundary * 10000
            and after == amount(plan["total_shares_10k"]) * 10000 == amount(data.get("f84"))
            and after != before, "later quote reviewed before/after/delta differs")
    return review, stamp, complete


def verify_quote_dated_corroboration(manifest, *, symbol, end, boundary, data, receipt,
                                    sina, plans, bound, now, root):
    """Reconcile later provider observations; assert no legal event execution.

    The original target-day daily/EM/Sina checks run before this function. An
    extra, independently collected Sina inventory corroborates the later quote;
    neither the quote timestamp nor an issuer plan dates the target's f84.
    """
    require(manifest.get("quote_dated_corroboration") == {"schema": QUOTE_CORROBORATION_KIND},
            "later quote corroboration is not explicitly selected")
    review, stamp, quote_complete = _later_quote_plan(symbol=symbol, end=end, boundary=boundary,
                                                     data=data, receipt=receipt, plans=plans, root=root)
    raw, after_receipt = _artifact(manifest["files"]["quote_dated_corroboration"], bound,
        url=SINA_URL.format(symbol=symbol), params={}, end=end, now=now)
    start, complete = clock(after_receipt["http_start"]), clock(after_receipt["http_complete"])
    frame = stamp.astimezone(timezone(timedelta(hours=8))).date()
    require(quote_complete <= start <= complete
            and start.astimezone(timezone(timedelta(hours=8))).date() == frame
            and complete.astimezone(timezone(timedelta(hours=8))).date() == frame,
            "later Sina observation is not after the quote on its reviewed provider date")
    after = parse_sina_total_history(raw, symbol=symbol, observed_session=end, allowed_future_plans=plans)
    require(after["records"] == sina["records"], "later Sina complete inventory changed")
    target = [r for r in after["records"] if day(r["provider_date"]) <= day(end)]
    future = [r for r in after["records"] if day(r["provider_date"]) > day(end)]
    require(target and amount(target[-1]["total_shares_10k"]) == boundary
            and len(future) == 1 and future[0]["provider_date"] == review["planned_date"]
            and amount(future[0]["total_shares"]) == amount(data.get("f84")) == review["after_total_shares"],
            "later Sina target or reviewed after total differs")
    return dict(schema=QUOTE_CORROBORATION_KIND, basis="later_provider_observation_reconciled",
                target_session=end, target_total_shares=str(boundary * 10000),
                later_provider_date=frame.isoformat(), later_total_shares=str(amount(data.get("f84"))),
                announcement_id=review["announcement_id"], issuer_document_sha256=review["pdf_sha256"],
                sina_after_raw_sha256=sha(raw), quote_frame_time=stamp.isoformat(),
                f84_independent_asof_guarantee_proven=False, execution_or_legal_effect_not_asserted=True)


def collect_quote_dated_corroboration(manifest, *, symbol, end, boundary, bound, now, root, save):
    """Only supplement a verified target whose sole remaining error is f84.

    Callers must first run their complete original verifier and see its exact
    terminal-total mismatch. Existing target artifacts and their clocks stay.
    """
    require(os.environ.get(QUOTE_CORROBORATION_ENV) == "1", "later quote collection is not opted in")
    files = manifest["files"]
    raw, receipt = _artifact(files["quote"], bound, url=QUOTE_URL,
                            params=quote_params(symbol), end=end, now=now)
    quote = json.loads(raw, parse_float=Decimal)
    data = quote.get("data")
    require(quote.get("rc") == 0 and isinstance(data, dict) and data.get("f57") == symbol,
            "later quote issuer/status differs")
    unfiltered = _em_inventory(files["unfiltered"], bound, symbol, end, False, now)
    future = [r for r in unfiltered if _em_date(r["END_DATE"]) > day(end)]
    plans, _ = _reviewed_future_plans(manifest, future, symbol=symbol, end=end, boundary=boundary,
                                      bound=bound, now=now, root=root)
    review, _, _ = _later_quote_plan(symbol=symbol, end=end, boundary=boundary, data=data,
                                     receipt=receipt, plans=plans, root=root)
    purpose = "sina_quote_date_" + review["planned_date"]
    files["quote_dated_corroboration"], _ = save(SINA_URL.format(symbol=symbol), {}, purpose)
    manifest["quote_dated_corroboration"] = {"schema": QUOTE_CORROBORATION_KIND}


def _reviewed_future_plans(manifest, future, *, symbol, end, boundary, bound, now, root):
    documents = manifest.get("future_plan_documents", [])
    require(isinstance(documents, list), "future document inventory not list")
    require(len(documents) == len(future), "missing/extra reviewed future document")
    if not future:
        return [], []
    reviews = plan_reviews(root)
    allowed, used, disclosures = [], set(), []
    for row in future:
        end_day, listing, notice = (_em_date(row[key]) for key in DATE_FIELDS)
        require(end_day == listing and end_day > day(end) and notice <= day(end), "ambiguous future plan dates")
        matches = [r for r in reviews if r["symbol"] == symbol and r["planned_date"] == end_day.isoformat()]
        require(len(matches) == 1, "future event has no exact reviewed plan")
        review = matches[0]
        require(review["announcement_id"] not in used, "future plan duplicated")
        used.add(review["announcement_id"])
        require(review["provider_notice_date"] == notice.isoformat()
                and Decimal(review["before_total_shares"]) / 10000 == boundary
                and Decimal(review["after_total_shares"]) == amount(row["TOTAL_SHARES"]), "future plan shares/notice differ")
        require(row.get("INFO_CODE") == review["announcement_id"], "future announcement id differs")
        refs = [r for r in documents if r.get("announcement_id") == review["announcement_id"]]
        require(len(refs) == 1, "future PDF reference differs")
        document = refs[0]
        raw = bound(document["raw_path"], document["raw_sha256"])
        require(document["raw_sha256"] == review["pdf_sha256"] and len(raw) == review["pdf_bytes"]
                and raw.startswith(b"%PDF-") and b"%%EOF" in raw[-1024:], "reviewed PDF bytes differ")
        receipt = json.loads(bound(document["receipt_path"], document["receipt_sha256"]))
        headers = receipt.get("response_headers", {})
        require("request_headers" not in receipt and "headers" not in receipt and isinstance(headers, dict)
                and all(isinstance(k, str) and k.lower() in SAFE_HEADERS and isinstance(v, str)
                        and "\r" not in v and "\n" not in v for k, v in headers.items()), "private PDF receipt headers")
        schema = receipt.get("original_receipt_schema", receipt.get("schema"))
        if schema == "issuer_pdf_observation_v1":
            requested, actual = receipt.get("requested_url"), receipt.get("final_url")
            require(actual is not None and receipt.get("redirects_allowed") is False,
                    "issuer PDF receipt missing recorded final URL/redirect configuration")
            tls, status, size = receipt.get("tls_verify_configuration"), receipt.get("status"), receipt.get("raw_bytes")
            start, complete = clock(receipt["observation_start"]), clock(receipt["observation_end"])
        else:
            requested = receipt.get("url")
            actual = receipt.get("actual_url", receipt.get("final_url"))
            tls, status, size = receipt.get("tls_verification"), receipt.get("http_status"), receipt.get("bytes")
            start, complete = clock(receipt["http_start"]), clock(receipt["http_complete"])
        trusted_urls = [review["pdf_url"], *review["pdf_alternate_urls"]]
        require(requested in trusted_urls and tls is True and status == 200
                and receipt.get("raw_sha256") == review["pdf_sha256"] and size == review["pdf_bytes"], "reviewed PDF receipt/TLS differs")
        require(actual is None or actual == requested, "reviewed PDF redirected to a different endpoint")
        if actual is not None:
            parsed = urlparse(actual)
            require(parsed.scheme == "https" and not parsed.query and not parsed.fragment, "reviewed PDF actual endpoint differs")
        require(datetime.fromisoformat(end + "T15:00:00+08:00") <= start <= complete <= now, "PDF observation clock differs")
        disclosures.append(dict(announcement_id=review["announcement_id"], requested_url=requested,
                                actual_url=actual, actual_url_recorded=actual is not None,
                                actual_url_basis="recorded_final_url" if actual is not None else "request_url_only_final_endpoint_not_recorded",
                                tls_configuration_recorded=True, tls_handshake_proof_recorded=False))
        allowed.append(dict(date=end_day.isoformat(), total_shares_10k=str(Decimal(review["after_total_shares"]) / 10000)))
    return allowed, disclosures


def verify_manifest(manifest, *, symbol, previous_day, end, candidate, bound, now=None, root=None):
    """Validate actual raw objects; caller recursively verifies original chain."""
    if manifest.get("schema") == "sse_dated_unchanged_tail_20261104_v1":
        from scripts.sse_share_daily_unchanged_20261104 import verify_manifest as verify_sse
        return verify_sse(manifest,symbol=symbol,previous_day=previous_day,end=end,candidate=candidate,bound=bound,now=now,root=root)
    now = now or datetime.now(timezone.utc)
    d, t = day(previous_day), day(end)
    require(d < t and manifest.get("schema") == KIND and manifest.get("symbol") == symbol
            and manifest.get("previous_day") == previous_day and manifest.get("query_end") == end,
            "manifest identity/window differs")
    require_adjacent(previous_day, end)
    old = pd.read_csv(io.BytesIO(candidate), dtype=str, keep_default_na=False)
    require(not old.empty and "change_date" in old and "total_shares_10k" in old, "missing original candidate")
    old_dates = [day(value[:10]) for value in old.change_date]
    require(old_dates == sorted(set(old_dates)) and max(old_dates) <= d, "old history after previous cutoff")
    boundary = amount(old.iloc[-1].total_shares_10k)
    approved_event = _approved_unchanged_event(root=root, symbol=symbol, previous_day=previous_day,
                                              end=end, candidate=candidate, now=now)
    files = manifest["files"]
    unfiltered = _em_inventory(files["unfiltered"], bound, symbol, end, False, now, approved_event)
    bounded = _em_inventory(files["bounded"], bound, symbol, end, True, now, approved_event)
    projection = [row for row in unfiltered if _em_date(row["END_DATE"]) <= t]
    require(projection == bounded, "EM bounded/full inventory drift or wrong asof projection")
    future = [row for row in unfiltered if _em_date(row["END_DATE"]) > t]
    future_plans, future_transport = _reviewed_future_plans(manifest, future, symbol=symbol, end=end, boundary=boundary,
                                           bound=bound, now=now, root=root)
    for row in projection:
        ds = [_em_date(row[key]) for key in DATE_FIELDS]
        if _matches_approved_event(row, approved_event):
            require(amount(row["TOTAL_SHARES"]) / 10000 == boundary, "approved event boundary differs")
            continue
        require(not any(d < dt <= t for dt in ds), "EM new or ambiguous tail event")
        require(not (min(ds) <= d < max(ds)), "EM event dates cross previous cutoff")
    require(projection and amount(projection[0]["TOTAL_SHARES"]) / 10000 == boundary, "EM boundary total differs")
    sina_raw, _ = _artifact(files["sina"], bound, url=SINA_URL.format(symbol=symbol), params={}, end=end, now=now)
    sina = parse_sina_total_history(sina_raw, symbol=symbol, observed_session=end, allowed_future_plans=future_plans)
    tail = [row for row in sina["records"] if d < day(row["provider_date"]) <= t]
    if approved_event is not None:
        require(sum(_matches_approved_event(row, approved_event) for row in projection) == 1,
                "approved event absent or duplicated")
        if approved_event.get("reobserved_existing_approved_event"):
            require(not tail and sina["records"][-1] == {
                "provider_date": "2026-11-05", "total_shares_10k": "10801", "total_shares": "108010000"},
                "reobserved approved event Sina identity differs")
        else:
            require(tail == [{"provider_date": end, "total_shares_10k": "10801", "total_shares": "108010000"}],
                    "approved event Sina identity differs")
    else:
        require(not tail, "Sina tail event")
    sina_asof = [row for row in sina["records"] if day(row["provider_date"]) <= t]
    require(sina_asof and amount(sina_asof[-1]["total_shares_10k"]) == boundary, "Sina boundary total differs")
    typed_quote = None
    if manifest.get("quote_source") is not None:
        typed_quote = verify_typed_quote(manifest, symbol=symbol, end=end, bound=bound, now=now)
        stamp = clock(typed_quote["tencent_frame_time"])
        historical = None
        if any(day(value) > day(end) for value in typed_quote["source_market_dates"].values()):
            require("historical_close" in files, "later typed quote needs dated target evidence")
            historical = _historical_close(files["historical_close"], bound, symbol=symbol,
                previous_day=previous_day, end=end, boundary=boundary, now=now)
        require(amount(typed_quote["total_shares"]) / 10000 == boundary, "typed quote total differs")
        corroboration = None
    else:
        qp = quote_params(symbol)
        quote_raw, quote_receipt = _artifact(files["quote"], bound, url=QUOTE_URL, params=qp, end=end, now=now)
        quote = json.loads(quote_raw, parse_float=Decimal)
        data = quote.get("data")
        require(quote.get("rc") == 0 and isinstance(data, dict) and data.get("f57") == symbol, "quote issuer/status differs")
        stamp = datetime.fromtimestamp(integer(data.get("f86")), timezone.utc)
        frame_day = stamp.astimezone(timezone(timedelta(hours=8))).date()
        historical = None
        if "historical_close" in files:
            # Only a genuinely later observation may take this path. A missing,
            # stale, future or conflicting quote clock remains a hard failure.
            observed_start = clock(quote_receipt["http_start"]).astimezone(timezone(timedelta(hours=8))).date()
            observed_end = clock(quote_receipt["http_complete"]).astimezone(timezone(timedelta(hours=8))).date()
            require(t < frame_day == observed_start == observed_end
                    and stamp <= clock(quote_receipt["http_complete"]), "later quote observation clock differs")
            historical = _historical_close(files["historical_close"], bound, symbol=symbol,
                                           previous_day=previous_day, end=end, boundary=boundary, now=now)
        else:
            require(frame_day == t
                    and datetime.fromisoformat(end + "T15:00:00+08:00") <= stamp <= clock(quote_receipt["http_complete"]), "quote frame not target close")
        extra_clock = data.get("f124")
        if extra_clock is not None:
            extra_epoch = integer(extra_clock)
            if extra_epoch:
                extra_stamp = datetime.fromtimestamp(extra_epoch, timezone.utc)
                require(extra_stamp.astimezone(timezone(timedelta(hours=8))).date() == frame_day
                        and (historical is not None or datetime.fromisoformat(end + "T15:00:00+08:00") <= extra_stamp)
                        and extra_stamp <= clock(quote_receipt["http_complete"]), "quote extra clock conflicts")
        corroboration = None
        if amount(data.get("f84")) / 10000 == boundary:
            require("quote_dated_corroboration" not in manifest and "quote_dated_corroboration" not in files,
                    "unused later quote corroboration")
        else:
            require("quote_dated_corroboration" in manifest, "quote total differs")
            corroboration = verify_quote_dated_corroboration(manifest, symbol=symbol, end=end, boundary=boundary,
                data=data, receipt=quote_receipt, sina=sina, plans=future_plans, bound=bound, now=now, root=root)
    facts = dict(kind=KIND, symbol=symbol, query_end=end, previous_day=previous_day,
                total_shares_10k=str(boundary), unchanged_observations=True,
                frame_time=stamp.isoformat(), f84_independent_asof_guarantee_proven=False,
                provider_latency_guaranteed=False, old_candidate_sha256=sha(candidate),
                historical_close_evidence=historical,
                quote_is_target_close_frame=historical is None,
                future_plans_preserved_not_consumed=future_plans, future_pdf_transport_disclosures=future_transport,
                approved_late_unchanged_event={key: value for key, value in approved_event.items() if key != "row"}
                    if approved_event is not None else None)
    if typed_quote is not None:
        facts.pop("f84_independent_asof_guarantee_proven", None)
        facts.pop("frame_time", None)
        facts["typed_quote"] = typed_quote
    if corroboration is not None:
        facts["quote_dated_corroboration"] = corroboration
    return facts


def _save_public_observation(*, directory, source, url, params, purpose, end, referer=None, max_attempts=4):
    """Spend the former two-run transport budget on this request only.

    Four attempts equal the previous two local attempts times two whole-refresh
    attempts. Exhaustion is terminal to the outer runner; completed inputs and
    earlier successful requests are never repeated to consume this budget.
    """
    require(type(max_attempts) is int and 1 <= max_attempts <= 4, "invalid public request attempt budget")
    for attempt in range(max_attempts):
        start = datetime.now(timezone.utc)
        require(start >= datetime.fromisoformat(end + "T15:00:00+08:00"), "not close eligible")
        try:
            headers={"User-Agent":"Mozilla/5.0"}
            if referer is not None:
                require(referer=="https://www.sse.com.cn/assortment/stock/list/info/company/index.shtml?COMPANY_CODE=603028"
                        or (referer=="https://finance.sina.com.cn/" and url.startswith("https://hq.sinajs.cn/list=")), "unknown source referer")
                headers["Referer"]=referer
            response = requests.get(url, params=params, headers=headers, timeout=20, verify=True)
        except requests.exceptions.SSLError:
            raise
        except (requests.Timeout, requests.ConnectionError) as error:
            failure = dict(url=url, params=params, http_start=start.isoformat(),
                           http_complete=datetime.now(timezone.utc).isoformat(),
                           error_type=type(error).__name__, status="transport_failure_no_response")
            (directory / f"{purpose}_attempt{attempt + 1}.transport.json").write_text(
                json.dumps(failure, ensure_ascii=False, indent=2), encoding="utf8")
            if attempt == max_attempts - 1:
                raise RuntimeError("SHARE_REQUEST_RETRY_EXHAUSTED: " + purpose) from error
            print(f"[share-source] transient public request {purpose}; retry={attempt + 1}/3", flush=True)
            time.sleep(20 if attempt == 1 else 1)
            continue
        try:
            raw = response.content
            complete = datetime.now(timezone.utc)
            stem = purpose if response.status_code < 400 else f"{purpose}_attempt{attempt + 1}"
            path = directory / (stem + ".raw")
            path.write_bytes(raw)
            receipt = dict(url=url, actual_url=response.url, params=params, http_start=start.isoformat(),
                           http_complete=complete.isoformat(), http_status=response.status_code, tls_verification=True,
                           raw_sha256=sha(raw), bytes=len(raw), schema="share_unchanged_tail_receipt_v1",
                           header_scope="allowlisted_no_cookies",
                           response_headers={k: v for k, v in response.headers.items() if k.lower() in SAFE_HEADERS})
            rp = directory / (stem + ".receipt.json")
            rp.write_text(json.dumps(receipt, ensure_ascii=False, indent=2), encoding="utf8")
            try:
                response.raise_for_status()
            except requests.HTTPError as error:
                if response.status_code in (500, 502, 503, 504):
                    if attempt == max_attempts - 1:
                        raise RuntimeError("SHARE_REQUEST_RETRY_EXHAUSTED: " + purpose) from error
                    print(f"[share-source] HTTP {response.status_code} {purpose}; retry={attempt + 1}/3", flush=True)
                    time.sleep(20 if attempt == 1 else 1)
                    continue
                raise
            return dict(raw_path=path.relative_to(source).as_posix(), raw_sha256=sha(raw),
                        receipt_path=rp.relative_to(source).as_posix(), receipt_sha256=sha(rp.read_bytes())), raw
        finally:
            response.close()
    raise RuntimeError("Public share-source request did not complete")


def collect_manifest(*, root, symbol, previous_day, end, candidate):
    """Real public HTTPS observations; only immutable evidence is created."""
    require(day(previous_day) < day(end), "invalid collection window")
    require_adjacent(previous_day, end)
    source = Path(root) / ".microcap_index_cache/share_sources"
    require(source.resolve().is_relative_to(Path(root).resolve()), "source root escapes workspace")
    directory = source / "providers" / KIND / symbol / uuid.uuid4().hex
    directory.mkdir(parents=True)
    def save(url, params, purpose):
        return _save_public_observation(directory=directory, source=source, url=url,
                                        params=params, purpose=purpose, end=end)
    files = {}
    for bounded, name in ((False, "unfiltered"), (True, "bounded")):
        entries, raw = [], None
        first, raw = save(EM_URL, em_params(symbol, end, bounded), name + "_1")
        entries.append(first)
        payload = json.loads(raw)
        require(payload.get("success") is True and payload.get("code") == 0 and isinstance(payload.get("result"), dict), "EM collection failed")
        pages = integer(payload["result"].get("pages"))
        require(1 <= pages <= 100, "EM page budget invalid")
        for page in range(2, pages + 1):
            entry, _ = save(EM_URL, em_params(symbol, end, bounded, page), name + "_" + str(page))
            entries.append(entry)
        files[name] = entries
    future_documents = []
    reviews = plan_reviews(root)
    for entry in files["unfiltered"]:
        payload = json.loads((source / entry["raw_path"]).read_bytes())
        for row in payload["result"]["data"]:
            if _em_date(row.get("END_DATE")) > day(end):
                matches = [r for r in reviews if r["symbol"] == symbol and r["planned_date"] == row["END_DATE"][:10]]
                require(len(matches) == 1, "unreviewed future event; no source promotion")
                review = matches[0]
                reference, _ = save(review["pdf_url"], {}, "pdf_" + review["announcement_id"])
                future_documents.append({**reference, "announcement_id": review["announcement_id"]})
    files["sina"], _ = save(SINA_URL.format(symbol=symbol), {}, "sina")
    quote_metadata, quote_files = collect_quote_inputs(directory=directory, source=source, symbol=symbol, end=end)
    files.update(quote_files)
    manifest = dict(schema=KIND, symbol=symbol, previous_day=previous_day, query_end=end, files=files,
                    future_plan_documents=future_documents, **quote_metadata)
    if not quote_metadata:
        quote = json.loads((source / files["quote"]["raw_path"]).read_bytes())
        quote_data = quote.get("data")
        if quote.get("rc") == 0 and isinstance(quote_data, dict) and quote_data.get("f57") == symbol:
            stamp = datetime.fromtimestamp(integer(quote_data.get("f86")), timezone.utc)
            if stamp.astimezone(timezone(timedelta(hours=8))).date() > day(end):
                files["historical_close"], _ = save(HISTORICAL_URL, historical_params(symbol, previous_day, end), "historical_close")
    def bound(relative, expected):
        p = (source / relative).resolve()
        require(p.is_relative_to(source.resolve()), "source path escapes")
        raw = p.read_bytes()
        require(sha(raw) == expected, "source digest differs")
        return raw
    if quote_metadata:
        observed_quote = verify_typed_quote(manifest, symbol=symbol, end=end, bound=bound, now=datetime.now(timezone.utc))
        if any(day(value) > day(end) for value in observed_quote["source_market_dates"].values()):
            files["historical_close"], _ = save(HISTORICAL_URL, historical_params(symbol, previous_day, end), "historical_close")
    if (symbol, previous_day, end) == ("603028", "2026-11-04", "2026-11-05"):
        from scripts.sse_share_daily_unchanged_20261104 import collect_transition
        manifest = collect_transition(manifest, symbol=symbol, previous_day=previous_day,
                                      end=end, candidate=candidate, bound=bound, root=root)
    try:
        if manifest.get("schema") == "sse_dated_transition_20261105_v1":
            from scripts.sse_share_daily_unchanged_20261104 import approved_transition, verify_transition
            new = approved_transition(root, symbol=symbol, previous_day=previous_day, end=end, previous_candidate=candidate)[0]
            verify_transition(manifest, symbol=symbol, previous_day=previous_day, end=end,
                              candidate=new, previous_candidate=candidate, bound=bound, root=root)
        else:
            try:
                verify_manifest(manifest, symbol=symbol, previous_day=previous_day, end=end, candidate=candidate, bound=bound, root=root)
            except ValueError as mismatch:
                if (str(mismatch) != "Unchanged share tail: quote total differs"
                        or os.environ.get(QUOTE_CORROBORATION_ENV) != "1"):
                    raise
                old = pd.read_csv(io.BytesIO(candidate), dtype=str, keep_default_na=False)
                collect_quote_dated_corroboration(manifest, symbol=symbol, end=end,
                    boundary=amount(old.iloc[-1].total_shares_10k), bound=bound,
                    now=datetime.now(timezone.utc), root=root, save=save)
                verify_manifest(manifest, symbol=symbol, previous_day=previous_day, end=end, candidate=candidate, bound=bound, root=root)
    except ValueError as error:
        if (str(error)!="Unchanged share tail: EM new or ambiguous tail event"
                or (symbol,previous_day,end)!=("603028","2026-11-03","2026-11-04")):
            raise
        from scripts.sse_share_daily_unchanged_20261104 import collect_override
        manifest=collect_override(root=root,source=source,directory=directory,manifest=manifest,symbol=symbol,
                                  previous_day=previous_day,end=end,candidate=candidate)
    path = directory / "manifest.json"
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf8")
    return dict(path=path.relative_to(source).as_posix(), sha256=sha(path.read_bytes()))
