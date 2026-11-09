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
EM_URL = "https://datacenter.eastmoney.com/securities/api/data/v1/get"
QUOTE_URL = "https://push2.eastmoney.com/api/qt/stock/get"
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


def _em_date(value):
    require(isinstance(value, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}(?: 00:00:00)?", value), "missing/ambiguous EM date")
    return day(value[:10])


def _em_inventory(entries, bound, symbol, end, bounded, now, approved_event=None):
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
    identities, dates = set(), []
    for row in rows:
        require(row.get("SECUCODE") == em_symbol(symbol) and row.get("SECURITY_CODE") == symbol, "EM issuer differs")
        ds = tuple(_em_date(row.get(key)) for key in DATE_FIELDS)
        require((ds[2] <= day(end) or _matches_approved_event(row, approved_event))
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
    require(amount(data.get("f84")) / 10000 == boundary, "quote total differs")
    return dict(kind=KIND, symbol=symbol, query_end=end, previous_day=previous_day,
                total_shares_10k=str(boundary), unchanged_observations=True,
                frame_time=stamp.isoformat(), f84_independent_asof_guarantee_proven=False,
                provider_latency_guaranteed=False, old_candidate_sha256=sha(candidate),
                historical_close_evidence=historical,
                quote_is_target_close_frame=historical is None,
                future_plans_preserved_not_consumed=future_plans, future_pdf_transport_disclosures=future_transport,
                approved_late_unchanged_event={key: value for key, value in approved_event.items() if key != "row"}
                    if approved_event is not None else None)


def _save_public_observation(*, directory, source, url, params, purpose, end, referer=None):
    """Spend the former two-run transport budget on this request only.

    Four attempts equal the previous two local attempts times two whole-refresh
    attempts. Exhaustion is terminal to the outer runner; completed inputs and
    earlier successful requests are never repeated to consume this budget.
    """
    for attempt in range(4):
        start = datetime.now(timezone.utc)
        require(start >= datetime.fromisoformat(end + "T15:00:00+08:00"), "not close eligible")
        try:
            headers={"User-Agent":"Mozilla/5.0"}
            if referer is not None:
                require(referer=="https://www.sse.com.cn/assortment/stock/list/info/company/index.shtml?COMPANY_CODE=603028", "unknown source referer")
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
            if attempt == 3:
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
                    if attempt == 3:
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
    qp = quote_params(symbol)
    files["quote"], quote_raw = save(QUOTE_URL, qp, "quote")
    quote = json.loads(quote_raw)
    quote_data = quote.get("data")
    if quote.get("rc") == 0 and isinstance(quote_data, dict) and quote_data.get("f57") == symbol:
        stamp = datetime.fromtimestamp(integer(quote_data.get("f86")), timezone.utc)
        if stamp.astimezone(timezone(timedelta(hours=8))).date() > day(end):
            files["historical_close"], _ = save(HISTORICAL_URL, historical_params(symbol, previous_day, end), "historical_close")
    manifest = dict(schema=KIND, symbol=symbol, previous_day=previous_day, query_end=end, files=files,
                    future_plan_documents=future_documents)
    def bound(relative, expected):
        p = (source / relative).resolve()
        require(p.is_relative_to(source.resolve()), "source path escapes")
        raw = p.read_bytes()
        require(sha(raw) == expected, "source digest differs")
        return raw
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
