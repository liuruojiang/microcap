"""Source-bound unadjusted Tencent bars and explicit Eastmoney suspension evidence.

Positive-volume bars prove trading. A missing/zero-volume bar never proves a
suspension: an issuer-specific full-session suspension record is required.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import math
import time

import requests

PROVIDER = "tencent_day_eastmoney_suspension_v1"
PRICE_URL = "https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get"
SUSPENSION_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
FIELDS = ["date", "code", "close", "volume", "tradestatus", "adjustflag"]


def _suspension_params(day):
    return {"sortColumns": "SUSPEND_START_DATE", "sortTypes": "-1",
            "pageSize": "500", "pageNumber": "1",
            "reportName": "RPT_CUSTOM_SUSPEND_DATA_INTERFACE", "columns": "ALL",
            "source": "WEB", "client": "WEB",
            "filter": f'(MARKET="全部")(DATETIME=\'{day}\')'}


def _body(receipt, url, params):
    if (receipt["url"] != url or receipt["params"] != params or receipt["status"] != 200
            or hashlib.sha256(receipt["body"].encode()).hexdigest() != receipt["sha256"]):
        raise ValueError("Alternative provider source/response binding failed")
    stamp = datetime.fromisoformat(receipt["retrieved_at"])
    if stamp.tzinfo is None:
        raise ValueError("Alternative provider receipt requires a timezone")
    return json.loads(receipt["body"])


def normalize(native):
    symbol, start, end = native["symbol"], native["start"], native["end"]
    if len(symbol) != 6 or not symbol.isascii() or not symbol.isdigit():
        raise ValueError("Alternative provider invalid issuer")
    code = ("sh" if symbol.startswith("6") else "sz") + symbol
    params = {"param": f"{code},day,{start},{end},640,"}
    if datetime.fromisoformat(native["prices"]["retrieved_at"]) < datetime.fromisoformat(end + "T15:00:00+08:00"):
        raise ValueError("Alternative daily bars were collected before the requested close")
    response = _body(native["prices"], PRICE_URL, params)
    if response.get("code") != 0:
        raise ValueError("Tencent did not return a successful price response")
    bars = response["data"][code]["day"]  # Never substitute qfq/hfq bars.
    observed, last_day = {}, ""
    for bar in bars:
        day = str(bar[0])
        if len(bar) < 6 or datetime.strptime(day, "%Y-%m-%d").date().isoformat() != day or day <= last_day:
            raise ValueError("Tencent bars are malformed/duplicated/unsorted")
        last_day = day
        close, volume = float(bar[2]), float(bar[5])
        if not math.isfinite(close) or close <= 0 or not math.isfinite(volume) or volume < 0:
            raise ValueError("Tencent invalid raw close/volume")
        if start <= day <= end:
            observed[day] = (str(bar[2]), str(bar[5]))
    days = native["sessions"]
    if not days or days != sorted(set(days)) or any(not start <= d <= end for d in days):
        raise ValueError("Alternative provider invalid requested exchange sessions")
    prior = float(native["prior_raw_close"])
    if not math.isfinite(prior) or prior <= 0:
        raise ValueError("Alternative provider requires a real prior raw valuation")
    rows, used = [], set()
    for day in days:
        value = observed.get(day)
        if value and float(value[1]) > 0:
            close, volume, status = value[0], value[1], "1"
        else:
            receipt = native["suspensions"].get(day)
            if receipt is None:
                raise ValueError(f"No explicit full-session suspension evidence: {symbol} {day}")
            result = _body(receipt, SUSPENSION_URL, _suspension_params(day))
            data = result.get("result")
            if not result.get("success") or not data or data["pages"] != 1 or data["count"] != len(data["data"]):
                raise ValueError("Incomplete suspension response")
            matching = [r for r in data["data"] if r["SECURITY_CODE"] == symbol]
            if len(matching) != 1:
                raise ValueError("Suspension response does not uniquely identify issuer")
            record = matching[0]
            begin = record["SUSPEND_START_TIME"]
            finish = record.get("SUSPEND_END_TIME")
            resume = record.get("PREDICT_RESUME_DATE")
            # The daily query may include resumed names. Presence alone is insufficient.
            if (record["SECUCODE"] != symbol + (".SH" if symbol.startswith("6") else ".SZ")
                    or not begin or datetime.fromisoformat(begin) > datetime.fromisoformat(day + "T09:30:00")
                    or (finish and datetime.fromisoformat(finish) < datetime.fromisoformat(day + "T15:00:00"))
                    or (resume and datetime.fromisoformat(resume).date().isoformat() <= day)):
                raise ValueError("Suspension does not cover the full requested trading session")
            if value and abs(float(value[0]) - prior) > 1e-8:
                raise ValueError("Suspended Tencent valuation conflicts with prior raw close")
            close, volume, status = str(prior), "0", "0"
            used.add(day)
        rows.append([day, code[:2] + "." + symbol, close, volume, status, "3"])
        prior = float(close)
    if set(native["suspensions"]) != used:
        raise ValueError("Unused alternative suspension evidence")
    return rows


def query(symbol, start, end, sessions, prior_raw_close):
    deadline = time.monotonic() + 45
    def fetch(url, params):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Alternative source query exceeded its total deadline")
        r = requests.get(url, params=params, timeout=(min(5, remaining), min(10, remaining)))
        r.raise_for_status()
        body = r.content.decode("utf-8")
        return {"url": url, "params": params, "status": r.status_code,
                "retrieved_at": datetime.now(timezone.utc).isoformat(), "body": body,
                "sha256": hashlib.sha256(body.encode()).hexdigest()}
    code = ("sh" if symbol.startswith("6") else "sz") + symbol
    prices = fetch(PRICE_URL, {"param": f"{code},day,{start},{end},640,"})
    bars = json.loads(prices["body"])["data"][code]["day"]
    traded = {r[0] for r in bars if len(r) >= 6 and float(r[5]) > 0}
    native = dict(symbol=symbol, start=start, end=end, sessions=list(sessions),
                  prior_raw_close=float(prior_raw_close), prices=prices,
                  suspensions={day: fetch(SUSPENSION_URL, _suspension_params(day))
                               for day in sessions if day not in traded})
    rows = normalize(native)
    return {"provider": PROVIDER, "request": {"code": code[:2] + "." + symbol,
            "start": start, "end": end, "frequency": "d", "adjustflag": "3"},
            "error_code": "0", "error_msg": "", "fields": FIELDS, "rows": rows,
            "native_evidence": native}


def verify(payload):
    if payload.get("provider") != PROVIDER:
        raise ValueError("Unknown alternative daily provider")
    native = payload["native_evidence"]
    expected_code = ("sh." if native["symbol"].startswith("6") else "sz.") + native["symbol"]
    expected_request = dict(code=expected_code, start=native["start"], end=native["end"], frequency="d", adjustflag="3")
    if payload["request"] != expected_request or payload["fields"] != FIELDS or payload["rows"] != normalize(native):
        raise ValueError("Alternative provider rows do not reproduce native evidence")
