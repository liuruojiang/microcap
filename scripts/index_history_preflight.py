"""Validated ZZ1000 history providers for local/cloud delivery and preflight."""
from __future__ import annotations

from datetime import date
import json
import time

import numpy as np
import pandas as pd
import requests

from scripts import exchange_calendar

REQUEST_TIMEOUT = 8


def _get(url, **kwargs):
    response = requests.get(url, timeout=REQUEST_TIMEOUT,
                            headers={"User-Agent": "Mozilla/5.0"}, **kwargs)
    response.raise_for_status()
    return response


def sina_static(start: date, end: date) -> pd.DataFrame:
    response = _get("https://finance.sina.com.cn/realstock/company/sh000852/hisdata/klc_kl.js")
    # Execute only the installed decoder, never JavaScript received over HTTP.
    from akshare.index.index_stock_zh import hk_js_decode, py_mini_racer
    encoded, _ = json.JSONDecoder().raw_decode(response.text.split("=", 1)[1].lstrip())
    if not isinstance(encoded, str) or not encoded:
        raise ValueError("Invalid Sina encoded history")
    with py_mini_racer.MiniRacer() as decoder:
        decoder.eval(hk_js_decode)
        rows = decoder.call("d", encoded)
    frame = pd.DataFrame(rows)
    # Decoder represents exchange date labels as midnight UTC, not quote times.
    frame["date"] = pd.to_datetime(frame["date"], utc=True).dt.tz_localize(None)
    return frame


def tencent(start: date, end: date) -> pd.DataFrame:
    payload = _get("https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get",
                   params={"param": f"sh000852,day,{start},{end},640,qfq"}).json()
    if payload.get("code") != 0:
        raise ValueError("Tencent history unsuccessful")
    # Index raw 'day' only; never substitute adjusted equity qfqday data.
    rows = payload["data"]["sh000852"]["day"]
    return pd.DataFrame([dict(date=row[0], open=row[1], close=row[2], high=row[3],
                              low=row[4], volume=row[5]) for row in rows])


def eastmoney(start: date, end: date) -> pd.DataFrame:
    payload = _get("https://push2his.eastmoney.com/api/qt/stock/kline/get", params={
        "secid": "1.000852", "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58", "klt": 101,
        "fqt": 0, "beg": start.strftime("%Y%m%d"), "end": end.strftime("%Y%m%d"),
        "lmt": 10000}).json()["data"]
    if str(payload["code"]) != "000852":
        raise ValueError("Wrong Eastmoney instrument")
    rows = [row.split(",") for row in payload["klines"]]
    return pd.DataFrame([dict(date=row[0], open=row[1], close=row[2], high=row[3],
                              low=row[4], volume=row[5]) for row in rows])


def sina_legacy(start: date, end: date) -> pd.DataFrame:
    payload = _get("https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData",
                   params={"symbol": "sh000852", "scale": 240, "ma": "no", "datalen": 6000}).json()
    return pd.DataFrame(payload).rename(columns={"day": "date"})


def validate_history(frame: pd.DataFrame, start: date, end: date,
                     sessions: tuple[date, ...]) -> pd.DataFrame:
    required = ["date", "open", "close", "high", "low", "volume"]
    if frame.empty or not set(required).issubset(frame.columns):
        raise ValueError("Missing index history schema")
    frame = frame[required].copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="raise")
    if frame["date"].isna().any() or not frame["date"].eq(frame["date"].dt.normalize()).all():
        raise ValueError("Invalid daily timestamps")
    # Providers can return a broader history; no padding/dedup/filling is allowed.
    frame = frame.loc[frame["date"].between(pd.Timestamp(start), pd.Timestamp(end))].copy()
    if frame.empty or frame["date"].duplicated().any():
        raise ValueError("Empty or duplicate history dates")
    expected = [day for day in sessions if start <= day <= end]
    actual = sorted(frame["date"].dt.date.tolist())
    if not expected or expected[-1] != end or actual != expected:
        raise ValueError(f"History differs from independent calendar through {end}")
    for column in required[1:]:
        frame[column] = pd.to_numeric(frame[column], errors="raise")
    if not np.isfinite(frame[required[1:]].to_numpy(dtype=float)).all():
        raise ValueError("Nonfinite history values")
    if (frame[["open", "close", "high", "low"]] <= 0).any().any() or (frame.volume < 0).any():
        raise ValueError("Nonpositive prices or negative volume")
    if ((frame.high < frame[["open", "close", "low"]].max(axis=1)) |
            (frame.low > frame[["open", "close", "high"]].min(axis=1))).any():
        raise ValueError("Invalid OHLC bounds")
    return frame.sort_values("date").reset_index(drop=True)


def _fetch_history(start: date, expected_date: date | None, providers) -> pd.DataFrame:
    end = exchange_calendar.latest_completed_session()
    if expected_date is not None and expected_date != end:
        raise RuntimeError(f"Expected calendar day {expected_date} differs from completed session {end}")
    sessions = exchange_calendar.sessions_for_day(end)
    attempts = []
    for name, provider in providers:
        began = time.monotonic()
        try:
            frame = validate_history(provider(start, end), start, end, sessions)
        except Exception as exc:
            attempts.append({"source": name, "ok": False, "error": f"{type(exc).__name__}: {exc}",
                             "elapsed_seconds": round(time.monotonic() - began, 3)})
            continue
        attempts.append({"source": name, "ok": True,
                         "elapsed_seconds": round(time.monotonic() - began, 3)})
        frame.attrs.update(independent_history_source=name, independent_history_attempts=attempts,
                           independent_history_start=str(frame.date.iloc[0].date()),
                           independent_history_end=str(frame.date.iloc[-1].date()),
                           independent_calendar_source=exchange_calendar.SOURCE)
        return frame
    raise RuntimeError(f"All independent preflight history sources failed: {attempts}")


def fetch_preflight_history(start: date, expected_date: date | None = None) -> pd.DataFrame:
    return _fetch_history(start, expected_date, (("sina_static", sina_static), ("tencent", tencent)))


def fetch_delivery_history(start: date, expected_date: date | None = None) -> pd.DataFrame:
    """Keep primary provider order; failures/invalid data try independently validated backups."""
    return _fetch_history(start, expected_date, (("eastmoney", eastmoney), ("sina_legacy", sina_legacy),
                                                ("sina_static", sina_static), ("tencent", tencent)))


def preserve_existing_closes(history: pd.DataFrame, reference: pd.DataFrame,
                             price_column: str) -> pd.DataFrame:
    """Backup precision must not rewrite existing prices; material disagreement blocks."""
    reference = reference[["date", price_column]].copy()
    reference["date"] = pd.to_datetime(reference.date, errors="raise")
    if reference.date.isna().any() or reference.date.duplicated().any():
        raise ValueError("Invalid canonical history dates")
    known = pd.to_numeric(reference.set_index("date")[price_column], errors="raise")
    result = history.copy()
    result["date"] = pd.to_datetime(result.date, errors="raise")
    overlap = result.date.isin(known.index)
    previous = result.loc[overlap, "date"].map(known)
    if not np.isfinite(previous.to_numpy(dtype=float)).all() or (previous <= 0).any():
        raise ValueError("Invalid canonical index prices")
    delta = (result.loc[overlap, "close"] - previous).abs()
    if (delta > 0.010000001).any():
        raise ValueError("Backup index prices materially disagree with canonical history")
    result.loc[overlap, "close"] = previous.to_numpy()
    result.attrs.update(canonical_overlap_rows=int(overlap.sum()),
                        canonical_max_precision_difference=float(delta.max()) if len(delta) else 0.)
    return result
