"""Strict ratio-adjusted ex-reference-price returns; no fetching or raw fallback.

This is not a cash/share entitlement wealth ledger. Inputs and cache identities
are explicit so that a new adjustment basis cannot silently reuse legacy caches.
"""
from __future__ import annotations

import hashlib
import io
import json
import re
from typing import Iterable

import numpy as np
import pandas as pd

RETURN_BASIS_REVISION = "sina_ratio_ex_reference_v1"
CSV_LINE_TERMINATOR = "\r\n"  # Preserve approved candidate bytes on every OS.


def _hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _dates(values: Iterable[str]) -> pd.DatetimeIndex:
    result = pd.DatetimeIndex(pd.to_datetime(list(values), errors="raise"))
    if result.hasnans or result.tz is not None or not (result == result.normalize()).all():
        raise ValueError("Expected timezone-free session dates")
    return result


def _factor(symbol: str, kind: str, source: bytes, receipt: dict) -> pd.DataFrame:
    market = ("sh" if symbol.startswith("6") else "sz") + symbol
    url = f"https://finance.sina.com.cn/realstock/company/{market}/{kind}.js"
    if receipt.get("status") != 200 or receipt.get("url") != url or receipt.get("sha256") != _hash(source):
        raise ValueError("Factor source receipt/identity/hash mismatch")
    text = source.decode("utf-8")
    prefix = f"var {market}{kind}="
    if not text.startswith(prefix):
        raise ValueError("Factor body identity mismatch")
    obj, _ = json.JSONDecoder().raw_decode(text[len(prefix):].lstrip())
    records = obj.get("data")
    if not isinstance(records, list) or not records or int(obj["total"]) != len(records):
        raise ValueError("Empty or incomplete factor history")
    frame = pd.DataFrame(records)[["d", "f"]].rename(columns={"d": "date", "f": kind})
    frame["date"] = _dates(frame["date"])
    frame[kind] = pd.to_numeric(frame[kind], errors="raise")
    if frame.date.duplicated().any() or not np.isfinite(frame[kind]).all() or not frame[kind].gt(0).all():
        raise ValueError("Invalid factor dates or values")
    return frame.sort_values("date")


def build_ratio_return_basis(
    *, symbol: str, raw_csv: bytes, qfq_source: bytes, hfq_source: bytes,
    qfq_receipt: dict, hfq_receipt: dict, as_of: str, sessions: Iterable[str],
    list_date: str, delist_date: str | None = None, anchor_date: str | None = None,
    return_basis_revision: str = RETURN_BASIS_REVISION,
) -> tuple[pd.DataFrame, dict]:
    """Build one entire symbol on a fixed anchor; never splice price bases.

    Every legal raw row must have both factors. This cannot prove missing raw
    market observations or vendor corporate-action completeness; callers retain
    responsibility for their raw coverage/freshness and trading-state gates.
    """
    if return_basis_revision != RETURN_BASIS_REVISION:
        raise ValueError("Unsupported return_basis_revision")
    if not re.fullmatch(r"\d{6}", symbol):
        raise ValueError("Invalid symbol")
    end, listed = _dates([as_of, list_date])
    delisted = _dates([delist_date])[0] if delist_date else None
    calendar = _dates(sessions).sort_values().unique()
    if end not in calendar:
        raise ValueError("as_of must be a verified exchange session")
    raw = pd.read_csv(io.BytesIO(raw_csv))[["date", "close_raw"]]
    raw["date"] = _dates(raw.date)
    raw["close_raw"] = pd.to_numeric(raw.close_raw, errors="raise")
    if raw.date.duplicated().any() or not np.isfinite(raw.close_raw).all() or not raw.close_raw.gt(0).all():
        raise ValueError("Invalid raw prices or duplicate dates")
    raw = raw.sort_values("date")
    legal = raw.date.le(end) & raw.date.ge(listed) & raw.date.isin(calendar)
    if delisted is not None:
        legal &= raw.date.lt(delisted)
    excluded = raw.loc[~legal, "date"].dt.strftime("%Y-%m-%d").tolist()
    raw = raw.loc[legal].copy()
    if raw.empty:
        raise ValueError("No legal raw observations")
    anchor = _dates([anchor_date])[0] if anchor_date else raw.date.iloc[0]
    if anchor not in set(raw.date):
        raise ValueError("Fixed anchor is missing from raw history")
    q = _factor(symbol, "qfq", qfq_source, qfq_receipt)
    h = _factor(symbol, "hfq", hfq_source, hfq_receipt)
    future = int(q.date.gt(end).sum() + h.date.gt(end).sum())
    q = q.loc[q.date.le(end)]; h = h.loc[h.date.le(end)]
    frame = pd.merge_asof(raw, q, on="date", direction="backward")
    frame = pd.merge_asof(frame, h, on="date", direction="backward")
    if frame[["qfq", "hfq"]].isna().any().any():
        raise ValueError("Incomplete factor coverage")
    forward, backward = frame.close_raw / frame.qfq, frame.close_raw * frame.hfq
    if not (np.isfinite(forward).all() and np.isfinite(backward).all() and forward.gt(0).all() and backward.gt(0).all()):
        raise ValueError("Invalid intermediate adjusted prices")
    error = (forward.pct_change(fill_method=None) - backward.pct_change(fill_method=None)).abs().max()
    if pd.notna(error) and error > 1e-9:
        raise ValueError("qfq/hfq return basis disagreement")
    anchor_factor = float(frame.loc[frame.date.eq(anchor), "hfq"].iloc[0])
    out = pd.DataFrame({"date": frame.date, "close_adj": backward / anchor_factor})
    if not np.isfinite(out.close_adj).all() or not out.close_adj.gt(0).all():
        raise ValueError("Nonpositive or nonfinite adjusted candidate")
    out = out.reset_index(drop=True)
    manifest = dict(return_basis_revision=RETURN_BASIS_REVISION, symbol=symbol,
        raw_sha256=_hash(raw_csv), qfq_sha256=_hash(qfq_source), hfq_sha256=_hash(hfq_source),
        sessions_sha256=_hash("\n".join(calendar[calendar<=end].strftime("%Y-%m-%d")).encode()),
        as_of=str(end.date()), list_date=str(listed.date()),
        delist_date=str(delisted.date()) if delisted is not None else None,
        anchor_date=str(anchor.date()), rows=len(out), first=str(out.date.min().date()),
        last=str(out.date.max().date()), excluded_raw_dates=excluded,
        future_factor_rows_excluded=future, qfq_hfq_return_max_abs_difference=0. if pd.isna(error) else float(error),
        candidate_sha256=_hash(out.to_csv(index=False, lineterminator=CSV_LINE_TERMINATOR).encode("utf-8")),
        cash_dividend_wealth_accounting=False)
    return out, manifest


def validate_cached_candidate(candidate_csv: bytes, manifest: dict, *, expected_manifest: dict) -> None:
    """Fail closed on any source, calendar, anchor, revision or output mismatch.

    expected_manifest must come from the current validated inputs/build, not an
    untrusted cache. Exact dictionary equality also rejects omitted/new fields.
    """
    if manifest.get("return_basis_revision") != RETURN_BASIS_REVISION:
        raise ValueError("Legacy or missing return basis revision")
    if manifest != expected_manifest or _hash(candidate_csv) != manifest.get("candidate_sha256"):
        raise ValueError("Cached return basis provenance/content mismatch")
