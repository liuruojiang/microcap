"""Source-bound daily all-market notice archive, without advancing strategy state."""
from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
from zoneinfo import ZoneInfo

import requests

from scripts.st_notice_evidence import StNoticeEvidenceError, is_equity_issuer

API = "https://np-anotice-stock.eastmoney.com/api/security/ann"
SCHEMA = "eastmoney_all_market_daily_v1"


def _dump(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _params(day, page):
    return dict(sr="-1", page_size="100", page_index=str(page), ann_type="A",
                client_source="web", f_node="0", s_node="0",
                begin_time=day, end_time=day)


def _provider_time(value):
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}:\d{2}(?:[.:]\d{1,6})?)?", value):
        raise StNoticeEvidenceError("Daily notice provider timestamp invalid")
    if re.search(r":\d{3,6}$", value):
        value = value.rsplit(":", 1)[0] + "." + value.rsplit(":", 1)[1]
    return datetime.fromisoformat(value).replace(tzinfo=ZoneInfo("Asia/Shanghai"))


def verify_day(folder, day, *, required_asof=None):
    """Reconstruct completeness from bound raw pages, never trust derived totals."""
    folder = Path(folder)
    day = date.fromisoformat(day).isoformat()
    manifest = json.loads((folder / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("schema") != SCHEMA or manifest.get("date") != day:
        raise StNoticeEvidenceError("Daily notice manifest identity differs")
    total = None
    rows = []
    files = {}
    observations = []
    for number, entry in enumerate(manifest["pages"], 1):
        expected = {"response": f"page_{number:04}.json", "receipt": f"page_{number:04}.receipt.json"}
        if any(entry.get(k) != v for k, v in expected.items()):
            raise StNoticeEvidenceError("Daily notice page sequence differs")
        raw = (folder / entry["response"]).read_bytes()
        receipt_raw = (folder / entry["receipt"]).read_bytes()
        receipt = json.loads(receipt_raw)
        if (receipt.get("status") != 200 or receipt.get("url") != API
                or receipt.get("params") != _params(day, number)
                or receipt.get("sha256") != _sha(raw)
                or entry.get("response_sha256") != _sha(raw)
                or entry.get("receipt_sha256") != _sha(receipt_raw)):
            raise StNoticeEvidenceError("Daily notice response binding differs")
        collected = datetime.fromisoformat(receipt["collected_at"])
        completed = datetime.fromisoformat(receipt.get("completed_at", receipt["collected_at"]))
        close = datetime.combine(date.fromisoformat(day), datetime.min.time(), ZoneInfo("Asia/Shanghai")) + timedelta(hours=15)
        if collected.tzinfo is None or completed.tzinfo is None or collected < close or completed < collected or completed > datetime.now(timezone.utc):
            raise StNoticeEvidenceError("Daily notice receipt is not a completed-session observation")
        if required_asof is not None and (required_asof.tzinfo is None or collected < required_asof):
            raise StNoticeEvidenceError("Daily notice snapshot is older than required observation; use a new collection directory")
        observations.append(completed)
        data = json.loads(raw)
        if data.get("success") != 1:
            raise StNoticeEvidenceError("Daily notice provider query failed")
        data = data["data"]
        count = int(data["total_hits"])
        if total is None:
            total = count
        if count < 0 or count != total or int(data["page_index"]) != number or int(data["page_size"]) != 100:
            raise StNoticeEvidenceError("Daily notice page count changed")
        page_rows = data["list"]
        if len(page_rows) != max(0, min(100, total - (number - 1) * 100)):
            raise StNoticeEvidenceError("Daily notice page is incomplete")
        for row in page_rows:
            if _provider_time(row.get("notice_date")).date().isoformat() != day or not re.fullmatch(r"AN\d+", row.get("art_code", "")) or not isinstance(row.get("title"), str):
                raise StNoticeEvidenceError("Daily notice source identity/date invalid")
            codes = row.get("codes")
            if not isinstance(codes, list) or not codes or any(not isinstance(c, dict) or not all(isinstance(c.get(k), str) and c[k] for k in ("stock_code", "market_code", "ann_type")) for c in codes):
                raise StNoticeEvidenceError("Daily notice issuer identity is unresolved")
            for key in ("display_time", "eiTime"):
                if row.get(key) and _provider_time(row[key]) > completed:
                    raise StNoticeEvidenceError("Daily notice publication is later than observation")
        rows.extend(page_rows)
        files[entry["response"]] = _sha(raw)
        files[entry["receipt"]] = _sha(receipt_raw)
    if total is None or len(manifest["pages"]) != max(1, (total + 99) // 100) or len({r["art_code"] for r in rows}) != total or len(rows) != total:
        raise StNoticeEvidenceError("Daily notice missing pages or duplicate IDs")
    by_symbol = {}
    for row in rows:
        for symbol in {c.get("stock_code") for c in row.get("codes", []) if isinstance(c, dict)}:
            if isinstance(symbol, str) and re.fullmatch(r"[036]\d{5}", symbol) and is_equity_issuer(row, symbol):
                by_symbol.setdefault(symbol, []).append(row)
    return dict(date=day, total_hits=total, pages=len(manifest["pages"]),
                by_symbol=by_symbol, source_files=files,
                manifest_sha256=_sha((folder / "manifest.json").read_bytes()),
                observation_start=min(observations).isoformat(), observation_end=max(observations).isoformat(),
                scope="provider_all_market_pages_not_PIT_event_adjudication")


def collect_days(root, start, end, *, session=None, required_asof=None):
    """Archive each calendar day once; interrupted/failed days never become complete."""
    start, end = date.fromisoformat(start), date.fromisoformat(end)
    if start > end:
        raise ValueError("Notice interval is reversed")
    required_asof = required_asof or datetime.now(timezone.utc)
    if required_asof.tzinfo is None or required_asof > datetime.now(timezone.utc):
        raise ValueError("Notice required observation must be an actual aware past timestamp")
    if datetime.now(ZoneInfo("Asia/Shanghai")) < datetime.combine(end, datetime.min.time(), ZoneInfo("Asia/Shanghai")) + timedelta(hours=15):
        raise ValueError("Notice interval includes an uncompleted session")
    own_session = session is None
    session = session or requests.Session()
    results = []
    try:
        current = start
        while current <= end:
            day = current.isoformat()
            folder = Path(root) / day
            folder.mkdir(parents=True, exist_ok=True)
            if (folder / "manifest.json").exists():
                verified = verify_day(folder, day, required_asof=required_asof)
            else:
                pages, total, number = [], None, 1
                while total is None or number <= max(1, (total + 99) // 100):
                    raw_path = folder / f"page_{number:04}.json"
                    receipt_path = folder / f"page_{number:04}.receipt.json"
                    if raw_path.exists() or receipt_path.exists():
                        raise StNoticeEvidenceError("Interrupted notice day requires review; existing evidence preserved")
                    params = _params(day, number)
                    receipt = dict(url=API, params=params, collected_at=datetime.now(timezone.utc).isoformat())
                    try:
                        response = session.get(API, params=params, timeout=20)
                        raw = response.content
                        raw_path.write_bytes(raw)
                        receipt.update(status=response.status_code, sha256=_sha(raw), completed_at=datetime.now(timezone.utc).isoformat())
                        _dump(receipt_path, receipt)
                        response.raise_for_status()
                        payload = json.loads(raw)
                        if payload.get("success") != 1:
                            raise StNoticeEvidenceError("Daily notice query unsuccessful")
                        count = int(payload["data"]["total_hits"])
                        if count < 0 or (total is not None and total != count):
                            raise StNoticeEvidenceError("Daily notice pagination total changed")
                        total = count
                    except Exception as exc:
                        receipt.update(error=type(exc).__name__, detail=str(exc))
                        _dump(receipt_path, receipt)
                        raise
                    pages.append(dict(response=raw_path.name, receipt=receipt_path.name,
                                      response_sha256=_sha(raw), receipt_sha256=_sha(receipt_path.read_bytes())))
                    number += 1
                # Verify a staging manifest first; publish completion only after readback.
                candidate = dict(schema=SCHEMA, date=day, pages=pages)
                _dump(folder / "manifest.json", candidate)
                try:
                    verified = verify_day(folder, day, required_asof=required_asof)
                except Exception:
                    (folder / "manifest.json").replace(folder / "invalid_manifest.json")
                    raise
            results.append({k: v for k, v in verified.items() if k != "by_symbol"})
            current += timedelta(days=1)
    finally:
        if own_session:
            session.close()
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    args = parser.parse_args()
    print(json.dumps(collect_days(args.root, args.start, args.end), ensure_ascii=False, indent=2))
