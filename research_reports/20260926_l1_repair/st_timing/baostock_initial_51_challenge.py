"""Independent BaoStock cross-check for frozen L1 historical ST evidence."""

from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import baostock as bs


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
OUT = HERE / "baostock_initial_51"
FIELDS = "date,code,open,high,low,close,volume,tradestatus,isST"
SAMPLE_SYMBOLS = (
    "000010", "600080", "600082", "600119", "600180", "600187",
    "600423", "600476", "600491", "600543", "600506", "600241",
)


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, object]], fields: list[str]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def query(symbol: str, start: str, end: str) -> dict[str, object]:
    code = ("sh." if symbol.startswith("6") else "sz.") + symbol
    result = bs.query_history_k_data_plus(
        code, FIELDS, start_date=start, end_date=end,
        frequency="d", adjustflag="3",
    )
    rows: list[list[str]] = []
    while result.error_code == "0" and result.next():
        rows.append(result.get_row_data())
    return {
        "symbol": symbol,
        "code": code,
        "start_date": start,
        "end_date": end,
        "fields": result.fields,
        "adjustflag": "3",
        "frequency": "d",
        "error_code": result.error_code,
        "error_msg": result.error_msg,
        "rows": rows,
    }


def local_close(symbol: str, date: str, *, sample: bool) -> str:
    path = (
        ROOT / ".microcap_index_cache" / "prices_raw" / f"{symbol}.csv"
        if sample else HERE.parent / "data_recovery" / "prices_raw" / f"{symbol}.csv"
    )
    if not path.exists():
        return ""
    for row in read_csv(path):
        if row["date"] == date:
            return row["close_raw"]
    return ""


def compare(
    symbol: str,
    date: str,
    expected: str,
    source: str,
    response: dict[str, object],
) -> dict[str, object]:
    fields = list(response["fields"])
    matched = [dict(zip(fields, row, strict=True)) for row in response["rows"] if row[0] == date]
    row = matched[0] if len(matched) == 1 else {}
    raw = local_close(symbol, date, sample=source == "st_sample")
    bs_close = row.get("close", "")
    close_match = "N/A"
    if bs_close and raw:
        close_match = str(Decimal(bs_close) == Decimal(raw))
    bs_st = row.get("isST", "")
    st_match = "N/A" if not bs_st else str(bs_st == expected)
    return {
        "source": source,
        "symbol": symbol,
        "date": date,
        "expected_isST": expected,
        "baostock_isST": bs_st,
        "isST_match": st_match,
        "baostock_tradestatus": row.get("tradestatus", ""),
        "baostock_close_raw": bs_close,
        "local_sina_close_raw": raw,
        "close_match": close_match,
        "baostock_open": row.get("open", ""),
        "baostock_high": row.get("high", ""),
        "baostock_low": row.get("low", ""),
        "baostock_volume_shares": row.get("volume", ""),
        "query_error_code": response["error_code"],
        "query_error_msg": response["error_msg"],
        "query_row_count": len(response["rows"]),
        "date_row_count": len(matched),
        "row_present": str(len(matched) == 1),
    }


def main() -> None:
    OUT.mkdir(exist_ok=True)
    mother = read_csv(HERE / "initial_2010_st" / "two_date_st_status_evidence.csv")
    assert len(mother) == 51 and len({(r["symbol"], r["target_date"]) for r in mother}) == 51
    certified = read_csv(HERE / "certified_st_target_lower_bound.csv")
    expansion = read_csv(HERE / "expansion_top30" / "new_proven_bad_targets.csv")
    samples: list[tuple[str, str]] = []
    for symbol in SAMPLE_SYMBOLS:
        pool = expansion if symbol in {"600506", "600241"} else certified
        dates = sorted(r["target_date"] for r in pool if r["symbol"] == symbol)
        assert dates, symbol
        samples.append((symbol, dates[0]))

    login = bs.login()
    assert login.error_code == "0", (login.error_code, login.error_msg)
    responses: list[dict[str, object]] = []
    checked: list[dict[str, object]] = []
    try:
        by_symbol: dict[str, dict[str, object]] = {}
        for symbol in sorted({r["symbol"] for r in mother}):
            by_symbol[symbol] = query(symbol, "2010-01-04", "2010-01-14")
            responses.append(by_symbol[symbol])
        for row in mother:
            checked.append(compare(
                row["symbol"], row["target_date"],
                "1" if row["status"] == "proven_ST" else "0",
                "initial_51", by_symbol[row["symbol"]],
            ))
        for symbol, date in samples:
            response = query(symbol, date, date)
            responses.append(response)
            checked.append(compare(symbol, date, "1", "st_sample", response))
    finally:
        bs.logout()

    response_path = OUT / "baostock_query_rows.json"
    response_path.write_text(
        json.dumps({"retrieved_utc": datetime.now(timezone.utc).isoformat(),
                    "baostock_version": getattr(bs, "__version__", "unknown"),
                    "login_error_code": login.error_code, "queries": responses},
                   ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    checked_path = OUT / "target_comparison.csv"
    write_csv(checked_path, checked, list(checked[0]))
    manifest = {
        "retrieved_utc": datetime.now(timezone.utc).isoformat(),
        "baostock_version": getattr(bs, "__version__", "unknown"),
        "source": "BaoStock query_history_k_data_plus",
        "fields": FIELDS,
        "adjustflag": "3",
        "frequency": "d",
        "initial_target_count": len(mother),
        "initial_symbol_count": len({r["symbol"] for r in mother}),
        "st_sample_count": len(samples),
        "sample_rule": "earliest certified wrong target date for 12 frozen symbols",
        "sample_symbols": list(SAMPLE_SYMBOLS),
        "query_count": len(responses),
        "query_error_count": sum(r["error_code"] != "0" for r in responses),
        "comparison_counts": {
            source: {
                "rows": sum(r["source"] == source for r in checked),
                "missing_date_rows": sum(r["source"] == source and r["row_present"] != "True" for r in checked),
                "st_mismatch": sum(r["source"] == source and r["isST_match"] == "False" for r in checked),
                "close_mismatch": sum(r["source"] == source and r["close_match"] == "False" for r in checked),
                "no_local_close": sum(r["source"] == source and not r["local_sina_close_raw"] for r in checked),
                "not_trading": sum(r["source"] == source and r["baostock_tradestatus"] != "1" for r in checked),
            } for source in ("initial_51", "st_sample")
        },
        "files": {
            response_path.name: digest(response_path),
            checked_path.name: digest(checked_path),
        },
    }
    (OUT / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(manifest, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
