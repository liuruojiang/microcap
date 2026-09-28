"""Research-only delisted A-share source audit. Never writes formal caches."""

from __future__ import annotations

import argparse
import hashlib
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import requests


ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
START = pd.Timestamp("2010-01-04")
END = pd.Timestamp("2026-09-24")
EASTMONEY = "https://datacenter.eastmoney.com/securities/api/data/v1/get"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inventory() -> pd.DataFrame:
    HERE.mkdir(parents=True, exist_ok=True)
    master = pd.read_csv(ROOT / ".microcap_index_cache/security_master.csv", dtype={"symbol": str})
    master["list_date"] = pd.to_datetime(master["list_date"], errors="coerce")
    master["delist_date"] = pd.to_datetime(master["delist_date"], errors="coerce")
    candidates = master.loc[
        master["delist_date"].notna()
        & master["list_date"].le(END)
        & master["delist_date"].ge(START)
        & master["exchange"].isin(["SSE", "SZSE"])
        & ~master["symbol"].str.startswith(("2", "9"))
    ].copy()
    roots = [ROOT / ".microcap_index_cache"]
    roots += [x / ".microcap_index_cache" for x in ROOT.parent.iterdir() if x != ROOT and (x / ".microcap_index_cache").exists()]
    for kind in ("prices_raw", "share_change"):
        candidates[kind + "_exists"] = candidates["symbol"].map(lambda s: any((root / kind / f"{s}.csv").exists() for root in roots))
    missing = candidates.loc[~candidates["prices_raw_exists"] & ~candidates["share_change_exists"]].copy()
    missing = missing.sort_values("symbol").reset_index(drop=True)
    missing.to_csv(HERE / "missing_delisted_inventory.csv", index=False, encoding="utf-8")
    return missing


def fetch_sina(symbol: str) -> tuple[pd.DataFrame, str]:
    market = "sh" if symbol.startswith(("5", "6", "9")) else "sz"
    url = "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"
    params = {"symbol": market + symbol, "scale": 240, "ma": "no", "datalen": 6000}
    response = requests.get(url, params=params, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, list) or not data:
        raise ValueError("empty Sina history")
    frame = pd.DataFrame({"date": [v.get("day") for v in data], "close_raw": [v.get("close") for v in data]})
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["close_raw"] = pd.to_numeric(frame["close_raw"], errors="coerce")
    frame = frame.dropna().drop_duplicates("date").sort_values("date")
    frame = frame.loc[frame["date"].between(START, END)]
    if frame.empty:
        raise ValueError("no in-window Sina history")
    if not frame["close_raw"].gt(0).all():
        raise ValueError("non-positive Sina close")
    return frame, response.url


def fetch_eastmoney_shares(symbol: str, exchange: str) -> tuple[pd.DataFrame, str, dict]:
    suffix = "SH" if exchange == "SSE" else "SZ"
    secucode = symbol + "." + suffix
    params = {
        "reportName": "RPT_F10_EH_EQUITY",
        "columns": "ALL",
        "filter": f'(SECUCODE="{secucode}")',
        "pageNumber": "1",
        "pageSize": "500",
        "sortTypes": "-1",
        "sortColumns": "END_DATE",
        "source": "HSF10",
        "client": "PC",
    }
    first_url = ""
    all_rows = []
    pagination = {}
    for page in range(1, 21):
        params["pageNumber"] = str(page)
        response = requests.get(EASTMONEY, params=params, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()
        if not first_url:
            first_url = response.url
        body = response.json()
        result = body.get("result")
        if not isinstance(result, dict):
            raise ValueError(f"empty Eastmoney result: {str(body)[:160]}")
        rows = result.get("data") or []
        if page == 1:
            pagination = {k: v for k, v in result.items() if k != "data"}
        all_rows.extend(rows)
        expected_pages = int(result.get("pages") or 0)
        if page >= expected_pages or not rows:
            break
    else:
        raise ValueError("Eastmoney pagination exceeded 20 pages")
    if not all_rows:
        raise ValueError("empty Eastmoney shares")
    raw = pd.DataFrame(all_rows)
    required = {"END_DATE", "TOTAL_SHARES", "SECUCODE", "NOTICE_DATE", "LISTING_DATE"}
    if not required.issubset(raw.columns):
        raise ValueError(f"share columns missing: {sorted(required - set(raw.columns))}")
    if set(raw["SECUCODE"].dropna()) != {secucode}:
        raise ValueError("Eastmoney symbol mismatch")
    frame = pd.DataFrame(
        {
            "change_date": pd.to_datetime(raw["END_DATE"], errors="coerce"),
            "notice_date": pd.to_datetime(raw["NOTICE_DATE"], errors="coerce"),
            "listing_date": pd.to_datetime(raw["LISTING_DATE"], errors="coerce"),
            "total_shares_10k": pd.to_numeric(raw["TOTAL_SHARES"], errors="coerce") / 10000,
            "reason": raw.get("CHANGE_REASON", pd.Series(index=raw.index, dtype=str)),
        }
    ).dropna(subset=["change_date", "total_shares_10k"])
    frame = frame.sort_values("change_date").drop_duplicates("change_date", keep="last")
    if frame.empty or not frame["total_shares_10k"].gt(0).all():
        raise ValueError("empty or non-positive shares")
    if len(raw) != int(pagination.get("count") or len(raw)):
        raise ValueError(f"pagination incomplete: got {len(raw)} vs count={pagination.get('count')}")
    return frame, first_url, pagination


def fetch_one(symbol: str, missing: pd.DataFrame, kind_filter: str) -> dict:
    item = missing.loc[missing["symbol"].eq(symbol)].iloc[0]
    record: dict = {"symbol": symbol, "exchange": item["exchange"], "list_date": str(item["list_date"].date()), "delist_date": str(item["delist_date"].date())}
    for kind, fn in (
        ("prices_raw", lambda: fetch_sina(symbol)),
        ("share_change", lambda: fetch_eastmoney_shares(symbol, item["exchange"])),
    ):
        if kind_filter != "both" and kind_filter != kind:
            continue
        try:
            result = fn()
            frame, url = result[:2]
            path = HERE / kind / f"{symbol}.csv"
            path.parent.mkdir(parents=True, exist_ok=True)
            frame.to_csv(path, index=False, encoding="utf-8")
            readback = pd.read_csv(path)
            if len(readback) != len(frame):
                raise ValueError("readback row mismatch")
            date_col = "date" if kind == "prices_raw" else "change_date"
            record[kind + "_status"] = "fetched"
            record[kind + "_rows"] = len(frame)
            record[kind + "_first"] = str(frame[date_col].min().date())
            record[kind + "_last"] = str(frame[date_col].max().date())
            record[kind + "_sha256"] = sha256(path)
            record[kind + "_url"] = url
            if len(result) > 2:
                record[kind + "_pagination"] = json.dumps(result[2], ensure_ascii=False)
                record[kind + "_notice_after_end_rows"] = int(frame["notice_date"].gt(frame["change_date"]).sum())
                record[kind + "_notice_missing_rows"] = int(frame["notice_date"].isna().sum())
        except Exception as exc:
            record[kind + "_status"] = "failed"
            record[kind + "_error"] = f"{type(exc).__name__}: {str(exc)[:240]}"
    return record


def run(symbols: list[str], kind_filter: str, workers: int) -> None:
    missing = inventory()
    HERE.mkdir(parents=True, exist_ok=True)
    rows = []
    for symbol in symbols:
        if not missing["symbol"].eq(symbol).any():
            raise ValueError(f"{symbol} not in missing inventory")
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(fetch_one, symbol, missing, kind_filter): symbol for symbol in symbols}
        for future in as_completed(futures):
            record = future.result()
            rows.append(record)
            print(json.dumps(record, ensure_ascii=False), flush=True)
    manifest = pd.DataFrame(rows)
    manifest.sort_values("symbol").to_csv(HERE / f"source_manifest_{kind_filter}.csv", index=False, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbols", nargs="*", default=[])
    parser.add_argument("--compare", default="")
    parser.add_argument("--schema", default="")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--kind", choices=["both", "prices_raw", "share_change"], default="both")
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()
    missing = inventory()
    print(f"missing={len(missing)} sample={missing['symbol'].head(5).tolist()}", flush=True)
    if args.schema:
        suffix = "SH" if args.schema.startswith("6") else "SZ"
        params = {"reportName": "RPT_F10_EH_EQUITY", "columns": "ALL", "filter": f'(SECUCODE="{args.schema}.{suffix}")', "pageNumber": "1", "pageSize": "500", "sortTypes": "-1", "sortColumns": "END_DATE", "source": "HSF10", "client": "PC"}
        response = requests.get(EASTMONEY, params=params, timeout=30)
        response.raise_for_status()
        body = response.json()
        result = body.get("result") or {}
        data = result.get("data") or []
        print(json.dumps({"symbol": args.schema, "keys": list(data[0]) if data else [], "2015_05_25": [row for row in data if str(row.get("END_DATE", "")).startswith("2015-05-25")][:1], "count": result.get("count"), "url": response.url}, ensure_ascii=False), flush=True)
    if args.compare:
        frame, url, pagination = fetch_eastmoney_shares(args.compare, "SSE" if args.compare.startswith("6") else "SZSE")
        reference = pd.read_csv(ROOT / ".microcap_index_cache/share_change" / f"{args.compare}.csv")
        reference["change_date"] = pd.to_datetime(reference["change_date"])
        merged = frame.merge(reference, on="change_date", how="outer", suffixes=("_em", "_cninfo"), indicator=True)
        merged["difference_10k"] = merged["total_shares_10k_em"] - merged["total_shares_10k_cninfo"]
        output = HERE / f"compare_{args.compare}_eastmoney_cninfo.csv"
        merged.to_csv(output, index=False, encoding="utf-8")
        print(json.dumps({"symbol": args.compare, "em_rows": len(frame), "cninfo_rows": len(reference), "merged_rows": len(merged), "both": int(merged["_merge"].eq("both").sum()), "max_abs_difference_10k": float(merged["difference_10k"].abs().max()), "pagination": pagination, "url": url}, ensure_ascii=False), flush=True)
    if args.all:
        run(missing["symbol"].tolist(), args.kind, args.workers)
    elif args.symbols:
        run(args.symbols, args.kind, args.workers)


if __name__ == "__main__":
    main()
