"""Research-only Tencent vs Sina raw close challenge for delisted A shares.

Writes solely below data_recovery/price_challenge. Rate-limit responses stop new
requests. A failed symbol is reported, never silently accepted as matching.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests


HERE = Path(__file__).resolve().parent
OUT = HERE / "price_challenge"
TX_OUT = OUT / "tencent_raw"
API = "https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get"
START = pd.Timestamp("2010-01-04")
END = pd.Timestamp("2026-09-24")
STOP = threading.Event()


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def priority_symbols() -> list[str]:
    initial = pd.read_csv(HERE / "initial_2010_01_04_candidate_caps.csv", dtype={"symbol": str})
    retained = pd.read_csv(HERE / "candidate_cap_comparisons.csv", dtype={"symbol": str})
    a = set(initial.loc[initial["possible_below_rank100"].eq(True), "symbol"])
    b = set(retained.loc[retained["rebalance_date"].eq("2010-01-14") & retained["possible_below_rank100"].eq(True), "symbol"])
    assert len(a) == 24 and len(b) == 27 and len(a | b) == 29
    return sorted(a | b)


def all_symbols() -> list[str]:
    manifest = pd.read_csv(HERE / "source_manifest_both.csv", dtype={"symbol": str})
    symbols = manifest.loc[manifest["prices_raw_status"].eq("fetched"), "symbol"].tolist()
    assert len(symbols) == 251 and len(set(symbols)) == 251
    return sorted(symbols)


def get_page(session: requests.Session, symbol: str, year: int) -> tuple[list, dict]:
    params = {
        "_var": f"kline_day{year}",
        "param": f"{symbol},day,{year}-01-01,{year + 1}-12-31,640,",
        "r": "0.8205512681390605",
    }
    last_error = None
    for attempt in range(2):
        if STOP.is_set():
            raise RuntimeError("stopped_after_rate_limit")
        try:
            fetched_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
            response = session.get(API, params=params, timeout=18)
            if response.status_code in (403, 429):
                STOP.set()
                raise RuntimeError(f"Tencent rate limited HTTP {response.status_code}")
            response.raise_for_status()
            body = response.content
            content = response.text
            marker = content.find("={")
            if marker < 0:
                raise ValueError(f"unexpected Tencent JSONP prefix: {content[:80]}")
            payload = json.loads(content[marker + 1 :])
            if payload.get("code") != 0:
                raise ValueError(f"Tencent API code {payload.get('code')}: {str(payload.get('msg'))[:80]}")
            data = (payload.get("data") or {}).get(symbol)
            if not isinstance(data, dict):
                raise ValueError(f"Tencent symbol missing {symbol}")
            rows = data.get("day")
            if not isinstance(rows, list):
                raise ValueError("Tencent unadjusted day missing")
            return rows, {"symbol": symbol, "year_query": year, "retrieved_at_utc": fetched_at, "url": response.url, "http_status": response.status_code, "body_sha256": digest(body), "rows": len(rows)}
        except Exception as exc:
            last_error = exc
            if STOP.is_set():
                break
            if attempt == 0:
                time.sleep(0.5)
    raise RuntimeError(f"year {year} failed: {type(last_error).__name__}: {last_error}")


def check_symbol(symbol: str, delist_date: pd.Timestamp, rebalance_dates: set[str]) -> tuple[dict, list[dict], pd.DataFrame]:
    started = datetime.now(timezone.utc).isoformat(timespec="seconds")
    prefix = "sh" if symbol.startswith("6") else "sz"
    tx_symbol = prefix + symbol
    logs = []
    all_rows = []
    with requests.Session() as session:
        for year in range(2010, min(END.year, delist_date.year + 1) + 1):
            rows, log = get_page(session, tx_symbol, year)
            logs.append(log)
            all_rows.extend(rows)
    if not all_rows:
        raise ValueError("Tencent returned zero rows")
    tx = pd.DataFrame(all_rows)
    if tx.shape[1] < 3:
        raise ValueError(f"Tencent row width {tx.shape[1]}")
    tx = tx.iloc[:, [0, 2]].copy()
    tx.columns = ["date", "close_tencent"]
    tx["date"] = pd.to_datetime(tx["date"], errors="coerce")
    tx["close_tencent"] = pd.to_numeric(tx["close_tencent"], errors="coerce")
    tx = tx.dropna().drop_duplicates("date", keep="last").sort_values("date")
    tx = tx.loc[tx["date"].between(START, min(END, delist_date))]
    source = HERE / "prices_raw" / f"{symbol}.csv"
    sina = pd.read_csv(source, parse_dates=["date"])
    merged = sina.merge(tx, on="date", how="outer", indicator=True)
    merged["abs_error"] = (merged["close_raw"] - merged["close_tencent"]).abs()
    merged["symbol"] = symbol
    merged["is_rebalance_date"] = merged["date"].dt.strftime("%Y-%m-%d").isin(rebalance_dates)
    greater_1c = merged.loc[merged["abs_error"].gt(0.01000001)]
    nonzero = merged.loc[merged["abs_error"].gt(1e-6)]
    tx_path = TX_OUT / f"{symbol}.csv"
    tx.to_csv(tx_path, index=False, encoding="utf-8")
    assert len(pd.read_csv(tx_path)) == len(tx)
    result = {
        "symbol": symbol, "status": "checked", "retrieved_started_utc": started,
        "retrieved_finished_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "sina_source_url": f"https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?symbol={tx_symbol}&scale=240&ma=no&datalen=6000",
        "sina_file_sha256": digest(source.read_bytes()),
        "tencent_file_sha256": digest(tx_path.read_bytes()),
        "tencent_first_request_url": logs[0]["url"],
        "tencent_last_request_url": logs[-1]["url"],
        "tencent_request_count": len(logs),
        "sina_rows": len(sina), "tencent_rows": len(tx),
        "overlap_rows": int(merged["_merge"].eq("both").sum()),
        "sina_only_dates": int(merged["_merge"].eq("left_only").sum()),
        "tencent_only_dates": int(merged["_merge"].eq("right_only").sum()),
        "nonzero_price_difference_dates": len(nonzero),
        "over_0p01_price_difference_dates": len(greater_1c),
        "max_abs_price_difference": float(merged["abs_error"].max()) if len(nonzero) else 0.0,
        "first_price_difference_date": nonzero["date"].min().date().isoformat() if len(nonzero) else "",
        "first_over_0p01_difference_date": greater_1c["date"].min().date().isoformat() if len(greater_1c) else "",
        "price_difference_on_rebalance_dates": int((nonzero["is_rebalance_date"]).sum()),
    }
    return result, logs, merged.loc[merged["_merge"].ne("both") | merged["abs_error"].gt(1e-6)].copy()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", choices=["pilot", "priority", "remaining"], required=True)
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    TX_OUT.mkdir(parents=True, exist_ok=True)
    inventory = pd.read_csv(HERE / "missing_delisted_inventory.csv", dtype={"symbol": str}, parse_dates=["delist_date"])
    delist = dict(zip(inventory["symbol"], inventory["delist_date"]))
    dates = set(pd.read_csv(HERE / "candidate_cap_comparisons.csv", usecols=["rebalance_date"])["rebalance_date"].astype(str))
    priority = priority_symbols()
    if args.stage == "pilot":
        symbols = ["000023", "600385", "600898"]
    elif args.stage == "priority":
        symbols = priority
    else:
        symbols = [s for s in all_symbols() if s not in priority]
    results = []
    events = []
    retrievals = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(check_symbol, symbol, delist[symbol], dates): symbol for symbol in symbols}
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                result, logs, changed = future.result()
                results.append(result)
                retrievals.extend(logs)
                if not changed.empty:
                    events.append(changed)
            except Exception as exc:
                results.append({"symbol": symbol, "status": "failed", "error": f"{type(exc).__name__}: {str(exc)[:280]}", "retrieved_finished_utc": datetime.now(timezone.utc).isoformat(timespec="seconds")})
            print(json.dumps(results[-1], ensure_ascii=False), flush=True)
    pd.DataFrame(results).sort_values("symbol").to_csv(OUT / f"summary_{args.stage}.csv", index=False, encoding="utf-8")
    pd.DataFrame(retrievals).to_csv(OUT / f"request_manifest_{args.stage}.csv", index=False, encoding="utf-8")
    if events:
        pd.concat(events, ignore_index=True).sort_values(["symbol", "date"]).to_csv(OUT / f"date_differences_{args.stage}.csv", index=False, encoding="utf-8")
    print(json.dumps({"stage": args.stage, "symbols": len(symbols), "checked": sum(r["status"] == "checked" for r in results), "failed": sum(r["status"] != "checked" for r in results), "rate_limited": STOP.is_set()}, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
