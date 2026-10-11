"""Scan old candidate rebalance orders for IPO price-limit exemption windows."""

from __future__ import annotations

import csv
import hashlib
import json
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
TARGET = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv"
EFFECTIVE = ROOT / "research_reports/20260926_l1_repair/full_replay/members_by_rebalance.csv"
META = ROOT / ".microcap_index_cache/security_meta"
RAW = ROOT / ".microcap_index_cache/prices_raw"


def rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def eligible_five_day_board(symbol: str, listing_date: str) -> bool:
    if symbol.startswith(("688", "689")):
        return True
    if symbol.startswith(("300", "301")) and listing_date >= "2020-08-24":
        return True
    if symbol.startswith(("000", "001", "002", "003", "600", "601", "603", "605")) and listing_date >= "2023-04-10":
        return True
    return False


def main() -> None:
    targets: dict[str, list[str]] = defaultdict(list)
    effective: dict[str, set[str]] = defaultdict(set)
    for r in rows(TARGET):
        targets[r["rebalance_date"]].append(r["symbol"].zfill(6))
    for r in rows(EFFECTIVE):
        effective[r["rebalance_date"]].add(r["symbol"].zfill(6))
    dates = sorted(d for d in targets if d >= "2019-07-01")
    all_dates = sorted(targets)
    seen, candidates = {}, []
    for date in dates:
        prev = all_dates[all_dates.index(date) - 1]
        for symbol in targets[date]:
            if symbol in effective[prev]:
                continue
            if symbol not in seen:
                meta = json.loads((META / f"{symbol}.json").read_text(encoding="utf-8"))
                bars = rows(RAW / f"{symbol}.csv")
                seen[symbol] = (meta, [r["date"] for r in bars])
            meta, traded_dates = seen[symbol]
            listing = meta.get("list_date") or ""
            if not eligible_five_day_board(symbol, listing):
                continue
            if date not in traded_dates[:5]:
                continue
            candidates.append({"signal_date": date, "symbol": symbol, "list_date": listing,
                               "board": meta.get("board"), "first_five_actual_trade_dates": traded_dates[:5],
                               "candidate_buy_in_unlimited_IPO_window": True})
    source_hashes = {str(p.relative_to(ROOT)): sha(p) for p in (TARGET, EFFECTIVE)}
    for c in candidates:
        for directory, suffix in ((META, ".json"), (RAW, ".csv")):
            path = directory / (c["symbol"] + suffix)
            source_hashes[str(path.relative_to(ROOT))] = sha(path)
    out = {"status": "old_candidate_IPO_window_scanned", "window_start": "2019-07-01",
           "window_end": dates[-1], "distinct_candidate_symbols_checked": len(seen),
           "IPO_five_day_candidate_buys": candidates, "count": len(candidates),
           "source_sha256": source_hashes,
           "scope": "Old same-close candidate buys only; board/date exemptions based on exchange rules. Does not certify listing status for reconstructed new L1 pool or T+1 orders."}
    HERE.mkdir(parents=True, exist_ok=True)
    (HERE / "ipo_no_limit_old_orders.json").write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"symbols_checked": len(seen), "IPO_five_day_candidate_buys": len(candidates)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
