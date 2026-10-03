"""Screen frozen first two old Top100 lists with BaoStock historical day flags.

Research-only triage. Third-party isST is never a formal historical ST verdict.
"""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import baostock as bs


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]
INPUTS = {
    "2010-01-04": ROOT / "research_reports/20260926_l1_repair/data_recovery/initial_formal_seed_caps.csv",
    "2010-01-14": ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv",
}
RANKS = ROOT / "research_reports/20260926_l1_repair/data_recovery/conditional_two_date_rank.csv"
FIELDS = "date,code,open,high,low,close,volume,tradestatus,isST"
TARGETS = tuple(INPUTS)
RESPONSE_DIR = HERE / "baostock_responses"


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def get_baostock(symbol: str) -> dict:
    path = RESPONSE_DIR / f"{symbol}.json"
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    code = ("sh." if symbol.startswith("6") else "sz.") + symbol
    result = bs.query_history_k_data_plus(
        code, FIELDS, start_date=TARGETS[0], end_date=TARGETS[-1],
        frequency="d", adjustflag="3",
    )
    rows = []
    while result.error_code == "0" and result.next():
        rows.append(dict(zip(result.fields, result.get_row_data(), strict=True)))
    response = {"symbol": symbol, "code": code,
                "start_date": TARGETS[0], "end_date": TARGETS[-1],
                "fields": FIELDS, "adjustflag": "3", "frequency": "d",
                "error_code": result.error_code, "error_msg": result.error_msg,
                "rows": rows}
    path.write_text(json.dumps(response, ensure_ascii=False), encoding="utf-8")
    return response


def meta_at_date(symbol: str, day: str) -> tuple[str, int, bool]:
    path = ROOT / ".microcap_index_cache" / "security_meta" / f"{symbol}.json"
    if not path.exists():
        return "missing", 0, False
    meta = json.loads(path.read_text(encoding="utf-8"))
    intervals = meta.get("st_intervals") or []
    matches = [x for x in intervals if x.get("start", "9999") <= day <= x.get("end", "9999")]
    return ("1" if matches else "0"), len(intervals), True


def main() -> None:
    HERE.mkdir(parents=True, exist_ok=True)
    RESPONSE_DIR.mkdir(exist_ok=True)
    old = {
        "2010-01-04": read_csv(INPUTS["2010-01-04"]),
        "2010-01-14": [x for x in read_csv(INPUTS["2010-01-14"])
                       if x["rebalance_date"] == "2010-01-14"],
    }
    assert all(len(x) == 100 for x in old.values())
    ranks = {(x["date"], x["symbol"]): x for x in read_csv(RANKS)}
    symbols = sorted({x["symbol"].zfill(6) for group in old.values() for x in group})
    assert 100 <= len(symbols) <= 200
    assert all((day, x["symbol"].zfill(6)) in ranks
               for day, group in old.items() for x in group)

    login = bs.login()
    assert login.error_code == "0", login.error_msg
    responses = {}
    try:
        for count, symbol in enumerate(symbols, 1):
            responses[symbol] = get_baostock(symbol)
            if count % 20 == 0 or count == len(symbols):
                print(f"queried {count}/{len(symbols)}", flush=True)
    finally:
        bs.logout()

    results = []
    for day, group in old.items():
        for item in group:
            symbol = item["symbol"].zfill(6)
            response = responses[symbol]
            matched = [x for x in response["rows"] if x["date"] == day]
            history_st, interval_count, meta_exists = meta_at_date(symbol, day)
            results.append({
                "date": day, "symbol": symbol, "old_rank": item["rank"],
                "conditional_rank": ranks[(day, symbol)]["conditional_rank"],
                "old_market_cap_rmb": item["market_cap"],
                "baostock_row_count": len(matched),
                "baostock_isST": matched[0]["isST"] if len(matched) == 1 else "",
                "baostock_tradestatus": matched[0]["tradestatus"] if len(matched) == 1 else "",
                "baostock_volume": matched[0]["volume"] if len(matched) == 1 else "",
                "baostock_close": matched[0]["close"] if len(matched) == 1 else "",
                "baostock_error_code": response["error_code"],
                "baostock_error_msg": response["error_msg"],
                "cached_meta_st_interval_match": history_st,
                "cached_meta_interval_count": interval_count,
                "cached_meta_exists": meta_exists,
                "official_600603_two_date_status": "ST" if symbol == "600603" else "",
            })
    out = HERE / "old100_two_date_screen.csv"
    with out.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(results[0]))
        writer.writeheader()
        writer.writerows(results)
    manifest = {
        "scope": "BaoStock triage only; no formal status certification",
        "run_utc": datetime.now(timezone.utc).isoformat(),
        "input_sha256": {day: sha(path) for day, path in INPUTS.items()},
        "conditional_rank_sha256": sha(RANKS),
        "symbols": len(symbols), "old_rows": len(results),
        "response_files": len(list(RESPONSE_DIR.glob("*.json"))),
        "response_error_symbols": [s for s, x in responses.items() if x["error_code"] != "0"],
        "response_missing_target_rows": [f"{x['date']}:{x['symbol']}" for x in results
                                         if x["baostock_row_count"] != 1],
        "raw_response_fingerprint": hashlib.sha256(
            "".join(f"{s}:{sha(RESPONSE_DIR / (s + '.json'))}\n" for s in symbols).encode()
        ).hexdigest(),
    }
    (HERE / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps({"rows": len(results),
                      "vendor_ST_rows": sum(x["baostock_isST"] == "1" for x in results),
                      "vendor_untraded_rows": sum(x["baostock_tradestatus"] == "0" for x in results),
                      "errors": len(manifest["response_error_symbols"]),
                      "missing": len(manifest["response_missing_target_rows"])}, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
