"""Read-only L1 ST coverage and execution-clock audit on frozen local inputs."""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from datetime import datetime
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parent
META = ROOT / ".microcap_index_cache/security_meta"
MEMBERS = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv"
TURNOVER = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_turnover.csv"
NAME_CHANGES = ROOT / ".microcap_index_cache/sz_name_change_short.csv"
SOURCE = ROOT / "microcap_top100_mom16_biweekly_live_v2_0.py"
EFFECTIVE = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_effective_members.csv"
PROXY = ROOT / "outputs/wind_microcap_top_100_biweekly_thursday_16y_cached.csv"


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def interval_overlaps(date: str, intervals: list[dict]) -> bool:
    return any(row["start"] <= date and (row.get("end") is None or date < row["end"]) for row in intervals)


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    members = pd.read_csv(MEMBERS, dtype={"symbol": str, "rebalance_date": str})
    members["symbol"] = members["symbol"].str.zfill(6)
    turnover = pd.read_csv(TURNOVER, dtype=str)
    changes = pd.read_csv(NAME_CHANGES, dtype={"symbol": str, "change_date": str})
    changes["symbol"] = changes["symbol"].str.zfill(6)
    rows = []
    member_counts = Counter(members["symbol"])
    for symbol, count in sorted(member_counts.items()):
        path = META / f"{symbol}.json"
        meta = json.loads(path.read_text(encoding="utf-8"))
        if meta.get("notice_query_status") == "ok":
            continue
        subset = members.loc[members["symbol"] == symbol]
        dates = sorted(subset["rebalance_date"].tolist())
        intervals = meta.get("st_intervals") or []
        name = changes.loc[changes["symbol"] == symbol]
        rows.append({
            "symbol": symbol,
            "target_rows": count,
            "first_target_date": dates[0],
            "last_target_date": dates[-1],
            "first_trade_date": meta.get("first_trade_date"),
            "last_trade_date": meta.get("last_trade_date"),
            "notice_query_status": meta.get("notice_query_status"),
            "name_history_status": meta.get("name_history_status"),
            "name_change_rows": len(name),
            "name_change_st_rows": int(name[["old_name", "new_name"]].fillna("").astype(str).apply(lambda c: c.str.upper().str.replace(" ", "", regex=False).str.startswith(("ST", "*ST", "PT"))).any(axis=1).sum()),
            "cached_st_interval_count": len(intervals),
            "cached_st_interval_json": json.dumps(intervals, ensure_ascii=False, separators=(",", ":")),
            "target_rows_overlapping_cached_st": sum(interval_overlaps(d, intervals) for d in dates),
            "meta_sha256": sha(path),
        })
    frame = pd.DataFrame(rows)
    frame.to_csv(OUT / "st_57_coverage.csv", index=False, encoding="utf-8-sig")

    # The last frozen event is material to the trace. It is not assigned a
    # return start from the final event row when the next trading day is absent.
    events = turnover[["rebalance_date", "execution_timing", "constraint_trade_date", "execution_date", "effective_date", "return_start_date"]].copy()
    for col in ("rebalance_date", "constraint_trade_date", "execution_date", "effective_date", "return_start_date"):
        events[col] = pd.to_datetime(events[col], errors="coerce", format="mixed").dt.strftime("%Y-%m-%d").fillna("")
    events.to_csv(OUT / "official_event_clock.csv", index=False, encoding="utf-8-sig")
    bad_same_close = events.loc[(events["execution_timing"] == "close") & (events["execution_date"] == events["rebalance_date"])]
    effective = pd.read_csv(EFFECTIVE, dtype={"symbol": str, "as_of_date": str})
    effective["symbol"] = effective["symbol"].str.zfill(6)
    final_date = events.iloc[-1]["rebalance_date"]
    effective = effective.loc[effective["as_of_date"] == final_date].copy()
    proxy = pd.read_csv(PROXY, dtype={"date": str})
    next_rows = proxy.loc[proxy["date"] > final_date]
    next_date = next_rows.iloc[0]["date"] if len(next_rows) else None
    return_components = []
    if next_date:
        for symbol in effective["symbol"]:
            prices = pd.read_csv(ROOT / ".microcap_index_cache/prices_raw" / f"{symbol}.csv", dtype={"date": str})
            vals = prices.set_index("date")["close_raw"]
            if final_date not in vals.index or next_date not in vals.index:
                return_components.append({"symbol": symbol, "raw_return": None})
            else:
                return_components.append({"symbol": symbol, "raw_return": float(vals.loc[next_date]) / float(vals.loc[final_date]) - 1})
    pd.DataFrame(return_components).to_csv(OUT / "tail_first_return_components.csv", index=False, encoding="utf-8-sig")
    first_return = next_rows.iloc[0] if len(next_rows) else None
    raw_returns = [x["raw_return"] for x in return_components if x["raw_return"] is not None]
    bridge = {
        "final_event_date": final_date,
        "event_return_start_date_field": events.iloc[-1]["return_start_date"],
        "observed_next_proxy_trade_date": next_date,
        "final_effective_member_rows": len(effective),
        "final_effective_unique_symbols": effective["symbol"].nunique(),
        "raw_price_return_coverage": len(raw_returns),
        "mean_100_raw_returns": sum(raw_returns) / len(raw_returns) if len(raw_returns) == 100 else None,
        "proxy_daily_return": None if first_return is None else float(first_return["daily_return"]),
        "difference": None if first_return is None or len(raw_returns) != 100 else sum(raw_returns) / 100 - float(first_return["daily_return"]),
    }
    summary = {
        "created_at": datetime.now().astimezone().isoformat(),
        "source_sha256": sha(SOURCE),
        "members_sha256": sha(MEMBERS),
        "turnover_sha256": sha(TURNOVER),
        "name_change_sha256": sha(NAME_CHANGES),
        "member_rows": len(members),
        "unique_members": members["symbol"].nunique(),
        "notice_failed_symbols": len(frame),
        "notice_failed_member_rows": int(frame["target_rows"].sum()),
        "name_history_status_counts": frame["name_history_status"].value_counts().to_dict(),
        "target_rows_overlapping_cached_st": int(frame["target_rows_overlapping_cached_st"].sum()),
        "same_date_close_execution_events": len(bad_same_close),
        "first_same_date_close_execution_event": bad_same_close.iloc[0].to_dict(),
        "last_event": events.iloc[-1].to_dict(),
        "tail_first_return_bridge": bridge,
    }
    (OUT / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
