"""Replay recovered official ST-title evidence against every frozen target date."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pandas as pd


OUT = Path(__file__).resolve().parent
ROOT = OUT.parents[2]


def load_freq():
    path = ROOT / "microcap_top100_mom16_biweekly_live_v2_0.py"
    spec = importlib.util.spec_from_file_location("l1_st_replay_v2", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module.freq_mod


def in_intervals(date: str, intervals: list[dict]) -> bool:
    return any(x["start"] <= date and (x.get("end") is None or date < x["end"]) for x in intervals)


def main() -> None:
    freq = load_freq()
    coverage = pd.read_csv(OUT / "st_57_coverage.csv", dtype={"symbol": str}).fillna("")
    members = pd.read_csv(
        ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv",
        dtype={"symbol": str, "rebalance_date": str},
    )
    members["symbol"] = members["symbol"].str.zfill(6)
    rows = []
    differences = []
    notice_rows = []
    for row in coverage.to_dict("records"):
        symbol = row["symbol"]
        raw_path = OUT / "cninfo_raw" / f"{symbol}.json"
        if not raw_path.exists():
            continue
        result = json.loads(raw_path.read_text(encoding="utf-8"))
        all_specs_complete = len(result["specs"]) == 6 and all(
            spec.get("error") is None
            and int(spec.get("pages_fetched") or 0) == (int(spec.get("totalAnnouncement") or 0) + 29) // 30
            for spec in result["specs"]
        )
        meta = json.loads((ROOT / ".microcap_index_cache/security_meta" / f"{symbol}.json").read_text(encoding="utf-8"))
        notices = pd.DataFrame([{
            "notice_date": x["notice_time_shanghai"],
            "title": x["title"],
        } for x in result["announcements"]])
        if len(notices):
            notices["notice_date"] = pd.to_datetime(notices["notice_date"]).dt.tz_localize(None)
        else:
            notices = pd.DataFrame(columns=["notice_date", "title"])
        official_intervals = freq.build_st_intervals_from_notices(
            pd.Timestamp(meta["first_trade_date"]), pd.Timestamp(meta["last_trade_date"]), notices
        )
        combined = freq.merge_st_intervals([*(meta.get("st_intervals") or []), *official_intervals])
        selection = members.loc[members["symbol"] == symbol]
        for item in result["announcements"]:
            notice_rows.append({
                "symbol": symbol,
                "notice_time_shanghai": item["notice_time_shanghai"],
                "title": item["title"],
                "source_url": "https://static.cninfo.com.cn/" + str(item.get("adjunctUrl") or ""),
                "announcementId": item.get("announcementId"),
            })
        symbol_diff = []
        for target in selection.itertuples(index=False):
            date = str(target.rebalance_date)
            old = in_intervals(date, meta.get("st_intervals") or [])
            new = in_intervals(date, combined)
            if old != new:
                item = {"symbol": symbol, "target_date": date, "rank": target.rank, "cached_st": old, "replayed_st": new}
                differences.append(item)
                symbol_diff.append(item)
        rows.append({
            "symbol": symbol,
            "target_rows": len(selection),
            "all_six_queries_complete": all_specs_complete,
            "successful_specs": sum(not x.get("error") for x in result["specs"]),
            "failed_specs": sum(bool(x.get("error")) for x in result["specs"]),
            "fetched_notice_titles": len(result["announcements"]),
            "replayed_notice_intervals": json.dumps(official_intervals, ensure_ascii=False, separators=(",", ":")),
            "cached_intervals": json.dumps(meta.get("st_intervals") or [], ensure_ascii=False, separators=(",", ":")),
            "combined_intervals": json.dumps(combined, ensure_ascii=False, separators=(",", ":")),
            "replayed_notice_st_target_rows": sum(in_intervals(str(d), official_intervals) for d in selection.rebalance_date),
            "newly_st_target_rows": len(symbol_diff),
            "first_newly_st_target_date": min((x["target_date"] for x in symbol_diff), default=""),
        })
    report = pd.DataFrame(rows).sort_values("symbol")
    report.to_csv(OUT / "st_notice_replay.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(differences, columns=["symbol", "target_date", "rank", "cached_st", "replayed_st"]).to_csv(
        OUT / "st_membership_differences.csv", index=False, encoding="utf-8-sig"
    )
    pd.DataFrame(notice_rows).drop_duplicates(subset=["symbol", "notice_time_shanghai", "title"]).to_csv(
        OUT / "cninfo_notice_evidence.csv", index=False, encoding="utf-8-sig"
    )
    summary = {
        "symbols_replayed": len(report),
        "complete_six_query_symbols": int(report["all_six_queries_complete"].sum()),
        "incomplete_six_query_symbols": int((~report["all_six_queries_complete"]).sum()),
        "fetched_notice_titles": int(report["fetched_notice_titles"].sum()),
        "target_rows_covered_by_complete_six_queries": int(report.loc[report["all_six_queries_complete"], "target_rows"].sum()),
        "target_rows_incomplete_queries": int(report.loc[~report["all_six_queries_complete"], "target_rows"].sum()),
        "replayed_notice_st_target_rows": int(report["replayed_notice_st_target_rows"].sum()),
        "newly_st_target_rows": len(differences),
        "earliest_newly_st_target": min((x["target_date"] for x in differences), default=None),
    }
    (OUT / "st_notice_replay_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
