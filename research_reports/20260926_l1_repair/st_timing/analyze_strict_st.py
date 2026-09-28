"""Cross-check recovered CNInfo titles against all selected member dates."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

import pandas as pd

from strict_st_classifier import build_intervals, classify_title, status_on_target


OUT = Path(__file__).resolve().parent
ROOT = OUT.parents[2]


def main() -> None:
    coverage = pd.read_csv(OUT / "st_57_coverage.csv", dtype={"symbol": str}).fillna("")
    members = pd.read_csv(
        ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv",
        dtype={"symbol": str, "rebalance_date": str},
    )
    members["symbol"] = members["symbol"].str.zfill(6)
    classified = []
    target_rows = []
    symbol_rows = []
    for row in coverage.to_dict("records"):
        symbol = row["symbol"]
        raw_path = OUT / "cninfo_raw" / f"{symbol}.json"
        if not raw_path.exists():
            continue
        raw = json.loads(raw_path.read_text(encoding="utf-8"))
        meta = json.loads((ROOT / ".microcap_index_cache/security_meta" / f"{symbol}.json").read_text(encoding="utf-8"))
        intervals = build_intervals(meta["first_trade_date"], raw["announcements"])
        actions = Counter()
        for item in raw["announcements"]:
            action = classify_title(item["title"])
            actions[action] += 1
            classified.append({
                "symbol": symbol,
                "notice_date": (item["notice_time_shanghai"] or "")[:10],
                "notice_time_shanghai": item["notice_time_shanghai"],
                "title": item["title"],
                "strict_action": action,
                "source_url": "https://static.cninfo.com.cn/" + str(item.get("adjunctUrl") or ""),
            })
        subset = members.loc[members["symbol"] == symbol]
        local_status = Counter()
        for target in subset.itertuples(index=False):
            statuses = {status_on_target(str(target.rebalance_date), interval) for interval in intervals}
            statuses.discard(None)
            if "explicit_entry_interior" in statuses:
                status = "explicit_entry_interior"
            elif "boundary_needs_pdf" in statuses:
                status = "boundary_needs_pdf"
            elif "initial_state_inferred" in statuses:
                status = "initial_state_inferred"
            else:
                status = "not_flagged_by_strict_titles"
            local_status[status] += 1
            target_rows.append({
                "symbol": symbol,
                "target_date": target.rebalance_date,
                "rank": target.rank,
                "strict_title_status": status,
                "cached_meta_st": any(
                    i["start"] <= str(target.rebalance_date) and (i.get("end") is None or str(target.rebalance_date) < i["end"])
                    for i in meta.get("st_intervals") or []
                ),
            })
        symbol_rows.append({
            "symbol": symbol,
            "target_rows": len(subset),
            "name_history_status": meta.get("name_history_status"),
            "all_six_queries_complete": len(raw["specs"]) == 6 and all(
                not s.get("error") and s.get("first_page_sha256") and
                int(s.get("pages_fetched") or 0) == (int(s.get("totalAnnouncement") or 0) + 29) // 30
                for s in raw["specs"]
            ),
            "query_start_date": raw.get("query_start_date"),
            "query_end_date": raw.get("query_end_date"),
            "query_column": raw.get("column"),
            "notice_titles": len(raw["announcements"]),
            "entry_titles": actions["entry"],
            "exit_titles": actions["exit"],
            "remain_st_titles": actions["remain_st"],
            "needs_review_titles": actions["needs_review"],
            "strict_intervals": json.dumps(intervals, ensure_ascii=False, separators=(",", ":")),
            **local_status,
        })
    for key in ("explicit_entry_interior", "boundary_needs_pdf", "initial_state_inferred", "not_flagged_by_strict_titles"):
        for row in symbol_rows:
            row.setdefault(key, 0)
    pd.DataFrame(classified).drop_duplicates(subset=["symbol", "notice_time_shanghai", "title"]).to_csv(
        OUT / "strict_notice_classification.csv", index=False, encoding="utf-8-sig"
    )
    targets = pd.DataFrame(target_rows)
    targets.to_csv(OUT / "strict_target_status.csv", index=False, encoding="utf-8-sig")
    symbols = pd.DataFrame(symbol_rows).sort_values("symbol")
    symbols.to_csv(OUT / "strict_symbol_coverage.csv", index=False, encoding="utf-8-sig")
    summary = {
        "symbols_analyzed": len(symbols),
        "all_six_queries_complete_symbols": int(symbols["all_six_queries_complete"].sum()),
        "target_rows": len(targets),
        "notice_titles": int(symbols["notice_titles"].sum()),
        "entry_titles": int(symbols["entry_titles"].sum()),
        "exit_titles": int(symbols["exit_titles"].sum()),
        "remain_st_titles": int(symbols["remain_st_titles"].sum()),
        "needs_review_titles": int(symbols["needs_review_titles"].sum()),
        "target_status_counts": targets["strict_title_status"].value_counts().to_dict(),
        "first_explicit_entry_interior_target": (
            targets.loc[targets["strict_title_status"] == "explicit_entry_interior"]
            .sort_values("target_date").head(1)[["symbol", "target_date", "rank"]].to_dict("records")
        ),
    }
    (OUT / "strict_st_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
