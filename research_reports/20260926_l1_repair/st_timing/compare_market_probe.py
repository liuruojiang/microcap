"""Compare bulk CNInfo yearly retrieval to the 57 per-symbol retrievals."""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

import pandas as pd


OUT = Path(__file__).resolve().parent


def main() -> None:
    known = defaultdict(dict)
    for path in (OUT / "cninfo_raw").glob("*.json"):
        item = json.loads(path.read_text(encoding="utf-8"))
        for notice in item["announcements"]:
            year = int(str(notice["notice_time_shanghai"])[:4])
            key = (str(item["symbol"]), str(notice.get("announcementId")))
            known[year][key] = notice
    annual = defaultdict(dict)
    for path in (OUT / "market_archive").glob("*/**/manifest.json"):
        item = json.loads(path.read_text(encoding="utf-8"))
        if not item.get("complete"):
            continue
        year = int(item["year"])
        for notice in item["candidate_announcements"]:
            key = (str(notice["symbol"]), str(notice.get("announcementId")))
            annual[year][key] = notice
    rows = []
    missing = []
    for year in sorted(annual):
        expected = known[year]
        actual = annual[year]
        absent = sorted(set(expected) - set(actual))
        rows.append({"year": year, "known_57_notice_ids": len(expected),
                     "bulk_candidate_notice_ids": len(actual), "missing_known_57_notice_ids": len(absent)})
        for symbol, notice_id in absent:
            notice = expected[(symbol, notice_id)]
            missing.append({"year": year, "symbol": symbol, "announcementId": notice_id,
                            "title": notice["title"], "notice_time_shanghai": notice["notice_time_shanghai"]})
    pd.DataFrame(rows).to_csv(OUT / "market_probe_crosscheck.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(missing, columns=["year", "symbol", "announcementId", "title", "notice_time_shanghai"]).to_csv(
        OUT / "market_probe_missing_known.csv", index=False, encoding="utf-8-sig"
    )
    print(pd.DataFrame(rows).to_string(index=False))


if __name__ == "__main__":
    main()
