"""Verify page hashes and collapse yearly CNInfo archive to candidate evidence."""

from __future__ import annotations

import gzip
import hashlib
import json
from collections import defaultdict
from pathlib import Path

import pandas as pd


OUT = Path(__file__).resolve().parent
LABELS = {"category", "risk", "special", "remove_risk", "remove_special", "remove_hat"}


def main() -> None:
    manifests = []
    notices = defaultdict(dict)
    for path in sorted((OUT / "market_archive").glob("*/**/manifest.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        year, label = int(data["year"]), str(data["label"])
        assert label in LABELS
        assert data["complete"], path
        total = int(data["totalAnnouncement"])
        pages = (total + 29) // 30
        assert len(data["page_manifests"]) == pages, path
        observed_ids = set()
        for page in data["page_manifests"]:
            source = path.parent / f"page_{page['page']:04d}.json.gz"
            raw = gzip.decompress(source.read_bytes())
            assert hashlib.sha256(raw).hexdigest() == page["sha256_raw_response"], source
            payload = json.loads(raw)
            assert int(payload["totalAnnouncement"]) == total, source
            items = payload.get("announcements") or []
            assert len(items) == page["row_count"], source
            for item in items:
                key = str(item.get("announcementId"))
                assert key not in observed_ids, (source, key)
                observed_ids.add(key)
        assert len(observed_ids) == total, path
        for item in data["candidate_announcements"]:
            symbol = str(item["symbol"]).zfill(6)
            key = (symbol, str(item.get("announcementId")))
            if key not in notices[year]:
                notices[year][key] = {
                    "year": year, "symbol": symbol,
                    "announcementId": item.get("announcementId"),
                    "announcementTime": item.get("announcementTime"),
                    "title": item.get("title"),
                    "source_url": "https://static.cninfo.com.cn/" + str(item.get("adjunctUrl") or ""),
                    "matched_specs": set(),
                }
            notices[year][key]["matched_specs"].add(label)
        manifests.append({"year": year, "label": label, "total": total, "pages": pages,
                          "candidate_rows_within_query": len(data["candidate_announcements"]), "complete": True})
    report = pd.DataFrame(manifests).sort_values(["year", "label"])
    report.to_csv(OUT / "market_archive_verified_specs.csv", index=False, encoding="utf-8-sig")
    events = []
    for year in sorted(notices):
        for item in notices[year].values():
            item["matched_specs"] = "|".join(sorted(item["matched_specs"]))
            events.append(item)
    pd.DataFrame(events).sort_values(["year", "symbol", "announcementTime"]).to_csv(
        OUT / "market_archive_candidate_notices.csv", index=False, encoding="utf-8-sig"
    )
    complete_years = [year for year, part in report.groupby("year") if set(part["label"]) == LABELS]
    summary = {
        "years_with_all_six_specs": complete_years,
        "missing_years_2010_2026": sorted(set(range(2010, 2027)) - set(complete_years)),
        "complete_specs": len(report),
        "raw_response_pages_verified": int(report["pages"].sum()),
        "query_response_rows_verified": int(report["total"].sum()),
        "candidate_notice_ids_deduped": len(events),
        "candidate_symbols_with_any_matched_notice": len({item["symbol"] for item in events}),
    }
    (OUT / "market_archive_audit_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
