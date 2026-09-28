"""Certify only first-30 target rows inside body-proved ST intervals."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

import pandas as pd


OUT = Path(__file__).resolve().parent
ROOT = OUT.parents[2]
DEST = OUT / "expansion_top30"

# Official body dates, hand-read. Every interval must include the full exit
# document; *ST-to-ST downgrades remain inside the ST interval.
INTERVALS = [
    {"symbol": "600506", "start": "2009-03-13", "downgrade": "2010-11-11", "end": "2012-08-24",
     "entry_id": "50084440", "downgrade_id": "58645540", "exit_id": "61464917",
     "entry_phrase": "2009年3月13日", "downgrade_phrase": "2010年11月11日", "exit_phrase": "2012年8月24日"},
    {"symbol": "600241", "start": "2020-04-27", "downgrade": "2021-05-18", "end": "2023-05-31",
     "entry_id": "1207586688", "downgrade_id": "1209981072", "exit_id": "1216934551",
     "entry_phrase": "2020年4月27日", "downgrade_phrase": "2021年5月18日", "exit_phrase": "2023年5月31日"},
]


def main() -> None:
    frozen = pd.read_csv(DEST / "frozen_top30.csv", dtype={"symbol": str})
    notices = pd.read_csv(DEST / "frozen_notice_leads.csv", dtype={"symbol": str, "announcementId": str})
    members = pd.read_csv(ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv", dtype={"symbol": str})
    certified = []
    event_evidence = []
    for interval in INTERVALS:
        symbol = interval["symbol"]
        assert symbol in set(frozen["symbol"])
        assert interval["start"] < interval["downgrade"] < interval["end"]
        proofs = {}
        for stage in ("entry", "downgrade", "exit"):
            notice_id = interval[f"{stage}_id"]
            notice = notices.loc[(notices["symbol"] == symbol) & (notices["announcementId"] == notice_id)].iloc[0]
            pdf = DEST / "pdfs" / symbol / f"{notice_id}.pdf"
            body = (DEST / "pdfs" / symbol / f"{notice_id}.txt").read_text(encoding="utf-8")
            digest = hashlib.sha256(pdf.read_bytes()).hexdigest()
            normalized = re.sub(r"\s+", "", body)
            assert symbol in normalized and interval[f"{stage}_phrase"] in normalized
            if stage == "downgrade":
                assert "ST" in normalized and ("其他特别处理" in normalized or "其他风险警示" in normalized)
            if stage == "exit":
                assert "ST" in normalized and ("变更为" in normalized or "撤销" in normalized)
            proofs[stage] = (notice["source_url"], digest)
            event_evidence.append({"symbol": symbol, "stage": stage, "effective_date": interval[
                "start" if stage == "entry" else "downgrade" if stage == "downgrade" else "end"],
                "announcementId": notice_id, "source_url": notice["source_url"],
                "sha256": digest, "local_pdf": str(pdf), "verified_phrase": interval[f"{stage}_phrase"]})
        target = members.loc[(members["symbol"] == symbol) &
                             (members["rebalance_date"] >= interval["start"]) &
                             (members["rebalance_date"] < interval["end"])]
        for row in target.itertuples(index=False):
            certified.append({"symbol": symbol, "target_date": row.rebalance_date, "rank": row.rank,
                              "entry_effective_date": interval["start"],
                              "downgrade_effective_date": interval["downgrade"],
                              "full_exit_effective_date": interval["end"],
                              "downgrade_source_url": proofs["downgrade"][0],
                              "downgrade_pdf_sha256": proofs["downgrade"][1],
                              "full_exit_source_url": proofs["exit"][0],
                              "full_exit_pdf_sha256": proofs["exit"][1]})
    out = pd.DataFrame(certified).sort_values(["target_date", "symbol"])
    assert len(out) == 37 and not out.duplicated(["target_date", "symbol"]).any()
    out.to_csv(DEST / "new_proven_bad_targets.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(event_evidence).to_csv(DEST / "body_proved_event_evidence.csv", index=False, encoding="utf-8-sig")
    summary = {"new_proven_bad_target_rows": len(out), "new_proven_symbols": out["symbol"].nunique(),
               "by_symbol": out.groupby("symbol").size().to_dict(),
               "earliest": out.iloc[0][["symbol", "target_date", "rank"]].to_dict(),
               "event_pdfs_sha_verified": len(event_evidence)}
    (DEST / "certified_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
