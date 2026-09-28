"""PDF-date-reviewed lower bound of erroneous historical ST target rows."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd


OUT = Path(__file__).resolve().parent
ROOT = OUT.parents[2]

# Hand-read from the corresponding official attachments in evidence/.
ENTRY_EFFECTIVE = {
    ("600080", "2010-04-26"): "2010-04-27",
    ("600080", "2020-06-01"): "2020-06-02",
    ("600080", "2026-04-29"): "2026-04-30",
    ("600082", "2026-04-11"): "2026-04-14",
    ("600119", "2019-04-30"): "2019-05-06",
    ("600119", "2026-04-27"): "2026-04-28",
    ("600180", "2008-06-26"): "2008-06-27",
    ("600180", "2026-04-29"): "2026-04-30",
    ("600187", "2026-04-30"): "2026-05-06",
    ("600423", "2017-04-29"): "2017-05-03",
    ("600423", "2026-04-25"): "2026-04-28",
    ("600476", "2026-04-28"): "2026-04-29",
    ("600491", "2026-04-30"): "2026-05-06",
    ("600543", "2023-04-28"): "2023-05-04",
    ("600543", "2026-04-24"): "2026-04-27",
    ("600678", "2009-06-29"): "2009-06-30",
    ("600678", "2026-04-29"): "2026-04-30",
    ("600818", "2026-04-28"): "2026-04-29",
    ("603272", "2026-04-28"): "2026-04-29",
    ("603359", "2026-04-29"): "2026-04-30",
    ("603378", "2026-04-30"): "2026-05-06",
    ("603729", "2021-04-30"): "2021-05-06",
    ("603729", "2026-04-29"): "2026-04-30",
    ("688121", "2026-07-06"): "2026-07-07",
    ("688189", "2026-06-10"): "2026-06-11",
}

EXIT_EFFECTIVE = {
    ("600080", "2012-06-11"): "2012-06-12",
    ("600080", "2021-05-11"): "2021-05-12",
    ("600119", "2021-05-14"): "2021-05-17",
    # 600180 removed delisting warning but remained ST from 2011-12-15;
    # using this earlier date as a conservative end for the old interval.
    ("600180", "2011-12-14"): "2011-12-15",
    ("600423", "2021-05-19"): "2021-05-20",
    ("600543", "2024-07-05"): "2024-07-08",
    ("600678", "2013-02-01"): "2013-02-04",
    ("603729", "2022-05-30"): "2022-05-31",
}


def main() -> None:
    symbols = pd.read_csv(OUT / "strict_symbol_coverage.csv", dtype={"symbol": str})
    targets = pd.read_csv(OUT / "strict_target_status.csv", dtype={"symbol": str})
    entries = pd.read_csv(OUT / "entry_notice_pdf_evidence.csv", dtype={"symbol": str}).fillna("")
    exits = pd.read_csv(OUT / "exit_notice_pdf_evidence.csv", dtype={"symbol": str}).fillna("")
    assert len(ENTRY_EFFECTIVE) == len(entries) == 25
    assert len(EXIT_EFFECTIVE) == len(exits) == 8
    evidence = []
    for row in symbols.itertuples(index=False):
        selection = targets.loc[(targets["symbol"] == row.symbol) &
                                (targets["strict_title_status"] == "explicit_entry_interior")]
        for interval in json.loads(row.strict_intervals):
            if interval["basis"] != "explicit_entry":
                continue
            start = interval["start_notice_date"]
            end = interval["end_notice_date"]
            inside = selection.loc[(selection["target_date"] > start) &
                                   (True if end is None else selection["target_date"] < end)]
            if inside.empty:
                continue
            entry_date = ENTRY_EFFECTIVE[(row.symbol, start)]
            assert start <= entry_date <= inside["target_date"].min(), (row.symbol, start, entry_date)
            entry = entries.loc[(entries["symbol"] == row.symbol) & (entries["entry_notice_date"] == start)].iloc[0]
            assert not entry.error and Path(entry.local_path).exists()
            assert hashlib.sha256(Path(entry.local_path).read_bytes()).hexdigest() == entry.sha256
            exit_date = None
            exit_url = ""
            exit_sha = ""
            if end is not None:
                exit_date = EXIT_EFFECTIVE[(row.symbol, end)]
                assert inside["target_date"].max() < exit_date, (row.symbol, end, exit_date)
                exit_row = exits.loc[(exits["symbol"] == row.symbol) & (exits["exit_notice_date"] == end)].iloc[0]
                assert not exit_row.error and Path(exit_row.local_path).exists()
                assert hashlib.sha256(Path(exit_row.local_path).read_bytes()).hexdigest() == exit_row.sha256
                exit_url, exit_sha = exit_row.source_url, exit_row.sha256
            for target in inside.itertuples(index=False):
                evidence.append({
                    "symbol": row.symbol, "target_date": target.target_date, "rank": target.rank,
                    "proof_kind": "official_entry_exit_pdf_effective_dates",
                    "entry_effective_date": entry_date, "exit_effective_date": exit_date,
                    "entry_notice_url": entry.source_url, "entry_pdf_sha256": entry.sha256,
                    "exit_notice_url": exit_url, "exit_pdf_sha256": exit_sha,
                })
    prefix = pd.read_csv(OUT / "legacy_prefix_target_differences.csv", dtype={"symbol": str})
    partial_url = "https://static.cninfo.com.cn/finalpage/2007-03-20/21601653.PDF"
    final_url = "https://static.cninfo.com.cn/finalpage/2013-10-17/63162842.PDF"
    partial_sha = "4ad99c019e32e9cd4cf929cf735e874a1e86b298baf99bf75ec1a03a6e5c404e"
    final_sha = "9dc47541596bf0537c3fa636bedcdd0479338a4bb7222b55dac1df2c59b9f357"
    assert hashlib.sha256((OUT / "evidence/000010_20070320_partial_exit.pdf").read_bytes()).hexdigest() == partial_sha
    assert hashlib.sha256((OUT / "evidence/000010_20131017_full_exit.pdf").read_bytes()).hexdigest() == final_sha
    for target in prefix.itertuples(index=False):
        evidence.append({
            "symbol": target.symbol, "target_date": target.target_date, "rank": target.rank,
            "proof_kind": "official_partial_exit_and_final_exit_pdf",
            "entry_effective_date": "2007-03-21", "exit_effective_date": "2013-10-18",
            "entry_notice_url": partial_url, "entry_pdf_sha256": partial_sha,
            "exit_notice_url": final_url, "exit_pdf_sha256": final_sha,
        })
    # 2008-06-05 official body says *ST 金花 -> ST 金花 on 2008-06-06;
    # the 2010-04-26 official body still lists prior name ST 金花 before
    # ST -> *ST on 2010-04-27. Thus the eight intervening 2010 targets
    # were ST even though a title-only "撤销退市风险警示" parser ended the interval.
    downgrade_path = OUT / "evidence/600080_20080605_downgrade.pdf"
    downgrade_sha = "5cb797e7bf1d8182a1a5dbe45f2a88a5efc264ca3cde5a9ecac35a5747192579"
    assert hashlib.sha256(downgrade_path.read_bytes()).hexdigest() == downgrade_sha
    next_entry = entries.loc[(entries["symbol"] == "600080") &
                             (entries["entry_notice_date"] == "2010-04-26")].iloc[0]
    assert hashlib.sha256(Path(next_entry.local_path).read_bytes()).hexdigest() == next_entry.sha256
    bridge = targets.loc[(targets["symbol"] == "600080") &
                         (targets["target_date"] >= "2008-06-06") &
                         (targets["target_date"] < "2010-04-27")]
    assert len(bridge) == 8 and bridge["target_date"].max() == "2010-04-22"
    for target in bridge.itertuples(index=False):
        evidence.append({
            "symbol": "600080", "target_date": target.target_date, "rank": target.rank,
            "proof_kind": "official_downgrade_pdf_and_next_entry_prior_name",
            "entry_effective_date": "2008-06-06", "exit_effective_date": None,
            "entry_notice_url": "https://static.cninfo.com.cn/finalpage/2008-06-05/40271256.PDF",
            "entry_pdf_sha256": downgrade_sha,
            "exit_notice_url": "", "exit_pdf_sha256": "",
            "corroborating_notice_url": next_entry.source_url,
            "corroborating_pdf_sha256": next_entry.sha256,
        })
    out = pd.DataFrame(evidence).sort_values(["target_date", "rank", "symbol"])
    assert not out.duplicated(["target_date", "symbol"]).any()
    out.to_csv(OUT / "certified_st_target_lower_bound.csv", index=False, encoding="utf-8-sig")
    summary = {
        "official_pdf_proved_rows": int((out["proof_kind"] == "official_entry_exit_pdf_effective_dates").sum()),
        "official_partial_exit_pdf_proved_rows": int((out["proof_kind"] == "official_partial_exit_and_final_exit_pdf").sum()),
        "official_ST_downgrade_bridge_rows": int((out["proof_kind"] == "official_downgrade_pdf_and_next_entry_prior_name").sum()),
        "minimum_proved_bad_target_rows": len(out),
        "affected_symbols": out["symbol"].nunique(),
        "first_proved_bad_target": out.iloc[0][["symbol", "target_date", "rank", "proof_kind"]].to_dict(),
        "last_proved_bad_target": out.iloc[-1][["symbol", "target_date", "rank", "proof_kind"]].to_dict(),
    }
    (OUT / "certified_st_target_lower_bound_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
