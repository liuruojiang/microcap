"""Screen frozen 2010 old Top100 against SSE 2008/2009 ST yearbook tables.

This produces investigation leads, never formal point-in-time status or NAV.
"""

from __future__ import annotations

import csv
import hashlib
import json
import re
from collections import Counter
from pathlib import Path

import fitz


ROOT = Path(__file__).resolve().parent
REPO = ROOT.parents[3]
RANK = REPO / "research_reports/20260926_l1_repair/data_recovery/conditional_two_date_rank.csv"
META = REPO / ".microcap_index_cache/security_meta"


def codes(text: str) -> set[str]:
    return set(re.findall(r"\b600\d{3}\b", text))


def main() -> None:
    pdf08 = fitz.open(ROOT / "sse_factbook_2008.pdf")
    pdf09 = fitz.open(ROOT / "sse_factbook_2009.pdf")
    page08 = pdf08[84].get_text(sort=True)
    # Two adjacent year-end tables are on the same printed page.
    end08 = page08.split("2008年退市风险警示公司（截至2008年12月31日）", 1)[1]
    st08 = set(re.findall(r"\b(600\d{3})\s+[S*]*ST", end08))
    risk09 = codes(pdf09[213].get_text(sort=True).split("2009年退市风险警示的公司", 1)[1])
    special09 = codes(pdf09[214].get_text(sort=True).split("2009年特别处理的公司", 1)[1].split("2009年暂停上市的公司", 1)[0])
    exits09 = codes(pdf09[215].get_text(sort=True).split("2009年取消特别处理的公司", 1)[1])
    assert "600603" in st08 and "600137" not in st08
    assert "600506" in risk09 and "600603" not in exits09
    with RANK.open(encoding="utf-8-sig", newline="") as handle:
        rank_rows = list(csv.DictReader(handle))
    old = [x for x in rank_rows if x["source"] == "old_100" and x["symbol"].startswith("6")]
    assert Counter(x["date"] for x in rank_rows if x["source"] == "old_100") == {
        "2010-01-04": 100, "2010-01-14": 100
    }
    out = []
    for row in old:
        symbol, date = row["symbol"], row["date"]
        path = META / f"{symbol}.json"
        meta = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
        intervals = meta.get("st_intervals", [])
        old_meta_st = any(x["start"] <= date and (x["end"] is None or date <= x["end"])
                          for x in intervals)
        source_flags = []
        if symbol in st08:
            source_flags.append("2008-12-31_ST_snapshot")
        if symbol in risk09:
            source_flags.append("2009_risk_warning_entry")
        if symbol in special09:
            source_flags.append("2009_other_ST_entry")
        if symbol in exits09:
            source_flags.append("2009_ST_exit_or_risk_downgrade")
        # Only year-end ST with no listed 2009 cancellation, or 2009 entry.
        lead = bool((symbol in st08 and symbol not in exits09) or symbol in risk09 or symbol in special09)
        if not lead:
            continue
        out.append({
            "date": date, "symbol": symbol, "old_rank": row["old_rank"],
            "conditional_rank": row["conditional_rank"],
            "conditional_top100": row["conditional_top100"],
            "old_meta_st": str(old_meta_st),
            "old_meta_present": str(bool(meta)),
            "old_meta_sha256": hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else "",
            "old_meta_notice_count": meta.get("notice_count", ""),
            "source_flags": ";".join(source_flags),
            "unresolved_old_meta_non_st_lead": str(not old_meta_st),
        })
    target = ROOT / "initial_old_sse_st_yearbook_screen.csv"
    with target.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(out[0]))
        writer.writeheader()
        writer.writerows(out)
    summary = {
        "rank_path": str(RANK.relative_to(REPO)),
        "rank_sha256": hashlib.sha256(RANK.read_bytes()).hexdigest(),
        "old_100_sse_rows": dict(Counter(x["date"] for x in old)),
        "yearend_2008_st_codes": len(st08),
        "year_2009_risk_entry_codes": len(risk09),
        "year_2009_other_st_entry_codes": len(special09),
        "year_2009_exit_or_downgrade_codes": len(exits09),
        "yearbook_lead_rows": dict(Counter(x["date"] for x in out)),
        "old_meta_non_st_lead_rows": dict(Counter(x["date"] for x in out if x["old_meta_st"] == "False")),
        "lead_unique_symbols": len({x["symbol"] for x in out}),
        "csv_sha256": hashlib.sha256(target.read_bytes()).hexdigest(),
        "screen_not_proof_of_absence_or_st_state": True,
    }
    (ROOT / "initial_old_sse_st_yearbook_screen_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(summary, ensure_ascii=False))
    for x in out:
        if x["old_meta_st"] == "False":
            print(x)


if __name__ == "__main__":
    main()
