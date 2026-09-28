"""Source-bound ST classification of the 29 early delisted low-cap candidates."""

from __future__ import annotations

import gzip
import hashlib
import json
import re
from pathlib import Path

import fitz
import pandas as pd

from initial_2010_st_fetch import target_symbols


OUT = Path(__file__).resolve().parent / "initial_2010_st"
MANIFEST_DIRS = ["all_notices_2009_to_20100114", "all_notices_20100115_to_20100228"]
SOURCE_600355 = "https://static.cninfo.com.cn/finalpage/2010-01-27/57547383.PDF"
SHA_600355 = "1fc2d69a11ab7c204a6be5441b2c03bb64cb7f65e124ae98065d780d15c92e15"
SOURCE_600306 = "https://static.cninfo.com.cn/finalpage/2010-03-10/57671746.PDF"
SHA_600306 = "7baa5941f24ee6df0e83896a263523c4824d294e34450af8d517d38c5b8c015b"
SOURCE_000673 = "https://static.cninfo.com.cn/finalpage/2010-04-22/57849264.PDF"
SHA_000673 = "65c1cc27240bd4161fdc8b5f082853f9127288fb5394ac1a400de1e3d0dc6b40"
FULL_ST_NAMES = {
    "000018": "*ST中冠A", "000673": "ST大水", "000971": "ST迈亚",
    "000979": "*ST科苑", "002002": "*ST琼花", "600083": "ST博信",
    "600242": "ST华龙", "600385": "ST金泰", "600656": "ST方源",
    "600898": "*ST三联",
}


def st_name(name: str) -> bool:
    clean = re.sub(r"\s+", "", str(name).upper())
    return bool(re.match(r"^(?:[SG])?\*?(?:ST|PT)", clean))


def verify_archive() -> dict:
    count = pages = notices = 0
    for dirname in MANIFEST_DIRS:
        for path in (OUT / dirname).glob("*/manifest.json"):
            item = json.loads(path.read_text(encoding="utf-8"))
            assert item["complete"] and len(item["pages"]) == item["page_count"]
            ids = set()
            for page in item["pages"]:
                raw = gzip.decompress((path.parent / f"page_{page['page']:04d}.json.gz").read_bytes())
                assert hashlib.sha256(raw).hexdigest() == page["raw_sha256"]
                data = json.loads(raw)
                assert int(data["totalAnnouncement"] or 0) == item["totalAnnouncement"]
                assert len(data.get("announcements") or []) == page["rows"]
                for doc in data.get("announcements") or []:
                    key = str(doc["announcementId"])
                    assert key not in ids, (path, key)
                    ids.add(key)
            assert len(ids) == item["totalAnnouncement"]
            count += 1
            pages += item["page_count"]
            notices += item["totalAnnouncement"]
    assert count == 58
    return {"complete_symbol_period_queries": count, "raw_pages_verified": pages,
            "raw_notice_ids_verified": notices}


def evidence() -> pd.DataFrame:
    parts = [pd.read_csv(OUT / name, dtype={"symbol": str}).fillna("") for name in
             ("near_date_pdf_evidence.csv", "post_date_pdf_evidence.csv")]
    docs = pd.concat(parts, ignore_index=True)
    assert len(docs) == 210 and not docs["sha256"].eq("").any()
    for row in docs.itertuples(index=False):
        assert hashlib.sha256(Path(row.path).read_bytes()).hexdigest() == row.sha256
    # MuPDF repeats header tokens in these two SSE PDFs, so the narrow regex
    # misses the clearly printed issuer name. Assert the actual body text.
    index_600213 = docs.index[(docs["symbol"] == "600213") & (docs["announcement_date"] == "2010-01-28")][0]
    assert "600213" in docs.loc[index_600213, "text_excerpt"] and "亚星客车" in docs.loc[index_600213, "text_excerpt"]
    docs.loc[index_600213, "header_name_candidates"] = "亚星客车"
    p = OUT / "near_date_pdfs/600355_57547383.pdf"
    assert hashlib.sha256(p.read_bytes()).hexdigest() == SHA_600355
    body = " ".join(fitz.open(p)[i].get_text() for i in range(1))
    assert "600355" in body and "精伦电子" in body and "会因2008年、2009年连续两年亏损" in body.replace(" ", "")
    docs = pd.concat([docs, pd.DataFrame([{
        "symbol": "600355", "announcement_date": "2010-01-27",
        "announcementId": "57547383", "title": "2009年年度业绩预亏暨退市风险提示公告",
        "url": SOURCE_600355, "sha256": SHA_600355, "bytes": p.stat().st_size,
        "path": str(p), "text_excerpt": body[:4500], "header_name_candidates": "精伦电子", "error": "",
    }])], ignore_index=True)
    late_raw = gzip.decompress((OUT / "600306_late_query.json.gz").read_bytes())
    assert hashlib.sha256(late_raw).hexdigest() == "1fe39f41d32d0e518ad07267c46e10a1b5e12f5df21eb215c9d953939c160b19"
    late = json.loads(late_raw)
    assert int(late["totalAnnouncement"]) == len(late["announcements"]) == 18
    assert str(late["announcements"][-1]["announcementId"]) == "57671746" or any(
        str(x["announcementId"]) == "57671746" for x in late["announcements"])
    p = OUT / "near_date_pdfs/600306_57671746.pdf"
    assert hashlib.sha256(p.read_bytes()).hexdigest() == SHA_600306
    body = fitz.open(p)[0].get_text()
    assert "600306" in body and "商业城" in body
    docs = pd.concat([docs, pd.DataFrame([{
        "symbol": "600306", "announcement_date": "2010-03-10",
        "announcementId": "57671746", "title": "关于股东偿还股改垫付对价的提示性公告",
        "url": SOURCE_600306, "sha256": SHA_600306, "bytes": p.stat().st_size,
        "path": str(p), "text_excerpt": body[:4500], "header_name_candidates": "商业城", "error": "",
    }])], ignore_index=True)
    p = OUT / "near_date_pdfs/000673_57849264.pdf"
    assert hashlib.sha256(p.read_bytes()).hexdigest() == SHA_000673
    body = fitz.open(p)[0].get_text()
    assert "000673" in body and "ST 大水" in body and "2010 年4 月23 日" in body
    docs = pd.concat([docs, pd.DataFrame([{
        "symbol": "000673", "announcement_date": "2010-04-22",
        "announcementId": "57849264", "title": "董事会关于公司股票交易实行退市风险警示的公告",
        "url": SOURCE_000673, "sha256": SHA_000673, "bytes": p.stat().st_size,
        "path": str(p), "text_excerpt": body[:4500], "header_name_candidates": "ST 大水", "error": "",
    }])], ignore_index=True)
    return docs


def main() -> None:
    archive = verify_archive()
    docs = evidence()
    sz = pd.read_csv(OUT / "szse_historical_names_two_dates.csv", dtype={"symbol": str}).fillna("")
    initial, first = target_symbols()
    rows = []
    for symbol in sorted(initial | first):
        subset = docs.loc[(docs["symbol"] == symbol) & (docs["header_name_candidates"] != "")].copy()
        subset = subset.sort_values(["announcement_date", "announcementId"])
        for date, cohort in (("2010-01-04", "initial_seed"), ("2010-01-14", "first_rebalance")):
            if date == "2010-01-04" and symbol not in initial:
                continue
            if date == "2010-01-14" and symbol not in first:
                continue
            before = subset.loc[subset["announcement_date"] <= date]
            after = subset.loc[subset["announcement_date"] >= date]
            pre = before.iloc[-1] if not before.empty else None
            post = after.iloc[0] if not after.empty else None
            name = pre["header_name_candidates"].split("|")[0] if pre is not None else ""
            pre_st = st_name(name) if name else None
            post_name = post["header_name_candidates"].split("|")[0] if post is not None else ""
            post_st = st_name(post_name) if post_name else None
            sz_hit = sz.loc[(sz["symbol"] == symbol) & (sz["target_date"] == date)]
            sz_st = bool(sz_hit.iloc[0]["is_st_prefix"]) if len(sz_hit) else None
            if pre_st is not None and post_st is not None and pre_st == post_st:
                status = "proven_ST" if pre_st else "proven_non_ST"
                basis = "official_issuer_pdf_short_name_bracket"
            elif pre_st is not None and sz_st is not None and pre_st == sz_st:
                status = "proven_ST" if pre_st else "proven_non_ST"
                basis = "official_issuer_pdf_plus_SZSE_historical_name"
            else:
                status = "insufficient_evidence"
                basis = "no_consistent_bracketing_official_short_name"
            if status == "proven_ST":
                assert re.sub(r"\s+", "", FULL_ST_NAMES[symbol]) in re.sub(
                    r"\s+", "", pre["text_excerpt"]), (symbol, date, FULL_ST_NAMES[symbol])
            rows.append({
                "symbol": symbol, "target_date": date, "cohort": cohort,
                "status": status, "basis": basis,
                "historical_short_name": FULL_ST_NAMES.get(symbol, name) if status == "proven_ST" else name,
                "raw_pre_header_name": name,
                "pre_notice_date": pre["announcement_date"] if pre is not None else "",
                "pre_source_url": pre["url"] if pre is not None else "",
                "pre_pdf_sha256": pre["sha256"] if pre is not None else "",
                "post_notice_date": post["announcement_date"] if post is not None else "",
                "post_historical_short_name": post_name,
                "post_source_url": post["url"] if post is not None else "",
                "post_pdf_sha256": post["sha256"] if post is not None else "",
                "szse_historical_short_name": sz_hit.iloc[0]["historical_short_name"] if len(sz_hit) else "",
                "szse_last_change_date": sz_hit.iloc[0]["last_change_date"] if len(sz_hit) else "",
                "szse_next_change_date": sz_hit.iloc[0]["next_change_date"] if len(sz_hit) else "",
                "szse_source_url": sz_hit.iloc[0]["source_url"] if len(sz_hit) else "",
                "szse_cache_sha256": sz_hit.iloc[0]["local_derived_cache_sha256"] if len(sz_hit) else "",
            })
    out = pd.DataFrame(rows).sort_values(["target_date", "symbol"])
    assert len(out) == 51
    out.to_csv(OUT / "two_date_st_status_evidence.csv", index=False, encoding="utf-8-sig")
    counts = out.groupby(["target_date", "status"]).size().unstack(fill_value=0).to_dict("index")
    summary = {**archive, "additional_600306_late_page_verified": 1,
               "additional_600306_late_notice_ids_verified": 18,
               "total_raw_pages_verified": archive["raw_pages_verified"] + 1,
               "total_raw_notice_ids_verified": archive["raw_notice_ids_verified"] + 18,
               "pdfs_verified": len(docs), "target_rows": len(out),
               "status_counts": counts, "unknown_symbols": out.loc[out["status"] == "insufficient_evidence", "symbol"].unique().tolist()}
    (OUT / "two_date_st_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
