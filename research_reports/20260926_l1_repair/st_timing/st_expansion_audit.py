"""Verify top-30 source hashes and enumerate open event review items."""

from __future__ import annotations

import hashlib
import json
import re
from html import unescape
from pathlib import Path

import pandas as pd


OUT = Path(__file__).resolve().parent / "expansion_top30"
CERTIFIED_IDS = {"50084440", "58645540", "61464917", "1207586688", "1209981072", "1216934551"}
STATE_WORDS = re.compile(r"风险警示|特别处理|摘帽|解除|撤销|撤消|变更.{0,5}简称")


def main() -> None:
    manifest = pd.read_csv(OUT / "notice_pdf_manifest.csv", dtype={"symbol": str, "announcementId": str}).fillna("")
    frozen = pd.read_csv(OUT / "frozen_top30.csv", dtype={"symbol": str})
    assert len(manifest) == 262 and manifest["symbol"].nunique() == 30
    assert not manifest.duplicated(["symbol", "announcementId"]).any()
    assert not manifest["error"].ne("").any()
    assert manifest["format"].value_counts().to_dict() == {"pdf": 248, "html": 14}
    for row in manifest.itertuples(index=False):
        raw = Path(row.local_source_path).read_bytes()
        assert hashlib.sha256(raw).hexdigest() == row.sha256
        assert len(raw) == row.bytes
        body = Path(row.local_text_path).read_text(encoding="utf-8")
        assert len(body) == row.extracted_chars
    manifest["notice_date"] = pd.to_datetime(manifest["announcementTime"], unit="ms", utc=True).dt.tz_convert("Asia/Shanghai").dt.date.astype(str)
    manifest["clean_title"] = manifest["title"].map(lambda t: re.sub(r"<[^>]*>", "", unescape(t)))
    events = manifest.loc[manifest["clean_title"].map(lambda title: bool(STATE_WORDS.search(title)))].copy()
    events = events.loc[~events["announcementId"].isin(CERTIFIED_IDS)].copy()
    events = events.merge(frozen[["symbol", "priority", "old_target_rows", "first_target_date", "last_target_date"]], on="symbol")
    events["date_relation_to_old_targets"] = events.apply(
        lambda row: "before_first_target" if row.notice_date < row.first_target_date else
        "after_last_target" if row.notice_date > row.last_target_date else "within_old_target_span", axis=1)
    events["review_reason"] = events.apply(
        lambda row: "scanned_or_unextractable_body" if row.extracted_chars < 50 else
        "prehistory_or_initial_state_needs_closure" if row.date_relation_to_old_targets == "before_first_target" else
        "effective_date_and_full_exit_needs_review" if row.date_relation_to_old_targets == "within_old_target_span" else
        "after_last_target_no_direct_old_row_overlap", axis=1)
    events = events.sort_values(["priority", "notice_date", "announcementId"])
    events[["priority", "symbol", "old_target_rows", "notice_date", "announcementId", "clean_title",
            "title_class", "date_relation_to_old_targets", "review_reason", "format", "extracted_chars",
            "source_url", "sha256", "local_source_path", "local_text_path"]].to_csv(
        OUT / "unresolved_state_notice_review.csv", index=False, encoding="utf-8-sig")
    proved = pd.read_csv(OUT / "new_proven_bad_targets.csv", dtype={"symbol": str})
    symbol_rows = []
    for target in frozen.itertuples(index=False):
        subset = events.loc[events["symbol"] == target.symbol]
        symbol_rows.append({"priority": target.priority, "symbol": target.symbol,
                            "old_target_rows": target.old_target_rows,
                            "first_target_date": target.first_target_date, "last_target_date": target.last_target_date,
                            "all_six_query_source_docs": int((manifest["symbol"] == target.symbol).sum()),
                            "unresolved_state_title_docs": len(subset),
                            "unresolved_pre_first_docs": int((subset["date_relation_to_old_targets"] == "before_first_target").sum()),
                            "unresolved_within_span_docs": int((subset["date_relation_to_old_targets"] == "within_old_target_span").sum()),
                            "unresolved_after_last_docs": int((subset["date_relation_to_old_targets"] == "after_last_target").sum()),
                            "new_pdf_proved_bad_target_rows": int((proved["symbol"] == target.symbol).sum()),
                            "scope_note": "title_or_body_gap_remains; do_not_infer_non_ST"})
    pd.DataFrame(symbol_rows).to_csv(OUT / "top30_symbol_disposition.csv", index=False, encoding="utf-8-sig")
    summary = {"frozen_symbols": 30, "old_target_rows": int(frozen["old_target_rows"].sum()),
               "source_docs_hash_verified": len(manifest), "pdf_docs": int((manifest["format"] == "pdf").sum()),
               "official_html_docs": int((manifest["format"] == "html").sum()),
               "extraction_under_50_chars": int((manifest["extracted_chars"] < 50).sum()),
               "unresolved_state_title_docs": len(events),
               "unresolved_before_first": int((events["date_relation_to_old_targets"] == "before_first_target").sum()),
               "unresolved_within_old_target_span": int((events["date_relation_to_old_targets"] == "within_old_target_span").sum()),
               "unresolved_after_last": int((events["date_relation_to_old_targets"] == "after_last_target").sum()),
               "new_proven_bad_target_rows": len(proved),
               "symbols_with_new_proven_bad_targets": int(proved["symbol"].nunique())}
    (OUT / "source_audit_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
