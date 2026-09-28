"""Classify archived candidate titles as review leads, never as PIT ST states."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from strict_st_classifier import classify_title


OUT = Path(__file__).resolve().parent
ROOT = OUT.parents[2]


def main() -> None:
    notices = pd.read_csv(OUT / "market_archive_candidate_notices.csv", dtype={"symbol": str}).fillna("")
    notices["symbol"] = notices["symbol"].str.zfill(6)
    members = pd.read_csv(ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv", dtype={"symbol": str})
    members["symbol"] = members["symbol"].str.zfill(6)
    recovery = pd.read_csv(ROOT / "research_reports/20260926_l1_repair/data_recovery/coverage_by_symbol.csv", dtype={"symbol": str})
    old = set(members["symbol"])
    extra = set(recovery.loc[pd.to_numeric(recovery["price_rows"], errors="coerce") > 0, "symbol"].str.zfill(6)) - old
    assert len(old) == 1185 and len(extra) == 251 and set(notices["symbol"]) <= old | extra
    notices["cohort"] = notices["symbol"].map(lambda symbol: "old_target_1185" if symbol in old else "new_delisted_251")
    notices["title_class"] = notices["title"].map(classify_title)
    notices.to_csv(OUT / "market_archive_title_triage.csv", index=False, encoding="utf-8-sig")
    counts = notices.groupby(["cohort", "title_class"]).agg(notices=("announcementId", "size"), symbols=("symbol", "nunique")).reset_index()
    counts.to_csv(OUT / "market_archive_title_triage_counts.csv", index=False, encoding="utf-8-sig")
    actionable = notices.loc[notices["title_class"].isin(["entry", "remain_st", "needs_review"])]
    old_leads = set(actionable.loc[actionable["cohort"] == "old_target_1185", "symbol"])
    extra_leads = set(actionable.loc[actionable["cohort"] == "new_delisted_251", "symbol"])
    candidate_old_rows = members.loc[members["symbol"].isin(old_leads)]
    # These are historical target rows on names with an event lead somewhere
    # in the source period; each needs a dated-status audit, not all are wrong.
    summary = {
        "all_title_lead_notices": len(notices),
        "all_title_lead_symbols": notices["symbol"].nunique(),
        "old_title_lead_symbols": len(set(notices["symbol"]) & old),
        "extra_title_lead_symbols": len(set(notices["symbol"]) & extra),
        "old_actionable_event_lead_symbols": len(old_leads),
        "extra_actionable_event_lead_symbols": len(extra_leads),
        "old_historical_target_rows_on_actionable_lead_symbols_needing_PIT_review": len(candidate_old_rows),
        "old_historical_target_dates_on_actionable_lead_symbols": candidate_old_rows["rebalance_date"].nunique(),
        "title_classes_are_discovery_only": True,
        "classification_counts": counts.to_dict(orient="records"),
    }
    (OUT / "market_archive_title_triage_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
