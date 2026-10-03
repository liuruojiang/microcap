"""Verify archived 600633 issuer chain and match it to frozen old target/state dates."""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

import pandas as pd


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]
ANCHOR_IDS = {"57435883", "57472798", "57494686", "57616191",
              "57705138", "57803955", "57898745"}
SYMBOL = "600633"


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    query = json.loads((HERE / "query_manifest.json").read_text(encoding="utf-8"))
    assert query["total"] == query["unique_ids"] == 135
    assert len(query["pages"]) == 5
    for page in query["pages"]:
        path = HERE / f"page_{page['page']:03d}.json"
        assert path.stat().st_size == page["bytes"] and sha(path) == page["sha256"]
    notices = read_csv(HERE / "all_notices.csv")
    assert len(notices) == len({x["id"] for x in notices}) == 135
    sources = json.loads((HERE / "pdf_manifest.json").read_text(encoding="utf-8"))
    assert len(sources) == 11
    for source in sources:
        assert sha(HERE / "pdfs" / f"{source['id']}.pdf") == source["pdf_sha256"]
    source_by_id = {x["id"]: x for x in sources}
    anchors = sorted((source_by_id[x] for x in ANCHOR_IDS), key=lambda x: x["date"])

    members = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv"
    assert sha(members) == "04a851bb6e7e67f181607c499a0ca3264443b6e3b655817ac0933532517f7517"
    old = [x for x in read_csv(members) if x["symbol"].zfill(6) == SYMBOL]
    assert len(old) == 8 and old[0]["rebalance_date"] == "2010-01-14"
    assert old[-1]["rebalance_date"] == "2010-04-22"
    bracket_rows = []
    for date, rank in [("2010-01-04", "initial_seed_9")] + [
        (x["rebalance_date"], x["rank"]) for x in old
    ]:
        before = max((x for x in anchors if x["date"] <= date), key=lambda x: x["date"])
        after = min((x for x in anchors if x["date"] > date), key=lambda x: x["date"])
        bracket_rows.append({"old_date": date, "old_rank": rank,
                             "before_date": before["date"], "before_id": before["id"],
                             "after_date": after["date"], "after_id": after["id"]})
    with (HERE / "old_target_status_brackets.csv").open(
        "w", encoding="utf-8-sig", newline=""
    ) as handle:
        writer = csv.DictWriter(handle, fieldnames=list(bracket_rows[0]))
        writer.writeheader()
        writer.writerows(bracket_rows)

    replay = ROOT / "research_reports/20260926_l1_repair/full_replay"
    state = [x["rebalance_date"] for x in read_csv(replay / "members_by_rebalance.csv")
             if x["symbol"] == SYMBOL]
    assert len(state) == 45 and state[0] == "2010-01-14" and state[-1] == "2011-09-22"
    halted = [x for x in state if "2010-04-30" <= x < "2011-09-29"]
    assert len(halted) == 37
    sellable = pd.read_parquet(replay / "sellable.parquet")
    assert all(not sellable.loc[x, SYMBOL] for x in halted)
    raw_price = ROOT / ".microcap_index_cache/prices_raw/600633.csv"
    price_dates = [x["date"] for x in read_csv(raw_price)]
    last_before_halt = max(x for x in price_dates if x < "2010-04-30")
    first_after_halt = min(x for x in price_dates if x >= "2010-04-30")
    assert last_before_halt == "2010-04-29" and first_after_halt == "2011-09-29"
    result = {
        "scope": "research_only_600633_old_replay_status_not_valuation",
        "old_members_sha256": sha(members),
        "full_replay_members_sha256": sha(replay / "members_by_rebalance.csv"),
        "full_replay_sellable_sha256": sha(replay / "sellable.parquet"),
        "raw_price_sha256": sha(raw_price),
        "issuer_announcements": 135, "issuer_pdfs_checked": 11,
        "old_target_rows": len(old), "old_initial_seed_mentioned": True,
        "old_effective_holding_rebalance_states": len(state),
        "halted_unsellable_rebalance_states": len(halted),
        "halted_state_first": halted[0], "halted_state_last": halted[-1],
        "last_raw_price_before_halt": last_before_halt,
        "first_raw_price_after_halt": first_after_halt,
        "risk_warning_entry_effective": "2009-03-24",
        "halt_issuer_notice_date": "2010-04-30",
        "listing_suspension_effective": "2010-05-25",
        "resume_full_ST_exit_effective": "2011-09-29",
        "warning": "Rebalance holding snapshots are not trades or an account valuation.",
    }
    (HERE / "audit_summary.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps({k: result[k] for k in (
        "issuer_announcements", "old_target_rows",
        "old_effective_holding_rebalance_states", "halted_unsellable_rebalance_states",
        "resume_full_ST_exit_effective")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
