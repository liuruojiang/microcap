"""Independent read-only challenge of the frozen Top30 ST expansion."""

from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import fitz
import requests


HERE = Path(__file__).resolve().parent
ST = HERE.parent
ROOT = HERE.parents[3]


def rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def remote_hash(event: dict[str, str]) -> dict[str, object]:
    response = requests.get(event["source_url"], timeout=30)
    return {
        "symbol": event["symbol"],
        "stage": event["stage"],
        "http_status": response.status_code,
        "remote_sha_match": response.status_code == 200 and hashlib.sha256(response.content).hexdigest() == event["sha256"],
    }


def main() -> None:
    events = rows(HERE / "body_proved_event_evidence.csv")
    new = rows(HERE / "new_proven_bad_targets.csv")
    old = rows(ST / "challenge_target_checks.csv") + rows(ST / "challenge_additional_targets.csv")
    members = rows(ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv")
    effective_members = rows(ST.parent / "full_replay/members_by_rebalance.csv")
    top30 = rows(HERE / "frozen_top30.csv")
    assert len(events) == 6
    assert len(top30) == 30
    member_keys = {(r["symbol"].zfill(6), r["rebalance_date"], r["rank"]) for r in members}
    effective_keys = {(r["symbol"].zfill(6), r["rebalance_date"]) for r in effective_members}
    old_keys = {(r["symbol"], r["target_date"], r["rank"]) for r in old}
    new_keys = {(r["symbol"], r["target_date"], r["rank"]) for r in new}
    old_effective = {(symbol, day) for symbol, day, _ in old_keys if (symbol, day) in effective_keys}
    new_effective = {(symbol, day) for symbol, day, _ in new_keys if (symbol, day) in effective_keys}

    event_checks = []
    for event in events:
        path = Path(event["local_pdf"])
        payload = path.read_bytes()
        body = "".join(page.get_text() for page in fitz.open(stream=payload, filetype="pdf"))
        compact = "".join(body.split())
        y, m, d = [int(s) for s in event["effective_date"].split("-")]
        event_checks.append({
            "symbol": event["symbol"],
            "stage": event["stage"],
            "local_sha_match": hashlib.sha256(payload).hexdigest() == event["sha256"],
            "code_in_body": event["symbol"] in compact[:500],
            "effective_date_in_body": f"{y}年{m}月{d}日" in compact,
            "body_length": len(compact),
        })
    with ThreadPoolExecutor(max_workers=6) as pool:
        remote_checks = list(pool.map(remote_hash, events))

    meta = {symbol: json.loads((ROOT / ".microcap_index_cache/security_meta" / f"{symbol}.json").read_text(encoding="utf-8"))
            for symbol in {r["symbol"] for r in new}}
    old_meta_st = []
    for row in new:
        day = row["target_date"]
        old_meta_st.append(any(i["start"] <= day and (i.get("end") is None or day <= i["end"])
                               for i in meta[row["symbol"]].get("st_intervals") or []))

    periods = {symbol: {r["stage"]: r["effective_date"] for r in events if r["symbol"] == symbol}
               for symbol in {r["symbol"] for r in events}}
    independent_period_members = {symbol: {
        (r["symbol"].zfill(6), r["rebalance_date"], r["rank"])
        for r in members
        if r["symbol"].zfill(6) == symbol and
        periods[symbol]["entry"] <= r["rebalance_date"] < periods[symbol]["exit"]
    } for symbol in periods}
    for symbol, group in independent_period_members.items():
        assert all(periods[symbol]["entry"] <= day < periods[symbol]["exit"] for _, day, _ in group)

    summary = {
        "event_checks": event_checks,
        "remote_checks": remote_checks,
        "new_input_rows": len(new),
        "new_unique_keys": len(new_keys),
        "new_by_symbol": dict(Counter(r["symbol"] for r in new)),
        "independent_period_member_counts": {k: len(v) for k, v in independent_period_members.items()},
        "independent_period_matches_new": {k: v == {x for x in new_keys if x[0] == k} for k, v in independent_period_members.items()},
        "new_exact_formal_member_count": len(new_keys & member_keys),
        "old_250_overlap_count": len(new_keys & old_keys),
        "old_meta_st_count": sum(old_meta_st),
        "new_after_downgrade_count": sum(periods[r["symbol"]]["downgrade"] <= r["target_date"] for r in new),
        "new_effective_rebalance_member_count": len(new_effective),
        "new_effective_first": min(day for _, day in new_effective),
        "old_effective_rebalance_member_count": len(old_effective),
        "combined_effective_rebalance_member_count": len(old_effective | new_effective),
        "combined_effective_symbols": len({symbol for symbol, _ in old_effective | new_effective}),
        "combined_effective_first": min(day for _, day in old_effective | new_effective),
        "other_top30_symbols_not_closed": sorted({r["symbol"] for r in top30} - set(periods)),
    }
    (HERE / "independent_challenge_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
