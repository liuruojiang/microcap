"""Build a 600543 issuer-dated ST metadata proposal without touching formal state."""

from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter
from pathlib import Path

from pypdf import PdfReader


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
OLD_META = ROOT / ".microcap_index_cache/security_meta/600543.json"
OLD_TARGET = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv"
OLD_EFFECTIVE = ROOT / "research_reports/20260926_l1_repair/full_replay/members_by_rebalance.csv"
GATE = ROOT / "research_reports/20260926_l1_repair/shadow_rank_gate/summary.json"
SECURITY_MASTER = ROOT / ".microcap_index_cache/security_master.csv"
PROXY_META = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_meta.json"
PDF_DIR = ROOT / "research_reports/20261002_l2_full_repair/adversary"

EXPECTED = {
    OLD_META: "446a5919cbe82c5483ab4d4c0c46bcfb83df3d03e836cd8b4ac08322f974fc93",
    OLD_TARGET: "04a851bb6e7e67f181607c499a0ca3264443b6e3b655817ac0933532517f7517",
    OLD_EFFECTIVE: "7563fdab9b4a37f35fc2a8e54c967fc6bf6e3ad6a4dc6f298aec59f4beb58b1c",
    PDF_DIR / "600543_20230428_stcn.pdf": "89251a8f76f8190db7c8937997b8d081aacbde4820117dd7646ccd9e2a7202c5",
    PDF_DIR / "600543_20240705_MREZ_mirror.pdf": "9db7b121274a7d1cb2d7c0cc148048ce40cd60b1d861fb57fd50c52ff66cec62",
    PDF_DIR / "600543_20260424_NEMZ.pdf": "e44b19fbb6bee6dde1eb8011b714fc278654595e5d2d795d44fd4732fef99fb1",
}

EVENTS = [
    {
        "kind": "ST_entry",
        "announcement_date": "2023-04-28",
        "effective_date": "2023-05-04",
        "local_pdf": PDF_DIR / "600543_20230428_stcn.pdf",
        "source_url": "https://epaper.stcn.com/att/202304/28/2abb1e5a-46dc-4930-980f-fd25cbab4944.pdf",
        "required_text": "2023年5月4日",
    },
    {
        "kind": "ST_exit",
        "announcement_date": "2024-07-05",
        "effective_date": "2024-07-08",
        "local_pdf": PDF_DIR / "600543_20240705_MREZ_mirror.pdf",
        "source_url": "https://static.sse.com.cn/disclosure/listedinfo/announcement/c/new/2024-07-05/600543_20240705_MREZ.pdf",
        "local_pdf_origin": "https://www.cczq.com/home/uploads/2024/07-05/08-29-460171-1615289614.pdf",
        "required_text": "2024年7月8日",
    },
    {
        "kind": "ST_entry",
        "announcement_date": "2026-04-24",
        "effective_date": "2026-04-27",
        "local_pdf": PDF_DIR / "600543_20260424_NEMZ.pdf",
        "source_url": "https://static.sse.com.cn/disclosure/listedinfo/announcement/c/new/2026-04-24/600543_20260424_NEMZ.pdf",
        "local_pdf_origin": "https://stockmc.xueqiu.com/202604/600543_20260424_NEMZ.pdf",
        "required_text": "2026年4月27日",
    },
]


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def is_affected(date: str) -> bool:
    return "2023-05-04" <= date <= "2024-07-07" or date >= "2026-04-27"


def write_csv(path: Path, rows: list[dict[str, object]], fields: list[str]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    for path, expected in EXPECTED.items():
        actual = sha(path)
        if actual != expected:
            raise RuntimeError(f"Frozen input changed: {path}: expected {expected}, got {actual}")
    evidence = []
    for event in EVENTS:
        pdf = event["local_pdf"]
        text = "".join("".join((page.extract_text() or "").split()) for page in PdfReader(pdf).pages)
        assert event["required_text"] in text, (pdf, event["required_text"])
        evidence.append({
            key: str(value.relative_to(ROOT)) if key == "local_pdf" else value
            for key, value in event.items() if key != "required_text"
        } | {"local_pdf_sha256": sha(pdf), "required_text_confirmed": event["required_text"]})

    original = json.loads(OLD_META.read_text(encoding="utf-8"))
    assert original["symbol"] == "600543"
    assert original["st_intervals"] == [{"start": "2026-09-17", "end": None, "source": "current_name_snapshot"}]
    assert str(original["notice_query_status"]).startswith("error:")
    candidate = dict(original)
    candidate["current_st_history_resolved"] = True
    candidate["st_intervals"] = [
        {"start": "2023-05-04", "end": "2024-07-07", "source": "issuer_announcements_2023-04-28_2024-07-05"},
        {"start": "2026-04-27", "end": None, "source": "issuer_announcement_2026-04-24"},
    ]
    candidate_path = HERE / "600543.proposed.json"
    candidate_path.write_text(json.dumps(candidate, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    target_rows = read_csv(OLD_TARGET)
    assert len(target_rows) == 43200
    affected_target = [
        {"rebalance_date": row["rebalance_date"], "rank": int(row["rank"]), "symbol": row["symbol"].zfill(6),
         "name": row["name"], "market_cap": row["market_cap"],
         "event_interval": "2023-05-04_to_2024-07-07" if row["rebalance_date"] <= "2024-07-07" else "2026-04-27_onward"}
        for row in target_rows if row["symbol"].zfill(6) == "600543" and is_affected(row["rebalance_date"])
    ]
    effective_rows = read_csv(OLD_EFFECTIVE)
    # The effective file has no rank. Compute its within-date list position only.
    effective_ordinal = Counter()
    corrected_effective = []
    for row in effective_rows:
        date = row["rebalance_date"]
        effective_ordinal[date] += 1
        if row["symbol"].zfill(6) == "600543" and is_affected(date):
            corrected_effective.append({
                "rebalance_date": date, "symbol": "600543", "old_effective_list_position": effective_ordinal[date],
                "event_interval": "2023-05-04_to_2024-07-07" if date <= "2024-07-07" else "2026-04-27_onward",
            })
    assert len(affected_target) == 28 and len(corrected_effective) == 27
    assert Counter(row["event_interval"] for row in affected_target) == {"2023-05-04_to_2024-07-07": 18, "2026-04-27_onward": 10}
    assert Counter(row["event_interval"] for row in corrected_effective) == {"2023-05-04_to_2024-07-07": 18, "2026-04-27_onward": 9}
    write_csv(HERE / "affected_old_targets.csv", affected_target,
              ["rebalance_date", "rank", "symbol", "name", "market_cap", "event_interval"])
    write_csv(HERE / "affected_old_effective.csv", corrected_effective,
              ["rebalance_date", "symbol", "old_effective_list_position", "event_interval"])

    gate = json.loads(GATE.read_text(encoding="utf-8"))
    assert gate["dates_with_certified_corrected_top100"] == 0
    security_master_rows = read_csv(SECURITY_MASTER)
    price_symbols = {path.stem for path in (ROOT / ".microcap_index_cache/prices_raw").glob("*.csv")}
    share_symbols = {path.stem for path in (ROOT / ".microcap_index_cache/share_change").glob("*.csv")}
    cached_both = price_symbols & share_symbols
    meta_files = list((ROOT / ".microcap_index_cache/security_meta").glob("*.json"))
    notice_status = Counter()
    for path in meta_files:
        row = json.loads(path.read_text(encoding="utf-8"))
        notice_status[str(row.get("notice_query_status", "missing")).split(":")[0]] += 1
    assert len(security_master_rows) == 5532 and len(cached_both) == 4975
    manifest = {
        "scope": "isolated_600543_metadata_candidate_only",
        "formal_files_modified": False,
        "old_source_sha256": {str(path.relative_to(ROOT)): digest for path, digest in EXPECTED.items()},
        "additional_source_sha256": {
            str(path.relative_to(ROOT)): sha(path) for path in (GATE, SECURITY_MASTER, PROXY_META)
        },
        "issuer_event_evidence": evidence,
        "proposed_metadata": str(candidate_path.relative_to(ROOT)),
        "proposed_metadata_sha256": sha(candidate_path),
        "affected_old_target_rows": len(affected_target),
        "affected_old_effective_rows": len(corrected_effective),
        "affected_old_target_by_interval": dict(Counter(row["event_interval"] for row in affected_target)),
        "affected_old_effective_by_interval": dict(Counter(row["event_interval"] for row in corrected_effective)),
        "affected_csv_sha256": {
            name: sha(HERE / name) for name in ("affected_old_targets.csv", "affected_old_effective.csv")
        },
        "full_pool_replacement_status": "blocked_unproven_historical_security_master_ST_share_PIT",
        "certified_corrected_top100_dates_in_existing_gate": gate["dates_with_certified_corrected_top100"],
        "ranking_input_inventory": {
            "security_master_rows": len(security_master_rows),
            "security_master_source_counts": dict(Counter(row["source"] for row in security_master_rows)),
            "delisted_rows_in_master": sum(bool(row["delist_date"]) for row in security_master_rows),
            "symbols_with_both_cached_raw_prices_and_share_change": len(cached_both),
            "security_metadata_file_count": len(meta_files),
            "metadata_notice_query_status_counts": dict(notice_status),
        },
        "required_before_approval": [
            "Complete historical security master and every possibly ranking-eligible stock with point-in-time ST, share-count, raw-price and trading-status evidence for initial seed and affected rebalance dates.",
            "Independent Top100 reranking on every affected date, including a proven replacement for rank 100; no 99-member shortcut.",
            "Continuous executed-member, corporate-action, fixed-share cash-account, proxy and v2.0/v2.3/v2.5 reconstruction to the common latest completed close.",
            "Exact source and old/new output hashes, dated deltas, zero ST-membership violations and clean second run without a migration override.",
        ],
        "candidate_is_approved_migration_report": False,
    }
    (HERE / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: manifest[k] for k in (
        "affected_old_target_rows", "affected_old_effective_rows", "full_pool_replacement_status",
    )}, ensure_ascii=False))


if __name__ == "__main__":
    main()
