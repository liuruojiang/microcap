"""Independent 2026 issuer ST event versus frozen member and paper ledger data."""

from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter
from pathlib import Path
from pypdf import PdfReader


ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
EFFECTIVE = "2026-04-27"
SYMBOL = "600543"
TARGET = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv"
OLD_EFFECTIVE = ROOT / "research_reports/20260926_l1_repair/full_replay/members_by_rebalance.csv"
META = ROOT / ".microcap_index_cache/security_meta/600543.json"
SOURCE_PDF = HERE / "600543_20260424_NEMZ.pdf"
SOURCE_2023 = HERE / "600543_20230428_stcn.pdf"
SOURCE_2024 = HERE / "600543_20240705_MREZ_mirror.pdf"
ENGINE = ROOT / "research_reports/20261002_l2_full_repair/engine"


def rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def pdf_text(path: Path) -> str:
    assert path.read_bytes()[:4] == b"%PDF"
    return "".join("".join((page.extract_text() or "").split()) for page in PdfReader(path).pages)


def main() -> None:
    assert "2026年4月27日" in pdf_text(SOURCE_PDF)
    assert "2023年5月4日" in pdf_text(SOURCE_2023)
    assert "2024年7月8日" in pdf_text(SOURCE_2024)
    meta = json.loads(META.read_text(encoding="utf-8"))
    intervals = meta["st_intervals"]
    assert not any(i["start"] <= EFFECTIVE and (i.get("end") is None or i["end"] >= EFFECTIVE)
                   for i in intervals), "ST interval already corrected; rerun target lineage"
    target = [r for r in rows(TARGET) if r["symbol"].zfill(6) == SYMBOL
              and EFFECTIVE <= r["rebalance_date"] <= "2026-09-30"]
    effective = {r["rebalance_date"] for r in rows(OLD_EFFECTIVE)
                 if r["symbol"].zfill(6) == SYMBOL and EFFECTIVE <= r["rebalance_date"] <= "2026-09-30"}
    target_dates = {r["rebalance_date"] for r in target}
    assert len(target) == len(target_dates) == 10
    assert target[0]["rebalance_date"] == "2026-04-30"
    old_target = [r for r in rows(TARGET) if r["symbol"].zfill(6) == SYMBOL
                  and "2023-05-04" <= r["rebalance_date"] < "2024-07-08"]
    old_effective = {r["rebalance_date"] for r in rows(OLD_EFFECTIVE)
                     if r["symbol"].zfill(6) == SYMBOL
                     and "2023-05-04" <= r["rebalance_date"] < "2024-07-08"}
    assert len(old_target) == len(old_effective) == 18
    branches = {}
    for branch in ("prefunded", "netted"):
        hpath = ENGINE / f"recent_{branch}_holdings.csv"
        opath = ENGINE / f"recent_{branch}_orders.csv"
        held = [r for r in rows(hpath) if r["symbol"] == SYMBOL and
                EFFECTIVE <= r["date"] <= "2026-09-30" and
                r["state"] == "settled_shares" and float(r["shares"]) > 0]
        orders = [r for r in rows(opath) if r["symbol"] == SYMBOL and
                  EFFECTIVE <= r["execution_date"] <= "2026-09-30"]
        assert len(held) == len({r["date"] for r in held})
        branches[branch] = {
            "hold_days_after_actual_ST_start": len(held),
            "first_held": {k: held[0][k] for k in ("date", "shares", "market_value")} if held else None,
            "last_held": {k: held[-1][k] for k in ("date", "shares", "market_value")} if held else None,
            "orders_after_ST_start": len(orders),
            "booked_buys_after_ST_start": sum(r["side"] == "buy" and r["booked"] == "True" for r in orders),
            "order_side_counts": dict(Counter(r["side"] for r in orders)),
            "first_post_ST_order": {k: orders[0][k] for k in ("signal_date", "execution_date", "side", "booked", "frozen_shares")} if orders else None,
            "file_sha256": {p.name: sha(p) for p in (hpath, opath)},
        }
    out = {
        "status": "FAIL_recent_point_in_time_ST_member_gate",
        "issuer_announcement_date": "2026-04-24",
        "actual_ST_effective_date": EFFECTIVE,
        "issuer_notice_mirror": "https://stockmc.xueqiu.com/202604/600543_20260424_NEMZ.pdf",
        "official_exchange_notice_url": "https://static.sse.com.cn/disclosure/listedinfo/announcement/c/new/2026-04-24/600543_20260424_NEMZ.pdf",
        "independent_SSE_hosted_corroboration_url": "https://static.sse.com.cn/disclosure/bond/announcement/company/c/new/2026-07-30/243808_20260730_KCLJ.pdf",
        "issuer_notice_mirror_sha256": sha(SOURCE_PDF),
        "prior_ST_interval": {
            "start": "2023-05-04", "end_inclusive": "2024-07-07",
            "removed_at_open": "2024-07-08",
            "issuer_entry_notice_source": "https://epaper.stcn.com/att/202304/28/2abb1e5a-46dc-4930-980f-fd25cbab4944.pdf",
            "issuer_entry_notice_sha256": sha(SOURCE_2023),
            "issuer_exit_exchange_original": "https://static.sse.com.cn/disclosure/listedinfo/announcement/c/new/2024-07-05/600543_20240705_MREZ.pdf",
            "issuer_exit_notice_mirror": "https://www.cczq.com/home/uploads/2024/07-05/08-29-460171-1615289614.pdf",
            "issuer_exit_notice_mirror_sha256": sha(SOURCE_2024),
            "bad_targets": [{"date": r["rebalance_date"], "rank": int(r["rank"])} for r in old_target],
            "bad_target_count": len(old_target),
            "bad_effective_count": len(old_effective),
        },
        "cache_intervals": intervals,
        "cache_notice_status": meta["notice_query_status"],
        "source_file_sha256": {p.name: sha(p) for p in (TARGET, OLD_EFFECTIVE, META)},
        "first_bad_target": target[0]["rebalance_date"],
        "bad_targets": [{"date": r["rebalance_date"], "rank": int(r["rank"])} for r in target],
        "bad_target_count": len(target),
        "bad_effective_rebalance_dates": sorted(effective),
        "bad_effective_count": len(effective),
        "target_effective_overlap_count": len(target_dates & effective),
        "in_last_1y_window_ending_2026_09_30": True,
        "recent_paper_account": branches,
        "correction_scope": "Two issuer-dated candidate intervals: 2023-05-04 through 2024-07-07, and 2026-04-27 onward. Rebuild full historical ranked member universe, proxy and downstream version streams in isolated lineage; do not delete rank 600543 to make a 99-stock NAV. No current-snapshot backfill.",
    }
    dest = HERE / "st_600543_2026_audit.json"
    dest.write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: out[k] for k in ("status", "first_bad_target", "bad_target_count", "bad_effective_count", "target_effective_overlap_count")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
