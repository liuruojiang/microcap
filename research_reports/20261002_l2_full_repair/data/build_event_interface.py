"""Normalize existing public-source rights data for the isolated L2 paper ledger.

This is an event *candidate* interface, not a complete corporate-action feed.
It never infers zero events from a missing row or uses adjusted prices as cash.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
from collections import Counter, defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "research_reports/20260926_l1_repair/corporate_actions"
OUT = Path(__file__).resolve().parent
CUTOFF = "2026-09-30"
INPUTS = {
    "cninfo": SRC / "cninfo_distribution_events.csv",
    "cninfo_initial": SRC / "cninfo_distribution_events_initial_seed_extra.csv",
    "sina_held": SRC / "sina_held_2010_2014_distribution_and_rights.csv",
    "sina_recovered": SRC / "sina_recovered_delisted_distribution_and_rights.csv",
    "sina_rights": SRC / "sina_all_held_rights_2010_2014_distribution_and_rights.csv",
    "sina_missing_rights": OUT / "sina_missing_old_member_rights.csv",
    "cninfo_tail": OUT / "latest_old_members_cninfo_full_history.csv",
    "sina_tail": OUT / "sina_tail_dividend_full_history.csv",
    "cninfo_manifest": SRC / "cninfo_source_manifest.json",
    "cninfo_initial_manifest": SRC / "cninfo_source_manifest_initial_seed_extra.json",
    "sina_held_manifest": SRC / "sina_held_2010_2014_source_manifest.json",
    "sina_recovered_manifest": SRC / "sina_recovered_delisted_source_manifest.json",
    "sina_rights_manifest": SRC / "sina_all_held_rights_2010_2014_source_manifest.json",
    "sina_missing_rights_manifest": OUT / "sina_missing_old_member_rights_manifest.json",
    "cninfo_tail_manifest": OUT / "latest_old_members_cninfo_manifest.json",
    "sina_tail_manifest": OUT / "sina_tail_dividend_manifest.json",
    "old_targets": ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv",
    "initial_seed": SRC / "initial_2010_01_04_executed_seed.csv",
}


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def number(value: str | None) -> float:
    try:
        result = float(value or 0)
        return result if math.isfinite(result) else 0.0
    except (TypeError, ValueError):
        return 0.0


def date(value: str | None) -> str:
    x = (value or "").strip().replace("/", "-")
    return x[:10] if len(x) >= 10 else ""


def active_date(x: str) -> bool:
    return "2010-01-01" <= x <= CUTOFF


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    targets = read_csv(INPUTS["old_targets"])
    target_syms = {r["symbol"].zfill(6) for r in targets}
    seed_rows = read_csv(INPUTS["initial_seed"])
    seed_syms = {r["symbol"].zfill(6) for r in seed_rows}
    universe = target_syms | seed_syms

    cninfo: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for name in ("cninfo", "cninfo_initial", "cninfo_tail"):
        for r in read_csv(INPUTS[name]):
            symbol = r["symbol"].zfill(6)
            ex = date(r.get("除权日"))
            if name == "cninfo_tail" and ex < "2026-09-25":
                continue
            bonus = number(r.get("送股比例")) + number(r.get("转增比例"))
            cash = number(r.get("派息比例"))
            if symbol in universe and active_date(ex) and (bonus > 0 or cash > 0):
                cninfo[symbol, ex].append({
                    "source": name,
                    "record_date": date(r.get("股权登记日")),
                    "announcement_date": date(r.get("实施方案公告日期")),
                    "cash_pay_date": date(r.get("派息日")),
                    "share_credit_date": date(r.get("股份到账日")),
                    "bonus_per_10": bonus,
                    "cash_per_10": cash,
                })

    sina_div: dict[tuple[str, str], list[dict]] = defaultdict(list)
    sina_rights: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for name in ("sina_held", "sina_recovered", "sina_rights", "sina_missing_rights", "sina_tail"):
        for r in read_csv(INPUTS[name]):
            symbol = r["symbol"].zfill(6)
            if symbol not in universe:
                continue
            if r.get("indicator") == "分红":
                ex = date(r.get("除权除息日"))
                if name == "sina_tail" and ex < "2026-09-25":
                    continue
                bonus = number(r.get("送股")) + number(r.get("转增"))
                cash = number(r.get("派息"))
                if active_date(ex) and (bonus > 0 or cash > 0):
                    sina_div[symbol, ex].append({
                        "source": name,
                        "record_date": date(r.get("股权登记日")),
                        "announcement_date": date(r.get("公告日期")),
                        "share_credit_date": date(r.get("红股上市日")),
                        "bonus_per_10": bonus,
                        "cash_per_10": cash,
                    })
            elif r.get("indicator") == "配股":
                ex = date(r.get("除权日"))
                ratio = number(r.get("配股方案"))
                if active_date(ex) and ratio > 0:
                    sina_rights[symbol, ex].append({
                        "source": name,
                        "record_date": date(r.get("股权登记日")),
                        "announcement_date": date(r.get("公告日期")),
                        "payment_start": date(r.get("缴款起始日")),
                        "payment_end": date(r.get("缴款终止日")),
                        "share_credit_date": date(r.get("配股上市日")),
                        "rights_per_10": ratio,
                        "subscription_price": number(r.get("配股价格")),
                    })

    rows: list[dict] = []
    for key in sorted(set(cninfo) | set(sina_div)):
        c, s = cninfo.get(key, []), sina_div.get(key, [])
        primary = (c or s)[0]
        identity = lambda e: (e["record_date"], e["bonus_per_10"], e["cash_per_10"])
        conflict = len({identity(e) for e in c}) > 1 or len({identity(e) for e in s}) > 1
        if c and s:
            other = s[0]
            match = (abs(primary["bonus_per_10"] - other["bonus_per_10"]) < 1e-8
                     and abs(primary["cash_per_10"] - other["cash_per_10"]) < 1e-8
                     and primary["record_date"] == other["record_date"])
            status = "two_source_match" if match and not conflict else "source_conflict_or_duplicate"
        else:
            status = "single_source_only" if not conflict else "source_conflict_or_duplicate"
        rows.append({
            "symbol": key[0], "ex_date": key[1], "event_type": "dividend_or_bonus",
            "record_date": primary["record_date"],
            "announcement_date": primary["announcement_date"],
            "cash_pay_date": primary.get("cash_pay_date", ""),
            "share_credit_date": primary.get("share_credit_date", ""),
            "bonus_per_10": primary["bonus_per_10"], "cash_per_10_gross": primary["cash_per_10"],
            "rights_per_10": "", "subscription_price": "", "payment_start": "", "payment_end": "",
            "source": primary["source"], "source_status": status,
            "source_rows": f"cninfo={len(c)};sina={len(s)}",
        })
    for key in sorted(sina_rights):
        events = sina_rights[key]
        unique = {(e["record_date"], e["rights_per_10"], e["subscription_price"]) for e in events}
        primary = events[0]
        rows.append({
            "symbol": key[0], "ex_date": key[1], "event_type": "rights_issue",
            "record_date": primary["record_date"],
            "announcement_date": primary["announcement_date"],
            "cash_pay_date": "", "share_credit_date": primary["share_credit_date"],
            "bonus_per_10": "", "cash_per_10_gross": "",
            "rights_per_10": primary["rights_per_10"],
            "subscription_price": primary["subscription_price"],
            "payment_start": primary["payment_start"], "payment_end": primary["payment_end"],
            "source": primary["source"],
            "source_status": "single_source_only" if len(unique) == 1 else "source_conflict_or_duplicate",
            "source_rows": f"sina={len(events)}",
        })
    rows.sort(key=lambda r: (r["ex_date"], r["symbol"], r["event_type"]))
    columns = list(rows[0])
    output = OUT / "event_candidates_2010_20260930.csv"
    with output.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=columns)
        w.writeheader()
        w.writerows(rows)

    cninfo_status = {
        r["symbol"].zfill(6): r["status"]
        for r in json.loads(INPUTS["cninfo_manifest"].read_text(encoding="utf-8"))["per_symbol"]
    }
    cninfo_status.update({
        r["symbol"].zfill(6): r["status"]
        for r in json.loads(INPUTS["cninfo_initial_manifest"].read_text(encoding="utf-8"))["per_symbol"]
    })
    sina_job_status: dict[tuple[str, str], str] = {}
    for name in ("sina_held_manifest", "sina_recovered_manifest", "sina_rights_manifest", "sina_missing_rights_manifest"):
        manifest = json.loads(INPUTS[name].read_text(encoding="utf-8"))
        for r in manifest.get("per_symbol_indicator", []):
            if r.get("status") == "ok":
                sina_job_status[r["symbol"].zfill(6), r["indicator"]] = "ok"
    for r in json.loads(INPUTS["sina_tail_manifest"].read_text(encoding="utf-8"))["statuses"]:
        if r.get("status") == "ok":
            sina_job_status[r["symbol"].zfill(6), r["indicator"]] = "ok"
    coverage = []
    for symbol in sorted(universe):
        coverage.append({
            "symbol": symbol,
            "in_old_target": symbol in target_syms,
            "in_initial_seed": symbol in seed_syms,
            "has_raw_file": (ROOT / ".microcap_index_cache/prices_raw" / f"{symbol}.csv").exists(),
            "cninfo_dividend_job": cninfo_status.get(symbol, "not_requested_or_extra"),
            "sina_dividend_job": sina_job_status.get((symbol, "分红"), "not_requested"),
            "sina_rights_job": sina_job_status.get((symbol, "配股"), "not_requested"),
        })
    coverage_file = OUT / "old_member_source_coverage.csv"
    with coverage_file.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(coverage[0]))
        w.writeheader()
        w.writerows(coverage)

    summary = {
        "cutoff": CUTOFF,
        "scope": "old target and initial seed union; diagnostic only",
        "universe_symbols": len(universe),
        "target_symbols": len(target_syms),
        "initial_seed_symbols": len(seed_syms),
        "source_coverage": {
            "raw_file": sum(r["has_raw_file"] for r in coverage),
            "cninfo_ok": sum(r["cninfo_dividend_job"] == "ok" for r in coverage),
            "sina_dividend_ok": sum(r["sina_dividend_job"] == "ok" for r in coverage),
            "sina_rights_ok": sum(r["sina_rights_job"] == "ok" for r in coverage),
            "without_dividend_job": [r["symbol"] for r in coverage
                                     if r["cninfo_dividend_job"] != "ok" and r["sina_dividend_job"] != "ok"],
            "without_rights_job_count": sum(r["sina_rights_job"] != "ok" for r in coverage),
        },
        "events": dict(Counter(f"{r['event_type']}:{r['source_status']}" for r in rows)),
        "first_candidate": rows[0],
        "first_dividend_or_bonus": next(r for r in rows if r["event_type"] == "dividend_or_bonus"),
        "first_rights_issue": next(r for r in rows if r["event_type"] == "rights_issue"),
        "input_sha256": {k: sha(v) for k, v in INPUTS.items()},
        "output_sha256": {output.name: sha(output), coverage_file.name: sha(coverage_file)},
    }
    (OUT / "event_interface_manifest.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({k: summary[k] for k in ("universe_symbols", "source_coverage", "events", "first_candidate")}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
