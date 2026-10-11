"""Source-independent arithmetic check for the 2026-09-30 003017 cash event.

No engine account function is imported.  A position quantity is optional:
only a frozen 2026-09-29 close quantity can produce an account cash amount.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from decimal import Decimal
from pathlib import Path


D = Decimal
ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
CNINFO = ROOT / "research_reports/20260926_l1_repair/corporate_actions/cninfo_distribution_events.csv"
SINA = ROOT / "research_reports/20261002_l2_full_repair/data/sina_tail_dividend_full_history.csv"
MEMBERS = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_effective_members.csv"
TARGET = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv"
RAW = ROOT / ".microcap_index_cache/prices_raw/003017.csv"
ENGINE = ROOT / "research_reports/20261002_l2_full_repair/engine"
VERSION = ROOT / "research_reports/20261002_l2_full_repair/version_integration"


def rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


def main() -> None:
    cli = argparse.ArgumentParser()
    cli.add_argument("--record-shares", type=D, help="Actual shares held at 2026-09-29 close, if independently audited")
    args = cli.parse_args()
    cn = [r for r in rows(CNINFO) if r["symbol"] == "003017" and r["除权日"] == "2026-09-30"]
    si = [r for r in rows(SINA) if r["symbol"] == "003017" and r["除权除息日"] == "2026-09-30"]
    assert len(cn) == len(si) == 1
    assert cn[0]["股权登记日"] == si[0]["股权登记日"] == "2026-09-29"
    assert D(cn[0]["派息比例"]) == D(si[0]["派息"]) == 3
    assert not cn[0]["送股比例"] and not cn[0]["转增比例"]
    assert D(si[0]["送股"]) == D(si[0]["转增"]) == 0
    members = rows(MEMBERS)
    asof = max(r["as_of_date"] for r in members)
    held_old_latest = [r for r in members if r["as_of_date"] == asof and r["symbol"] == "003017"]
    assert asof == "2026-09-17" and len(held_old_latest) == 1
    target = [r for r in rows(TARGET) if r["rebalance_date"] == "2026-09-17" and r["symbol"].zfill(6) == "003017"]
    assert len(target) == 1 and target[0]["rank"] == "82"
    raw = {r["date"]: D(r["close_raw"]) for r in rows(RAW)}
    before, after = raw["2026-09-29"], raw["2026-09-30"]
    per_share = D("3") / D(10)
    raw_return = after / before - 1
    economic_return = (after + per_share) / before - 1
    assert abs((economic_return - raw_return) - per_share / before) < D("1e-25")
    shares = args.record_shares
    if shares is not None and (shares < 0 or not shares.is_finite()):
        raise ValueError("record shares must be nonnegative and finite")
    cash_due = None if shares is None else shares * per_share
    branches = {}
    for branch in ("prefunded", "netted"):
        hpath = ENGINE / f"recent_{branch}_holdings.csv"
        rpath = ENGINE / f"recent_{branch}_rights.csv"
        position = [r for r in rows(hpath) if r["date"] == "2026-09-29" and
                    r["symbol"] == "003017" and r["state"] == "settled_shares"]
        events = [r for r in rows(rpath) if r["symbol"] == "003017" and
                  r["record_date"] == "2026-09-29"]
        assert len(position) <= 1
        quantity = D(position[0]["shares"]) if position else D(0)
        phases = {r["phase"]: r for r in events}
        if quantity:
            assert set(phases) == {"record", "ex", "cash_paid"}
            assert phases["record"]["date"] == "2026-09-29"
            assert phases["ex"]["date"] == phases["cash_paid"]["date"] == "2026-09-30"
            for event in phases.values():
                assert D(event["record_shares"]) == quantity
                assert abs(D(event["cash_due"]) - quantity * per_share) < D("1e-25")
                assert D(event["bonus_due"]) == 0
                assert event["source_status"] == "two_source_match"
        else:
            assert not events
        branches[branch] = {
            "held_at_record_close": bool(quantity), "record_shares": str(quantity),
            "gross_cash_due_and_paid": str(quantity * per_share),
            "phase_dates": {phase: ev["date"] for phase, ev in phases.items()},
            "ledger_sha256": {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in (hpath, rpath)},
        }
    strategy_accounts = {}
    for branch in ("prefunded", "netted"):
        for version in ("v2_0", "v2_3", "v2_5"):
            stem = f"recent_{branch}_{version}_strategy"
            hpath, rpath = (VERSION / f"{stem}_{name}.csv" for name in ("holdings", "rights"))
            position = [r for r in rows(hpath) if r["date"] == "2026-09-29" and r["symbol"] == "003017"]
            events = [r for r in rows(rpath) if r["symbol"] == "003017" and r["record_date"] == "2026-09-29"]
            assert len(position) <= 1
            quantity = D(position[0]["shares"]) if position else D(0)
            phases = {r["phase"]: r for r in events}
            assert (set(phases) == {"record", "ex", "cash_paid"}) if quantity else not events
            for ev in events:
                assert D(ev["record_shares"]) == quantity
                assert abs(D(ev["cash_due"]) - quantity*per_share) < D("1e-25")
            strategy_accounts[f"{branch}_{version.replace('_', '.')}"] = {
                "record_shares": str(quantity), "gross_cash_due_and_paid": str(quantity*per_share),
                "phase_dates": {phase: event["date"] for phase,event in phases.items()},
                "ledger_sha256": {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in (hpath,rpath)},
            }
    out = {
        "status": "source_and_actual_recent_record_entitlement_pass",
        "source_sha256": {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in (CNINFO, SINA, MEMBERS, TARGET, RAW)},
        "symbol": "003017", "old_effective_snapshot_asof": asof,
        "old_target_rank": target[0]["rank"],
        "old_effective_rank": held_old_latest[0]["rank"],
        "record_date": "2026-09-29", "ex_date": "2026-09-30",
        "gross_cash_per_10": "3", "gross_cash_per_share": str(per_share),
        "raw_close_record": str(before), "raw_close_ex": str(after),
        "raw_price_return": str(raw_return), "gross_economic_return_if_record_holder": str(economic_return),
        "difference_per_stock": str(economic_return - raw_return),
        "conditional_old_100_equal_weight_difference": str((economic_return - raw_return) / D(100)),
        "record_shares_supplied": None if shares is None else str(shares),
        "gross_cash_receivable": None if cash_due is None else str(cash_due),
        "recent_paper_account": branches,
        "six_signal_gated_strategy_paper_accounts": strategy_accounts,
        "qualification_note": "New T+1 fixed-share ledger's 2026-09-29 settled shares independently verified against all record, ex and cash-paid phases. Old equal-weight amount remains conditional because its share count was not recorded.",
    }
    HERE.mkdir(parents=True, exist_ok=True)
    (HERE / "entitlement_003017.json").write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": out["status"], "per_share": out["gross_cash_per_share"],
                      "economic_minus_raw": out["difference_per_stock"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
