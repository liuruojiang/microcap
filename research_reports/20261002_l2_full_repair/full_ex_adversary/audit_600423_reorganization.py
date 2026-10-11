"""Independent source/account audit plus direct fail-closed mutant for 600423."""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
import sys
from decimal import Decimal, getcontext
from pathlib import Path

import fitz


ROOT = Path(__file__).resolve().parents[3]
getcontext().prec = 50
BASE = ROOT / "research_reports/20261002_l2_full_repair"
ENGINE = BASE / "engine"
OUT = Path(__file__).resolve().parent
PDF = BASE / "data/suspension_ex_evidence/600423_20181212_reorganization.pdf"
CANDIDATES = BASE / "data/event_candidates_2010_20260930.csv"
CNINFO = ROOT / "research_reports/20260926_l1_repair/corporate_actions/cninfo_distribution_events.csv"


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def main() -> None:
    manifest_path = ENGINE / "full_history_result.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert sha(ENGINE / "full_history.py") == manifest["code_sha256"]
    assert sha(PDF) == "7a3f03ea4d7ac18c66d36598efbbb16f6251a4dbfc60ecd39a53bc8bc6412604"
    source = "".join(page.get_text() for page in fitz.open(PDF))
    compact = "".join(source.split())
    for phrase in (
        "本次资本公积金转增股份不向股东分配",
        "公司原股东并未获得转增股份",
        "2018年12月17日",
        "2018年12月18日",
        "2018年12月19日",
        "4.45元/股",
    ):
        assert phrase in compact, phrase
    candidates = [r for r in rows(CANDIDATES)
                  if (r["symbol"].zfill(6), r["ex_date"]) == ("600423", "2018-12-18")]
    assert len(candidates) == 1
    c = candidates[0]
    assert (c["record_date"], c["announcement_date"], c["bonus_per_10"],
            c["cash_per_10_gross"], c["share_credit_date"], c["source_status"],
            c["source_rows"]) == ("2018-12-17", "2018-12-12", "10.0", "0.0", "",
                                   "single_source_only", "cninfo=1;sina=0")
    originals = [r for r in rows(CNINFO)
                 if (r["symbol"].zfill(6), r["除权日"]) == ("600423", "2018-12-18")]
    assert len(originals) == 1
    original_sha = hashlib.sha256(json.dumps(originals[0], ensure_ascii=False,
                                             sort_keys=True).encode("utf-8")).hexdigest()
    assert original_sha == "f754305bf46225fa262ef6c8505a431108e3732452ed0b701b216b83e9ded08f"
    assert originals[0]["分红类型"] == "重整转增"
    assert manifest["branches"]["prefunded"]["first_block"] == {
        "date": "2018-12-17",
        "reason": "non_shareholder_reorg_not_entitlement:2018-12-17:600423",
    }
    assert not any(r["symbol"] == "600423" and r["date"] >= "2018-12-17"
                   for r in rows(ENGINE / "full_history_rights_events.csv"))
    position = [r for r in rows(ENGINE / "full_history_daily_positions.csv")
                if (r["branch"], r["date"], r["state"], r["symbol"]) ==
                ("prefunded", "2018-12-14", "settled_shares", "600423")]
    assert len(position) == 1
    held = Decimal(position[0]["shares"])
    assert held > 0
    # A naive vendor-row interpretation doubles the holder's share count.
    # The issuer explicitly allocates those new shares to creditors/investors.
    phantom_bonus = held * Decimal(c["bonus_per_10"]) / Decimal(10)
    assert phantom_bonus == held
    assert Decimal(position[0]["valuation_price"]) == Decimal("4.83")
    # Mutate only the missing credit date: the special event must remain blocked.
    spec = importlib.util.spec_from_file_location("full_history_600423_mutant", ENGINE / "full_history.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(ENGINE))
    spec.loader.exec_module(module)
    mutant = dict(c, share_credit_date="2018-12-19")
    try:
        module.verified_event_contract(mutant, held, {}, set(), set(), {})
    except ValueError as exc:
        assert str(exc) == "non_shareholder_reorg_not_entitlement:2018-12-17:600423", str(exc)
    else:
        raise AssertionError("credit-date mutant was accepted as a shareholder bonus")
    result = {
        "status": "PASS_SPECIAL_RESTRUCTURING_REJECTED",
        "manifest_sha256": sha(manifest_path),
        "code_sha256": sha(ENGINE / "full_history.py"),
        "issuer_pdf_sha256": sha(PDF),
        "cninfo_original_row_sha256": original_sha,
        "issuer_entitlement": "zero new shares and zero cash for existing shareholders",
        "issuer_special_reference_price": "4.45",
        "candidate_bonus_per_10": c["bonus_per_10"],
        "last_account_day": "2018-12-14",
        "last_settled_account_shares": str(held),
        "naive_phantom_bonus_shares": str(phantom_bonus),
        "mutant_credit_date": mutant["share_credit_date"],
        "mutant_result": "rejected_non_shareholder_reorg_not_entitlement",
        "first_block": manifest["branches"]["prefunded"]["first_block"],
        "limitation": "This proves rejection, not a valued continuation through the restructuring.",
    }
    (OUT / "600423_reorganization_adversarial_audit.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
