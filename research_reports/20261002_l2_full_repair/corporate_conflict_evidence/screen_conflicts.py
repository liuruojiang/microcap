"""Independent frozen overlap screen for normalized distribution conflicts."""

from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter
from decimal import Decimal
from pathlib import Path

from pypdf import PdfReader


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
DATA = ROOT / "research_reports/20261002_l2_full_repair/data"
ENGINE = ROOT / "research_reports/20261002_l2_full_repair/engine"
CLASS = DATA / "event_conflict_classification.csv"
EVENTS = DATA / "event_candidates_2010_20260930.csv"
POSITIONS = ENGINE / "full_history_daily_positions.csv"
RESULT = ENGINE / "full_history_result.json"
PDF_300062 = DATA / "suspension_ex_evidence/300062_20130711_issuer_dividend.pdf"
PDF_300120 = DATA / "suspension_ex_evidence/300120_20140604_issuer_dividend.pdf"


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def pdf_text(path: Path) -> str:
    return "".join("".join((page.extract_text() or "").split()) for page in PdfReader(path).pages)


def status(branch: str, row: dict[str, str], held: set[tuple[str, str, str]], info: dict) -> str:
    record = row["record_date"]
    symbol = row["symbol"]
    stop = info["last_settled_date"]
    block = info["first_block"]
    if record <= stop:
        return "held_at_record_before_stop" if (branch, record, symbol) in held else "not_held_at_record_before_stop"
    if record == block["date"] and f":{row['ex_date']}:{symbol}" in block["reason"]:
        return "blocked_at_record_due_to_this_conflict"
    return "unreached_after_stop"


def main() -> None:
    assert sha(PDF_300062) == "8ba3fd6885ee620d7eb614f2613171b2791921e2d29685588bafc58ba92fa95d"
    assert sha(PDF_300120) == "31aafbfcc0499c3e9d26260b2ce07713b651a5eef722fd907a3fa93d9bea9a37"
    s62, s120 = pdf_text(PDF_300062), pdf_text(PDF_300120)
    for term in ("1.002524", "2013年7月16日", "2013年7月17日"):
        assert term in s62, term
    for term in ("2.003731", "1.001865", "2014年6月10日", "2014年6月11日"):
        assert term in s120, term
    assert "2014年6月11日直接记入股东证券账户" in s120
    assert "2014年6月11日通过股东托管证券公司" in s120

    candidates = {(x["symbol"], x["ex_date"]): x for x in read_csv(EVENTS)}
    conflicts = read_csv(CLASS)
    assert len(conflicts) == 51
    input_hash_at_start = {str(path.relative_to(ROOT)): sha(path) for path in (CLASS, EVENTS, POSITIONS, RESULT, PDF_300062, PDF_300120)}
    held = {
        (x["branch"], x["date"], x["symbol"])
        for x in read_csv(POSITIONS)
        if x["state"] == "settled_shares" and Decimal(x["shares"]) > 0
    }
    branch_info = json.loads(RESULT.read_text(encoding="utf-8"))["branches"]
    out = []
    for conflict in conflicts:
        key = (conflict["symbol"], conflict["ex_date"])
        candidate = candidates[key]
        cninfo_rows = json.loads(conflict["cninfo_rows"])
        sina_rows = json.loads(conflict["sina_rows"])
        out.append({
            "symbol": key[0],
            "record_date": candidate["record_date"],
            "ex_date": key[1],
            "conflict_kind": conflict["kind"],
            "old_held_at_record_snapshot": conflict["old_held_at_record_snapshot"],
            "old_held_at_ex_snapshot": conflict["old_held_at_ex_snapshot"],
            "prefunded_status": status("prefunded", candidate, held, branch_info["prefunded"]),
            "netted_status": status("netted", candidate, held, branch_info["netted"]),
            "cninfo_bonus_per_10": "|".join(str(x["bonus"]) for x in cninfo_rows),
            "cninfo_cash_per_10_gross": "|".join(str(x["cash"]) for x in cninfo_rows),
            "sina_bonus_per_10": "|".join(str(x["bonus"]) for x in sina_rows),
            "sina_cash_per_10": "|".join(str(x["cash"]) for x in sina_rows),
            "issuer_exact_ratio_verified_here": key in {("300062", "2013-07-17"), ("300120", "2014-06-11")},
        })
    assert [x["symbol"] for x in out if x["old_held_at_record_snapshot"] == "True" or x["old_held_at_ex_snapshot"] == "True"] == ["002034", "300062", "300120"]
    input_hash_at_end = {str(path.relative_to(ROOT)): sha(path) for path in (CLASS, EVENTS, POSITIONS, RESULT, PDF_300062, PDF_300120)}
    if input_hash_at_end != input_hash_at_start:
        raise RuntimeError("Source changed during screen; rerun after account output freezes")
    with (HERE / "conflict_path_screen.csv").open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(out[0]))
        writer.writeheader()
        writer.writerows(out)
    manifest = {
        "scope": "old effective member snapshots and stopped diagnostic fixed-share accounts only",
        "conflicts": len(out),
        "kinds": dict(Counter(x["conflict_kind"] for x in out)),
        "old_snapshot_overlap": sum(x["old_held_at_record_snapshot"] == "True" or x["old_held_at_ex_snapshot"] == "True" for x in out),
        "old_snapshot_overlap_keys": [[x["symbol"], x["ex_date"]] for x in out if x["old_held_at_record_snapshot"] == "True" or x["old_held_at_ex_snapshot"] == "True"],
        "prefunded_status_counts": dict(Counter(x["prefunded_status"] for x in out)),
        "netted_status_counts": dict(Counter(x["netted_status"] for x in out)),
        "branch_last_settled": {k: v["last_settled_date"] for k, v in branch_info.items()},
        "branch_first_block": {k: v["first_block"] for k, v in branch_info.items()},
        "input_sha256": input_hash_at_start,
        "screen_sha256": sha(HERE / "conflict_path_screen.csv"),
        "limits": [
            "Old member snapshot overlap is a lower-bound trigger screen, not exact post-repair fixed-share exposure.",
            "A branch's future conflict status remains unknown after its first block; unreached does not mean not held.",
            "Issuer verification here covers only 300062 and 300120; existing 002034 evidence is in data/suspension_ex_evidence/.",
        ],
    }
    (HERE / "conflict_path_screen_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: manifest[k] for k in ("conflicts", "kinds", "old_snapshot_overlap", "prefunded_status_counts", "netted_status_counts")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
