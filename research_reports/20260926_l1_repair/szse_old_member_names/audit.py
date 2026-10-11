"""Research-only audit of dated SZSE short names for two frozen old Top100 lists."""

from __future__ import annotations

import csv
import hashlib
import json
import re
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
INPUTS = {
    "derived_szse_names": (ROOT / ".microcap_index_cache/sz_name_change_short.csv", "856e18b71a21c14eba80b7111316ad5a99a30660d007248b95773affb8caca93"),
    "old_initial": (HERE.parent / "data_recovery/initial_formal_seed_caps.csv", "e667587245d9944950d858da78763ba6c4d06accdb083b799052828429acd9f4"),
    "old_targets": (ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv", "04a851bb6e7e67f181607c499a0ca3264443b6e3b655817ac0933532517f7517"),
    "official_proven_st": (HERE.parent / "st_timing/certified_st_target_lower_bound.csv", "7c0f0cab78c93e70cf3c262c258b53e9740ac344a3fe66d7949927480e568da0"),
    "official_initial_candidate_names": (HERE.parent / "st_timing/initial_2010_st/two_date_st_status_evidence.csv", "8e840d7037e4b39b27cc0cf7f037d63fcbab856c7ed09e818f767235a905b7b3"),
}


def rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as source:
        return list(csv.DictReader(source))


def norm(name: str) -> str:
    return re.sub(r"\s+", "", unicodedata.normalize("NFKC", name)).upper()


def is_st(name: str) -> bool:
    return bool(re.match(r"^(?:[SG])?\*?(?:ST|PT)", norm(name)))


def main() -> None:
    hashes = {}
    for label, (path, expected) in INPUTS.items():
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        if actual != expected:
            raise RuntimeError(f"Frozen input changed: {label}: {actual}")
        hashes[label] = actual

    changes = rows(INPUTS["derived_szse_names"][0])
    assert changes and set(changes[0]) == {"change_date", "symbol", "old_name", "new_name"}
    assert all(row["change_date"] and row["symbol"] and row["old_name"] and row["new_name"] for row in changes)
    assert len({(row["symbol"], row["change_date"]) for row in changes}) == len(changes)
    by_symbol = defaultdict(list)
    for change in changes:
        by_symbol[change["symbol"]].append(change)
    for history in by_symbol.values():
        history.sort(key=lambda row: row["change_date"])

    initial = rows(INPUTS["old_initial"][0])
    target = [row for row in rows(INPUTS["old_targets"][0]) if row["rebalance_date"] == "2010-01-14"]
    assert len(initial) == len(target) == 100
    official_st = {(row["target_date"], row["symbol"].zfill(6)) for row in rows(INPUTS["official_proven_st"][0])}

    output = []
    for date, cohort in (("2010-01-04", initial), ("2010-01-14", target)):
        for member in cohort:
            symbol = member["symbol"].zfill(6)
            if symbol.startswith("6"):
                continue
            history = by_symbol.get(symbol, [])
            prior = [row for row in history if row["change_date"] <= date]
            after = [row for row in history if row["change_date"] > date]
            last = prior[-1] if prior else None
            next_row = after[0] if after else None
            if last is not None:
                name = last["new_name"]
                provenance = "last_dated_change_new_name"
            elif next_row is not None:
                name = next_row["old_name"]
                provenance = "next_change_old_name_retrospective_only"
            else:
                name = ""
                provenance = "no_change_record"
            chain_break = bool(last and next_row and norm(last["new_name"]) != norm(next_row["old_name"]))
            if not name:
                issue = "no_initial_or_change_state"
            elif last is None:
                issue = "pre_first_event_start_state_not_directly_dated"
            elif chain_break:
                issue = "next_old_name_conflicts_last_new_name_possible_missing_event"
            else:
                issue = "no_exhaustiveness_or_raw_xlsx_proof"
            official = (date, symbol) in official_st
            output.append({
                "date": date,
                "rank": member["rank"],
                "symbol": symbol,
                "derived_short_name": name,
                "derived_st_prefix": "" if not name else str(is_st(name)),
                "name_provenance": provenance,
                "last_change_date": last["change_date"] if last else "",
                "next_change_date": next_row["change_date"] if next_row else "",
                "next_old_name": next_row["old_name"] if next_row else "",
                "chain_break": str(chain_break),
                "official_ST_independently_proven": str(official),
                "official_vs_derived_conflict": str(official and bool(name) and not is_st(name)),
                "certification_block": issue,
            })

    path = HERE / "old_szse_two_date_names.csv"
    with path.open("w", encoding="utf-8-sig", newline="") as target_file:
        writer = csv.DictWriter(target_file, fieldnames=list(output[0]))
        writer.writeheader()
        writer.writerows(output)
    cross = []
    for evidence in rows(INPUTS["official_initial_candidate_names"][0]):
        symbol = evidence["symbol"]
        if symbol.startswith("6"):
            continue
        date = evidence["target_date"]
        prior = [row for row in by_symbol[symbol] if row["change_date"] <= date]
        assert prior, (symbol, date)
        derived = prior[-1]["new_name"]
        official_name = evidence["historical_short_name"]
        cross.append({
            "date": date,
            "symbol": symbol,
            "derived_short_name": derived,
            "official_issuer_short_name": official_name,
            "official_status": evidence["status"],
            "name_match_normalized": str(norm(derived) == norm(official_name)),
            "status_match": str(is_st(derived) == (evidence["status"] == "proven_ST")),
            "official_pre_notice_url": evidence["pre_source_url"],
            "official_post_notice_url": evidence["post_source_url"],
        })
    assert len(cross) == 20
    with (HERE / "known_ST_crosscheck.csv").open("w", encoding="utf-8-sig", newline="") as target_file:
        writer = csv.DictWriter(target_file, fieldnames=list(cross[0]))
        writer.writeheader()
        writer.writerows(cross)
    summary = {
        "scope": "frozen_old_Top100_SZSE_members_two_dates_only",
        "input_sha256": hashes,
        "derived_cache_rows": len(changes),
        "derived_cache_symbols": len(by_symbol),
        "derived_cache_first_change_date": min(row["change_date"] for row in changes),
        "derived_cache_last_change_date": max(row["change_date"] for row in changes),
        "old_SZSE_member_rows": len(output),
        "old_SZSE_member_symbols": len({row["symbol"] for row in output}),
        "date_counts": dict(Counter(row["date"] for row in output)),
        "provenance_counts": dict(Counter(row["name_provenance"] for row in output)),
        "derived_ST_prefix_rows": sum(row["derived_st_prefix"] == "True" for row in output),
        "chain_break_rows": sum(row["chain_break"] == "True" for row in output),
        "official_ST_rows_in_scope": sum(row["official_ST_independently_proven"] == "True" for row in output),
        "official_vs_derived_conflict_rows": sum(row["official_vs_derived_conflict"] == "True" for row in output),
        "separate_official_candidate_crosscheck_rows": len(cross),
        "separate_official_candidate_name_conflicts": sum(row["name_match_normalized"] == "False" for row in cross),
        "separate_official_candidate_ST_conflicts": sum(row["status_match"] == "False" for row in cross),
        "certified_ST_status_from_derived_table_alone": 0,
        "limits": [
            "Only the derived CSV is available locally; the source XLSX bytes and headers could not be read back in this run.",
            "Before the first dated change, old_name is retrospective and lacks a direct starting-state date.",
            "No-change rows and continuity do not prove an exhaustive list of every ST entry and exit.",
            "Current member names are never used as historical names.",
        ],
    }
    (HERE / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
