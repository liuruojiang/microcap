"""Read-only input inventory for isolated rebuild preparation; never certifies trading data."""
from pathlib import Path
import concurrent.futures
import csv
from datetime import date
import hashlib
import importlib.metadata
import json
import shutil
import sys

ROOT = Path(__file__).resolve().parents[2]
RUN = ROOT / "research_reports/20261029_st_history_repair"
TARGET = "2026-10-29"
sys.path.insert(0, str(ROOT))


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def date_extent(path, column):
    if path is None or not path.exists():
        return {"present": False, "rows": 0, "first": "", "last": ""}
    with path.open(encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.DictReader(stream))
    values = [date.fromisoformat(row[column][:10]).isoformat() for row in rows]
    return {"present": True, "rows": len(values), "first": min(values) if values else "", "last": max(values) if values else ""}


def main():
    local = ROOT / ".microcap_index_cache"
    shared = next((p / ".microcap_index_cache" for p in ROOT.parent.iterdir()
                   if (p / ".microcap_index_cache").exists() and (p / ".microcap_index_cache") != local), None)
    old = json.loads((RUN / "replay/frozen4975_parallel_diagnostic/scope.json").read_text())["universe"]
    master_path = RUN / "cross_source_official/security_master_combined_candidate.csv"
    with master_path.open(encoding="utf-8-sig", newline="") as stream:
        master = {row["symbol"]: row for row in csv.DictReader(stream)}
    def resolve(folder, symbol):
        for cache in [local, shared]:
            if cache is not None:
                path = cache / folder / (symbol + ".csv")
                if path.exists():
                    return path
        return None
    registry_path = ROOT / "data/st_notice_evidence/registry.json"
    registry_sha = sha(registry_path)
    meta_before = {p.name: sha(p) for p in (local / "security_meta").glob("*.json")}

    def one(symbol):
        row = {"symbol": symbol, "target_date": TARGET, "master_present": symbol in master,
               "master_list_date": master.get(symbol, {}).get("list_date", ""),
               "master_delist_date": master.get(symbol, {}).get("delist_date", "")}
        try:
            for label, folder, column in [("raw", "prices_raw", "date"), ("qfq", "prices_qfq", "date"), ("shares", "share_change", "change_date")]:
                path = resolve(folder, symbol)
                for key, value in date_extent(path, column).items():
                    row[label + "_" + key] = value
                row[label + "_path"] = str(path) if path else ""
            meta = json.loads((local / "security_meta" / (symbol + ".json")).read_text())
            row.update(meta_last_trade_date=meta.get("last_trade_date"),
                       meta_notice_status=meta.get("notice_query_status"),
                       meta_parser_revision=meta.get("st_notice_parser_revision", ""),
                       meta_evidence_sha256=meta.get("st_notice_evidence_sha256", ""),
                       meta_current_registry=meta.get("st_notice_evidence_sha256") == registry_sha,
                       meta_intervals=len(meta.get("st_intervals", [])))
            row["raw_tail_before_target"] = row["raw_last"] < TARGET
            row["raw_tail_classification"] = ("delisted_before_target" if row["master_delist_date"] and row["master_delist_date"] <= TARGET
                                               else "requires_real_tail_or_suspension_evidence" if row["raw_tail_before_target"]
                                               else "date_reached_requires_value_validation")
            row["share_event_date_is_not_fetch_freshness"] = True
            row["error"] = ""
        except Exception as exc:
            row["error"] = type(exc).__name__ + ": " + str(exc)
        return row

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        records = list(executor.map(one, old))
    out = RUN / "resume_audit"
    out.mkdir(exist_ok=True)
    fields = sorted(set().union(*(row.keys() for row in records)))
    with (out / "delivery_old4975_input_coverage.csv").open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(records)
    calendar = {"target": TARGET, "verified": False}
    try:
        from scripts.exchange_calendar import latest_completed_session, SOURCE
        actual = latest_completed_session().isoformat()
        calendar.update(actual_latest_completed=actual, source=SOURCE, verified=actual == TARGET)
    except Exception as exc:
        calendar["error"] = type(exc).__name__ + ": " + str(exc)
    meta_after = {p.name: sha(p) for p in (local / "security_meta").glob("*.json")}
    summary = {"scope": "read_only_rebuild_input_inventory_not_data_certification", "approved": False,
               "target_date": TARGET, "independent_calendar": calendar, "old_universe_count": len(records),
               "master_rows": len(master), "master_sha256": sha(master_path),
               "raw_before_target": sum(row.get("raw_tail_before_target", False) for row in records),
               "raw_tail_requires_evidence": sum(row.get("raw_tail_classification") == "requires_real_tail_or_suspension_evidence" for row in records),
               "qfq_missing": sum(not row.get("qfq_present", False) for row in records),
               "shares_missing": sum(not row.get("shares_present", False) for row in records),
               "metadata_current_registry": sum(row.get("meta_current_registry", False) for row in records),
               "errors": [row for row in records if row.get("error")],
               "formal_metadata_unchanged": meta_before == meta_after, "shared_cache_resolved": str(shared),
               "free_bytes": shutil.disk_usage(ROOT).free,
               "dependencies": {name: importlib.metadata.version(name) for name in ("numpy", "pandas", "requests", "akshare", "pytest")},
               "isolated_rebuild_route": "Reuse immutable old runtime raw/share inputs; new code/master/ST/output under separate research runtime. No links that allow writes into formal or old input caches.",
               "new_candidates": {"a_share_missing_code_scope": 465, "separate_incremental_code": "688287", "not_yet_certified": True}}
    (out / "delivery_input_readiness.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({key: value for key, value in summary.items() if key != "errors"}, ensure_ascii=False))


if __name__ == "__main__":
    main()
