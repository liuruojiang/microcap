"""Snapshot current reviewed code and replay complete archived notices, with no network.

Run only after the parent fixes the final source/evidence snapshot. Never promotes data.
"""
from pathlib import Path
import argparse
import hashlib
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import time


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-name", required=True)
    parser.add_argument("--symbols", nargs="+", help="Explicit affected-symbol incremental replay, never full certification")
    args = parser.parse_args()
    if Path(args.output_name).name != args.output_name:
        raise ValueError("Output name must be a single new directory name")
    root = Path(__file__).resolve().parents[2]
    research = root / "research_reports/20261029_st_history_repair"
    discovery = research / "st_recert_runtime"
    runtime = research / args.output_name
    if runtime.exists():
        raise FileExistsError("Never overwrite an existing offline snapshot")
    template = json.loads((discovery / "source_manifest.json").read_text())
    spec = importlib.util.spec_from_file_location("reviewed_evidence", root / "scripts/st_notice_evidence.py")
    evidence = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(evidence)
    evidence_paths = evidence.evidence_source_files(root)
    paths = {x.replace("\\", "/") for x in template["files"]}
    paths = {x for x in paths if not x.startswith("data/st_notice_evidence/")}
    paths.update(evidence_paths)
    paths.update({"scripts/top100_delivery.py", "scripts/top100_cloud_delivery.py"})
    source_files = {}
    runtime.mkdir()
    for rel in sorted(paths):
        source = discovery / rel if rel.startswith(".microcap_index_cache/") else root / rel
        target = runtime / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        assert sha(target) == sha(source)
        source_files[rel] = sha(target)
    write(runtime / "source_manifest.json", {
        "git_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
        "working_tree_content_authoritative": True, "approved": False, "files": source_files,
        "evidence_fingerprint": evidence.evidence_fingerprint(root),
    })
    frozen = root / "research_reports/20261029_v23_no_overheat_promotion/audit_sync/isolated_lineage_replay/runtime"
    formal = root / ".microcap_index_cache/security_meta"
    oldmeta = frozen / ".microcap_index_cache/security_meta"
    hashes = lambda folder: {p.name: sha(p) for p in folder.glob("*.json")}
    before = {"formal": hashes(formal), "frozen": hashes(oldmeta)}
    out = runtime / "replay"
    out.mkdir()
    (out / "metadata").mkdir()
    write(out / "metadata_before_hashes.json", before)
    expected = json.loads((research / "replay/frozen4975_parallel_diagnostic/exact_hash_manifest.json").read_text())["legacy_nonempty_out_of_scope"]
    if args.symbols:
        if len(set(args.symbols)) != len(args.symbols) or not set(args.symbols) <= set(expected):
            raise ValueError("Incremental symbols must be unique members of the original507")
        expected = sorted(args.symbols)
    notices = {}
    for symbol in expected:
        candidates = [discovery / part / "notices" / (symbol + ".csv")
                      for part in ("discovery507_direct", "discovery507", "canary10")]
        found = [p for p in candidates if p.exists()]
        if len({sha(p) for p in found}) > 1:
            raise ValueError("Conflicting notice archives: " + symbol)
        if found:
            notices[symbol] = found[0]
    write(out / "notice_input_manifest.json", {s: {"path": str(p), "sha256": sha(p)} for s, p in notices.items()})
    import requests
    attempts = []
    cached_requests = []
    raw_inputs = {}
    response_cache = {}
    archives = [(discovery / "canary10", "network.json"), (discovery / "discovery507", "network.jsonl")]
    if (discovery / "discovery507_direct/network.jsonl").exists():
        archives.append((discovery / "discovery507_direct", "network.jsonl"))
    for archive, filename in archives:
        logpath = archive / filename
        entries = (json.loads(logpath.read_text(encoding="utf-8")) if filename.endswith(".json")
                   else [json.loads(line) for line in logpath.read_text(encoding="utf-8").splitlines()])
        raw_inputs[str(logpath)] = sha(logpath)
        for entry in entries:
            if entry.get("status") != 200:
                continue
            rawpath = archive / "raw" / entry["file"]
            if sha(rawpath) != entry["sha256"]:
                raise ValueError("Archived response hash mismatch: " + str(rawpath))
            key = json.dumps([entry["method"].lower(), entry["url"], entry.get("data"), entry.get("params")], sort_keys=True)
            if key in response_cache and response_cache[key][1]["sha256"] != entry["sha256"]:
                raise ValueError("Conflicting successful response for exact request")
            response_cache[key] = (rawpath, entry)
            raw_inputs[str(rawpath)] = entry["sha256"]

    def no_network(*a, **kw):
        attempts.append({"args": str(a[1:3]), "reason": "offline network forbidden"})
        raise RuntimeError("Offline replay prohibits network")

    requests.sessions.Session.request = no_network
    sys.path.insert(0, str(runtime))
    os.chdir(runtime)
    import microcap_top100_mom16_biweekly_live_v2_0 as v
    import pandas as pd
    f = v.freq_mod
    g = f.build_security_meta.__globals__
    assert Path(g["ROOT"]).resolve() == runtime
    g["PRICE_DIR"] = frozen / ".microcap_index_cache/prices_raw"
    g["SHARE_DIR"] = frozen / ".microcap_index_cache/share_change"
    for key in ["SHARED_PRICE_DIR", "SHARED_SHARE_DIR", "SHARED_SECURITY_META_DIR", "SHARED_SECURITY_MASTER_CACHE"]:
        g[key] = None
    g["SECURITY_META_DIR"] = out / "metadata"
    original_fetch = g["fetch_cninfo_st_notices"]

    def cached_request(self, method, url, **kwargs):
        key = json.dumps([method.lower(), url, kwargs.get("data"), kwargs.get("params")], sort_keys=True)
        if key not in response_cache:
            raise RuntimeError("No verified archived response for exact request: " + key)
        rawpath, entry = response_cache[key]
        cached_requests.append({"symbol": kwargs.get("data", {}).get("stock", "").split(",")[0],
                                "request": key, "path": str(rawpath), "sha256": entry["sha256"]})
        response = requests.Response()
        response.status_code = 200
        response.url = url
        response._content = rawpath.read_bytes()
        return response

    def fetch(symbol, start_date, end_date):
        # Execute the real query, identity, count and pagination parser on exact raw requests.
        # A successful CSV (including an empty one) is not independent source proof.
        before_requests = len(cached_requests)
        requests.sessions.Session.request = cached_request
        try:
            rebuilt = original_fetch(symbol, start_date, end_date)
        finally:
            requests.sessions.Session.request = no_network
        used = cached_requests[before_requests:]
        query_groups = {}
        for item in used:
            request = json.loads(item["request"])
            payload = dict(request[2])
            page = int(payload.pop("pageNum"))
            group = json.dumps(payload, sort_keys=True)
            query_groups.setdefault(group, {})[page] = json.loads(Path(item["path"]).read_text(encoding="utf-8"))
        if len(query_groups) != 6:
            raise ValueError("Not all six independent query specifications were replayed")
        for pages in query_groups.values():
            identifiers = [str(row.get("announcementId") or "") for page in pages.values() for row in (page.get("announcements") or [])]
            if len(identifiers) != len(set(identifiers)):
                raise ValueError("Duplicate announcement IDs within paginated query")
        archived = pd.read_csv(notices[symbol], dtype={"announcement_id": str, "issuer_symbol": str}, parse_dates=["notice_date"])
        pd.testing.assert_frame_equal(rebuilt, archived, check_dtype=False)
        return rebuilt

    g["fetch_cninfo_st_notices"] = fetch
    results = []
    started = time.monotonic()
    try:
        for symbol in expected:
            if symbol not in notices:
                results.append({"symbol": symbol, "status": "blocked_missing_complete_notice_archive"})
                continue
            oldpath = oldmeta / (symbol + ".json")
            old = json.loads(oldpath.read_text())
            record = {"symbol": symbol, "old_sha256": sha(oldpath), "old_intervals": old["st_intervals"]}
            try:
                new = f.build_security_meta(symbol)
                record.update(new_intervals=new["st_intervals"], notice_status=new["notice_query_status"],
                              name_status=new["name_history_status"], resolved=new["current_st_history_resolved"],
                              notice_count=new["notice_count"], new_sha256=sha(g["SECURITY_META_DIR"] / (symbol + ".json")))
                if new["notice_query_status"] != "ok" or new["name_history_status"].startswith("error:") or not new["current_st_history_resolved"]:
                    raise ValueError("Incomplete source quality")
                assert f.load_security_meta(symbol) == new
                record.update(status="candidate_built", semantic_intervals_changed=[(x["start"], x["end"]) for x in old["st_intervals"]] != [(x["start"], x["end"]) for x in new["st_intervals"]])
            except Exception as exc:
                record.update(status="blocked", error_type=type(exc).__name__, error=str(exc))
            results.append(record)
    finally:
        after = {"formal": hashes(formal), "frozen": hashes(oldmeta)}
        summary = {"approved": False, "formal_unchanged": before["formal"] == after["formal"],
                   "scope": "affected_symbols_incremental" if args.symbols else "legacy507_archived_notice_subset",
                   "frozen_unchanged": before["frozen"] == after["frozen"],
                   "source_snapshot_unchanged": all(sha(runtime / p) == h for p, h in source_files.items()),
                   "raw_inputs_unchanged": all(sha(Path(p)) == h for p, h in raw_inputs.items()),
                   "expected": len(expected), "complete_notice_archives": len(notices),
                   "network_attempts": len(attempts), "elapsed_seconds": time.monotonic() - started,
                   "records": results}
        write(out / "results.json", summary)
        write(out / "network_attempts.json", attempts)
        write(out / "verified_cached_requests.json", cached_requests)
        write(out / "raw_input_manifest.json", raw_inputs)
        write(out / "metadata_after_hashes.json", after)
        print(json.dumps({k: value for k, value in summary.items() if k != "records"}))
    if attempts or not summary["formal_unchanged"] or not summary["frozen_unchanged"] or not summary["source_snapshot_unchanged"] or not summary["raw_inputs_unchanged"]:
        raise RuntimeError("Offline integrity gate failed")


if __name__ == "__main__":
    main()
