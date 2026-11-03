"""One-use source-only CNInfo handoff. Never installs data in production.

The native pinned refresh/validator own all source parsing and publication.
This adapter only transports their exact verified dependency closure and runs
the unchanged refresh serially in a fresh isolated cloud directory.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import importlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys
import time
import zipfile

SOURCE = "b5ac304790aa8644407de4c0712920f2c8ea2fb0"
PREFIX = ".microcap_index_cache/share_sources/"
CACHE = ".microcap_index_cache/share_change/"
CODE = ("scripts/share_change_source.py", "scripts/stock_return_basis_store.py",
        "scripts/stock_return_basis.py")
SCHEMA = "share_source_only_handoff_20261103_v1"


def require(value, message):
    if not value:
        raise RuntimeError(message)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def encoded(value):
    return json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False).encode("utf8")


def now():
    return datetime.now(timezone.utc).isoformat()


def safe(name):
    p = PurePosixPath(name)
    require(not p.is_absolute() and ".." not in p.parts and "\\" not in name and ":" not in name,
            "Unsafe transport path: " + name)
    return p.as_posix()


def native(source_root):
    source_root = source_root.resolve()
    bindings = {}
    for name in CODE:
        raw = subprocess.check_output(["git", "-C", str(source_root), "show", SOURCE + ":" + name], timeout=60)
        require((source_root / name).read_bytes().replace(b"\r\n", b"\n") == raw,
                "Native execution bytes differ from approved source: " + name)
        bindings[name] = digest(raw)
    sys.path.insert(0, str(source_root))
    module = importlib.import_module("scripts.share_change_source")
    require(Path(module.__file__).resolve() == source_root / CODE[0], "Unexpected imported native module")
    return module, bindings


def closure(module, root, symbols):
    """Only files actually bound by native verifier, plus original side receipts."""
    result, states = {}, {}
    for symbol in symbols:
        require(not (root / PREFIX / "locks" / (symbol + ".lock")).exists(), "Active share writer: " + symbol)
        require(not (root / PREFIX / "incomplete" / (symbol + ".json")).exists(), "Native incomplete publication: " + symbol)
        state = json.loads((root / PREFIX / (symbol + ".json")).read_bytes())
        checked = module.verify_share_cache(root=root, symbol=symbol, as_of=state["query_end"])
        states[symbol] = dict(query_end=state["query_end"], candidate_sha256=checked["candidate_sha256"])
        for name, expected in checked["source_files"].items():
            path = PREFIX + safe(name)
            raw = (root / path).read_bytes()
            require(digest(raw) == expected, "Native bound source changed: " + path)
            result[path] = raw
            if name.startswith("responses/") and not name.endswith(".receipt.json"):
                side = path[:-5] + ".receipt.json"
                body = (root / side).read_bytes()
                receipt = json.loads(body)
                require(receipt.get("response_path") == name and receipt.get("response_sha256") == expected
                        and receipt.get("status") == 200 and receipt.get("symbol") == symbol,
                        "Original response receipt mismatch: " + side)
                result[side] = body
        path = CACHE + symbol + ".csv"
        raw = (root / path).read_bytes()
        require(digest(raw) == checked["candidate_sha256"], "Share CSV changed: " + symbol)
        result[path] = raw
    # The native current/previous states reproduce each source chain; preserve
    # and separately bind the provider-response sidecar written before state
    # publication, rather than treating an unvalidated sidecar as authority.
    for name, raw in tuple(result.items()):
        relative = name.removeprefix(PREFIX)
        if relative.startswith("states/") or relative in (s + ".json" for s in symbols):
            state = json.loads(raw)
            if state.get("kind") == "approved_baseline":
                continue
            side = PREFIX + state["response_path"][:-5] + ".receipt.json"
            receipt = json.loads(result[side])
            keys = ("schema_version", "symbol", "url", "params", "status", "retrieved_at_utc",
                    "response_sha256", "response_path", "query_end")
            require(all(k in receipt and receipt[k] == state[k] for k in keys)
                    and receipt.get("transport") == state.get("transport"),
                    "Original sidecar/state binding differs: " + side)
    require(all((root / name).read_bytes() == raw for name, raw in result.items()), "Share closure changed during capture")
    return result, states


def archive_write(path, manifest, payloads):
    require(not path.exists(), "Never overwrite a prior handoff archive")
    path.parent.mkdir(parents=True, exist_ok=True)
    manifest["files"] = {safe(name): digest(raw) for name, raw in sorted(payloads.items())}
    with zipfile.ZipFile(path, "x", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
        archive.writestr("handoff_manifest.json", encoded(manifest))
        for name, raw in sorted(payloads.items()):
            archive.writestr(name, raw)
    return dict(path=str(path), sha256=digest(path.read_bytes()), bytes=path.stat().st_size,
                manifest_sha256=digest(encoded(manifest)), files=len(payloads), status=manifest["status"])


def archive_read(path, expected):
    raw = path.read_bytes()
    require(digest(raw) == expected, "Transport ZIP SHA mismatch")
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        names = archive.namelist()
        require(len(names) == len(set(names)) and sum(x.file_size for x in archive.infolist()) < 512 * 1024 * 1024,
                "Duplicate or unbounded source archive")
        require(all(safe(n) == n for n in names), "Unsafe ZIP names")
        manifest = json.loads(archive.read("handoff_manifest.json"))
        require(manifest.get("schema") == SCHEMA and manifest.get("source_sha") == SOURCE,
                "Unexpected source handoff identity")
        require(set(names) == set(manifest["files"]) | {"handoff_manifest.json"}, "ZIP inventory differs from manifest")
        payload = {name: archive.read(name) for name in manifest["files"]}
        require(all(digest(body) == manifest["files"][name] for name, body in payload.items()), "Payload SHA mismatch")
        require(all(name.startswith(("root/" + PREFIX, "root/" + CACHE, "audit/")) for name in payload),
                "Payload is outside the share-source-only scope")
    return manifest, payload


def prepare(a):
    module, code = native(a.source_root)
    inventory = a.symbols_json.read_bytes()
    data = json.loads(inventory)
    symbols = data if isinstance(data, list) else data["symbols"]
    require(symbols == sorted(set(symbols)) and len(symbols) == 102
            and all(re.fullmatch(r"[0-9]{6}", s) for s in symbols), "Expected exact bound 102-symbol union")
    files, states = closure(module, a.market_root.resolve(), symbols)
    require(all(s["query_end"] <= a.target for s in states.values()), "Source state is newer than requested handoff")
    payloads = {"root/" + name: body for name, body in files.items()}
    payloads["audit/symbols_binding.json"] = encoded(dict(symbols=symbols,
        original_inventory_sha256=digest(inventory), local_inventory_path=str(a.symbols_json)))
    stop = a.market_root / PREFIX / "RATE_LIMIT_STOP.json"
    require(stop.exists(), "Expected original local failure evidence is missing")
    stop_raw = stop.read_bytes()
    failure = json.loads(stop_raw)
    require(failure.get("status") in (403, 429), "Unexpected local stop evidence")
    body_path = a.market_root / PREFIX / safe(failure["response_path"])
    receipt_path = a.market_root / PREFIX / safe(failure["response_path"][:-5] + ".receipt.json")
    require(digest(body_path.read_bytes()) == failure["response_sha256"],
            "Original failed response SHA differs")
    # A 403 HTML body may contain client IP/security event data. Keep all raw
    # evidence locally and transport only this deliberately bounded summary.
    summary = {key:failure[key] for key in ("symbol","url","params","status","retrieved_at_utc","query_end")}
    summary["local_evidence"] = [{"path":str(path),"sha256":digest(path.read_bytes()),"bytes":path.stat().st_size}
                                 for path in (stop,body_path,receipt_path)]
    summary["additional_local_failure_evidence"] = []
    for path in a.failure_evidence:
        summary["additional_local_failure_evidence"].append(dict(path=str(path),sha256=digest(path.read_bytes()),bytes=path.stat().st_size))
    payloads["audit/local_failure_summary_no_response_body.json"] = encoded(summary)
    require(not any("RATE_LIMIT_STOP" in name or "/incomplete/" in name or "/locks/" in name
                    for name in payloads if name.startswith("root/")), "Production control file entered isolated input")
    manifest = dict(schema=SCHEMA, source_sha=SOURCE, code_lf_sha256=code, target=a.target,
                    prepared_at=now(), symbols=symbols, input_states=states,
                    symbols_binding_sha256=digest(inventory), status="verified_source_input_not_migrated",
                    scope="share_sources_only_no_history_signal_or_mail")
    receipt = archive_write(a.output_zip, manifest, payloads)
    a.output_zip.with_suffix(".receipt.json").write_bytes(encoded(receipt))
    print(json.dumps(receipt, indent=2))


def run_cloud(a):
    require(os.environ.get("GITHUB_ACTIONS") == "true" and os.environ.get("GITHUB_RUN_ID")
            and os.environ.get("GITHUB_SHA"), "Source-only network execution is restricted to the approved cloud workflow")
    module, code = native(a.source_root)
    original, payloads = archive_read(a.input_zip, a.input_sha256)
    require(original["status"] == "verified_source_input_not_migrated" and original["code_lf_sha256"] == code,
            "Input is not the approved pinned source closure")
    require(not any("RATE_LIMIT_STOP" in name or "/incomplete/" in name or "/locks/" in name
                    for name in payloads if name.startswith("root/")), "Input contains a production control file")
    root = a.work_root.resolve()
    require(not root.exists(), "Cloud work root must be fresh, not a stopped prior run")
    root.mkdir(parents=True)
    for name, raw in payloads.items():
        if name.startswith("root/"):
            path = root / safe(name[5:]); path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(raw)
    symbols = original["symbols"]
    old_files, states = closure(module, root, symbols)
    require(states == original["input_states"], "Restored native states differ")
    started = now(); records = []; status = "failed_source_only_not_adopted"
    error = None
    try:
        for index, symbol in enumerate(symbols):
            if index and a.interval_seconds:
                time.sleep(a.interval_seconds)
            before = now()
            module.refresh_share_change(root=root, symbol=symbol, cache_path=root / CACHE / (symbol + ".csv"),
                                        as_of=original["target"], allow_network=True)
            checked = module.verify_share_cache(root=root, symbol=symbol, as_of=original["target"])
            completed = now()
            if states[symbol]["query_end"] != original["target"]:
                state = json.loads((root / PREFIX / (symbol + ".json")).read_bytes())
                require(datetime.fromisoformat(before) <= datetime.fromisoformat(state["retrieved_at_utc"])
                        <= datetime.fromisoformat(completed), "New source collection time is outside actual call interval")
            records.append(dict(symbol=symbol, started_at=before, completed_at=completed, native_result=checked,
                                reused_existing_target=states[symbol]["query_end"] == original["target"]))
        status = "native_share_sources_complete_not_adopted"
    except BaseException as exc:
        error = repr(exc)
    finished = now()
    # Failure evidence stays intact; never remove a cloud rate-limit stop or
    # continue the next symbol after the unchanged native function has failed.
    try:
        after_files, after_states = closure(module, root, symbols)
        for name, raw in old_files.items():
            if name.endswith(".csv") and name.startswith(CACHE):
                import pandas as pd
                old = pd.read_csv(io.BytesIO(raw)).fillna("")
                new = pd.read_csv(io.BytesIO(after_files[name])).fillna("")
                pd.testing.assert_frame_equal(old, new.iloc[:len(old)].reset_index(drop=True), check_dtype=False, check_exact=True)
            elif name not in (PREFIX + s + ".json" for s in symbols):
                require(after_files.get(name) == raw, "Immutable source prefix changed: " + name)
        for symbol in symbols:
            old = old_files[PREFIX + symbol + ".json"]
            if after_files[PREFIX + symbol + ".json"] != old:
                require(after_files.get(PREFIX + "states/" + digest(old) + ".json") == old,
                        "New native state does not preserve old exact predecessor: " + symbol)
        if status == "native_share_sources_complete_not_adopted":
            require(len(records) == len(symbols) and all(x["query_end"] == original["target"] for x in after_states.values()),
                    "Incomplete target source coverage")
    except BaseException as exc:
        # Incomplete native publication must still leave a downloadable full
        # failure artifact; it cannot leave a qualifying root payload.
        status = "failed_source_only_not_adopted"
        error = repr(exc) if error is None else error + "; final_validation=" + repr(exc)
        after_files, after_states = {}, {}
    output = {"root/" + name: raw for name, raw in after_files.items()}
    output.update({name: raw for name, raw in payloads.items() if name.startswith("audit/")})
    # Preserve all new failed responses/receipts and the cloud stop separately.
    for path in (root / PREFIX).rglob("*"):
        if path.is_file() and path.relative_to(root).as_posix() not in after_files:
            output["audit/cloud_share_sources/" + path.relative_to(root / PREFIX).as_posix()] = path.read_bytes()
    for path in (root / CACHE).glob("*.csv"):
        if path.relative_to(root).as_posix() not in after_files:
            output["audit/cloud_share_change/" + path.name] = path.read_bytes()
    manifest = dict(schema=SCHEMA, source_sha=SOURCE, code_lf_sha256=code,
                    target=original["target"],symbols=symbols,input_zip_sha256=a.input_sha256,
                    started_at=started,finished_at=finished,status=status,error=error,records=records,
                    output_states=after_states,scope="share_sources_only_no_history_signal_or_mail",
                    github={k:os.environ.get(k,"") for k in ("GITHUB_RUN_ID","GITHUB_RUN_ATTEMPT","GITHUB_SHA","GITHUB_REPOSITORY","GITHUB_WORKFLOW_REF")})
    receipt = archive_write(a.output_zip, manifest, output)
    a.output_zip.with_suffix(".receipt.json").write_bytes(encoded(receipt))
    print(json.dumps(receipt, indent=2))
    require(status == "native_share_sources_complete_not_adopted", "Native source fetch failed; failure artifact retained: " + str(error))


def main():
    require(not sys.flags.optimize, "Run without Python optimization")
    p = argparse.ArgumentParser(); sub = p.add_subparsers(dest="action", required=True)
    q = sub.add_parser("prepare")
    q.add_argument("--source-root", type=Path, required=True); q.add_argument("--market-root", type=Path, required=True)
    q.add_argument("--symbols-json", type=Path, required=True);q.add_argument("--target",required=True)
    q.add_argument("--failure-evidence", type=Path,action="append",default=[])
    q.add_argument("--output-zip",type=Path,required=True)
    q = sub.add_parser("run-cloud")
    q.add_argument("--source-root",type=Path,required=True);q.add_argument("--input-zip",type=Path,required=True)
    q.add_argument("--input-sha256",required=True);q.add_argument("--work-root",type=Path,required=True)
    q.add_argument("--output-zip",type=Path,required=True);q.add_argument("--interval-seconds",type=float,default=3)
    a=p.parse_args()
    if a.action == "prepare":
        require(datetime.fromisoformat(a.target+"T15:00:00+08:00") <= datetime.now(timezone.utc), "Target is not close confirmed")
        prepare(a)
    else:
        require(0 <= a.interval_seconds <= 60, "Invalid serial pacing interval")
        run_cloud(a)


if __name__ == "__main__":
    main()
