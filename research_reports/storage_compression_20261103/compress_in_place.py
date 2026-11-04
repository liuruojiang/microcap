"""Authorized NTFS compression only. No deletion, moving, or content rewriting."""
from pathlib import Path
import ctypes
from ctypes import wintypes
import hashlib
import json
import os
import shutil
import subprocess
import time
from datetime import datetime, timezone

OUT = Path(__file__).resolve().parent
REPO = OUT.parents[1]
GROUPS = [(name, REPO / name) for name in (
    "quant_param_scan_runs", "archive", "归档", ".codex_backups", "backups",
    "research_reports", ".microcap_ohlc_cache", ".autobuild_top100_cache",
    "outputs", ".microcap_index_cache")]
prior = json.loads((REPO / "research_reports/storage_cleanup_20261103/cleanup_receipt.json").read_bytes())
for item in prior["remaining_targets"]:
    path = Path(item["path"]).resolve()
    if not path.is_relative_to(REPO):
        GROUPS.append(("external_test_" + path.name, path))
SKIP_EXT = {".zip", ".gz", ".bz2", ".xz", ".zst", ".7z", ".rar", ".whl",
            ".png", ".jpg", ".jpeg", ".webp", ".gif", ".mp4", ".pdf",
            ".exe", ".dll", ".pyd", ".so", ".db", ".sqlite", ".sqlite3",
            ".wal", ".shm"}
size_on_disk = ctypes.WinDLL("kernel32", use_last_error=True).GetCompressedFileSizeW
size_on_disk.argtypes = [wintypes.LPCWSTR, ctypes.POINTER(wintypes.DWORD)]
size_on_disk.restype = wintypes.DWORD

def stamp():
    return datetime.now(timezone.utc).isoformat()

def native(path):
    value = str(path)
    return value if value.startswith("\\\\?\\") else "\\\\?\\" + value

def disk_bytes(path):
    high = wintypes.DWORD()
    ctypes.set_last_error(0)
    low = size_on_disk(native(path), ctypes.byref(high))
    if low == 0xFFFFFFFF and ctypes.get_last_error():
        raise ctypes.WinError(ctypes.get_last_error())
    return (high.value << 32) | low

def digest(path):
    with open(native(path), "rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()

state = {"method": "compact.exe /C; native NTFS compression; no deletion",
         "started_at": stamp(), "free_before_bytes": shutil.disk_usage(REPO).free,
         "groups": [], "verified_files": 0, "saved_bytes_api": 0,
         "failures": [], "content_mismatches": []}

def save(phase, group=None):
    progress = {**state, "phase": phase, "active_group": group,
                "updated_at": stamp(), "free_now_bytes": shutil.disk_usage(REPO).free}
    (OUT / "status.json").write_text(json.dumps(progress, ensure_ascii=False, indent=2), encoding="utf-8")

with (OUT / "files.jsonl").open("w", encoding="utf-8") as records, (OUT / "compact.log").open("w", encoding="utf-8") as log:
    def batch_compress(batch, group):
        prepared = []
        for path, expected in batch:
            try:
                stat = os.stat(native(path), follow_symlinks=False)
                if (stat.st_size, stat.st_mtime_ns) != expected or stat.st_file_attributes & (0x400 | 0x4000 | 0x200 | 0x800):
                    group["changed_or_already_compressed"] += 1
                    continue
                prepared.append({"path": str(path), "size": stat.st_size,
                                 "sha256_before": digest(path), "before_bytes": disk_bytes(path)})
            except OSError as exc:
                state["failures"].append({"path": str(path), "error": str(exc)})
        if not prepared:
            return
        (OUT / "batch_journal.json").write_text(json.dumps({"state": "pending", "files": prepared}, ensure_ascii=False), encoding="utf-8")
        result = subprocess.run(["compact.exe", "/C", "/I", "/Q", "/A"] +
                                [row["path"] for row in prepared],
                                stdout=log, stderr=log, timeout=240)
        successes = 0
        for row in prepared:
            path = Path(row["path"])
            row["sha256_after"] = digest(path)
            row["size_after"] = os.stat(native(path)).st_size
            row["after_bytes"] = disk_bytes(path)
            row["compressed"] = bool(os.stat(native(path)).st_file_attributes & 0x800)
            if row["sha256_before"] != row["sha256_after"] or row["size"] != row["size_after"]:
                state["content_mismatches"].append(row)
                save("stopped_content_changed", group["name"])
                raise RuntimeError("Content changed; stop without overwriting or restoring this file: " + str(path))
            state["verified_files"] += 1
            group["verified_files"] += 1
            group["before_bytes"] += row["before_bytes"]
            group["after_bytes"] += row["after_bytes"]
            state["saved_bytes_api"] += row["before_bytes"] - row["after_bytes"]
            successes += row["compressed"]
            if not row["compressed"]:
                state["failures"].append({"path": str(path), "error": "compression flag not set", "exit_code": result.returncode})
            records.write(json.dumps(row, ensure_ascii=False) + "\n")
        records.flush()
        log.flush()
        (OUT / "batch_journal.json").write_text(json.dumps({"state": "verified", "files": prepared}, ensure_ascii=False), encoding="utf-8")
        save("compressing", group["name"])
        if result.returncode and successes == 0:
            raise RuntimeError("Compression command failed for the whole batch; no unchanged retry")

    for name, root in GROUPS:
        if not root.is_dir():
            continue
        root = root.resolve()
        group = {"name": name, "path": str(root), "started_at": stamp(), "seen_files": 0,
                 "seen_bytes": 0, "eligible_files": 0, "already_compressed": 0,
                 "changed_or_already_compressed": 0, "skipped_links": 0, "skipped_long_paths": 0,
                 "verified_files": 0, "before_bytes": 0, "after_bytes": 0}
        state["groups"].append(group)
        save("scanning_and_compressing", name)
        print("START", name, flush=True)
        pending, chars = [], 0
        stack = [native(root)]
        while stack:
            directory = stack.pop()
            with os.scandir(directory) as entries:
                for entry in entries:
                    stat = entry.stat(follow_symlinks=False)
                    attrs = stat.st_file_attributes
                    if attrs & 0x400:
                        group["skipped_links"] += 1
                        continue
                    path = Path(entry.path[4:] if entry.path.startswith("\\\\?\\") else entry.path)
                    if entry.is_dir(follow_symlinks=False):
                        if entry.name.lower() not in {".git", ".venv", "node_modules"} and path != OUT:
                            stack.append(entry.path)
                        continue
                    if not entry.is_file(follow_symlinks=False):
                        continue
                    group["seen_files"] += 1
                    group["seen_bytes"] += stat.st_size
                    if attrs & 0x800:
                        group["already_compressed"] += 1
                        continue
                    if stat.st_size < 16384 or attrs & (0x4000 | 0x200) or stat.st_nlink > 1 or path.suffix.lower() in SKIP_EXT:
                        continue
                    if len(str(path)) > 240:
                        group["skipped_long_paths"] += 1
                        continue
                    if not path.resolve().is_relative_to(root):
                        raise RuntimeError("Resolved file outside selected root: " + str(path))
                    group["eligible_files"] += 1
                    pending.append((path, (stat.st_size, stat.st_mtime_ns)))
                    chars += len(str(path)) + 8
                    if len(pending) >= 60 or chars >= 17000:
                        batch_compress(pending, group)
                        pending, chars = [], 0
        batch_compress(pending, group)
        group["completed_at"] = stamp()
        group["saved_bytes_api"] = group["before_bytes"] - group["after_bytes"]
        save("group_complete", name)
        print("DONE", name, "verified", group["verified_files"], "saved_GiB", round(group["saved_bytes_api"] / 2**30, 3), flush=True)
state.update(completed_at=stamp(), free_after_bytes=shutil.disk_usage(REPO).free)
save("complete")
(OUT / "result.json").write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
print("COMPLETE", "verified", state["verified_files"], "saved_GiB", round(state["saved_bytes_api"] / 2**30, 3), "free_GiB", round(state["free_after_bytes"] / 2**30, 2), flush=True)
