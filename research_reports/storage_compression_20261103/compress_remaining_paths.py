"""Compress remaining long paths and immutable data/fixtures via native NTFS metadata IOCTL."""
from pathlib import Path
from datetime import datetime, timezone
import ctypes
from ctypes import wintypes as W
import hashlib
import json
import os
import shutil

OUT = Path(__file__).resolve().parent
REPO = OUT.parents[1]
prior = json.loads((OUT / "result.json").read_bytes())
assert not prior["content_mismatches"] and not prior["failures"]
roots = [(g["name"], Path(g["path"]), True) for g in prior["groups"] if g["skipped_long_paths"]]
roots += [(name, REPO / name, False) for name in ("data", "tests")]
skip_ext = {".zip", ".gz", ".bz2", ".xz", ".zst", ".7z", ".rar", ".whl", ".png", ".jpg", ".jpeg", ".webp", ".gif", ".mp4", ".pdf", ".exe", ".dll", ".pyd", ".so", ".db", ".sqlite", ".sqlite3", ".wal", ".shm"}
K = ctypes.WinDLL("kernel32", use_last_error=True)
K.CreateFileW.argtypes = [W.LPCWSTR, W.DWORD, W.DWORD, W.LPVOID, W.DWORD, W.DWORD, W.HANDLE]
K.CreateFileW.restype = W.HANDLE
K.DeviceIoControl.argtypes = [W.HANDLE, W.DWORD, W.LPVOID, W.DWORD, W.LPVOID, W.DWORD, ctypes.POINTER(W.DWORD), W.LPVOID]
K.DeviceIoControl.restype = W.BOOL
K.CloseHandle.argtypes = [W.HANDLE]
K.CloseHandle.restype = W.BOOL
K.GetCompressedFileSizeW.argtypes = [W.LPCWSTR, ctypes.POINTER(W.DWORD)]
K.GetCompressedFileSizeW.restype = W.DWORD

def native(path):
    return "\\\\?\\" + str(path)

def digest(path):
    with open(native(path), "rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()

def disk_bytes(path):
    high = W.DWORD()
    ctypes.set_last_error(0)
    low = K.GetCompressedFileSizeW(native(path), ctypes.byref(high))
    if low == 0xFFFFFFFF and ctypes.get_last_error():
        raise ctypes.WinError(ctypes.get_last_error())
    return (high.value << 32) | low

result = {"method": "FSCTL_SET_COMPRESSION LZNT1; only file compression state changes", "source": "https://learn.microsoft.com/en-us/windows/win32/api/winioctl/ni-winioctl-fsctl_set_compression", "started_at": datetime.now(timezone.utc).isoformat(), "verified_files": 0, "saved_bytes_api": 0, "failures": [], "content_mismatches": [], "groups": []}
with (OUT / "remaining_files.jsonl").open("w", encoding="utf-8") as records:
    for name, root, long_only in roots:
        if not root.is_dir():
            continue
        root = root.resolve()
        group = {"name": name, "long_paths_only": long_only, "verified_files": 0, "saved_bytes_api": 0}
        result["groups"].append(group)
        print("START", name, "long_only", long_only, flush=True)
        stack = [native(root)]
        while stack:
            directory = stack.pop()
            with os.scandir(directory) as entries:
                for entry in entries:
                    stat = entry.stat(follow_symlinks=False)
                    if stat.st_file_attributes & 0x400:
                        continue
                    path = Path(entry.path[4:])
                    if entry.is_dir(follow_symlinks=False):
                        if entry.name.lower() not in {".git", ".venv", "node_modules"} and path != OUT:
                            stack.append(entry.path)
                        continue
                    if (not entry.is_file(follow_symlinks=False) or (long_only and len(str(path)) <= 240)
                            or stat.st_size < 16384 or stat.st_size > 30 * 2**30 or stat.st_nlink > 1
                            or stat.st_file_attributes & (0x800 | 0x200 | 0x4000) or path.suffix.lower() in skip_ext):
                        continue
                    assert path.resolve().is_relative_to(root)
                    # Deny simultaneous writers/deleters while this existing file is checked/compressed.
                    handle = K.CreateFileW(native(path), 0xC0000000, 1, None, 3, 0x00200000, None)
                    if handle == ctypes.c_void_p(-1).value:
                        result["failures"].append({"path": str(path), "error": str(ctypes.WinError(ctypes.get_last_error()))})
                        continue
                    try:
                        row = {"path": str(path), "size": stat.st_size, "sha256_before": digest(path), "before_bytes": disk_bytes(path)}
                        (OUT / "remaining_journal.json").write_text(json.dumps(row, ensure_ascii=False), encoding="utf-8")
                        mode = ctypes.c_ushort(2)
                        returned = W.DWORD()
                        if not K.DeviceIoControl(handle, 0x9C040, ctypes.byref(mode), ctypes.sizeof(mode), None, 0, ctypes.byref(returned), None):
                            result["failures"].append({"path": str(path), "error": str(ctypes.WinError(ctypes.get_last_error()))})
                            continue
                        row.update(sha256_after=digest(path), size_after=os.stat(native(path)).st_size, after_bytes=disk_bytes(path))
                        if row["sha256_before"] != row["sha256_after"] or row["size"] != row["size_after"]:
                            result["content_mismatches"].append(row)
                            raise RuntimeError("Content changed; stop without overwriting files")
                        result["verified_files"] += 1
                        group["verified_files"] += 1
                        result["saved_bytes_api"] += row["before_bytes"] - row["after_bytes"]
                        group["saved_bytes_api"] += row["before_bytes"] - row["after_bytes"]
                        records.write(json.dumps(row, ensure_ascii=False) + "\n")
                        records.flush()
                    finally:
                        K.CloseHandle(handle)
            (OUT / "remaining_status.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        print("DONE", name, "verified", group["verified_files"], "saved_GiB", round(group["saved_bytes_api"] / 2**30, 3), flush=True)
result.update(completed_at=datetime.now(timezone.utc).isoformat(), free_after_bytes=shutil.disk_usage(REPO).free)
(OUT / "remaining_result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
print("COMPLETE", result["verified_files"], round(result["saved_bytes_api"] / 2**30, 3), flush=True)
