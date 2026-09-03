from __future__ import annotations

import argparse
import csv
import hashlib
import json
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath


CACHE_REL = PurePosixPath(".microcap_index_cache")
MANIFEST_NAME = "full_rebalance_cache_manifest.json"
MANIFEST_VERSION = 1
CACHE_DIRS = {
    "prices_raw": ".csv",
    "security_meta": ".json",
    "share_change": ".csv",
}
REQUIRED_ROOT_FILES = ("active_universe.csv", "current_st.csv")


def _safe_rel(value: str) -> PurePosixPath:
    rel = PurePosixPath(value.replace("\\", "/").lstrip("/"))
    if not rel.parts or ".." in rel.parts or rel.is_absolute():
        raise ValueError(f"unsafe cache bundle path: {value}")
    return rel


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _cache_files(root: Path) -> list[Path]:
    cache_root = root.resolve() / CACHE_REL.as_posix()
    files: list[Path] = []
    for directory, suffix in CACHE_DIRS.items():
        files.extend(sorted((cache_root / directory).glob(f"*{suffix}")))
    files.extend(cache_root / name for name in REQUIRED_ROOT_FILES)
    return [path for path in files if path.is_file()]


def _csv_unique_codes(path: Path) -> int:
    if not path.is_file():
        return 0
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        field = "code" if "code" in (reader.fieldnames or []) else "symbol"
        if field not in (reader.fieldnames or []):
            return 0
        return len(
            {
                str(row.get(field) or "").strip().zfill(6)
                for row in reader
                if str(row.get(field) or "").strip()
            }
        )


def validate_cache(root: Path, min_symbols: int = 4500) -> dict[str, object]:
    cache_root = root.resolve() / CACHE_REL.as_posix()
    counts = {
        directory: len(list((cache_root / directory).glob(f"*{suffix}")))
        for directory, suffix in CACHE_DIRS.items()
    }
    active_universe_count = _csv_unique_codes(cache_root / "active_universe.csv")
    errors: list[str] = []
    for directory, count in counts.items():
        if count < min_symbols:
            errors.append(
                f"full rebalance cache is incomplete: {directory} count={count} minimum={min_symbols}"
            )
    if active_universe_count < min_symbols:
        errors.append(
            "full rebalance active universe is incomplete: "
            f"count={active_universe_count} minimum={min_symbols}"
        )
    for name in REQUIRED_ROOT_FILES:
        path = cache_root / name
        if not path.is_file() or path.stat().st_size <= 0:
            errors.append(f"full rebalance cache is missing required file: {name}")
    return {
        "ok": not errors,
        "errors": errors,
        "cache_root": str(cache_root),
        "counts": counts,
        "active_universe_count": active_universe_count,
        "min_symbols": min_symbols,
    }


def pack_cache(root: Path, bundle: Path, min_symbols: int = 4500) -> dict[str, object]:
    report = validate_cache(root, min_symbols=min_symbols)
    if not report["ok"]:
        return report
    root = root.resolve()
    files = _cache_files(root)
    entries = []
    for path in files:
        rel = path.relative_to(root).as_posix()
        entries.append({"path": rel, "bytes": path.stat().st_size, "sha256": _sha256_path(path)})
    manifest = {
        "version": MANIFEST_VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "validation": report,
        "files": entries,
    }
    bundle = bundle.resolve()
    bundle.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(dir=bundle.parent, suffix=".zip", delete=False) as handle:
            temp_path = Path(handle.name)
        with zipfile.ZipFile(temp_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
            for path, entry in zip(files, entries, strict=True):
                archive.write(path, entry["path"])
            archive.writestr(MANIFEST_NAME, json.dumps(manifest, ensure_ascii=False, indent=2))
        temp_path.replace(bundle)
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)
    return {**report, "bundle": str(bundle), "bundle_bytes": bundle.stat().st_size, "files": len(entries)}


def _verified_manifest(archive: zipfile.ZipFile) -> dict[str, object]:
    names = archive.namelist()
    if MANIFEST_NAME not in names:
        raise ValueError(f"full cache bundle is missing {MANIFEST_NAME}")
    if len(names) != len(set(names)):
        raise ValueError("full cache bundle contains duplicate archive members")
    manifest = json.loads(archive.read(MANIFEST_NAME).decode("utf-8"))
    if manifest.get("version") != MANIFEST_VERSION:
        raise ValueError(f"unsupported full cache bundle version: {manifest.get('version')!r}")
    entries = manifest.get("files")
    if not isinstance(entries, list) or not entries:
        raise ValueError("full cache bundle has no manifest file entries")
    declared: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError("full cache bundle has an invalid manifest entry")
        rel = _safe_rel(str(entry.get("path") or "")).as_posix()
        if not rel.startswith(f"{CACHE_REL.as_posix()}/"):
            raise ValueError(f"full cache bundle member is outside cache root: {rel}")
        if rel in declared:
            raise ValueError(f"full cache bundle manifest contains duplicate path: {rel}")
        declared.add(rel)
        if rel not in names:
            raise ValueError(f"full cache bundle is missing declared file: {rel}")
        info = archive.getinfo(rel)
        expected_bytes = entry.get("bytes")
        expected_sha = str(entry.get("sha256") or "").lower()
        if not isinstance(expected_bytes, int) or info.file_size != expected_bytes:
            raise ValueError(f"full cache bundle byte count mismatch for: {rel}")
        digest = hashlib.sha256()
        with archive.open(info, "r") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
        if len(expected_sha) != 64 or digest.hexdigest() != expected_sha:
            raise ValueError(f"full cache bundle sha256 mismatch for: {rel}")
    payload = {name for name in names if name != MANIFEST_NAME and not name.endswith("/")}
    if payload != declared:
        raise ValueError("full cache bundle payload does not exactly match its manifest")
    return manifest


def restore_cache(root: Path, bundle: Path, min_symbols: int = 4500) -> dict[str, object]:
    if not bundle.is_file():
        return {"ok": False, "errors": [f"missing full cache bundle: {bundle}"]}
    root = root.resolve()
    with zipfile.ZipFile(bundle, "r") as archive:
        manifest = _verified_manifest(archive)
        for entry in manifest["files"]:
            rel = _safe_rel(str(entry["path"]))
            target = (root / rel.as_posix()).resolve()
            if root not in target.parents:
                raise ValueError(f"unsafe full cache extraction target: {rel}")
            target.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(rel.as_posix(), "r") as source, target.open("wb") as destination:
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    destination.write(chunk)
    report = validate_cache(root, min_symbols=min_symbols)
    report["restore_source"] = str(bundle.resolve())
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Pack, restore, or validate the full Top100 rebalance cache.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("validate", "pack", "restore"):
        child = subparsers.add_parser(command)
        child.add_argument("--root", type=Path, default=Path("."))
        child.add_argument("--min-symbols", type=int, default=4500)
        if command in {"pack", "restore"}:
            child.add_argument("--bundle", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "validate":
        report = validate_cache(args.root, min_symbols=args.min_symbols)
    elif args.command == "pack":
        report = pack_cache(args.root, args.bundle, min_symbols=args.min_symbols)
    else:
        report = restore_cache(args.root, args.bundle, min_symbols=args.min_symbols)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
