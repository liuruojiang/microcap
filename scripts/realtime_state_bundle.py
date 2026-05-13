from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Iterable


MANIFEST_NAME = "top100_realtime_state_manifest.json"

REQUIRED_FILES = (
    "outputs/wind_microcap_top_100_biweekly_thursday_16y_cached.csv",
    "outputs/microcap_top100_mom16_biweekly_live_summary.json",
    "outputs/microcap_top100_mom16_biweekly_live_v1_1_proxy_meta.json",
    "outputs/microcap_top100_mom16_biweekly_live_v1_1_proxy_members.csv",
    "outputs/microcap_top100_mom16_biweekly_live_v1_1_proxy_turnover.csv",
    "outputs/microcap_top100_mom16_hedge_zz1000_0p8x_biweekly_thursday_16y_costed_nav.csv",
    ".microcap_index_cache/active_universe.csv",
    ".microcap_index_cache/current_st.csv",
)

OPTIONAL_GLOBS = (
    ".microcap_index_cache/realtime/*.json",
    ".microcap_index_cache/realtime/*.csv",
    ".microcap_index_cache/*_static_*.json",
    ".microcap_index_cache/*_static_*.csv",
)


def _repo_path(path: str) -> PurePosixPath:
    posix = path.replace("\\", "/").lstrip("/")
    pure = PurePosixPath(posix)
    if ".." in pure.parts:
        raise ValueError(f"unsafe path outside repository: {path}")
    return pure


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _iter_bundle_files(root: Path) -> list[str]:
    found: set[str] = set()
    for rel in REQUIRED_FILES:
        if (root / rel).is_file():
            found.add(rel)
    for pattern in OPTIONAL_GLOBS:
        for path in root.glob(pattern):
            if path.is_file():
                found.add(path.relative_to(root).as_posix())
    return sorted(found)


def _read_csv_header_and_rows(path: Path) -> tuple[list[str], int, dict[str, str] | None]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        header = list(reader.fieldnames or [])
        rows = list(reader)
    return header, len(rows), rows[-1] if rows else None


def _parse_date(value: str) -> date | None:
    if not value:
        return None
    text = str(value).strip()
    for sep in ("T", " "):
        if sep in text:
            text = text.split(sep, 1)[0]
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _csv_last_date(path: Path, candidates: Iterable[str]) -> date | None:
    if not path.is_file():
        return None
    header, rows, last_row = _read_csv_header_and_rows(path)
    if rows <= 0 or last_row is None:
        return None
    for name in candidates:
        if name in header:
            return _parse_date(last_row.get(name, ""))
    return None


def validate_state(root: Path, max_anchor_age_days: int | None = None, today: date | None = None) -> dict[str, object]:
    root = root.resolve()
    errors: list[str] = []
    warnings: list[str] = []
    files: list[dict[str, object]] = []

    for rel in REQUIRED_FILES:
        path = root / rel
        if not path.exists():
            errors.append(f"missing required file: {rel}")
            continue
        if not path.is_file():
            errors.append(f"required path is not a file: {rel}")
            continue
        if path.stat().st_size <= 0:
            errors.append(f"required file is empty: {rel}")
            continue
        files.append({"path": rel, "bytes": path.stat().st_size, "sha256": _sha256(path)})

    for rel in REQUIRED_FILES:
        path = root / rel
        if not path.is_file() or path.stat().st_size <= 0:
            continue
        if rel.endswith(".json"):
            try:
                json.loads(path.read_text(encoding="utf-8"))
            except Exception as exc:
                errors.append(f"invalid json in {rel}: {exc}")
        elif rel.endswith(".csv"):
            try:
                header, rows, _last = _read_csv_header_and_rows(path)
            except Exception as exc:
                errors.append(f"invalid csv in {rel}: {exc}")
                continue
            if not header:
                errors.append(f"csv has no header: {rel}")
            if rows <= 0:
                errors.append(f"csv has no data rows: {rel}")

    anchor_dates = {
        "proxy_index": _csv_last_date(
            root / "outputs/wind_microcap_top_100_biweekly_thursday_16y_cached.csv",
            ("date",),
        ),
        "costed_nav": _csv_last_date(
            root / "outputs/microcap_top100_mom16_hedge_zz1000_0p8x_biweekly_thursday_16y_costed_nav.csv",
            ("date",),
        ),
        "proxy_turnover": _csv_last_date(
            root / "outputs/microcap_top100_mom16_biweekly_live_v1_1_proxy_turnover.csv",
            ("rebalance_date", "date"),
        ),
    }
    for name, value in anchor_dates.items():
        if value is None:
            errors.append(f"cannot read last date for {name}")

    if max_anchor_age_days is not None and not errors:
        today_value = today or date.today()
        for name in ("proxy_index", "costed_nav"):
            value = anchor_dates.get(name)
            if value is None:
                continue
            age_days = (today_value - value).days
            if age_days > max_anchor_age_days:
                errors.append(
                    f"{name} is stale: last_date={value.isoformat()} age_days={age_days} "
                    f"max_anchor_age_days={max_anchor_age_days}"
                )
            elif age_days < 0:
                warnings.append(f"{name} has a future date: last_date={value.isoformat()}")

    return {
        "ok": not errors,
        "errors": errors,
        "warnings": warnings,
        "files": files,
        "anchor_dates": {key: value.isoformat() if value else None for key, value in anchor_dates.items()},
    }


def pack_state(root: Path, bundle: Path, max_anchor_age_days: int | None) -> dict[str, object]:
    report = validate_state(root, max_anchor_age_days=max_anchor_age_days)
    if not report["ok"]:
        return report
    root = root.resolve()
    bundle.parent.mkdir(parents=True, exist_ok=True)
    file_names = _iter_bundle_files(root)
    manifest = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "required_files": list(REQUIRED_FILES),
        "files": [
            {
                "path": rel,
                "bytes": (root / rel).stat().st_size,
                "sha256": _sha256(root / rel),
            }
            for rel in file_names
        ],
        "validation": report,
    }
    with zipfile.ZipFile(bundle, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for rel in file_names:
            archive.write(root / rel, rel)
        archive.writestr(MANIFEST_NAME, json.dumps(manifest, ensure_ascii=False, indent=2))
    return {**report, "bundle": str(bundle), "bundle_files": file_names}


def restore_state(root: Path, bundle: Path, max_anchor_age_days: int | None) -> dict[str, object]:
    if not bundle.is_file():
        return {"ok": False, "errors": [f"missing bundle: {bundle}"], "warnings": []}
    root = root.resolve()
    with zipfile.ZipFile(bundle, "r") as archive:
        for member in archive.infolist():
            if member.is_dir() or member.filename == MANIFEST_NAME:
                continue
            rel = _repo_path(member.filename)
            target = (root / rel.as_posix()).resolve()
            if root not in target.parents and target != root:
                raise ValueError(f"unsafe bundle member outside repository: {member.filename}")
            target.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(member, "r") as source, target.open("wb") as dest:
                dest.write(source.read())
    return validate_state(root, max_anchor_age_days=max_anchor_age_days)


def _print_report(report: dict[str, object]) -> None:
    print(json.dumps(report, ensure_ascii=False, indent=2))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate, pack, and restore Top100 realtime production state.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_common(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument("--root", type=Path, default=Path("."), help="repository root")
        subparser.add_argument("--max-anchor-age-days", type=int, default=None)

    validate_parser = subparsers.add_parser("validate")
    add_common(validate_parser)

    pack_parser = subparsers.add_parser("pack")
    add_common(pack_parser)
    pack_parser.add_argument("--bundle", type=Path, required=True)

    restore_parser = subparsers.add_parser("restore")
    add_common(restore_parser)
    restore_parser.add_argument("--bundle", type=Path, required=True)

    args = parser.parse_args(argv)
    if args.command == "validate":
        report = validate_state(args.root, max_anchor_age_days=args.max_anchor_age_days)
    elif args.command == "pack":
        report = pack_state(args.root, args.bundle, max_anchor_age_days=args.max_anchor_age_days)
    else:
        report = restore_state(args.root, args.bundle, max_anchor_age_days=args.max_anchor_age_days)
    _print_report(report)
    return 0 if report.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
