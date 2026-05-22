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
    "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_summary.json",
    "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_panel_refreshed.csv",
    "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_meta.json",
    "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv",
    "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_turnover.csv",
    "outputs/microcap_top100_mom16_hedge_zz1000_0p8x_biweekly_thursday_16y_v2_0_base_costed_nav.csv",
    ".microcap_index_cache/active_universe.csv",
    ".microcap_index_cache/current_st.csv",
)

OPTIONAL_GLOBS = (
    ".microcap_index_cache/realtime/*.json",
    ".microcap_index_cache/realtime/*.csv",
    ".microcap_index_cache/*_static_*.json",
    ".microcap_index_cache/*_static_*.csv",
)

PRICE_CACHE_DIR = ".microcap_index_cache/prices_raw"
SHARE_CACHE_DIR = ".microcap_index_cache/share_change"
STATIC_EFFECTIVE_MEMBER_GLOBS = (
    ".microcap_index_cache/realtime/*static_effective_members.csv",
    ".microcap_index_cache/*_static_effective_members.csv",
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
    for rel in _iter_current_member_cache_files(root):
        if (root / rel).is_file():
            found.add(rel)
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


def _csv_symbols(path: Path) -> list[str]:
    if not path.is_file():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if "symbol" not in (reader.fieldnames or []):
            return []
        symbols: list[str] = []
        for row in reader:
            value = str(row.get("symbol") or "").strip()
            if value:
                symbols.append(value.zfill(6))
        return symbols


def _latest_proxy_member_symbols(root: Path) -> list[str]:
    path = root / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv"
    if not path.is_file():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if "symbol" not in (reader.fieldnames or []) or "rebalance_date" not in (reader.fieldnames or []):
            return []
        rows = list(reader)
    dated_rows = [(row, _parse_date(str(row.get("rebalance_date") or ""))) for row in rows]
    dates = [value for _row, value in dated_rows if value is not None]
    if not dates:
        return []
    latest = max(dates)
    return [
        str(row.get("symbol") or "").strip().zfill(6)
        for row, value in dated_rows
        if value == latest and str(row.get("symbol") or "").strip()
    ]


def _current_member_symbols(root: Path) -> list[str]:
    symbols: set[str] = set()
    for pattern in STATIC_EFFECTIVE_MEMBER_GLOBS:
        for path in root.glob(pattern):
            symbols.update(_csv_symbols(path))
    if not symbols:
        symbols.update(_latest_proxy_member_symbols(root))
    return sorted(symbols)


def _iter_current_member_cache_files(root: Path) -> list[str]:
    files: list[str] = []
    for symbol in _current_member_symbols(root):
        for cache_dir in (PRICE_CACHE_DIR, SHARE_CACHE_DIR):
            rel = f"{cache_dir}/{symbol}.csv"
            if (root / rel).is_file():
                files.append(rel)
    return sorted(set(files))


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
            root / "outputs/microcap_top100_mom16_hedge_zz1000_0p8x_biweekly_thursday_16y_v2_0_base_costed_nav.csv",
            ("date",),
        ),
        "panel_shadow": _csv_last_date(
            root / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_panel_refreshed.csv",
            ("date",),
        ),
        "proxy_turnover": _csv_last_date(
            root / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_turnover.csv",
            ("rebalance_date", "date"),
        ),
    }
    for name, value in anchor_dates.items():
        if value is None:
            errors.append(f"cannot read last date for {name}")

    if max_anchor_age_days is not None and not errors:
        today_value = today or date.today()
        for name in ("proxy_index", "costed_nav", "panel_shadow"):
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

    current_symbols = _current_member_symbols(root)
    price_cache_files: list[dict[str, object]] = []
    price_anchor = anchor_dates.get("proxy_index")
    if not current_symbols:
        errors.append("cannot identify current effective member symbols for realtime price-cache validation")
    for symbol in current_symbols:
        rel = f"{PRICE_CACHE_DIR}/{symbol}.csv"
        path = root / rel
        if not path.is_file():
            warnings.append(f"missing current member price cache: {rel}")
            continue
        if path.stat().st_size <= 0:
            warnings.append(f"current member price cache is empty: {rel}")
            continue
        last_date = _csv_last_date(path, ("date",))
        if last_date is None:
            warnings.append(f"cannot read last date for current member price cache: {rel}")
            continue
        if price_anchor is not None and last_date < price_anchor:
            warnings.append(
                f"current member price cache is stale: {rel} last_date={last_date.isoformat()} "
                f"anchor_date={price_anchor.isoformat()}"
            )
        price_cache_files.append(
            {
                "path": rel,
                "bytes": path.stat().st_size,
                "sha256": _sha256(path),
                "last_date": last_date.isoformat(),
            }
        )

    return {
        "ok": not errors,
        "errors": errors,
        "warnings": warnings,
        "files": files,
        "anchor_dates": {key: value.isoformat() if value else None for key, value in anchor_dates.items()},
        "current_member_symbols": current_symbols,
        "price_cache_files": price_cache_files,
    }


def enforce_anchor_target(report: dict[str, object], target_end_date: date) -> dict[str, object]:
    anchor_dates = report.get("anchor_dates", {})
    if isinstance(anchor_dates, dict):
        for name in ("proxy_index", "costed_nav", "panel_shadow"):
            anchor_date = _parse_date(str(anchor_dates.get(name) or ""))
            if anchor_date is None or anchor_date < target_end_date:
                report.setdefault("errors", []).append(
                    f"{name} is older than refresh target: "
                    f"last_date={anchor_dates.get(name)} target_end_date={target_end_date.isoformat()}"
                )
    report["ok"] = not report.get("errors")
    return report


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


def refresh_state(
    root: Path,
    max_workers: int = 8,
    force_refresh_static_inputs: bool = False,
    max_anchor_age_days: int | None = None,
) -> dict[str, object]:
    sys.path.insert(0, str(root.resolve()))
    from run_top100_v1_6_v1_8_realtime_signals import ensure_static_realtime_inputs
    import microcap_top100_mom16_biweekly_live_v2_0 as v2_0

    ensure_static_realtime_inputs(force_refresh=force_refresh_static_inputs)

    v2_0._sync_embedded_base_config()
    args = v2_0._build_base_args(max_workers=max_workers)
    base_paths = v2_0.base_mod.build_output_paths(v2_0.base_mod.DEFAULT_OUTPUT_PREFIX)
    target_end_date: date | None = None
    try:
        panel_path, target_end_ts = v2_0.base_mod.build_refreshed_panel_shadow(args, base_paths)
        target_end_date = _parse_date(str(target_end_ts))
        if target_end_date is None:
            raise ValueError(f"cannot parse refresh target date: {target_end_ts}")
        v2_0.base_mod.ensure_strategy_files(args, base_paths, panel_path, target_end_ts)
        base_context = v2_0.base_mod.ensure_realtime_query_base_context(
            args,
            base_paths,
            panel_path,
            target_end_ts,
        )
        v2_0.base_mod.ensure_static_members_fresh(
            args,
            base_paths,
            panel_path,
            target_end_ts,
            base_context,
        )
        report = validate_state(root, max_anchor_age_days=max_anchor_age_days)
        context_anchor_date = base_context["close_df"].index[-1].date()
        report["context_anchor_date"] = context_anchor_date.isoformat()
        report["target_end_date"] = target_end_date.isoformat()
        report["panel_path"] = str(panel_path)
        report["refresh_source"] = "fresh"
        enforce_anchor_target(report, target_end_date)
        if context_anchor_date < target_end_date:
            report.setdefault("errors", []).append(
                f"aligned close_df anchor is older than refresh target: "
                f"last_date={context_anchor_date.isoformat()} target_end_date={target_end_date.isoformat()}"
            )
            report["ok"] = False
        if not report["ok"]:
            raise RuntimeError(
                "state refresh completed but produced stale anchors: "
                + "; ".join(str(error) for error in report.get("errors", []))
            )
        return report
    except Exception as exc:
        report = validate_state(root, max_anchor_age_days=max_anchor_age_days)
        if target_end_date is not None:
            report["target_end_date"] = target_end_date.isoformat()
            enforce_anchor_target(report, target_end_date)
        report["refresh_source"] = "existing_validated_state"
        report["refresh_warning"] = f"state refresh failed; reused existing validated state: {exc}"
        if not report["ok"]:
            raise RuntimeError(
                "state refresh failed and no reusable validated state is available: "
                + "; ".join(str(error) for error in report.get("errors", []))
            ) from exc
        return report


def restore_state(root: Path, bundle: Path, max_anchor_age_days: int | None) -> dict[str, object]:
    if not bundle.is_file():
        return {"ok": False, "errors": [f"missing bundle: {bundle}"], "warnings": []}
    root = root.resolve()
    with zipfile.ZipFile(bundle, "r") as archive:
        bundle_validation: dict[str, object] | None = None
        if MANIFEST_NAME in archive.namelist():
            manifest = json.loads(archive.read(MANIFEST_NAME).decode("utf-8"))
            validation = manifest.get("validation")
            if isinstance(validation, dict):
                bundle_validation = validation

        current_report = validate_state(root, max_anchor_age_days=max_anchor_age_days)
        if current_report.get("ok") and bundle_validation and bundle_validation.get("ok"):
            current_anchors = current_report.get("anchor_dates", {})
            bundle_anchors = bundle_validation.get("anchor_dates", {})
            if isinstance(current_anchors, dict) and isinstance(bundle_anchors, dict):
                current_dates = {
                    name: _parse_date(str(current_anchors.get(name) or ""))
                    for name in ("proxy_index", "costed_nav")
                }
                bundle_dates = {
                    name: _parse_date(str(bundle_anchors.get(name) or ""))
                    for name in ("proxy_index", "costed_nav")
                }
                comparable = all(current_dates[name] is not None and bundle_dates[name] is not None for name in current_dates)
                if comparable and all(current_dates[name] >= bundle_dates[name] for name in current_dates) and any(
                    current_dates[name] > bundle_dates[name] for name in current_dates
                ):
                    current_report["restore_source"] = "existing_checkout_state"
                    current_report["restore_warning"] = (
                        "skipped older state bundle: "
                        f"bundle_proxy_index={bundle_anchors.get('proxy_index')} "
                        f"bundle_costed_nav={bundle_anchors.get('costed_nav')}"
                    )
                    return current_report

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
    report = validate_state(root, max_anchor_age_days=max_anchor_age_days)
    report["restore_source"] = "bundle"
    return report


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

    refresh_parser = subparsers.add_parser("refresh")
    refresh_parser.add_argument("--root", type=Path, default=Path("."), help="repository root")
    refresh_parser.add_argument("--max-workers", type=int, default=8)
    refresh_parser.add_argument("--force-refresh-static-inputs", action="store_true")
    refresh_parser.add_argument("--max-anchor-age-days", type=int, default=None)

    restore_parser = subparsers.add_parser("restore")
    add_common(restore_parser)
    restore_parser.add_argument("--bundle", type=Path, required=True)

    args = parser.parse_args(argv)
    if args.command == "validate":
        report = validate_state(args.root, max_anchor_age_days=args.max_anchor_age_days)
    elif args.command == "pack":
        report = pack_state(args.root, args.bundle, max_anchor_age_days=args.max_anchor_age_days)
    elif args.command == "refresh":
        report = refresh_state(
            args.root,
            max_workers=args.max_workers,
            force_refresh_static_inputs=args.force_refresh_static_inputs,
            max_anchor_age_days=args.max_anchor_age_days,
        )
    else:
        report = restore_state(args.root, args.bundle, max_anchor_age_days=args.max_anchor_age_days)
    _print_report(report)
    return 0 if report.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
