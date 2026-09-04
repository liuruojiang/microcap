from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
import tempfile
import zipfile
from datetime import date, datetime, timedelta, timezone
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
V2_STATIC_CONTEXT_PREFIX = (
    ".microcap_index_cache/realtime/"
    "microcap_top100_mom16_biweekly_live_v2_0_base_static"
)
TOP_N = 100
REFRESH_PROOF_REL = ".microcap_index_cache/realtime/top100_realtime_refresh_proof.json"
REFRESH_PROOF_VERSION = 1
REFRESH_PROOF_SOURCE = "independent_close_history_refresh"


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


def _build_refresh_proof(target_end_date: date) -> dict[str, object]:
    return {
        "version": REFRESH_PROOF_VERSION,
        "source": REFRESH_PROOF_SOURCE,
        "target_end_date": target_end_date.isoformat(),
        "verified_on": _cn_today().isoformat(),
        "verified_at_utc": datetime.now(timezone.utc).isoformat(),
    }


def _write_refresh_proof(root: Path, target_end_date: date, evidence: dict[str, object] | None = None) -> dict[str, object]:
    proof = _build_refresh_proof(target_end_date)
    if evidence is not None:
        proof["preflight_evidence"] = evidence
    path = root.resolve() / REFRESH_PROOF_REL
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            json.dump(proof, handle, ensure_ascii=False, indent=2)
            temp_path = Path(handle.name)
        temp_path.replace(path)
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)
    return proof


def _load_refresh_proof(root: Path) -> dict[str, object] | None:
    path = root.resolve() / REFRESH_PROOF_REL
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _cn_today() -> date:
    return (datetime.now(timezone.utc) + timedelta(hours=8)).date()


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


def _csv_has_valid_named_symbols(path: Path, expected_count: int = TOP_N) -> bool:
    if not path.is_file():
        return False
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fields = set(reader.fieldnames or [])
        if not {"symbol", "name"}.issubset(fields):
            return False
        rows = list(reader)
    symbols = [str(row.get("symbol") or "").strip().zfill(6) for row in rows]
    names = [str(row.get("name") or "").strip() for row in rows]
    return bool(
        len(rows) == int(expected_count)
        and len(set(symbols)) == int(expected_count)
        and all(symbols)
        and all(names)
    )


def _latest_proxy_member_symbols(root: Path) -> list[str]:
    path = root / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv"
    if not path.is_file():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if "symbol" not in (reader.fieldnames or []) or "rebalance_date" not in (reader.fieldnames or []):
            return []
        rows = list(reader)
    if "name" not in (reader.fieldnames or []):
        return []
    dated_rows = [(row, _parse_date(str(row.get("rebalance_date") or ""))) for row in rows]
    dates = [value for _row, value in dated_rows if value is not None]
    if not dates:
        return []
    latest = max(dates)
    return [
        str(row.get("symbol") or "").strip().zfill(6)
        for row, value in dated_rows
        if value == latest
        and str(row.get("symbol") or "").strip()
        and str(row.get("name") or "").strip()
    ]


def _current_member_symbols(root: Path) -> list[str]:
    # A valid v2.0 static context is the sole authority for the current live
    # member set. Older version-family snapshots are retained for audit only;
    # unioning them can silently reintroduce obsolete or ST securities.
    if _has_current_v2_static_member_context(root):
        _target_path, effective_path = _current_v2_static_member_paths(root)
        return sorted(set(_csv_symbols(effective_path)))

    symbols: set[str] = set()
    for pattern in STATIC_EFFECTIVE_MEMBER_GLOBS:
        for path in root.glob(pattern):
            symbols.update(_csv_symbols(path))
    if not symbols:
        symbols.update(_latest_proxy_member_symbols(root))
    return sorted(symbols)


def _current_v2_static_member_paths(root: Path) -> tuple[Path, Path]:
    prefix = root / V2_STATIC_CONTEXT_PREFIX
    return Path(f"{prefix}_target_members.csv"), Path(f"{prefix}_effective_members.csv")


def _has_current_v2_static_member_context(root: Path) -> bool:
    proxy_members = root / REQUIRED_FILES[4]
    latest_proxy_rebalance = _csv_last_date(proxy_members, ("rebalance_date",))
    prefix = root / V2_STATIC_CONTEXT_PREFIX
    meta_path = Path(f"{prefix}_meta.json")
    target_path, effective_path = _current_v2_static_member_paths(root)
    changes_path = Path(f"{prefix}_rebalance_changes.csv")
    if latest_proxy_rebalance is None or not all(
        path.is_file() and path.stat().st_size > 0
        for path in (meta_path, target_path, effective_path, changes_path)
    ):
        return False
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return False
    if _parse_date(str(meta.get("latest_rebalance") or "")) != latest_proxy_rebalance:
        return False
    for path in (target_path, effective_path):
        if not _csv_has_valid_named_symbols(path):
            return False
    return True


def _current_st_symbols(root: Path) -> set[str]:
    path = root / ".microcap_index_cache/current_st.csv"
    if not path.is_file():
        return set()
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        code_field = "code" if "code" in (reader.fieldnames or []) else "symbol"
        if code_field not in (reader.fieldnames or []):
            return set()
        return {
            str(row.get(code_field) or "").strip().zfill(6)
            for row in reader
            if str(row.get(code_field) or "").strip()
        }


def _current_v2_effective_member_st_names(root: Path) -> list[str]:
    if not _has_current_v2_static_member_context(root):
        return []
    _target_path, effective_path = _current_v2_static_member_paths(root)
    if not effective_path.is_file():
        return []
    with effective_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if "name" not in (reader.fieldnames or []):
            return []
        return sorted(
            {
                name
                for row in reader
                if (name := str(row.get("name") or "").strip())
                and name.upper().startswith(("ST", "*ST", "PT"))
            }
        )


def _iter_current_member_cache_files(root: Path) -> list[str]:
    files: list[str] = []
    for symbol in _current_member_symbols(root):
        for cache_dir in (PRICE_CACHE_DIR, SHARE_CACHE_DIR):
            rel = f"{cache_dir}/{symbol}.csv"
            if (root / rel).is_file():
                files.append(rel)
    return sorted(set(files))


def validate_state(
    root: Path,
    max_anchor_age_days: int | None = None,
    today: date | None = None,
    *,
    require_current_refresh_proof: bool = True,
) -> dict[str, object]:
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

    today_value = today or _cn_today()
    refresh_proof = _load_refresh_proof(root)
    evidence = (refresh_proof or {}).get("preflight_evidence", {})
    target_text = (refresh_proof or {}).get("target_end_date")
    independently_current = (
        require_current_refresh_proof and isinstance(evidence, dict) and
        (refresh_proof or {}).get("version") == REFRESH_PROOF_VERSION and
        (refresh_proof or {}).get("source") == REFRESH_PROOF_SOURCE and
        (refresh_proof or {}).get("verified_on") == today_value.isoformat() and
        evidence.get("expected_calendar_day") == target_text and
        evidence.get("independent_completed_day") == target_text and
        evidence.get("member_name_quote_day") == today_value.isoformat() and
        evidence.get("member_name_count") == TOP_N and
        evidence.get("current_st_name_intersection") == 0 and
        all(value == _parse_date(str(target_text or "")) for name, value in anchor_dates.items()
            if name != "proxy_turnover")
    )
    if not errors:
        for name in ("proxy_index", "costed_nav", "panel_shadow"):
            value = anchor_dates.get(name)
            if value is None:
                continue
            age_days = (today_value - value).days
            if max_anchor_age_days is not None and age_days > max_anchor_age_days and not independently_current:
                errors.append(
                    f"{name} is stale: last_date={value.isoformat()} age_days={age_days} "
                    f"max_anchor_age_days={max_anchor_age_days}"
                )
            elif age_days < 0:
                errors.append(f"{name} has a future date: last_date={value.isoformat()}")

    if require_current_refresh_proof and refresh_proof is None:
        errors.append(f"missing or invalid independent realtime refresh proof: {REFRESH_PROOF_REL}")
    elif require_current_refresh_proof and refresh_proof is not None:
        expected_fields = {
            "version": REFRESH_PROOF_VERSION,
            "source": REFRESH_PROOF_SOURCE,
            "verified_on": today_value.isoformat(),
        }
        for field, expected in expected_fields.items():
            if refresh_proof.get(field) != expected:
                errors.append(
                    "independent realtime refresh proof is stale or inconsistent: "
                    f"field={field} actual={refresh_proof.get(field)!r} expected={expected!r}"
                )
        proof_target = _parse_date(str(refresh_proof.get("target_end_date") or ""))
        if proof_target is None:
            errors.append("independent realtime refresh proof has no valid target_end_date")
        else:
            for name in ("proxy_index", "costed_nav", "panel_shadow"):
                if anchor_dates.get(name) != proof_target:
                    errors.append(
                        "state anchor does not match independent realtime refresh proof: "
                        f"name={name} last_date={anchor_dates.get(name)} "
                        f"target_end_date={proof_target.isoformat()}"
                    )

    current_symbols = _current_member_symbols(root)
    price_cache_files: list[dict[str, object]] = []
    price_anchor = anchor_dates.get("proxy_index")
    if not current_symbols:
        errors.append("cannot identify current effective member symbols for realtime price-cache validation")
    elif len(current_symbols) != TOP_N:
        errors.append(
            "current effective member symbols are not exactly the required unique count: "
            f"actual={len(current_symbols)} required={TOP_N}"
        )
    current_st_overlap = sorted(set(current_symbols) & _current_st_symbols(root))
    if current_st_overlap:
        errors.append(
            "current v2.0 effective members intersect current ST universe: "
            + ",".join(current_st_overlap)
        )
    current_st_names = _current_v2_effective_member_st_names(root)
    if current_st_names:
        errors.append(
            "current v2.0 effective members contain ST/PT names: "
            + ",".join(current_st_names)
        )
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
    if not _has_current_v2_static_member_context(root):
        errors.append(
            "current v2.0 static member context is missing, incomplete, or older than "
            "the latest proxy-members rebalance"
        )

    return {
        "ok": not errors,
        "scope": "base_state_only",
        "final_outputs_validated": False,
        "errors": errors,
        "warnings": warnings,
        "files": files,
        "anchor_dates": {key: value.isoformat() if value else None for key, value in anchor_dates.items()},
        "current_member_symbols": current_symbols,
        "price_cache_files": price_cache_files,
        "refresh_proof": refresh_proof,
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


def preflight_state(root: Path, max_anchor_age_days: int | None = None,
                    expected_date: date | None = None) -> dict[str, object]:
    """Independently recheck existing state without refreshing/rebuilding its NAV."""
    root = root.resolve()
    before = validate_state(root, max_anchor_age_days=None if expected_date else max_anchor_age_days,
                            require_current_refresh_proof=False)
    if not before["ok"]:
        return before
    sys.path.insert(0, str(root))
    import microcap_top100_mom16_biweekly_live_v2_0 as v2_0
    v2_0._sync_embedded_base_config()
    base = v2_0.base_mod
    anchor = _parse_date(str(before["anchor_dates"]["proxy_index"]))
    history = base.fetch_eastmoney_index_history(
        "1.000852", base.pd.Timestamp(anchor) - base.pd.Timedelta(days=20)
    )
    target = base.latest_closed_history_date(history).date()
    if expected_date is not None and target != expected_date:
        raise RuntimeError(f"Independent history {target} differs from expected calendar day {expected_date}")
    evidence = verify_live_member_names(base, before["current_member_symbols"])
    evidence["independent_completed_day"] = target.isoformat()
    evidence["independent_history_rows"] = len(history)
    if expected_date is not None:
        evidence["expected_calendar_day"] = expected_date.isoformat()
    return certify_existing_state(root, before, target, max_anchor_age_days, evidence)


def verify_live_member_names(base, symbols: list[str]) -> dict[str, object]:
    quotes = base.fetch_member_realtime_quotes(symbols)
    today = _cn_today().isoformat()
    valid = quotes.loc[quotes["trade_date"].astype(str).eq(today)].copy()
    if len(valid) != TOP_N or set(valid["code"]) != set(symbols):
        raise RuntimeError(f"current member-name audit requires {TOP_N}/{TOP_N} same-day quotes")
    base.assert_no_st_members(valid, "independent preflight current member names")
    return {"member_name_quote_day": today, "member_name_count": len(valid),
            "member_name_source": quotes.attrs.get("quote_source", "unknown"),
            "current_st_name_intersection": 0}


def certify_existing_state(root: Path, before: dict[str, object], target: date,
                           max_anchor_age_days: int | None = None,
                           evidence: dict[str, object] | None = None) -> dict[str, object]:
    """Commit proof only after independent history and unchanged state agree."""
    calendar_match = evidence is not None and evidence.get("expected_calendar_day") == target.isoformat()
    after = validate_state(root, max_anchor_age_days=None if calendar_match else max_anchor_age_days,
                           require_current_refresh_proof=False)
    for name in ("proxy_index", "costed_nav", "panel_shadow"):
        if after.get("anchor_dates", {}).get(name) != target.isoformat():
            after.setdefault("errors", []).append(
                f"preflight anchor mismatch: {name}={after.get('anchor_dates', {}).get(name)} "
                f"independent_completed_day={target.isoformat()}; explicit state repair required"
            )
    if before.get("files") != after.get("files") or before.get("price_cache_files") != after.get("price_cache_files"):
        after.setdefault("errors", []).append("state changed during independent preflight")
    if after.get("errors"):
        after["ok"] = False
        return after
    if evidence is None:
        _write_refresh_proof(root, target)
    else:
        _write_refresh_proof(root, target, evidence)
    report = validate_state(root, max_anchor_age_days=max_anchor_age_days)
    report["preflight_source"] = "official_index_history_loader; no panel/NAV rebuild"
    return report


def pack_state(root: Path, bundle: Path, max_anchor_age_days: int | None,
               extra_files: Iterable[str] = ()) -> dict[str, object]:
    report = validate_state(root, max_anchor_age_days=max_anchor_age_days)
    if not report["ok"]:
        return report
    root = root.resolve()
    bundle.parent.mkdir(parents=True, exist_ok=True)
    file_names = sorted(set(_iter_bundle_files(root)) |
                        {_repo_path(name).as_posix() for name in extra_files})
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


def _verify_bundle_manifest(archive: zipfile.ZipFile) -> dict[str, object]:
    names = archive.namelist()
    if MANIFEST_NAME not in names:
        raise ValueError(f"state bundle is missing integrity manifest: {MANIFEST_NAME}")
    if len(names) != len(set(names)):
        raise ValueError("state bundle contains duplicate archive members")
    try:
        manifest = json.loads(archive.read(MANIFEST_NAME).decode("utf-8"))
    except Exception as exc:
        raise ValueError(f"state bundle has invalid integrity manifest: {exc}") from exc
    entries = manifest.get("files")
    if not isinstance(entries, list) or not entries:
        raise ValueError("state bundle integrity manifest has no file entries")

    declared: set[str] = set()
    portable_paths: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError("state bundle integrity manifest contains an invalid file entry")
        rel = _repo_path(str(entry.get("path") or "")).as_posix()
        if not rel or rel == "." or rel == MANIFEST_NAME:
            raise ValueError(f"state bundle integrity manifest contains an invalid path: {rel!r}")
        if rel in declared:
            raise ValueError(f"state bundle integrity manifest contains a duplicate path: {rel}")
        portable = "/".join(part.rstrip(". ").casefold() for part in PurePosixPath(rel).parts)
        if portable in portable_paths:
            raise ValueError(f"state bundle contains a Windows path alias collision: {rel}")
        if any(":" in part or part.endswith((".", " ")) for part in PurePosixPath(rel).parts):
            raise ValueError(f"state bundle contains a nonportable path alias: {rel}")
        portable_paths.add(portable)
        declared.add(rel)
        if rel not in names:
            raise ValueError(f"state bundle is missing declared file: {rel}")
        info = archive.getinfo(rel)
        expected_bytes = entry.get("bytes")
        expected_sha = str(entry.get("sha256") or "").lower()
        if not isinstance(expected_bytes, int) or expected_bytes < 0:
            raise ValueError(f"state bundle has invalid byte count for: {rel}")
        if info.file_size != expected_bytes:
            raise ValueError(
                f"state bundle byte count mismatch for {rel}: "
                f"actual={info.file_size} expected={expected_bytes}"
            )
        digest = hashlib.sha256()
        with archive.open(info, "r") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
        if len(expected_sha) != 64 or digest.hexdigest() != expected_sha:
            raise ValueError(f"state bundle sha256 mismatch for: {rel}")

    payload_names = {name for name in names if name != MANIFEST_NAME and not name.endswith("/")}
    undeclared = sorted(payload_names - declared)
    if undeclared:
        raise ValueError("state bundle contains undeclared files: " + ",".join(undeclared))
    return manifest


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
        _write_refresh_proof(root, target_end_date)
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
        if target_end_date is None:
            report = validate_state(root, max_anchor_age_days=max_anchor_age_days)
            report.setdefault("errors", []).append(
                "state refresh failed before an independent latest-completed-session target "
                "could be established"
            )
            report["ok"] = False
            raise RuntimeError(
                "state refresh failed closed because no independent refresh target is available: "
                + "; ".join(str(error) for error in report.get("errors", []))
            ) from exc
        _write_refresh_proof(root, target_end_date)
        report = validate_state(root, max_anchor_age_days=max_anchor_age_days)
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
        manifest = _verify_bundle_manifest(archive)
        bundle_validation: dict[str, object] | None = None
        validation = manifest.get("validation")
        if isinstance(validation, dict):
            bundle_validation = validation

        current_report = validate_state(
            root,
            max_anchor_age_days=max_anchor_age_days,
            require_current_refresh_proof=False,
        )
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
    # A restored bundle may be a prior-session bootstrap seed. The realtime
    # refresh step must establish a current proof before any signal is emitted.
    report = validate_state(
        root,
        max_anchor_age_days=max_anchor_age_days,
        require_current_refresh_proof=False,
    )
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

    preflight_parser = subparsers.add_parser("preflight")
    add_common(preflight_parser)
    preflight_parser.add_argument("--expected-date", type=date.fromisoformat,
                                  help="latest completed date from the formal exchange calendar")

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
    elif args.command == "preflight":
        report = preflight_state(args.root, max_anchor_age_days=args.max_anchor_age_days,
                                 expected_date=args.expected_date)
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
