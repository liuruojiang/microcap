"""Verified whole-delivery transport; never rebuild history or authorize signals."""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import realtime_state_bundle as state
from scripts import top100_delivery as delivery

ARTIFACT = "microcap-whole-delivery-state"
REPOSITORY = "liuruojiang/codex-daily-automation-probe"
SOURCE_FILES = [delivery.AUTHORITY] + [
    f"microcap_top100_mom16_biweekly_live_v2_{v}.py" for v in delivery.COSTED
]


def copy_file(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)


def final_files(version: str) -> list[str]:
    prefix = f"outputs/microcap_top100_mom16_biweekly_live_v2_{version}"
    return [f"outputs/{delivery.COSTED[version]}"] + [
        f"{prefix}_{suffix}" for suffix in (
            "nav.csv", "latest_signal.csv", "summary.json", "performance_nav.csv",
            "performance_summary.csv", "performance_summary.json", "performance_yearly.csv",
        )
    ]


def require_ok(report: dict) -> None:
    if not report.get("ok"):
        raise RuntimeError("; ".join(map(str, report.get("errors", ["verification failed"]))))


def pack(roots: dict[str, Path], bundle: Path, expected: str) -> dict:
    if set(roots) != set(delivery.COSTED):
        raise ValueError("All three version roots are required")
    primary = roots["0"]
    before = {v: delivery.input_hashes(root) for v, root in roots.items()}
    if any(value != before["0"] for value in before.values()):
        raise ValueError("Isolated version workspaces do not share identical core and base inputs")
    for root in roots.values():
        if (root / delivery.LOCK).exists():
            raise RuntimeError("A whole-delivery refresh is active")
        require_ok(state.validate_state(root, max_anchor_age_days=5))
    with tempfile.TemporaryDirectory(prefix="top100-pack-") as directory:
        staged = Path(directory)
        for name in set(state._iter_bundle_files(primary)) | set(before["0"]):
            copy_file(primary / name, staged / name)
        for version, root in roots.items():
            for name in final_files(version):
                copy_file(root / name, staged / name)
        report = delivery.inspect_outputs(staged, expected)
        require_ok(report)
        if before != {v: delivery.input_hashes(root) for v, root in roots.items()}:
            raise RuntimeError("Inputs changed during cloud packing")
        report.update(status="complete", verified_at=datetime.now(timezone.utc).isoformat(),
                      release_sha=subprocess.check_output(
                          ["git", "rev-parse", "HEAD"], cwd=primary, text=True).strip())
        delivery.write_manifest(staged, report)
        extras = (set(report["artifacts"]) | set(report["inputs"]) |
                  {delivery.MANIFEST}) - set(SOURCE_FILES)
        packed = state.pack_state(staged, bundle, 5, extra_files=extras)
        require_ok(packed)
        with zipfile.ZipFile(bundle) as archive:
            state._verify_bundle_manifest(archive)
        return {"ok": True, "expected_date": expected, "release_sha": report["release_sha"],
                "bundle": str(bundle), "scope": "whole_workspace_delivery",
                "files": len(packed["bundle_files"])}


def restore(root: Path, bundle: Path, expected: str) -> dict:
    """Validate in staging, back up every replacement, then use the formal restore."""
    root = root.resolve()
    lock = root / delivery.LOCK
    lock.parent.mkdir(parents=True, exist_ok=True)
    handle = os.open(lock, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
    try:
        with os.fdopen(handle, "w") as stream:
            stream.write(str(os.getpid()))
        with zipfile.ZipFile(bundle) as archive:
            manifest = state._verify_bundle_manifest(archive)
            names = [entry["path"] for entry in manifest["files"]]
            for name in names:
                parts = Path(name).parts
                if (name in SOURCE_FILES or not parts or
                    parts[0] not in ("outputs", ".microcap_index_cache") or
                    ":" in name or "\\" in name or name.startswith("/")):
                    raise ValueError(f"Unexpected delivery payload path: {name}")
            for name in (delivery.BASE_PANEL, delivery.BASE_FILES["proxy_index"],
                         delivery.BASE_FILES["costed_nav"]):
                path = root / "outputs" / name
                if path.exists() and delivery.csv_info(path)[0]["latest_date"] > expected:
                    raise ValueError("Refusing to roll back newer local state")
            with tempfile.TemporaryDirectory(prefix="top100-restore-") as directory:
                staged = Path(directory)
                for name in SOURCE_FILES:
                    copy_file(root / name, staged / name)
                for name in names:
                    path = staged / name
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(archive.read(name))
                require_ok(delivery.validate_manifest(
                    staged, delivery.inspect_outputs(staged, expected)))
                require_ok(state.validate_state(
                    staged, max_anchor_age_days=5, require_current_refresh_proof=False))
                backup = root / ".codex_backups" / (
                    "cloud_restore_" + datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f"))
                backup.mkdir(parents=True)
                changes = []
                for name in names:
                    path = root / name
                    if path.exists():
                        copy_file(path, backup / name)
                    changes.append({"path": name, "existed": path.exists(),
                                    "before_sha256": state._sha256(path) if path.exists() else None})
                (backup / "manifest.json").write_text(
                    json.dumps({"bundle": str(bundle), "expected_date": expected,
                                "files": changes}, indent=2), encoding="utf-8")
                require_ok(state.restore_state(root, bundle, max_anchor_age_days=5))
                # The lock intentionally keeps external check/publication blocked until done.
                report = delivery.inspect_outputs(root, expected)
                require_ok(report)
                saved = json.loads((root / delivery.MANIFEST).read_text(encoding="utf-8"))
                for key in ("expected_date", "inputs", "artifacts"):
                    if saved.get(key) != report.get(key):
                        raise RuntimeError(f"Restored delivery read-back mismatch: {key}")
                return {"ok": True, "expected_date": expected, "backup": str(backup),
                        "restore_source": "verified_cloud_delivery",
                        "signal_ready": False, "next": "same-day preflight then whole-delivery check"}
    except Exception as exc:
        # Do not retain a misleading complete marker after a partially applied restore.
        # Validation-only rejection must leave existing local state and manifest unchanged.
        if "backup" in locals():
            delivery.write_manifest(root, {"status": "blocked", "errors": [str(exc)]})
        raise
    finally:
        lock.unlink()


def gh_json(*args: str):
    return json.loads(subprocess.check_output(["gh", *args], text=True, encoding="utf-8"))


def sync(root: Path, expected: str) -> dict:
    existing = delivery.validate_manifest(root, delivery.inspect_outputs(root, expected))
    if existing["ok"]:
        require_ok(state.validate_state(root, max_anchor_age_days=5, require_current_refresh_proof=False))
        return {"ok": True, "expected_date": expected, "restore_source": "existing_whole_delivery",
                "signal_ready": False}
    if (root / delivery.LOCK).exists():
        raise RuntimeError("Another whole-delivery operation is active")
    runs = gh_json("run", "list", "--repo", REPOSITORY, "--workflow",
                   "microcap-realtime-digest.yml", "--branch", "main", "--status", "success",
                   "--limit", "10", "--json", "databaseId,headSha")
    for run in runs:
        artifacts = gh_json("api", f"repos/{REPOSITORY}/actions/runs/{run['databaseId']}/artifacts")
        if not any(item["name"] == ARTIFACT and not item["expired"]
                   for item in artifacts.get("artifacts", [])):
            continue
        with tempfile.TemporaryDirectory(prefix="top100-download-") as directory:
            subprocess.run(["gh", "run", "download", str(run["databaseId"]), "--repo", REPOSITORY,
                            "--name", ARTIFACT, "--dir", directory], check=True)
            report = restore(root, Path(directory) / f"{ARTIFACT}.zip", expected)
            return {**report, "github_run": run["databaseId"], "automation_sha": run["headSha"]}
    raise RuntimeError("No successful GitHub whole-delivery artifact is available")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("pack", "restore", "sync"))
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--v23-root", type=Path)
    parser.add_argument("--v25-root", type=Path)
    parser.add_argument("--bundle", type=Path)
    parser.add_argument("--expected-date", required=True)
    args = parser.parse_args()
    try:
        if state._parse_date(args.expected_date) is None:
            raise ValueError("Invalid expected date")
        if args.command == "pack":
            if not args.bundle or not args.v23_root or not args.v25_root:
                raise ValueError("pack requires bundle and all isolated version roots")
            report = pack({"0": args.root, "3": args.v23_root, "5": args.v25_root},
                          args.bundle, args.expected_date)
        elif args.command == "restore":
            if not args.bundle:
                raise ValueError("restore requires bundle")
            report = restore(args.root, args.bundle, args.expected_date)
        else:
            report = sync(args.root, args.expected_date)
    except Exception as exc:
        report = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
