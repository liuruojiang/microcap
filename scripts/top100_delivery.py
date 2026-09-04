"""Whole-workspace delivery gate. A base-state validation is not delivery success."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import realtime_state_bundle as state
from scripts.restore_approved_top100_seed import BASE_FILES, COSTED

MANIFEST = "outputs/top100_delivery_manifest.json"
LOCK = "outputs/top100_delivery.lock"
BASE_PANEL = "microcap_top100_mom16_biweekly_live_v2_0_base_panel_refreshed.csv"
AUTHORITY = "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_frozen_tail_authority.json"
V20_STRATEGY_REVISION = "plain_mom16_fixed1_20260904"


def plain_v20_identity(row: dict) -> bool:
    """A version number alone cannot distinguish the retired v2.0 overlay."""
    try:
        return (row.get("strategy_revision") == V20_STRATEGY_REVISION
                and str(row.get("target_vol_enabled")) == "False"
                and str(row.get("overheat_enabled")) == "False"
                and float(row.get("current_execution_scale", -1)) in (0., 1.)
                and float(row.get("next_session_actionable_scale", -1)) in (0., 1.))
    except (TypeError, ValueError):
        return False


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()


def csv_info(path: Path, column: str = "date") -> tuple[dict, list[dict]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if column not in (reader.fieldnames or []):
            raise ValueError(f"Missing {column}: {path.name}")
        rows = list(reader)
    dates = [state._parse_date(row[column]) for row in rows]
    if not dates or any(day is None for day in dates) or dates != sorted(set(dates)):
        raise ValueError(f"Empty, invalid, duplicate or unsorted dates: {path.name}")
    return {"rows": len(rows), "latest_date": dates[-1].isoformat(), "sha256": sha(path)}, rows


def input_hashes(root: Path) -> dict:
    names = [f"outputs/{name}" for name in (*BASE_FILES.values(), BASE_PANEL)]
    names += [AUTHORITY] + [f"microcap_top100_mom16_biweekly_live_v2_{v}.py" for v in COSTED]
    return {name: sha(root / name) for name in names}


def inspect_outputs(root: Path, expected: str) -> dict:
    """Read written files, not stdout or a successful base-only report."""
    errors, streams, artifacts = [], {}, {}
    try:
        inputs = input_hashes(root)
        for name in (BASE_PANEL, BASE_FILES["proxy_index"], BASE_FILES["costed_nav"]):
            info, _ = csv_info(root / "outputs" / name)
            streams[name] = info
            if info["latest_date"] != expected:
                errors.append(f"base date mismatch: {name}={info['latest_date']} expected={expected}")
        turnover, _ = csv_info(root / "outputs" / BASE_FILES["proxy_turnover"], "rebalance_date")
        streams[BASE_FILES["proxy_turnover"]] = turnover
        if turnover["latest_date"] > expected:
            errors.append("turnover has a future rebalance")
        for v, costed in COSTED.items():
            prefix = f"microcap_top100_mom16_biweekly_live_v2_{v}"
            summary_path = root / "outputs" / f"{prefix}_summary.json"
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            if str(summary.get("version", "")).removeprefix("v") != f"2.{v}":
                errors.append(f"v2.{v} summary identity mismatch")
            if v == "0":
                params = summary.get("core_params", {})
                if (summary.get("strategy_revision") != V20_STRATEGY_REVISION or
                        params.get("momentum_gap_exit_buffer") != 0 or
                        params.get("target_volatility_scaling", {}).get("enabled") is not False or
                        params.get("overheat_defense", {}).get("enabled") is not False):
                    errors.append("v2.0 summary plain revision mismatch")
            if summary.get("historical_rewrite_audit", {}).get("status") != "clean":
                errors.append(f"v2.{v} has no clean second-run rewrite audit")
            if summary.get("latest_nav_date") != expected or summary.get("latest_trade_date") != expected:
                errors.append(f"v2.{v} summary date mismatch")
            proof = summary.get("data_freshness_proof", {})
            if proof.get("expected_latest_date") != expected:
                errors.append(f"v2.{v} freshness proof date mismatch")
            if proof.get("expected_latest_rebalance_date") != turnover["latest_date"]:
                errors.append(f"v2.{v} turnover/rebalance mismatch")
            for name in (costed, f"{prefix}_nav.csv", f"{prefix}_performance_nav.csv", f"{prefix}_latest_signal.csv"):
                info, rows = csv_info(root / "outputs" / name)
                streams[name] = info
                if info["latest_date"] != expected:
                    errors.append(f"final date mismatch: {name}={info['latest_date']} expected={expected}")
                if v == "0" and not name.endswith("performance_nav.csv"):
                    if not all(plain_v20_identity(row) for row in rows):
                        errors.append(f"v2.0 plain revision/state mismatch: {name}")
                if name.endswith("latest_signal.csv"):
                    if len(rows) != 1 or rows[0].get("version") != f"2.{v}":
                        errors.append(f"v2.{v} final CSV identity mismatch")
                    # Member instructions must carry explicit dated action fields.
                    if rows[0].get("member_rebalance_actionable") not in ("True", "False"):
                        errors.append(f"v2.{v} missing explicit member action flag")
                    if rows[0].get("member_rebalance_actionable") == "True":
                        # Close-confirmed contract: today's rebalance may plan NEXT session,
                        # unlike an intraday CSV whose executable date must be today.
                        execution = state._parse_date(rows[0].get("member_rebalance_execution_date", ""))
                        if (rows[0].get("member_rebalance_signal_date") != expected or
                                rows[0].get("member_rebalance_required") != "True" or
                                rows[0].get("member_rebalance_official") != "True" or
                                execution is None or execution <= state._parse_date(expected)):
                            errors.append(f"v2.{v} actionable members violate the close-confirmed dated contract")
            if sha(root / "outputs" / costed) != sha(root / "outputs" / f"{prefix}_nav.csv"):
                errors.append(f"v2.{v} costed NAV and display NAV differ")
            for suffix in ("summary.json", "performance_summary.json", "performance_summary.csv", "performance_yearly.csv"):
                name = f"outputs/{prefix}_{suffix}"
                artifacts[name] = sha(root / name)
        artifacts.update({f"outputs/{name}": info["sha256"] for name, info in streams.items()})
    except (OSError, ValueError, KeyError, TypeError) as exc:
        errors.append(str(exc))
        inputs = {}
    return {"ok": not errors, "scope": "whole_workspace_delivery", "errors": errors,
            "expected_date": expected, "streams": streams, "inputs": inputs, "artifacts": artifacts}


def validate_manifest(root: Path, report: dict) -> dict:
    if (root / LOCK).exists():
        report["errors"].append("whole-delivery refresh is running or requires interrupted-run review")
    try:
        manifest = json.loads((root / MANIFEST).read_text(encoding="utf-8"))
        for key in ("expected_date", "inputs", "artifacts"):
            if manifest.get(key) != report.get(key):
                report["errors"].append(f"delivery manifest mismatch: {key}; run refresh-all")
        if manifest.get("status") != "complete":
            report["errors"].append("delivery refresh is incomplete")
    except (OSError, ValueError) as exc:
        report["errors"].append(f"missing/invalid whole-delivery manifest: {exc}")
    report["ok"] = not report["errors"]
    return report


def independent_target(root: Path) -> str:
    """Date-only independent gate; actual market streams remain separately verified."""
    from scripts.exchange_calendar import latest_completed_session
    return latest_completed_session().isoformat()




def verify_release(root: Path) -> str:
    """Compare core code and authority to live remote main, not a stale tracking ref."""
    remote = subprocess.check_output(["git", "ls-remote", "origin", "refs/heads/main"], cwd=root, text=True).split()[0]
    try:
        subprocess.check_output(["git", "cat-file", "-e", f"{remote}^{{commit}}"],
                                cwd=root, stderr=subprocess.DEVNULL)
    except subprocess.CalledProcessError:
        # Fetch only the immutable object: never merge, checkout or alter dirty files.
        subprocess.run(["git", "fetch", "--no-tags", "origin", remote], cwd=root, check=True)
    for name in [AUTHORITY] + [f"microcap_top100_mom16_biweekly_live_v2_{v}.py" for v in COSTED]:
        payload = subprocess.check_output(["git", "show", f"{remote}:{name}"], cwd=root)
        if hashlib.sha256(payload.replace(b"\r\n", b"\n")).hexdigest() != sha(root / name):
            raise ValueError(f"Local core/authority differs from remote release: {name}")
    return remote


def write_manifest(root: Path, report: dict) -> None:
    path = root / MANIFEST
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)


def _refresh_all_unlocked(root: Path) -> dict:
    release = verify_release(root)
    write_manifest(root, {"status": "refreshing", "verified_at": datetime.now(timezone.utc).isoformat()})
    base_report = state.refresh_state(root, max_anchor_age_days=5)
    if not base_report.get("ok"):
        raise RuntimeError(f"Base refresh failed: {base_report.get('errors')}")
    target = independent_target(root)
    before = input_hashes(root)
    for v in COSTED:
        subprocess.run([sys.executable, "-X", "utf8", f"microcap_top100_mom16_biweekly_live_v2_{v}.py"], cwd=root, check=True)
    if input_hashes(root) != before:
        raise RuntimeError("Shared inputs changed during generation; rerun whole delivery")
    report = inspect_outputs(root, target)
    if independent_target(root) != target:
        raise RuntimeError("Completed session advanced during generation; rerun whole delivery")
    final_base = state.validate_state(root, max_anchor_age_days=5)
    report["errors"].extend(final_base.get("errors", []))
    report["ok"] = not report["errors"]
    report["release_sha"] = release
    report["status"] = "complete" if report["ok"] else "blocked"
    report["verified_at"] = datetime.now(timezone.utc).isoformat()
    write_manifest(root, report)
    return report


def refresh_all(root: Path) -> dict:
    lock = root / LOCK
    lock.parent.mkdir(parents=True, exist_ok=True)
    try:
        handle = os.open(lock, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
    except FileExistsError as exc:
        raise RuntimeError("Another group refresh is active; inspect the delivery lock before retrying") from exc
    try:
        with os.fdopen(handle, "w") as stream:
            stream.write(str(os.getpid()))
        return _refresh_all_unlocked(root)
    except BaseException as exc:
        write_manifest(root, {"status": "blocked", "errors": [str(exc)],
                              "verified_at": datetime.now(timezone.utc).isoformat()})
        raise
    finally:
        lock.unlink()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("check", "refresh-all"))
    args = parser.parse_args(argv)
    root = Path(__file__).resolve().parents[1]
    try:
        if args.command == "refresh-all":
            report = refresh_all(root)
        else:
            release = verify_release(root)
            report = validate_manifest(root, inspect_outputs(root, independent_target(root)))
            report["release_sha"] = release
            report["errors"].extend(state.validate_state(root, max_anchor_age_days=5).get("errors", []))
            report["ok"] = not report["errors"]
    except Exception as exc:
        report = {"ok": False, "scope": "whole_workspace_delivery", "errors": [str(exc)]}
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
