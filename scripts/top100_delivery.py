"""Whole-workspace delivery gate. A base-state validation is not delivery success."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import re
import subprocess
import sys
from datetime import datetime, timedelta, timezone
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
V23_STRATEGY_REVISION = "plain_lb25_hl2p5_r2off_vol10_26_20_20260904"
V25_STRATEGY_REVISION = "plain_lb20_hl3_entry0_exit0_20260905"


def plain_v23_identity(row: dict) -> bool:
    try:
        return (row.get("strategy_revision") == V23_STRATEGY_REVISION
                and str(row.get("target_vol_enabled")) == "False"
                and str(row.get("r2_gate_enabled")) == "False"
                and str(row.get("cash_day_yield_enabled")) == "False"
                and str(row.get("financing_enabled")) == "False"
                and float(row.get("r2_entry_gate", -1)) == 0.
                and float(row.get("signal_spread_hedge_ratio", -1)) == 1.
                and abs(float(row.get("momentum_gap_exit_buffer", -1)) - .08) < 1e-12
                # NAV and final signal use these two established entry aliases.
                and any(key in row for key in ("entry_threshold", "momentum_gap_entry_threshold"))
                and all(float(row[key]) == 0. for key in ("entry_threshold", "momentum_gap_entry_threshold")
                        if key in row)
                and abs(float(row.get("overheat_trigger_threshold", -1)) - .26) < 1e-12
                and abs(float(row.get("overheat_recovery_threshold", -1)) - .20) < 1e-12)
    except (TypeError, ValueError):
        return False


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


def plain_v25_identity(row: dict) -> bool:
    """Reject retired v2.5 threshold lines even when their version field still says 2.5."""
    try:
        return (
            row.get("strategy_revision") == V25_STRATEGY_REVISION
            and str(row.get("target_vol_enabled")) == "False"
            and str(row.get("cash_day_yield_enabled")) == "False"
            and str(row.get("financing_enabled")) == "False"
            and float(row.get("lookback", -1)) == 20.0
            and float(row.get("halflife", -1)) == 3.0
            and float(row.get("entry_threshold", -1)) == 0.0
            and float(row.get("exit_threshold", -1)) == 0.0
            and float(row.get("signal_spread_hedge_ratio", -1)) == 0.0
            and float(row.get("execution_hedge_ratio", -1)) == 0.0
        )
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


def canonical_member_rebalance(root: Path, expected: str) -> dict[str, object]:
    """Derive formal list changes from the delivered point-in-time member lineage."""
    path = root / "outputs" / BASE_FILES["proxy_members"]
    with path.open(encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    if not rows or any(key not in rows[0] for key in ("rebalance_date", "symbol")):
        raise ValueError("proxy members are missing rebalance_date/symbol")
    target = state._parse_date(expected)
    if target is None:
        raise ValueError("invalid expected date for proxy-member lineage")
    by_date: dict[object, list[str]] = {}
    for row in rows:
        day = state._parse_date(row.get("rebalance_date", ""))
        symbol = str(row.get("symbol", "")).strip().zfill(6)
        if day is None or len(symbol) != 6 or not symbol.isdigit() or symbol == "000000":
            raise ValueError("proxy members contain an invalid rebalance date or symbol")
        if day <= target:
            by_date.setdefault(day, []).append(symbol)
    dates = sorted(by_date)
    if len(dates) < 2:
        raise ValueError("proxy members do not contain the latest two formal rebalances")
    previous, current = dates[-2:]
    previous_symbols, current_symbols = set(by_date[previous]), set(by_date[current])
    if (len(by_date[previous]) != 100 or len(previous_symbols) != 100 or
            len(by_date[current]) != 100 or len(current_symbols) != 100):
        raise ValueError("latest proxy member snapshots are not exactly 100 unique symbols")
    enter_count = len(current_symbols - previous_symbols)
    exit_count = len(previous_symbols - current_symbols)
    required = bool(enter_count or exit_count)
    return {
        "signal_date": current.isoformat(),
        "required": required,
        "enter_count": enter_count,
        "exit_count": exit_count,
        "label": f"名单调仓（调入 {enter_count}，调出 {exit_count}）" if required else "名单不变",
    }


def validate_final_nav(rows: list[dict], name: str) -> None:
    """Validate realized economics only; warm-up diagnostic NaNs are allowed."""
    cumulative = 1.0
    for row in rows:
        try:
            ret, nav = float(row["return_net"]), float(row["nav_net"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"Missing/invalid final return_net or nav_net: {name} {row.get('date')}") from exc
        if not math.isfinite(ret) or not math.isfinite(nav) or ret <= -1 or nav <= 0:
            raise ValueError(f"Nonfinite/invalid final return_net or nav_net: {name} {row.get('date')}")
        cumulative *= 1.0 + ret
        if not math.isfinite(cumulative) or not math.isclose(nav, cumulative, rel_tol=1e-9, abs_tol=1e-10):
            raise ValueError(f"Final NAV cumulative return mismatch: {name} {row.get('date')}")


def validate_final_signal(signal: dict, nav: dict, version: str) -> None:
    """Latest signal is a projection of the same-version final costed NAV."""
    active_holdings = {"0": "long_microcap_short_zz1000", "3": "long_microcap_short_zz1000",
                       "5": "long_microcap_top100"}
    if version not in active_holdings:
        raise ValueError(f"Unsupported final signal version: {version}")
    allowed_holdings = {"cash", active_holdings[version]}
    for signal_key, nav_key in (("current_holding", "holding"), ("next_holding", "next_holding")):
        holding = signal.get(signal_key)
        if holding not in allowed_holdings or holding != nav.get(nav_key):
            raise ValueError(f"v2.{version} final signal/NAV {signal_key} mismatch")
    for key, holding_key in (("current_execution_scale", "current_holding"),
                             ("next_session_actionable_scale", "next_holding")):
        try:
            scale, nav_scale = float(signal[key]), float(nav[key])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"v2.{version} missing/invalid final signal/NAV {key}") from exc
        if (not math.isfinite(scale) or not math.isfinite(nav_scale) or scale < 0 or
                not math.isclose(scale, nav_scale, rel_tol=0., abs_tol=1e-12) or
                scale != (0.0 if signal[holding_key] == "cash" else 1.0) or
                nav_scale != (0.0 if signal[holding_key] == "cash" else 1.0)):
            raise ValueError(f"v2.{version} final signal/NAV {key} mismatch")


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
        member_rebalance = canonical_member_rebalance(root, expected)
        if member_rebalance["signal_date"] != turnover["latest_date"]:
            errors.append("proxy members and turnover latest rebalance differ")
        for v, costed in COSTED.items():
            final_rows = {}
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
            if v == "3":
                params = summary.get("core_params", {})
                if (summary.get("strategy_revision") != V23_STRATEGY_REVISION or
                        params.get("signal_model", {}).get("r2_entry_gate") != 0 or
                        params.get("overheat_defense", {}).get("trigger_threshold") != .26 or
                        params.get("overheat_defense", {}).get("recovery_threshold") != .20):
                    errors.append("v2.3 summary plain revision mismatch")
            if v == "5":
                params = summary.get("core_params", {})
                signal_model = params.get("signal_model", {})
                if (summary.get("strategy_revision") != V25_STRATEGY_REVISION or
                        signal_model.get("lookback") != 20 or
                        signal_model.get("halflife") != 3.0 or
                        params.get("entry_threshold") != 0.0 or
                        params.get("exit_threshold") != 0.0 or
                        params.get("target_volatility_scaling", {}).get("enabled") is not False):
                    errors.append("v2.5 plain revision mismatch")
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
                final_rows[name] = rows
                if not name.endswith("latest_signal.csv"):
                    validate_final_nav(rows, name)
                if info["latest_date"] != expected:
                    errors.append(f"final date mismatch: {name}={info['latest_date']} expected={expected}")
                if v == "0" and not name.endswith("performance_nav.csv"):
                    if not all(plain_v20_identity(row) for row in rows):
                        errors.append(f"v2.0 plain revision/state mismatch: {name}")
                if v == "3" and not name.endswith("performance_nav.csv"):
                    if not all(plain_v23_identity(row) for row in rows):
                        errors.append(f"v2.3 plain revision/state mismatch: {name}")
                if v == "5" and not name.endswith("performance_nav.csv"):
                    if not all(plain_v25_identity(row) for row in rows):
                        errors.append(f"v2.5 plain revision/state mismatch: {name}")
                if name.endswith("latest_signal.csv"):
                    if len(rows) != 1 or rows[0].get("version") != f"2.{v}":
                        errors.append(f"v2.{v} final CSV identity mismatch")
                    # Member instructions must carry explicit dated action fields.
                    if any(rows[0].get(key) not in ("True", "False") for key in (
                            "member_rebalance_actionable", "member_rebalance_required", "member_rebalance_official")):
                        errors.append(f"v2.{v} missing explicit member action flags")
                    try:
                        member_fields_match = (
                            rows[0].get("member_rebalance_signal_date") == member_rebalance["signal_date"]
                            and rows[0].get("member_rebalance_required") == str(member_rebalance["required"])
                            and int(rows[0].get("member_enter_count", -1)) == member_rebalance["enter_count"]
                            and int(rows[0].get("member_exit_count", -1)) == member_rebalance["exit_count"]
                            and rows[0].get("member_rebalance_label") == member_rebalance["label"]
                        )
                    except (TypeError, ValueError):
                        member_fields_match = False
                    if not member_fields_match:
                        errors.append(f"v2.{v} final member counts differ from formal proxy-member lineage")
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
            costed_rows = final_rows[costed]
            performance_rows = final_rows[f"{prefix}_performance_nav.csv"]
            if (len(costed_rows) != len(performance_rows) or any(
                    left["date"] != right["date"] or
                    not math.isclose(float(left["return_net"]), float(right["return_net"]), rel_tol=0., abs_tol=1e-12)
                    for left, right in zip(costed_rows, performance_rows))):
                errors.append(f"v2.{v} costed NAV and performance stream differ")
            validate_final_signal(final_rows[f"{prefix}_latest_signal.csv"][0], costed_rows[-1], v)
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


def reusable_confirmed_today(root: Path, now: datetime | None = None) -> str | None:
    """Reuse at most 15 minutes of exact, independently proven TODAY's close.

    Yesterday's anchor, pre-close/future proofs and changed bytes never qualify.
    This is only a date-proof reuse; the complete artifact check still runs.
    """
    current = (now or datetime.now(timezone.utc)).astimezone(timezone(timedelta(hours=8)))
    try:
        manifest = json.loads((root / MANIFEST).read_text(encoding="utf-8"))
        proof = json.loads((root / state.REFRESH_PROOF_REL).read_text(encoding="utf-8"))
        code = (root / "microcap_top100_mom16_biweekly_live_v2_0.py").read_text(encoding="utf-8")
        match = re.search(r'^CN_CLOSE_CONFIRM_TIME = "(\d{2}):(\d{2})"', code, re.MULTILINE)
        if match is None:
            return None
        close_time = current.replace(hour=int(match[1]), minute=int(match[2]), second=0, microsecond=0)
        today = current.date().isoformat()
        if current < close_time or manifest.get("expected_date") != today:
            return None
        if proof.get("version") != state.REFRESH_PROOF_VERSION or proof.get("source") != state.REFRESH_PROOF_SOURCE:
            return None
        if proof.get("target_end_date") != today or proof.get("verified_on") != today:
            return None
        for stamp in (manifest.get("verified_at"), proof.get("verified_at_utc")):
            verified = datetime.fromisoformat(stamp)
            if verified.tzinfo is None or not close_time <= verified <= current:
                return None
            if current - verified > timedelta(minutes=15):
                return None
        report = validate_manifest(root, inspect_outputs(root, today))
        return today if report["ok"] else None
    except (OSError, ValueError, TypeError, KeyError):
        return None


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
            cached_target = reusable_confirmed_today(root)
            target = cached_target or independent_target(root)
            report = validate_manifest(root, inspect_outputs(root, target))
            report["date_proof_source"] = "unchanged_independent_today_close_proof_max_15min" if cached_target else "live_official_history_loader"
            report["release_sha"] = release
            report["errors"].extend(state.validate_state(root, max_anchor_age_days=5).get("errors", []))
            report["ok"] = not report["errors"]
    except Exception as exc:
        report = {"ok": False, "scope": "whole_workspace_delivery", "errors": [str(exc)]}
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
