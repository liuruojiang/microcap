"""Whole-workspace delivery gate. A base-state validation is not delivery success."""
from __future__ import annotations

import argparse
import copy
import csv
import hashlib
import json
import math
import os
import re
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import realtime_state_bundle as state
from scripts.top100_data_contracts import daily_session_date, validate_exchange_session_coverage
from scripts.restore_approved_top100_seed import BASE_FILES, COSTED
from scripts.st_notice_evidence import evidence_source_files

MANIFEST = "outputs/top100_delivery_manifest.json"
LOCK = "outputs/top100_delivery.lock"
BASE_PANEL = "microcap_top100_mom16_biweekly_live_v2_0_base_panel_refreshed.csv"
AUTHORITY = "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_frozen_tail_authority.json"
RELEASE_FILES = [
    AUTHORITY,
    "analyze_microcap_zz1000_hedge.py",
    "analyze_top100_rebalance_frequency.py",
    "microcap_top100_mom16_biweekly_live.py",
    "scripts/exchange_calendar.py",
    "scripts/realtime_state_bundle.py",
    "scripts/top100_cloud_delivery.py",
    "scripts/top100_delivery.py",
    "scripts/top100_data_contracts.py",
] + [f"microcap_top100_mom16_biweekly_live_v2_{v}.py" for v in COSTED]
RELEASE_FILES += evidence_source_files(Path(__file__).resolve().parents[1])
RETURN_BASIS_FILES = [
    "scripts/bounded_market_data.py",
    "scripts/stock_return_basis.py", "scripts/stock_return_basis_store.py",
    "scripts/stock_return_pipeline.py", "fetch_wind_microcap_index.py",
    "scripts/tradeability_evidence.py",
    "scripts/stock_input_contract.py",
    "scripts/st_notice_archive.py",
    "scripts/st_history_scope.py",
    "scripts/st_notice_incremental.py",
    "scripts/st_notice_refresh.py",
    "scripts/realtime_member_notice_audit.py",
    "scripts/index_history_preflight.py",
    "scripts/st_metadata_refresh.py",
    "scripts/stock_tail_returns.py",
    "scripts/share_change_source.py",
    "scripts/share_unchanged_tail.py",
    "data/share_source_future_plan_reviews.json",
    "scripts/sina_share_history.py",
    "scripts/stock_daily_inputs.py",
    "scripts/stock_daily_alternative.py",
]
RELEASE_FILES += RETURN_BASIS_FILES
from scripts.sse_share_daily_unchanged_20261104 import SOURCE_FILES as SSE_DATED_SHARE_SOURCE_FILES
RETURN_BASIS_FILES += SSE_DATED_SHARE_SOURCE_FILES
RELEASE_FILES += SSE_DATED_SHARE_SOURCE_FILES
RELEASE_FILES = list(dict.fromkeys(RELEASE_FILES))
V20_STRATEGY_REVISION = "plain_mom16_fixed1_20260904"
V23_STRATEGY_REVISION = "plain_lb25_hl2p5_r2off_no_overheat_20261029"
V25_STRATEGY_REVISION = "plain_lb20_hl3_entry0_exit0_20260905"
DAILY_STATE_ANCHORS = ("proxy_index", "costed_nav", "panel_shadow")


def plain_v23_identity(row: dict) -> bool:
    try:
        return (row.get("strategy_revision") == V23_STRATEGY_REVISION
                and str(row.get("target_vol_enabled")) == "False"
                and str(row.get("r2_gate_enabled")) == "False"
                and any(key in row for key in ("overheat_enabled", "overheat_overlay_enabled"))
                and all(str(row[key]) == "False" for key in ("overheat_enabled", "overheat_overlay_enabled")
                        if key in row)
                and str(row.get("cash_day_yield_enabled")) == "False"
                and str(row.get("financing_enabled")) == "False"
                and float(row.get("lookback", -1)) == 25.
                and float(row.get("halflife", -1)) == 2.5
                and float(row.get("r2_entry_gate", -1)) == 0.
                and float(row.get("signal_spread_hedge_ratio", -1)) == 1.
                and float(row.get("execution_hedge_ratio", -1)) == .8
                and abs(float(row.get("momentum_gap_exit_buffer", -1)) - .08) < 1e-12
                # NAV and final signal use these two established entry aliases.
                and any(key in row for key in ("entry_threshold", "momentum_gap_entry_threshold"))
                and all(float(row[key]) == 0. for key in ("entry_threshold", "momentum_gap_entry_threshold")
                        if key in row)
                and abs(float(row.get("overheat_trigger_threshold", -1)) - .26) < 1e-12
                and abs(float(row.get("overheat_recovery_threshold", -1)) - .20) < 1e-12
                and float(row.get("overheat_feature_window", -1)) == 10.)
    except (TypeError, ValueError):
        return False


def plain_v20_identity(row: dict) -> bool:
    """A version number alone cannot distinguish the retired v2.0 overlay."""
    try:
        return (row.get("strategy_revision") == V20_STRATEGY_REVISION
                and str(row.get("target_vol_enabled")) == "False"
                and str(row.get("overheat_enabled")) == "False"
                and ("lookback" not in row or float(row["lookback"]) == 16.)
                and ("fixed_hedge_ratio" not in row or float(row["fixed_hedge_ratio"]) == .8)
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
            and all(str(row[key]) == "False" for key in ("overheat_enabled", "overheat_overlay_enabled")
                    if key in row)
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
        fields = reader.fieldnames or []
        if len(fields) != len(set(fields)):
            raise ValueError(f"Duplicate CSV column names: {path.name}")
        if column not in (reader.fieldnames or []):
            raise ValueError(f"Missing {column}: {path.name}")
        rows = list(reader)
    if any(None in row or any(value is None for value in row.values()) for row in rows):
        raise ValueError(f"CSV row width differs from its header: {path.name}")
    dates = [daily_session_date(row[column]) for row in rows]
    if not dates or any(day is None for day in dates) or dates != sorted(set(dates)):
        raise ValueError(f"Empty, invalid, duplicate or unsorted dates: {path.name}")
    return {"rows": len(rows), "latest_date": dates[-1].isoformat(), "sha256": sha(path)}, rows


def input_hashes(root: Path) -> dict:
    names = [f"outputs/{name}" for name in (*BASE_FILES.values(), BASE_PANEL)]
    names += [AUTHORITY] + [f"microcap_top100_mom16_biweekly_live_v2_{v}.py" for v in COSTED]
    names += ["scripts/top100_data_contracts.py"]
    names += evidence_source_files(root)
    names += RETURN_BASIS_FILES
    return {name: (hashlib.sha256((root / name).read_bytes()).hexdigest()
                   if name.startswith("data/st_notice_evidence/") else sha(root / name))
            for name in names}


def canonical_member_rebalance(root: Path, expected: str) -> dict[str, object]:
    """Derive formal list changes from the delivered point-in-time member lineage."""
    path = root / "outputs" / BASE_FILES["proxy_members"]
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fields = reader.fieldnames or []
        if len(fields) != len(set(fields)):
            raise ValueError("proxy members contain duplicate CSV column names")
        rows = list(reader)
    if any(None in row or any(value is None for value in row.values()) for row in rows):
        raise ValueError("proxy members contain a row/header width mismatch")
    if not rows or any(key not in rows[0] for key in ("rebalance_date", "symbol", "rank")):
        raise ValueError("proxy members are missing rebalance_date/symbol/rank")
    target = state._parse_date(expected)
    if target is None:
        raise ValueError("invalid expected date for proxy-member lineage")
    by_date: dict[object, list[str]] = {}
    ranks_by_date: dict[object, list[str]] = {}
    for row in rows:
        day = state._parse_date(row.get("rebalance_date", ""))
        symbol = str(row.get("symbol", "")).strip().zfill(6)
        if day is None or re.fullmatch(r"[0-9]{6}", symbol) is None or symbol == "000000":
            raise ValueError("proxy members contain an invalid rebalance date or symbol")
        if day <= target:
            by_date.setdefault(day, []).append(symbol)
            ranks_by_date.setdefault(day, []).append(row["rank"])
    dates = sorted(by_date)
    if len(dates) < 2:
        raise ValueError("proxy members do not contain the latest two formal rebalances")
    previous, current = dates[-2:]
    previous_symbols, current_symbols = set(by_date[previous]), set(by_date[current])
    if (len(by_date[previous]) != 100 or len(previous_symbols) != 100 or
            len(by_date[current]) != 100 or len(current_symbols) != 100):
        raise ValueError("latest proxy member snapshots are not exactly 100 unique symbols")
    for day in (previous, current):
        try:
            ranks = [int(value) for value in ranks_by_date[day]]
        except (TypeError, ValueError) as exc:
            raise ValueError("latest proxy member snapshots contain invalid ranks") from exc
        if sorted(ranks) != list(range(1, 101)):
            raise ValueError("latest proxy member snapshots must contain unique ranks 1-100")
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
    if str(signal.get("version", "")) != f"2.{version}":
        raise ValueError(f"v2.{version} final version identity mismatch")
    if (version in {"3", "5"} or "strategy_version" in signal) and signal.get("strategy_version") != f"v2.{version}":
        raise ValueError(f"v2.{version} final strategy_version identity mismatch")
    if (signal.get("signal_timing") != "close_confirmed"
            or str(signal.get("official_close_confirmed_signal")) != "True"):
        raise ValueError(f"v2.{version} final signal is not an official close-confirmed signal")
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
    if signal.get("signal_label") != signal["next_holding"]:
        raise ValueError(f"v2.{version} final signal_label/next_holding mismatch")
    current, nxt = signal["current_holding"], signal["next_holding"]
    action = "hold" if current == nxt else ("open" if current == "cash" else "close")
    if signal.get("trade_state") != action:
        raise ValueError(f"v2.{version} final trade_state/holding transition mismatch")
    for key in ("effective_trade_state", "holding_trade_state", "momentum_trade_state"):
        if key in signal and signal[key] != action:
            raise ValueError(f"v2.{version} final {key}/holding transition mismatch")
    if "scale_trade_state" in signal and signal["scale_trade_state"] != "hold_scale":
        raise ValueError(f"v2.{version} final scale_trade_state/fixed-scale contract mismatch")
    for key, expected in (("position_transition", current != nxt), ("scale_trade_required", False)):
        if key in signal and str(signal[key]) != str(expected):
            raise ValueError(f"v2.{version} final {key}/holding transition mismatch")
    current_scale = float(signal["current_execution_scale"])
    next_scale = float(signal["next_session_actionable_scale"])
    aliases = {key: next_scale for key in ("target_position_scale", "next_session_target_scale",
                                          "raw_next_target_scale", "target_vol_scale_next_session")}
    aliases["execution_scale"] = current_scale
    aliases.update({key: next_scale - current_scale for key in (
        "raw_scale_delta", "actionable_scale_delta", "scale_delta", "position_scale_delta")})
    for key, expected in aliases.items():
        if key not in signal:
            continue
        try:
            actual = float(signal[key])
        except (TypeError, ValueError) as exc:
            raise ValueError(f"v2.{version} final {key}/execution scale mismatch") from exc
        if not math.isfinite(actual) or not math.isclose(actual, expected, rel_tol=0., abs_tol=1e-12):
            raise ValueError(f"v2.{version} final {key}/execution scale mismatch")
    for key in ("microcap_mom", "hedge_mom", "momentum_gap", "annualized_log_wls_score"):
        if key not in nav:
            continue
        try:
            actual, expected = float(signal[key]), float(nav[key])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"v2.{version} missing/invalid final signal {key}") from exc
        if (not math.isfinite(actual) or not math.isfinite(expected)
                or not math.isclose(actual, expected, rel_tol=0.0, abs_tol=1e-12)):
            raise ValueError(f"v2.{version} final signal/NAV {key} mismatch")


def inspect_outputs(root: Path, expected: str, *, new_generation: bool = False) -> dict:
    """Read written files, not stdout or a successful base-only report."""
    errors, streams, artifacts = [], {}, {}
    try:
        from scripts.exchange_calendar import sessions_for_day

        target_day = state._parse_date(expected)
        if target_day is None:
            raise ValueError(f"invalid expected exchange session: {expected}")
        # Whole delivery needs the actual-position price/share legs through
        # the target session; base-state structural readability is insufficient.
        errors.extend(state.validate_member_cache_transport(root, target_day))
        exchange_sessions = set(sessions_for_day(target_day))
        _, proxy_date_rows = csv_info(root / "outputs" / BASE_FILES["proxy_index"])
        strategy_start = state._parse_date(proxy_date_rows[0]["date"])

        def check_session_rows(name: str, rows: list[dict], column: str = "date", *, daily: bool = True) -> None:
            invalid = next((row[column] for row in rows
                            if state._parse_date(row[column]) >= strategy_start
                            and state._parse_date(row[column]) not in exchange_sessions), None)
            if invalid is not None:
                errors.append(f"non-exchange-session date in {name}: {invalid}")
            if daily:
                try:
                    validate_exchange_session_coverage(
                        (row[column] for row in rows), name, start=strategy_start,
                        end=target_day, sessions=exchange_sessions,
                    )
                except ValueError as exc:
                    errors.append(str(exc))

        inputs = input_hashes(root)
        for name in (BASE_PANEL, BASE_FILES["proxy_index"], BASE_FILES["costed_nav"]):
            info, rows = csv_info(root / "outputs" / name)
            streams[name] = info
            check_session_rows(name, rows)
            if info["latest_date"] != expected:
                errors.append(f"base date mismatch: {name}={info['latest_date']} expected={expected}")
        turnover, turnover_rows = csv_info(root / "outputs" / BASE_FILES["proxy_turnover"], "rebalance_date")
        streams[BASE_FILES["proxy_turnover"]] = turnover
        check_session_rows(BASE_FILES["proxy_turnover"], turnover_rows, "rebalance_date", daily=False)
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
                        params.get("lookback") != 16 or
                        params.get("fixed_hedge_ratio") != .8 or
                        params.get("momentum_gap_exit_buffer") != 0 or
                        params.get("target_volatility_scaling", {}).get("enabled") is not False or
                        params.get("overheat_defense", {}).get("enabled") is not False):
                    errors.append("v2.0 summary plain revision mismatch")
            if v == "3":
                params = summary.get("core_params", {})
                signal_model = params.get("signal_model", {})
                overheat = params.get("overheat_defense", {})
                if (summary.get("strategy_revision") != V23_STRATEGY_REVISION or
                        signal_model.get("lookback") != 25 or
                        signal_model.get("halflife") != 2.5 or
                        signal_model.get("r2_entry_gate") != 0 or
                        params.get("execution_hedge_ratio") != .8 or
                        overheat.get("enabled") is not False or
                        overheat.get("feature_window") != 10 or
                        overheat.get("trigger_threshold") != .26 or
                        overheat.get("recovery_threshold") != .20):
                    errors.append("v2.3 summary plain revision mismatch")
            if v == "5":
                params = summary.get("core_params", {})
                signal_model = params.get("signal_model", {})
                if (summary.get("strategy_revision") != V25_STRATEGY_REVISION or
                        signal_model.get("lookback") != 20 or
                        signal_model.get("halflife") != 3.0 or
                        params.get("entry_threshold") != 0.0 or
                        params.get("exit_threshold") != 0.0 or
                        params.get("overheat_overlay", {}).get("enabled") is not False or
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
                check_session_rows(name, rows, daily=not name.endswith("latest_signal.csv"))
                if not name.endswith("latest_signal.csv"):
                    validate_final_nav(rows, name)
                if info["latest_date"] != expected:
                    errors.append(f"final date mismatch: {name}={info['latest_date']} expected={expected}")
                if not name.endswith("performance_nav.csv") and any(row.get("version") != f"2.{v}" for row in rows):
                    errors.append(f"v2.{v} final stream version identity mismatch: {name}")
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
                    if v == "0" and (rows[0].get("lookback") != "16" or
                                     float(rows[0].get("fixed_hedge_ratio", -1)) != .8):
                        errors.append("v2.0 final lookback/hedge identity mismatch")
                    if v == "3" and rows[0].get("overheat_enabled") != "False":
                        errors.append("v2.3 final overheat enabled identity mismatch")
                    if v == "5" and rows[0].get("overheat_enabled") != "False":
                        errors.append("v2.5 final overheat disabled identity mismatch")
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
                            and rows[0].get("member_rebalance_state") == ("rebalance" if member_rebalance["required"] else "none")
                        )
                    except (TypeError, ValueError):
                        member_fields_match = False
                    if not member_fields_match:
                        errors.append(f"v2.{v} final member counts differ from formal proxy-member lineage")
                    required = member_rebalance["required"]
                    signal_date = state._parse_date(member_rebalance["signal_date"])
                    actionable = rows[0].get("member_rebalance_actionable") == "True"
                    should_act = required and signal_date == state._parse_date(expected)
                    if actionable != should_act or (required and rows[0].get("member_rebalance_official") != "True"):
                        errors.append(f"v2.{v} member actionability differs from formal lineage")
                    if required:
                        # Check the first next session independently of the CSV.
                        from scripts.exchange_calendar import sessions_for_day

                        execution = state._parse_date(rows[0].get("member_rebalance_execution_date", ""))
                        later = [day for day in sessions_for_day(signal_date) if day > signal_date]
                        if not later or execution != later[0]:
                            errors.append(f"v2.{v} member execution date is not the next exchange session")
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
    except (OSError, ValueError, KeyError, TypeError, RuntimeError) as exc:
        errors.append(str(exc))
        inputs = {}
    report = {"ok": not errors, "scope": "whole_workspace_delivery", "errors": errors,
              "expected_date": expected, "streams": streams, "inputs": inputs, "artifacts": artifacts}
    # A newly packed report must retain the distinction between the original
    # producer and a separately certified validator, rather than dropping it.
    try:
        saved_bytes = (root / MANIFEST).read_bytes()
        saved = json.loads(saved_bytes)
        if not isinstance(saved, dict):
            raise ValueError("Whole manifest is not an object")
    except (OSError, ValueError) as exc:
        if new_generation and (root / MANIFEST).exists():
            report["errors"].append(f"Unreadable predecessor whole manifest: {exc}")
            report["ok"] = False
        saved = {}
    if "source_validation_compatibility" in saved and "previous_source_validation" in saved:
        report["errors"].append("Ambiguous active and previous source validation")
        report["ok"] = False
    elif "source_validation_compatibility" in saved:
        try:
            if new_generation:
                # New data is checked by the complete whole/base/transport gates.
                # Authenticate the exact old receipt as ancestry, not as a rule
                # that tomorrow's inputs/outputs must equal yesterday's.
                previous = {"manifest_sha256": hashlib.sha256(saved_bytes).hexdigest(),
                            "manifest_utf8": saved_bytes.decode("utf-8")}
                report["previous_source_validation"] = validate_previous_source_validation(root, report, previous)
            else:
                report["source_validation_compatibility"] = validate_source_compatibility(root, report, saved)
        except (OSError, ValueError, KeyError, TypeError) as exc:
            report["errors"].append(f"invalid source validation compatibility: {exc}")
            report["ok"] = False
    elif "previous_source_validation" in saved:
        try:
            report["previous_source_validation"] = validate_previous_source_validation(
                root, report, saved["previous_source_validation"])
        except (OSError, ValueError, KeyError, TypeError) as exc:
            report["errors"].append(f"invalid previous source validation: {exc}")
            report["ok"] = False
    return report


def validate_previous_source_validation(root: Path, current: dict, previous: dict) -> dict:
    """Preserve and authenticate the exact predecessor without freezing new data."""
    if not isinstance(previous, dict) or set(previous) != {"manifest_sha256", "manifest_utf8"}:
        raise ValueError("Incomplete previous source validation")
    if not isinstance(previous["manifest_utf8"], str):
        raise ValueError("Invalid previous source manifest bytes")
    raw = previous["manifest_utf8"].encode("utf-8")
    if hashlib.sha256(raw).hexdigest() != previous["manifest_sha256"]:
        raise ValueError("Previous source manifest bytes differ")
    saved = json.loads(raw)
    if not isinstance(saved, dict):
        raise ValueError("Previous source manifest is not an object")
    if "previous_source_validation" in saved:
        raise ValueError("Nested previous source validation is not supported")
    validate_source_compatibility(root, saved, saved)
    prior_day = daily_session_date(saved["expected_date"])
    current_day = daily_session_date(current["expected_date"])
    if prior_day is None or current_day is None or current_day < prior_day:
        raise ValueError("Previous source validation is after the current delivery")
    return copy.deepcopy(previous)


def _require_source_only_transition(original: dict, current: dict, review: dict) -> None:
    """Only the four exact reviewed code hashes may differ from original D4."""
    if (original.get("status") != "complete" or original.get("ok") is not True
            or original.get("errors") != [] or current.get("ok") is not True
            or current.get("errors") != [] or original.get("scope") != "whole_workspace_delivery"
            or current.get("scope") != "whole_workspace_delivery" or "source_validation_compatibility" in original):
        raise ValueError("Source recertification requires complete original and current whole checks")
    for key in ("expected_date", "artifacts", "streams"):
        if key not in original or original[key] != current.get(key):
            raise ValueError("Source-only recertification changed " + key)
    if original["expected_date"] != review["expected_date"]:
        raise ValueError("Source-only recertification is limited to its reviewed session")
    old, new = original.get("inputs"), current.get("inputs")
    if (not isinstance(old, dict) or not isinstance(new, dict) or not old
            or set(old) != set(new) or not original["artifacts"] or not original["streams"]):
        raise ValueError("Source recertification requires identical complete input/artifact keys")
    changed = {name for name in old if old[name] != new[name]}
    if changed != set(review["changes"]) or any(
            (old[name], new[name]) != tuple(review["changes"][name]) for name in changed):
        raise ValueError("Source recertification has an unreviewed input/hash change")


def validate_source_compatibility(root: Path, current: dict, saved: dict) -> dict:
    """Authenticate portable provenance without trusting a claimed old SHA alone."""
    receipt = saved.get("source_validation_compatibility")
    keys = {"revision", "expected_date", "original_manifest_sha256", "original_manifest_utf8",
            "stock_fingerprint_sha256", "source_changes", "validated_at", "validation_release_sha"}
    if not isinstance(receipt, dict) or set(receipt) != keys:
        raise ValueError("Incomplete source recertification receipt")
    review = state.validation_source_review(root)
    if (receipt["revision"] != review["revision"] or receipt["expected_date"] != review["expected_date"]
            or receipt["original_manifest_sha256"] != review["manifest_sha256"]
            or receipt["stock_fingerprint_sha256"] != review["stock_fingerprint_sha256"]
            or receipt["source_changes"] != {name: list(pair) for name, pair in review["changes"].items()}
            or not isinstance(receipt["original_manifest_utf8"], str)
            or re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", str(receipt["validation_release_sha"])) is None):
        raise ValueError("Source recertification receipt differs from exact review")
    raw = receipt["original_manifest_utf8"].encode("utf-8")
    if hashlib.sha256(raw).hexdigest() != review["manifest_sha256"]:
        raise ValueError("Original producer manifest bytes differ from reviewed SHA")
    stamp = datetime.fromisoformat(receipt["validated_at"])
    close = datetime.fromisoformat(review["expected_date"] + "T15:00:00+08:00")
    if stamp.tzinfo is None or not close <= stamp <= datetime.now(timezone.utc):
        raise ValueError("Source recertification clock is naive/before session close/future")
    if saved.get("status") != "complete" or saved.get("ok") is not True or saved.get("errors") != []:
        raise ValueError("Source recertification manifest is incomplete")
    for key in ("expected_date", "inputs", "artifacts", "streams"):
        if key not in saved or saved[key] != current.get(key):
            raise ValueError("Certified source manifest/current mismatch: " + key)
    _require_source_only_transition(json.loads(raw), current, review)
    return copy.deepcopy(receipt)


def _recertification_raw_identity(root: Path, report: dict) -> dict:
    names = set(RELEASE_FILES) | set(report["inputs"]) | set(report["artifacts"])
    # This one-time source transition must also freeze nonmember inputs. The
    # ordinary whole report authenticates final streams/member transport; it is
    # not an inventory of every raw/share/ST/return-basis cache leaf.
    cache = root / ".microcap_index_cache"
    if cache.exists():
        if cache.is_symlink() or getattr(cache, "is_junction", lambda: False)():
            raise ValueError("Linked recertification cache")
        for directory, folders, files in os.walk(cache, followlinks=False):
            for name in folders:
                child = Path(directory) / name
                if child.is_symlink() or getattr(child, "is_junction", lambda: False)():
                    raise ValueError("Linked recertification cache directory: " + str(child))
            names.update((Path(directory) / name).relative_to(root).as_posix() for name in files)
    result = {}
    for name in sorted(names):
        path = root / name
        if not path.resolve().is_relative_to(root) or not path.is_file():
            raise ValueError("Missing or escaping recertification file: " + name)
        if any(parent.is_symlink() or getattr(parent, "is_junction", lambda: False)()
               for parent in (path, *path.parents) if parent.is_relative_to(root) and parent != root):
            raise ValueError("Linked recertification input: " + name)
        result[name] = state._sha256(path)
    return result


def recertify_validation_source(root: Path, expected_date: str) -> dict:
    """One exact, source-only adoption; never refresh data or rewrite history."""
    root = Path(root).resolve()
    lock = root / LOCK
    lock.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(lock, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
    except FileExistsError as exc:
        raise RuntimeError("Another group writer is active; source recertification refused") from exc
    lock_identity = os.fstat(descriptor).st_ino
    lock_body = str(os.getpid()).encode("ascii")
    retain_lock = False
    try:
        with os.fdopen(descriptor, "wb") as owner:
            owner.write(lock_body); owner.flush()
            review = state.validation_source_review(root)
            if expected_date != review["expected_date"] or independent_target(root) != expected_date:
                raise ValueError("Source recertification expected/latest completed session differs")
            original_bytes = (root / MANIFEST).read_bytes()
            saved = json.loads(original_bytes)
            already = "source_validation_compatibility" in saved
            if not already and hashlib.sha256(original_bytes).hexdigest() != review["manifest_sha256"]:
                raise ValueError("Source recertification original manifest is not the exact approved D4")
            release = verify_release(root)
            current = inspect_outputs(root, expected_date)
            if current.get("ok") is not True or current.get("errors") != []:
                raise ValueError("Source recertification whole check failed: " + str(current.get("errors")))
            if already:
                validate_source_compatibility(root, current, saved)
            else:
                _require_source_only_transition(saved, current, review)
            before = _recertification_raw_identity(root, current)
            base = validate_base_state_for_session(root, expected_date)
            if base.get("ok") is not True or base.get("errors") != []:
                raise ValueError("Source recertification base check failed: " + str(base.get("errors")))
            final = inspect_outputs(root, expected_date)
            for key in ("ok", "errors", "expected_date", "inputs", "artifacts", "streams"):
                if final.get(key) != current.get(key):
                    raise ValueError("Source recertification changed during validation: " + key)
            if (state.validation_source_review(root) != review
                    or _recertification_raw_identity(root, final) != before
                    or (root / MANIFEST).read_bytes() != original_bytes
                    or independent_target(root) != expected_date
                    or lock.stat().st_ino != lock_identity or lock.read_bytes() != lock_body):
                raise ValueError("Source recertification input/source/date/owner changed before commit")
            if already:
                validate_source_compatibility(root, final, saved)
                return {**saved, "recertification_action": "already_complete_revalidated"}
            receipt = {"revision": review["revision"], "expected_date": expected_date,
                       "original_manifest_sha256": review["manifest_sha256"],
                       "original_manifest_utf8": original_bytes.decode("utf-8"),
                       "stock_fingerprint_sha256": review["stock_fingerprint_sha256"],
                       "source_changes": {name: list(pair) for name, pair in review["changes"].items()},
                       "validated_at": datetime.now(timezone.utc).isoformat(), "validation_release_sha": release}
            result = {**saved, **final, "status": "complete", "release_sha": release,
                      "verified_at": receipt["validated_at"], "source_validation_compatibility": receipt}
            validate_source_compatibility(root, final, result)
            # Keep original bytes outside the delivered manifest before the
            # commit. A failed readback is not proof that replacement failed.
            manifest_path = root / MANIFEST
            backup = manifest_path.with_name("source_recertification_" +
                hashlib.sha256(original_bytes).hexdigest() + ".original")
            try:
                with backup.open("xb") as stream:
                    stream.write(original_bytes); stream.flush(); os.fsync(stream.fileno())
            except FileExistsError:
                pass
            if backup.is_symlink() or backup.read_bytes() != original_bytes:
                raise ValueError("Source recertification original backup differs")
            committed_bytes = json.dumps(result, ensure_ascii=False, indent=2).replace("\n", os.linesep).encode("utf-8")

            def owns_lock():
                return lock.stat().st_ino == lock_identity and lock.read_bytes() == lock_body

            try:
                write_manifest(root, result)
                if manifest_path.read_bytes() != committed_bytes:
                    raise ValueError("Source recertification manifest readback differs")
            except Exception as commit_error:
                recovery_error = None
                recovery_status = "original_preserved"
                try:
                    if not owns_lock():
                        raise RuntimeError("Source recertification owner changed after commit")
                    observed = manifest_path.read_bytes()
                    if observed != original_bytes:
                        if observed != committed_bytes:
                            raise RuntimeError("Manifest is not this transaction's committed object")
                        rollback = manifest_path.with_name(manifest_path.name + f".{os.getpid()}.rollback.tmp")
                        with rollback.open("xb") as stream:
                            stream.write(original_bytes); stream.flush(); os.fsync(stream.fileno())
                        if not owns_lock() or manifest_path.read_bytes() != observed:
                            raise RuntimeError("Owner/manifest changed before source recertification rollback")
                        rollback.replace(manifest_path)
                        recovery_status = "rolled_back"
                    if manifest_path.read_bytes() != original_bytes:
                        raise RuntimeError("Source recertification rollback readback differs")
                except Exception as exc:
                    recovery_status, recovery_error = "unknown", str(exc)
                    retain_lock = True
                    # Existing/replacement locks remain untouched. If the lock
                    # vanished, establish only an exclusive review blocker.
                    try:
                        with lock.open("xb") as blocker:
                            blocker.write(b"source-recertification-unknown; inspect original backup")
                            blocker.flush(); os.fsync(blocker.fileno())
                    except FileExistsError:
                        pass
                    except OSError as lock_error:
                        recovery_error += "; could not retain review lock: " + str(lock_error)
                failure = backup.with_name(backup.name + "." +
                    datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + ".failure.json")
                evidence = {"status": recovery_status, "original_backup": str(backup),
                            "original_sha256": hashlib.sha256(original_bytes).hexdigest(),
                            "attempted_sha256": hashlib.sha256(committed_bytes).hexdigest(),
                            "commit_error": str(commit_error), "recovery_error": recovery_error}
                try:
                    with failure.open("x", encoding="utf-8") as stream:
                        json.dump(evidence, stream, ensure_ascii=False, indent=2)
                        stream.flush(); os.fsync(stream.fileno())
                except OSError as evidence_error:
                    commit_error.add_note("Could not persist recovery evidence: " + str(evidence_error))
                if recovery_status == "unknown":
                    raise RuntimeError("Source recertification COMMIT_STATE_UNKNOWN; pack forbidden; "
                                       f"backup={backup}; recovery={recovery_error}") from commit_error
                commit_error.add_note(f"Source recertification {recovery_status}; evidence={failure}")
                raise
            return {**result, "recertification_action": "completed"}
    finally:
        # Never remove a replacement lock owned by another writer.
        if not retain_lock and lock.exists() and lock.stat().st_ino == lock_identity and lock.read_bytes() == lock_body:
            lock.unlink()


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
    if re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", remote) is None:
        raise ValueError("Invalid remote release object identity")
    try:
        subprocess.check_output(["git", "cat-file", "-e", f"{remote}^{{commit}}"],
                                cwd=root, stderr=subprocess.DEVNULL)
    except subprocess.CalledProcessError:
        # Fetch only the immutable object: never merge, checkout or alter dirty files.
        subprocess.run(["git", "fetch", "--no-tags", "origin", remote], cwd=root, check=True)
    names = tuple(RELEASE_FILES)
    if any(not isinstance(name, str) or not name or any(char in name for char in "\r\n\0")
           for name in names):
        raise ValueError("Invalid release path for Git batch protocol")
    requests = "".join(f"{remote}:{name}\n" for name in names).encode("utf-8")
    # One fixed-commit Git reader replaces one process per source file. Keep the
    # output on a temporary file, so all original evidence need not fit in RAM.
    # run() kills and reaps its own child on timeout; no reader survives a failure.
    with tempfile.TemporaryFile(mode="w+b") as objects:
        subprocess.run(["git", "cat-file", "--batch"], cwd=root, input=requests,
                       stdout=objects, stderr=subprocess.PIPE, check=True, timeout=300)
        total_bytes = objects.tell()
        objects.seek(0)
        for name in names:
            header = objects.readline(128)
            match = re.fullmatch(rb"([0-9a-f]{40}|[0-9a-f]{64}) blob ([0-9]+)\n", header)
            if match is None:
                raise ValueError(f"Missing/non-blob or invalid Git release response: {name}")
            identity, length = match[1], int(match[2])
            if length > total_bytes - objects.tell() - 1:
                raise ValueError(f"Truncated Git release blob: {name}")
            payload = objects.read(length)
            if len(payload) != length or objects.read(1) != b"\n":
                raise ValueError(f"Truncated Git release blob or invalid delimiter: {name}")
            algorithm = "sha1" if len(identity) == 40 else "sha256"
            actual = hashlib.new(algorithm, b"blob " + str(length).encode("ascii") + b"\0" + payload).hexdigest()
            if actual.encode("ascii") != identity:
                raise ValueError(f"Git release blob identity mismatch: {name}")
            if name.startswith("data/st_notice_evidence/"):
                matches = payload == (root / name).read_bytes()
            else:
                matches = hashlib.sha256(payload.replace(b"\r\n", b"\n")).hexdigest() == sha(root / name)
            if not matches:
                raise ValueError(f"Local core/authority differs from remote release: {name}")
        if objects.read(1):
            raise ValueError("Unexpected trailing Git release response")
    return remote


def write_manifest(root: Path, report: dict) -> None:
    path = root / MANIFEST
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)


def validate_base_state_for_session(root: Path, expected_date: str) -> dict:
    """Require current proof and exact latest-session anchors without a calendar-age cap."""
    report = state.validate_state(root, max_anchor_age_days=None,
                                  close_confirmed_session=state._parse_date(expected_date))
    errors = list(report.get("errors") or [])
    target_day = state._parse_date(expected_date)
    if target_day is None:
        errors.append(f"invalid expected exchange session: {expected_date}")
    else:
        errors.extend(state.validate_member_cache_transport(root, target_day))
    anchors = report.get("anchor_dates")
    if not isinstance(anchors, dict):
        errors.append("base state has no daily anchor dates")
    else:
        for name in DAILY_STATE_ANCHORS:
            actual = anchors.get(name)
            if actual != expected_date:
                errors.append(
                    f"{name} is not aligned with latest completed session: "
                    f"last_date={actual!r} expected_date={expected_date}"
                )
    report["errors"] = errors
    report["ok"] = report.get("ok") is True and not errors
    return report


def _refresh_all_unlocked(root: Path) -> dict:
    release = verify_release(root)
    write_manifest(root, {"status": "refreshing", "verified_at": datetime.now(timezone.utc).isoformat()})
    base_report = state.refresh_state(root, max_anchor_age_days=None)
    if not base_report.get("ok"):
        raise RuntimeError(f"Base refresh failed: {base_report.get('errors')}")
    target = independent_target(root)
    aligned_base = validate_base_state_for_session(root, target)
    if not aligned_base["ok"]:
        raise RuntimeError(f"Base state does not match latest completed session: {aligned_base.get('errors')}")
    before = input_hashes(root)
    for v in COSTED:
        subprocess.run([sys.executable, "-X", "utf8", f"microcap_top100_mom16_biweekly_live_v2_{v}.py"], cwd=root, check=True)
    if input_hashes(root) != before:
        raise RuntimeError("Shared inputs changed during generation; rerun whole delivery")
    report = inspect_outputs(root, target)
    if independent_target(root) != target:
        raise RuntimeError("Completed session advanced during generation; rerun whole delivery")
    final_base = validate_base_state_for_session(root, target)
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
    parser.add_argument("command", choices=("check", "refresh-all", "recertify-validation-source"))
    parser.add_argument("--expected-date")
    args = parser.parse_args(argv)
    if (args.command == "recertify-validation-source") != (args.expected_date is not None):
        parser.error("--expected-date is required only for recertify-validation-source")
    root = Path(__file__).resolve().parents[1]
    try:
        if args.command == "recertify-validation-source":
            report = recertify_validation_source(root, args.expected_date)
        elif args.command == "refresh-all":
            report = refresh_all(root)
        else:
            release = verify_release(root)
            cached_target = reusable_confirmed_today(root)
            target = cached_target or independent_target(root)
            report = validate_manifest(root, inspect_outputs(root, target))
            report["errors"].extend(state.validate_reference_summary(root, state._parse_date(target)))
            report["date_proof_source"] = "unchanged_independent_today_close_proof_max_15min" if cached_target else "live_official_history_loader"
            report["release_sha"] = release
            report["errors"].extend(validate_base_state_for_session(root, target).get("errors", []))
            report["ok"] = not report["errors"]
    except Exception as exc:
        report = {"ok": False, "scope": "whole_workspace_delivery", "errors": [str(exc)]}
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
