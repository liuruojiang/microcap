from __future__ import annotations

import argparse
import csv
import json
import math
import os
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo


BJ = ZoneInfo("Asia/Shanghai")


def now_bj() -> datetime:
    return datetime.now(BJ)


def clean_output(text: str, max_len: int = 45000) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n").strip()
    text = re.sub(r"\n{4,}", "\n\n\n", text)
    if len(text) > max_len:
        return text[: max_len - 200].rstrip() + "\n\n[输出过长，后续内容已截断。完整原始输出见 workflow artifact。]"
    return text


def extract_line(text: str, pattern: str) -> str:
    match = re.search(pattern, text, flags=re.I | re.M)
    return match.group(0).strip() if match else ""


def extract_value(text: str, key: str) -> str:
    line = extract_line(text, rf"^{re.escape(key)}\s*:[^\n]*")
    if ":" not in line:
        return ""
    return line.split(":", 1)[1].strip()


def parse_iso_date(value: str) -> date | None:
    match = re.search(r"\d{4}-\d{2}-\d{2}", value.strip())
    if not match:
        return None
    try:
        return date.fromisoformat(match.group(0))
    except ValueError:
        return None


def previous_weekday(value: date) -> date:
    value = value - timedelta(days=1)
    while value.weekday() >= 5:
        value = value - timedelta(days=1)
    return value


def classify_signal_output(
    output: str,
    exit_code: str,
    publication_mode: str = "realtime",
) -> tuple[str, str]:
    code = exit_code.strip().lower()
    if code and code not in {"0", "none", "unknown"}:
        return "FAILED", f"script exit code is {exit_code}"
    if "preflight_failed" in output:
        reason = extract_value(output, "reason")
        if reason:
            return "FAILED", reason
        refresh_code = extract_value(output, "refresh_exit_code")
        if refresh_code:
            return "FAILED", f"state refresh failed with exit code {refresh_code}"
        return "FAILED", "state refresh failed before realtime signal ran"
    expected_marker = "realtime_signal" if publication_mode == "realtime" else "signal"
    if re.search(rf"^{re.escape(expected_marker)}\s*$", output, flags=re.M) is None:
        return "FAILED", f"{expected_marker} marker is missing"

    anchor = parse_iso_date(extract_value(output, "latest_anchor_trade_date"))
    quote_trade_date = parse_iso_date(extract_value(output, "quote_trade_date"))
    if anchor and quote_trade_date and anchor < quote_trade_date:
        expected_anchor = previous_weekday(quote_trade_date)
        if anchor < expected_anchor:
            return "STALE", f"anchor {anchor.isoformat()} is older than expected {expected_anchor.isoformat()}"
    return "OK", ""


def worst_status(statuses: list[str]) -> str:
    if any(status == "FAILED" for status in statuses):
        return "FAILED"
    if any(status == "STALE" for status in statuses):
        return "STALE"
    return "OK"


def extract_signal_summary(output: str) -> str:
    keys = [
        "strategy_version",
        "base_version",
        "signal_model",
        "overlay",
        "snapshot_time",
        "latest_anchor_trade_date",
        "quote_trade_date",
        "current_holding",
        "next_holding",
        "microcap_mom",
        "hedge_mom",
        "momentum_gap",
        "trade_state",
        "holding_trade_state",
        "scale_trade_state",
        "current_execution_scale",
        "target_vol_current_execution_scale",
        "target_vol_next_execution_scale",
        "official_close_confirmed_signal",
        "annualized_log_wls_score",
        "log_wls_r2",
        "quote_source",
        "quote_coverage",
    ]
    lines = []
    for key in keys:
        line = extract_line(output, rf"^{re.escape(key)}[^\n]*")
        if line:
            lines.append(f"- {line}")
    if lines:
        return "\n".join(lines)
    return "详见附件中的原始实时信号输出。"


def split_version_spec(spec: str, default_version: str) -> tuple[str, str]:
    if "=" not in spec:
        return default_version, spec
    version, value = spec.split("=", 1)
    version = version.strip()
    if not version:
        raise ValueError(f"empty version in spec: {spec!r}")
    return version, value.strip()


def parse_exit_codes(specs: list[str]) -> tuple[dict[str, str], str]:
    mapped: dict[str, str] = {}
    default = ""
    for spec in specs:
        version, value = split_version_spec(spec, "")
        if version:
            mapped[version] = value
        else:
            default = value
    return mapped, default


def parse_output_fields(output: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for match in re.finditer(
        r"^(?P<key>[A-Za-z0-9_]+)[ \t]*:[ \t]*(?P<value>[^\n]*)$",
        output,
        flags=re.M,
    ):
        fields[match.group("key")] = match.group("value").strip()
    return fields


def read_last_csv_row(path_value: str) -> tuple[dict[str, str], str]:
    if not path_value:
        return {}, "final signal CSV is required"
    path = Path(path_value)
    if not path.exists():
        return {}, "final signal CSV is missing"
    if not path.is_file():
        return {}, "final signal CSV is unreadable"
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            rows = list(csv.DictReader(handle))
    except (OSError, UnicodeError, csv.Error) as exc:
        return {}, f"final signal CSV is unreadable: {type(exc).__name__}"
    if not rows:
        return {}, "final signal CSV is empty"
    row = {str(key): str(value).strip() for key, value in rows[-1].items() if key and value is not None}
    if not row or not any(row.values()):
        return {}, "final signal CSV is empty"
    return row, ""


def parse_signal_csv_specs(specs: list[str]) -> dict[str, str]:
    mapped: dict[str, str] = {}
    for spec in specs:
        version, value = split_version_spec(spec, "")
        if not version:
            raise ValueError("--signal-csv values must use version=path format")
        mapped[version] = value
    return mapped


def first_value(fields: dict[str, str], *keys: str) -> str:
    for key in keys:
        value = fields.get(key, "").strip()
        if value and value.lower() not in {"nan", "none"}:
            return value
    return ""


def parse_number(value: str) -> float | None:
    cleaned = value.strip().replace(",", "")
    if not cleaned or cleaned.lower() in {"nan", "none"}:
        return None
    is_percent = cleaned.endswith("%")
    if is_percent:
        cleaned = cleaned[:-1]
    try:
        number = float(cleaned)
    except ValueError:
        return None
    if not math.isfinite(number):
        return None
    return number / 100.0 if is_percent else number


def parse_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_strict_bool(value: str) -> bool | None:
    normalized = value.strip().lower()
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    return None


STRATEGY_IDENTITIES: dict[str, dict[str, dict[str, object]]] = {
    "v2.0": {
        "text": {
            "version": "2.0",
            "overlay_type": "plain_mom16_fixed1_20260904",
            "strategy_revision": "plain_mom16_fixed1_20260904",
        },
        "bool": {
            "overheat_enabled": False,
            "overheat_require_signal_reset": False,
            "blocked_until_signal_reset": False,
            "target_vol_enabled": False,
        },
        "number": {
            "overheat_window": 0.0,
            "overheat_threshold": 0.0,
            "target_vol": 0.0,
            "target_vol_window": 0.0,
            "max_leverage": 1.0,
            "lookback": 16.0,
            "momentum_gap_exit_buffer": 0.0,
            "fixed_hedge_ratio": 0.8,
        },
    },
    "v2.3": {
        "text": {
            "version": "2.3",
            "strategy_version": "v2.3",
            "overlay_type": "plain_lb25_hl2p5_r2off_vol10_26_20_20260904",
            "strategy_revision": "plain_lb25_hl2p5_r2off_vol10_26_20_20260904",
            "signal_model": "spread_nav_log_wls_exp_halflife_2p5_lb25_r2off_signal1p0_exec0p8_vol10_overheat26_recovery20",
        },
        "bool": {
            "overheat_enabled": True,
            "target_vol_enabled": False,
            "r2_gate_enabled": False,
            "cash_day_yield_enabled": False,
            "financing_enabled": False,
        },
        "number": {
            "lookback": 25.0,
            "halflife": 2.5,
            "r2_entry_gate": 0.0,
            "signal_spread_hedge_ratio": 1.0,
            "momentum_gap_entry_threshold": 0.0,
            "momentum_gap_exit_buffer": 0.08,
            "execution_hedge_ratio": 0.8,
            "overheat_feature_window": 10.0,
            "overheat_trigger_threshold": 0.26,
            "overheat_recovery_threshold": 0.20,
            "target_vol": 0.0,
            "target_vol_window": 0.0,
        },
    },
    "v2.5": {
        "text": {
            "version": "2.5",
            "strategy_version": "v2.5",
            "overlay_type": "plain_lb20_hl3_entry0_exit0_20260905",
            "strategy_revision": "plain_lb20_hl3_entry0_exit0_20260905",
            "signal_model": "microcap_only_log_wls_exp_halflife_3p0_lb20_entry0_exit0_no_targetvol",
        },
        "bool": {
            "hedge_removed": True,
            "overheat_enabled": False,
            "target_vol_enabled": False,
            "cash_day_yield_enabled": False,
            "financing_enabled": False,
        },
        "number": {
            "signal_spread_hedge_ratio": 0.0,
            "execution_hedge_ratio": 0.0,
            "fixed_hedge_ratio": 0.0,
            "lookback": 20.0,
            "halflife": 3.0,
            "entry_threshold": 0.0,
            "exit_threshold": 0.0,
            "target_vol": 0.0,
            "target_vol_window": 0.0,
        },
    },
}


def validate_strategy_identity(version: str, fields: dict[str, str]) -> tuple[bool, str]:
    identity = STRATEGY_IDENTITIES.get(version)
    if identity is None:
        return False, f"unsupported strategy version {version}"

    mismatches: list[str] = []
    for key, expected in identity["text"].items():
        actual = first_value(fields, key)
        if actual != expected:
            actual_display = escape_markdown_inline(actual) if actual else "<missing>"
            mismatches.append(f"{key} expected {expected}, got {actual_display}")

    for key, expected in identity["bool"].items():
        actual = first_value(fields, key)
        normalized = actual.lower()
        if normalized in {"1", "true", "yes", "y", "on"}:
            actual_bool: bool | None = True
        elif normalized in {"0", "false", "no", "n", "off"}:
            actual_bool = False
        else:
            actual_bool = None
        if actual_bool is not expected:
            actual_display = escape_markdown_inline(actual) if actual else "<missing>"
            mismatches.append(f"{key} expected {expected}, got {actual_display}")

    for key, expected in identity["number"].items():
        actual = first_value(fields, key)
        actual_number = parse_number(actual)
        if actual_number is None or not math.isclose(actual_number, float(expected), rel_tol=0.0, abs_tol=1e-9):
            actual_display = escape_markdown_inline(actual) if actual else "<missing>"
            mismatches.append(f"{key} expected {expected:g}, got {actual_display}")

    if mismatches:
        return False, "strategy identity mismatch: " + "; ".join(mismatches[:3])
    return True, ""


def validate_member_action_contract(fields: dict[str, str]) -> tuple[bool, str]:
    parsed: dict[str, bool] = {}
    for key in (
        "member_rebalance_required",
        "member_rebalance_actionable",
        "member_rebalance_official",
    ):
        raw = fields.get(key, "").strip()
        value = parse_strict_bool(raw)
        if value is None:
            actual = escape_markdown_inline(raw) if raw else "<missing>"
            return False, f"member action contract invalid: {key} must be True or False, got {actual}"
        parsed[key] = value

    required = parsed["member_rebalance_required"]
    actionable = parsed["member_rebalance_actionable"]
    official = parsed["member_rebalance_official"]
    if actionable and not required:
        return False, "member action contract invalid: actionable=True requires required=True"
    if actionable and not official:
        return False, "member action contract invalid: actionable=True requires official=True"
    if actionable:
        signal_raw = first_value(fields, "member_rebalance_signal_date")
        execution_raw = first_value(fields, "member_rebalance_execution_date")
        signal_date = parse_strict_iso_date(signal_raw)
        execution_date = parse_strict_iso_date(execution_raw)
        if signal_date is None:
            actual = escape_markdown_inline(signal_raw) if signal_raw else "<missing>"
            return False, f"member action contract invalid: invalid signal date {actual}"
        if execution_date is None:
            actual = escape_markdown_inline(execution_raw) if execution_raw else "<missing>"
            return False, f"member action contract invalid: invalid execution date {actual}"
        if execution_date <= signal_date:
            return False, "member action contract invalid: execution date must be after signal date"
    return True, ""


def validate_publication_contract(
    publication_mode: str,
    fields: dict[str, str],
) -> tuple[bool, str]:
    actionable = parse_strict_bool(first_value(fields, "member_rebalance_actionable")) is True
    if actionable:
        signal_date = parse_strict_iso_date(first_value(fields, "member_rebalance_signal_date"))
        execution_date = parse_strict_iso_date(first_value(fields, "member_rebalance_execution_date"))
        publication_date = parse_strict_iso_date(first_value(
            fields, "date" if publication_mode == "close_confirmed" else "quote_trade_date"
        ))
        action_date = signal_date if publication_mode == "close_confirmed" else execution_date
        if publication_date is None or action_date != publication_date:
            return False, "member action contract invalid: actionable date does not match publication session"
    if publication_mode != "close_confirmed":
        quote = parse_strict_iso_date(first_value(fields, "quote_trade_date"))
        anchor = parse_strict_iso_date(first_value(fields, "latest_anchor_trade_date"))
        expected_anchor = parse_strict_iso_date(first_value(fields, "expected_latest_completed_trade_date"))
        if quote != now_bj().date() or parse_strict_iso_date(first_value(fields, "date")) != quote:
            return False, "publication contract invalid: realtime final CSV must match today's quote session"
        if anchor is None or expected_anchor is None or anchor != expected_anchor or anchor >= quote:
            return False, "publication contract invalid: realtime final CSV anchor must match the verified previous completed session"
        try:
            snapshot = datetime.fromisoformat(first_value(fields, "snapshot_time"))
        except ValueError:
            return False, "publication contract invalid: invalid final CSV snapshot_time"
        if snapshot.tzinfo is None or snapshot.astimezone(BJ).date() != quote:
            return False, "publication contract invalid: final CSV snapshot must be timezone-aware and match quote session"
        if (parse_strict_bool(first_value(fields, "official_close_confirmed_signal")) is not False
                or first_value(fields, "signal_timing") != "intraday_hypothetical_if_now_close"):
            return False, "publication contract invalid: realtime cannot use a close-confirmed final CSV"
        return True, ""
    if parse_strict_bool(first_value(fields, "official_close_confirmed_signal")) is not True:
        return False, "publication contract invalid: official_close_confirmed_signal must be True"
    if first_value(fields, "signal_timing") != "close_confirmed":
        return False, "publication contract invalid: signal_timing must be close_confirmed"
    if parse_strict_iso_date(first_value(fields, "date")) is None:
        return False, "publication contract invalid: final CSV date must be YYYY-MM-DD"
    return True, ""


def validate_execution_contract(version: str, fields: dict[str, str]) -> tuple[bool, str]:
    active_holding = "long_microcap_top100" if version == "v2.5" else "long_microcap_short_zz1000"
    for holding_key, scale_key in (
        ("current_holding", "current_execution_scale"),
        ("next_holding", "next_session_actionable_scale"),
    ):
        holding = first_value(fields, holding_key)
        if holding not in {"cash", active_holding}:
            return False, f"final CSV execution contract invalid: {holding_key} missing or unsupported"
        scale = parse_number(first_value(fields, scale_key))
        expected = 0.0 if holding == "cash" else 1.0
        if scale is None or not math.isclose(scale, expected, rel_tol=0.0, abs_tol=1e-9):
            return False, f"final CSV execution contract invalid: {scale_key} must be finite and match fixed holding scale {expected:g}"
    return True, ""


def parse_strict_iso_date(value: str) -> date | None:
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value.strip()):
        return None
    try:
        return date.fromisoformat(value.strip())
    except ValueError:
        return None


def format_percent(value: str, *, signed: bool = True) -> str:
    number = parse_number(value)
    if number is None:
        return "N/A"
    return f"{number:+.2%}" if signed else f"{number:.2%}"


def format_r2(value: str) -> str:
    number = parse_number(value)
    return "N/A" if number is None else f"{number:.3f}"


def format_scale(fields: dict[str, str]) -> str:
    value = first_value(
        fields,
        "next_session_actionable_scale",
        "target_vol_next_execution_scale",
        "next_session_target_scale",
        "target_position_scale",
        "current_execution_scale",
        "execution_scale",
    )
    number = parse_number(value)
    return "N/A" if number is None else f"{number:.2f}"


HOLDING_LABELS = {
    "cash": "空仓",
    "long_microcap_short_zz1000": "微盘 Top100＋空头中证1000",
    "long_microcap_top100": "微盘 Top100",
}


def format_holding(value: str) -> str:
    normalized = value.strip()
    return HOLDING_LABELS.get(normalized, normalized or "未知")


def escape_markdown_inline(value: str) -> str:
    flattened = re.sub(r"\s*\n\s*", " ", value.replace("\r\n", "\n").replace("\r", "\n"))
    return re.sub(r"([\\`*_\[\]<>#])", r"\\\1", flattened)


def member_rebalance_action(fields: dict[str, str]) -> str:
    actionable = parse_strict_bool(first_value(fields, "member_rebalance_actionable"))
    official = parse_strict_bool(first_value(fields, "member_rebalance_official"))
    signal_date = first_value(fields, "member_rebalance_signal_date")
    execution_date = first_value(fields, "member_rebalance_execution_date")
    if not actionable or not official or not signal_date or not execution_date:
        return ""

    label = first_value(fields, "member_rebalance_label")
    if not label:
        enter_count = first_value(fields, "member_enter_count") or "0"
        exit_count = first_value(fields, "member_exit_count") or "0"
        label = f"名单调仓（调入 {enter_count}，调出 {exit_count}）"
    date_text = f"信号日 {signal_date}，执行日 {execution_date}"
    if label.startswith("名单调仓（") and label.endswith("）"):
        return label[:-1] + f"；{date_text}）"
    return f"{label}（{date_text}）"


def action_label(item: dict[str, object]) -> str:
    if item["status"] != "OK":
        return "异常"
    fields = item["fields"]
    assert isinstance(fields, dict)
    current = first_value(fields, "current_holding", "holding")
    next_holding = first_value(fields, "next_holding")
    if current and next_holding and current != next_holding:
        if current == "cash" and next_holding != "cash":
            return "开仓"
        if current != "cash" and next_holding == "cash":
            return "平仓"
        return "调仓"

    actions: list[str] = []
    current_scale = parse_number(first_value(fields, "current_execution_scale"))
    next_scale = parse_number(first_value(fields, "next_session_actionable_scale"))
    if current_scale is not None and next_scale is not None and not math.isclose(current_scale, next_scale, rel_tol=0, abs_tol=1e-9):
        actions.append("调整仓位")
    member_action = member_rebalance_action(fields)
    if member_action:
        actions.append(member_action)
    return "；".join(actions) or "无操作"


def momentum_text(version: str, fields: dict[str, str]) -> str:
    if version == "v2.0":
        values = [
            ("微盘", first_value(fields, "microcap_mom")),
            ("对冲", first_value(fields, "hedge_mom")),
            ("动量差", first_value(fields, "momentum_gap")),
        ]
        rendered = [f"{label} **{format_percent(value)}**" for label, value in values if value]
        return "；".join(rendered) or "N/A"

    score = first_value(fields, "annualized_log_wls_score", "momentum_gap")
    r2 = first_value(fields, "log_wls_r2")
    if version == "v2.3":
        label = "对冲价差年化 WLS 得分"
    elif version == "v2.5":
        label = "微盘年化 WLS 得分"
    else:
        label = "年化 WLS 得分"
    parts = []
    if score:
        parts.append(f"{label} **{format_percent(score)}**")
    if r2:
        parts.append(f"R² **{format_r2(r2)}**")
    return "；".join(parts) or "N/A"


def humanize_status_note(status: str, note: str) -> str:
    if status == "STALE":
        match = re.search(r"anchor (\d{4}-\d{2}-\d{2}) is older than expected (\d{4}-\d{2}-\d{2})", note)
        if match:
            return f"数据过期，锚点 {match.group(1)} 早于应有日期 {match.group(2)}"
        return f"数据过期：{note}"
    if "state refresh failed" in note.lower():
        return "状态刷新失败，实时信号未运行"
    match = re.search(r"script exit code is ([^\s]+)", note)
    if match:
        return f"脚本运行失败，退出码 {match.group(1)}"
    if "marker is missing" in note.lower():
        return "未生成实时信号标记"
    if note.startswith("required signal fields are missing:"):
        fields = note.split(":", 1)[1].strip()
        return f"缺少必要信号字段：{fields}"
    if note.startswith("strategy identity mismatch:"):
        detail = note.split(":", 1)[1].strip()
        return f"策略身份不匹配：{detail}"
    if note.startswith("final signal CSV"):
        details = {
            "final signal CSV is required": "缺少最终实时信号 CSV",
            "final signal CSV is missing": "最终实时信号 CSV 不存在",
            "final signal CSV is empty": "最终实时信号 CSV 为空",
            "final signal CSV is unreadable": "最终实时信号 CSV 无法读取",
        }
        return details.get(note, f"最终实时信号 CSV 无法读取：{note.split(':', 1)[-1].strip()}")
    if note.startswith("member action contract invalid:"):
        detail = note.split(":", 1)[1].strip()
        return f"成员调仓契约无效：{detail}"
    return f"运行失败：{note}" if note else "运行失败"


def risk_warnings(item: dict[str, object]) -> list[str]:
    version = str(item["version"])
    status = str(item["status"])
    note = str(item["status_note"])
    fields = item["fields"]
    assert isinstance(fields, dict)
    if status != "OK":
        return [f"{version}：{humanize_status_note(status, note)}"]

    warnings: list[str] = []
    fallback_warning = first_value(fields, "fallback_warning")
    validated_state_only_cache = (
        "production state-only mode avoids implicit cache rebuilds" in fallback_warning
        and first_value(fields, "expected_latest_completed_trade_date_source")
        == "independent_close_history_refresh"
        and first_value(fields, "latest_anchor_trade_date")
        == first_value(fields, "expected_latest_completed_trade_date")
        and bool(first_value(fields, "latest_anchor_trade_date"))
    )
    if fallback_warning and not validated_state_only_cache:
        warnings.append(f"{version}：行情使用回退数据（{escape_markdown_inline(fallback_warning)}）")

    if version == "v2.0" and parse_bool(first_value(fields, "blocked_until_signal_reset")):
        metric = first_value(fields, "overheat_metric")
        threshold = first_value(fields, "overheat_threshold")
        detail = "过热退出后锁定，等待基础信号重置"
        if metric and threshold:
            detail += f"；当前指标 {format_percent(metric, signed=False)}，触发线 {format_percent(threshold, signed=False)}"
        warnings.append(f"v2.0：{detail}")

    if version == "v2.3" and parse_bool(first_value(fields, "overheat_risk_off")):
        value = first_value(fields, "overheat_feature_value")
        trigger = first_value(fields, "overheat_trigger_threshold")
        recovery = first_value(fields, "overheat_recovery_threshold")
        detail = "过热风险关闭中"
        if value and trigger:
            detail += f"；当前指标 {format_percent(value, signed=False)}，触发线 {format_percent(trigger, signed=False)}"
        if recovery:
            detail += f"，恢复线 {format_percent(recovery, signed=False)}"
        warnings.append(f"v2.3：{detail}")
    return warnings


def subject_tag(results: list[dict[str, object]], publication_mode: str = "realtime") -> str:
    if any(item["status"] != "OK" for item in results):
        return "异常"
    prefix = "下个交易日" if publication_mode == "close_confirmed" else ""
    if any(action_label(item) != "无操作" for item in results):
        return prefix + "需操作"
    return prefix + "无需操作"


def conclusion_text(results: list[dict[str, object]], publication_mode: str = "realtime") -> str:
    if any(item["status"] != "OK" for item in results):
        return "存在异常版本，请勿执行异常版本信号。"
    actionable = [(str(item["version"]), action_label(item)) for item in results if action_label(item) != "无操作"]
    if publication_mode == "close_confirmed":
        if not actionable:
            return "本次收盘信号未提出下个交易日的新调仓。"
        phrases = [f"{escape_markdown_inline(version)} 需要{escape_markdown_inline(action)}" for version, action in actionable]
        suffix = "；其他版本无需新增调仓。" if len(actionable) < len(results) else "。"
        return "下个交易日计划：" + "；".join(phrases) + suffix
    if not actionable:
        return "所有版本均无需调仓。"
    phrases = [f"{escape_markdown_inline(version)} 需要{escape_markdown_inline(action)}" for version, action in actionable]
    if len(actionable) < len(results):
        return "；".join(phrases) + "；其他版本无需调仓。"
    return "；".join(phrases) + "。"


def shared_data_line(
    results: list[dict[str, object]],
    strategy_sha: str,
    publication_mode: str = "realtime",
) -> str:
    fields_list = [item["fields"] for item in results]
    if publication_mode == "close_confirmed":
        close_dates = [first_value(fields, "date") for fields in fields_list if isinstance(fields, dict)]
        close_dates = [value for value in close_dates if value]
        data_value = escape_markdown_inline(max(close_dates)) if close_dates else "未记录"
        line = f"收盘确认日期：{data_value}"
        if strategy_sha:
            line += f"｜策略代码：{escape_markdown_inline(strategy_sha)}"
        return line

    snapshots = [first_value(fields, "snapshot_time") for fields in fields_list if isinstance(fields, dict)]
    snapshots = [value for value in snapshots if value]
    quote_dates = [first_value(fields, "quote_trade_date") for fields in fields_list if isinstance(fields, dict)]
    quote_dates = [value for value in quote_dates if value]
    coverages = [first_value(fields, "quote_coverage") for fields in fields_list if isinstance(fields, dict)]
    coverages = [value for value in coverages if value]

    if snapshots:
        data_value = escape_markdown_inline(max(snapshots))
    elif quote_dates:
        data_value = escape_markdown_inline(max(quote_dates))
    else:
        data_value = "未记录"
    line = f"数据时间：{data_value}"
    if coverages and len(set(coverages)) == 1:
        line += f"｜报价覆盖：{escape_markdown_inline(coverages[0])}"
    if strategy_sha:
        line += f"｜策略代码：{escape_markdown_inline(strategy_sha)}"
    return line


def holdings_summary_text(results: list[dict[str, object]]) -> str:
    summaries: list[str] = []
    for item in results:
        version = str(item["version"])
        if item["status"] != "OK":
            summaries.append(f"{version}：持仓与下一交易日可执行仓位不可用")
            continue
        fields = item["fields"]
        assert isinstance(fields, dict)
        current = escape_markdown_inline(format_holding(first_value(fields, "current_holding", "holding")))
        next_holding = escape_markdown_inline(format_holding(first_value(fields, "next_holding")))
        summaries.append(
            f"{escape_markdown_inline(version)}：{current} → {next_holding}，下一交易日可执行仓位 {format_scale(fields)}"
        )
    return "；".join(summaries) + "。"


def escape_table_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")


def build_compact_digest(
    results: list[dict[str, object]],
    *,
    date_s: str,
    run_url: str,
    strategy_sha: str,
    subject_prefix: str = "",
    publication_mode: str = "realtime",
) -> tuple[str, str]:
    versions = "/".join(str(item["version"]) for item in results)
    tag = subject_tag(results, publication_mode)
    action_heading = "下个交易日操作" if publication_mode == "close_confirmed" else "今日操作"
    lines = [
        "## 今日结论",
        "",
        f"**信号类型：{'收盘确认' if publication_mode == 'close_confirmed' else '盘中实时'}**",
        "",
        f"**{holdings_summary_text(results)}**",
        "",
        f"**{conclusion_text(results, publication_mode)}**",
        "",
    ]
    if publication_mode == "close_confirmed":
        lines += ["当前持仓是模型在本交易日的状态，可能承接前次收盘信号；实际账户成交以交易记录为准。", ""]
    lines += [
        f"| 版本 | 当前 → 下一持仓 | {action_heading} | 下一交易日仓位 | 核心动量 |",
        "|---|---|---|---:|---|",
    ]
    for item in results:
        fields = item["fields"]
        assert isinstance(fields, dict)
        trusted = item["status"] == "OK"
        current = (
            escape_markdown_inline(format_holding(first_value(fields, "current_holding", "holding")))
            if trusted
            else "不可用"
        )
        next_holding = (
            escape_markdown_inline(format_holding(first_value(fields, "next_holding"))) if trusted else "不可用"
        )
        lines.append(
            "| "
            + " | ".join(
                [
                    escape_table_cell(str(item["version"])),
                    escape_table_cell(f"{current} → {next_holding}"),
                    escape_table_cell(escape_markdown_inline(action_label(item))),
                    escape_table_cell(format_scale(fields) if trusted else "不可用"),
                    escape_table_cell(momentum_text(str(item["version"]), fields) if trusted else "不可用"),
                ]
            )
            + " |"
        )

    warnings = [warning for item in results for warning in risk_warnings(item)]
    if date_s == "2026-09-04" and any(item["version"] == "v2.0" and item["status"] == "OK" for item in results):
        warnings.insert(0, "v2.0：今日已切换为朴素固定1倍规则；旧版模型为空仓，新版模型为持仓。上方无操作仅指新模型内部状态，不表示实盘已完成切换；请按账户实际持仓核对，本日报未发送订单。")
    if date_s == "2026-09-04" and any(item["version"] == "v2.3" and item["status"] == "OK" for item in results):
        warnings.insert(0, "v2.3：今日替换为朴素版，R²入场过滤关闭，过热26%触发/20%恢复；动量25天、半衰期2.5天、入场0、退出缓冲0.08、执行对冲0.8不变。模型持仓不代表账户已完成切换，请核对实际持仓；本日报未发送订单。")
    lines += ["", "## 需要关注", ""]
    if warnings:
        lines += [f"- **{warning.split('：', 1)[0]}：**{warning.split('：', 1)[1]}" for warning in warnings]
    else:
        lines.append("风险/数据异常：无")
    lines += ["", shared_data_line(results, strategy_sha, publication_mode)]
    if run_url:
        lines += [f"[查看完整诊断与原始输出]({run_url})"]
    body = "\n".join(lines)
    normalized_prefix = subject_prefix.strip().strip("[]")
    prefix = f"[{normalized_prefix}]" if normalized_prefix else ""
    mode_tag = "[收盘确认]" if publication_mode == "close_confirmed" else ""
    subject = f"{prefix}{mode_tag}[{tag}] 微盘股 {versions} 日报 - {date_s}"
    return subject, body


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a Gmail-ready microcap realtime signal digest.")
    parser.add_argument(
        "--result",
        action="append",
        required=True,
        help="Path to realtime signal output, or version=path. Can be repeated.",
    )
    parser.add_argument("--out-dir", required=True, help="Output artifact directory")
    parser.add_argument("--planned", default="12:45 Asia/Shanghai")
    parser.add_argument("--started", default="")
    parser.add_argument("--subject-prefix", default="")
    parser.add_argument("--strategy-sha", default="", help="Checked-out microcap strategy commit SHA")
    parser.add_argument(
        "--publication-mode",
        choices=("realtime", "close_confirmed"),
        default="realtime",
        help="Whether the final CSVs represent an intraday realtime snapshot or a close-confirmed signal.",
    )
    parser.add_argument(
        "--expected-signal-date",
        default="",
        help="Optional YYYY-MM-DD hard gate for scheduled close-confirmed publication.",
    )
    parser.add_argument("--exit-code", action="append", default=[], help="Exit code, or version=code. Can be repeated.")
    parser.add_argument(
        "--signal-csv",
        action="append",
        default=[],
        help="Required version=path final realtime signal CSV. Can be repeated.",
    )
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    exit_codes, default_exit_code = parse_exit_codes(args.exit_code)
    signal_csv_paths = parse_signal_csv_specs(args.signal_csv)
    result_specs = [split_version_spec(spec, "v2.0" if len(args.result) == 1 else "") for spec in args.result]
    results: list[dict[str, object]] = []
    for version, result_value in result_specs:
        if not version:
            raise ValueError("multiple --result values must use version=path format")
        result_path = Path(result_value)
        raw = result_path.read_text(encoding="utf-8", errors="replace") if result_path.exists() else "未找到实时信号输出文件。"
        output = clean_output(raw)
        exit_code = exit_codes.get(version, default_exit_code)
        status, status_note = classify_signal_output(output, exit_code, args.publication_mode)
        stdout_fields = parse_output_fields(output)
        csv_fields, csv_status_note = read_last_csv_row(signal_csv_paths.get(version, ""))
        fields: dict[str, str] = {}
        if status == "OK":
            if csv_status_note:
                status = "FAILED"
                status_note = csv_status_note
        if status == "OK":
            identity_ok, identity_note = validate_strategy_identity(version, csv_fields)
            if not identity_ok:
                status = "FAILED"
                status_note = identity_note
        if status == "OK":
            member_contract_ok, member_contract_note = validate_member_action_contract(csv_fields)
            if not member_contract_ok:
                status = "FAILED"
                status_note = member_contract_note
        if status == "OK":
            publication_ok, publication_note = validate_publication_contract(args.publication_mode, csv_fields)
            if not publication_ok:
                status = "FAILED"
                status_note = publication_note
        if status == "OK":
            execution_ok, execution_note = validate_execution_contract(version, csv_fields)
            if not execution_ok:
                status = "FAILED"
                status_note = execution_note
            else:
                # The final CSV alone owns trading state, dates, risk and identity.
                # Logs may supplement only non-authoritative presentation metadata.
                fields = dict(csv_fields)
                for key in ("quote_coverage", "quote_source"):
                    if not first_value(fields, key) and first_value(stdout_fields, key):
                        fields[key] = stdout_fields[key]
        results.append(
            {
                "version": version,
                "path": str(result_path),
                "output": output,
                "stdout_fields": stdout_fields,
                "csv_fields": csv_fields,
                "fields": fields,
                "exit_code": exit_code,
                "status": status,
                "status_note": status_note,
            }
        )

    if not results:
        raise ValueError("at least one realtime signal result is required")

    if args.publication_mode == "close_confirmed":
        close_dates = {
            first_value(item["csv_fields"], "date")
            for item in results
            if isinstance(item["csv_fields"], dict) and first_value(item["csv_fields"], "date")
        }
        if len(close_dates) != 1:
            for item in results:
                item["status"] = "FAILED"
                item["status_note"] = "publication contract invalid: close-confirmed CSV dates must match"
            date_s = now_bj().date().isoformat()
        else:
            date_s = next(iter(close_dates))
        if args.expected_signal_date:
            expected_signal_date = parse_strict_iso_date(args.expected_signal_date)
            if expected_signal_date is None or expected_signal_date.isoformat() != args.expected_signal_date:
                raise ValueError("--expected-signal-date must be an exact YYYY-MM-DD value")
            if close_dates != {args.expected_signal_date}:
                actual = ",".join(sorted(close_dates)) if close_dates else "<missing>"
                for item in results:
                    item["status"] = "FAILED"
                    item["status_note"] = (
                        "publication contract invalid: close-confirmed CSV date does not match "
                        f"scheduled delivery date; actual={actual} expected={args.expected_signal_date}"
                    )
                    item["fields"] = {}
                date_s = args.expected_signal_date
    else:
        date_s = now_bj().date().isoformat()
    run_url = os.environ.get("GITHUB_RUN_URL", "")
    subject, digest_text = build_compact_digest(
        results,
        date_s=date_s,
        run_url=run_url,
        strategy_sha=args.strategy_sha,
        subject_prefix=args.subject_prefix,
        publication_mode=args.publication_mode,
    )

    md = out_dir / f"microcap_realtime_signal_digest_{date_s}.md"
    md.write_text(digest_text, encoding="utf-8")

    meta = {
        "subject": subject,
        "body": digest_text,
        "attachment": None,
        "status": worst_status([str(item["status"]) for item in results]),
        "publication_mode": args.publication_mode,
        "signal_date": date_s,
    }
    (out_dir / "metadata.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
