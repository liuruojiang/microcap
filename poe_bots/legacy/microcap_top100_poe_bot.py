#!/usr/bin/env python
# -*- coding: utf-8 -*-
# poe: name=Microcap-Top100-Signal
# poe: privacy_shield=half
"""POE bot wrapper for the Top100 microcap live signal script."""

import contextlib
import io
import sys
from pathlib import Path
from types import SimpleNamespace

try:
    from fastapi_poe.types import SettingsResponse
except Exception:
    class SettingsResponse:  # type: ignore[no-redef]
        def __init__(self, introduction_message: str = "") -> None:
            self.introduction_message = introduction_message


try:
    poe  # type: ignore[name-defined]
except NameError:
    try:
        import fastapi_poe as poe  # type: ignore
    except Exception:
        poe = None  # type: ignore


import microcap_top100_mom16_biweekly_live as top100_mod


ROOT = Path(__file__).resolve().parent
DEFAULT_REALTIME_CACHE_SECONDS = 3600


def _update_settings() -> None:
    if poe is None or not hasattr(poe, "update_settings"):
        return
    poe.update_settings(
        SettingsResponse(
            introduction_message=(
                "📊 **Top100 微盘股对冲策略机器人**\n\n"
                "支持命令：\n"
                '- 发送 **"信号"** -> 返回最新收盘确认信号\n'
                '- 发送 **"实时信号"** -> 返回盘中实时信号，优先复用近 1 小时缓存\n'
                '- 发送 **"强制刷新实时信号"** -> 忽略缓存，重新抓取实时行情\n\n'
                "策略计算完全复用 `microcap_top100_mom16_biweekly_live.py`。"
            )
        )
    )


def _build_args(*, realtime_cache_seconds: int) -> SimpleNamespace:
    return SimpleNamespace(
        panel_path=top100_mod.hedge_mod.DEFAULT_PANEL,
        index_csv=top100_mod.DEFAULT_INDEX_CSV,
        costed_nav_csv=top100_mod.DEFAULT_COSTED_NAV_CSV,
        output_prefix=top100_mod.DEFAULT_OUTPUT_PREFIX,
        capital=None,
        max_workers=8,
        realtime_cache_seconds=realtime_cache_seconds,
        rebuild_index_if_missing=True,
        force_refresh=False,
        max_stale_anchor_days=top100_mod.DEFAULT_MAX_STALE_ANCHOR_DAYS,
        allow_stale_realtime=False,
    )


def _normalize_command(query_text: str) -> tuple[str, int]:
    text = (query_text or "").strip()
    if not text:
        return "信号", DEFAULT_REALTIME_CACHE_SECONDS
    if "实时" in text and "信号" in text:
        if "强制" in text or "刷新" in text:
            return "实时信号", 0
        return "实时信号", DEFAULT_REALTIME_CACHE_SECONDS
    if "信号" in text:
        return "信号", DEFAULT_REALTIME_CACHE_SECONDS
    return "帮助", DEFAULT_REALTIME_CACHE_SECONDS


def _refresh_summary(context: dict[str, object], args: SimpleNamespace) -> None:
    context["summary"] = top100_mod.build_summary(
        result=context["result"],
        latest_signal=context["latest_signal"],
        latest_rebalance=context["latest_rebalance"],
        prev_rebalance=context["prev_rebalance"],
        next_rebalance=context["next_rebalance"],
        members_df=context["target_members"],
        changes_df=context["changes_df"],
        capital=args.capital,
        anchor_freshness=context["anchor_freshness"],
    )


def _hydrate_static_members(context: dict[str, object], args: SimpleNamespace) -> dict[str, object]:
    cached_static = top100_mod.load_cached_static_context(
        paths=context["paths"],
        latest_rebalance=context["latest_rebalance"],
        prev_rebalance=context["prev_rebalance"],
        effective_rebalance=context["effective_rebalance"],
        rebalance_effective_date=context["rebalance_effective_date"],
        capital=args.capital,
    )
    if cached_static is not None:
        target_members, effective_members, changes_df = cached_static
        context["include_members"] = True
        context["target_members"] = target_members
        context["effective_members"] = effective_members
        context["changes_df"] = changes_df
        _refresh_summary(context, args)
        return context
    return top100_mod.build_base_context(args, include_members=True)


def _prepare_context(command: str, cache_seconds: int) -> tuple[SimpleNamespace, dict[str, object]]:
    args = _build_args(realtime_cache_seconds=cache_seconds)
    if command == "实时信号":
        context = top100_mod.build_base_context(args, include_members=False)
        context = _hydrate_static_members(context, args)
        return args, context
    context = top100_mod.build_base_context(args, include_members=False)
    return args, context


def _run_query(command: str, cache_seconds: int) -> tuple[str, list[Path]]:
    args, context = _prepare_context(command, cache_seconds)
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        top100_mod.handle_query(context, args, command)

    paths = context["paths"]
    attachments: list[Path] = []
    if command == "信号":
        attachments.append(paths["signal"])
    elif command == "实时信号":
        attachments.append(paths["realtime_signal"])
    return output.getvalue().strip(), attachments


def _help_text() -> str:
    return (
        "支持命令：\n"
        '1. `信号`\n'
        '2. `实时信号`\n'
        '3. `强制刷新实时信号`\n\n'
        "说明：`实时信号` 默认优先复用近 1 小时缓存；如需重新抓取盘中行情，用 `强制刷新实时信号`。"
    )


def _content_type(path: Path) -> str:
    if path.suffix.lower() == ".csv":
        return "text/csv"
    if path.suffix.lower() == ".json":
        return "application/json"
    if path.suffix.lower() == ".png":
        return "image/png"
    return "application/octet-stream"


def _attach_outputs(msg, attachments: list[Path]) -> None:
    for path in attachments:
        if not path.exists():
            continue
        msg.attach_file(
            name=path.name,
            contents=path.read_bytes(),
            content_type=_content_type(path),
        )


def _send_message(text: str, attachments: list[Path] | None = None) -> None:
    if poe is None or not hasattr(poe, "start_message"):
        print(text)
        if attachments:
            for path in attachments:
                print(f"[file] {path}")
        return

    with poe.start_message() as msg:
        msg.write(text)
        if attachments:
            _attach_outputs(msg, attachments)


class MicrocapTop100SignalBot:
    def run(self) -> None:
        query_text = ""
        if poe is not None and hasattr(poe, "query") and getattr(poe, "query", None) is not None:
            query_text = (poe.query.text or "").strip()
        else:
            query_text = " ".join(sys.argv[1:]).strip()

        command, cache_seconds = _normalize_command(query_text)
        if command == "帮助":
            _send_message(_help_text())
            return

        status = "⏳ 正在计算实时信号..." if command == "实时信号" else "⏳ 正在计算最新信号..."
        if poe is not None and hasattr(poe, "start_message"):
            _send_message(status)

        try:
            body, attachments = _run_query(command, cache_seconds)
        except Exception as exc:
            _send_message(f"计算失败：{exc}")
            return

        header = "## 实时信号\n\n" if command == "实时信号" else "## 收盘确认信号\n\n"
        _send_message(f"{header}```text\n{body}\n```", attachments=attachments)


_update_settings()


if __name__ == "__main__":
    MicrocapTop100SignalBot().run()
