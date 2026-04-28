#!/usr/bin/env python
# -*- coding: utf-8 -*-
# poe: name=Microcap-Top100-v1.6
# poe: privacy_shield=half
"""Dedicated POE entry for the Top100 microcap v1.6 strategy."""

import microcap_top100_poe_bot as _base

DEDICATED_VERSION = "1.6"
_original_build_intro_text = _base.build_intro_text
_original_build_help_text = _base.build_help_text


def _force_dedicated_strategy(query_text):
    _base.set_active_strategy(DEDICATED_VERSION)
    cleaned = _base.strip_strategy_version_tokens(query_text)
    return _base.get_strategy(), cleaned


def _build_intro_text():
    return (
        "Top100 微盘股对冲策略机器人（v1.6 专用入口）\n\n"
        + _original_build_intro_text().replace(
            "说明：默认按 v1.0 主版本运行；若命令里写明 1.4 / 1.5 / 1.6，例如“1.6的信号”“1.6 表现 近3年”，则切换到对应版本。\n",
            "说明：本入口固定按 v1.6 运行，忽略查询文本里的版本号；v1.0 / v1.4 请使用各自专用 Poe 入口。\n",
        )
    )


def _build_help_text():
    return _original_build_help_text().replace(
        "说明：默认按 v1.0 运行；若查询里明确写“1.4”/“v1.4”、“1.5”/“v1.5”或“1.6”/“v1.6”，则切换到对应版本。",
        "说明：本入口固定按 v1.6 运行；v1.0 / v1.4 请使用各自专用 Poe 入口。",
    )


_base.resolve_strategy_from_query = _force_dedicated_strategy
_base.build_intro_text = _build_intro_text
_base.build_help_text = _build_help_text
_base.set_active_strategy(DEDICATED_VERSION)
_base.update_settings()


if __name__ == "__main__":
    _base.main()
