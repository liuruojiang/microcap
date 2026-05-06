from __future__ import annotations

"""Shared v1.4 base/context adapter for later Top100 overlay versions.

The v1.6, v1.7, and v1.8 entrypoint scripts are standalone strategy
frontends, but their intended baseline is still the v1.4 signal-quality
context. Keeping that dependency here makes the relationship explicit without
copying the v1.4 refresh/realtime plumbing into every overlay script.
"""

import microcap_top100_mom16_biweekly_live_v1_4 as _v1_4


BASE_HEDGE_RATIO = _v1_4.BASE_HEDGE_RATIO
v1_1_mod = _v1_4.v1_1_mod

current_base_fingerprint = _v1_4.current_base_fingerprint
_load_base_v1_1_context = _v1_4._load_base_v1_1_context
_load_realtime_v1_1_context = _v1_4._load_realtime_v1_1_context
_load_reference_summary = _v1_4._load_reference_summary
build_realtime_v1_4_outputs = _v1_4.build_realtime_v1_4_outputs


def __getattr__(name: str) -> object:
    return getattr(_v1_4, name)


__all__ = [
    "BASE_HEDGE_RATIO",
    "v1_1_mod",
    "current_base_fingerprint",
    "_load_base_v1_1_context",
    "_load_realtime_v1_1_context",
    "_load_reference_summary",
    "build_realtime_v1_4_outputs",
]
