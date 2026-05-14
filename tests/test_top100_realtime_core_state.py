from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import top100_realtime_core as realtime_core


def test_realtime_args_use_current_base_costed_nav():
    args = realtime_core.build_v1_1_args()

    assert args.costed_nav_csv == realtime_core.BASE_COSTED_NAV_CSV


def test_tracked_realtime_state_is_reusable_without_refresh():
    args = realtime_core.build_v1_1_args()
    base_mod = realtime_core.base_mod
    base_paths = base_mod.build_output_paths(base_mod.DEFAULT_OUTPUT_PREFIX)
    panel_path, target_end_date = base_mod.refresh_history_anchor(args, base_paths)

    context = base_mod.build_realtime_context_from_cached_proxy(
        args,
        base_paths,
        panel_path,
        target_end_date,
        "test should not require cloud price-cache refresh",
    )

    assert context is not None
    assert "realtime base used cached proxy" in str(context.get("fallback_warning", ""))
