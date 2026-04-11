from __future__ import annotations

import microcap_top100_mom16_biweekly_live as base_mod


_ORIGINAL_BUILD_SUMMARY = base_mod.build_summary


base_mod.FIXED_HEDGE_RATIO = 0.8
base_mod.DEFAULT_OUTPUT_PREFIX = "microcap_top100_mom16_biweekly_live_v1_1"
base_mod.DEFAULT_COSTED_NAV_CSV = (
    base_mod.OUTPUT_DIR / "microcap_top100_mom16_hedge_zz1000_0p8x_biweekly_thursday_16y_costed_nav.csv"
)
base_mod.STRATEGY_TITLE = "Top100 Microcap Mom16 Biweekly v1.1 Mainline"


def build_summary(
    result,
    latest_signal,
    latest_rebalance,
    prev_rebalance,
    next_rebalance,
    members_df,
    changes_df,
    capital,
    anchor_freshness,
):
    summary = _ORIGINAL_BUILD_SUMMARY(
        result=result,
        latest_signal=latest_signal,
        latest_rebalance=latest_rebalance,
        prev_rebalance=prev_rebalance,
        next_rebalance=next_rebalance,
        members_df=members_df,
        changes_df=changes_df,
        capital=capital,
        anchor_freshness=anchor_freshness,
    )
    summary["version"] = "1.1"
    summary["version_role"] = "mainline"
    summary["version_note"] = (
        "Primary mainline version. Same live framework as v1.0, "
        "but fixed hedge ratio is reduced from 1.0x to 0.8x."
    )
    return summary


base_mod.build_summary = build_summary


if __name__ == "__main__":
    base_mod.main()
