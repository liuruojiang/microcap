from __future__ import annotations

import unittest

import pandas as pd

from poe_bots import microcap_top100_poe_bot as current_bot
from poe_bots.legacy import microcap_top100_poe_bot_autorebuild_singlefile as legacy_bot


def build_signal_row() -> pd.Series:
    return pd.Series(
        {
            "date": "2026-04-17",
            "next_holding": "cash",
            "trade_state": "hold_cash",
            "momentum_trade_state": "hold_cash",
            "member_rebalance_label": "名单调仓（调入 9，调出 9）",
            "momentum_gap": -0.0671,
            "microcap_close": 953.68,
            "hedge_close": 8307.44,
            "microcap_mom": 0.0046,
            "hedge_mom": 0.0718,
            "ratio_r2": 0.487,
            "member_enter_count": 9,
            "member_exit_count": 9,
        }
    )


def build_context() -> dict[str, object]:
    return {
        "latest_rebalance": "2026-04-09",
        "effective_rebalance": "2026-04-09",
        "rebalance_effective_date": "2026-04-10",
        "changes_df": pd.DataFrame([{"action": "enter", "symbol": "000001"}]),
        "freshness": {
            "status": "正常",
            "latest_trade_date": "2026-04-17",
            "current_date": "2026-04-18",
            "stale_calendar_days": 1,
        },
        "rebuild_meta": {
            "candidate_pool": 500,
            "history_symbols_ok": 500,
            "history_symbols_failed": 0,
            "strict_validated": True,
            "validated_exact_pools": [500],
            "last_trade_signal_date": None,
            "last_trade_signal_action": None,
        },
    }


class PoeBotSignalSummaryLabelTests(unittest.TestCase):
    def test_current_bot_uses_anchor_label_for_freshness_date(self) -> None:
        text = current_bot.format_signal_summary(build_signal_row(), build_context())
        self.assertIn("历史锚点交易日：2026-04-17", text)
        self.assertIn("最近一次动量交易：无", text)
        self.assertIn("最近一次调仓交易：2026-04-10", text)
        self.assertIn("当前生效名单对应调仓日：2026-04-09", text)
        self.assertNotIn("当前生效名单：2026-04-09", text)
        self.assertNotIn("调仓生效日：2026-04-09", text)
        self.assertNotIn("最近市场交易日", text)
        self.assertNotIn("最近一次策略交易日期", text)
        self.assertNotIn("最近一次交易日", text)
        self.assertNotIn("最近历史交易日", text)

    def test_legacy_singlefile_bot_uses_anchor_label_for_freshness_date(self) -> None:
        text = legacy_bot.format_signal_summary(build_signal_row(), build_context())
        self.assertIn("历史锚点交易日：2026-04-17", text)
        self.assertIn("最近一次动量交易：无", text)
        self.assertIn("最近一次调仓交易：2026-04-10", text)
        self.assertIn("当前生效名单对应调仓日：2026-04-09", text)
        self.assertNotIn("当前生效名单：2026-04-09", text)
        self.assertNotIn("调仓生效日：2026-04-09", text)
        self.assertNotIn("最近市场交易日", text)
        self.assertNotIn("最近一次策略交易日期", text)
        self.assertNotIn("最近一次交易日", text)
        self.assertNotIn("最近历史交易日", text)


if __name__ == "__main__":
    unittest.main()
