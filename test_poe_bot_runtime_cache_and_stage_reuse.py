from __future__ import annotations

import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from poe_bots import microcap_top100_poe_bot as bot


def build_context() -> dict[str, object]:
    today_text = str(pd.Timestamp.now().normalize().date())
    close_index = pd.to_datetime(["2026-04-16", "2026-04-17"])
    close_df = pd.DataFrame(
        {
            "microcap": [950.0, 953.68],
            "hedge": [8200.0, 8307.44],
        },
        index=close_index,
    )
    close_df.index.name = "date"
    latest_signal = pd.DataFrame(
        [
            {
                "date": pd.Timestamp("2026-04-17"),
                "signal_label": "cash",
                "next_holding": "cash",
                "current_holding": "cash",
                "trade_state": "hold",
                "momentum_trade_state": "hold",
                "member_rebalance_label": "名单调仓（调入 1，调出 1）",
                "member_enter_count": 1,
                "member_exit_count": 1,
                "microcap_close": 953.68,
                "hedge_close": 8307.44,
                "microcap_mom": 0.0046,
                "hedge_mom": 0.0718,
                "momentum_gap": -0.0671,
                "ratio_r2": 0.487,
            }
        ]
    )
    target_members = pd.DataFrame(
        [{"symbol": "000001", "rank": 1, "name": "A", "market_cap": 1.0, "target_weight": 0.01}]
    )
    effective_members = target_members.copy()
    changes_df = pd.DataFrame([{"action": "enter", "symbol": "000001"}])
    return {
        "close_df": close_df,
        "result": None,
        "latest_signal": latest_signal,
        "target_members": target_members,
        "effective_members": effective_members,
        "changes_df": changes_df,
        "latest_rebalance": "2026-04-17",
        "prev_rebalance": "2026-04-03",
        "next_rebalance": "2026-05-01",
        "effective_rebalance": "2026-04-17",
        "rebalance_effective_date": "2026-04-17",
        "freshness": {
            "status": "正常",
            "latest_trade_date": "2026-04-17",
            "current_date": today_text,
            "stale_calendar_days": 0,
        },
        "rebuild_meta": {
            "strategy_version": "1.0",
            "strategy_label": "v1.0（主版本，1.0x 对冲）",
            "fixed_hedge_ratio": 1.0,
            "core_params": bot.build_context_core_params(),
            "last_trade_signal_date": None,
            "last_trade_signal_action": None,
            "candidate_pool": 500,
            "history_symbols_ok": 500,
            "history_symbols_failed": 0,
            "rebalance_count": 2,
            "effective_member_count": 1,
            "stage_attempts": [500],
            "window_trade_days": 20,
            "strict_validated": True,
            "validated_exact_pools": [500],
        },
    }


class FakeAttachment:
    def __init__(self, name: str, contents: bytes) -> None:
        self.name = name
        self._contents = contents

    def get_contents(self) -> bytes:
        return self._contents


class FakeMessage:
    def __init__(self, attachments: list[FakeAttachment]) -> None:
        self.attachments = attachments


class PoeBotRuntimeCacheAndStageReuseTests(unittest.TestCase):
    def test_resolve_rebalance_effective_date_uses_next_trading_day(self) -> None:
        trading_dates = pd.to_datetime(["2026-04-09", "2026-04-10", "2026-04-13"])
        effective_date = bot.resolve_rebalance_effective_date(trading_dates, pd.Timestamp("2026-04-09"))
        self.assertEqual(effective_date, "2026-04-10")

    def test_extend_histories_for_candidate_pool_fetches_only_missing_symbols(self) -> None:
        pool_500 = pd.DataFrame({"code": ["000001", "000002"]})
        pool_600 = pd.DataFrame({"code": ["000001", "000002", "000003"]})
        history_frame = pd.DataFrame([{"date": "2026-04-17", "close_raw": 1.0, "close_adj": 1.0}])
        fetch_calls: list[list[str]] = []

        def fake_fetch(candidates, *args, **kwargs):
            symbols = candidates["code"].astype(str).tolist()
            fetch_calls.append(symbols)
            return {symbol: history_frame.copy() for symbol in symbols}, {}

        with patch.object(bot, "fetch_candidate_histories", side_effect=fake_fetch):
            stage_histories, stage_failures, cache_histories, cache_failures = bot.extend_histories_for_candidate_pool(
                pool_500,
                pd.Timestamp("2026-04-01"),
                pd.Timestamp("2026-04-17"),
                8,
            )
            self.assertEqual(sorted(stage_histories.keys()), ["000001", "000002"])
            self.assertEqual(stage_failures, {})
            self.assertEqual(sorted(cache_histories.keys()), ["000001", "000002"])
            self.assertEqual(cache_failures, {})

            stage_histories, stage_failures, cache_histories, cache_failures = bot.extend_histories_for_candidate_pool(
                pool_600,
                pd.Timestamp("2026-04-01"),
                pd.Timestamp("2026-04-17"),
                8,
                history_cache=cache_histories,
                failure_cache=cache_failures,
            )
            self.assertEqual(sorted(stage_histories.keys()), ["000001", "000002", "000003"])
            self.assertEqual(stage_failures, {})
            self.assertEqual(sorted(cache_histories.keys()), ["000001", "000002", "000003"])
            self.assertEqual(cache_failures, {})

        self.assertEqual(fetch_calls, [["000001", "000002"], ["000003"]])

    def test_build_context_uses_recent_thread_attachment_cache_before_refresh(self) -> None:
        context = build_context()
        attachment_name, attachment_bytes, _ = bot.build_thread_context_attachment(context)
        fake_poe = SimpleNamespace(default_chat=[FakeMessage([FakeAttachment(attachment_name, attachment_bytes)])])

        with patch.object(bot, "poe", fake_poe):
            with patch.object(bot, "read_context_cache", return_value=None):
                with patch.object(bot, "build_trade_dates", side_effect=AssertionError("should not refresh")):
                    restored = bot.build_context(force_refresh=False, require_latest=True)

        self.assertIsInstance(restored, dict)
        self.assertEqual(restored["freshness"]["latest_trade_date"], "2026-04-17")
        self.assertEqual(restored["latest_signal"].iloc[0]["signal_label"], "cash")


if __name__ == "__main__":
    unittest.main()
