from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

import analyze_top100_rebalance_frequency as freq_mod
import microcap_top100_mom16_biweekly_live as live_mod


class Top100RealtimeQueryFastPathTests(unittest.TestCase):
    def setUp(self) -> None:
        self.args = SimpleNamespace(output_prefix="microcap_top100_mom16_biweekly_live")
        self.paths = {"proxy_meta": Path("outputs/proxy_meta.json")}
        self.panel_path = Path("outputs/panel_shadow.csv")
        self.target_end_date = pd.Timestamp("2026-04-13")
        self.base_context = {"latest_signal": "stub"}
        self.member_context = {"effective_members": "stub"}

    def test_execute_query_realtime_signal_skips_strategy_refresh(self) -> None:
        with patch.object(live_mod, "build_output_paths", return_value=self.paths):
            with patch.object(live_mod, "refresh_history_anchor", return_value=(self.panel_path, self.target_end_date)):
                with patch.object(live_mod, "classify_query_kind", return_value="realtime_signal"):
                    with patch.object(
                        live_mod,
                        "ensure_realtime_query_base_context",
                        return_value=self.base_context,
                        create=True,
                    ) as realtime_base_mock:
                        with patch.object(
                            live_mod,
                            "ensure_base_signal_fresh",
                            side_effect=AssertionError("slow path used for realtime signal"),
                        ):
                            with patch.object(
                                live_mod,
                                "ensure_static_members_fresh",
                                return_value=self.member_context,
                            ) as static_mock:
                                with patch.object(live_mod, "handle_query") as handle_query_mock:
                                    live_mod.execute_query(self.args, "实时信号")

        realtime_base_mock.assert_called_once_with(self.args, self.paths, self.panel_path, self.target_end_date)
        static_mock.assert_called_once_with(
            self.args,
            self.paths,
            self.panel_path,
            self.target_end_date,
            self.base_context,
        )
        handle_query_mock.assert_called_once_with(self.member_context, self.args, "实时信号")

    def test_execute_query_realtime_changes_skips_strategy_refresh(self) -> None:
        with patch.object(live_mod, "build_output_paths", return_value=self.paths):
            with patch.object(live_mod, "refresh_history_anchor", return_value=(self.panel_path, self.target_end_date)):
                with patch.object(live_mod, "classify_query_kind", return_value="realtime_changes"):
                    with patch.object(
                        live_mod,
                        "ensure_realtime_query_base_context",
                        return_value=self.base_context,
                        create=True,
                    ) as realtime_base_mock:
                        with patch.object(
                            live_mod,
                            "ensure_base_signal_fresh",
                            side_effect=AssertionError("slow path used for realtime changes"),
                        ):
                            with patch.object(
                                live_mod,
                                "ensure_static_members_fresh",
                                return_value=self.member_context,
                            ) as static_mock:
                                with patch.object(live_mod, "handle_query") as handle_query_mock:
                                    live_mod.execute_query(self.args, "实时进出名单")

        realtime_base_mock.assert_called_once_with(self.args, self.paths, self.panel_path, self.target_end_date)
        static_mock.assert_called_once_with(
            self.args,
            self.paths,
            self.panel_path,
            self.target_end_date,
            self.base_context,
        )
        handle_query_mock.assert_called_once_with(self.member_context, self.args, "实时进出名单")

    def test_qveris_realtime_quotes_parse_nested_batch_response(self) -> None:
        class FakeResponse:
            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict[str, object]:
                return {
                    "success": True,
                    "result": {
                        "data": [
                            [
                                {
                                    "thscode": "000001.SZ",
                                    "time": "2026-04-30 16:01:18",
                                    "tradeDate": "2026-04-30",
                                    "latest": 11.49,
                                    "preClose": 11.52,
                                }
                            ],
                            [
                                {
                                    "thscode": "600030.SH",
                                    "time": "2026-04-30 16:01:19",
                                    "tradeDate": "2026-04-30",
                                    "latest": 27.22,
                                    "preClose": 27.32,
                                }
                            ],
                        ]
                    },
                }

        class FakeRequests:
            @staticmethod
            def post(*args, **kwargs):
                return FakeResponse()

        with patch.dict(os.environ, {"QVERIS_API_KEY": "test-key"}):
            with patch.object(live_mod, "requests", FakeRequests):
                quotes, source = live_mod.fetch_qveris_realtime_quotes(["000001", "600030"])

        self.assertEqual(source, "qveris_cn_financial_pro_realtime")
        self.assertEqual(set(quotes["code"]), {"000001", "600030"})
        self.assertEqual(float(quotes.loc[quotes["code"] == "000001", "rt_price"].iloc[0]), 11.49)

    def test_realtime_quote_coverage_requires_all_members(self) -> None:
        with self.assertRaisesRegex(ValueError, "99/100"):
            live_mod.ensure_realtime_quote_coverage(99, 100)

    def test_same_trade_date_realtime_close_replaces_anchor_row(self) -> None:
        close_df = pd.DataFrame(
            {"microcap": [100.0, 101.0], "hedge": [200.0, 201.0]},
            index=pd.to_datetime(["2026-04-29", "2026-04-30"]),
        )

        patched = live_mod.apply_realtime_close_to_signal_frame(
            close_df=close_df,
            latest_trade_date=pd.Timestamp("2026-04-30"),
            snapshot_ts=pd.Timestamp("2026-05-02 18:00:00"),
            microcap_rt_close=102.0,
            hedge_rt_close=202.0,
            quote_trade_date="2026-04-30",
        )

        self.assertEqual(list(patched.index), list(close_df.index))
        self.assertEqual(float(patched.loc[pd.Timestamp("2026-04-30"), "microcap"]), 102.0)
        self.assertEqual(float(patched.loc[pd.Timestamp("2026-04-30"), "hedge"]), 202.0)

    def test_stale_adjusted_price_tail_falls_back_to_raw_returns(self) -> None:
        trading_dates = pd.to_datetime(
            ["2026-04-09", "2026-04-10", "2026-04-13", "2026-04-14"]
        )
        raw_price = pd.DataFrame(
            {
                "date": trading_dates,
                "close_raw": [10.0, 11.0, 12.1, 13.31],
            }
        )
        stale_adjusted = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-04-09", "2026-04-10"]),
                "close_qfq": [10.0, 11.0],
            }
        )

        ret_series, _, _, _ = freq_mod.build_tradeability_series(
            symbol="000001",
            price=raw_price,
            trading_dates=trading_dates,
            return_price=stale_adjusted,
            trade_constraint_mode=freq_mod.TRADE_CONSTRAINT_MODE_CLOSE,
        )

        self.assertAlmostEqual(float(ret_series.loc[pd.Timestamp("2026-04-10")]), 0.10)
        self.assertAlmostEqual(float(ret_series.loc[pd.Timestamp("2026-04-13")]), 0.10)
        self.assertAlmostEqual(float(ret_series.loc[pd.Timestamp("2026-04-14")]), 0.10)

    def test_proxy_tail_flat_detector_flags_active_frozen_tail(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "proxy.csv"
            pd.DataFrame(
                {
                    "date": pd.to_datetime(
                        ["2026-04-10", "2026-04-13", "2026-04-14", "2026-04-15", "2026-04-16"]
                    ),
                    "close": [100.0, 100.0, 100.0, 100.0, 100.0],
                    "daily_return": [0.0, 0.0, 0.0, 0.0, 0.0],
                    "holding_count": [100, 100, 100, 100, 100],
                }
            ).to_csv(path, index=False)

            self.assertTrue(
                live_mod.proxy_tail_is_suspiciously_flat(path, pd.Timestamp("2026-04-16"))
            )

    def test_signal_match_guard_rejects_momentum_mismatch(self) -> None:
        signal_df = pd.DataFrame(
            [
                {
                    "current_holding": "cash",
                    "next_holding": "cash",
                    "microcap_mom": 0.11,
                    "hedge_mom": 0.02,
                    "momentum_gap": 0.09,
                }
            ]
        )
        result = pd.DataFrame(
            [
                {
                    "holding": "cash",
                    "next_holding": "cash",
                    "microcap_mom": 0.10,
                    "hedge_mom": 0.02,
                    "momentum_gap": 0.08,
                }
            ],
            index=pd.to_datetime(["2026-04-30"]),
        )

        with self.assertRaisesRegex(RuntimeError, "microcap_mom"):
            live_mod.assert_signal_matches_result(signal_df, result)

    def test_realtime_meta_guard_rejects_quote_fallback(self) -> None:
        meta = {
            "member_count": 100,
            "member_price_count": 100,
            "hedge_quote_source": "latest_cached_close_fallback",
            "quote_trade_date": "2026-04-30",
            "latest_anchor_trade_date": "2026-04-30",
        }

        with self.assertRaisesRegex(RuntimeError, "fallback"):
            live_mod.assert_realtime_meta_is_actionable(meta)


if __name__ == "__main__":
    unittest.main()
