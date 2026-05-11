import unittest
from pathlib import Path

import pandas as pd


class Top100RealtimeResilienceTest(unittest.TestCase):
    def test_fast_realtime_signal_retries_when_first_quote_fetch_is_incomplete(self):
        import microcap_top100_mom16_biweekly_live as base

        original_fetch = base.fetch_member_realtime_quotes
        original_hedge = base.fetch_hedge_realtime_quote_fast
        original_close_map = base.load_latest_close_map
        original_sleep = base.time.sleep
        original_run_signal = base.run_signal
        original_build_latest_signal = base.hedge_mod.build_latest_signal
        original_enrich = base.enrich_signal_frame
        original_rebalance = base.augment_signal_with_member_rebalance
        calls = []

        def fetch_member_realtime_quotes(symbols):
            calls.append(list(symbols))
            if len(calls) == 1:
                out = pd.DataFrame(
                    [{"code": "000001", "rt_price": 11.0, "pre_close": 10.0, "trade_date": "2026-05-11"}]
                )
            else:
                out = pd.DataFrame(
                    [
                        {"code": "000001", "rt_price": 11.0, "pre_close": 10.0, "trade_date": "2026-05-11"},
                        {"code": "000002", "rt_price": 21.0, "pre_close": 20.0, "trade_date": "2026-05-11"},
                    ]
                )
            out.attrs["quote_source"] = f"test_source_attempt_{len(calls)}"
            return out

        context = {
            "close_df": pd.DataFrame(
                [{"microcap": 100.0, "hedge": 50.0}],
                index=[pd.Timestamp("2026-05-08")],
            ),
            "effective_members": pd.DataFrame(
                [
                    {"symbol": "000001", "name": "one", "rank": 1},
                    {"symbol": "000002", "name": "two", "rank": 2},
                ]
            ),
            "result": pd.DataFrame(
                [
                    {
                        "signal_label": "cash",
                        "current_holding": "cash",
                        "next_holding": "cash",
                        "trade_state": "hold",
                        "momentum_trade_state": "hold",
                        "microcap_mom": 0.0,
                        "hedge_mom": 0.0,
                        "momentum_gap": 0.0,
                    }
                ],
                index=[pd.Timestamp("2026-05-08")],
            ),
        }

        try:
            base.fetch_member_realtime_quotes = fetch_member_realtime_quotes
            base.fetch_hedge_realtime_quote_fast = lambda: (55.0, "test_hedge")
            base.load_latest_close_map = lambda symbols, as_of_date=None: {"000001": 10.0, "000002": 20.0}
            base.time.sleep = lambda _seconds: None
            base.run_signal = lambda _close_df: context["result"]
            base.hedge_mod.build_latest_signal = lambda result: result.copy()
            base.enrich_signal_frame = lambda signal_df, _result: signal_df.copy()
            base.augment_signal_with_member_rebalance = lambda signal_df, _changes_df: signal_df.copy()

            signal_df, meta = base.build_realtime_signal_fast(context)
        finally:
            base.fetch_member_realtime_quotes = original_fetch
            base.fetch_hedge_realtime_quote_fast = original_hedge
            base.load_latest_close_map = original_close_map
            base.time.sleep = original_sleep
            base.run_signal = original_run_signal
            base.hedge_mod.build_latest_signal = original_build_latest_signal
            base.enrich_signal_frame = original_enrich
            base.augment_signal_with_member_rebalance = original_rebalance

        self.assertEqual(len(calls), 2)
        self.assertEqual(meta["member_price_count"], 2)
        self.assertEqual(meta["member_count"], 2)
        self.assertEqual(meta["quote_source"], "test_source_attempt_2")
        self.assertEqual(signal_df.iloc[0]["quote_source"], "test_source_attempt_2")
        self.assertAlmostEqual(float(meta["microcap_rt_close"]), 107.5)

    def test_quote_coverage_error_names_missing_symbols_and_source(self):
        import microcap_top100_mom16_biweekly_live as base

        with self.assertRaisesRegex(ValueError, "000002.*two.*test_source"):
            base.ensure_realtime_quote_coverage(
                1,
                2,
                missing_symbols=[{"symbol": "000002", "name": "two", "rank": 2}],
                quote_source="test_source",
            )

    def test_workflow_removes_stale_realtime_outputs_before_running_scripts(self):
        workflow = Path(".github/workflows/top100_realtime_signals.yml").read_text(encoding="utf-8")

        self.assertIn("rm -f realtime_signal_result.txt outputs/*_realtime_signal.csv", workflow)


if __name__ == "__main__":
    unittest.main()
