import unittest
from pathlib import Path

import pandas as pd


class Top100RealtimeResilienceTest(unittest.TestCase):
    def test_eastmoney_stock_spot_includes_previous_close(self):
        import microcap_top100_mom16_biweekly_live as base

        original_get = base.requests.get

        class Response:
            def raise_for_status(self):
                return None

            def json(self):
                return {
                    "data": {
                        "f43": 1234,
                        "f57": "000001",
                        "f58": "one",
                        "f60": 1200,
                    }
                }

        try:
            base.requests.get = lambda *args, **kwargs: Response()

            row = base.fetch_eastmoney_stock_spot("000001")
        finally:
            base.requests.get = original_get

        self.assertEqual(row["code"], "000001")
        self.assertEqual(row["name"], "one")
        self.assertEqual(float(row["rt_price"]), 12.34)
        self.assertEqual(float(row["pre_close"]), 12.0)

    def test_member_quote_fetch_falls_back_when_qveris_price_is_invalid(self):
        import microcap_top100_mom16_biweekly_live as base

        original_env = base.os.environ.get("QVERIS_API_KEY")
        original_qveris = base.fetch_qveris_realtime_quotes
        original_eastmoney = base.fetch_eastmoney_stock_spot

        def fetch_qveris_realtime_quotes(symbols):
            out = pd.DataFrame(
                [
                    {"code": "000001", "name": "one", "rt_price": 11.0},
                    {"code": "000002", "name": "two", "rt_price": None},
                ]
            )
            return out, "qveris_test"

        try:
            base.os.environ["QVERIS_API_KEY"] = "set"
            base.fetch_qveris_realtime_quotes = fetch_qveris_realtime_quotes
            base.fetch_eastmoney_stock_spot = lambda symbol: {
                "code": symbol,
                "name": "two-fallback",
                "rt_price": 21.0,
                "pre_close": 20.0,
            }

            result = base.fetch_member_realtime_quotes(["000001", "000002"], max_workers=1)
        finally:
            if original_env is None:
                base.os.environ.pop("QVERIS_API_KEY", None)
            else:
                base.os.environ["QVERIS_API_KEY"] = original_env
            base.fetch_qveris_realtime_quotes = original_qveris
            base.fetch_eastmoney_stock_spot = original_eastmoney

        row = result.set_index("code").loc["000002"]
        self.assertEqual(float(row["rt_price"]), 21.0)
        self.assertEqual(float(row["pre_close"]), 20.0)
        self.assertEqual(result.attrs["quote_source"], "qveris_test+eastmoney_stock_get_member_only")

    def test_realtime_last_close_map_refreshes_missing_member_price_cache(self):
        import microcap_top100_mom16_biweekly_live as base

        original_load = base.load_latest_close_map
        original_refresh = base.refresh_price_cache_tail
        loads = []
        refreshes = []

        def load_latest_close_map(symbols, as_of_date):
            loads.append(list(symbols))
            if len(loads) == 1:
                return {"000001": 10.0}
            return {"000001": 10.0, "000002": 20.0}

        try:
            base.load_latest_close_map = load_latest_close_map
            base.refresh_price_cache_tail = lambda as_of_date, max_workers, symbols=None: refreshes.append(
                (as_of_date, max_workers, list(symbols or []))
            )

            result = base.ensure_realtime_last_close_map(
                ["000001", "000002"],
                as_of_date=pd.Timestamp("2026-05-11"),
                max_workers=2,
            )
        finally:
            base.load_latest_close_map = original_load
            base.refresh_price_cache_tail = original_refresh

        self.assertEqual(result, {"000001": 10.0, "000002": 20.0})
        self.assertEqual(refreshes, [(pd.Timestamp("2026-05-11"), 2, ["000002"])])

    def test_missing_close_refresh_is_limited_to_unpriced_symbols_and_nonfatal(self):
        import microcap_top100_mom16_biweekly_live as base

        original_ensure = base.ensure_realtime_last_close_map
        calls = []

        try:
            base.ensure_realtime_last_close_map = lambda symbols, as_of_date: calls.append(list(symbols)) or {
                "000002": 20.0
            }

            result = base.maybe_refresh_missing_realtime_last_close_map(
                {"000001": 10.0},
                [
                    {"symbol": "000001", "name": "one", "rank": 1},
                    {"symbol": "000002", "name": "two", "rank": 2},
                ],
                as_of_date=pd.Timestamp("2026-05-11"),
            )
        finally:
            base.ensure_realtime_last_close_map = original_ensure

        self.assertEqual(calls, [["000002"]])
        self.assertEqual(result, {"000001": 10.0, "000002": 20.0})

        try:
            base.ensure_realtime_last_close_map = lambda symbols, as_of_date: (_ for _ in ()).throw(
                RuntimeError("network failure")
            )

            unchanged = base.maybe_refresh_missing_realtime_last_close_map(
                {"000001": 10.0},
                [{"symbol": "000002", "name": "two", "rank": 2}],
                as_of_date=pd.Timestamp("2026-05-11"),
            )
        finally:
            base.ensure_realtime_last_close_map = original_ensure

        self.assertEqual(unchanged, {"000001": 10.0})

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
