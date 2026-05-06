import contextlib
import io
import sys
import tempfile
import unittest
from pathlib import Path
from types import ModuleType

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def make_signal_row(**overrides):
    row = {
        "current_holding": "cash",
        "next_holding": "long_microcap_short_zz1000",
        "effective_trade_state": "enter",
        "current_execution_scale": 0.0,
        "next_session_actionable_scale": 1.25,
        "microcap_mom": 0.0123,
        "hedge_mom": -0.0045,
        "momentum_gap": 0.0168,
        "snapshot_time": "2026-05-06 10:30:00",
        "latest_anchor_trade_date": "2026-05-05",
        "quote_coverage": "100/100",
    }
    row.update(overrides)
    return pd.DataFrame([row])


class RunTop100RealtimeSignalsTest(unittest.TestCase):
    def tearDown(self):
        for module_name in [
            "fake_v16_realtime_module",
            "fake_v18_realtime_module",
            "fake_cache_builder_module",
        ]:
            sys.modules.pop(module_name, None)

    def test_default_specs_are_v1_6_and_v1_8_realtime_builders(self):
        import run_top100_v1_6_v1_8_realtime_signals as runner

        self.assertEqual([spec.version for spec in runner.DEFAULT_SPECS], ["v1.6", "v1.8"])
        self.assertEqual(
            [spec.builder_name for spec in runner.DEFAULT_SPECS],
            ["build_realtime_v1_6_outputs", "build_realtime_v1_8_outputs"],
        )

    def test_run_strategy_calls_realtime_builder_and_reports_csv_path(self):
        import run_top100_v1_6_v1_8_realtime_signals as runner

        module = ModuleType("fake_v16_realtime_module")
        module.REALTIME_SIGNAL_CSV = Path("outputs/fake_v16_realtime.csv")
        calls = []

        def build_realtime_v1_6_outputs():
            calls.append("called")
            return make_signal_row(), {"quote_source": "test_quotes"}, pd.DataFrame()

        module.build_realtime_v1_6_outputs = build_realtime_v1_6_outputs
        sys.modules[module.__name__] = module

        result = runner.run_strategy(
            runner.StrategySpec("v1.6", module.__name__, "build_realtime_v1_6_outputs", "REALTIME_SIGNAL_CSV")
        )

        self.assertEqual(calls, ["called"])
        self.assertEqual(result.version, "v1.6")
        self.assertEqual(result.csv_path, Path("outputs/fake_v16_realtime.csv"))
        self.assertEqual(result.row["next_holding"], "long_microcap_short_zz1000")
        self.assertEqual(result.meta["quote_source"], "test_quotes")

    def test_main_prints_compact_summary_for_both_versions(self):
        import run_top100_v1_6_v1_8_realtime_signals as runner

        v16 = ModuleType("fake_v16_realtime_module")
        v16.REALTIME_SIGNAL_CSV = Path("outputs/fake_v16.csv")
        v16.build_realtime_v1_6_outputs = lambda: (
            make_signal_row(snapshot_time="2026-05-06 10:31:00"),
            {"quote_source": "q16"},
            pd.DataFrame(),
        )
        sys.modules[v16.__name__] = v16

        v18 = ModuleType("fake_v18_realtime_module")
        v18.REALTIME_SIGNAL_CSV = Path("outputs/fake_v18.csv")
        v18.build_realtime_v1_8_outputs = lambda: (
            make_signal_row(next_session_actionable_scale=2.0, quote_coverage="99/100"),
            {"quote_source": "q18"},
            pd.DataFrame(),
        )
        sys.modules[v18.__name__] = v18

        specs = [
            runner.StrategySpec("v1.6", v16.__name__, "build_realtime_v1_6_outputs", "REALTIME_SIGNAL_CSV"),
            runner.StrategySpec("v1.8", v18.__name__, "build_realtime_v1_8_outputs", "REALTIME_SIGNAL_CSV"),
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            cache_builder = ModuleType("fake_cache_builder_module")
            cache_builder.UNIVERSE_CACHE = cache_dir / "active_universe.csv"
            cache_builder.CURRENT_ST_CACHE = cache_dir / "current_st.csv"
            cache_builder.fetch_active_universe = lambda force_refresh=False: cache_builder.UNIVERSE_CACHE.write_text(
                "symbol,name,code\nsz000001,test,000001\n",
                encoding="utf-8",
            )
            cache_builder.fetch_current_st_codes = lambda force_refresh=False: (
                cache_builder.CURRENT_ST_CACHE.write_text("code,name\n", encoding="utf-8")
                or set()
            )
            sys.modules[cache_builder.__name__] = cache_builder

            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = runner.main([], specs=specs, static_inputs_module=cache_builder.__name__)

        output = stdout.getvalue()
        self.assertEqual(exit_code, 0)
        self.assertIn("strategy_version: v1.6", output)
        self.assertIn("strategy_version: v1.8", output)
        self.assertIn("snapshot_time: 2026-05-06 10:31:00", output)
        self.assertIn("latest_anchor_trade_date: 2026-05-05", output)
        self.assertIn("current_holding: cash", output)
        self.assertIn("next_holding: long_microcap_short_zz1000", output)
        self.assertIn("trade_state: enter", output)
        self.assertIn("next_session_actionable_scale: 2.00", output)
        self.assertIn("quote_coverage: 99/100", output)
        self.assertIn("realtime_signal_csv: outputs/fake_v18.csv", output)

    def test_ensures_static_realtime_inputs_when_cache_files_are_missing(self):
        import run_top100_v1_6_v1_8_realtime_signals as runner

        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            module = ModuleType("fake_cache_builder_module")
            module.UNIVERSE_CACHE = cache_dir / "active_universe.csv"
            module.CURRENT_ST_CACHE = cache_dir / "current_st.csv"
            calls = []

            def fetch_active_universe(force_refresh=False):
                calls.append(("active", force_refresh))
                module.UNIVERSE_CACHE.write_text("symbol,name,code\nsz000001,test,000001\n", encoding="utf-8")

            def fetch_current_st_codes(force_refresh=False):
                calls.append(("st", force_refresh))
                module.CURRENT_ST_CACHE.write_text("code,name\n", encoding="utf-8")
                return set()

            module.fetch_active_universe = fetch_active_universe
            module.fetch_current_st_codes = fetch_current_st_codes
            sys.modules[module.__name__] = module

            runner.ensure_static_realtime_inputs(module_name=module.__name__)

            self.assertTrue(module.UNIVERSE_CACHE.exists())
            self.assertTrue(module.CURRENT_ST_CACHE.exists())
            self.assertEqual(calls, [("active", False), ("st", False)])

    def test_price_cache_tail_refreshes_price_and_share_inputs(self):
        import microcap_top100_mom16_biweekly_live as base

        original_fetch_mod = base.fetch_mod
        original_freq_mod = base.freq_mod
        price_calls = []
        share_calls = []

        fake_fetch = ModuleType("fake_fetch_module")

        def fetch_price_history(symbol, start_date, end_date, force_refresh=False):
            price_calls.append((symbol, start_date, end_date, force_refresh))
            return pd.DataFrame({"date": [end_date], "close_raw": [1.0]})

        def fetch_share_change(symbol, start_date, end_date, force_refresh=False):
            share_calls.append((symbol, start_date, end_date, force_refresh))
            return pd.DataFrame({"change_date": [end_date], "total_shares_10k": [1.0]})

        fake_fetch.fetch_price_history = fetch_price_history
        fake_fetch.fetch_share_change = fetch_share_change

        fake_freq = ModuleType("fake_freq_module")
        fake_freq.START_DATE = "2010-01-01"
        fake_freq.load_current_universe = lambda: ["000001", "000002"]

        try:
            base.fetch_mod = fake_fetch
            base.freq_mod = fake_freq
            base.refresh_price_cache_tail(pd.Timestamp("2026-05-06"), max_workers=2)
        finally:
            base.fetch_mod = original_fetch_mod
            base.freq_mod = original_freq_mod

        self.assertCountEqual(
            price_calls,
            [
                ("000001", "2010-01-01", "2026-05-06", False),
                ("000002", "2010-01-01", "2026-05-06", False),
            ],
        )
        self.assertCountEqual(
            share_calls,
            [
                ("000001", "2010-01-01", "2026-05-06", False),
                ("000002", "2010-01-01", "2026-05-06", False),
            ],
        )


if __name__ == "__main__":
    unittest.main()
