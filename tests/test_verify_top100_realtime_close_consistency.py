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
        "date": "2026-05-06",
        "current_holding": "long_microcap_short_zz1000",
        "next_holding": "long_microcap_short_zz1000",
        "effective_trade_state": "hold",
        "current_execution_scale": 1.5,
        "next_session_actionable_scale": 1.5,
        "microcap_mom": 0.11203,
        "hedge_mom": 0.082448,
        "momentum_gap": 0.029582,
    }
    row.update(overrides)
    return pd.DataFrame([row])


class VerifyTop100RealtimeCloseConsistencyTest(unittest.TestCase):
    def tearDown(self):
        sys.modules.pop("fake_consistency_module", None)

    def test_compare_rows_passes_when_key_fields_match_within_tolerance(self):
        import verify_top100_realtime_close_consistency as verifier

        close = make_signal_row()
        realtime = make_signal_row(
            snapshot_time="2026-05-06 16:10:00",
            latest_anchor_trade_date="2026-05-06",
            quote_trade_date="2026-05-06",
            official_close_confirmed_signal=False,
            microcap_mom=0.11203000001,
        )

        result = verifier.compare_signal_rows(
            version="v1.6",
            realtime_row=realtime.iloc[0].to_dict(),
            close_row=close.iloc[0].to_dict(),
            realtime_meta={},
            numeric_tolerance=1e-8,
        )

        self.assertTrue(result.passed)
        self.assertTrue(result.details["passed"].all())

    def test_compare_rows_fails_on_numeric_mismatch(self):
        import verify_top100_realtime_close_consistency as verifier

        close = make_signal_row(momentum_gap=0.02)
        realtime = make_signal_row(
            snapshot_time="2026-05-06 16:10:00",
            latest_anchor_trade_date="2026-05-06",
            quote_trade_date="2026-05-06",
            momentum_gap=0.03,
        )

        result = verifier.compare_signal_rows(
            version="v1.8",
            realtime_row=realtime.iloc[0].to_dict(),
            close_row=close.iloc[0].to_dict(),
            realtime_meta={},
            numeric_tolerance=1e-6,
        )

        failed = result.details.loc[~result.details["passed"]]
        self.assertFalse(result.passed)
        self.assertIn("momentum_gap", failed["field"].tolist())

    def test_main_writes_diff_outputs_and_returns_nonzero_on_mismatch(self):
        import verify_top100_realtime_close_consistency as verifier

        module = ModuleType("fake_consistency_module")
        module.REALTIME_SIGNAL_CSV = Path("outputs/fake_realtime.csv")
        module.LATEST_SIGNAL_CSV = Path("outputs/fake_close.csv")
        module.generate_outputs = lambda: ({}, make_signal_row(momentum_gap=0.02), pd.DataFrame())
        module.build_realtime_outputs = lambda: (
            make_signal_row(
                snapshot_time="2026-05-06 16:10:00",
                latest_anchor_trade_date="2026-05-06",
                quote_trade_date="2026-05-06",
                momentum_gap=0.03,
            ),
            {"quote_source": "test"},
            pd.DataFrame(),
        )
        sys.modules[module.__name__] = module

        spec = verifier.StrategySpec(
            version="v-test",
            module_name=module.__name__,
            close_builder_name="generate_outputs",
            realtime_builder_name="build_realtime_outputs",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            output_json = Path(tmpdir) / "summary.json"
            output_csv = Path(tmpdir) / "details.csv"
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = verifier.main(
                    [
                        "--output-json",
                        str(output_json),
                        "--output-csv",
                        str(output_csv),
                    ],
                    specs=[spec],
                )

            self.assertEqual(exit_code, 1)
            self.assertTrue(output_json.exists())
            self.assertTrue(output_csv.exists())
            self.assertIn("status: fail", stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
