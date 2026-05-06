import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import microcap_top100_mom16_biweekly_live_v1_8 as v18


class MicrocapTop100V18ParametersTest(unittest.TestCase):
    def test_v1_8_recommended_parameters_are_locked(self):
        self.assertEqual(v18.STRATEGY_VERSION, "1.8")
        self.assertEqual(v18.LOOKBACK, 11)
        self.assertAlmostEqual(v18.ENTRY_GAP_THRESHOLD, 0.006)
        self.assertAlmostEqual(v18.V1_8_MOMENTUM_GAP_EXIT_BUFFER, 0.006)
        self.assertAlmostEqual(v18.DECAY_RATIO_THRESHOLD, 0.30)
        self.assertAlmostEqual(v18.RECOVERY_RATIO_THRESHOLD, 0.30)
        self.assertAlmostEqual(v18.TARGET_VOL, 0.30)
        self.assertEqual(v18.TARGET_VOL_WINDOW, 20)
        self.assertAlmostEqual(v18.TARGET_VOL_MAX_LEVERAGE, 2.0)
        self.assertAlmostEqual(v18.TARGET_VOL_SCALE_REBALANCE_THRESHOLD, 0.25)
        self.assertEqual(v18.VOLUME_FILTER_MA, 53)
        self.assertEqual(v18.VOLUME_FILTER_CONSECUTIVE_DAYS, 13)
        self.assertAlmostEqual(v18.VOLUME_FILTER_SCALE, 0.25)
        self.assertAlmostEqual(v18.NAV_DD_TRIGGER, 0.13)
        self.assertAlmostEqual(v18.NAV_DD_SCALE, 0.80)
        self.assertAlmostEqual(v18.NAV_DD_RECOVER, 0.06)

    def test_v1_8_output_names_do_not_overwrite_existing_versions(self):
        self.assertEqual(v18.OUTPUT_PREFIX, "microcap_top100_mom16_biweekly_live_v1_8")
        self.assertTrue(v18.SUMMARY_JSON.name.endswith("_v1_8_summary.json"))
        self.assertIn("v1_8", v18.COSTED_NAV_CSV.name)

    def test_volume_overlay_uses_t_plus_1_execution_and_cost(self):
        index = pd.date_range("2024-01-01", periods=5, freq="D")
        base = pd.DataFrame({"return_net": [0.10, 0.10, 0.10, 0.10, 0.10]}, index=index)
        signal = pd.Series([False, True, True, False, False], index=index)

        out = v18.apply_broad_volume_filter(base, signal)

        expected_scale = pd.Series([1.0, 1.0, 0.25, 0.25, 1.0], index=index)
        expected_cost = expected_scale.diff().abs().fillna(0.0) * v18.VOLUME_FILTER_SCALE_CHANGE_COST
        expected_return = base["return_net"] * expected_scale - expected_cost
        pd.testing.assert_series_equal(out["volume_execution_scale"], expected_scale, check_names=False)
        pd.testing.assert_series_equal(out["volume_overlay_cost"], expected_cost, check_names=False)
        pd.testing.assert_series_equal(out["return_net"], expected_return, check_names=False)

    def test_nav_dd_overlay_uses_t_plus_1_execution_and_cost(self):
        index = pd.date_range("2024-01-01", periods=6, freq="D")
        base = pd.DataFrame({"return_net": [0.0, -0.14, 0.10, 0.10, 0.10, 0.0]}, index=index)

        out = v18.apply_nav_drawdown_throttle(base)

        expected_scale = pd.Series([1.0, 1.0, 0.8, 0.8, 1.0, 1.0], index=index)
        expected_cost = expected_scale.diff().abs().fillna(0.0) * v18.NAV_DD_SCALE_CHANGE_COST
        expected_return = base["return_net"] * expected_scale - expected_cost
        pd.testing.assert_series_equal(out["nav_dd_execution_scale"], expected_scale, check_names=False)
        pd.testing.assert_series_equal(out["nav_dd_overlay_cost"], expected_cost, check_names=False)
        np.testing.assert_allclose(out["return_net"].to_numpy(), expected_return.to_numpy())


if __name__ == "__main__":
    unittest.main()
