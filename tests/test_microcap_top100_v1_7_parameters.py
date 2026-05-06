import unittest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import microcap_top100_mom16_biweekly_live_v1_7 as v17


class MicrocapTop100V17ParametersTest(unittest.TestCase):
    def test_v1_7_candidate_parameters_are_locked(self):
        self.assertEqual(v17.STRATEGY_VERSION, "1.7")
        self.assertEqual(v17.LOOKBACK, 12)
        self.assertAlmostEqual(v17.ENTRY_GAP_THRESHOLD, 0.008)
        self.assertAlmostEqual(v17.V1_7_MOMENTUM_GAP_EXIT_BUFFER, 0.0035)
        self.assertAlmostEqual(v17.DECAY_RATIO_THRESHOLD, 0.25)
        self.assertAlmostEqual(v17.RECOVERY_RATIO_THRESHOLD, 0.25)
        self.assertAlmostEqual(v17.TARGET_VOL, 0.20)
        self.assertEqual(v17.TARGET_VOL_WINDOW, 40)
        self.assertAlmostEqual(v17.TARGET_VOL_SCALE_REBALANCE_THRESHOLD, 0.15)

    def test_v1_7_output_names_do_not_overwrite_v1_6(self):
        self.assertEqual(v17.OUTPUT_PREFIX, "microcap_top100_mom16_biweekly_live_v1_7")
        self.assertTrue(v17.SUMMARY_JSON.name.endswith("_v1_7_summary.json"))
        self.assertEqual(
            v17.COSTED_NAV_CSV.name,
            "microcap_top100_mom16_targetvol20_max1p5_v1_7_costed_nav.csv",
        )

    def test_v1_7_fingerprint_records_candidate_parameter_changes(self):
        fingerprint = v17.current_base_fingerprint()
        self.assertEqual(fingerprint["base_version"], "1.4")
        self.assertEqual(fingerprint["lookback"], 12)
        self.assertAlmostEqual(fingerprint["momentum_gap_entry_threshold"], 0.008)
        self.assertAlmostEqual(fingerprint["momentum_gap_exit_buffer"], 0.0035)
        self.assertAlmostEqual(fingerprint["target_vol"], 0.20)
        self.assertEqual(fingerprint["vol_window"], 40)
        self.assertAlmostEqual(fingerprint["scale_rebalance_threshold"], 0.15)
        self.assertAlmostEqual(fingerprint["recovery_ratio_threshold"], 0.25)


if __name__ == "__main__":
    unittest.main()
