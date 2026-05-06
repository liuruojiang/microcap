import sys
import unittest
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.verify_top100_v14_adapter_equivalence import compare_frames


class VerifyTop100V14AdapterEquivalenceTest(unittest.TestCase):
    def test_compare_frames_reports_exact_match(self):
        dates = pd.date_range("2026-01-01", periods=2)
        current = pd.DataFrame(
            {
                "date": dates,
                "holding": ["long", "cash"],
                "next_holding": ["cash", "long"],
                "return_net": [0.01, -0.02],
                "nav_net": [1.01, 0.9898],
                "execution_scale": [1.0, 0.0],
                "total_cost": [0.001, 0.0],
            }
        )
        backup = current.copy()

        report = compare_frames(current, backup, tolerance=1e-12)

        self.assertTrue(report["equal_within_tolerance"])
        self.assertEqual(report["common_index_rows"], 2)
        self.assertEqual(report["missing_in_current"], 0)
        self.assertEqual(report["missing_in_backup"], 0)
        self.assertEqual(report["categorical_mismatches"]["holding"], 0)
        self.assertEqual(report["numeric_mismatch_count_gt_tolerance"]["return_net"], 0)
        self.assertEqual(report["numeric_max_abs_diff"]["return_net"], 0.0)

    def test_compare_frames_detects_date_holding_and_numeric_mismatches(self):
        current = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-01-01", "2026-01-02"]),
                "holding": ["long", "cash"],
                "next_holding": ["cash", "long"],
                "return_net": [0.01, -0.02],
                "nav_net": [1.01, 0.9898],
                "execution_scale": [1.0, 0.0],
                "total_cost": [0.001, 0.0],
            }
        )
        backup = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-01-01", "2026-01-03"]),
                "holding": ["cash", "cash"],
                "next_holding": ["cash", "long"],
                "return_net": [0.02, -0.02],
                "nav_net": [1.02, 0.9996],
                "execution_scale": [1.0, 0.0],
                "total_cost": [0.001, 0.0],
            }
        )

        report = compare_frames(current, backup, tolerance=1e-12)

        self.assertFalse(report["equal_within_tolerance"])
        self.assertEqual(report["common_index_rows"], 1)
        self.assertEqual(report["missing_in_current"], 1)
        self.assertEqual(report["missing_in_backup"], 1)
        self.assertEqual(report["categorical_mismatches"]["holding"], 1)
        self.assertEqual(report["numeric_mismatch_count_gt_tolerance"]["return_net"], 1)
        self.assertAlmostEqual(report["numeric_max_abs_diff"]["return_net"], 0.01)


if __name__ == "__main__":
    unittest.main()
