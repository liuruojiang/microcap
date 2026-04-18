from __future__ import annotations

import unittest

import pandas as pd

import analyze_top100_mom16_v1_1_uphedge_on_drawdown as mod


class Top100V11UphedgeOnDrawdownTests(unittest.TestCase):
    def test_target_ratio_switches_to_full_hedge_after_drawdown_and_recovers_at_new_high(self) -> None:
        returns = pd.Series(
            [0.00, -0.05, 0.00, 0.01, 0.05, 0.00],
            index=pd.to_datetime(["2026-01-02", "2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08", "2026-01-09"]),
        )

        ratios = mod.compute_target_active_ratio_path(
            returns=returns,
            base_ratio=0.8,
            stress_ratio=1.0,
            drawdown_trigger=0.04,
        )

        self.assertEqual(ratios.round(4).tolist(), [0.8, 0.8, 1.0, 1.0, 1.0, 0.8])


if __name__ == "__main__":
    unittest.main()
