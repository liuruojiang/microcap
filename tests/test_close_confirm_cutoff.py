from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

import pandas as pd

import microcap_top100_mom16_biweekly_live as live
import microcap_top100_mom16_biweekly_live_v2_0 as v2_0


class CloseConfirmCutoffTests(unittest.TestCase):
    def setUp(self) -> None:
        self.history = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-05-21", "2026-05-22"]),
                "close": [8510.496, 8692.666],
            }
        )

    def test_history_current_day_is_available_after_a_share_close_confirm_cutoff(self) -> None:
        now = pd.Timestamp("2026-05-22 17:49:00+08:00")

        self.assertEqual(live.latest_closed_history_date(self.history, now), pd.Timestamp("2026-05-22"))
        self.assertEqual(v2_0.base_mod.latest_closed_history_date(self.history, now), pd.Timestamp("2026-05-22"))

    def test_history_current_day_is_blocked_before_close_confirm_cutoff(self) -> None:
        now = pd.Timestamp("2026-05-22 15:29:00+08:00")

        self.assertEqual(live.latest_closed_history_date(self.history, now), pd.Timestamp("2026-05-21"))
        self.assertEqual(v2_0.base_mod.latest_closed_history_date(self.history, now), pd.Timestamp("2026-05-21"))

    def test_close_confirmed_same_day_panel_cache_survives_longer_than_ten_minutes(self) -> None:
        now = pd.Timestamp("2026-05-22 17:49:00+08:00")
        stale_mtime = pd.Timestamp("2026-05-22 17:30:00+08:00").timestamp()

        with tempfile.TemporaryDirectory() as tmp:
            panel_shadow = Path(tmp) / "panel_shadow.csv"
            panel_shadow.write_text("date,1.000852\n2026-05-22,8692.666\n", encoding="utf-8")
            os.utime(panel_shadow, (stale_mtime, stale_mtime))

            self.assertTrue(live.panel_shadow_cache_is_reusable(panel_shadow, pd.Timestamp("2026-05-22"), now))
            self.assertTrue(
                v2_0.base_mod.panel_shadow_cache_is_reusable(panel_shadow, pd.Timestamp("2026-05-22"), now)
            )


if __name__ == "__main__":
    unittest.main()
