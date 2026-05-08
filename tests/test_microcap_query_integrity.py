from __future__ import annotations

import unittest
from pathlib import Path
import tempfile

import pandas as pd

import microcap_top100_mom16_biweekly_live as base_mod
import microcap_top100_mom16_biweekly_live_v1_6 as v16


class MicrocapQueryIntegrityTests(unittest.TestCase):
    def test_bare_one_year_performance_query_uses_trailing_year(self) -> None:
        now = pd.Timestamp("2026-05-08")

        start, end, label = base_mod.parse_date_range("表现 1年", now=now)

        self.assertEqual(pd.Timestamp("2025-05-08"), start)
        self.assertEqual(now, end)
        self.assertEqual("last_1_years", label)

    def test_v1_6_target_vol_preserves_zeroed_quality_mask_pnl(self) -> None:
        dates = pd.bdate_range("2025-01-01", periods=70)
        base = pd.DataFrame(
            {
                "return_raw": 0.01,
                "return": 0.01,
                "return_net": 0.01,
                "overlay_pre_cost_return": 0.01,
                "total_cost": 0.0,
                "holding": "long_microcap_short_zz1000",
                "next_holding": "long_microcap_short_zz1000",
            },
            index=dates,
        )
        masked_date = dates[-1]
        base.loc[masked_date, "return_raw"] = -0.04
        base.loc[masked_date, "return"] = -0.04
        base.loc[masked_date, "return_net"] = 0.0
        base.loc[masked_date, "overlay_pre_cost_return"] = 0.0

        out = v16.apply_target_vol_scaling(base)

        self.assertEqual("overlay_pre_cost_return", out.loc[masked_date, "base_pre_cost_return_source"])
        self.assertEqual(0.0, out.loc[masked_date, "base_pre_cost_return"])
        self.assertGreater(out.loc[masked_date, "current_execution_scale"], 0.0)
        self.assertLess(abs(out.loc[masked_date, "return_net"]), 0.002)

    def test_historical_rewrite_guard_rejects_old_date_changes(self) -> None:
        dates = pd.bdate_range("2026-01-01", periods=10)
        previous = pd.DataFrame(
            {
                "date": dates,
                "return_net": [0.001] * 10,
                "nav_net": (1.001 ** pd.Series(range(1, 11))).to_list(),
                "holding": ["cash"] * 10,
                "next_holding": ["cash"] * 10,
            }
        )
        candidate = previous.copy()
        candidate.loc[3, "return_net"] = -0.05

        with self.assertRaisesRegex(RuntimeError, "historical rewrite"):
            base_mod.assert_no_historical_rewrite(
                previous=previous,
                candidate=candidate,
                key_columns=["return_net", "holding", "next_holding"],
                allowed_tail_rows=1,
                label="unit_test_nav",
            )

    def test_performance_outputs_reject_duplicate_dates(self) -> None:
        perf_df = pd.DataFrame(
            {"return_net": [0.01, -0.02], "nav_net": [1.01, 0.9898]},
            index=[pd.Timestamp("2026-01-02"), pd.Timestamp("2026-01-02")],
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = {
                "performance_summary": root / "summary.csv",
                "performance_yearly": root / "yearly.csv",
                "performance_nav": root / "nav.csv",
                "performance_chart": root / "curve.png",
                "performance_json": root / "summary.json",
            }
            with self.assertRaisesRegex(ValueError, "duplicate dates"):
                base_mod.build_performance_outputs(
                    perf_df=perf_df,
                    ret_col="return_net",
                    nav_col="nav_net",
                    source_label="test",
                    query_text="表现 全部",
                    paths=paths,
                )

    def test_costed_tail_extension_uses_proxy_turnover_rebalance_dates(self) -> None:
        gross_dates = pd.bdate_range("2010-01-07", "2026-05-08")
        fallback_rebalances = base_mod.build_biweekly_rebalance_dates(pd.DatetimeIndex(gross_dates))
        self.assertIn(pd.Timestamp("2026-05-07"), fallback_rebalances)

        with tempfile.TemporaryDirectory() as tmp:
            turnover_path = Path(tmp) / "turnover.csv"
            pd.DataFrame({"rebalance_date": ["2026-04-16", "2026-04-30"]}).to_csv(turnover_path, index=False)

            missing = base_mod.find_missing_cost_rebalances(
                gross_index=pd.DatetimeIndex(gross_dates),
                current_costed_end=pd.Timestamp("2026-04-30"),
                target_end_date=pd.Timestamp("2026-05-08"),
                proxy_turnover_path=turnover_path,
            )

        self.assertEqual([], list(missing))


if __name__ == "__main__":
    unittest.main()
