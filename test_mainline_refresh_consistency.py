from __future__ import annotations

import json
import shutil
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

import microcap_top100_mom16_biweekly_live as base_mod


class MainlineRefreshConsistencyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.work_dir = Path(__file__).resolve().parent / "_tmp_mainline_refresh"
        shutil.rmtree(self.work_dir, ignore_errors=True)
        self.work_dir.mkdir(parents=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.work_dir, ignore_errors=True)

    def test_ensure_strategy_files_rewrites_costed_nav_when_costed_is_stale(self) -> None:
        index_csv = self.work_dir / "index.csv"
        costed_nav_csv = self.work_dir / "costed_nav.csv"
        proxy_meta = self.work_dir / "proxy_meta.json"
        proxy_turnover = self.work_dir / "proxy_turnover.csv"

        pd.DataFrame(
            {
                "date": ["2026-04-09", "2026-04-10"],
                "close": [1.0, 1.01],
                "holding_count": [100, 100],
                "holding_effective": [True, True],
            }
        ).to_csv(index_csv, index=False, encoding="utf-8")
        pd.DataFrame(
            {
                "date": ["2026-04-09"],
                "return_net": [0.01],
                "nav_net": [1.01],
            }
        ).to_csv(costed_nav_csv, index=False, encoding="utf-8")
        proxy_meta.write_text(
            json.dumps(
                {
                    "core_params": {
                        "execution_timing": base_mod.EXECUTION_TIMING,
                        "trade_constraint_mode": base_mod.TRADE_CONSTRAINT_MODE,
                        "research_stack_version": base_mod.RESEARCH_STACK_VERSION,
                    }
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        pd.DataFrame(
            {
                "rebalance_date": ["2026-04-10"],
                "turnover_frac_one_side": [0.2],
            }
        ).to_csv(proxy_turnover, index=False, encoding="utf-8")

        args = SimpleNamespace(
            index_csv=index_csv,
            costed_nav_csv=costed_nav_csv,
            rebuild_index_if_missing=True,
            max_workers=1,
        )
        paths = {
            "proxy_meta": proxy_meta,
            "proxy_members": self.work_dir / "proxy_members.csv",
            "proxy_turnover": proxy_turnover,
        }
        gross = pd.DataFrame(
            {
                "return": [0.0, 0.02],
            },
            index=pd.to_datetime(["2026-04-09", "2026-04-10"]),
        )
        net = pd.DataFrame(
            {
                "return_net": [0.0, 0.018],
                "nav_net": [1.0, 1.018],
            },
            index=pd.to_datetime(["2026-04-09", "2026-04-10"]),
        )

        with patch.object(base_mod, "load_close_df", return_value=pd.DataFrame(index=gross.index)):
            with patch.object(base_mod, "run_signal", return_value=gross):
                with patch.object(base_mod.freq_mod.cost_mod, "apply_cost_model", return_value=net):
                    base_mod.ensure_strategy_files(
                        args=args,
                        paths=paths,
                        panel_path=self.work_dir / "panel.csv",
                        target_end_date=pd.Timestamp("2026-04-10"),
                    )

        saved = pd.read_csv(costed_nav_csv)
        self.assertEqual(saved["date"].iloc[-1], "2026-04-10")
        self.assertAlmostEqual(float(saved["nav_net"].iloc[-1]), 1.018, places=10)

    def test_ensure_strategy_files_can_extend_non_rebalance_tail_without_turnover_history(self) -> None:
        index_csv = self.work_dir / "index.csv"
        costed_nav_csv = self.work_dir / "costed_nav.csv"
        proxy_meta = self.work_dir / "proxy_meta.json"

        pd.DataFrame(
            {
                "date": ["2026-04-09", "2026-04-10"],
                "close": [1.0, 1.01],
                "holding_count": [100, 100],
                "holding_effective": [True, True],
            }
        ).to_csv(index_csv, index=False, encoding="utf-8")
        pd.DataFrame(
            {
                "date": ["2026-04-09"],
                "holding": ["long_microcap_short_zz1000"],
                "next_holding": ["long_microcap_short_zz1000"],
                "return": [0.01],
                "entry_exit_cost": [0.0],
                "rebalance_cost": [0.0],
                "total_cost": [0.0],
                "return_net": [0.01],
                "nav_net": [1.01],
            }
        ).to_csv(costed_nav_csv, index=False, encoding="utf-8")
        proxy_meta.write_text(
            json.dumps(
                {
                    "core_params": {
                        "execution_timing": base_mod.EXECUTION_TIMING,
                        "trade_constraint_mode": base_mod.TRADE_CONSTRAINT_MODE,
                        "research_stack_version": base_mod.RESEARCH_STACK_VERSION,
                    }
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        args = SimpleNamespace(
            index_csv=index_csv,
            costed_nav_csv=costed_nav_csv,
            rebuild_index_if_missing=True,
            max_workers=1,
        )
        paths = {
            "proxy_meta": proxy_meta,
            "proxy_members": self.work_dir / "proxy_members.csv",
            "proxy_turnover": self.work_dir / "proxy_turnover.csv",
        }
        gross = pd.DataFrame(
            {
                "holding": ["long_microcap_short_zz1000", "long_microcap_short_zz1000"],
                "next_holding": ["long_microcap_short_zz1000", "long_microcap_short_zz1000"],
                "return": [0.01, 0.02],
            },
            index=pd.to_datetime(["2026-04-09", "2026-04-10"]),
        )

        with patch.object(base_mod, "load_close_df", return_value=pd.DataFrame(index=gross.index)):
            with patch.object(base_mod, "run_signal", return_value=gross):
                with patch.object(
                    base_mod,
                    "refresh_price_cache_tail",
                    side_effect=AssertionError("should not trigger full rebuild for a non-rebalance tail sync"),
                ):
                    base_mod.ensure_strategy_files(
                        args=args,
                        paths=paths,
                        panel_path=self.work_dir / "panel.csv",
                        target_end_date=pd.Timestamp("2026-04-10"),
                    )

        saved = pd.read_csv(costed_nav_csv)
        self.assertEqual(saved["date"].iloc[-1], "2026-04-10")
        self.assertAlmostEqual(float(saved["return_net"].iloc[-1]), 0.02, places=10)
        self.assertAlmostEqual(float(saved["nav_net"].iloc[-1]), 1.0302, places=10)

    def test_ensure_strategy_files_rebuilds_fresh_costed_when_turnover_history_is_missing(self) -> None:
        index_csv = self.work_dir / "index.csv"
        costed_nav_csv = self.work_dir / "costed_nav.csv"
        proxy_meta = self.work_dir / "proxy_meta.json"
        panel_csv = self.work_dir / "panel.csv"

        pd.DataFrame(
            {
                "date": ["2026-04-09", "2026-04-10"],
                "close": [1.0, 1.01],
                "holding_count": [100, 100],
                "holding_effective": [True, True],
            }
        ).to_csv(index_csv, index=False, encoding="utf-8")
        pd.DataFrame(
            {
                "date": ["2026-04-09", "2026-04-10"],
                "return_net": [0.01, 0.0],
                "nav_net": [1.01, 1.01],
            }
        ).to_csv(costed_nav_csv, index=False, encoding="utf-8")
        proxy_meta.write_text(
            json.dumps(
                {
                    "core_params": {
                        "execution_timing": base_mod.EXECUTION_TIMING,
                        "trade_constraint_mode": base_mod.TRADE_CONSTRAINT_MODE,
                        "research_stack_version": base_mod.RESEARCH_STACK_VERSION,
                    }
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        pd.DataFrame({"date": ["2026-04-09", "2026-04-10"]}).to_csv(panel_csv, index=False, encoding="utf-8")

        args = SimpleNamespace(
            index_csv=index_csv,
            costed_nav_csv=costed_nav_csv,
            rebuild_index_if_missing=True,
            max_workers=1,
        )
        paths = {
            "proxy_meta": proxy_meta,
            "proxy_members": self.work_dir / "proxy_members.csv",
            "proxy_turnover": self.work_dir / "proxy_turnover.csv",
        }
        rebuilt_index = pd.DataFrame(
            {
                "date": ["2026-04-09", "2026-04-10"],
                "close": [1.0, 1.01],
                "holding_count": [100, 100],
                "holding_effective": [True, True],
            }
        )
        rebuilt_members = pd.DataFrame(
            {
                "rebalance_date": ["2026-04-10"],
                "symbol": ["000001"],
            }
        )
        rebuilt_turnover = pd.DataFrame(
            {
                "rebalance_date": ["2026-04-10"],
                "two_side_cost_rate": [0.00063],
                "execution_timing": [base_mod.EXECUTION_TIMING],
            }
        )
        rebuilt_net = pd.DataFrame(
            {
                "return_net": [0.01, 0.002],
                "nav_net": [1.01, 1.01202],
            },
            index=pd.to_datetime(["2026-04-09", "2026-04-10"]),
        )

        with patch.object(base_mod, "refresh_price_cache_tail") as refresh_mock:
            with patch.object(
                base_mod,
                "build_local_proxy_bundle",
                return_value=(rebuilt_index, rebuilt_members, rebuilt_turnover, {"core_params": {}}),
            ):
                with patch.object(
                    base_mod,
                    "rebuild_costed_nav_from_proxy_turnover",
                    side_effect=lambda args, paths, panel_path: rebuilt_net.to_csv(
                        args.costed_nav_csv, index_label="date", encoding="utf-8"
                    ),
                ):
                    base_mod.ensure_strategy_files(
                        args=args,
                        paths=paths,
                        panel_path=panel_csv,
                        target_end_date=pd.Timestamp("2026-04-10"),
                    )

        refresh_mock.assert_called_once_with(pd.Timestamp("2026-04-10"), 1)
        saved = pd.read_csv(costed_nav_csv)
        self.assertEqual(saved["date"].iloc[-1], "2026-04-10")
        self.assertAlmostEqual(float(saved["nav_net"].iloc[-1]), 1.01202, places=10)
        self.assertTrue(paths["proxy_turnover"].exists())


if __name__ == "__main__":
    unittest.main()
