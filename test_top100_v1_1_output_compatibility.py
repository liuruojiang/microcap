from __future__ import annotations

import json
import shutil
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import microcap_top100_mom16_biweekly_live_v1_1 as v1_1_mod
import top100_v1_1_mainline_tools as tools_mod


class V11OutputCompatibilityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.work_dir = Path(__file__).resolve().parent / "_tmp_v1_1_output_compatibility"
        shutil.rmtree(self.work_dir, ignore_errors=True)
        self.work_dir.mkdir(parents=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.work_dir, ignore_errors=True)

    def _build_paths(self) -> tuple[dict[str, Path], Path]:
        paths = {
            "summary": self.work_dir / "v1_1_summary.json",
            "nav": self.work_dir / "v1_1_nav.csv",
            "proxy_turnover": self.work_dir / "v1_1_proxy_turnover.csv",
            "proxy_meta": self.work_dir / "v1_1_proxy_meta.json",
            "proxy_members": self.work_dir / "v1_1_proxy_members.csv",
            "signal": self.work_dir / "v1_1_signal.csv",
            "members": self.work_dir / "v1_1_members.csv",
            "changes": self.work_dir / "v1_1_changes.csv",
            "realtime_signal": self.work_dir / "v1_1_realtime_signal.csv",
            "realtime_members": self.work_dir / "v1_1_realtime_members.csv",
            "realtime_changes": self.work_dir / "v1_1_realtime_changes.csv",
            "performance_summary": self.work_dir / "v1_1_perf_summary.csv",
            "performance_yearly": self.work_dir / "v1_1_perf_yearly.csv",
            "performance_nav": self.work_dir / "v1_1_perf_nav.csv",
            "performance_chart": self.work_dir / "v1_1_perf_chart.png",
            "performance_json": self.work_dir / "v1_1_perf.json",
            "cache_static_meta": self.work_dir / "cache_static_meta.json",
            "cache_static_target": self.work_dir / "cache_static_target.csv",
            "cache_static_effective": self.work_dir / "cache_static_effective.csv",
            "cache_static_changes": self.work_dir / "cache_static_changes.csv",
            "cache_realtime_meta": self.work_dir / "cache_realtime_meta.json",
            "cache_realtime_members": self.work_dir / "cache_realtime_members.csv",
            "cache_realtime_changes": self.work_dir / "cache_realtime_changes.csv",
            "cache_realtime_signal": self.work_dir / "cache_realtime_signal.csv",
            "cache_fast_realtime_meta": self.work_dir / "cache_fast_realtime_meta.json",
            "cache_fast_realtime_signal": self.work_dir / "cache_fast_realtime_signal.csv",
            "panel_shadow": self.work_dir / "panel_shadow.csv",
        }
        costed_path = self.work_dir / "v1_1_costed.csv"
        return paths, costed_path

    def test_summary_is_current_v1_1_requires_backup_role(self) -> None:
        self.assertTrue(
            v1_1_mod.summary_is_current_v1_1(
                {
                    "version": "1.1",
                    "version_role": "backup_alternative",
                    "version_note": "Backup alternative to v1.0. Same live framework as v1.0.",
                }
            )
        )
        self.assertFalse(
            v1_1_mod.summary_is_current_v1_1(
                {
                    "version": "1.1",
                    "version_role": "mainline",
                    "version_note": "Primary mainline version. Same live framework as v1.0.",
                }
            )
        )

    def test_invalidate_incompatible_outputs_removes_stale_v1_1_files(self) -> None:
        paths, costed_path = self._build_paths()
        legacy_summary = {
            "version": "1.1",
            "version_role": "mainline",
            "version_note": "Primary mainline version. Same live framework as v1.0, but fixed hedge ratio is reduced from 1.0x to 0.8x.",
        }
        paths["summary"].write_text(json.dumps(legacy_summary, ensure_ascii=False), encoding="utf-8")
        for key, path in paths.items():
            if key == "summary":
                continue
            path.write_text("stale", encoding="utf-8")
        costed_path.write_text("stale", encoding="utf-8")

        removed = v1_1_mod.invalidate_incompatible_outputs(paths=paths, costed_nav_csv=costed_path)

        self.assertTrue(removed)
        self.assertFalse(paths["summary"].exists())
        self.assertFalse(paths["proxy_turnover"].exists())
        self.assertFalse(paths["nav"].exists())
        self.assertFalse(costed_path.exists())

    def test_invalidate_incompatible_outputs_keeps_current_outputs(self) -> None:
        paths, costed_path = self._build_paths()
        current_summary = {
            "version": "1.1",
            "version_role": "backup_alternative",
            "version_note": "Backup alternative to v1.0. Same live framework as v1.0, but fixed hedge ratio is reduced from 1.0x to 0.8x.",
        }
        paths["summary"].write_text(json.dumps(current_summary, ensure_ascii=False), encoding="utf-8")
        paths["nav"].write_text("fresh", encoding="utf-8")
        costed_path.write_text("fresh", encoding="utf-8")

        removed = v1_1_mod.invalidate_incompatible_outputs(paths=paths, costed_nav_csv=costed_path)

        self.assertEqual(removed, [])
        self.assertTrue(paths["summary"].exists())
        self.assertTrue(paths["nav"].exists())
        self.assertTrue(costed_path.exists())

    def test_costed_nav_matches_current_hedge_ratio_rejects_old_spread_formula(self) -> None:
        _, costed_path = self._build_paths()
        pd.DataFrame(
            [
                {
                    "date": "2026-04-10",
                    "holding": "long_microcap_short_zz1000",
                    "microcap_ret": 0.01,
                    "hedge_ret": 0.02,
                    "futures_drag": 0.00024,
                    "return_raw": 0.01 - 0.02 - 0.00024,
                }
            ]
        ).to_csv(costed_path, index=False, encoding="utf-8")

        self.assertFalse(v1_1_mod.costed_nav_matches_current_hedge_ratio(costed_path, hedge_ratio=0.8))
        self.assertTrue(v1_1_mod.costed_nav_matches_current_hedge_ratio(costed_path, hedge_ratio=1.0))

    def test_seed_proxy_bundle_from_v1_0_copies_current_mainline_proxy_files(self) -> None:
        paths, _ = self._build_paths()
        source_dir = self.work_dir / "source"
        source_dir.mkdir(parents=True)
        source_paths = {
            "proxy_meta": source_dir / "v1_0_proxy_meta.json",
            "proxy_members": source_dir / "v1_0_proxy_members.csv",
            "proxy_turnover": source_dir / "v1_0_proxy_turnover.csv",
        }
        source_paths["proxy_meta"].write_text(
            json.dumps(
                {
                    "core_params": {
                        "execution_timing": v1_1_mod.base_mod.EXECUTION_TIMING,
                        "trade_constraint_mode": v1_1_mod.base_mod.TRADE_CONSTRAINT_MODE,
                        "research_stack_version": v1_1_mod.base_mod.RESEARCH_STACK_VERSION,
                    }
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        source_paths["proxy_members"].write_text("date,symbol\n2026-04-10,000001\n", encoding="utf-8")
        source_paths["proxy_turnover"].write_text("date,turnover\n2026-04-10,0.1\n", encoding="utf-8")

        with patch.object(v1_1_mod.base_mod, "build_output_paths", return_value=source_paths):
            copied = v1_1_mod.seed_proxy_bundle_from_v1_0(paths)

        self.assertEqual({path.name for path in copied}, {"v1_1_proxy_meta.json", "v1_1_proxy_members.csv", "v1_1_proxy_turnover.csv"})
        self.assertEqual(paths["proxy_members"].read_text(encoding="utf-8"), "date,symbol\n2026-04-10,000001\n")

    def test_prepare_current_v1_1_outputs_seeds_proxy_bundle_when_summary_missing(self) -> None:
        paths, costed_path = self._build_paths()

        with patch.object(v1_1_mod, "invalidate_incompatible_outputs", return_value=[]):
            with patch.object(v1_1_mod, "seed_proxy_bundle_from_v1_0", return_value=[paths["proxy_turnover"]]) as seed_mock:
                state = v1_1_mod.prepare_current_v1_1_outputs(paths=paths, costed_nav_csv=costed_path)

        seed_mock.assert_called_once_with(paths)
        self.assertEqual(state, {"removed": [], "copied": [paths["proxy_turnover"]]})

    def test_prepare_current_v1_1_outputs_invalidates_costed_nav_when_formula_is_stale(self) -> None:
        paths, costed_path = self._build_paths()
        costed_path.write_text("stale", encoding="utf-8")

        with patch.object(v1_1_mod, "invalidate_incompatible_outputs", return_value=[]):
            with patch.object(v1_1_mod, "costed_nav_matches_current_hedge_ratio", return_value=False):
                with patch.object(v1_1_mod, "seed_proxy_bundle_from_v1_0", return_value=[]):
                    state = v1_1_mod.prepare_current_v1_1_outputs(paths=paths, costed_nav_csv=costed_path)

        self.assertFalse(costed_path.exists())
        self.assertEqual(state["removed"], [costed_path])

    def test_refresh_mainline_outputs_prepares_outputs_before_reuse_check(self) -> None:
        args = tools_mod.build_mainline_args()
        paths, _ = self._build_paths()
        fake_result = pd.DataFrame({"return": [0.0]}, index=pd.to_datetime(["2026-04-10"]))
        fake_close_df = pd.DataFrame({"close": [1.0]}, index=pd.to_datetime(["2026-04-10"]))

        with patch.object(tools_mod, "build_mainline_args", return_value=args):
            with patch.object(tools_mod.v1_1_mod.base_mod, "build_output_paths", return_value=paths):
                with patch.object(tools_mod.v1_1_mod, "prepare_current_v1_1_outputs") as prepare_mock:
                    with patch.object(tools_mod.v1_1_mod.base_mod, "build_refreshed_panel_shadow", return_value=(Path("panel.csv"), pd.Timestamp("2026-04-10"))):
                        with patch.object(tools_mod.v1_1_mod.base_mod, "ensure_strategy_files") as ensure_mock:
                            with patch.object(tools_mod.v1_1_mod.base_mod, "load_close_df", return_value=fake_close_df):
                                with patch.object(tools_mod.v1_1_mod.base_mod, "run_signal", return_value=fake_result):
                                    with patch.object(tools_mod, "synchronize_costed_nav_dates"):
                                        tools_mod.refresh_mainline_outputs()

        prepare_mock.assert_called_once_with(paths=paths, costed_nav_csv=args.costed_nav_csv)
        ensure_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
