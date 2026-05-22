from __future__ import annotations

import argparse
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd

import top100_realtime_core as core
from scripts import realtime_state_bundle


class Top100RealtimeCoreStateOnlyTests(unittest.TestCase):
    def test_state_only_refreshes_panel_shadow_before_loading_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            panel_path = root / "panel_shadow.csv"
            index_csv = root / "index.csv"
            costed_csv = root / "costed.csv"
            panel_path.write_text("date,1.000852\n2026-05-21,1\n", encoding="utf-8")
            index_csv.write_text("date,close\n2026-05-21,1\n", encoding="utf-8")
            costed_csv.write_text("date,nav\n2026-05-21,1\n", encoding="utf-8")
            args = argparse.Namespace(index_csv=index_csv, costed_nav_csv=costed_csv)
            base_paths = {"panel_shadow": panel_path}
            context = {"close_df": pd.DataFrame({"microcap": [1.0]}, index=[pd.Timestamp("2026-05-21")])}

            with (
                mock.patch.object(
                    core.base_mod,
                    "build_refreshed_panel_shadow",
                    return_value=(panel_path, pd.Timestamp("2026-05-21")),
                ) as refresh_panel,
                mock.patch.object(core.base_mod, "build_realtime_context_from_cached_proxy", return_value=context),
            ):
                _panel_path, target_end_date, base_context = core.cached_realtime_context_from_existing_state(
                    args,
                    base_paths,
                    "test",
                )

        refresh_panel.assert_called_once_with(args, base_paths)
        self.assertEqual(target_end_date, pd.Timestamp("2026-05-21"))
        self.assertIs(base_context, context)

    def test_state_only_rejects_aligned_context_older_than_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            panel_path = root / "panel_shadow.csv"
            index_csv = root / "index.csv"
            costed_csv = root / "costed.csv"
            panel_path.write_text("date,1.000852\n2026-05-21,1\n", encoding="utf-8")
            index_csv.write_text("date,close\n2026-05-21,1\n", encoding="utf-8")
            costed_csv.write_text("date,nav\n2026-05-21,1\n", encoding="utf-8")
            args = argparse.Namespace(index_csv=index_csv, costed_nav_csv=costed_csv)
            base_paths = {"panel_shadow": panel_path}
            context = {"close_df": pd.DataFrame({"microcap": [1.0]}, index=[pd.Timestamp("2026-05-20")])}

            with (
                mock.patch.object(
                    core.base_mod,
                    "build_refreshed_panel_shadow",
                    return_value=(panel_path, pd.Timestamp("2026-05-21")),
                ),
                mock.patch.object(core.base_mod, "build_realtime_context_from_cached_proxy", return_value=context),
            ):
                with self.assertRaisesRegex(RuntimeError, "close_df_last_date=2026-05-20"):
                    core.cached_realtime_context_from_existing_state(args, base_paths, "test")

    def test_state_bundle_requires_panel_shadow(self) -> None:
        self.assertIn(
            "outputs/microcap_top100_mom16_biweekly_live_v1_1_panel_refreshed.csv",
            realtime_state_bundle.REQUIRED_FILES,
        )


if __name__ == "__main__":
    unittest.main()
