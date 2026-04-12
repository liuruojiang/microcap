from __future__ import annotations

import shutil
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import top100_v1_1_mainline_tools as tools


class Top100V11MainlineToolsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.output_dir = Path(__file__).resolve().parent / "_tmp_test_outputs"
        shutil.rmtree(self.output_dir, ignore_errors=True)
        self.output_dir.mkdir(parents=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.output_dir, ignore_errors=True)

    def test_resolve_performance_source_prefers_costed_nav(self) -> None:
        costed = self.output_dir / tools.COSTED_NAV_FILENAME
        gross = self.output_dir / tools.MAINLINE_NAV_FILENAME

        pd.DataFrame(
            {
                "date": ["2026-04-09", "2026-04-10"],
                "return_net": [0.01, -0.02],
            }
        ).to_csv(costed, index=False)
        pd.DataFrame(
            {
                "date": ["2026-04-09", "2026-04-10"],
                "return": [0.03, -0.01],
            }
        ).to_csv(gross, index=False)

        path, ret_col, source_label = tools.resolve_performance_source(self.output_dir)

        self.assertEqual(path, costed)
        self.assertEqual(ret_col, "return_net")
        self.assertEqual(source_label, "costed")

    def test_build_recent_window_nav_rebases_last_three_years(self) -> None:
        returns = pd.DataFrame(
            {
                "date": pd.to_datetime(
                    ["2022-04-08", "2023-04-10", "2024-04-10", "2025-04-10", "2026-04-10"]
                ),
                "return_net": [0.10, -0.05, 0.02, 0.03, -0.01],
            }
        ).set_index("date")

        window = tools.build_recent_window_nav(returns, "return_net", years=3)

        self.assertEqual(str(window.index.min().date()), "2023-04-10")
        self.assertEqual(str(window.index.max().date()), "2026-04-10")
        self.assertAlmostEqual(window["nav_rebased"].iloc[0], 0.95, places=10)
        self.assertAlmostEqual(window["nav_rebased"].iloc[-1], 0.9880893, places=6)

    def test_plan_output_cleanup_keeps_mainline_required_files(self) -> None:
        keep_file = self.output_dir / tools.MAINLINE_SUMMARY_FILENAME
        source_file = self.output_dir / tools.COSTED_NAV_FILENAME
        stale_file = self.output_dir / "microcap_top100_mom16_biweekly_live_recent3y_curve.png"
        nested_dir = self.output_dir / "poe_command_outputs"
        nested_dir.mkdir()
        nested_file = nested_dir / "old.png"

        for path in (keep_file, source_file, stale_file, nested_file):
            path.write_text("x", encoding="utf-8")

        deletions = {path.relative_to(self.output_dir).as_posix() for path in tools.plan_output_cleanup(self.output_dir)}

        self.assertIn("microcap_top100_mom16_biweekly_live_recent3y_curve.png", deletions)
        self.assertIn("poe_command_outputs/old.png", deletions)
        self.assertNotIn(tools.MAINLINE_SUMMARY_FILENAME, deletions)
        self.assertNotIn(tools.COSTED_NAV_FILENAME, deletions)

    def test_generate_recent3y_chart_refreshes_mainline_outputs_first(self) -> None:
        events: list[str] = []
        frame = pd.DataFrame(
            {
                "return_net": [0.01, -0.02],
            },
            index=pd.to_datetime(["2026-04-09", "2026-04-10"]),
        )

        def fake_refresh() -> dict[str, object]:
            events.append("refresh")
            return {"target_end_date": pd.Timestamp("2026-04-10")}

        def fake_resolve(output_dir: Path) -> tuple[Path, str, str]:
            events.append("resolve")
            return output_dir / tools.COSTED_NAV_FILENAME, "return_net", "costed"

        def fake_render(window: pd.DataFrame, output_path: Path, title: str) -> Path:
            events.append("render")
            self.assertIn("costed", title)
            return output_path

        with patch.object(tools, "refresh_mainline_outputs", side_effect=fake_refresh, create=True):
            with patch.object(tools, "resolve_performance_source", side_effect=fake_resolve):
                with patch.object(tools, "load_returns_frame", return_value=frame):
                    with patch.object(tools, "render_recent_window_chart", side_effect=fake_render):
                        chart_path = tools.generate_recent3y_chart(self.output_dir)

        self.assertEqual(events[:2], ["refresh", "resolve"])
        self.assertEqual(chart_path, self.output_dir / tools.RECENT3Y_CHART_FILENAME)

    def test_refresh_mainline_outputs_recomputes_and_saves_gross_nav(self) -> None:
        paths = {"nav": self.output_dir / tools.MAINLINE_NAV_FILENAME}
        result = pd.DataFrame(
            {"return": [0.01, -0.02]},
            index=pd.to_datetime(["2026-04-09", "2026-04-10"]),
        )

        with patch.object(tools.v1_1_mod.base_mod, "build_output_paths", return_value=paths):
            with patch.object(
                tools.v1_1_mod.base_mod,
                "build_refreshed_panel_shadow",
                return_value=(self.output_dir / "panel.csv", pd.Timestamp("2026-04-10")),
            ):
                with patch.object(tools.v1_1_mod.base_mod, "ensure_strategy_files") as ensure_strategy_files:
                        with patch.object(tools.v1_1_mod.base_mod, "load_close_df", return_value=pd.DataFrame(index=result.index)):
                            with patch.object(tools.v1_1_mod.base_mod, "run_signal", return_value=result) as run_signal:
                                with patch.object(tools, "synchronize_costed_nav_dates") as synchronize_costed_nav_dates:
                                    state = tools.refresh_mainline_outputs()

        self.assertEqual(ensure_strategy_files.call_count, 1)
        self.assertEqual(run_signal.call_count, 1)
        self.assertEqual(synchronize_costed_nav_dates.call_count, 1)
        saved = pd.read_csv(paths["nav"])
        self.assertEqual(saved["date"].iloc[-1], "2026-04-10")
        self.assertIs(state["result"], result)

    def test_synchronize_costed_nav_dates_drops_unexpected_rows(self) -> None:
        costed_path = self.output_dir / tools.COSTED_NAV_FILENAME
        pd.DataFrame(
            {
                "date": ["2026-02-05", "2026-04-09", "2026-04-10"],
                "return_net": [0.1, 0.0, 0.0],
            }
        ).to_csv(costed_path, index=False, encoding="utf-8-sig")

        tools.synchronize_costed_nav_dates(
            pd.to_datetime(["2026-04-09", "2026-04-10"]),
            costed_path,
        )

        saved = pd.read_csv(costed_path)
        self.assertEqual(saved["date"].tolist(), ["2026-04-09", "2026-04-10"])


if __name__ == "__main__":
    unittest.main()
