from __future__ import annotations

import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

import microcap_top100_mom16_biweekly_live as live_mod


class Top100RealtimeQueryFastPathTests(unittest.TestCase):
    def setUp(self) -> None:
        self.args = SimpleNamespace(output_prefix="microcap_top100_mom16_biweekly_live")
        self.paths = {"proxy_meta": Path("outputs/proxy_meta.json")}
        self.panel_path = Path("outputs/panel_shadow.csv")
        self.target_end_date = pd.Timestamp("2026-04-13")
        self.base_context = {"latest_signal": "stub"}
        self.member_context = {"effective_members": "stub"}

    def test_execute_query_realtime_signal_skips_strategy_refresh(self) -> None:
        with patch.object(live_mod, "build_output_paths", return_value=self.paths):
            with patch.object(live_mod, "refresh_history_anchor", return_value=(self.panel_path, self.target_end_date)):
                with patch.object(live_mod, "classify_query_kind", return_value="realtime_signal"):
                    with patch.object(
                        live_mod,
                        "ensure_realtime_query_base_context",
                        return_value=self.base_context,
                        create=True,
                    ) as realtime_base_mock:
                        with patch.object(
                            live_mod,
                            "ensure_base_signal_fresh",
                            side_effect=AssertionError("slow path used for realtime signal"),
                        ):
                            with patch.object(
                                live_mod,
                                "ensure_static_members_fresh",
                                return_value=self.member_context,
                            ) as static_mock:
                                with patch.object(live_mod, "handle_query") as handle_query_mock:
                                    live_mod.execute_query(self.args, "实时信号")

        realtime_base_mock.assert_called_once_with(self.args, self.paths, self.panel_path, self.target_end_date)
        static_mock.assert_called_once_with(
            self.args,
            self.paths,
            self.panel_path,
            self.target_end_date,
            self.base_context,
        )
        handle_query_mock.assert_called_once_with(self.member_context, self.args, "实时信号")

    def test_execute_query_realtime_changes_skips_strategy_refresh(self) -> None:
        with patch.object(live_mod, "build_output_paths", return_value=self.paths):
            with patch.object(live_mod, "refresh_history_anchor", return_value=(self.panel_path, self.target_end_date)):
                with patch.object(live_mod, "classify_query_kind", return_value="realtime_changes"):
                    with patch.object(
                        live_mod,
                        "ensure_realtime_query_base_context",
                        return_value=self.base_context,
                        create=True,
                    ) as realtime_base_mock:
                        with patch.object(
                            live_mod,
                            "ensure_base_signal_fresh",
                            side_effect=AssertionError("slow path used for realtime changes"),
                        ):
                            with patch.object(
                                live_mod,
                                "ensure_static_members_fresh",
                                return_value=self.member_context,
                            ) as static_mock:
                                with patch.object(live_mod, "handle_query") as handle_query_mock:
                                    live_mod.execute_query(self.args, "实时进出名单")

        realtime_base_mock.assert_called_once_with(self.args, self.paths, self.panel_path, self.target_end_date)
        static_mock.assert_called_once_with(
            self.args,
            self.paths,
            self.panel_path,
            self.target_end_date,
            self.base_context,
        )
        handle_query_mock.assert_called_once_with(self.member_context, self.args, "实时进出名单")


if __name__ == "__main__":
    unittest.main()
