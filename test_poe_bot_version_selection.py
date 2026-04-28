from __future__ import annotations

import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from poe_bots import microcap_top100_poe_bot as bot


class PoeBotVersionSelectionTests(unittest.TestCase):
    def tearDown(self) -> None:
        bot.set_active_strategy("1.0")

    def _base_context(self) -> dict:
        return {
            "latest_signal": pd.DataFrame(
                [
                    {
                        "date": pd.Timestamp("2026-04-17"),
                        "next_holding": "cash",
                        "current_holding": "cash",
                        "trade_state": "hold",
                        "momentum_trade_state": "hold",
                        "member_rebalance_label": "\u540d\u5355\u4e0d\u53d8",
                        "member_enter_count": 0,
                        "member_exit_count": 0,
                        "microcap_close": 100.0,
                        "hedge_close": 200.0,
                        "microcap_mom": 0.01,
                        "hedge_mom": 0.02,
                        "momentum_gap": -0.01,
                        "ratio_r2": 0.5,
                    }
                ]
            ),
            "changes_df": pd.DataFrame(),
            "freshness": {"latest_trade_date": "2026-04-17"},
            "rebuild_meta": {
                "candidate_pool": 500,
                "history_symbols_ok": 500,
                "history_symbols_failed": 0,
                "strict_validated": True,
                "validated_exact_pools": [500],
                "last_trade_signal_date": None,
                "last_trade_signal_action": None,
            },
            "latest_rebalance": "2026-04-17",
            "effective_rebalance": "2026-04-17",
            "rebalance_effective_date": "2026-04-18",
        }

    def _official_v1_4_context(self) -> dict:
        context = self._base_context()
        context["latest_signal"] = pd.DataFrame(
            [
                {
                    "date": pd.Timestamp("2026-04-28"),
                    "version": "1.4",
                    "base_version": "1.1",
                    "signal_label": "cash",
                    "current_holding": "cash",
                    "next_holding": "cash",
                    "trade_state": "hold",
                    "momentum_trade_state": "hold",
                    "member_rebalance_label": "名单调仓（调入 15，调出 15）",
                    "member_enter_count": 15,
                    "member_exit_count": 15,
                    "microcap_close": 982677.8014313146,
                    "hedge_close": 8226.692,
                    "microcap_mom": 0.06209502500488595,
                    "hedge_mom": 0.0915654273810469,
                    "momentum_gap": -0.029470402376160942,
                    "execution_scale": 0.0,
                    "fixed_hedge_ratio": 0.8,
                    "momentum_gap_exit_buffer": 0.0025,
                    "decay_ratio_threshold": 0.25,
                    "derisk_scale": 0.0,
                    "recovery_ratio_threshold": 0.35,
                    "signal_quality_derisk_triggered": False,
                }
            ]
        )
        context["close_df"] = pd.DataFrame(
            {"microcap": [982677.8014313146], "hedge": [8226.692]},
            index=pd.to_datetime(["2026-04-28"]),
        )
        context["result"] = context["latest_signal"].assign(holding="cash").set_index("date")
        context["latest_rebalance"] = "2026-04-16"
        context["effective_rebalance"] = "2026-04-16"
        context["rebalance_effective_date"] = "2026-04-17"
        context["freshness"] = {"latest_trade_date": "2026-04-28"}
        context["rebuild_meta"].update(
            {
                "candidate_pool": "official_v1_4_source",
                "history_symbols_ok": 0,
                "validated_exact_pools": ["official_v1_4"],
            }
        )
        return context

    def _performance_df(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-04-16", "2026-04-17"]),
                "return_net": [0.0, 0.01],
                "nav_net": [1.0, 1.01],
            }
        ).set_index("date")

    def _overlay_close_df(self) -> pd.DataFrame:
        dates = pd.date_range("2026-03-01", periods=34, freq="D")
        microcap = [
            100,
            102,
            104,
            106,
            108,
            110,
            112,
            114,
            116,
            118,
            120,
            122,
            124,
            126,
            128,
            130,
            132,
            134,
            136,
            138,
            140,
            142,
            144,
            146,
            148,
            150,
            152,
            154,
            156,
            145,
            136,
            130,
            126,
            124,
        ]
        return pd.DataFrame(
            {"microcap": microcap, "hedge": [100.0] * len(dates)},
            index=dates,
        )

    def test_v1_4_selected_backtest_latest_signal_includes_overlay_columns(self) -> None:
        bot.set_active_strategy("1.4")
        result = bot.run_selected_strategy_backtest(self._overlay_close_df())
        signal = bot.build_latest_signal(result)
        row = signal.iloc[0]
        for col in [
            "execution_scale",
            "signal_quality_derisk_triggered",
            "decay_ratio_threshold",
            "derisk_scale",
            "recovery_ratio_threshold",
            "momentum_gap_exit_buffer",
            "overlay_type",
            "version",
        ]:
            self.assertIn(col, signal.columns)
        self.assertEqual(row["version"], "1.4")
        self.assertEqual(row["overlay_type"], "momentum_gap_peak_decay_derisk_new_peak_guard")
        self.assertEqual(float(row["momentum_gap_exit_buffer"]), 0.0025)

    def test_v1_5_selected_backtest_latest_signal_includes_overlay_and_nav_control_columns(self) -> None:
        bot.set_active_strategy("1.5")
        result = bot.run_selected_strategy_backtest(self._overlay_close_df())
        signal = bot.build_latest_signal(result)
        row = signal.iloc[0]
        for col in [
            "execution_scale",
            "signal_quality_derisk_triggered",
            "decay_ratio_threshold",
            "derisk_scale",
            "recovery_ratio_threshold",
            "overlay_type",
            "version",
            "nav_control_scale_last_applied",
            "nav_control_scale_next_session",
            "nav_control_state_last_applied",
            "nav_control_state_next_session",
            "nav_control_drawdown_last_close",
            "effective_hedge_ratio_last_applied",
            "effective_hedge_ratio_next_session",
            "nav_control_config",
        ]:
            self.assertIn(col, signal.columns)
        self.assertEqual(row["version"], "1.5")
        self.assertEqual(row["overlay_type"], "momentum_gap_peak_decay_exit_new_peak_guard_on_v1_2")

    def test_v1_6_selected_backtest_latest_signal_includes_target_vol_columns(self) -> None:
        bot.set_active_strategy("1.6")
        result = bot.run_selected_strategy_backtest(self._overlay_close_df())
        signal = bot.build_latest_signal(result)
        row = signal.iloc[0]
        for col in [
            "execution_scale",
            "target_vol",
            "target_vol_window",
            "target_vol_realized_vol",
            "target_vol_scale_next_session",
            "scale_change_cost",
            "financing_cost",
            "momentum_gap_exit_buffer",
            "overlay_type",
            "version",
        ]:
            self.assertIn(col, signal.columns)
        self.assertEqual(row["version"], "1.6")
        self.assertEqual(row["overlay_type"], "target_volatility_scaling")
        self.assertEqual(row["base_version"], "1.4")
        self.assertEqual(float(row["target_vol"]), 0.15)
        self.assertEqual(float(row["momentum_gap_exit_buffer"]), bot.TARGET_VOL_MOMENTUM_GAP_EXIT_BUFFER)
        self.assertEqual(float(bot.STRATEGIES["1.6"]["momentum_gap_exit_buffer"]), 0.003)
        self.assertLessEqual(float(result["execution_scale"].max()), 1.5)

    def test_realtime_signal_uses_selected_strategy_backtest_for_overlay_columns(self) -> None:
        bot.set_active_strategy("1.5")
        context = self._base_context()
        context["close_df"] = self._overlay_close_df()
        context["effective_members"] = pd.DataFrame(
            [
                {"symbol": "000001", "name": "A"},
                {"symbol": "000002", "name": "B"},
            ]
        )
        context["target_members"] = context["effective_members"].copy()
        context["result"] = bot.run_backtest(context["close_df"])
        overlay_result = bot.run_selected_strategy_backtest(context["close_df"])
        quote_frame = pd.DataFrame(
            [
                {"code": "000001", "name": "A", "rt_price": 10.5},
                {"code": "000002", "name": "B", "rt_price": 20.5},
            ]
        )
        histories = {
            "000001": pd.DataFrame({"date": [context["close_df"].index[-1]], "close_raw": [10.0]}),
            "000002": pd.DataFrame({"date": [context["close_df"].index[-1]], "close_raw": [20.0]}),
        }
        with patch.object(bot, "STRICT_EXACT_MODE", False):
            with patch.object(bot, "build_context", return_value=context):
                with patch.object(bot, "build_realtime_quote_frame", return_value=(quote_frame, "test_quotes")):
                    with patch.object(bot, "fetch_candidate_histories", return_value=(histories, [])):
                        with patch.object(bot, "fetch_hedge_realtime_quote", return_value=101.0):
                            with patch.object(bot, "run_selected_strategy_backtest", return_value=overlay_result) as run_mock:
                                with patch.object(
                                    bot,
                                    "build_thread_context_attachment",
                                    return_value=("thread.json.gz", b"{}", "application/gzip"),
                                ):
                                    body, csv_bytes, _attachments = bot.handle_realtime_signal()
        run_mock.assert_called_once()
        csv_text = csv_bytes.decode("utf-8-sig")
        self.assertIn("nav_control_scale_last_applied", csv_text)
        self.assertIn("execution_scale", csv_text)
        self.assertIn("NAV 节流状态", body)

    def test_realtime_signal_uses_member_quotes_without_universe_refresh(self) -> None:
        member_symbols = [f"{idx:06d}" for idx in range(1, bot.TOP_N + 1)]
        context = self._base_context()
        context["close_df"] = self._overlay_close_df()
        context["effective_members"] = pd.DataFrame(
            [{"symbol": symbol, "name": f"S{idx}"} for idx, symbol in enumerate(member_symbols, start=1)]
        )
        histories = {
            symbol: pd.DataFrame({"date": [context["close_df"].index[-1]], "close_raw": [10.0]})
            for symbol in member_symbols
        }
        quote_frame = pd.DataFrame(
            [{"code": symbol, "name": f"S{idx}", "rt_price": 10.1} for idx, symbol in enumerate(member_symbols, start=1)]
        )

        with patch.object(bot, "STRICT_EXACT_MODE", True):
            with patch.object(bot, "build_context", return_value=context):
                with patch.object(
                    bot,
                    "fetch_universe_spot",
                    side_effect=AssertionError("full universe refresh should not be required for realtime signal"),
                ):
                    with patch.object(bot, "fetch_realtime_quotes", return_value=quote_frame):
                        with patch.object(bot, "fetch_candidate_histories", return_value=(histories, [])):
                            with patch.object(bot, "fetch_hedge_realtime_quote", return_value=101.0):
                                with patch.object(
                                    bot,
                                    "build_thread_context_attachment",
                                    return_value=("thread.json.gz", b"{}", "application/gzip"),
                                ):
                                    body, csv_bytes, _attachments = bot.handle_realtime_signal()

        csv_text = csv_bytes.decode("utf-8-sig")
        self.assertIn("eastmoney_stock_get_member_only", csv_text)
        self.assertIn("成分股有效报价：100 / 100", body)

    def test_v1_4_realtime_signal_uses_official_source_not_proxy_rebuild(self) -> None:
        bot.set_active_strategy("1.4")
        context = self._official_v1_4_context()

        with patch.object(bot, "build_official_v1_4_context", return_value=context) as official_mock:
            with patch.object(bot, "build_context", side_effect=AssertionError("v1.4 must not use Poe proxy context")):
                with patch.object(
                    bot,
                    "fetch_candidate_histories",
                    side_effect=AssertionError("v1.4 official realtime must not append an intraday proxy row"),
                ):
                    with patch.object(
                        bot,
                        "build_thread_context_attachment",
                        return_value=("thread.json.gz", b"{}", "application/gzip"),
                    ):
                        body, csv_bytes, _attachments = bot.handle_realtime_signal()

        csv_text = csv_bytes.decode("utf-8-sig")
        official_mock.assert_called_once()
        self.assertIn("momentum_gap_exit_buffer", csv_text)
        self.assertIn("982677.8014313146", csv_text)
        self.assertIn("-0.029470402376160942", csv_text)
        self.assertIn("不使用 Poe 代理指数近似", body)
        self.assertIn("信号结论", body)

    def test_after_a_share_close_requires_same_trade_date(self) -> None:
        context = self._base_context()
        context["close_df"] = pd.DataFrame(
            {"microcap": [100.0], "hedge": [200.0]},
            index=pd.to_datetime(["2026-04-17"]),
        )

        self.assertTrue(bot.is_after_a_share_close_for_context(context, pd.Timestamp("2026-04-17 15:01:00")))
        self.assertFalse(bot.is_after_a_share_close_for_context(context, pd.Timestamp("2026-04-17 14:59:00")))
        self.assertFalse(bot.is_after_a_share_close_for_context(context, pd.Timestamp("2026-04-18 15:01:00")))

    def test_realtime_signal_allows_small_quote_gap_with_disclosure(self) -> None:
        member_symbols = [f"{idx:06d}" for idx in range(1, bot.TOP_N + 1)]
        context = self._base_context()
        context["close_df"] = self._overlay_close_df()
        context["effective_members"] = pd.DataFrame(
            [{"symbol": symbol, "name": f"S{idx}"} for idx, symbol in enumerate(member_symbols, start=1)]
        )
        histories = {
            symbol: pd.DataFrame({"date": [context["close_df"].index[-1]], "close_raw": [10.0]})
            for symbol in member_symbols
        }
        quote_frame = pd.DataFrame(
            [{"code": symbol, "name": f"S{idx}", "rt_price": 10.1} for idx, symbol in enumerate(member_symbols[:99], start=1)]
        )

        with patch.object(bot, "STRICT_EXACT_MODE", True):
            with patch.object(bot, "DEFAULT_MIN_REALTIME_MEMBER_QUOTES", 98):
                with patch.object(bot, "build_context", return_value=context):
                    with patch.object(bot, "fetch_universe_spot", side_effect=RuntimeError("universe down")):
                        with patch.object(bot, "fetch_realtime_quotes", return_value=quote_frame):
                            with patch.object(bot, "fetch_candidate_histories", return_value=(histories, [])):
                                with patch.object(bot, "fetch_hedge_realtime_quote", side_effect=RuntimeError("hedge quote down")):
                                    with patch.object(
                                        bot,
                                        "build_thread_context_attachment",
                                        return_value=("thread.json.gz", b"{}", "application/gzip"),
                                    ):
                                        body, csv_bytes, _attachments = bot.handle_realtime_signal()

        csv_text = csv_bytes.decode("utf-8-sig")
        self.assertIn(",99,100,98,", csv_text)
        self.assertIn("latest_cached_close_fallback", csv_text)
        self.assertIn("成分股有效报价：99 / 100", body)
        self.assertIn("最低报价要求：98 / 100", body)
        self.assertIn("对冲腿价格来源：latest_cached_close_fallback", body)

    def test_script_mode_from_poe_bots_dir_can_import_repo_root_modules(self) -> None:
        repo_root = Path(__file__).resolve().parent
        poe_dir = repo_root / "poe_bots"
        script = "\n".join(
            [
                "import importlib",
                "import microcap_top100_poe_bot as bot",
                "print(bot.ROOT)",
                "mod = importlib.import_module('microcap_top100_mom16_biweekly_live_v1_4')",
                "print(mod.__name__)",
            ]
        )
        proc = subprocess.run(
            [sys.executable, "-c", script],
            cwd=poe_dir,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(proc.returncode, 0, msg=proc.stderr or proc.stdout)
        output_lines = [line.strip() for line in proc.stdout.splitlines() if line.strip()]
        self.assertGreaterEqual(len(output_lines), 2, msg=proc.stdout)
        self.assertEqual(output_lines[0], str(repo_root))
        self.assertEqual(output_lines[1], "microcap_top100_mom16_biweekly_live_v1_4")

    def test_poepython_exec_without___file___uses_cwd_fallback(self) -> None:
        repo_root = Path(__file__).resolve().parent
        script_path = repo_root / "poe_bots" / "microcap_top100_poe_bot.py"
        script = "\n".join(
            [
                "from pathlib import Path",
                f"source = Path(r'{script_path}').read_text(encoding='utf-8-sig')",
                "ns = {'__name__': 'poepython_runtime'}",
                "exec(compile(source, '<poepython>', 'exec'), ns, ns)",
                "print(ns['ROOT'])",
                "print(ns['BOT_FILE'])",
            ]
        )
        proc = subprocess.run(
            [sys.executable, "-c", script],
            cwd=repo_root,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(proc.returncode, 0, msg=proc.stderr or proc.stdout)
        output_lines = [line.strip() for line in proc.stdout.splitlines() if line.strip()]
        self.assertGreaterEqual(len(output_lines), 2, msg=proc.stdout)
        self.assertEqual(output_lines[0], str(repo_root))
        self.assertEqual(output_lines[1], "None")

    def test_resolve_strategy_from_query_switches_to_v1_4(self) -> None:
        strategy, query_text = bot.resolve_strategy_from_query("1.4\u7684\u4fe1\u53f7")
        self.assertEqual(strategy["version"], "1.4")
        self.assertEqual(query_text, "\u4fe1\u53f7")

    def test_resolve_strategy_from_query_switches_to_v1_5(self) -> None:
        strategy, query_text = bot.resolve_strategy_from_query("1.5\u7684\u4fe1\u53f7")
        self.assertEqual(strategy["version"], "1.5")
        self.assertEqual(query_text, "\u4fe1\u53f7")

    def test_resolve_strategy_from_query_switches_to_v1_6(self) -> None:
        strategy, query_text = bot.resolve_strategy_from_query("1.6\u7684\u4fe1\u53f7")
        self.assertEqual(strategy["version"], "1.6")
        self.assertEqual(query_text, "\u4fe1\u53f7")

    def test_params_command_uses_default_v1_0(self) -> None:
        strategy, query_text = bot.resolve_strategy_from_query("\u53c2\u6570")
        self.assertEqual(strategy["version"], "1.0")
        self.assertEqual(bot.normalize_command(query_text), bot.CMD_PARAMS)
        body = bot.build_params_summary()
        self.assertIn("\u53c2\u6570\u8bf4\u660e", body)
        self.assertIn("v1.0", body)
        self.assertIn("\u6bcf\u4e24\u5468\u5468\u56db", body)
        self.assertIn("biweekly Thursday", body)
        self.assertNotIn("\u8c03\u4ed3\u57fa\u51c6\u65e5\uff1aThursday", body)
        self.assertIn("Overlay\uff1a\u672a\u542f\u7528", body)
        self.assertIn("NAV \u63a7\u5236\uff1a\u672a\u542f\u7528", body)

    def test_params_command_switches_to_v1_4(self) -> None:
        strategy, query_text = bot.resolve_strategy_from_query("1.4\u53c2\u6570")
        self.assertEqual(strategy["version"], "1.4")
        self.assertEqual(bot.normalize_command(query_text), bot.CMD_PARAMS)
        body = bot.build_params_summary()
        self.assertIn("decay=25%", body)
        self.assertIn("recover=35%", body)
        self.assertIn("\u89e6\u53d1\u540e\u4ed3\u4f4d derisk_scale\uff1a0%", body)
        self.assertIn("\u6062\u590d\u540e\u4ed3\u4f4d", body)
        self.assertIn("\u6062\u590d\u5230 100%", body)
        self.assertIn("NAV \u63a7\u5236\uff1a\u672a\u542f\u7528", body)

    def test_params_command_switches_to_v1_5(self) -> None:
        strategy, query_text = bot.resolve_strategy_from_query("1.5\u53c2\u6570")
        self.assertEqual(strategy["version"], "1.5")
        self.assertEqual(bot.normalize_command(query_text), bot.CMD_PARAMS)
        body = bot.build_params_summary()
        self.assertIn("decay=30%", body)
        self.assertIn("recover=40%", body)
        self.assertIn("\u89e6\u53d1\u540e\u4ed3\u4f4d derisk_scale\uff1a0%", body)
        self.assertIn("\u6062\u590d\u5230 100%", body)
        self.assertIn("NAV \u63a7\u5236\u53c2\u6570", body)
        self.assertIn("dd_moderate", body)
        self.assertIn("v1.5 \u7684 100% \u662f overlay \u5c42\u6ee1\u4ed3", body)

    def test_params_command_switches_to_v1_6(self) -> None:
        strategy, query_text = bot.resolve_strategy_from_query("1.6\u53c2\u6570")
        self.assertEqual(strategy["version"], "1.6")
        self.assertEqual(bot.normalize_command(query_text), bot.CMD_PARAMS)
        body = bot.build_params_summary()
        self.assertIn("target_vol", body)
        self.assertIn("15%", body)
        self.assertIn("max_leverage", body)
        self.assertIn("1.5x", body)
        self.assertIn("base_version\uff1av1.4", body)

    def test_non_default_strategy_adds_version_suffix_to_attachment_name(self) -> None:
        bot.set_active_strategy("1.4")
        self.assertEqual(
            bot.versioned_attachment_name("microcap_top100_autorebuild_signal.csv"),
            "microcap_top100_autorebuild_signal_v1_4.csv",
        )

    def test_v1_5_adds_version_suffix_to_attachment_name(self) -> None:
        bot.set_active_strategy("1.5")
        self.assertEqual(
            bot.versioned_attachment_name("microcap_top100_autorebuild_signal.csv"),
            "microcap_top100_autorebuild_signal_v1_5.csv",
        )

    def test_v1_6_adds_version_suffix_to_attachment_name(self) -> None:
        bot.set_active_strategy("1.6")
        self.assertEqual(
            bot.versioned_attachment_name("microcap_top100_autorebuild_signal.csv"),
            "microcap_top100_autorebuild_signal_v1_6.csv",
        )

    def test_handle_signal_uses_official_v1_4_source_and_ignores_proxy_rebuild(self) -> None:
        bot.set_active_strategy("1.4")
        context = self._official_v1_4_context()
        with patch.object(bot, "build_official_v1_4_context", return_value=context) as official_mock:
            with patch.object(bot, "build_context", side_effect=AssertionError("v1.4 must not use Poe proxy context")):
                with patch.object(bot, "run_selected_strategy_backtest", side_effect=AssertionError("proxy close_df must not be used")):
                    with patch.object(
                        bot,
                        "build_thread_context_attachment",
                        return_value=("thread.json.gz", b"{}", "application/gzip"),
                    ):
                        body, csv_bytes, attachments = bot.handle_signal()

        csv_text = csv_bytes.decode("utf-8-sig")
        official_mock.assert_called_once()
        self.assertIn("execution_scale", csv_text)
        self.assertIn("982677.8014313146", csv_text)
        self.assertIn("-0.029470402376160942", csv_text)
        self.assertIn("动量差峰值衰减去风险", body)
        self.assertEqual(attachments[0][0], "thread.json.gz")

    def test_v1_4_signal_requires_official_source_when_poe_proxy_would_be_available(self) -> None:
        bot.set_active_strategy("1.4")
        with patch.object(
            bot,
            "build_official_v1_4_context",
            side_effect=RuntimeError("missing official source"),
        ):
            with patch.object(bot, "build_context", side_effect=AssertionError("must not fall back to approximate proxy")):
                with self.assertRaisesRegex(RuntimeError, "missing official source"):
                    bot.handle_signal()

    def test_v1_4_signal_formats_current_holding_not_next_holding(self) -> None:
        bot.set_active_strategy("1.4")
        context = self._official_v1_4_context()
        context["latest_signal"].loc[0, "current_holding"] = "cash"
        context["latest_signal"].loc[0, "next_holding"] = "long_microcap_short_zz1000"
        context["latest_signal"].loc[0, "trade_state"] = "open"
        context["latest_signal"].loc[0, "momentum_trade_state"] = "open"
        with patch.object(bot, "build_official_v1_4_context", return_value=context):
            with patch.object(
                bot,
                "build_thread_context_attachment",
                return_value=("thread.json.gz", b"{}", "application/gzip"),
            ):
                body, _csv_bytes, _attachments = bot.handle_signal()
        self.assertIn("当前状态：空仓", body)
        self.assertIn("仓位动作（动量信号）：开仓", body)

    def test_non_v1_4_non_default_signal_refuses_approx_fallback_when_official_missing(self) -> None:
        bot.set_active_strategy("1.5")
        with patch.object(bot, "load_official_signal_bundle", return_value=(None, None)):
            with patch.object(bot, "build_context", side_effect=AssertionError("must not build approximate fallback")):
                with self.assertRaisesRegex(RuntimeError, "v1.5 官方信号"):
                    bot.handle_signal()

    def test_handle_signal_uses_official_v1_5_signal_output(self) -> None:
        bot.set_active_strategy("1.5")
        official_signal = pd.DataFrame(
            [
                {
                    "date": pd.Timestamp("2026-04-17"),
                    "signal_label": "cash",
                    "current_holding": "cash",
                    "next_holding": "cash",
                    "trade_state": "hold",
                    "momentum_trade_state": "hold",
                    "microcap_close": 123.0,
                    "hedge_close": 456.0,
                    "microcap_mom": -0.01,
                    "hedge_mom": 0.03,
                    "momentum_gap": -0.04,
                    "execution_scale": 0.0,
                    "signal_quality_derisk_triggered": False,
                    "fixed_hedge_ratio": 0.8,
                    "decay_ratio_threshold": 0.30,
                    "derisk_scale": 0.0,
                    "recovery_ratio_threshold": 0.40,
                    "nav_control_scale_last_applied": 0.8,
                    "nav_control_scale_next_session": 0.8,
                }
            ]
        )
        with patch.object(bot, "build_context", return_value=self._base_context()):
            with patch.object(
                bot,
                "build_thread_context_attachment",
                return_value=("thread.json.gz", b"{}", "application/gzip"),
            ):
                with patch.object(
                    bot,
                    "load_official_signal_bundle",
                    return_value=(official_signal, {"version": "1.5"}),
                ):
                    body, csv_bytes, attachments = bot.handle_signal()
        csv_text = csv_bytes.decode("utf-8-sig")
        self.assertIn("nav_control_scale_last_applied", csv_text)
        self.assertIn("v1.5", body)
        self.assertEqual(attachments[0][0], "thread.json.gz")

    def test_handle_signal_uses_official_v1_6_signal_output(self) -> None:
        bot.set_active_strategy("1.6")
        official_signal = pd.DataFrame(
            [
                {
                    "date": pd.Timestamp("2026-04-17"),
                    "signal_label": "cash",
                    "current_holding": "cash",
                    "next_holding": "cash",
                    "trade_state": "hold",
                    "momentum_trade_state": "hold",
                    "microcap_close": 123.0,
                    "hedge_close": 456.0,
                    "microcap_mom": -0.01,
                    "hedge_mom": 0.03,
                    "momentum_gap": -0.04,
                    "execution_scale": 0.0,
                    "target_vol": 0.15,
                    "target_vol_window": 60,
                    "target_vol_scale_next_session": 1.2,
                    "fixed_hedge_ratio": 1.0,
                    "version": "1.6",
                }
            ]
        )
        with patch.object(bot, "build_context", return_value=self._base_context()):
            with patch.object(
                bot,
                "build_thread_context_attachment",
                return_value=("thread.json.gz", b"{}", "application/gzip"),
            ):
                with patch.object(
                    bot,
                    "load_official_signal_bundle",
                    return_value=(official_signal, {"version": "1.6"}),
                ):
                    body, csv_bytes, attachments = bot.handle_signal()
        csv_text = csv_bytes.decode("utf-8-sig")
        self.assertIn("target_vol_scale_next_session", csv_text)
        self.assertIn("v1.6", body)
        self.assertEqual(attachments[0][0], "thread.json.gz")

    def test_build_performance_outputs_refreshes_official_v1_4_outputs_first(self) -> None:
        bot.set_active_strategy("1.4")
        with patch.object(bot, "ensure_selected_strategy_outputs") as ensure_mock:
            with patch.object(
                bot,
                "load_performance_source",
                return_value=(
                    self._performance_df(),
                    "return_net",
                    "nav_net",
                    "costed_v1_4",
                    "official_v1_4.csv",
                ),
            ):
                body, attachments = bot.build_performance_outputs("1.4 \u8868\u73b0")
        ensure_mock.assert_called_once()
        self.assertIn("v1.4", body)
        self.assertGreaterEqual(len(attachments), 2)

    def test_build_performance_outputs_refreshes_official_v1_5_outputs_first(self) -> None:
        bot.set_active_strategy("1.5")
        with patch.object(bot, "ensure_selected_strategy_outputs") as ensure_mock:
            with patch.object(
                bot,
                "load_performance_source",
                return_value=(
                    self._performance_df(),
                    "return_net",
                    "nav_net",
                    "costed_v1_5",
                    "official_v1_5.csv",
                ),
            ):
                body, attachments = bot.build_performance_outputs("1.5 \u8868\u73b0")
        ensure_mock.assert_called_once()
        self.assertIn("v1.5", body)
        self.assertGreaterEqual(len(attachments), 2)

    def test_build_performance_outputs_refreshes_official_v1_6_outputs_first(self) -> None:
        bot.set_active_strategy("1.6")
        with patch.object(bot, "ensure_selected_strategy_outputs") as ensure_mock:
            with patch.object(
                bot,
                "load_performance_source",
                return_value=(
                    self._performance_df(),
                    "return_net",
                    "nav_net",
                    "costed_v1_6",
                    "official_v1_6.csv",
                ),
            ):
                body, attachments = bot.build_performance_outputs("1.6 \u8868\u73b0")
        ensure_mock.assert_called_once()
        self.assertIn("v1.6", body)
        self.assertGreaterEqual(len(attachments), 2)


if __name__ == "__main__":
    unittest.main()
