import unittest
from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"


class Top100RealtimeWorkflowTest(unittest.TestCase):
    def test_replaces_old_failing_workflow(self):
        self.assertFalse((WORKFLOWS / "run_strategy.yml").exists())
        self.assertTrue((WORKFLOWS / "top100_realtime_signals.yml").exists())

    def test_runs_v1_6_v1_8_realtime_script_on_schedule_and_manual_dispatch(self):
        text = (WORKFLOWS / "top100_realtime_signals.yml").read_text(encoding="utf-8")

        self.assertIn("cron: '45 6 * * *'", text)
        self.assertIn("workflow_dispatch:", text)
        self.assertIn("python run_top100_v1_6_v1_8_realtime_signals.py", text)
        self.assertIn("QVERIS_API_KEY: ${{ secrets.QVERIS_API_KEY }}", text)

    def test_workflow_uses_github_native_run_notifications_and_summary(self):
        text = (WORKFLOWS / "top100_realtime_signals.yml").read_text(encoding="utf-8")

        self.assertNotIn("MAIL_", text)
        self.assertNotIn("smtplib", text)
        self.assertNotIn("SMTP", text)
        self.assertIn("GITHUB_STEP_SUMMARY", text)
        self.assertIn("realtime_signal_result.txt", text)
        self.assertIn("actions/upload-artifact@v4", text)

    def test_workflow_restores_generated_strategy_cache(self):
        text = (WORKFLOWS / "top100_realtime_signals.yml").read_text(encoding="utf-8")

        self.assertIn("actions/cache@v4", text)
        self.assertIn("outputs", text)
        self.assertIn(".microcap_index_cache", text)

    def test_workflow_opts_into_node24_actions_runtime(self):
        text = (WORKFLOWS / "top100_realtime_signals.yml").read_text(encoding="utf-8")

        self.assertIn("FORCE_JAVASCRIPT_ACTIONS_TO_NODE24: true", text)

    def test_clean_checkout_contains_realtime_seed_artifacts(self):
        required = [
            "outputs/wind_microcap_top_100_biweekly_thursday_16y_cached.csv",
            "outputs/microcap_top100_mom16_biweekly_live_v1_1_proxy_meta.json",
            "outputs/microcap_top100_mom16_biweekly_live_v1_1_proxy_members.csv",
            "outputs/microcap_top100_mom16_biweekly_live_v1_1_proxy_turnover.csv",
            "outputs/microcap_top100_mom16_hedge_zz1000_0p8x_biweekly_thursday_16y_costed_nav.csv",
            "outputs/microcap_top100_mom16_biweekly_live_summary.json",
            ".microcap_index_cache/active_universe.csv",
            ".microcap_index_cache/current_st.csv",
            ".microcap_index_cache/realtime/microcap_top100_mom16_biweekly_live_v1_1_static_meta.json",
            ".microcap_index_cache/realtime/microcap_top100_mom16_biweekly_live_v1_1_static_target_members.csv",
            ".microcap_index_cache/realtime/microcap_top100_mom16_biweekly_live_v1_1_static_effective_members.csv",
            ".microcap_index_cache/realtime/microcap_top100_mom16_biweekly_live_v1_1_static_rebalance_changes.csv",
        ]

        result = subprocess.run(
            ["git", "ls-files", "--error-unmatch", *required],
            cwd=ROOT,
            text=True,
            capture_output=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
