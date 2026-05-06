import unittest
from pathlib import Path


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


if __name__ == "__main__":
    unittest.main()
