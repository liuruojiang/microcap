from __future__ import annotations

import unittest

import poe_bots.microcap_top100_poe_bot as bot


class PoeBotV10OnlyTests(unittest.TestCase):
    def test_help_text_mentions_only_v1_0_mode(self) -> None:
        text = bot.build_help_text()
        self.assertIn("固定按 v1.0", text)
        self.assertNotIn("1.1", text)
        self.assertNotIn("1.2", text)

    def test_intro_text_mentions_only_v1_0_mode(self) -> None:
        text = bot.build_intro_text()
        self.assertIn("固定按 v1.0", text)
        self.assertNotIn("1.1", text)
        self.assertNotIn("1.2", text)

    def test_attachment_name_no_longer_appends_version_suffix(self) -> None:
        self.assertEqual(bot.versioned_attachment_name("signal.csv"), "signal.csv")


if __name__ == "__main__":
    unittest.main()
