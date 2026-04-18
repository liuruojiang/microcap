# POE Bot Pure v1.0 Sync Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert the POE bot into a standalone pure `v1.0` cloud bot and sync the current mainline `v1.0` refresh/performance rules into its single-file implementation.

**Architecture:** Keep `poe_bots/microcap_top100_poe_bot.py` as a deployable single file, but remove all in-file multi-version switching. Add a mainline-compatible `v1.0` refresh/cost layer inside the file and verify behavior with focused unit tests.

**Tech Stack:** Python, pandas, unittest, existing POE single-file bot runtime

---

### Task 1: Lock The Pure v1.0 Contract

**Files:**
- Modify: `C:/Users/Administrator.DESKTOP-95I7VVU/Desktop/动量策略/微盘股对冲策略/poe_bots/microcap_top100_poe_bot.py`
- Test: `C:/Users/Administrator.DESKTOP-95I7VVU/Desktop/动量策略/微盘股对冲策略/test_poe_bot_v1_0_only.py`

- [ ] **Step 1: Write the failing test**

```python
import unittest

import poe_bots.microcap_top100_poe_bot as bot


class PoeBotV10OnlyTests(unittest.TestCase):
    def test_help_text_no_longer_mentions_other_versions(self):
        self.assertNotIn("1.1", bot.build_help_text())
        self.assertNotIn("1.2", bot.build_help_text())
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest test_poe_bot_v1_0_only.py -v`
Expected: FAIL because `build_help_text` does not exist yet or still exposes `1.1/1.2`.

- [ ] **Step 3: Write minimal implementation**

```python
def build_help_text():
    return (
        "支持命令：\n"
        "1. 信号\n"
        "2. 实时信号\n"
        "3. 成分股\n"
        "4. 进出名单\n"
        "5. 实时进出名单\n"
        "6. 表现 近5年 / 表现 2024至今 / 最近三年表现\n\n"
        "说明：该机器人固定按 v1.0 主版本运行。"
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest test_poe_bot_v1_0_only.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add test_poe_bot_v1_0_only.py poe_bots/microcap_top100_poe_bot.py
git commit -m "Restrict POE bot to v1.0"
```

### Task 2: Sync Mainline-Compatible Costed Refresh Logic

**Files:**
- Modify: `C:/Users/Administrator.DESKTOP-95I7VVU/Desktop/动量策略/微盘股对冲策略/poe_bots/microcap_top100_poe_bot.py`
- Test: `C:/Users/Administrator.DESKTOP-95I7VVU/Desktop/动量策略/微盘股对冲策略/test_poe_bot_costed_refresh.py`

- [ ] **Step 1: Write the failing test**

```python
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import poe_bots.microcap_top100_poe_bot as bot


class PoeBotCostedRefreshTests(unittest.TestCase):
    def test_rebuild_costed_nav_from_proxy_turnover_writes_latest_date(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            turnover_path = tmp_path / "proxy_turnover.csv"
            costed_path = tmp_path / "costed.csv"
            index_path = tmp_path / "index.csv"
            pd.DataFrame({"rebalance_date": ["2026-04-10"], "turnover_frac_one_side": [0.2]}).to_csv(turnover_path, index=False)
            pd.DataFrame({"date": ["2026-04-09", "2026-04-10"], "close": [1.0, 1.01]}).to_csv(index_path, index=False)
            gross = pd.DataFrame({"return": [0.0, 0.02]}, index=pd.to_datetime(["2026-04-09", "2026-04-10"]))
            net = pd.DataFrame({"return_net": [0.0, 0.018], "nav_net": [1.0, 1.018]}, index=pd.to_datetime(["2026-04-09", "2026-04-10"]))
            with patch.object(bot, "load_close_df", return_value=pd.DataFrame(index=gross.index)):
                with patch.object(bot, "run_signal", return_value=gross):
                    with patch.object(bot.freq_mod.cost_mod, "apply_cost_model", return_value=net):
                        bot.rebuild_costed_nav_from_proxy_turnover(index_path, turnover_path, costed_path)
            saved = pd.read_csv(costed_path)
            self.assertEqual(saved["date"].iloc[-1], "2026-04-10")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest test_poe_bot_costed_refresh.py -v`
Expected: FAIL because the helper does not exist yet.

- [ ] **Step 3: Write minimal implementation**

```python
def rebuild_costed_nav_from_proxy_turnover(index_csv, turnover_path, costed_nav_csv):
    turnover_df = pd.read_csv(turnover_path)
    turnover_df["rebalance_date"] = pd.to_datetime(turnover_df["rebalance_date"], errors="coerce")
    close_df = load_close_df(index_csv)
    gross = run_signal(close_df)
    net = freq_mod.cost_mod.apply_cost_model(gross, turnover_df.dropna(subset=["rebalance_date"]))
    net.to_csv(costed_nav_csv, index_label="date", encoding="utf-8")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest test_poe_bot_costed_refresh.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add test_poe_bot_costed_refresh.py poe_bots/microcap_top100_poe_bot.py
git commit -m "Sync POE bot costed refresh flow"
```

### Task 3: Wire Pure v1.0 Messaging And Performance Output

**Files:**
- Modify: `C:/Users/Administrator.DESKTOP-95I7VVU/Desktop/动量策略/微盘股对冲策略/poe_bots/microcap_top100_poe_bot.py`
- Test: `C:/Users/Administrator.DESKTOP-95I7VVU/Desktop/动量策略/微盘股对冲策略/test_poe_bot_v1_0_only.py`

- [ ] **Step 1: Write the failing test**

```python
def test_intro_text_mentions_fixed_v1_0_mode(self):
    intro = bot.build_intro_text()
    self.assertIn("固定按 v1.0", intro)
    self.assertNotIn("1.1", intro)
    self.assertNotIn("1.2", intro)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest test_poe_bot_v1_0_only.py -v`
Expected: FAIL because intro builder does not exist or still mentions other versions.

- [ ] **Step 3: Write minimal implementation**

```python
def build_intro_text():
    return (
        "Top100 微盘股对冲策略机器人\n\n"
        "- 发送 信号：在线重建最近窗口后给出收盘确认信号\n"
        "- 发送 实时信号：在线重建名单并抓实时行情，给出盘中信号\n"
        "- 发送 成分股 / 进出名单 / 实时进出名单 / 表现\n"
        "说明：该机器人固定按 v1.0 主版本运行。"
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest test_poe_bot_v1_0_only.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add test_poe_bot_v1_0_only.py poe_bots/microcap_top100_poe_bot.py
git commit -m "Simplify POE bot messaging to v1.0"
```

### Task 4: Verify The Focused Regression Suite

**Files:**
- Test: `C:/Users/Administrator.DESKTOP-95I7VVU/Desktop/动量策略/微盘股对冲策略/test_poe_bot_v1_0_only.py`
- Test: `C:/Users/Administrator.DESKTOP-95I7VVU/Desktop/动量策略/微盘股对冲策略/test_poe_bot_costed_refresh.py`

- [ ] **Step 1: Run the focused suite**

Run: `python -m unittest test_poe_bot_v1_0_only.py test_poe_bot_costed_refresh.py -v`
Expected: PASS with 0 failures.

- [ ] **Step 2: Run the existing safety tests**

Run: `python -m unittest test_mainline_refresh_consistency.py test_validate_top100_versions_consistency.py test_compare_top100_versions_recent2y.py test_top100_v1_1_mainline_tools.py test_v1_2_costed_base.py -v`
Expected: PASS with 0 failures.

- [ ] **Step 3: Review diff before closing**

Run: `git diff --stat`
Expected: only POE bot, tests, and spec/plan docs relevant to the pure `v1.0` sync.

- [ ] **Step 4: Commit**

```bash
git add docs/superpowers/specs/2026-04-12-poe-bot-v1-0-sync-design.md docs/superpowers/plans/2026-04-12-poe-bot-v1-0-sync.md test_poe_bot_v1_0_only.py test_poe_bot_costed_refresh.py poe_bots/microcap_top100_poe_bot.py
git commit -m "Sync POE bot with standalone v1.0 flow"
```
