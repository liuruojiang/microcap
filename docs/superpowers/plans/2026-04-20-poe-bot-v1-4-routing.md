# POE Bot v1.4 Query Routing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let the existing POE bot keep `v1.0` as default but switch to `v1.4` when the user explicitly writes commands like `1.4的信号`.

**Architecture:** Add a small strategy registry to `poe_bots/microcap_top100_poe_bot.py`, route query parsing through it, and use official `v1.4` output files for the daily signal and performance paths. Keep members, changes, and realtime commands on the current fast context path, but relabel them under the selected strategy and use the selected hedge ratio.

**Tech Stack:** Python, pandas, unittest, existing POE single-file bot runtime

---

### Task 1: Add failing tests for version parsing and file naming

**Files:**
- Create: `C:/Users/Administrator.DESKTOP-95I7VVU/Desktop/动量策略/微盘股对冲策略/test_poe_bot_version_selection.py`
- Modify: `C:/Users/Administrator.DESKTOP-95I7VVU/Desktop/动量策略/微盘股对冲策略/poe_bots/microcap_top100_poe_bot.py`

- [ ] Write tests that prove `1.4的信号` resolves to `v1.4`, stripped command text becomes `信号`, and non-default attachments carry a `v1_4` suffix.
- [ ] Run `python -m unittest test_poe_bot_version_selection.py -v` and confirm the new tests fail before implementation.
- [ ] Implement only the minimal strategy registry, version parser, and attachment naming changes needed to make those tests pass.
- [ ] Re-run `python -m unittest test_poe_bot_version_selection.py -v` and confirm green.

### Task 2: Add failing tests for official v1.4 signal/performance routing

**Files:**
- Modify: `C:/Users/Administrator.DESKTOP-95I7VVU/Desktop/动量策略/微盘股对冲策略/test_poe_bot_version_selection.py`
- Modify: `C:/Users/Administrator.DESKTOP-95I7VVU/Desktop/动量策略/微盘股对冲策略/poe_bots/microcap_top100_poe_bot.py`

- [ ] Add tests that patch the `v1.4` output loader and prove `handle_signal()` and `build_performance_outputs()` consume official `v1.4` outputs when the active strategy is `1.4`.
- [ ] Run the focused test target again and verify it fails for the expected missing-routing reason.
- [ ] Implement the minimal official `v1.4` output loading helpers and wire them into the daily signal and performance paths.
- [ ] Re-run the focused target and confirm green.

### Task 3: Keep fast query paths working under selected strategy

**Files:**
- Modify: `C:/Users/Administrator.DESKTOP-95I7VVU/Desktop/动量策略/微盘股对冲策略/poe_bots/microcap_top100_poe_bot.py`
- Modify: `C:/Users/Administrator.DESKTOP-95I7VVU/Desktop/动量策略/微盘股对冲策略/test_poe_bot_runtime_cache_and_stage_reuse.py`

- [ ] Add one focused test that proves `build_context()` metadata reflects the selected strategy version and hedge ratio.
- [ ] Run `python -m unittest test_poe_bot_runtime_cache_and_stage_reuse.py -v` and confirm the new assertion fails before the patch.
- [ ] Patch the context metadata and strategy header rendering so members, changes, and realtime paths report the chosen strategy consistently.
- [ ] Re-run the focused runtime-cache test target and confirm green.

### Task 4: Verify the narrow regression suite

**Files:**
- Test: `C:/Users/Administrator.DESKTOP-95I7VVU/Desktop/动量策略/微盘股对冲策略/test_poe_bot_version_selection.py`
- Test: `C:/Users/Administrator.DESKTOP-95I7VVU/Desktop/动量策略/微盘股对冲策略/test_poe_bot_runtime_cache_and_stage_reuse.py`
- Test: `C:/Users/Administrator.DESKTOP-95I7VVU/Desktop/动量策略/微盘股对冲策略/test_poe_bot_signal_summary_labels.py`

- [ ] Run `python -m unittest test_poe_bot_version_selection.py test_poe_bot_runtime_cache_and_stage_reuse.py test_poe_bot_signal_summary_labels.py -v`.
- [ ] Review failures, make the smallest corrective patch if needed, and rerun until green.
- [ ] Inspect `git diff --stat` to confirm the diff stays limited to the POE bot, tests, and this plan doc.
