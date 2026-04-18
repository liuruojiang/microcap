# POE Bot Performance NL Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the standalone POE bot recognize natural-language performance requests and default bare `表现` queries to the last one year.

**Architecture:** Keep all changes inside `poe_bots/microcap_top100_poe_bot.py`. Add small helpers for performance intent detection and period parsing, then reuse the existing performance output pipeline.

**Tech Stack:** Python, `unittest`, pandas, current POE single-file bot

---

### Task 1: Lock the natural-language query contract

**Files:**
- Create: `C:/Users/Administrator.DESKTOP-95I7VVU/Desktop/动量策略/微盘股对冲策略/test_poe_bot_performance_nl.py`
- Modify: `C:/Users/Administrator.DESKTOP-95I7VVU/Desktop/动量策略/微盘股对冲策略/poe_bots/microcap_top100_poe_bot.py`

- [ ] **Step 1: Write failing tests**
- [ ] **Step 2: Run `python -m unittest test_poe_bot_performance_nl.py -v` and verify failure**
- [ ] **Step 3: Implement minimal helpers for performance intent + date parsing**
- [ ] **Step 4: Run `python -m unittest test_poe_bot_performance_nl.py -v` and verify pass**

### Task 2: Wire default last-year behavior into output flow

**Files:**
- Modify: `C:/Users/Administrator.DESKTOP-95I7VVU/Desktop/动量策略/微盘股对冲策略/poe_bots/microcap_top100_poe_bot.py`
- Test: `C:/Users/Administrator.DESKTOP-95I7VVU/Desktop/动量策略/微盘股对冲策略/test_poe_bot_performance_nl.py`

- [ ] **Step 1: Add a failing test for bare `表现` using last-one-year default**
- [ ] **Step 2: Run the focused test and verify failure**
- [ ] **Step 3: Implement default-range fallback inside performance period resolution**
- [ ] **Step 4: Re-run the focused test and verify pass**

### Task 3: Run regression coverage

**Files:**
- Test: `C:/Users/Administrator.DESKTOP-95I7VVU/Desktop/动量策略/微盘股对冲策略/test_poe_bot_performance_nl.py`
- Test: `C:/Users/Administrator.DESKTOP-95I7VVU/Desktop/动量策略/微盘股对冲策略/test_poe_bot_v1_0_only.py`
- Test: `C:/Users/Administrator.DESKTOP-95I7VVU/Desktop/动量策略/微盘股对冲策略/test_poe_bot_costed_refresh.py`

- [ ] **Step 1: Run `python -m unittest test_poe_bot_performance_nl.py test_poe_bot_v1_0_only.py test_poe_bot_costed_refresh.py -v`**
- [ ] **Step 2: Run `python -m unittest test_mainline_refresh_consistency.py test_validate_top100_versions_consistency.py test_compare_top100_versions_recent2y.py test_top100_v1_1_mainline_tools.py test_v1_2_costed_base.py -v`**
- [ ] **Step 3: Review `git diff --stat` and report only the POE-related delta**
