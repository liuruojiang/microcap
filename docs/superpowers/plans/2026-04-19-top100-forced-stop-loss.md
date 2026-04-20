# Top100 Forced Stop Loss Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a costed-net single-trade forced stop-loss execution layer to the Top100 `v1.0` mainline, with stale-signal lockout until the base signal resets through a cash window, and support a fixed `2%/3%/4%/5%` threshold scan.

**Architecture:** Keep the base gross signal engine unchanged. Add a post-processing execution state machine inside `microcap_top100_mom16_biweekly_live.py` that replays the base signal plus turnover table, computes executed holdings, applies the existing stock cost model on executed states, and records when stop-loss exits invalidate the current signal until a new signal cycle appears.

**Tech Stack:** Python, `unittest`, `pandas`, existing Top100 mainline/cost-model helpers

---

### Task 1: Lock the behavior with failing tests

**Files:**
- Create: `test_top100_forced_stop_loss.py`
- Modify: `microcap_top100_mom16_biweekly_live.py`
- Test: `test_top100_forced_stop_loss.py`

- [ ] **Step 1: Write the failing tests**
- [ ] **Step 2: Run `python -m unittest test_top100_forced_stop_loss.py -v` and verify the failures are due to missing stop-loss helpers**
- [ ] **Step 3: Add minimal helper/function stubs only**
- [ ] **Step 4: Re-run the tests and confirm failures narrow to unimplemented behavior**

### Task 2: Implement the execution state machine

**Files:**
- Modify: `microcap_top100_mom16_biweekly_live.py`
- Test: `test_top100_forced_stop_loss.py`

- [ ] **Step 1: Implement executed-holding replay on top of the base gross result**
- [ ] **Step 2: Track trade-level `costed net` cumulative return and force exit on threshold breach**
- [ ] **Step 3: Add stale-signal lockout until base signal first returns to `cash`**
- [ ] **Step 4: Re-run `python -m unittest test_top100_forced_stop_loss.py -v` and confirm green**

### Task 3: Add threshold scan and wire a real rebuild path

**Files:**
- Modify: `microcap_top100_mom16_biweekly_live.py`
- Test: `test_top100_forced_stop_loss.py`

- [ ] **Step 1: Add a helper that scans `0.02/0.03/0.04/0.05` and summarizes each threshold**
- [ ] **Step 2: Expose a minimal real-data rebuild/scan path using existing `proxy_turnover` and `costed_nav` infrastructure**
- [ ] **Step 3: Re-run the targeted test file**

### Task 4: Verify on real local data

**Files:**
- Modify: `microcap_top100_mom16_biweekly_live.py` if verification exposes defects

- [ ] **Step 1: Back up risky files before edits**
- [ ] **Step 2: Run a minimal real-data verification command against the local workspace**
- [ ] **Step 3: Inspect the generated summary to confirm all four thresholds are present**
- [ ] **Step 4: Report the exact command, data path, and observed output**
