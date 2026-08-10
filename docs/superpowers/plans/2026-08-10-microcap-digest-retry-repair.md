# Microcap Digest Retry Repair Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Repair the fresh-runner member snapshot failure and preserve automatic retry after an abnormal Microcap digest.

**Architecture:** The strategy constructs complete dated member snapshots from raw cache data when possible and falls back to the already-produced proxy-members artifact when raw caches are absent. The state bundle refuses to label unusable state as valid. The automation workflow records the normal daily delivery marker only after all three formal signal commands and Gmail delivery succeed.

**Tech Stack:** Python 3.11, pandas, pytest, unittest, GitHub Actions YAML, GitHub CLI.

---

### Task 1: Strategy member snapshot fallback

**Files:**
- Create: `tests/test_realtime_member_snapshot_fallback.py`
- Modify: `microcap_top100_mom16_biweekly_live_v2_0.py`

- [ ] Write a failing test that makes `load_member_snapshot()` raise the production `available=0 required=100` error while `proxy_members` contains 100 rows for every required rebalance date.
- [ ] Run `python -m pytest tests/test_realtime_member_snapshot_fallback.py -q` and confirm the error escapes before the proxy fallback.
- [ ] Back up the strategy source with `backup_paths.py`.
- [ ] Add one helper that catches the raw snapshot failure, fills all missing dates from `proxy_members`, validates exactly `TOP_N` unique symbols per date, and re-raises the raw error when the fallback is incomplete.
- [ ] Replace the three duplicated raw-then-fallback call sites with the helper.
- [ ] Re-run the focused test and the complete strategy suite.

### Task 2: State validation fail-closed contract

**Files:**
- Modify: `tests/test_realtime_member_snapshot_fallback.py`
- Modify: `scripts/realtime_state_bundle.py`

- [ ] Write a failing test for a fresh runner with zero current-member price caches and only obsolete static context; assert `validate_state()` returns `ok=false`.
- [ ] Run the focused test and confirm current validation incorrectly returns `ok=true`.
- [ ] Back up `scripts/realtime_state_bundle.py`.
- [ ] Validate that either current price caches exist or a complete static target/effective context matches the latest proxy rebalance date; otherwise append an error.
- [ ] Re-run focused and full strategy tests.

### Task 3: Automation retry marker

**Files:**
- Modify: `tests/test_microcap_workflow_refresh_gate.py`
- Modify: `.github/workflows/microcap-realtime-digest.yml`

- [ ] Write a failing workflow-contract test requiring the marker preparation and upload conditions to include zero exits for v2.0, v2.3, and v2.5.
- [ ] Run `python -m pytest tests/test_microcap_workflow_refresh_gate.py -q` and confirm it fails against the current email-only marker condition.
- [ ] Change both marker steps to require Gmail success plus all three zero signal exits.
- [ ] Run all Microcap automation tests, then the full automation suite and record the pre-existing ETF date-fixture failure separately.

### Task 4: Review, merge, and production correction

**Files:**
- Modify after strategy merge: automation `.github/workflows/microcap-realtime-digest.yml` strategy SHA pin.

- [ ] Review the strategy diff and tests, then commit, push, open a PR, wait for checks, and merge without force-push.
- [ ] Read remote strategy `main`, update the automation pin to that exact SHA, run tests, review, commit, push, open a PR, wait for checks, and merge.
- [ ] Dispatch `Microcap Realtime Digest` with `correction=true`.
- [ ] Download the production artifact and verify readable final CSVs for v2.0/v2.3/v2.5, exact identity fields, latest completed-session anchor, current quote date, dated member-action fields, and non-stale holdings.
- [ ] Record remote strategy SHA, remote automation SHA, workflow URL, and delivered corrected message.

