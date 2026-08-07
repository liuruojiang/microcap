# Microcap Production Reconciliation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make production v2.0/v2.3/v2.5 strategy signals, member actions, email rendering, remote `main`, and the corrected delivery agree on one verified state.

**Architecture:** Integrate the reviewed v2 strategy lineage into a clean branch based on current production `main`, then add explicit production-identity and member-execution fields at the signal boundary. The automation repository validates that contract and renders holdings and dated actions; deployment is accepted only after remote-SHA and artifact readback.

**Tech Stack:** Python 3.11, pandas, pytest, GitHub Actions YAML, Git/GitHub CLI, Gmail SMTP workflow.

---

### Task 1: Integrate the formal v2.0/v2.3 lineage without regressing v2.5

**Files:**
- Modify: `microcap_top100_mom16_biweekly_live_v2_0.py`
- Modify: `microcap_top100_mom16_biweekly_live_v2_3.py`
- Modify: `microcap_top100_mom16_biweekly_live_v2_5.py`
- Create/modify: `tests/test_microcap_v2_review_remediations.py`
- Create/modify: `tests/test_top100_data_guards.py`
- Create/modify: `tests/test_v2_5_scan_integrity.py`

- [ ] **Step 1: Add a failing production-identity test**

Require v2.0 to expose 60/23% overheat plus TV15/window75/max1.5 and v2.3 to expose LB25/HL2.5/R2 0.08 plus vol10/26%/19.5% with target-vol disabled. Require v2.5 to remain native and unhedged.

```python
def test_formal_v2_strategy_identities_are_production_defaults():
    assert v20.OVERHEAT_WINDOW == 60
    assert v20.OVERHEAT_THRESHOLD == pytest.approx(0.23)
    assert v20.TARGET_VOL == pytest.approx(0.15)
    assert v20.TARGET_VOL_WINDOW == 75
    assert v23.LOOKBACK == 25
    assert v23.HALFLIFE == pytest.approx(2.5)
    assert v23.R2_ENTRY_GATE == pytest.approx(0.08)
    assert v23.OVERHEAT_FEATURE_WINDOW == 10
    assert v23.OVERHEAT_TRIGGER_THRESHOLD == pytest.approx(0.26)
    assert v23.OVERHEAT_RECOVERY_THRESHOLD == pytest.approx(0.195)
    assert v25.EXECUTION_HEDGE_RATIO == pytest.approx(0.0)
```

- [ ] **Step 2: Run the test and verify RED**

Run: `python -m pytest tests/test_microcap_v2_review_remediations.py -q`

Expected: FAIL on production `main` because the formal v2.0/v2.3 identity is absent.

- [ ] **Step 3: Integrate the reviewed lineage**

Merge the clean reviewed tip `6c89997491cfeb8c4105347e167d93dd30e8fd9c` into the current branch based on `origin/main`. Resolve conflicts by preserving PR #23's native v2.5 realtime holdings while retaining the reviewed v2.0/v2.3 overheat state machines, data guards, and terminal close-turnover correction.

- [ ] **Step 4: Run the strategy regression tests**

Run:

```text
python -m pytest tests/test_microcap_v2_review_remediations.py tests/test_top100_data_guards.py tests/test_v2_5_scan_integrity.py tests/test_v2_5_realtime_signal_label_compat.py -q
```

Expected: all selected tests pass.

- [ ] **Step 5: Commit the integration**

Stage only the merge resolution and new regression tests. Commit with `fix: reconcile formal microcap v2 production lineage`.

### Task 2: Make realtime state and member actions self-describing

**Files:**
- Modify: `microcap_top100_mom16_biweekly_live_v2_0.py`
- Modify: `microcap_top100_mom16_biweekly_live_v2_3.py`
- Modify: `microcap_top100_mom16_biweekly_live_v2_5.py`
- Modify: `tests/test_microcap_v2_review_remediations.py`

- [ ] **Step 1: Add failing tests for standalone refresh and dated member action**

The first test calls the standalone realtime refresh route far enough to prove `DEFAULT_MAX_STALE_ANCHOR_DAYS` is defined. The second builds a realtime row with latest rebalance equal to the close-confirmed anchor and quote date on the next session, requiring:

```python
assert row["member_rebalance_actionable"] is True
assert row["member_rebalance_signal_date"] == "2026-08-06"
assert row["member_rebalance_execution_date"] == "2026-08-07"
```

A later-anchor fixture must retain historical counts but require `member_rebalance_actionable=False`. A preview fixture with `official_rebalance=False` must never become actionable.

- [ ] **Step 2: Run the focused tests and verify RED**

Run: `python -m pytest tests/test_microcap_v2_review_remediations.py -k "standalone_refresh or member_rebalance_actionable" -q`

Expected: FAIL because the refresh constant and dated action contract are missing.

- [ ] **Step 3: Implement the minimal signal contract**

Export the missing refresh constant from the embedded base module. Add one helper that computes official member action metadata from `latest_rebalance`, `latest_anchor_trade_date`, `quote_trade_date`, change counts, and `official_rebalance`. Apply the fields to v2.0 and preserve them through v2.3/v2.5 native signal construction.

- [ ] **Step 4: Run focused and full strategy tests**

Run:

```text
python -m pytest tests/test_microcap_v2_review_remediations.py -q
python -m pytest -q
```

Expected: both commands pass.

- [ ] **Step 5: Commit**

Commit with `fix: publish dated microcap member actions`.

### Task 3: Fail closed on strategy mismatch and restore holdings-first email

**Repository:** `codex-daily-automation-probe`

**Files:**
- Modify: `scripts/build_microcap_realtime_digest.py`
- Modify: `.github/workflows/microcap-realtime-digest.yml`
- Modify: `tests/test_microcap_digest_email_body.py`
- Modify: `tests/test_microcap_workflow_refresh_gate.py`

- [ ] **Step 1: Add failing digest tests**

Add four fixtures:

1. Old production v2.0 output without overheat fields becomes `[异常]`.
2. Old production v2.3 output without native vol10 fields becomes `[异常]`.
3. Valid v2.0/v2.3 cash states render a bold holdings sentence before any member-list text.
4. A 7-in/7-out row is actionable only when `member_rebalance_actionable=True`; historical or preview rows render no new member action.

The valid body must contain:

```text
持仓：v2.0 空仓（0.00）；v2.3 空仓（0.00）；v2.5 继续持有微盘 Top100（...）
名单：2026-08-06 收盘确认，2026-08-07 执行，调入 7、调出 7
```

- [ ] **Step 2: Run digest tests and verify RED**

Run: `python -m pytest tests/test_microcap_digest_email_body.py -q`

Expected: FAIL on identity validation, holdings-first conclusion, or action-date gating.

- [ ] **Step 3: Implement validation and rendering**

Add `validate_strategy_identity(version, fields)` with exact native-field checks. Make identity failures override `[需操作]`. Add `holdings_conclusion(results)` before `action_conclusion(results)`. Change member action detection to require the explicit actionable flag and include both dates.

- [ ] **Step 4: Wire the production SHA into the email**

Add `--strategy-sha` to the builder. In the workflow, pass `$(git -C microcap rev-parse HEAD)` and render it beside the data timestamp and run URL.

- [ ] **Step 5: Run automation tests**

Run:

```text
python -m pytest tests/test_microcap_digest_email_body.py tests/test_microcap_workflow_refresh_gate.py -q
python -m pytest -q
```

Expected: microcap tests pass. The full suite may retain only the documented date-sensitive ETF dedupe baseline failure; no Microcap failure is allowed.

- [ ] **Step 6: Commit**

Commit with `fix: reconcile microcap digest with production signals`.

### Task 4: Verify real data, review, merge, and deliver

**Files:**
- Create: `docs/microcap_production_incident_20260807.md`
- Modify only if needed: `AGENTS.md`
- Modify only if needed: automation `README.md`

- [ ] **Step 1: Produce mandatory freshness proof**

Refresh the clean strategy worktree and read back latest dates and row counts for panel, proxy index, proxy turnover, base costed NAV, v2.0, v2.3, and v2.5 streams. All formal streams must share the latest close-confirmed date or the deployment stops.

- [ ] **Step 2: Run fresh realtime signals**

Run the same pre-refresh/state-only sequence as production, then all three realtime entrypoints. Verify anchor, quote date, coverage, holdings, next holdings, scales, overheat fields, member action dates, and production identity.

- [ ] **Step 3: Request two-stage review**

Review strategy changes for specification compliance and code quality, then repeat for automation changes. Fix every Critical or Important finding and rerun affected tests.

- [ ] **Step 4: Write the incident record and durable guardrail**

Document the five causes, exact wrong/correct parameters, verification commands, remote SHAs, and recovery evidence in `docs/microcap_production_incident_20260807.md`. Keep `AGENTS.md` limited to the durable remote-main identity/readback rule.

- [ ] **Step 5: Push, create PRs, and merge**

Push both feature branches, open ready PRs, wait for checks, merge into `main`, and read back both remote `main` SHAs through the GitHub API. Do not force-push.

- [ ] **Step 6: Run production correction and inspect before accepting**

Dispatch the Microcap workflow with `correction=true`. Download the artifact and verify the three realtime CSV rows plus rendered body. Confirm v2.0/v2.3 cash and zero scale unless fresh evidence changed them, ensure the production SHA matches remote `main`, and verify the member action is dated rather than repeated.

- [ ] **Step 7: Verify Gmail delivery and remote synchronization**

Read the corrected email from Gmail, compare its subject/body with the artifact, then report both merged PRs, both remote SHAs, workflow URL, verified holdings, and any unrelated baseline failure.
