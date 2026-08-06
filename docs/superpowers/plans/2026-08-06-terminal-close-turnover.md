# Terminal Close Rebalance Turnover Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Record a close-execution rebalance on the final available trading date so the Top100 turnover table, transaction costs, and freshness proof all reach the latest close.

**Architecture:** Extend the existing proxy simulator only at the terminal-date boundary. Reuse its trade-constraint function and turnover schema, leave the future return date missing, and retain the current following-day path whenever a following trading day exists. Then rerun the official v2.0 refresh and generate official costed v2.0, v2.3, and v2.5 streams before calculating a common trailing-year comparison.

**Tech Stack:** Python 3.12, pandas, pytest, AKShare, matplotlib, repository-native Top100 generators.

---

### Task 1: Reproduce terminal turnover omission

**Files:**
- Modify: `tests/test_microcap_v2_review_remediations.py`
- Test: `tests/test_microcap_v2_review_remediations.py`

- [ ] **Step 1: Write the failing terminal-date test**

Add a test that calls `v2_0.freq_mod.simulate_rebalance_path()` with rebalances on the first and final dates. Assert that the final-date turnover row exists, its execution date is the final date, its future return date is missing, its two-sided cost is `0.006`, and the effective holdings change to the new symbol.

```python
def test_terminal_close_rebalance_records_turnover_without_future_return_date() -> None:
    dates = pd.to_datetime(["2026-08-04", "2026-08-05", "2026-08-06"])
    _, turnover, effective = v2_0.freq_mod.simulate_rebalance_path(
        trading_dates=dates,
        returns_df=pd.DataFrame({"A": [0.0, 0.0, 0.01], "B": [0.0, 0.0, 0.0]}, index=dates),
        target_members_map={dates[0]: ["A"], dates[-1]: ["B"]},
        rebalance_dates=pd.DatetimeIndex([dates[0], dates[-1]]),
        buyable_df=pd.DataFrame(True, index=dates, columns=["A", "B"]),
        sellable_df=pd.DataFrame(True, index=dates, columns=["A", "B"]),
        one_side_cost_rate=0.003,
        top_n=1,
        execution_timing=v2_0.freq_mod.EXECUTION_TIMING_CLOSE,
    )
    assert turnover["rebalance_date"].tolist() == [dates[0], dates[-1]]
    terminal = turnover.iloc[-1]
    assert terminal["execution_date"] == dates[-1]
    assert pd.isna(terminal["return_start_date"])
    assert terminal["two_side_cost_rate"] == pytest.approx(0.006)
    assert effective[dates[-1]] == ["B"]
```

- [ ] **Step 2: Run the new test and verify the intended failure**

Run:

```powershell
.\.venv-refresh\Scripts\python.exe -m pytest tests\test_microcap_v2_review_remediations.py::test_terminal_close_rebalance_records_turnover_without_future_return_date -q
```

Expected: FAIL because the turnover dates contain only `2026-08-04`.

- [ ] **Step 3: Write the no-duplicate following-day test**

Add a second test with `2026-08-07` present. Assert that the `2026-08-06` rebalance appears exactly once and its `return_start_date` is `2026-08-07`.

- [ ] **Step 4: Run the second test against the pre-fix implementation**

Run both named tests. Expected: the terminal-date test fails and the following-day test passes.

### Task 2: Implement terminal close turnover registration

**Files:**
- Modify: `microcap_top100_mom16_biweekly_live_v2_0.py:3207-3285`
- Test: `tests/test_microcap_v2_review_remediations.py`

- [ ] **Step 1: Add the minimal terminal branch**

After the daily loop, if the execution timing is `close`, the final date is a rebalance date, and it has not already been processed, call `apply_trade_constraints()` using the final close and append the standard turnover record. Set `return_start_date` to `pd.NaT`; do not create a future index row.

- [ ] **Step 2: Verify both focused tests pass**

Run the two named tests. Expected: `2 passed`.

- [ ] **Step 3: Run the full remediation test module**

Run:

```powershell
.\.venv-refresh\Scripts\python.exe -m pytest tests\test_microcap_v2_review_remediations.py -q
```

Expected: all tests pass with no new failures.

- [ ] **Step 4: Commit the tested fix**

Stage only the strategy source and remediation test, then commit with message `fix: record terminal close rebalance turnover`.

### Task 3: Refresh and generate official costed streams

**Files:**
- Update through official generators: `outputs/`

- [ ] **Step 1: Rerun the official v2.0 forced refresh**

Run:

```powershell
.\.venv-refresh\Scripts\python.exe microcap_top100_mom16_biweekly_live_v2_0.py --force-refresh --max-workers 8
```

Expected: the existing freshness guard passes and the v2.0 costed NAV is written through `2026-08-06`.

- [ ] **Step 2: Generate official v2.3 and v2.5 outputs**

Run each official entrypoint without weakening freshness checks:

```powershell
.\.venv-refresh\Scripts\python.exe microcap_top100_mom16_biweekly_live_v2_3.py --max-workers 8
.\.venv-refresh\Scripts\python.exe microcap_top100_mom16_biweekly_live_v2_5.py --max-workers 8
```

Expected: both costed NAV streams are written through the same latest date.

### Task 4: Prove freshness and create the trailing-year comparison

**Files:**
- Create: visualization output `microcap_v20_v23_v25_trailing_1y_metrics.csv`
- Create: visualization output `microcap_v20_v23_v25_trailing_1y_nav.csv`
- Create: visualization output `microcap_v20_v23_v25_trailing_1y_curve.png`
- Create: visualization output `microcap_v20_v23_v25_freshness.json`

- [ ] **Step 1: Read back artifact dates and row counts**

Read the refreshed panel shadow, proxy index, proxy turnover, base costed NAV, and all three version costed NAV files from disk. Require daily streams to share the latest close date and turnover to include the latest required rebalance date.

- [ ] **Step 2: Align the common trailing-year window**

Use all common dates from one calendar year before the shared end date through the end date. Normalize each costed NAV to `1.0` on the first common date.

- [ ] **Step 3: Calculate performance**

For each version, calculate total return, annualized return using 244 trading days, maximum drawdown, and annualized volatility from `return_net` over the common date window.

- [ ] **Step 4: Render and inspect the overlay chart**

Plot the three normalized costed NAV series on one chart, save at 1600x900 resolution, and visually inspect the rendered PNG for legibility and correct date/legend labels.

- [ ] **Step 5: Report formal results**

Report the exact common date range, metrics, freshness evidence, environmental refresh issue resolved, and clickable artifact links. Mark all numbers as costed and avoid mixing gross streams.
