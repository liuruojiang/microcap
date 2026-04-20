# v1.1 Momentum Gap Peak Decay Exit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `v1.1` research overlay that exits when trade-level `momentum_gap` decays enough from its own peak, then scan `0.2` to `0.8` decay thresholds on refreshed real data.

**Architecture:** Keep the repaired `v1.1` signal engine unchanged. Add one post-processing execution helper in `microcap_top100_mom16_biweekly_live.py` that replays the base gross path, tracks trade-level `momentum_gap` peak and decay ratio, enforces strict signal reset after exit, and preserves the corrected executed-return invariants. Then run the scan against refreshed `v1.1` data and save research tables under `outputs/`.

**Tech Stack:** Python, `pandas`, `unittest`, existing Top100 live helpers and cost-model utilities

---

### Task 1: Lock the signal-quality exit behavior with failing tests

**Files:**
- Modify: `C:\Users\Administrator.DESKTOP-95I7VVU\Desktop\动量策略\微盘股对冲策略\test_top100_forced_stop_loss.py`
- Test: `C:\Users\Administrator.DESKTOP-95I7VVU\Desktop\动量策略\微盘股对冲策略\test_top100_forced_stop_loss.py`

- [ ] **Step 1: Write failing tests for trade-level `momentum_gap` peak decay exit**

Add tests that prove:

```python
def test_gap_peak_decay_exit_triggers_when_gap_drops_below_trade_peak_ratio():
    ...

def test_gap_peak_decay_exit_blocks_until_base_signal_resets():
    ...

def test_gap_peak_decay_cash_days_have_zero_return_net_after_exit():
    ...
```

- [ ] **Step 2: Run the focused test file and verify the new tests fail for missing behavior**

Run:

```powershell
python -m unittest .\test_top100_forced_stop_loss.py -v
```

Expected: the new `momentum_gap` peak-decay tests fail because the helper does not exist yet.

- [ ] **Step 3: Do not change production logic yet**

Keep the failure as proof the tests are actually checking missing behavior.

- [ ] **Step 4: Record the exact failing test names before implementation**

Expected failing names include:

```text
test_gap_peak_decay_exit_triggers_when_gap_drops_below_trade_peak_ratio
test_gap_peak_decay_exit_blocks_until_base_signal_resets
test_gap_peak_decay_cash_days_have_zero_return_net_after_exit
```

### Task 2: Implement the execution helper with corrected return semantics

**Files:**
- Modify: `C:\Users\Administrator.DESKTOP-95I7VVU\Desktop\动量策略\微盘股对冲策略\microcap_top100_mom16_biweekly_live.py`
- Test: `C:\Users\Administrator.DESKTOP-95I7VVU\Desktop\动量策略\微盘股对冲策略\test_top100_forced_stop_loss.py`

- [ ] **Step 1: Add a new helper**

Add:

```python
def apply_momentum_gap_peak_decay_exit(
    gross_result: pd.DataFrame,
    turnover_df: pd.DataFrame,
    decay_ratio_threshold: float,
    require_signal_reset_after_exit: bool = True,
) -> pd.DataFrame:
    ...
```

- [ ] **Step 2: Track peak and decay ratio per executed trade**

Inside the helper, maintain:

```python
gap_peak = max(gap_peak, current_gap)
gap_decay_ratio = current_gap / gap_peak if gap_peak > 0 else 0.0
```

Use the trade's executed state, not the base signal alone.

- [ ] **Step 3: Exit only on active executed trades whose base signal still wants to remain active**

Core trigger shape:

```python
if current_active and desired_next_active and gap_decay_ratio <= decay_ratio_threshold:
    desired_next_active = False
    signal_quality_exit_triggered = True
```

- [ ] **Step 4: Preserve corrected cash-day return semantics**

Use the repaired invariant already applied to other helpers:

```python
gross_daily_return = float(returns.loc[dt])
realized_daily_return = gross_daily_return if current_active else 0.0
```

Blocked cash days must not inherit base `gross` returns.

- [ ] **Step 5: Add output columns needed for audit and scan**

Populate:

```python
gap_peak
gap_decay_ratio
signal_quality_exit_triggered
blocked_until_signal_reset
signal_reset_seen
trade_id
trade_return_net
entry_exit_cost
rebalance_cost
total_cost
return_net
nav_net
```

- [ ] **Step 6: Re-run the focused test file and confirm green**

Run:

```powershell
python -m unittest .\test_top100_forced_stop_loss.py -v
```

Expected: all tests pass.

### Task 3: Add a real-data scan path

**Files:**
- Modify: `C:\Users\Administrator.DESKTOP-95I7VVU\Desktop\动量策略\微盘股对冲策略\microcap_top100_mom16_biweekly_live.py`

- [ ] **Step 1: Add a lightweight scan helper for the decay-ratio grid**

Add a helper shaped like:

```python
def run_momentum_gap_peak_decay_scan(
    gross_result: pd.DataFrame,
    turnover_df: pd.DataFrame,
    decay_thresholds: tuple[float, ...],
) -> pd.DataFrame:
    ...
```

Each row should report:

```python
threshold
threshold_pct
signal_quality_exit_count
annual_return
max_drawdown
total_return
final_nav
```

- [ ] **Step 2: Use the first-pass threshold grid from the spec**

Use:

```python
(0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8)
```

- [ ] **Step 3: Save the research scan output under `outputs/`**

Write:

```text
outputs/microcap_top100_v1_1_momentum_gap_peak_decay_scan_corrected.csv
```

### Task 4: Run refreshed real-data validation on `v1.1`

**Files:**
- Modify: `C:\Users\Administrator.DESKTOP-95I7VVU\Desktop\动量策略\微盘股对冲策略\microcap_top100_mom16_biweekly_live.py` only if verification exposes defects
- Output: `C:\Users\Administrator.DESKTOP-95I7VVU\Desktop\动量策略\微盘股对冲策略\outputs\microcap_top100_v1_1_momentum_gap_peak_decay_scan_corrected.csv`

- [ ] **Step 1: Refresh the repaired `v1.1` baseline to the latest trade date**

Run the existing `v1.1` refresh chain indirectly from Python before scanning.

- [ ] **Step 2: Recompute the real `v1.1` gross result and turnover table**

Use the real workspace files, not cached summary numbers.

- [ ] **Step 3: Run the scan and save the full-sample table**

Expected output file:

```text
outputs/microcap_top100_v1_1_momentum_gap_peak_decay_scan_corrected.csv
```

- [ ] **Step 4: Build the window comparison table**

Create:

```text
outputs/microcap_top100_v1_1_momentum_gap_peak_decay_scan_corrected_windows.csv
```

For each threshold, include:

```text
full, 1y, 3y, 5y, 10y
annual_return
max_drawdown
total_return
signal_quality_exit_count
```

- [ ] **Step 5: Run baseline safety tests**

Run:

```powershell
python -m unittest .\test_top100_v1_1_output_compatibility.py .\test_top100_hedge_ratio_application.py -v
```

Expected: PASS.

- [ ] **Step 6: Report corrected observed results**

Report:

- exact files inspected;
- exact data path used;
- exact commands run;
- whether the best threshold actually beats repaired `v1.1 baseline`;
- whether execution-layer invariants remained clean.
