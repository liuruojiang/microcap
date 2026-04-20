# v1.1 Momentum Gap Peak Decay Derisk Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `v1.1` research overlay that derisks an active trade to `30% / 50% / 70%` scale when trade-level `momentum_gap` decays enough from its own peak, then scan `20%` to `80%` decay thresholds on refreshed real data.

**Architecture:** Keep the repaired `v1.1` signal engine unchanged. Add one post-processing execution helper in `microcap_top100_mom16_biweekly_live.py` that replays the base gross path, tracks trade-level `momentum_gap` peak and decay ratio, and scales realized spread returns from `1.0x` down to a chosen derisk scale without forcing immediate cash exit. Preserve the repaired executed-return invariants and report corrected research outputs under `outputs/`.

**Tech Stack:** Python, `pandas`, `unittest`, existing Top100 live helpers and cost-model utilities

---

### Task 1: Write failing tests for the derisk state machine

**Files:**
- Modify: `C:\Users\Administrator.DESKTOP-95I7VVU\Desktop\动量策略\微盘股对冲策略\test_top100_forced_stop_loss.py`
- Test: `C:\Users\Administrator.DESKTOP-95I7VVU\Desktop\动量策略\微盘股对冲策略\test_top100_forced_stop_loss.py`

- [ ] **Step 1: Add a deterministic fixture for a trade that should derisk after gap decay**

Use a helper fixture that includes:

```python
return
holding
next_holding
signal_on
momentum_gap
microcap_close
hedge_close
```

with a path where `momentum_gap` first rises, then decays below the tested ratio while the trade remains active.

- [ ] **Step 2: Add failing tests that lock the intended behavior**

Add tests shaped like:

```python
def test_gap_peak_decay_derisk_switches_execution_scale_after_threshold():
    ...

def test_gap_peak_decay_derisk_scales_return_net_on_derisked_days():
    ...

def test_gap_peak_decay_derisk_does_not_use_full_return_after_scale_cut():
    ...
```

- [ ] **Step 3: Run the focused test file to verify these new tests fail**

Run:

```powershell
python -m unittest .\test_top100_forced_stop_loss.py -v
```

Expected: the new derisk tests fail because the helper does not exist yet.

### Task 2: Implement the corrected derisk execution helper

**Files:**
- Modify: `C:\Users\Administrator.DESKTOP-95I7VVU\Desktop\动量策略\微盘股对冲策略\microcap_top100_mom16_biweekly_live.py`
- Test: `C:\Users\Administrator.DESKTOP-95I7VVU\Desktop\动量策略\微盘股对冲策略\test_top100_forced_stop_loss.py`

- [ ] **Step 1: Add the new helper**

Add:

```python
def apply_momentum_gap_peak_decay_derisk(
    gross_result: pd.DataFrame,
    turnover_df: pd.DataFrame,
    decay_ratio_threshold: float,
    derisk_scale: float,
) -> pd.DataFrame:
    ...
```

- [ ] **Step 2: Track trade-level gap peak and decay ratio**

Within each executed trade, maintain:

```python
gap_peak = max(gap_peak, current_gap)
gap_decay_ratio = current_gap / gap_peak if gap_peak > 0 else 0.0
```

- [ ] **Step 3: Track execution scale as part of the state machine**

Use:

```python
execution_scale = 1.0
if gap_decay_ratio <= decay_ratio_threshold:
    execution_scale = derisk_scale
```

Once derisked, keep the trade at `derisk_scale` until the base signal exits to `cash`.

- [ ] **Step 4: Scale realized returns with the corrected invariant**

Use the repaired pattern:

```python
gross_daily_return = float(returns.loc[dt])
realized_daily_return = gross_daily_return * execution_scale if current_active else 0.0
```

Never allow a full base return after the trade has been derisked.

- [ ] **Step 5: Emit audit columns**

Populate:

```python
gap_peak
gap_decay_ratio
signal_quality_derisk_triggered
execution_scale
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

### Task 3: Add a real-data grid scan

**Files:**
- Modify: `C:\Users\Administrator.DESKTOP-95I7VVU\Desktop\动量策略\微盘股对冲策略\microcap_top100_mom16_biweekly_live.py`

- [ ] **Step 1: Add a scan helper over decay thresholds and derisk scales**

Add:

```python
def run_momentum_gap_peak_decay_derisk_scan(
    gross_result: pd.DataFrame,
    turnover_df: pd.DataFrame,
    decay_thresholds: tuple[float, ...],
    derisk_scales: tuple[float, ...],
) -> pd.DataFrame:
    ...
```

- [ ] **Step 2: Use the first-pass grid**

Use:

```python
decay_thresholds = (0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8)
derisk_scales = (0.3, 0.5, 0.7)
```

- [ ] **Step 3: Save the full-sample scan output**

Write:

```text
outputs/microcap_top100_v1_1_momentum_gap_peak_decay_derisk_scan_corrected.csv
```

Each row should include:

```text
threshold
threshold_pct
derisk_scale
signal_quality_derisk_count
annual_return
max_drawdown
total_return
final_nav
```

### Task 4: Run refreshed real-data validation on `v1.1`

**Files:**
- Output: `C:\Users\Administrator.DESKTOP-95I7VVU\Desktop\动量策略\微盘股对冲策略\outputs\microcap_top100_v1_1_momentum_gap_peak_decay_derisk_scan_corrected.csv`
- Output: `C:\Users\Administrator.DESKTOP-95I7VVU\Desktop\动量策略\微盘股对冲策略\outputs\microcap_top100_v1_1_momentum_gap_peak_decay_derisk_scan_corrected_windows.csv`

- [ ] **Step 1: Refresh the repaired `v1.1` baseline and load real gross result plus turnover**

Use the repo's real refresh chain before scanning.

- [ ] **Step 2: Run the full grid on real local data**

Generate the full-sample table.

- [ ] **Step 3: Build the window comparison table**

Create:

```text
outputs/microcap_top100_v1_1_momentum_gap_peak_decay_derisk_scan_corrected_windows.csv
```

For each `(threshold, derisk_scale)` pair, report:

```text
full, 1y, 3y, 5y, 10y
annual_return
max_drawdown
total_return
signal_quality_derisk_count
```

- [ ] **Step 4: Run baseline safety tests**

Run:

```powershell
python -m unittest .\test_top100_v1_1_output_compatibility.py .\test_top100_hedge_ratio_application.py -v
```

Expected: PASS.

- [ ] **Step 5: Report corrected observed results**

Report:

- exact files inspected;
- exact data path used;
- exact commands run;
- best threshold-scale pair;
- whether any pair beats repaired `v1.1 baseline` on both return and drawdown, or only on one side.
