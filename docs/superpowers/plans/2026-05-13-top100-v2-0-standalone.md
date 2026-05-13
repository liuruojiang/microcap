# Top100 v2.0 Standalone Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `microcap_top100_mom16_biweekly_live_v2_0.py` as a standalone v2.0 entrypoint that preserves the current repaired v1.6 behavior while leaving v1.6 runnable.

**Architecture:** v2.0 will freeze the current production code it needs into one file, using only standard library plus third-party packages at runtime. The v2.0 overlay keeps the repaired v1.6 target-vol behavior, writes only v2.0 formal output names, and owns its close-confirmed, realtime, and performance-query paths.

**Tech Stack:** Python, pandas, numpy, matplotlib, requests, akshare, unittest, local CSV/cache data.

---

### Task 1: Lock Standalone Contract

**Files:**
- Create: `tests/test_top100_v2_0_standalone_contract.py`
- Create: `microcap_top100_mom16_biweekly_live_v2_0.py`

- [x] **Step 1: Write the failing contract test**

Create a unittest that fails until the v2.0 file exists and contains no forbidden old-version imports or local strategy/core imports.

- [x] **Step 2: Run the contract test and verify RED**

Run: `python -m unittest tests.test_top100_v2_0_standalone_contract -v`

Expected before implementation: FAIL because `microcap_top100_mom16_biweekly_live_v2_0.py` does not exist.

### Task 2: Build the Standalone Script

**Files:**
- Create: `microcap_top100_mom16_biweekly_live_v2_0.py`

- [x] **Step 1: Embed dependency code**

Embed the currently used helper code from the mainline/runtime modules into v2.0 namespaces so v2.0 does not import old version scripts, `top100_v14_base_context.py`, `top100_realtime_core.py`, or local strategy/core helper modules.

- [x] **Step 2: Set v2.0 baseline parameters**

Set v2.0 to `hedge ratio = 0.8`, `momentum gap exit buffer = 0.0030`, peak-decay parameters `0.25 / 0.0 / 0.35`, `target vol = 25%`, `max leverage = 1.5x`, and volatility source priority beginning with `constructed_microcap_minus_hedge`.

- [x] **Step 3: Isolate v2.0 outputs**

Use these formal paths:
`outputs/microcap_top100_mom16_biweekly_live_v2_0_summary.json`,
`outputs/microcap_top100_mom16_biweekly_live_v2_0_latest_signal.csv`,
`outputs/microcap_top100_mom16_targetvol25_max1p5_v2_0_costed_nav.csv`,
`outputs/microcap_top100_mom16_biweekly_live_v2_0_realtime_signal.csv`,
and `outputs/microcap_top100_mom16_biweekly_live_v2_0_performance_*`.

### Task 3: Verify Commands and Parity

**Files:**
- Create: `outputs/microcap_top100_mom16_biweekly_live_v2_0_parity_vs_v1_6.csv`

- [x] **Step 1: Run static dependency check**

Run: `rg "microcap_top100_mom16_biweekly_live_v1_|top100_v14_base_context|top100_realtime_core|import microcap_top100_mom16_biweekly_live" microcap_top100_mom16_biweekly_live_v2_0.py`

Expected: no matches.

- [x] **Step 2: Run official v2.0 outputs**

Run: `python microcap_top100_mom16_biweekly_live_v2_0.py`

Expected: v2.0 summary, latest signal, and target-vol costed NAV files are generated.

- [x] **Step 3: Run query routes**

Run:
`python microcap_top100_mom16_biweekly_live_v2_0.py "信号"`
`python microcap_top100_mom16_biweekly_live_v2_0.py "实时信号"`
`python microcap_top100_mom16_biweekly_live_v2_0.py "表现 1年"`

Expected: signal query prints v2.0 close-confirmed fields, realtime query writes v2.0 realtime CSV, and performance query writes v2.0 query-specific files.

- [x] **Step 4: Build parity table**

Compare current v1.6 and v2.0 on the same generated date index for key fields: `return_net`, `nav_net`, `holding`, `next_holding`, `execution_scale`, `current_execution_scale`, `next_session_actionable_scale`, `momentum_gap`, `base_pre_cost_return`, `target_vol_realized_vol`, and latest signal fields. Save the parity table and summarize material differences.

### Task 4: Final Low-Priority Cleanup

**Files:**
- Update: `microcap_top100_mom16_biweekly_live_v2_0.py`
- Update: `tests/test_top100_v2_0_standalone_contract.py`

- [x] **Step 1: Isolate overlay execution namespace**

Replace direct `exec(..., globals())` for `OVERLAY_SOURCE` with `_exec_embedded_module(...)`, expose the overlay through `overlay_mod`, and keep the outer output-generation lock calling `overlay_mod.generate_v2_0_outputs()`.

- [x] **Step 2: Rename internal compatibility lineage**

Rename v1.1/v1.4 compatibility adapter fields and overlay internals to embedded-lineage names, including context loaders, fingerprint fields, and base-return passthrough columns.

- [x] **Step 3: Verify cleanup**

Run the static contract tests, syntax check, no-import/no-globals searches, and v2.0 output/query routes after the cleanup.

### Task 5: Strategy-Level Data-Lineage Disclosure

**Files:**
- Update: `microcap_top100_mom16_biweekly_live_v2_0.py`
- Update: `tests/test_top100_v2_0_standalone_contract.py`

- [x] **Step 1: Add failing disclosure contract**

Require v2.0 summary output to expose microcap data lineage, official-Wind-vs-proxy status, ST-filter bias notes, proxy construction policy, and performance source labeling.

- [x] **Step 2: Implement lineage-aware summary and performance source labels**

Load the v2.0 base proxy meta, derive `data_lineage`, mark non-official proxy runs as `public_or_local_proxy_not_official_wind`, and include ST/history and proxy construction caveats at the top summary level.

- [x] **Step 3: Verify real outputs**

Run the contract test, syntax check, `python microcap_top100_mom16_biweekly_live_v2_0.py`, `python microcap_top100_mom16_biweekly_live_v2_0.py "表现 1年"`, and parse generated JSON with Python to confirm the new fields.

### Task 6: Query-Lock and Runtime Hygiene Fixes

**Files:**
- Update: `microcap_top100_mom16_biweekly_live_v2_0.py`
- Update: `tests/test_top100_v2_0_standalone_contract.py`

- [x] **Step 1: Lock close-confirmed query regeneration**

Save the unlocked overlay generator once, wrap it with the outer v2 generation lock, and point the overlay namespace `generate_v2_0_outputs` at the locked wrapper so `信号` and `表现` routes cannot bypass the lock.

- [x] **Step 2: Tighten runtime/dependency metadata**

Add `urllib3` to runtime dependency probing and make the wheelhouse runtime tag/path resolution work on non-Windows machines.

- [x] **Step 3: Clean output lineage edge cases**

Use `embedded_v2_base` in v2.0 NAV/signal output base-version fields, invalidate existing outputs when `SUMMARY_JSON` is missing, and unify `_safe_bool()` string handling.

- [x] **Step 4: Verify concurrency and outputs**

Run contract tests, py_compile, concurrent `信号`/`表现 1年` queries, and parse generated JSON/CSV to confirm source labels and base-version fields.
