# Microcap v2 Adversarial Review Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Repair every approved formal and research-path defect from the v2.0/v2.3/v2.5 adversarial review while preserving close execution, full-capital redistribution, official signal parameters, and the synthetic-basket execution abstraction.

**Architecture:** Add fail-closed validation and scoped publication helpers to the existing standalone version family, then centralize v2.5 scan baseline/cost invariants in one small helper. Keep version algorithms intact and make all repaired paths prove freshness, schema, execution state, and baseline parity before publishing.

**Tech Stack:** Python 3, pandas, NumPy, pytest, matplotlib, existing local Top100 CSV/JSON artifacts.

---

### Task 1: Preserve the workspace and establish a fresh test baseline

**Files:**
- Verify: `AGENTS.md`
- Back up: `microcap_top100_mom16_biweekly_live_v2_0.py`
- Back up: `microcap_top100_mom16_biweekly_live_v2_3.py`
- Back up: `microcap_top100_mom16_biweekly_live_v2_5.py`
- Back up: `scripts/run_microcap_v2_5_*.py`
- Back up: the four currently modified `outputs/` artifacts

- [ ] **Step 1: Run the quant-research backup helper**

```powershell
python D:/Codex/home/skills/quant-research/scripts/backup_paths.py --root . microcap_top100_mom16_biweekly_live_v2_0.py microcap_top100_mom16_biweekly_live_v2_3.py microcap_top100_mom16_biweekly_live_v2_5.py scripts outputs/microcap_top100_mom16_biweekly_live_v2_0_base_panel_refreshed.csv outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_meta.json outputs/microcap_top100_mom16_hedge_zz1000_0p8x_biweekly_thursday_16y_v2_0_base_costed_nav.csv outputs/wind_microcap_top_100_biweekly_thursday_16y_cached.csv
```

Expected: a new backup directory is reported and exists.

- [ ] **Step 2: Refresh the official streams before running Top100 tests**

Run the repository's auditable no-new-rebalance tail-extension route when its preconditions hold; otherwise use the official generators. Read back panel, proxy index, proxy turnover, base costed NAV, v2.0, v2.3, and v2.5 dates and row counts.

Expected: all daily streams share one latest close-confirmed date and turnover reaches the latest required rebalance date. If refresh cannot align them, stop formal execution and use only source/AST checks until the blocker is resolved.

- [ ] **Step 3: Record preservation tests for the two approved assumptions**

Add to `tests/test_microcap_v2_review_remediations.py`:

```python
def test_close_execution_remains_the_official_proxy_timing() -> None:
    assert v2_0.base_mod.EXECUTION_TIMING == v2_0.freq_mod.EXECUTION_TIMING_CLOSE


def test_underfilled_proxy_keeps_total_capital_fully_invested() -> None:
    idx = pd.to_datetime(["2026-01-05", "2026-01-06"])
    result, _, _ = v2_0.freq_mod.simulate_rebalance_path(
        trading_dates=idx,
        returns_df=pd.DataFrame({"000001": [0.0, 0.10]}, index=idx),
        target_members_map={idx[0]: ["000001", "000002"]},
        rebalance_dates=pd.DatetimeIndex([idx[0]]),
        buyable_df=pd.DataFrame({"000001": [True, True], "000002": [False, False]}, index=idx),
        sellable_df=pd.DataFrame(True, index=idx, columns=["000001", "000002"]),
        one_side_cost_rate=0.003,
        top_n=2,
        execution_timing=v2_0.freq_mod.EXECUTION_TIMING_CLOSE,
    )
    assert result.iloc[-1]["daily_return"] == pytest.approx(0.10)
```

- [ ] **Step 4: Run the preservation tests**

Run: `python -m pytest tests/test_microcap_v2_review_remediations.py -k "close_execution or underfilled_proxy" -q -p no:cacheprovider`

Expected: PASS before production changes.

### Task 2: Make realtime data fail closed and preserve provenance

**Files:**
- Modify: `microcap_top100_mom16_biweekly_live_v2_0.py:6810-6902,8669-9115,10309-10463,12616-12720`
- Modify: `microcap_top100_mom16_biweekly_live_v2_3.py:1427-1456,1764-1885`
- Modify: `microcap_top100_mom16_biweekly_live_v2_5.py:2018-2128`
- Test: `tests/test_microcap_v2_review_remediations.py`

- [ ] **Step 1: Write failing realtime tests**

```python
def test_flat_fallback_quotes_do_not_count_as_actionable_coverage():
    members = ["000001", "000002"]
    quotes = pd.DataFrame(
        [{"code": "000001", "rt_price": 10.1, "pre_close": 10.0, "trade_date": "2026-07-06", "quote_time": "14:59:00"}]
    )
    augmented, fallback_count = v2_0.base_mod.add_last_close_flat_fallback_quotes(
        quotes,
        member_symbols=members,
        last_close_map={"000001": 10.0, "000002": 20.0},
        latest_trade_date=pd.Timestamp("2026-07-03"),
        max_missing_count=1,
        min_quoted_fraction=0.5,
    )
    indexed = augmented.set_index("code")
    stats = v2_0.base_mod.extract_member_quote_trade_date_stats(indexed, members, pd.Timestamp("2026-07-03"))
    meta = {
        "member_count": 2,
        "member_price_count": 2,
        "member_quote_flat_fallback_count": fallback_count,
        "member_quote_bad_symbols": stats["member_quote_bad_symbols"],
        "member_quote_trade_date_min": stats["member_quote_trade_date_min"],
        "member_quote_trade_date_max": stats["member_quote_trade_date_max"],
        "member_quote_trade_date_count": stats["member_quote_trade_date_count"],
        "hedge_quote_source": sorted(v2_0.base_mod.ALLOWED_ACTIONABLE_HEDGE_QUOTE_SOURCES)[0],
        "hedge_quote_trade_date": "2026-07-06",
        "quote_trade_date": "2026-07-06",
        "latest_anchor_trade_date": "2026-07-03",
        "expected_latest_completed_trade_date": "2026-07-03",
    }
    with pytest.raises(RuntimeError, match="synthetic|fallback"):
        v2_0.base_mod.assert_realtime_meta_is_actionable(meta)


def test_anchor_guard_rejects_calendar_that_does_not_reach_expected_close(monkeypatch):
    monkeypatch.setattr(v2_0.base_mod, "_load_realtime_anchor_calendar_index", lambda: pd.to_datetime(["2026-07-02"]))
    with pytest.raises(RuntimeError, match="2026-07-03"):
        v2_0.base_mod.assert_realtime_anchor_precedes_quote_trade_date(
            {"latest_anchor_trade_date": "2026-07-02", "quote_trade_date": "2026-07-06", "expected_latest_completed_trade_date": "2026-07-03"}
        )


def test_realtime_signal_rows_preserve_fallback_and_snapshot_provenance():
    row = pd.DataFrame([{}])
    v2_0.overlay_mod._apply_realtime_meta_columns_to_signal_row(
        row,
        {
            "fallback_warning": "stale cache",
            "member_quote_flat_fallback_count": 2,
            "snapshot_row_appended": True,
            "from_cache": True,
            "cache_age_seconds": 12.5,
        },
    )
    assert row.at[0, "fallback_warning"] == "stale cache"
    assert row.at[0, "member_quote_flat_fallback_count"] == 2
    assert bool(row.at[0, "snapshot_row_appended"]) is True
```

- [ ] **Step 2: Verify the realtime tests fail for the reviewed defects**

Run: `python -m pytest tests/test_microcap_v2_review_remediations.py -k "flat_fallback or anchor_guard or snapshot_provenance" -vv -p no:cacheprovider`.

Expected: fallback is currently accepted, independent expected close is ignored, and provenance columns are absent.

- [ ] **Step 3: Implement fail-closed metadata checks**

Add these mandatory semantics to v2.0's realtime validator:

```python
flat_count = int(meta.get("member_quote_flat_fallback_count") or 0)
if flat_count:
    raise RuntimeError(f"Realtime snapshot uses {flat_count} synthetic last-close fallback quotes; preview only.")
expected_close = str(meta.get("expected_latest_completed_trade_date") or "").strip()
if not expected_close:
    raise RuntimeError("Realtime meta missing independently refreshed expected latest completed trade date.")
if pd.Timestamp(meta["latest_anchor_trade_date"]).normalize() != pd.Timestamp(expected_close).normalize():
    raise RuntimeError("Realtime anchor does not equal independently refreshed latest completed trade date.")
```

Keep synthetic rows in preview data, but calculate genuine coverage before adding them and propagate the fallback count/source separately.

- [ ] **Step 4: Use the fresh base calendar for v2.3 realtime**

Build the v2.3 official index from the validated realtime base historical frame through `latest_anchor_trade_date`, then union only a verified snapshot row. Do not read the v2.0 target-vol costed file for this calendar.

- [ ] **Step 5: Propagate and print provenance in all versions**

Add `fallback_warning`, `member_quote_flat_fallback_count`, `from_cache`, `cache_age_seconds`, `snapshot_row_appended`, `latest_anchor_trade_date`, and `quote_trade_date` to forced realtime columns and stdout.

- [ ] **Step 6: Run focused realtime tests and commit**

Run: `python -m pytest tests/test_microcap_v2_review_remediations.py -k realtime -q -p no:cacheprovider`

Expected: PASS.

Commit: `fix: fail closed on stale microcap realtime state`

### Task 3: Publish formal outputs only after validated staging

**Files:**
- Modify: `microcap_top100_mom16_biweekly_live_v2_0.py:10828-10872,11202-11284,12122-12588`
- Modify: `microcap_top100_mom16_biweekly_live_v2_3.py:416-456,1286-1323,1622-1755`
- Modify: `microcap_top100_mom16_biweekly_live_v2_5.py:426-466,1522-1561,1861-2003`
- Test: `tests/test_microcap_v2_review_remediations.py`

- [ ] **Step 1: Write failing publication tests**

```python
@pytest.mark.parametrize("module,generate_name", [(v2_0.overlay_mod, "generate_v2_0_outputs"), (v2_3, "_generate_v2_3_outputs_unlocked"), (v2_5, "_generate_v2_5_outputs_unlocked")])
def test_failed_candidate_freshness_does_not_replace_official_artifacts(module, generate_name, tmp_path, monkeypatch):
    paths = patch_generation_paths(module, tmp_path, initial_text="old")
    monkeypatch.setattr(v2_0, "assert_top100_candidate_fresh", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("stale")))
    with pytest.raises(RuntimeError, match="stale"):
        getattr(module, generate_name)()
    assert all(path.read_text() == "old" for path in paths)
```

- [ ] **Step 2: Verify RED**

Expected: current generators replace costed/NAV before raising.

- [ ] **Step 3: Add shared staging and candidate freshness helpers**

Expose from v2.0:

```python
@contextmanager
def staged_output_bundle(targets: list[Path], *, summary_path: Path):
    staged = {target: _atomic_temp_path(target) for target in targets}
    try:
        yield staged
        ordered = [p for p in targets if p != summary_path] + [summary_path]
        for target in ordered:
            if not staged[target].exists():
                raise RuntimeError(f"staged output missing: {target}")
        for target in ordered:
            _replace_with_retry(staged[target], target)
    finally:
        for path in staged.values():
            path.unlink(missing_ok=True)
```

Add `assert_top100_candidate_fresh(candidate_index, expected_latest_date, label)` to validate base artifact dates and candidate continuity without reading a newly published official file.

- [ ] **Step 4: Make performance builders accept explicit staged paths**

Add a `paths` mapping argument to each `build_performance_payload`; write CSV/JSON/PNG only to supplied staged paths.

- [ ] **Step 5: Stage, validate, promote, and read back in all three generators**

Generate frames and summaries in memory, run historical rewrite and candidate freshness checks, write all staged files, promote summary last, and then call the existing read-back freshness proof.

- [ ] **Step 6: Run publication tests and commit**

Commit: `fix: stage microcap outputs before publication`

### Task 4: Harden proxy loading, audit state, path isolation, and locks

**Files:**
- Modify: `microcap_top100_mom16_biweekly_live_v2_0.py:2958-3099,4402-4433,7455-7589,9961-9998,10502-10579,11541-11564,12437-12451,12861-12904`
- Test: `tests/test_microcap_v2_review_remediations.py`

- [ ] **Step 1: Write failing tests for corrupted cache, prefix, audit, state flag, and dead lock**

Add these exact cases to `tests/test_microcap_v2_review_remediations.py`:

```python
def test_custom_v2_output_prefix_isolates_overlay_paths():
    v2_0.configure_output_paths(output_prefix="audit_v20", costed_nav_csv=None)
    assert v2_0.OUTPUT_PREFIX == "audit_v20"
    assert v2_0.SUMMARY_JSON.name == "audit_v20_summary.json"
    assert v2_0.COSTED_NAV_CSV.name == "audit_v20_costed_nav.csv"


def test_clean_rewrite_audit_removes_stale_failure_rows(tmp_path):
    audit = tmp_path / "audit.csv"
    audit.write_text("date,column\n2020-01-01,return_net\n", encoding="utf-8")
    v2_0.base_mod.clear_rewrite_audit_after_clean_result(audit)
    assert not audit.exists()


def test_dead_pid_lock_is_recovered_immediately(tmp_path, monkeypatch):
    monkeypatch.setattr(v2_0, "OUTPUT_DIR", tmp_path)
    monkeypatch.setattr(v2_0, "_pid_is_alive", lambda _pid: False)
    lock = tmp_path / "dead.lock"
    lock.write_text("999999", encoding="ascii")
    with v2_0._v2_file_lock("dead.lock", wait_timeout_seconds=0.2, stale_lock_seconds=600.0):
        assert lock.exists()
    assert not lock.exists()
```

Extend the existing proxy-loader test fixture so a malformed price CSV raises `RuntimeError` containing the symbol, while a symbol whose valid history begins after the requested window is counted in `legitimately_unavailable`. Extend `test_v2_0_volatility_overheat_exit_blocks_until_base_signal_reset` with `assert bool(out.loc[trigger_date, "blocked_until_signal_reset"]) is True`.

- [ ] **Step 2: Verify RED for each behavior**

- [ ] **Step 3: Stop swallowing unexpected symbol errors**

Keep explicit `None` returns only for legitimate no-history/pre-listing cases. Allow schema/parse exceptions to reach `load_cache_panels`, collect `symbol -> exception`, and raise one deterministic error after all futures complete. Store load statistics in `returns_df.attrs["symbol_load_stats"]` and proxy metadata.

- [ ] **Step 4: Enforce ranked-universe availability**

Before trade constraints, raise with rebalance dates/counts if any cap map has fewer than `TOP_N` eligible candidates. Do not change post-trade full-capital redistribution.

- [ ] **Step 5: Isolate output paths and runtime state**

Add a v2.0 overlay `configure_output_paths()` matching v2.3/v2.5 and call it from runtime configuration. Add a context manager that saves and restores `_V2_RUNTIME_ARGS` for imported/in-process use.

- [ ] **Step 6: Clean audit and lock state**

Delete stale audit CSV on a clean result and set `audit_csv` to `None`; append blocked state after processing the trigger; remove a confirmed dead-PID lock immediately.

- [ ] **Step 7: Run focused tests and commit**

Commit: `fix: harden microcap proxy and audit guards`

### Task 5: Normalize v2.3 and v2.5 schemas without changing signals

**Files:**
- Modify: `microcap_top100_mom16_biweekly_live_v2_3.py:491-660,1089-1147,1326-1379`
- Modify: `microcap_top100_mom16_biweekly_live_v2_5.py:1241-1354,1564-1626,1921-1988,2018-2128`
- Test: `tests/test_microcap_v2_review_remediations.py`

- [ ] **Step 1: Write failing schema tests**

Assert v2.3 has one coherent overheat schema, disabled target-vol fields are zero/disabled, component momentum fields are not aliases, v2.5 lacks `summary_version_key="hedge_0.8"`, and realtime CSV rows contain provenance.

- [ ] **Step 2: Verify RED**

- [ ] **Step 3: Calculate component log-WLS diagnostics in v2.3**

Use the existing `log_wls_score_and_r2` on microcap and hedge component NAV series. Keep `annualized_log_wls_score` as the only signal score and `momentum_gap` as the documented compatibility alias.

- [ ] **Step 4: Explicitly overwrite inherited fields**

Mirror v2.5's clearing pattern in v2.3 for overheat/target-vol fields. Remove incompatible inherited v2.0 summary keys in v2.5 and add `synthetic_basket_execution=True` metadata to all formal summaries.

- [ ] **Step 5: Run schema tests and commit**

Commit: `fix: normalize microcap v2 signal schemas`

### Task 6: Repair v2.5 target-vol and staged/cooldown cost replay

**Files:**
- Create: `scripts/microcap_v2_5_scan_common.py`
- Modify: `microcap_top100_mom16_biweekly_live_v2_5.py:973-1141`
- Modify: `scripts/run_microcap_v2_5_staged_entry_scan.py:76-220,285-420`
- Modify: `scripts/run_microcap_v2_5_env_breadth_cooldown_scan.py:236-308`
- Modify: `scripts/run_microcap_v2_5_target_vol_window_threshold_scan.py`
- Modify: `scripts/run_microcap_v2_5_volatility_shock_derisk_overlay_scan.py`
- Test: `tests/test_v2_5_scan_integrity.py`

- [ ] **Step 1: Write failing replay tests**

Add deterministic three-row fixtures with `holding=[cash,long,cash]`, `next_holding=[long,cash,cash]`, `base_pre_cost_return=0`, and `total_cost=[0.003,0.003,0]`. Assert:

```python
def test_target_vol_replay_preserves_exit_cost(transition_frame):
    out = v25.apply_target_vol(transition_frame, target_vol=0.30)
    assert out.loc[transition_frame.index[1], "return_net"] == pytest.approx(-0.003)


def test_staged_none_matches_official_return_net(transition_frame):
    out = staged.apply_staged_entry_overlay(transition_frame, trigger_scope="none")
    pd.testing.assert_series_equal(out["return_net"], transition_frame["return_net"], check_names=False)


def test_cooldown_does_not_double_charge_base_transition(transition_frame):
    out = cooldown._apply_cooldown(transition_frame, cooldown_days=3)
    assert out.loc[transition_frame.index[1], "return_net"] == pytest.approx(-0.003)


def test_legacy_static_preflight_is_rejected(tmp_path):
    legacy = tmp_path / "microcap_top100_mom16_biweekly_live_v2_5_scan_preflight_20260601_costed_nav.csv"
    legacy.write_text("date,return_net\n2026-06-01,0\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="official v2.5"):
        scan_common.reject_legacy_preflight(legacy)
```

For every candidate returned by the scale/cooldown helpers, assert that `holding.eq("cash")` is identical to `current_execution_scale.eq(0)` and that `next_holding.eq("cash")` is identical to `next_session_actionable_scale.eq(0)`.

- [ ] **Step 2: Verify RED with the reviewed 30bp and 59.91bp discrepancies**

- [ ] **Step 3: Implement shared baseline and incremental-cost helpers**

```python
def load_fresh_official_v25() -> tuple[dict[str, object], pd.DataFrame]:
    summary, _, frame = v25.generate_v2_5_outputs()
    v25.v2_0.assert_top100_outputs_fresh(
        expected_latest_date=frame.index.max(),
        extra_daily_paths={"v2_5_costed_nav": v25.COSTED_NAV_CSV},
    )
    if not v25.summary_matches_current_v2_5_base(summary):
        raise RuntimeError("official v2.5 fingerprint mismatch")
    return summary, frame


def base_cost_scale(holding, next_holding, current_scale, next_scale):
    scale = current_scale.copy()
    scale.loc[holding.eq("cash") & next_holding.ne("cash")] = next_scale
    scale.loc[holding.ne("cash") & next_holding.eq("cash")] = current_scale
    return scale.clip(lower=0.0)
```

- [ ] **Step 4: Make parity failures fatal and charge only incremental overlay trades**

The staged `none` candidate must assert equality with official `return_net`. Cooldown and scale filters subtract costs only for overlay deltas relative to the base executable scale path.

- [ ] **Step 5: Replace static preflight paths with the shared official loader**

- [ ] **Step 6: Run scan replay tests and commit**

Commit: `fix: repair v2.5 scan cost replay`

### Task 7: Enforce external feature freshness, adjustment, coverage, and canonical state

**Files:**
- Modify: `scripts/run_microcap_v2_5_env_breadth_cooldown_scan.py:134-261`
- Modify: `scripts/run_microcap_v2_5_zz2000_cyb_volume_scan.py:180-217,380-420`
- Modify: other v2.5 volume/breadth consumers located by `rg "common_end|close_raw|execution_scale" scripts/run_microcap_v2_5_*.py`
- Test: `tests/test_v2_5_scan_integrity.py`

- [ ] **Step 1: Write failing feature-integrity tests**

Test that stale amount data, under-covered breadth, raw-only price caches, and candidate returns with unchanged canonical holdings/scales are rejected.

- [ ] **Step 2: Verify RED**

- [ ] **Step 3: Require external end-date equality**

Replace the current min-of-NAV-and-feature end-date assignment with an explicit equality check and raise with both end dates.

- [ ] **Step 4: Use adjusted prices and coverage thresholds**

Load the repository adjusted-return column used by the proxy. Require at least 95 valid members and 95% coverage on every evaluated date; include failing dates and counts in errors.

- [ ] **Step 5: Rewrite canonical candidate state fields**

When an overlay changes exposure, update `holding`, `next_holding`, `current_execution_scale`, and `next_session_actionable_scale` to the executed candidate state while preserving `base_*` columns for comparison.

- [ ] **Step 6: Write reproducibility metadata and commit**

Include source dates/counts, adjustment mode, baseline fingerprint, parity, costs, and execution timing.

Commit: `fix: validate v2.5 scan feature inputs`

### Task 8: Full verification, official refresh, and handoff

**Files:**
- Verify all modified source and tests
- Update: `docs/microcap_top100_v2_3_v2_5_cleanup_sync_20260630.md` only if its active-path statements become stale

- [ ] **Step 1: Parse all modified Python files without writing bytecode**

Run AST parsing with `utf-8-sig`; expected: all files parse.

- [ ] **Step 2: Run focused tests, then the full suite**

```powershell
$env:PYTHONDONTWRITEBYTECODE='1'
python -m pytest tests/test_microcap_v2_review_remediations.py tests/test_v2_5_scan_integrity.py -q -p no:cacheprovider
python -m pytest tests -q -p no:cacheprovider
```

Expected: zero failures.

- [ ] **Step 3: Refresh and run official paths on real data**

Run v2.0, v2.3, and v2.5 official generation only after refresh proof. Read back panel, proxy index, proxy turnover, base costed NAV, all three version streams, summaries, and latest signals.

Expected: identical latest close-confirmed date; continuous daily indices; coherent schema; no stale/fallback actionable realtime result.

- [ ] **Step 4: Run scan baseline smoke tests**

Run baseline-only/parity modes for staged, cooldown, target-vol, volatility-shock, breadth, and volume scripts. Do not publish candidate performance.

- [ ] **Step 5: Inspect final diff and working tree**

Ensure the user's pre-existing output modifications are either preserved in the backup or intentionally superseded by the documented refresh. Exclude unrelated files.

- [ ] **Step 6: Request code review and commit final documentation**

Commit: `docs: record microcap v2 remediation verification`
