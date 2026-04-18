# Mainline Query Refresh Routing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `microcap_top100_mom16_biweekly_live.py` return the latest accurate results for `信号` / `实时信号` / `成分股名单` / `进出名单` / `实时进出名单` / `净值图` / `净值表现` while avoiding unnecessary refresh work on each command.

**Architecture:** Add a command-aware query router inside `microcap_top100_mom16_biweekly_live.py`. Split the current monolithic `build_base_context()` path into smaller refresh helpers so each command refreshes only the artifacts it truly depends on, while still forcing the historical anchor and any required strategy files to the latest trading date.

**Tech Stack:** Python, `unittest`, `unittest.mock`, `pandas`, existing mainline strategy helpers in `microcap_top100_mom16_biweekly_live.py`

---

### Task 1: Add Routing Regression Tests

**Files:**
- Create: `test_mainline_query_routing.py`
- Modify: `microcap_top100_mom16_biweekly_live.py:2573-2778`
- Test: `test_mainline_query_routing.py`

- [ ] **Step 1: Write the failing tests for command classification and lean routing**

```python
from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

import microcap_top100_mom16_biweekly_live as base_mod


class MainlineQueryRoutingTests(unittest.TestCase):
    def make_args(self) -> SimpleNamespace:
        return SimpleNamespace(
            output_prefix="microcap_top100_mom16_biweekly_live",
            panel_path=base_mod.DEFAULT_PANEL_PATH,
            index_csv=base_mod.DEFAULT_INDEX_CSV,
            costed_nav_csv=base_mod.DEFAULT_COSTED_NAV_CSV,
            capital=None,
            max_workers=1,
            realtime_cache_seconds=30,
            allow_stale_realtime=False,
            rebuild_index_if_missing=True,
            max_stale_anchor_days=base_mod.DEFAULT_MAX_STALE_ANCHOR_DAYS,
        )

    def test_performance_query_routes_to_performance_kind(self) -> None:
        self.assertEqual(base_mod.classify_query_kind("净值表现 最近一年"), "performance")
        self.assertEqual(base_mod.classify_query_kind("净值图 最近一年"), "performance")

    def test_signal_query_routes_to_signal_kind(self) -> None:
        self.assertEqual(base_mod.classify_query_kind("信号"), "signal")

    def test_members_query_routes_to_members_kind(self) -> None:
        self.assertEqual(base_mod.classify_query_kind("成分股名单"), "members")

    def test_changes_query_routes_to_changes_kind(self) -> None:
        self.assertEqual(base_mod.classify_query_kind("进出名单"), "changes")

    def test_realtime_signal_query_routes_to_realtime_signal_kind(self) -> None:
        self.assertEqual(base_mod.classify_query_kind("实时信号"), "realtime_signal")

    def test_realtime_changes_query_routes_to_realtime_changes_kind(self) -> None:
        self.assertEqual(base_mod.classify_query_kind("实时进出名单"), "realtime_changes")

    def test_performance_query_does_not_load_member_snapshot(self) -> None:
        args = self.make_args()
        paths = base_mod.build_output_paths(args.output_prefix)
        perf_df = pd.DataFrame(
            {
                "return_net": [0.01, -0.02, 0.03],
                "nav_net": [1.01, 0.9898, 1.019494],
            },
            index=pd.to_datetime(["2026-04-08", "2026-04-09", "2026-04-10"]),
        )

        with patch.object(base_mod, "refresh_history_anchor", return_value=(paths["panel_shadow"], pd.Timestamp("2026-04-10"))):
            with patch.object(base_mod, "ensure_strategy_nav_fresh") as ensure_nav_mock:
                with patch.object(base_mod, "load_performance_source", return_value=(perf_df, "return_net", "nav_net", "costed")):
                    with patch.object(base_mod, "build_performance_outputs") as build_perf_mock:
                        with patch.object(base_mod, "load_member_snapshot", side_effect=AssertionError("must not load members")):
                            base_mod.execute_query(args, "净值表现 最近一年")

        ensure_nav_mock.assert_called_once()
        build_perf_mock.assert_called_once()

    def test_signal_query_does_not_load_member_snapshot_by_default(self) -> None:
        args = self.make_args()
        paths = base_mod.build_output_paths(args.output_prefix)
        base_context = {
            "paths": paths,
            "close_df": pd.DataFrame(index=pd.to_datetime(["2026-04-09", "2026-04-10"])),
            "latest_signal": pd.DataFrame([{"signal_label": "long", "trade_state": "hold"}]),
            "anchor_freshness": {
                "status": "fresh",
                "latest_trade_date": "2026-04-10",
                "current_date": "2026-04-12",
                "stale_calendar_days": 2,
            },
        }

        with patch.object(base_mod, "refresh_history_anchor", return_value=(paths["panel_shadow"], pd.Timestamp("2026-04-10"))):
            with patch.object(base_mod, "ensure_base_signal_fresh", return_value=base_context):
                with patch.object(base_mod, "load_member_snapshot", side_effect=AssertionError("must not load members")):
                    base_mod.execute_query(args, "信号")
```

- [ ] **Step 2: Run the new test file and verify it fails for the right reason**

Run: `python -m unittest test_mainline_query_routing.py -v`

Expected: FAIL with `AttributeError` for missing routing helpers such as `classify_query_kind`, `refresh_history_anchor`, `ensure_strategy_nav_fresh`, `ensure_base_signal_fresh`, or `execute_query`.

- [ ] **Step 3: Add routing helper stubs in the mainline script**

```python
def classify_query_kind(query: str) -> str:
    text = str(query or "").strip()
    if text == "实时信号":
        return "realtime_signal"
    if text == "实时进出名单":
        return "realtime_changes"
    if text in {"成分股", "成分股名单"}:
        return "members"
    if text == "进出名单":
        return "changes"
    if text == "信号":
        return "signal"
    if PERFORMANCE_PATTERN.search(text):
        return "performance"
    return "default"


def refresh_history_anchor(args: argparse.Namespace, paths: dict[str, Path]) -> tuple[Path, pd.Timestamp]:
    return build_refreshed_panel_shadow(args, paths)


def ensure_strategy_nav_fresh(
    args: argparse.Namespace,
    paths: dict[str, Path],
    panel_path: Path,
    target_end_date: pd.Timestamp,
) -> None:
    ensure_strategy_files(args, paths, panel_path, target_end_date)


def ensure_base_signal_fresh(
    args: argparse.Namespace,
    paths: dict[str, Path],
    panel_path: Path,
    target_end_date: pd.Timestamp,
) -> dict[str, object]:
    raise NotImplementedError


def execute_query(args: argparse.Namespace, query: str) -> None:
    raise NotImplementedError
```

- [ ] **Step 4: Run the test file again to verify the failure gets narrower**

Run: `python -m unittest test_mainline_query_routing.py -v`

Expected: FAIL because `ensure_base_signal_fresh()` / `execute_query()` are not implemented yet, while `classify_query_kind()` tests now pass.

- [ ] **Step 5: Commit the red baseline**

```bash
git add test_mainline_query_routing.py microcap_top100_mom16_biweekly_live.py
git commit -m "test: add mainline query routing regression coverage"
```

### Task 2: Implement History-Anchor and Performance Fast Path

**Files:**
- Modify: `microcap_top100_mom16_biweekly_live.py:1661-1783`
- Modify: `microcap_top100_mom16_biweekly_live.py:2573-2778`
- Test: `test_mainline_query_routing.py`

- [ ] **Step 1: Extend the failing tests to prove the performance path only refreshes required artifacts**

```python
    def test_performance_query_uses_latest_refreshed_anchor(self) -> None:
        args = self.make_args()
        paths = base_mod.build_output_paths(args.output_prefix)
        perf_df = pd.DataFrame(
            {
                "return_net": [0.01, 0.02],
                "nav_net": [1.01, 1.0302],
            },
            index=pd.to_datetime(["2026-04-09", "2026-04-10"]),
        )

        with patch.object(base_mod, "refresh_history_anchor", return_value=(paths["panel_shadow"], pd.Timestamp("2026-04-10"))) as refresh_mock:
            with patch.object(base_mod, "ensure_strategy_nav_fresh") as ensure_nav_mock:
                with patch.object(base_mod, "load_performance_source", return_value=(perf_df, "return_net", "nav_net", "costed")):
                    with patch.object(base_mod, "build_performance_outputs") as build_perf_mock:
                        base_mod.execute_query(args, "净值图 最近一年")

        refresh_mock.assert_called_once()
        ensure_nav_mock.assert_called_once_with(args, paths, paths["panel_shadow"], pd.Timestamp("2026-04-10"))
        build_perf_mock.assert_called_once()
```

- [ ] **Step 2: Run the performance-focused tests and verify they fail**

Run: `python -m unittest test_mainline_query_routing.py -v`

Expected: FAIL because `execute_query()` still does not implement the performance branch.

- [ ] **Step 3: Implement the lean base-signal helper and performance query handler**

```python
def ensure_base_signal_fresh(
    args: argparse.Namespace,
    paths: dict[str, Path],
    panel_path: Path,
    target_end_date: pd.Timestamp,
) -> dict[str, object]:
    ensure_strategy_nav_fresh(args, paths, panel_path, target_end_date)
    close_df = load_close_df(panel_path, args.index_csv)
    result = run_signal(close_df)
    latest_signal = enrich_signal_frame(hedge_mod.build_latest_signal(result), result)
    anchor_freshness = assess_history_anchor_freshness(
        latest_trade_date=pd.Timestamp(result.index[-1]),
        max_stale_days=args.max_stale_anchor_days,
    )
    return {
        "paths": paths,
        "panel_path": panel_path,
        "target_end_date": pd.Timestamp(target_end_date),
        "close_df": close_df,
        "result": result,
        "latest_signal": latest_signal,
        "anchor_freshness": anchor_freshness,
    }


def handle_performance_query_fast(
    args: argparse.Namespace,
    paths: dict[str, Path],
    panel_path: Path,
    target_end_date: pd.Timestamp,
    query_text: str,
) -> None:
    ensure_strategy_nav_fresh(args, paths, panel_path, target_end_date)
    perf_df, ret_col, nav_col, source_label = load_performance_source(
        args.costed_nav_csv,
        fallback_result=pd.DataFrame(),
        index_csv=args.index_csv,
    )
    build_performance_outputs(
        perf_df=perf_df,
        ret_col=ret_col,
        nav_col=nav_col,
        source_label=source_label,
        query_text=query_text,
        paths=paths,
    )
    summary = pd.read_csv(paths["performance_summary"])
    yearly = pd.read_csv(paths["performance_yearly"])
    print("表现汇总")
    print(format_table(summary))
    print("年度分解")
    print(format_table(yearly, max_rows=30))
    print(f"已保存: {paths['performance_chart'].name}")
    print(f"已保存: {paths['performance_summary'].name}")
    print(f"已保存: {paths['performance_yearly'].name}")
    print(f"已保存: {paths['performance_nav'].name}")
    print(f"已保存: {paths['performance_json'].name}")
```

- [ ] **Step 4: Implement `execute_query()` to short-circuit performance commands**

```python
def execute_query(args: argparse.Namespace, query: str) -> None:
    paths = build_output_paths(args.output_prefix)
    panel_path, target_end_date = refresh_history_anchor(args, paths)
    kind = classify_query_kind(query)

    if kind == "performance":
        handle_performance_query_fast(args, paths, panel_path, target_end_date, query)
        return

    if kind in {"signal", "realtime_signal", "members", "changes", "realtime_changes"}:
        context = ensure_base_signal_fresh(args, paths, panel_path, target_end_date)
        handle_query(context, args, query)
        return

    context = build_base_context(args, include_members=True)
    if query:
        handle_query(context, args, query)
        return
    save_base_outputs(context)
    print_console_summary(context["summary"])
```

- [ ] **Step 5: Run the routing tests and ensure the performance path is green**

Run: `python -m unittest test_mainline_query_routing.py -v`

Expected: PASS for performance routing tests; other tests may still fail because members routing is not lean yet.

- [ ] **Step 6: Commit the performance fast path**

```bash
git add test_mainline_query_routing.py microcap_top100_mom16_biweekly_live.py
git commit -m "feat: add lean performance query routing"
```

### Task 3: Make Closed Signal Routing Lean by Default

**Files:**
- Modify: `microcap_top100_mom16_biweekly_live.py:1355-1475`
- Modify: `microcap_top100_mom16_biweekly_live.py:2573-2778`
- Test: `test_mainline_query_routing.py`

- [ ] **Step 1: Add a failing test that `信号` does not request static member snapshots**

```python
    def test_execute_query_signal_uses_base_signal_context_only(self) -> None:
        args = self.make_args()
        paths = base_mod.build_output_paths(args.output_prefix)
        base_context = {
            "paths": paths,
            "close_df": pd.DataFrame(index=pd.to_datetime(["2026-04-09", "2026-04-10"])),
            "latest_signal": pd.DataFrame([{"signal_label": "long", "trade_state": "hold"}]),
            "anchor_freshness": {
                "status": "fresh",
                "latest_trade_date": "2026-04-10",
                "current_date": "2026-04-12",
                "stale_calendar_days": 2,
            },
        }

        with patch.object(base_mod, "refresh_history_anchor", return_value=(paths["panel_shadow"], pd.Timestamp("2026-04-10"))):
            with patch.object(base_mod, "ensure_base_signal_fresh", return_value=base_context) as ensure_base_mock:
                with patch.object(base_mod, "handle_query") as handle_query_mock:
                    base_mod.execute_query(args, "信号")

        ensure_base_mock.assert_called_once()
        handle_query_mock.assert_called_once_with(base_context, args, "信号")
```

- [ ] **Step 2: Run the focused test and verify it fails if `execute_query()` still overbuilds**

Run: `python -m unittest test_mainline_query_routing.MainlineQueryRoutingTests.test_execute_query_signal_uses_base_signal_context_only -v`

Expected: FAIL if `execute_query()` still routes through `build_base_context()` or otherwise requires member data for `信号`.

- [ ] **Step 3: Harden `handle_query()` so `信号` only reads fields that exist in the lean base context**

```python
def handle_query(context: dict[str, object], args: argparse.Namespace, query: str) -> None:
    query = query.strip()
    paths = context["paths"]
    anchor_freshness = context.get("anchor_freshness", {})

    if query == "信号":
        ensure_closed_signal_anchor_is_fresh(context)
        latest_signal = context["latest_signal"]
        latest_signal.to_csv(paths["signal"], index=False, encoding="utf-8")
        print("确认信号")
        print(format_table(latest_signal))
        if anchor_freshness:
            print(
                "历史锚点: {status} | latest={latest} | today={today} | lag={lag}d".format(
                    status=anchor_freshness.get("status"),
                    latest=anchor_freshness.get("latest_trade_date"),
                    today=anchor_freshness.get("current_date"),
                    lag=anchor_freshness.get("stale_calendar_days"),
                )
            )
        print(f"已保存: {paths['signal'].name}")
        return
```

- [ ] **Step 4: Run the routing test file again**

Run: `python -m unittest test_mainline_query_routing.py -v`

Expected: PASS for `信号` routing tests.

- [ ] **Step 5: Commit the lean signal path**

```bash
git add test_mainline_query_routing.py microcap_top100_mom16_biweekly_live.py
git commit -m "feat: avoid member snapshot work for closed signal queries"
```

### Task 4: Add Static-Member Routing for Member and Change Commands

**Files:**
- Modify: `microcap_top100_mom16_biweekly_live.py:1042-1211`
- Modify: `microcap_top100_mom16_biweekly_live.py:1355-1459`
- Modify: `microcap_top100_mom16_biweekly_live.py:2573-2778`
- Test: `test_mainline_query_routing.py`

- [ ] **Step 1: Write failing tests for static member cache routing**

```python
    def test_members_query_builds_static_members_after_base_signal_context(self) -> None:
        args = self.make_args()
        paths = base_mod.build_output_paths(args.output_prefix)
        base_context = {
            "paths": paths,
            "close_df": pd.DataFrame(index=pd.to_datetime(["2026-04-09", "2026-04-10"])),
            "latest_signal": pd.DataFrame([{"signal_label": "long", "trade_state": "hold"}]),
            "latest_rebalance": pd.Timestamp("2026-04-10"),
            "prev_rebalance": pd.Timestamp("2026-03-27"),
            "effective_rebalance": pd.Timestamp("2026-04-10"),
            "rebalance_effective_date": pd.Timestamp("2026-04-11"),
            "anchor_freshness": {
                "status": "fresh",
                "latest_trade_date": "2026-04-10",
                "current_date": "2026-04-12",
                "stale_calendar_days": 2,
            },
        }
        member_context = dict(base_context)
        member_context["target_members"] = pd.DataFrame([{"rank": 1, "symbol": "000001", "name": "平安银行"}])
        member_context["changes_df"] = pd.DataFrame([{"action": "enter", "symbol": "000001", "name": "平安银行"}])

        with patch.object(base_mod, "refresh_history_anchor", return_value=(paths["panel_shadow"], pd.Timestamp("2026-04-10"))):
            with patch.object(base_mod, "ensure_base_signal_fresh", return_value=base_context):
                with patch.object(base_mod, "ensure_static_members_fresh", return_value=member_context) as ensure_members_mock:
                    with patch.object(base_mod, "handle_query") as handle_query_mock:
                        base_mod.execute_query(args, "成分股名单")

        ensure_members_mock.assert_called_once()
        handle_query_mock.assert_called_once_with(member_context, args, "成分股名单")
```

- [ ] **Step 2: Run the focused members-routing test and verify it fails**

Run: `python -m unittest test_mainline_query_routing.MainlineQueryRoutingTests.test_members_query_builds_static_members_after_base_signal_context -v`

Expected: FAIL because `ensure_static_members_fresh()` does not exist yet.

- [ ] **Step 3: Implement `ensure_static_members_fresh()` by reusing the existing member-cache flow**

```python
def ensure_static_members_fresh(
    args: argparse.Namespace,
    paths: dict[str, Path],
    panel_path: Path,
    target_end_date: pd.Timestamp,
    base_context: dict[str, object],
) -> dict[str, object]:
    context = dict(base_context)
    latest_rebalance = pd.Timestamp(context["latest_rebalance"])
    prev_rebalance = context.get("prev_rebalance")
    effective_rebalance = context.get("effective_rebalance")
    rebalance_effective_date = context.get("rebalance_effective_date")

    cached_static = load_cached_static_context(
        paths=paths,
        latest_rebalance=latest_rebalance,
        prev_rebalance=prev_rebalance,
        effective_rebalance=effective_rebalance,
        rebalance_effective_date=rebalance_effective_date,
        capital=args.capital,
    )
    if cached_static is None:
        snapshot_dates = [dt for dt in [latest_rebalance, prev_rebalance, effective_rebalance] if dt is not None]
        snapshots = load_member_snapshot(snapshot_dates=snapshot_dates, max_workers=args.max_workers)
        target_members = snapshots[pd.Timestamp(latest_rebalance)].copy()
        prev_members = snapshots.get(pd.Timestamp(prev_rebalance)) if prev_rebalance is not None else None
        effective_members = snapshots.get(pd.Timestamp(effective_rebalance)) if effective_rebalance is not None else target_members.copy()
        target_members = add_capital_columns(target_members, capital=args.capital)
        changes_df = build_change_table(prev_members, target_members)
        save_static_context_cache(
            paths=paths,
            latest_rebalance=latest_rebalance,
            prev_rebalance=prev_rebalance,
            effective_rebalance=effective_rebalance,
            rebalance_effective_date=rebalance_effective_date,
            target_members=target_members.drop(columns=["target_notional"], errors="ignore"),
            effective_members=effective_members,
            changes_df=changes_df,
        )
    else:
        target_members, effective_members, changes_df = cached_static

    context["target_members"] = target_members
    context["effective_members"] = effective_members
    context["changes_df"] = changes_df
    return context
```

- [ ] **Step 4: Update `execute_query()` so only member-oriented commands call the static-members helper**

```python
    if kind in {"members", "changes", "realtime_changes"}:
        base_context = ensure_base_signal_fresh(args, paths, panel_path, target_end_date)
        member_context = ensure_static_members_fresh(args, paths, panel_path, target_end_date, base_context)
        handle_query(member_context, args, query)
        return
```

- [ ] **Step 5: Run the routing tests and the existing refresh consistency suite**

Run: `python -m unittest test_mainline_query_routing.py test_mainline_refresh_consistency.py -v`

Expected: PASS

- [ ] **Step 6: Commit the static-member routing**

```bash
git add test_mainline_query_routing.py test_mainline_refresh_consistency.py microcap_top100_mom16_biweekly_live.py
git commit -m "feat: route member queries through static cache helpers"
```

### Task 5: Route Realtime Commands Without Reintroducing Full Base Context

**Files:**
- Modify: `microcap_top100_mom16_biweekly_live.py:1501-1556`
- Modify: `microcap_top100_mom16_biweekly_live.py:2282-2550`
- Modify: `microcap_top100_mom16_biweekly_live.py:2573-2778`
- Test: `test_mainline_query_routing.py`

- [ ] **Step 1: Add failing tests for realtime command routing**

```python
    def test_realtime_signal_query_uses_base_signal_context(self) -> None:
        args = self.make_args()
        paths = base_mod.build_output_paths(args.output_prefix)
        base_context = {
            "paths": paths,
            "close_df": pd.DataFrame(index=pd.to_datetime(["2026-04-09", "2026-04-10"])),
            "latest_signal": pd.DataFrame([{"signal_label": "long", "trade_state": "hold"}]),
            "anchor_freshness": {
                "status": "fresh",
                "latest_trade_date": "2026-04-10",
                "current_date": "2026-04-12",
                "stale_calendar_days": 2,
                "is_stale": False,
            },
        }

        with patch.object(base_mod, "refresh_history_anchor", return_value=(paths["panel_shadow"], pd.Timestamp("2026-04-10"))):
            with patch.object(base_mod, "ensure_base_signal_fresh", return_value=base_context):
                with patch.object(base_mod, "handle_query") as handle_query_mock:
                    base_mod.execute_query(args, "实时信号")

        handle_query_mock.assert_called_once_with(base_context, args, "实时信号")
```

- [ ] **Step 2: Run the realtime-routing test and verify it fails if the router still builds too much**

Run: `python -m unittest test_mainline_query_routing.MainlineQueryRoutingTests.test_realtime_signal_query_uses_base_signal_context -v`

Expected: FAIL until realtime commands are explicitly routed.

- [ ] **Step 3: Update `execute_query()` to route realtime commands through the leanest valid context**

```python
    if kind == "realtime_signal":
        base_context = ensure_base_signal_fresh(args, paths, panel_path, target_end_date)
        handle_query(base_context, args, query)
        return

    if kind == "realtime_changes":
        base_context = ensure_base_signal_fresh(args, paths, panel_path, target_end_date)
        member_context = ensure_static_members_fresh(args, paths, panel_path, target_end_date, base_context)
        handle_query(member_context, args, query)
        return
```

- [ ] **Step 4: Run the targeted routing suite and then the full relevant regression set**

Run: `python -m unittest test_mainline_query_routing.py test_mainline_refresh_consistency.py test_top100_v1_0_runtime_bootstrap.py -v`

Expected: PASS

- [ ] **Step 5: Commit the realtime routing changes**

```bash
git add test_mainline_query_routing.py microcap_top100_mom16_biweekly_live.py
git commit -m "feat: route realtime queries through lean refresh paths"
```

### Task 6: Final Verification and Manual Command Smoke Tests

**Files:**
- Modify: `microcap_top100_mom16_biweekly_live.py`
- Test: `test_mainline_query_routing.py`
- Test: `test_mainline_refresh_consistency.py`
- Test: `test_top100_v1_0_runtime_bootstrap.py`

- [ ] **Step 1: Run the full automated verification set**

Run: `python -m unittest test_mainline_query_routing.py test_mainline_refresh_consistency.py test_top100_v1_0_runtime_bootstrap.py -v`

Expected: PASS

- [ ] **Step 2: Run a manual smoke test for lean performance routing**

Run: `python .\microcap_top100_mom16_biweekly_live.py 净值表现 最近一年`

Expected:
- Produces latest refreshed `performance_summary.csv`, `performance_yearly.csv`, `performance_nav.csv`, `performance_curve.png`
- Does not rebuild static member snapshots

- [ ] **Step 3: Run a manual smoke test for lean closed-signal routing**

Run: `python .\microcap_top100_mom16_biweekly_live.py 信号`

Expected:
- Returns latest closed signal
- Does not trigger static member snapshot work

- [ ] **Step 4: Run manual smoke tests for member and realtime commands**

Run:

```bash
python .\microcap_top100_mom16_biweekly_live.py 成分股名单
python .\microcap_top100_mom16_biweekly_live.py 进出名单
python .\microcap_top100_mom16_biweekly_live.py 实时信号
python .\microcap_top100_mom16_biweekly_live.py 实时进出名单
```

Expected:
- Each command succeeds using the latest anchor
- Static commands only build static member artifacts
- Realtime commands reuse valid short-window realtime cache when available

- [ ] **Step 5: Commit the verified implementation**

```bash
git add microcap_top100_mom16_biweekly_live.py test_mainline_query_routing.py test_mainline_refresh_consistency.py
git commit -m "feat: split mainline query routing by artifact dependency"
```
