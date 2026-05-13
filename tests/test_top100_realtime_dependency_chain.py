from __future__ import annotations

import ast
from pathlib import Path

import pandas as pd
import pytest

import top100_realtime_core as realtime_core


ROOT = Path(__file__).resolve().parents[1]


def _function_tree(path: str, function_name: str) -> ast.FunctionDef:
    module = ast.parse((ROOT / path).read_text(encoding="utf-8-sig"))
    for node in module.body:
        if isinstance(node, ast.FunctionDef) and node.name == function_name:
            return node
    raise AssertionError(f"missing function {function_name} in {path}")


def _attribute_path(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        prefix = _attribute_path(node.value)
        return f"{prefix}.{node.attr}" if prefix else node.attr
    return None


def _referenced_paths(function: ast.FunctionDef) -> set[str]:
    paths: set[str] = set()
    for node in ast.walk(function):
        path = _attribute_path(node)
        if path:
            paths.add(path)
    return paths


def test_v1_6_realtime_does_not_call_v1_4_realtime_builder() -> None:
    function = _function_tree("microcap_top100_mom16_biweekly_live_v1_6.py", "build_realtime_v1_6_outputs")
    paths = _referenced_paths(function)

    assert "realtime_core.load_realtime_base" in paths
    assert "v14_context.build_realtime_v1_4_outputs" not in paths
    assert not any(path.startswith("v14_context.v1_1_mod.base_mod") for path in paths)


def test_v1_8_realtime_uses_shared_realtime_core_boundary() -> None:
    function = _function_tree("microcap_top100_mom16_biweekly_live_v1_8.py", "build_realtime_v1_8_outputs")
    paths = _referenced_paths(function)

    assert "realtime_core.load_realtime_base" in paths
    assert not any(path.startswith("v14_context.v1_1_mod.base_mod") for path in paths)


def test_realtime_meta_values_are_csv_safe_for_one_row_signal() -> None:
    signal_row = pd.DataFrame({"strategy_version": ["vX"]})

    realtime_core.apply_realtime_meta_to_signal_row(
        signal_row,
        {
            "member_quote_bad_symbols": [],
            "snapshot_time": pd.Timestamp("2026-05-13 11:30:00+08:00"),
        },
    )

    assert signal_row.loc[0, "member_quote_bad_symbols"] == "[]"
    assert signal_row.loc[0, "snapshot_time"] == "2026-05-13T11:30:00+08:00"


def test_production_realtime_requires_validated_state_before_rebuild(monkeypatch: pytest.MonkeyPatch) -> None:
    paths = {
        "proxy_meta": Path("missing_meta.json"),
        "proxy_members": Path("missing_members.csv"),
        "proxy_turnover": Path("missing_turnover.csv"),
    }
    monkeypatch.setenv(realtime_core.REQUIRE_STATE_ENV, "1")
    monkeypatch.setattr(realtime_core.base_mod, "build_output_paths", lambda _prefix: paths)
    monkeypatch.setattr(realtime_core.v1_1_mod, "prepare_current_v1_1_outputs", lambda **_kwargs: None)
    monkeypatch.setattr(realtime_core, "_missing_base_state", lambda _paths: [Path("missing_turnover.csv")])

    def fail_rebuild(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("production mode must not rebuild missing realtime state")

    monkeypatch.setattr(realtime_core.base_mod, "build_refreshed_panel_shadow", fail_rebuild)
    monkeypatch.setattr(realtime_core.base_mod, "ensure_strategy_files", fail_rebuild)

    with pytest.raises(FileNotFoundError, match="refusing implicit rebuild"):
        realtime_core.ensure_base_outputs()


def test_production_realtime_context_uses_cached_proxy_boundary(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    paths = {
        "proxy_meta": Path("meta.json"),
        "proxy_members": Path("members.csv"),
        "proxy_turnover": Path("turnover.csv"),
    }
    cached_context = {"close_df": pd.DataFrame({"microcap": [1.0], "hedge": [1.0]}, index=[pd.Timestamp("2026-05-11")])}

    monkeypatch.setenv(realtime_core.REQUIRE_STATE_ENV, "1")
    monkeypatch.setattr(realtime_core, "ensure_base_outputs", lambda: None)
    monkeypatch.setattr(realtime_core, "load_reference_summary", lambda: {"summary": "ok"})
    monkeypatch.setattr(realtime_core.base_mod, "build_output_paths", lambda _prefix: paths)
    monkeypatch.setattr(
        realtime_core.base_mod,
        "refresh_history_anchor",
        lambda *_args: (Path("panel.csv"), pd.Timestamp("2026-05-13")),
    )

    def cached_proxy(*_args: object, **_kwargs: object) -> dict[str, object]:
        calls.append("cached_proxy")
        return cached_context

    def fail_fresh(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("production mode must not call fresh/rebuild context path")

    monkeypatch.setattr(realtime_core.base_mod, "build_realtime_context_from_cached_proxy", cached_proxy)
    monkeypatch.setattr(realtime_core.base_mod, "ensure_realtime_query_base_context", fail_fresh)
    monkeypatch.setattr(realtime_core.base_mod, "ensure_base_signal_fresh", fail_fresh)
    monkeypatch.setattr(realtime_core.base_mod, "ensure_static_members_fresh", lambda *_args: cached_context)
    monkeypatch.setattr(pd, "read_csv", lambda *_args, **_kwargs: pd.DataFrame({"rebalance_date": ["2026-05-07"]}))

    context, _turnover, summary = realtime_core.load_realtime_context()

    assert calls == ["cached_proxy"]
    assert context is cached_context
    assert summary == {"summary": "ok"}


def test_production_realtime_skips_price_cache_refresh(monkeypatch: pytest.MonkeyPatch) -> None:
    base_mod = realtime_core.base_mod
    monkeypatch.setenv(realtime_core.REQUIRE_STATE_ENV, "1")
    monkeypatch.setattr(base_mod, "load_latest_close_snapshot_map", lambda *_args, **_kwargs: {})

    def fail_refresh(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("production mode must not refresh price caches during realtime send")

    monkeypatch.setattr(base_mod, "refresh_price_cache_tail", fail_refresh)

    assert base_mod.ensure_realtime_last_close_map(["000001"], as_of_date=pd.Timestamp("2026-05-11")) == {}
