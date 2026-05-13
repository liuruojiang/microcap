from __future__ import annotations

import ast
from pathlib import Path

import pandas as pd

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
