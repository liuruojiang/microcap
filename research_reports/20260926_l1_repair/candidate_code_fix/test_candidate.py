"""Exercise patched pure functions extracted from both strategy entrypoints."""

from __future__ import annotations

import ast
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
import re
from types import SimpleNamespace
import unittest

import pandas as pd

from build_patch import FILES, transformed


def embedded_source(source: str, name: str) -> str:
    tree = ast.parse(source)
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == name for t in node.targets):
            return ast.literal_eval(node.value)
    raise AssertionError(f"missing embedded source {name}")


def functions(source: str, names: tuple[str, ...], namespace: dict) -> SimpleNamespace:
    tree = ast.parse(source)
    wanted = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names]
    assert {node.name for node in wanted} == set(names)
    code = compile(ast.Module(body=wanted, type_ignores=[]), "<candidate-function-extract>", "exec")
    exec(code, namespace)
    return SimpleNamespace(**{name: namespace[name] for name in names})


def frequency(source: str) -> SimpleNamespace:
    namespace = {
        "re": re, "pd": pd, "Decimal": Decimal, "ROUND_HALF_UP": ROUND_HALF_UP,
        "CHINEXT_LIMIT_SWITCH": pd.Timestamp("2020-08-24"), "LIMIT_PRICE_REL_EPS": 1e-6,
    }
    return functions(source, ("is_st_name", "round_limit_price", "get_price_limit_ratio", "is_price_at_limit", "detect_close_limit_blocks"), namespace)


def live(source: str, freq: SimpleNamespace) -> SimpleNamespace:
    # This gate must also work before frequency dependencies are initialized.
    namespace = {"re": re, "freq_mod": None, "NON_TRADABLE_NAME_PATTERN": re.compile(r"(退$|退市|摘牌)")}
    return functions(source, ("is_tradable_name", "is_live_tradable_name"), namespace)


class CandidateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.frequency_sources = []
        cls.live_sources = []
        for path in FILES:
            original, candidate = transformed(path)
            if path == "analyze_top100_rebalance_frequency.py":
                cls.frequency_sources.append(("standalone", original, candidate))
            elif path == "microcap_top100_mom16_biweekly_live.py":
                cls.live_sources.append(("standalone", original, candidate))
            else:
                cls.frequency_sources.append(("embedded", embedded_source(original, "FREQ_SOURCE"), embedded_source(candidate, "FREQ_SOURCE")))
                cls.live_sources.append(("embedded", embedded_source(original, "BASE_SOURCE"), embedded_source(candidate, "BASE_SOURCE")))

    def test_st_name_and_live_gate_both_entrypoints(self) -> None:
        risky = ("S ST华新", "S*ST华新", "G ST三木", "G*ST三木", "S PT甲", "ST测试", "*ST测试", "PT测试", "*ST TCL", "S ST TCL")
        clean = ("中国平安", "G三木", "S证券", "G STAR科技", "STOCK科技", "PTA科技", "")
        for (_, old_f, new_f), (_, old_b, new_b) in zip(self.frequency_sources, self.live_sources):
            before = frequency(old_f)
            after = frequency(new_f)
            live_after = live(new_b, after)
            self.assertFalse(before.is_st_name("S ST华新"))
            for name in risky:
                with self.subTest(name=name):
                    self.assertTrue(after.is_st_name(name))
                    self.assertFalse(live_after.is_live_tradable_name(name))
            for name in clean:
                self.assertFalse(after.is_st_name(name))
            self.assertTrue(live_after.is_live_tradable_name("中国平安"))
            self.assertFalse(live_after.is_live_tradable_name("某某退市"))

    def test_decimal_half_up_000632_counterexample(self) -> None:
        for _, old_f, new_f in self.frequency_sources:
            before = frequency(old_f)
            after = frequency(new_f)
            self.assertEqual(after.round_limit_price(Decimal("4.10") * Decimal("0.95")), 3.90)
            self.assertFalse(before.is_price_at_limit(3.90, 4.10, 0.05, -1))
            self.assertTrue(after.is_price_at_limit(3.90, 4.10, 0.05, -1))
            self.assertFalse(after.is_price_at_limit(3.89, 4.10, 0.05, -1))
            self.assertTrue(after.is_price_at_limit(4.51, 4.10, 0.10, 1))
            self.assertEqual(before.detect_close_limit_blocks("000632", pd.Timestamp("2026-04-30"), 4.10, 3.90, is_st=True), (False, False))
            self.assertEqual(after.detect_close_limit_blocks("000632", pd.Timestamp("2026-04-30"), 4.10, 3.90, is_st=True), (False, True))

    def test_sh_sz_mainboard_rule_switch_and_other_boards(self) -> None:
        for _, old_f, new_f in self.frequency_sources:
            before = frequency(old_f)
            after = frequency(new_f)
            for symbol in ("000632", "002883", "600080", "601001", "603000", "605000"):
                self.assertEqual(after.get_price_limit_ratio(symbol, pd.Timestamp("2026-07-03"), True), 0.05)
                self.assertEqual(after.get_price_limit_ratio(symbol, pd.Timestamp("2026-07-06"), True), 0.10)
                self.assertEqual(after.get_price_limit_ratio(symbol, pd.Timestamp("2026-09-17"), True), 0.10)
                self.assertEqual(after.get_price_limit_ratio(symbol, pd.Timestamp("2026-07-06"), False), 0.10)
                self.assertEqual(before.get_price_limit_ratio(symbol, pd.Timestamp("2026-07-06"), True), 0.05)
            self.assertEqual(after.get_price_limit_ratio("300001", pd.Timestamp("2019-01-01"), True), 0.05)
            self.assertEqual(after.get_price_limit_ratio("300001", pd.Timestamp("2026-07-06"), True), 0.20)
            self.assertEqual(after.get_price_limit_ratio("688001", pd.Timestamp("2026-07-06"), True), 0.20)
            self.assertEqual(after.get_price_limit_ratio("830001", pd.Timestamp("2026-07-06"), True), 0.30)

    def test_standalone_and_embedded_changed_cases(self) -> None:
        f1 = frequency(self.frequency_sources[0][2])
        f2 = frequency(self.frequency_sources[1][2])
        for name in ("S ST华新", "G*ST三木", "普通证券"):
            self.assertEqual(f1.is_st_name(name), f2.is_st_name(name))
        for symbol in ("000632", "600080", "300001", "688001", "830001"):
            for date in ("2026-07-03", "2026-07-06"):
                self.assertEqual(f1.get_price_limit_ratio(symbol, pd.Timestamp(date), True), f2.get_price_limit_ratio(symbol, pd.Timestamp(date), True))
        self.assertEqual(f1.is_price_at_limit(3.90, 4.10, 0.05, -1), f2.is_price_at_limit(3.90, 4.10, 0.05, -1))


if __name__ == "__main__":
    unittest.main(verbosity=2)
