"""Category-state contract; executes exact standalone and embedded source AST.

The tests do not import the strategy entrypoint or modify approved metadata.
Synthetic counterexamples are explicit. The separate research report binds the
600241 title fixture to the real archived notices and actual metadata hashes.
"""
from __future__ import annotations

import ast
from pathlib import Path
import re

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
ENTRY_D = "关于公司股票被实施退市风险警示的公告"
ENTRY_O = "关于公司股票被实施其他风险警示的公告"
EXIT_D = "关于撤销退市风险警示的公告"
EXIT_O = "关于撤销其他风险警示的公告"
MIXED = "关于撤销退市风险警示并实施其他风险警示的公告"


@pytest.fixture(params=["standalone", "embedded"])
def funcs(request):
    filename = "analyze_top100_rebalance_frequency.py" if request.param == "standalone" else "microcap_top100_mom16_biweekly_live_v2_0.py"
    source = (ROOT / filename).read_text(encoding="utf-8-sig")
    tree = ast.parse(source)
    if request.param == "embedded":
        assignment = next(n for n in tree.body if isinstance(n, ast.Assign)
                          and any(isinstance(t, ast.Name) and t.id == "FREQ_SOURCE" for t in n.targets))
        tree = ast.parse(ast.literal_eval(assignment.value))
    required = {"_parse_st_notice_risk_transition", "classify_st_notice_risk_actions", "build_st_intervals_from_notices", "merge_st_intervals", "build_st_status_series"}
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in required]
    assert {n.name for n in nodes} == required
    ns = {"pd": pd, "re": re}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), filename, "exec"), ns)
    return ns


def intervals(funcs, events, first="2010-01-04", last="2026-10-23"):
    return funcs["build_st_intervals_from_notices"](pd.Timestamp(first), pd.Timestamp(last),
        pd.DataFrame(events, columns=["notice_date", "title"]))


def spans(result):
    return [(r["start"], r["end"]) for r in result]


@pytest.mark.parametrize("transition", [MIXED, "关于撤销退市风险警示并继续实施其他风险警示的公告",
    "关于撤销退市风险警示和实施其他风险警示的公告", "关于撤销退市风险警示后实施其他风险警示的公告",
    "关于撤销退市风险警示并转为其他风险警示的公告"])
def test_starST_to_ST_is_continuous(funcs, transition):
    result = intervals(funcs, [("2024-04-30", ENTRY_D), ("2025-05-20", transition), ("2026-05-20", EXIT_O)])
    assert spans(result) == [("2024-04-30", "2026-05-20")]


def test_ST_to_starST_is_continuous(funcs):
    result = intervals(funcs, [("2024-04-30", ENTRY_O),
        ("2025-05-20", "关于撤销其他风险警示并实施退市风险警示的公告"), ("2026-05-20", EXIT_D)])
    assert spans(result) == [("2024-04-30", "2026-05-20")]


@pytest.mark.parametrize("withdrawal", ["关于撤销退市风险警示及其他风险警示的公告", "关于撤销特别处理的公告", "关于股票摘帽的公告"])
def test_complete_withdrawal_closes_all_categories(funcs, withdrawal):
    result = intervals(funcs, [("2024-04-30", "关于实施退市风险警示及其他风险警示的公告"), ("2025-05-20", withdrawal)])
    assert spans(result) == [("2024-04-30", "2025-05-20")]


def test_withdrawing_one_category_keeps_other_active(funcs):
    result = intervals(funcs, [("2024-04-30", "关于实施退市风险警示及其他风险警示的公告"), ("2025-05-20", EXIT_D)])
    assert spans(result) == [("2024-04-30", None)]


def test_partial_withdrawal_cannot_clear_category(funcs):
    result = intervals(funcs, [("2024-04-30", ENTRY_O), ("2025-05-20", "关于撤销部分其他风险警示的公告")])
    assert spans(result) == [("2024-04-30", None)]


def test_same_category_reason_transition_retains_warning(funcs):
    title = "关于公司股票交易撤销因重整而被实施的退市风险警示以及继续实施财务类退市风险警示及其他风险警示的公告"
    result = intervals(funcs, [("2024-04-30", ENTRY_D), ("2025-05-20", title), ("2026-05-20", EXIT_O)])
    assert spans(result) == [("2024-04-30", None)]


@pytest.mark.parametrize("second_date", ["2016-05-20", "2016-05-23"])
@pytest.mark.parametrize("reverse", [False, True])
def test_repeated_exit_never_refills_known_nonST_history(funcs, second_date, reverse):
    events = [("2014-12-13", ENTRY_D), ("2016-05-20", EXIT_D), (second_date, "关于撤销退市风险警示的提示性公告")]
    if reverse:
        events.reverse()
    assert spans(intervals(funcs, events)) == [("2014-12-13", "2016-05-20")]


@pytest.mark.parametrize("reverse", [False, True])
def test_same_day_conflicting_titles_fail_closed_independent_of_order(funcs, reverse):
    events = [("2014-12-13", ENTRY_D), ("2016-05-20", ENTRY_D), ("2016-05-20", EXIT_D)]
    if reverse:
        events.reverse()
    with pytest.raises(ValueError, match="Unresolved conflicting ST notice evidence"):
        intervals(funcs, events)


@pytest.mark.parametrize("reverse", [False, True])
def test_same_day_distinct_category_transition_order_independent(funcs, reverse):
    events = [("2014-12-13", ENTRY_D), ("2016-05-20", ENTRY_O), ("2016-05-20", EXIT_D)]
    if reverse:
        events.reverse()
    assert spans(intervals(funcs, events)) == [("2014-12-13", None)]


def test_initial_unknown_exit_retains_conservative_inference_once(funcs):
    assert spans(intervals(funcs, [("2016-05-20", EXIT_D), ("2016-05-23", EXIT_D)])) == [("2010-01-04", "2016-05-20")]


def test_initial_unknown_mixed_transition_also_infers_prior_warning(funcs):
    assert spans(intervals(funcs, [("2016-05-20", MIXED)])) == [("2010-01-04", None)]


def test_initial_partial_withdrawal_proves_prior_warning(funcs):
    title = "关于撤销部分其他风险警示的公告"
    assert funcs["classify_st_notice_risk_actions"](title) == {"other": True}
    assert spans(intervals(funcs, [("2016-05-20", title)])) == [("2010-01-04", None)]


def test_known_nonST_partial_withdrawal_does_not_refill_history(funcs):
    assert spans(intervals(funcs, [("2009-12-20", EXIT_O),
        ("2016-05-20", "关于撤销部分其他风险警示的公告")])) == [("2016-05-20", None)]


@pytest.mark.parametrize("title", ["关于可能撤销部分其他风险警示并实施退市风险警示的公告",
    "关于申请撤销部分其他风险警示的公告", "关于可能撤销部分其他风险警示的公告"])
def test_ineffective_partial_withdrawal_does_not_prove_prior_warning(funcs, title):
    expected = [("2016-05-20", None)] if "并实施" in title else []
    assert spans(intervals(funcs, [("2016-05-20", title)])) == expected


def test_known_nonST_from_pre_sample_exit_prevents_backfill(funcs):
    assert intervals(funcs, [("2009-12-20", EXIT_D), ("2016-05-23", EXIT_D)]) == []


def test_pre_sample_mixed_transition_retains_active_state(funcs):
    assert spans(intervals(funcs, [("2008-12-20", ENTRY_D), ("2009-12-20", MIXED)])) == [("2010-01-04", None)]


def test_entry_exit_reentry_keeps_normal_gap(funcs):
    result = intervals(funcs, [("2014-12-13", ENTRY_D), ("2016-05-20", EXIT_D), ("2017-01-03", ENTRY_O)])
    assert spans(result) == [("2014-12-13", "2016-05-20"), ("2017-01-03", None)]


@pytest.mark.parametrize("title", ["关于申请撤销退市风险警示并实施其他风险警示的公告",
    "关于可能撤销退市风险警示的公告", "关于公司股票可能被实施退市风险警示的公告",
    "关于公司股票存在被实施退市风险警示的公告", "关于拟实施退市风险警示的公告"])
def test_requests_potential_actions_do_not_take_effect(funcs, title):
    assert intervals(funcs, [("2024-04-30", title)]) == []
    assert spans(intervals(funcs, [("2024-04-29", ENTRY_D), ("2024-04-30", title)])) == [("2024-04-29", None)]


def test_unknown_generic_subtype_not_cleared_by_specific_exit(funcs):
    result = intervals(funcs, [("2014-12-13", "关于实施特别处理的公告"), ("2016-05-20", EXIT_D)])
    assert spans(result) == [("2014-12-13", None)]


def test_legacy_delisting_special_treatment_is_one_category(funcs):
    result = intervals(funcs, [("2014-12-13", "关于股票交易实行退市风险警示特别处理的公告"), ("2016-05-20", EXIT_D)])
    assert spans(result) == [("2014-12-13", "2016-05-20")]


def test_notice_date_policy_and_inclusive_exit_are_preserved(funcs):
    result = intervals(funcs, [("2024-04-30", ENTRY_D), ("2025-05-20", EXIT_D)])
    dates = pd.DatetimeIndex(["2024-04-29", "2024-04-30", "2025-05-20", "2025-05-21"])
    assert funcs["build_st_status_series"]({"st_intervals": result}, dates).tolist() == [False, True, True, False]


def test_real_600241_archived_title_chain_is_continuous(funcs):
    # Real recorded announcement dates/titles, not fabricated returns or prices.
    events = [("2020-04-24", "关于公司股票实施退市风险警示暨临时停牌的公告"),
              ("2021-04-28", "辽宁时代万恒股份有限公司关于申请撤销公司股票退市风险警示并实施其他风险警示的公告"),
              ("2021-05-15", "辽宁时代万恒股份有限公司关于公司股票撤销退市风险警示并实施其他风险警示暨临时停牌的公告"),
              ("2022-04-22", "辽宁时代万恒股份有限公司关于公司股票继续被实施其他风险警示的公告"),
              ("2023-05-30", "辽宁时代万恒股份有限公司关于撤销其他风险警示暨停牌的公告")]
    assert spans(intervals(funcs, events)) == [("2020-04-24", "2023-05-30")]
