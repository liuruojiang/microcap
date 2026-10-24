"""New synthetic release-audit cases; no metadata, dates or prices are market evidence."""
import pytest

from test_st_notice_state_machine_20261024 import (
    ENTRY_D, ENTRY_O, EXIT_D, funcs, intervals, spans,
)


@pytest.mark.parametrize("joiner", ["及", "以及", "暨", "和", "并"])
@pytest.mark.parametrize("entry,partial", [
    ("关于实施退市风险警示及其他风险警示的公告", "退市风险警示{joiner}部分其他风险警示"),
    ("关于实施退市风险警示及其他风险警示的公告", "其他风险警示{joiner}部分退市风险警示"),
])
def test_elided_withdrawal_keeps_partial_category_active(funcs, joiner, entry, partial):
    title = "关于撤销" + partial.format(joiner=joiner) + "的公告"
    actual = intervals(funcs, [("2024-01-02", entry), ("2024-02-01", title)])
    assert spans(actual) == [("2024-01-02", None)]


@pytest.mark.parametrize("title", [
    "关于拟撤销退市风险警示的公告", "关于拟撤销其他风险警示的公告",
    "关于拟撤销退市风险警示及其他风险警示的公告", "关于拟摘帽的公告",
])
def test_proposed_withdrawal_neither_closes_warning_nor_infers_unknown_history(funcs, title):
    assert funcs["classify_st_notice_risk_actions"](title) == {}
    assert intervals(funcs, [("2024-02-01", title)]) == []
    assert spans(intervals(funcs, [("2024-01-02", ENTRY_D), ("2024-02-01", title)])) == [("2024-01-02", None)]


def test_proposed_exit_does_not_suppress_separate_effective_other_entry(funcs):
    title = "关于拟撤销退市风险警示并实施其他风险警示的公告"
    assert funcs["classify_st_notice_risk_actions"](title) == {"other": True}
    assert spans(intervals(funcs, [("2024-02-01", title)])) == [("2024-02-01", None)]


def test_elided_partial_withdrawal_unknown_history_and_known_nonST(funcs):
    title = "关于撤销退市风险警示及部分其他风险警示的公告"
    assert spans(intervals(funcs, [("2024-02-01", title)])) == [("2010-01-04", None)]
    assert spans(intervals(funcs, [("2009-12-01", EXIT_D), ("2024-02-01", title)])) == [("2024-02-01", None)]


def test_full_elided_withdrawal_still_closes_all_categories(funcs):
    actual = intervals(funcs, [("2024-01-02", "关于实施退市风险警示及其他风险警示的公告"),
                              ("2024-02-01", "关于撤销退市风险警示及其他风险警示的公告")])
    assert spans(actual) == [("2024-01-02", "2024-02-01")]
