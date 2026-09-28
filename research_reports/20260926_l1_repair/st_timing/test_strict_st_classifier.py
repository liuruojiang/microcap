"""Real CNInfo title counterexamples for the L1 diagnostic parser."""

from strict_st_classifier import build_intervals, classify_title, status_on_target


def test_progress_toward_removal_is_not_removal():
    assert classify_title("关于为撤销退市风险警示采取措施及有关工作进展情况的公告") == "context"
    assert classify_title("关于公司股票交易实行退市风险警示的公告") == "entry"
    assert classify_title("关于实施退市<em>风险</em><em>警示</em>暨停牌的公告") == "entry"
    assert classify_title("关于撤销公司退市风险警示的提示性公告") == "needs_review"


def test_mixed_warning_change_does_not_end_st():
    assert classify_title("关于撤销退市风险警示并实施其他特别处理的公告") == "remain_st"
    assert classify_title("关于公司股票可能被实施退市风险警示的提示性公告") == "context"
    assert classify_title("关于公司债券可能被实行风险警示的提示性公告") == "context"
    assert classify_title("关于公司股票实施退市风险警示和公司债券可能暂停交易的公告") == "entry"


def test_unknown_initial_state_is_not_backfilled_from_removal_title():
    notices = [{
        "notice_time_shanghai": "2012-08-31T06:32:00+08:00",
        "title": "关于股票交易撤销其他风险警示的公告",
    }]
    intervals = build_intervals("2010-01-04", notices)
    assert intervals == []


def test_removal_of_one_warning_censors_title_interval():
    assert classify_title("撤销退市风险警示公告") == "needs_review"
    assert classify_title("关于公司撤销退市风险警示的公告") == "needs_review"
    notices = [
        {"notice_time_shanghai": "2007-04-16", "title": "关于股票交易实行退市风险警示特别处理的公告"},
        {"notice_time_shanghai": "2008-06-05", "title": "撤销退市风险警示公告"},
    ]
    assert build_intervals("2007-01-01", notices) == [{
        "start_notice_date": "2007-04-16", "end_notice_date": "2008-06-05", "basis": "explicit_entry",
    }]


def test_explicit_entry_interior_vs_notice_day():
    notices = [{
        "notice_time_shanghai": "2026-04-30T00:00:00+08:00",
        "title": "标准股份关于实施其他风险警示暨停牌的公告",
    }]
    interval = build_intervals("2010-01-04", notices)[0]
    assert status_on_target("2026-04-30", interval) == "boundary_needs_pdf"
    assert status_on_target("2026-05-14", interval) == "explicit_entry_interior"
