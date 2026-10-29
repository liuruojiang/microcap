"""Synthetic semantic counterexamples, not market evidence. Exact embedded AST."""
import ast
from pathlib import Path
import re
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]

@pytest.fixture(params=['standalone','embedded'])
def funcs(request):
    file = ROOT / ('microcap_top100_mom16_biweekly_live_v2_0.py' if request.param == 'embedded' else 'analyze_top100_rebalance_frequency.py')
    tree = ast.parse(file.read_text(encoding='utf-8-sig'))
    if request.param == 'embedded':
        node = next(n for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='FREQ_SOURCE' for t in n.targets))
        tree=ast.parse(ast.literal_eval(node.value))
    names={'_parse_st_notice_risk_transition','classify_st_notice_risk_actions','build_st_intervals_from_notices','merge_st_intervals','build_st_status_series'}
    ns={'pd':pd,'re':re}
    exec(compile(ast.Module(body=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in names],type_ignores=[]),str(file),'exec'),ns)
    return ns

@pytest.mark.parametrize('title',[
 '关于公司股票将被实施其他风险警示的提示性公告',
 '关于公司股票将于下月实施退市风险警示的公告',
 '关于回复撤销其他风险警示事项问询函的公告',
 '关于控股子公司股票被实施其他风险警示的公告',
 '关于2012年公司债券复牌暨实行风险警示公告',
 '关于可转债实施风险警示的公告',
])
def test_title_alone_cannot_prove_effective_issuer_event(funcs,title):
    # Ambiguous prospective/question/other-issuer title must not mutate issuer
    # state without authenticated body evidence. Empty or fail-closed is legal.
    try:
        actions=funcs['classify_st_notice_risk_actions'](title)
    except ValueError:
        return
    assert actions == {}

@pytest.mark.parametrize('title',[
 '关于申请撤销退市风险警示的公告',
 '关于公司股票可能被实施退市风险警示的公告',
 '关于拟撤销其他风险警示的公告',
])
def test_existing_ineffective_guard_controls(funcs,title):
    assert funcs['classify_st_notice_risk_actions'](title) == {}

def test_retained_other_and_duplicate_withdrawal(funcs):
    events=pd.DataFrame([
       ('2024-01-02','关于实施退市风险警示及其他风险警示的公告'),
       ('2024-02-01','关于撤销退市风险警示的公告'),
       ('2024-02-02','关于撤销退市风险警示的提示性公告'),
       ('2024-03-01','关于撤销其他风险警示的公告')],columns=['notice_date','title'])
    spans=funcs['build_st_intervals_from_notices'](pd.Timestamp('2020-01-02'),pd.Timestamp('2025-01-02'),events)
    assert [(r['start'],r['end']) for r in spans]==[('2024-01-02','2024-03-01')]

def test_notice_date_policy_inclusive_exit(funcs):
    spans=[{'start':'2024-01-02','end':'2024-03-01','source':'cninfo_notice'}]
    days=pd.DatetimeIndex(['2024-01-01','2024-01-02','2024-03-01','2024-03-04'])
    assert funcs['build_st_status_series']({'st_intervals':spans},days).tolist()==[False,True,True,False]
