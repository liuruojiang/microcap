import pytest
import analyze_top100_rebalance_frequency as frequency

@pytest.mark.parametrize(('title','expected'),[
 ('关于争取撤销风险警示所采取的措施及有关工作进展情况的公告',{}),
 ('关于存在被证券交易所实施其他风险警示的风险提示公告',{}),
 ('关于股票交易存在被实行退市风险警示的风险提示性公告',{}),
 ('关于公司股票交易免于被实施退市风险警示的公告',{}),
 ('关于不能撤销股票交易其他风险警示的公告',{}),
 ('关于撤消公司股票交易退市风险警示的公告',{'delisting':False}),
 ('关于公司股票撤销退市风险警示继续实施其他风险警示暨停牌的公告',{'delisting':False,'other':True}),
 ('关于公司股票及可转换公司债券进入退市整理期交易的第二次风险提示公告',{}),
])
def test_actual_discovered_title_semantics(title,expected):
 assert frequency.classify_st_notice_risk_actions(title)==expected
