"""Explicit B-share exclusion without changing existing other-market rules."""
import ast
from pathlib import Path

import pandas as pd
import pytest

ROOT=Path(__file__).resolve().parents[1]


def load_function(path, embedded):
    tree=ast.parse(path.read_text(encoding='utf-8-sig'))
    if embedded:
        assignment=next(n for n in tree.body if isinstance(n,ast.Assign)
                        and any(isinstance(t,ast.Name) and t.id=='FREQ_SOURCE' for t in n.targets))
        tree=ast.parse(ast.literal_eval(assignment.value))
    selected=[n for n in tree.body if isinstance(n,ast.FunctionDef)
              and n.name in {'list_backtest_universe_symbols','_infer_exchange_board'}]
    ns={'pd':pd,'PRICE_DIR':'prices','SHARED_PRICE_DIR':None,'SHARE_DIR':'shares','SHARED_SHARE_DIR':None}
    exec(compile(ast.Module(body=selected,type_ignores=[]),str(path),'exec'),ns)
    return ns


@pytest.fixture(params=['standalone','embedded'])
def funcs(request):
    embedded=request.param=='embedded'
    path=ROOT/('microcap_top100_mom16_biweekly_live_v2_0.py' if embedded else 'analyze_top100_rebalance_frequency.py')
    return load_function(path,embedded)


@pytest.mark.parametrize('master_kind',['present','empty','missing_symbol_column'])
def test_b_shares_with_both_caches_never_enter_ashare_universe(funcs,master_kind):
    cached={'000001','300001','600000','688001','200001','200002','900901','900902'}
    funcs['_existing_symbols']=lambda *a:set(cached)
    master=pd.DataFrame({'symbol':sorted(cached),'exchange':['SZSE']*len(cached)})
    if master_kind=='empty': master=pd.DataFrame()
    if master_kind=='missing_symbol_column': master=pd.DataFrame({'name':['fixture']})
    funcs['load_security_master']=lambda:master
    assert funcs['list_backtest_universe_symbols']()==['000001','300001','600000','688001']


def test_existing_cache_intersection_and_unknown_prefix_rules_preserved(funcs):
    funcs['_existing_symbols']=lambda directory,*a: {'000001','600000','999999','200001','900901'} if directory=='prices' else {'000001','999999','200001','900901'}
    funcs['load_security_master']=lambda:pd.DataFrame({'symbol':['000001','600000','999999','200001','900901']})
    assert funcs['list_backtest_universe_symbols']()==['000001','999999']


def test_empty_master_fallback_does_not_add_bse_or_unknown_exclusions(funcs):
    cached={'833001','920001','999999','200001','900901'}
    funcs['_existing_symbols']=lambda *a:set(cached)
    funcs['load_security_master']=lambda:pd.DataFrame()
    assert funcs['list_backtest_universe_symbols']()==['833001','920001','999999']


