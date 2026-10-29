"""Real registry/day-state integration, plus isolated fail-closed builder faults."""
import ast
import json
from pathlib import Path
import re
from unittest.mock import Mock
from types import SimpleNamespace

import pandas as pd
import pytest

from scripts.st_notice_evidence import StNoticeEvidenceError, load_verified_events, evidence_fingerprint

ROOT=Path(__file__).resolve().parents[1]
EVENTS=json.loads((ROOT/'data/st_notice_evidence/registry.json').read_text(encoding='utf-8-sig'))['events']


def notices(symbol):
    return pd.DataFrame([{'announcement_id':e['announcement_id'],'issuer_symbol':e['symbol'],
                          'notice_date':e['notice_date'],'title':e['title'],'source_url':e['source_url']}
                         for e in EVENTS if e['symbol']==symbol])


@pytest.fixture(params=['standalone','embedded'])
def funcs(request):
    filename=ROOT/('analyze_top100_rebalance_frequency.py' if request.param=='standalone' else 'microcap_top100_mom16_biweekly_live_v2_0.py')
    tree=ast.parse(filename.read_text(encoding='utf-8-sig'))
    if request.param=='embedded':
        node=next(n for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='FREQ_SOURCE' for t in n.targets))
        tree=ast.parse(ast.literal_eval(node.value))
    names={'_parse_st_notice_risk_transition','classify_st_notice_risk_actions','build_st_intervals_from_notices','merge_st_intervals','build_st_status_series','fetch_cninfo_st_notices'}
    ns={'pd':pd,'re':re}
    exec(compile(ast.Module(body=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in names],type_ignores=[]),str(filename),'exec'),ns)
    return ns


@pytest.mark.parametrize('symbol',['600180','002193','002198','002514','000677'])
@pytest.mark.parametrize('reverse',[False,True])
def test_all_seven_real_events_produce_order_independent_day_states(funcs,symbol,reverse):
    rows=notices(symbol)
    if reverse: rows=rows.iloc[::-1]
    first,last=pd.Timestamp('2010-01-04'),pd.Timestamp('2026-10-29')
    verified=load_verified_events(ROOT,symbol,first,last,rows)
    result=funcs['build_st_intervals_from_notices'](first,last,rows,verified_events=verified)
    expected=[('2010-01-04','2012-08-31')] if symbol=='600180' else [('2026-04-30',None)]
    if symbol=='000677': expected=[('2010-01-04','2016-03-24'),('2026-04-30',None)]
    assert [(x['start'],x['end']) for x in result]==expected
    dates=pd.DatetimeIndex(['2026-04-28','2026-04-29','2026-04-30','2026-05-06'])
    if symbol=='002514':
        assert funcs['build_st_status_series']({'st_intervals':result},dates).tolist()==[False,False,True,True]


@pytest.mark.parametrize('fault',['wrong_issuer','wrong_date','missing_certified','ambiguous_unreviewed','same_day_conflict'])
def test_builder_does_not_swallow_evidence_error_or_preserve_old_metadata(tmp_path,monkeypatch,fault):
    import microcap_top100_mom16_biweekly_live_v2_0 as v20
    ns=v20.freq_mod.build_security_meta.__globals__
    price=tmp_path/'isolated_dates.csv'
    price.write_text('date\n2010-01-04\n2026-10-29\n',encoding='utf-8')
    rows=notices('600180')
    if fault=='wrong_issuer': rows.loc[0,'issuer_symbol']='600181'
    if fault=='wrong_date': rows.loc[0,'notice_date']='2011-12-15'
    if fault=='missing_certified': rows=rows.iloc[1:]
    if fault=='ambiguous_unreviewed':
        rows.loc[len(rows)]={'announcement_id':'999999999','issuer_symbol':'600180','notice_date':'2020-01-02',
                             'title':'关于回复撤销其他风险警示事项问询函的公告','source_url':'https://static.cninfo.com.cn/finalpage/2020-01-02/999999999.PDF'}
    if fault=='same_day_conflict':
        rows.loc[len(rows)]={'announcement_id':'999999998','issuer_symbol':'600180','notice_date':'2011-12-14',
                             'title':'关于撤销其他风险警示的公告','source_url':'https://static.cninfo.com.cn/finalpage/2011-12-14/999999998.PDF'}
    monkeypatch.setitem(ns,'resolve_cache_path',lambda *a:price)
    monkeypatch.setitem(ns,'ROOT',ROOT)
    monkeypatch.setitem(ns,'load_security_master',lambda:pd.DataFrame())
    monkeypatch.setitem(ns,'fetch_cninfo_st_notices',lambda **kw:rows)
    preserve=Mock(side_effect=AssertionError('must not preserve after semantic evidence failure'))
    old_path=Mock(side_effect=AssertionError('must fail before looking up older metadata'))
    monkeypatch.setitem(ns,'_preserve_security_meta_evidence',preserve)
    monkeypatch.setitem(ns,'resolve_security_meta_path',old_path)
    with pytest.raises(StNoticeEvidenceError): v20.freq_mod.build_security_meta('600180')
    preserve.assert_not_called(); old_path.assert_not_called()


def test_three_versions_use_same_live_evidence_consumer():
    import microcap_top100_mom16_biweekly_live_v2_0 as a
    import microcap_top100_mom16_biweekly_live_v2_3 as b
    import microcap_top100_mom16_biweekly_live_v2_5 as c
    assert b.v2_0.base_mod.freq_mod.build_security_meta is a.freq_mod.build_security_meta
    assert c.v2_0.base_mod.freq_mod.build_security_meta is a.freq_mod.build_security_meta


@pytest.mark.parametrize('fault',[None,'missing_parser','old_parser','missing_hash','wrong_hash'])
def test_historical_nonempty_st_cache_requires_current_provenance_before_builder(tmp_path,monkeypatch,fault):
    import microcap_top100_mom16_biweekly_live_v2_0 as v20
    ns=v20.freq_mod.load_security_meta.__globals__
    payload={'meta_version':ns['SECURITY_META_VERSION'],'st_notice_policy_version':ns['ST_NOTICE_POLICY_VERSION'],
             'st_notice_parser_revision':ns['ST_NOTICE_PARSER_REVISION'],'st_notice_evidence_sha256':evidence_fingerprint(ROOT),
             'symbol':'600180','first_trade_date':'2010-01-04','last_trade_date':'2026-10-29',
             'current_st_snapshot_name':None,'current_st_history_resolved':True,
             'name_history_status':'not_applicable','notice_query_status':'ok',
             'st_intervals':[{'start':'2010-01-04','end':'2012-08-31','source':'cninfo_notice'}]}
    if fault=='missing_parser': payload.pop('st_notice_parser_revision')
    if fault=='old_parser': payload['st_notice_parser_revision']='old-v3'
    if fault=='missing_hash': payload.pop('st_notice_evidence_sha256')
    if fault=='wrong_hash': payload['st_notice_evidence_sha256']='0'*64
    cache=tmp_path/'600180.json'; cache.write_text(json.dumps(payload),encoding='utf-8')
    before=cache.read_bytes()
    monkeypatch.setitem(ns,'ROOT',ROOT)
    monkeypatch.setitem(ns,'resolve_security_meta_path',lambda *a:cache)
    monkeypatch.setitem(ns,'load_current_st_name_map',lambda:{})
    builder=Mock(side_effect=AssertionError('unverified history must not silently rebuild or overwrite'))
    monkeypatch.setitem(ns,'build_security_meta',builder)
    if fault is None:
        assert ns['load_security_meta']('600180')==payload
    else:
        with pytest.raises(ns['SecurityMetaHistoryCorrectionRequired'],match='provenance'):
            ns['load_security_meta']('600180')
    builder.assert_not_called(); assert cache.read_bytes()==before


@pytest.mark.parametrize('fault',[None,'wrong_issuer','conflicting_title','conflicting_date','conflicting_url','missing_issuer'])
def test_real_fetch_provider_identity_is_verified_before_title_consumption(funcs,fault):
    import requests
    original={'announcementId':'999999991','secCode':'600180',
              'announcementTime':int(pd.Timestamp('2020-01-02',tz='Asia/Shanghai').timestamp()*1000),
              'announcementTitle':'关于实施其他风险警示的公告',
              'adjunctUrl':'finalpage/2020-01-02/999999991.PDF'}
    items=[dict(original)]
    if fault=='wrong_issuer': items[0]['secCode']='600181'
    if fault=='missing_issuer': items[0].pop('secCode')
    if fault and fault.startswith('conflicting_'):
        items.append(dict(original))
        if fault=='conflicting_title': items[-1]['announcementTitle']='关于撤销其他风险警示的公告'
        if fault=='conflicting_date': items[-1]['announcementTime']+=86400000
        if fault=='conflicting_url': items[-1]['adjunctUrl']='finalpage/2020-01-02/999999992.PDF'
    reply=Mock(); reply.json.return_value={'totalAnnouncement':len(items),'announcements':items}
    funcs['requests']=SimpleNamespace(post=Mock(return_value=reply),RequestException=requests.RequestException)
    funcs['time']=SimpleNamespace(sleep=Mock())
    funcs['fetch_cninfo_org_map']=lambda:{'600180':'fixture-org'}
    if fault is None:
        result=funcs['fetch_cninfo_st_notices']('600180','20200101','20200103')
        assert len(result)==1 and result.iloc[0]['issuer_symbol']=='600180'
    else:
        with pytest.raises(StNoticeEvidenceError):
            funcs['fetch_cninfo_st_notices']('600180','20200101','20200103')
