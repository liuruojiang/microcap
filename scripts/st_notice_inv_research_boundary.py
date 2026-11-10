"""Compare past an added, strictly non-equity INV research index record.

Raw observations, clocks and archive history remain untouched. Every other
boundary difference still goes through the complete original native review.
This is a selector-bounded transport rule, not absolute ST completeness.
"""
from pathlib import Path
import json
import re


def is_unconsumed_inv_research(identity,row):
    from scripts.st_notice_evidence import is_equity_issuer
    import microcap_top100_mom16_biweekly_live_v2_0 as base
    if (not isinstance(row,dict) or row.get('art_code')!=identity
            or re.fullmatch(r'AN[0-9]+',str(identity)) is None
            or not isinstance(row.get('title'),str) or not row['title'].strip()
            or base.freq_mod.classify_st_notice_risk_actions(row['title'])!={}):
        return False
    columns=row.get('columns');codes=row.get('codes')
    if (not isinstance(columns,list) or len(columns)!=1 or not isinstance(columns[0],dict)
            or columns[0].get('column_code')!='050003' or columns[0].get('column_name')!='调研活动'
            or not isinstance(codes,list) or not codes):
        return False
    for code in codes:
        if not isinstance(code,dict):return False
        symbol=code.get('stock_code')
        if (not isinstance(symbol,str) or re.fullmatch(r'[036][0-9]{5}',symbol) is None
                or code.get('ann_type')!='INV'
                or code.get('market_code')!=('1' if symbol.startswith('6') else '0')
                or is_equity_issuer(row,symbol)):
            return False
    return True


def reviewed_additions(previous,current):
    from scripts.st_notice_boundary_review import canonical,sha
    from scripts.st_notice_boundary_20261110 import REGISTRY,REGISTRY_SHA
    from scripts.st_notice_evidence import StNoticeEvidenceError
    candidates={k for k in current.keys()-previous.keys() if is_unconsumed_inv_research(k,current[k])}
    if not candidates:return None
    raw=(Path(__file__).resolve().parents[1]/REGISTRY).read_bytes()
    if sha(raw)!=REGISTRY_SHA:
        raise StNoticeEvidenceError('Reviewed morning boundary registry changed')
    registry=json.loads(raw)
    stable=lambda rows:{k:{field:value for field,value in row.items() if field!='display_time'} for k,row in rows.items()}
    # Do not strip the five already-adjudicated INV additions from the exact
    # 480 -> 486 review. Its other metadata/IPO changes still require that review.
    protected=set()
    if stable(previous)==stable(registry['previous_rows']):
        protected=set(registry['current_rows'])-set(registry['previous_rows'])
    ignored=sorted(candidates-protected)
    if not ignored:return None
    comparison={k:row for k,row in current.items() if k not in set(ignored)}
    from scripts.st_notice_refresh import _boundary_precision_drift
    original=_boundary_precision_drift(previous,comparison)
    changed=sorted((previous.keys()^current.keys())|
                   {key for key in previous.keys()&current.keys() if previous[key]!=current[key]})
    return dict(original,policy='non_equity_INV_research_additions_with_native_boundary_v1',
                underlying_boundary_review=original,
                non_equity_added_ids=ignored,
                non_equity_added_rows_sha256=sha(canonical({k:current[k] for k in ignored})),
                exact_profile_registry_sha256=sha(raw),
                changed_event_count=len(changed),
                changed_event_ids_sha256=sha(json.dumps(changed,separators=(',',':')).encode()),
                removed_event_ids=original.get('removed_event_ids',[]),
                risk_projection_unchanged=True,boundary_native_content_unchanged=False)
