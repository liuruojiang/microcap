"""Exact observed non-ST source delta, with unchanged native risk selection.

Both observations and their publication clocks remain immutable. This receipt
accepts no other source pair and does not authorize a historical state rewrite.
"""
from pathlib import Path
import hashlib
import json

REGISTRY = 'data/st_notice_boundary_reviews/reviewed_20261110/registry.json'
REGISTRY_SHA = '18ad66f02a134fcf410b006a107be4b7e95c2df9d2800dc91202a60205c813a1'


def reviewed_boundary(previous,current,precision):
    from scripts.st_notice_boundary_review import canonical,sha
    from scripts.st_notice_evidence import StNoticeEvidenceError,is_equity_issuer
    from scripts.st_history_scope import _daily_notice_row
    import microcap_top100_mom16_biweekly_live_v2_0 as base
    root=Path(__file__).resolve().parents[1]
    path=root/REGISTRY
    if not path.exists():return None
    raw=path.read_bytes()
    if sha(raw)!=REGISTRY_SHA:raise StNoticeEvidenceError('Reviewed morning boundary registry changed')
    review=json.loads(raw)
    stable=lambda rows:{k:{f:v for f,v in row.items() if f!='display_time'} for k,row in rows.items()}
    if stable(previous)!=stable(review['previous_rows']) or stable(current)!=stable(review['current_rows']):return None
    precision(review['previous_rows'],previous);precision(review['current_rows'],current)
    if (review['decision']!='exact_source_review_no_history_rewrite'
            or review['boundary_day']!='2026-11-09' or len(previous)!=480 or len(current)!=486
            or previous.keys()-current.keys() or sorted(current.keys()-previous.keys())!=review['added']
            or set(review['documents'])!=set(review['added'])):
        raise StNoticeEvidenceError('Reviewed morning boundary scope changed')
    def selected(row):
        return {c['stock_code']:_daily_notice_row(row,c['stock_code'],'2026-11-09',{})
                for c in row['codes'] if is_equity_issuer(row,c['stock_code'])
                and _daily_notice_row(row,c['stock_code'],'2026-11-09',{}) is not None}
    for key in previous.keys()&current.keys():
        if selected(previous[key])!=selected(current[key]):
            raise StNoticeEvidenceError('Reviewed morning boundary altered an ST event')
    for key in review['added']:
        if selected(current[key]) or base.freq_mod.classify_st_notice_risk_actions(current[key]['title']):
            raise StNoticeEvidenceError('Reviewed morning addition now has an ST action')
        for artifact in review['documents'][key].values():
            target=(root/'data/st_notice_boundary_reviews'/artifact['path']).resolve()
            if not target.is_relative_to((root/'data/st_notice_boundary_reviews').resolve()) or sha(target.read_bytes())!=artifact['sha256']:
                raise StNoticeEvidenceError('Reviewed morning source document changed')
    changed=sorted((previous.keys()^current.keys())|{k for k in previous.keys()&current.keys() if previous[k]!=current[k]})
    return dict(policy='exact_morning_non_st_boundary_20261110_v1',
                registry_sha256=sha(raw),changed_event_count=len(changed),
                changed_event_ids_sha256=sha(json.dumps(changed,separators=(',',':')).encode()),
                reviewed_non_st_added_ids=review['added'],removed_event_ids=[],
                risk_projection_unchanged=True,boundary_native_content_unchanged=False)
