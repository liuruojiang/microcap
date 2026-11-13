"""Exact Nov13 D11 catalog reobservation; old authority and ST decisions retained."""
from pathlib import Path
import copy
import hashlib
import json

PREFIX = 'data/st_notice_boundary_reviews/reviewed_20261113/'
REGISTRY = PREFIX + 'registry.json'
REGISTRY_SHA = '62a23dd548fc870729bdbfdec859e4a2cc5fe4c83d710cd8a3137d6f6e86ff13'
CURRENT_STABLE_SHA = 'af1ebfc0660d5c783019ee3edfa09a49b966e18960c9d62e98901196ea78fb8f'
ARTIFACTS = ('manifest.json', *[f'page_{n:04}.{suffix}' for n in range(1,14) for suffix in ('json','receipt.json')],
    *[art+suffix for art in ('AN202611121831660319','AN202611121831660320') for suffix in ('.pdf','.receipt.json')])
SOURCE_FILES = ['scripts/st_notice_boundary_20261113.py',REGISTRY]+[PREFIX+n for n in ARTIFACTS]

def canonical(value):
    return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()

def sha(raw):
    return hashlib.sha256(raw).hexdigest()

def stable(rows):
    return {k:{f:v for f,v in row.items() if f!='display_time'} for k,row in rows.items()}

def need(condition,message):
    from scripts.st_notice_evidence import StNoticeEvidenceError
    if not condition:raise StNoticeEvidenceError('Exact Nov13 observation: '+message)

def load_review(root=None):
    from scripts import st_notice_boundary_20261112_0726 as prior
    from scripts.st_notice_refresh import _native
    root=Path(root) if root is not None else Path(__file__).resolve().parents[1]
    raw=(root/REGISTRY).read_bytes();need(sha(raw)==REGISTRY_SHA,'registry bytes changed')
    r=json.loads(raw)
    need(r['decision']=='exact_20261113_observation_no_history_rewrite'
        and r['historical_state_rewrite_authorized'] is False and r['target_freshness_granted'] is False
        and r['prior_registry_sha256']==prior.REGISTRY_SHA and set(r['artifacts'])==set(ARTIFACTS),'review scope changed')
    _,old=prior.load_review(root)
    for name,digest in r['artifacts'].items():need(sha((root/PREFIX/name).read_bytes())==digest,'artifact changed: '+name)
    current=_native(root/PREFIX,'2026-11-11')
    need(sha(canonical(current))==r['current_native_sha'] and sha(canonical(stable(current)))==CURRENT_STABLE_SHA,'snapshot changed')
    return r,dict(previous=old['previous'],prior_current=old['current'],current=current)

def reviewed_boundary(previous,current,precision):
    from scripts import st_notice_boundary_20261112_0726 as prior
    if sha(canonical(stable(previous)))!=prior.PREVIOUS_STABLE_SHA or sha(canonical(stable(current)))!=CURRENT_STABLE_SHA:return None
    r,rows=load_review();precision(rows['previous'],previous);precision(rows['current'],current)
    added=sorted(current.keys()-rows['prior_current'].keys())
    need(added==r['added_ids'] and len(added)==2 and len(current)==1249,'added identity scope changed')
    from scripts.st_notice_boundary_20261111 import _selectors,_projection
    import microcap_top100_mom16_biweekly_live_v2_0 as base
    for identity in added:
        row=current[identity]
        need(base.freq_mod.classify_st_notice_risk_actions(row['title'])=={} and not _selectors(row),'registration now has ST semantics')
    positive=r['positive_risk_id']
    need(stable({positive:current[positive]})==stable({positive:r['positive_after']})
        and set(_selectors(current[positive]))=={'002731'},'positive risk issuer lost')
    comparison=copy.deepcopy(current)
    for identity in added:del comparison[identity]
    for field in ('listing_state','codes'):comparison[positive][field]=copy.deepcopy(rows['prior_current'][positive][field])
    underlying=prior.reviewed_boundary(previous,comparison,precision)
    need(underlying is not None and underlying['risk_projection_unchanged'],'prior adjudication failed')
    need(_projection(previous)==_projection(current),'full native ST projection changed')
    changed=sorted((previous.keys()^current.keys())|{k for k in previous if previous[k]!=current[k]})
    return dict(underlying,policy='exact_native_catalog_observation_20261113_v1',underlying_boundary_review=underlying,
        review_registry_sha256=REGISTRY_SHA,current_stable_native_sha256=CURRENT_STABLE_SHA,
        changed_event_count=len(changed),changed_event_ids_sha256=sha(json.dumps(changed,separators=(',',':')).encode()),
        reviewed_non_st_added_ids=sorted(current.keys()-previous.keys()),risk_projection_unchanged=True,
        original_archive_boundary_preserved=True,historical_state_rewrite_authorized=False,target_freshness_granted=False)
