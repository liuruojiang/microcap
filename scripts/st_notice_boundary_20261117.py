"""Exact Nov17 reobservation: immutable old archive and native ST decisions."""
from pathlib import Path
import copy,json,hashlib
PREFIX='data/st_notice_boundary_reviews/reviewed_20261117/'
REGISTRY=PREFIX+'registry.json'
REGISTRY_SHA='cf8032f59544cd05b73639c1101017520b3be49fa66853db9d5aeff815917be3'
ARTIFACTS=('AN202611121831657482.pdf', 'AN202611121831657482.receipt.json', 'AN202611121831657482.txt', 'AN202611131831691559.pdf', 'AN202611131831691559.receipt.json', 'AN202611131831691559.txt', 'AN202611131831696311.pdf', 'AN202611131831696311.receipt.json', 'AN202611131831696311.txt', 'AN202611131831696312.pdf', 'AN202611131831696312.receipt.json', 'AN202611131831696312.txt', 'AN202611131831697401.pdf', 'AN202611131831697401.receipt.json', 'AN202611131831697401.txt', 'AN202611131831699724.pdf', 'AN202611131831699724.receipt.json', 'AN202611131831699724.txt', 'current/manifest.json', 'current/page_0001.json', 'current/page_0001.receipt.json', 'current/page_0002.json', 'current/page_0002.receipt.json', 'current/page_0003.json', 'current/page_0003.receipt.json', 'current/page_0004.json', 'current/page_0004.receipt.json', 'current/page_0005.json', 'current/page_0005.receipt.json', 'current/page_0006.json', 'current/page_0006.receipt.json', 'current/page_0007.json', 'current/page_0007.receipt.json', 'current/page_0008.json', 'current/page_0008.receipt.json', 'current/page_0009.json', 'current/page_0009.receipt.json', 'current/page_0010.json', 'current/page_0010.receipt.json', 'current/page_0011.json', 'current/page_0011.receipt.json', 'current/page_0012.json', 'current/page_0012.receipt.json', 'current/page_0013.json', 'current/page_0013.receipt.json', 'current/page_0014.json', 'current/page_0014.receipt.json', 'current/page_0015.json', 'current/page_0015.receipt.json', 'current/page_0016.json', 'current/page_0016.receipt.json', 'previous/manifest.json', 'previous/page_0001.json', 'previous/page_0001.receipt.json', 'previous/page_0002.json', 'previous/page_0002.receipt.json', 'previous/page_0003.json', 'previous/page_0003.receipt.json', 'previous/page_0004.json', 'previous/page_0004.receipt.json', 'previous/page_0005.json', 'previous/page_0005.receipt.json', 'previous/page_0006.json', 'previous/page_0006.receipt.json', 'previous/page_0007.json', 'previous/page_0007.receipt.json', 'previous/page_0008.json', 'previous/page_0008.receipt.json', 'previous/page_0009.json', 'previous/page_0009.receipt.json', 'previous/page_0010.json', 'previous/page_0010.receipt.json', 'previous/page_0011.json', 'previous/page_0011.receipt.json', 'previous/page_0012.json', 'previous/page_0012.receipt.json', 'previous/page_0013.json', 'previous/page_0013.receipt.json', 'previous/page_0014.json', 'previous/page_0014.receipt.json', 'previous/page_0015.json', 'previous/page_0015.receipt.json', 'previous/page_0016.json', 'previous/page_0016.receipt.json')
SOURCE_FILES=['scripts/st_notice_boundary_20261117.py',REGISTRY]+[PREFIX+n for n in ARTIFACTS]
def sha(raw):return hashlib.sha256(raw).hexdigest()
def canonical(v):return json.dumps(v,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()
def stable(v):return {k:{f:t for f,t in r.items() if f!='display_time'} for k,r in v.items()}
def need(ok,msg):
 from scripts.st_notice_evidence import StNoticeEvidenceError
 if not ok:raise StNoticeEvidenceError('Exact Nov17 catalog review: '+msg)
def load_review(root=None):
 from scripts.st_notice_refresh import _native
 root=Path(root) if root is not None else Path(__file__).resolve().parents[1]
 raw=(root/REGISTRY).read_bytes();need(sha(raw)==REGISTRY_SHA,'registry changed');r=json.loads(raw)
 need(set(r['artifacts'])==set(ARTIFACTS) and r['historical_state_rewrite_authorized'] is False and r['target_freshness_granted'] is False,'scope changed')
 for name,digest in r['artifacts'].items():need(sha((root/PREFIX/name).read_bytes())==digest,'artifact changed '+name)
 rows={k:_native(root/PREFIX/k,'2026-11-13') for k in ('previous','current')}
 for k in rows:need(sha(canonical(stable(rows[k])))==r[k+'_stable_sha256'],'snapshot changed')
 return r,rows
def reviewed_boundary(previous,current,precision):
 if sha(canonical(stable(previous)))!='cdaa2ed71e14409c79084e6d394796f9a383b0037a8ef6eea2fe135374363796' or sha(canonical(stable(current)))!='499031f9f06aa1920f9bca83ed9aa525315a9675235916f88948631a9f678da1':return None
 r,rows=load_review();precision(rows['previous'],previous);precision(rows['current'],current)
 from scripts.st_notice_boundary_20261111 import _projection,_selectors
 import microcap_top100_mom16_biweekly_live_v2_0 as base
 need(previous.keys()==current.keys() and _projection(previous)==_projection(current),'native ST projection or identity changed')
 changed=[k for k in previous if stable({k:previous[k]})!=stable({k:current[k]})]
 need(set(changed)==set(r['changed']),'metadata scope changed')
 for k in changed:
  for row in (previous[k],current[k]):need(not _selectors(row) and base.freq_mod.classify_st_notice_risk_actions(row['title'])=={},'new ST semantics')
 comparison=copy.deepcopy(current)
 for k in changed:
  for f in set(previous[k])|set(current[k]):
   if f!='display_time':comparison[k][f]=copy.deepcopy(previous[k][f])
 result=precision(previous,comparison)
 all_changed=sorted(k for k in previous if previous[k]!=current[k])
 return dict(result,policy='exact_native_catalog_observation_20261117_v1',review_registry_sha256=REGISTRY_SHA,changed_event_count=len(all_changed),changed_event_ids_sha256=sha(json.dumps(all_changed,separators=(',',':')).encode()),reviewed_metadata_changed_ids=sorted(changed),removed_event_ids=[],risk_projection_unchanged=True,boundary_native_content_unchanged=False,original_archive_boundary_preserved=True,historical_state_rewrite_authorized=False,target_freshness_granted=False)
