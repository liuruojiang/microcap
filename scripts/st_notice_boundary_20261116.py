"""Exact Nov16 reobservation: immutable old archive and native ST decisions."""
from pathlib import Path
import copy,json,hashlib
PREFIX='data/st_notice_boundary_reviews/reviewed_20261116/'
REGISTRY=PREFIX+'registry.json'
REGISTRY_SHA='24250141f159b27131bbe26c1d8b65b31ea8c72f9c7a4dfb565b7816b31c8ff7'
ARTIFACTS=('AN202611111831629972.pdf', 'AN202611111831629972.receipt.json', 'AN202611111831629972.txt', 'AN202611111831633016.pdf', 'AN202611111831633016.receipt.json', 'AN202611111831633016.txt', 'AN202611121831660125.pdf', 'AN202611121831660125.receipt.json', 'AN202611121831660125.txt', 'AN202611121831663341.pdf', 'AN202611121831663341.receipt.json', 'AN202611121831663341.txt', 'AN202611131831681746.pdf', 'AN202611131831681746.receipt.json', 'AN202611131831681746.txt', 'AN202611131831684406.pdf', 'AN202611131831684406.receipt.json', 'AN202611131831684406.txt', 'current/manifest.json', 'current/page_0001.json', 'current/page_0001.receipt.json', 'current/page_0002.json', 'current/page_0002.receipt.json', 'current/page_0003.json', 'current/page_0003.receipt.json', 'current/page_0004.json', 'current/page_0004.receipt.json', 'current/page_0005.json', 'current/page_0005.receipt.json', 'current/page_0006.json', 'current/page_0006.receipt.json', 'current/page_0007.json', 'current/page_0007.receipt.json', 'current/page_0008.json', 'current/page_0008.receipt.json', 'current/page_0009.json', 'current/page_0009.receipt.json', 'current/page_0010.json', 'current/page_0010.receipt.json', 'current/page_0011.json', 'current/page_0011.receipt.json', 'current/page_0012.json', 'current/page_0012.receipt.json', 'current/page_0013.json', 'current/page_0013.receipt.json', 'current/page_0014.json', 'current/page_0014.receipt.json', 'previous/manifest.json', 'previous/page_0001.json', 'previous/page_0001.receipt.json', 'previous/page_0002.json', 'previous/page_0002.receipt.json', 'previous/page_0003.json', 'previous/page_0003.receipt.json', 'previous/page_0004.json', 'previous/page_0004.receipt.json', 'previous/page_0005.json', 'previous/page_0005.receipt.json', 'previous/page_0006.json', 'previous/page_0006.receipt.json', 'previous/page_0007.json', 'previous/page_0007.receipt.json', 'previous/page_0008.json', 'previous/page_0008.receipt.json', 'previous/page_0009.json', 'previous/page_0009.receipt.json', 'previous/page_0010.json', 'previous/page_0010.receipt.json', 'previous/page_0011.json', 'previous/page_0011.receipt.json', 'previous/page_0012.json', 'previous/page_0012.receipt.json', 'previous/page_0013.json', 'previous/page_0013.receipt.json', 'previous/page_0014.json', 'previous/page_0014.receipt.json')
SOURCE_FILES=['scripts/st_notice_boundary_20261116.py',REGISTRY]+[PREFIX+n for n in ARTIFACTS]
def sha(raw):return hashlib.sha256(raw).hexdigest()
def canonical(v):return json.dumps(v,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()
def stable(v):return {k:{f:t for f,t in r.items() if f!='display_time'} for k,r in v.items()}
def need(ok,msg):
 from scripts.st_notice_evidence import StNoticeEvidenceError
 if not ok:raise StNoticeEvidenceError('Exact Nov16 catalog review: '+msg)
def load_review(root=None):
 from scripts.st_notice_refresh import _native
 root=Path(root) if root is not None else Path(__file__).resolve().parents[1]
 raw=(root/REGISTRY).read_bytes();need(sha(raw)==REGISTRY_SHA,'registry changed');r=json.loads(raw)
 need(set(r['artifacts'])==set(ARTIFACTS) and r['historical_state_rewrite_authorized'] is False and r['target_freshness_granted'] is False,'scope changed')
 for name,digest in r['artifacts'].items():need(sha((root/PREFIX/name).read_bytes())==digest,'artifact changed '+name)
 rows={k:_native(root/PREFIX/k,'2026-11-12') for k in ('previous','current')}
 for k in rows:need(sha(canonical(stable(rows[k])))==r[k+'_stable_sha256'],'snapshot changed')
 return r,rows
def reviewed_boundary(previous,current,precision):
 # Exact native content match; every new, missing or changed row remains closed.
 if sha(canonical(stable(previous)))!='b88ac32a38a464df74068d7c9bfa64860a94b61489870ab0c1f7435751fac482' or sha(canonical(stable(current)))!='bf5c9e723c1891e14dcdf49a838690031e4d191a74b99bd1e9935a04ed031aae':return None
 r,rows=load_review();precision(rows['previous'],previous);precision(rows['current'],current)
 from scripts.st_notice_boundary_20261111 import _projection,_selectors
 import microcap_top100_mom16_biweekly_live_v2_0 as base
 need(_projection(previous)==_projection(current),'native ST projection changed')
 removed=set(previous)-set(current);added=set(current)-set(previous)
 need(removed=={'AN202611121831663341'} and added=={'AN202611131831681746','AN202611131831684406'},'identity scope changed')
 for k in removed|added:
  row=previous[k] if k in removed else current[k]
  need(not _selectors(row) and base.freq_mod.classify_st_notice_risk_actions(row['title'])=={},'research now has ST semantics')
 # Normalize only the comparison view. Raw pages, authority and history stay intact.
 comparison=copy.deepcopy(current)
 for k in added:del comparison[k]
 for k in removed:comparison[k]=copy.deepcopy(previous[k])
 for k in previous.keys()&current.keys():
  for f in set(previous[k])|set(current[k]):
   if f!='display_time':comparison[k][f]=copy.deepcopy(previous[k][f])
 result=precision(previous,comparison)
 changed=sorted((previous.keys()^current.keys())|{k for k in previous.keys()&current.keys() if previous[k]!=current[k]})
 return dict(result,policy='exact_native_catalog_observation_20261116_v1',review_registry_sha256=REGISTRY_SHA,changed_event_count=len(changed),changed_event_ids_sha256=sha(json.dumps(changed,separators=(',',':')).encode()),reviewed_non_st_added_ids=sorted(added),reviewed_non_st_removed_ids=sorted(removed),removed_event_ids=sorted(removed),risk_projection_unchanged=True,boundary_native_content_unchanged=False,original_archive_boundary_preserved=True,historical_state_rewrite_authorized=False,target_freshness_granted=False)
