from pathlib import Path
import sys,json,hashlib,shutil
ROOT=Path(__file__).resolve().parents[2];sys.path[:0]=[str(ROOT),str(ROOT/'tests')]
import pytest,pandas as pd,requests,socket,subprocess
from test_frozen_tail_chain_20260917 import chain
from test_share_change_source import install_fixture
from scripts import realtime_state_bundle as bundle
from scripts import share_change_source as share
from scripts.stock_return_basis_store import _extended
BASE=ROOT/'research_reports/20261029_st_history_repair/resume_audit/root_fixes_independent_20261101';BASE.mkdir(exist_ok=True)
results=[]
for fault in ['nonfinite_anchor','execution_boundary']:
 p=BASE/fault;p.mkdir(exist_ok=True);mp=pytest.MonkeyPatch()
 def no(*a,**kw):raise AssertionError('network and subprocess disabled in audit')
 mp.setattr(requests.sessions.Session,'request',no);mp.setattr(socket.socket,'connect',no);mp.setattr(subprocess,'run',no)
 try:
  st=chain.__wrapped__(p,mp)
  if fault=='nonfinite_anchor':
   df=pd.read_csv(st.args.index_csv);df.loc[0,'close']=float('inf');df.to_csv(st.args.index_csv,index=False)
  else:
   df=pd.read_csv(st.paths['proxy_turnover'])
   for col in ['execution_date','effective_date','return_start_date']:df[col]='2026-09-18'
   df.to_csv(st.paths['proxy_turnover'],index=False)
  before=st.args.index_csv.read_bytes()
  try:answer=st.base.try_extend_proxy_index_without_rebalance(st.args,st.paths,st.paths['panel_shadow'],pd.Timestamp('2026-09-18'))
  except ValueError as e:answer=False;reason=str(e)
  else:reason='returned '+str(answer)
  assert answer is False and st.args.index_csv.read_bytes()==before
  results.append(dict(case=fault,status='PASS',reason=reason,proxy_unchanged=True))
 finally:mp.undo()
R=BASE/'source';R.mkdir(exist_ok=True);cache,state,_=install_fixture(R);state.unlink();A=BASE/'test_authority';A.mkdir(exist_ok=True)
relative='issuer_original_evidence_with_unchanged_provider_bytes_'+'x'*90+'/source.json';src=_extended(A/relative);src.parent.mkdir(parents=True,exist_ok=True);body=(ROOT/'tests/fixtures/share_change_real/response.json').read_bytes();src.write_bytes(body)
record=dict(schema='share_baseline_approval_v1',approved=True,fixture_only=True,records=[dict(symbol='000422',candidate_sha256=hashlib.sha256(cache.read_bytes()).hexdigest(),certified_cutoff='2026-10-29',source_files=[dict(path=relative,sha256=hashlib.sha256(body).hexdigest())])]);p=A/'approval.json';p.write_text(json.dumps(record));share.install_approved_share_baseline(root=R,symbol='000422',cache_path=cache,approval_path=p,approval_sha256=hashlib.sha256(p.read_bytes()).hexdigest())
# Real inventory discovers the cache via security metadata, no mocked file selector.
m=R/'.microcap_index_cache/security_meta';m.mkdir();(m/'000422.json').write_text('{}')
files=bundle._iter_bundle_files(R);D=BASE/'relocated';D.mkdir(exist_ok=True)
for rel in files:
 source=_extended(R/rel);dest=_extended(D/rel);dest.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(source,dest)
proof=share.verify_share_cache(root=D,symbol='000422',as_of='2026-10-29');assert len(proof['source_files'])==3
results.append(dict(case='long_path_closure_relocated',status='PASS',files=len(files),verified_source_files=len(proof['source_files']),max_path_chars=max(len(str(D/rel)) for rel in files)))
report=dict(scope='independent isolated boundary/real source fixture audit; no network or formal writes',results=results,code_sha256={f:hashlib.sha256((ROOT/f).read_bytes()).hexdigest() for f in ['scripts/realtime_state_bundle.py','microcap_top100_mom16_biweekly_live_v2_0.py']})
(BASE/'result.json').write_text(json.dumps(report,indent=2));print(report)
