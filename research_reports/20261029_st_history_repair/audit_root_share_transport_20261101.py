from pathlib import Path
import sys,json,hashlib,shutil
ROOT=Path(__file__).resolve().parents[2];sys.path[:0]=[str(ROOT),str(ROOT/'tests')]
from scripts import realtime_state_bundle as b
from scripts import share_change_source as s
from scripts.stock_return_basis_store import _extended
from test_share_change_source import install_fixture
BASE=ROOT/'research_reports/20261029_st_history_repair/resume_audit/root_transport_independent_20261101';BASE.mkdir(exist_ok=True)
R=BASE/'source';R.mkdir(exist_ok=True);cache,state,_=install_fixture(R);state.unlink()
A=BASE/'test_authority';A.mkdir(exist_ok=True);relative='issuer_original_evidence_with_unchanged_provider_bytes_'+'x'*90+'/source.json';src=_extended(A/relative);src.parent.mkdir(parents=True,exist_ok=True);body=(ROOT/'tests/fixtures/share_change_real/response.json').read_bytes();src.write_bytes(body)
record=dict(schema='share_baseline_approval_v1',approved=True,fixture_only=True,records=[dict(symbol='000422',candidate_sha256=hashlib.sha256(cache.read_bytes()).hexdigest(),certified_cutoff='2026-10-29',source_files=[dict(path=relative,sha256=hashlib.sha256(body).hexdigest())])]);p=A/'approval.json';p.write_text(json.dumps(record));s.install_approved_share_baseline(root=R,symbol='000422',cache_path=cache,approval_path=p,approval_sha256=hashlib.sha256(p.read_bytes()).hexdigest())
proof=s.verify_share_cache(root=R,symbol='000422',as_of='2026-10-29');print('direct verifier pass',len(proof['source_files']))
try:
 files=b._iter_bundle_files(R);result=dict(status='PASS',files=len(files))
except Exception as e:result=dict(status='FAIL',error_type=type(e).__name__,error=str(e),source_verifier='PASS')
(BASE/'result.json').write_text(json.dumps(result,indent=2));print(result)
