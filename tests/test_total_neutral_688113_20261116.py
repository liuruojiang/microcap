import base64,copy,json,zlib
from datetime import datetime
from pathlib import Path
import pytest
from scripts import share_change_source as s
from frozen_exchange_calendar import frozen_exchange_sessions
ROOT=Path(__file__).resolve().parents[1]
@pytest.fixture(autouse=True)
def calendar(frozen_exchange_sessions):return frozen_exchange_sessions
@pytest.fixture
def case():
 v=json.loads((ROOT/'tests/fixtures/total_neutral_688113_20261116/case.json').read_bytes())
 return v,{k:zlib.decompress(base64.b64decode(b)) for k,b in v['observations_zlib_b64'].items()}
def verify(case):
 v,b=case
 def bound(k,h):
  raw=b[k]
  if s._sha(raw)!=h:raise ValueError('fixture object changed')
  return raw
 return s.verify_dated_window(v['manifest'],symbol='688113',previous_day=v['manifest']['previous_day'],end='2026-11-13',previous_candidate=base64.b64decode(v['previous_candidate_b64']),bound=bound,root=ROOT,now=datetime.fromisoformat(v['observed_at']))
def test_original_failure_and_exact_unchanged_real_csv(case,monkeypatch):
 original=copy.deepcopy(case);old=base64.b64decode(case[0]['previous_candidate_b64'])
 actual,facts=verify(case)
 assert actual==old and s._sha(actual)=='5c6640819f2c394f44e41ec03becf79fc704e7b1b861a786e4a397b618e94c15'
 assert facts==case[0]['verified_facts'] and facts['changes']==[] and facts['structure_events'][0]['old_total_unchanged']
 assert case==original
 monkeypatch.setattr(s,'_zero_change_review',lambda symbol:s.STRUCTURE_ONLY_EVENT_20261105)
 with pytest.raises(ValueError,match='exact reviewed route'):verify(case)
@pytest.mark.parametrize('fault',['missing_pdf','duplicate_pdf','changed_pdf','unknown_row','changed_total'])
def test_exact_identity_documents_and_total_gates_remain(case,fault):
 v,b=case;files=v['manifest']['files']
 if fault=='missing_pdf':files.pop('structure_documents')
 elif fault=='duplicate_pdf':files['structure_documents']*=2
 elif fault=='changed_pdf':
  e=files['structure_documents'][0];b[e['raw_path']]+=b'changed'
 else:
  for e in files['bounded']+files['unfiltered']:
   data=json.loads(b[e['raw_path']]);row=next(r for r in data['result']['data'] if r.get('INFO_CODE')=='AN202611111831629103')
   row['TOTAL_SHARES' if fault=='changed_total' else 'LIMITED_SHARES_CHANGE']=(row['TOTAL_SHARES']+1 if fault=='changed_total' else 0)
   raw=json.dumps(data).encode();b[e['raw_path']]=raw;e['raw_sha256']=s._sha(raw)
   receipt=json.loads(b[e['receipt_path']]);receipt.update(raw_sha256=s._sha(raw),bytes=len(raw))
   raw=json.dumps(receipt).encode();b[e['receipt_path']]=raw;e['receipt_sha256']=s._sha(raw)
 with pytest.raises(ValueError):verify(case)
