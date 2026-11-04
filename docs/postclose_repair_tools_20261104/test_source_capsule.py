import hashlib,importlib.util,json
from pathlib import Path
import pytest
HERE=Path(__file__).parent
spec=importlib.util.spec_from_file_location('capsule',HERE/'adopt_source_capsule.py');a=importlib.util.module_from_spec(spec);spec.loader.exec_module(a)

@pytest.fixture
def prefix(tmp_path):
    directory=tmp_path/'.microcap_index_cache/share_sources';directory.mkdir(parents=True)
    csv=b'unit-fixture-only';old=b'{"query_end":"2026-11-03"}'
    state={'symbol':'603028','query_end':'2026-11-04','previous_state':{'sha256':a.sha(old)},'candidate_sha256':a.sha(csv)}
    new=json.dumps(state).encode();(directory/'603028.json').write_bytes(old)
    path=tmp_path/'.microcap_index_cache/share_change/603028.csv';path.parent.mkdir();path.write_bytes(csv)
    row={'previous_session':'2026-11-03','query_end':'2026-11-04','native_verified':True,'previous_state_sha256':a.sha(old),'new_state_sha256':a.sha(new),'candidate_sha256':a.sha(csv)}
    return tmp_path,{'states':{'603028':row}},{'603028.json':new},old

@pytest.mark.parametrize('same',[False,True])
def test_prefix_accepts_only_exact_previous_or_identical_successor(prefix,same):
    root,m,o,old=prefix
    if same:(root/'.microcap_index_cache/share_sources/603028.json').write_bytes(o['603028.json'])
    before,csvs=a.validate_prefix(root,m,o);assert len(before)==len(csvs)==1

@pytest.mark.parametrize('wrong',['branch','csv','successor','date','symbol'])
def test_prefix_or_history_conflict_rejected_before_publication(prefix,wrong):
    root,m,o,old=prefix
    if wrong=='branch':(root/'.microcap_index_cache/share_sources/603028.json').write_bytes(b'newer-or-fork')
    elif wrong=='csv':(root/'.microcap_index_cache/share_change/603028.csv').write_bytes(b'changed')
    elif wrong=='successor':o['603028.json']=b'changed'
    elif wrong=='date':m['states']['603028']['query_end']='2026-11-05'
    else:m['states']['603028']['previous_session']='2026-11-02'
    with pytest.raises((RuntimeError,ValueError)):a.validate_prefix(root,m,o)

def test_unknown_lock_is_preserved(tmp_path):
    lock=tmp_path/'603028.lock';lock.write_text('other-owner')
    with pytest.raises(FileExistsError):
        with a.owned_lock(lock):pass
    assert lock.read_text()=='other-owner'

def test_owned_lock_released_and_changed_owner_preserved(tmp_path):
    lock=tmp_path/'603028.lock'
    with a.owned_lock(lock):assert lock.exists()
    assert not lock.exists()
    with pytest.raises(RuntimeError,match='ownership changed'):
        with a.owned_lock(lock):lock.write_text('changed-owner')
    assert lock.read_text()=='changed-owner'

def test_wrong_capsule_bytes_rejected_before_parsing(tmp_path):
    path=tmp_path/'source.zip';path.write_bytes(b'unit malformed archive')
    with pytest.raises(RuntimeError,match='archive identity'):a.read_capsule(path)
