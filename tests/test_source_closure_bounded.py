import hashlib,json,copy,threading,time
from pathlib import Path
import pytest
from scripts import st_history_scope as scope
from scripts.st_notice_evidence import StNoticeEvidenceError

def fixture(root):
 def save(name,raw):
  p=root/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(raw);return {'path':name,'sha256':hashlib.sha256(raw).hexdigest()}
 shared=save('objects/source.bin',b'actual shared source');a=save('objects/a.json',b'[]');b=save('objects/b.json',b'[]')
 manifest={'schema_version':2,'symbols':{'000001':{'source_files':[shared],'records':a},'000002':{'source_files':[copy.deepcopy(shared)],'records':b}}}
 def seal():return save('archive.json',json.dumps(manifest).encode())
 return manifest,shared,seal

def test_complete_unique_closure_and_read_counts(tmp_path,monkeypatch):
 manifest,shared,seal=fixture(tmp_path);ref=seal();real=scope.read_bound;calls=[];lock=threading.Lock()
 def read(root,spec):
  with lock:calls.append(spec['path'])
  return real(root,spec)
 monkeypatch.setattr(scope,'read_bound',read)
 result=scope.source_closure(tmp_path,ref['path'],ref['sha256'])
 assert set(result)=={'archive.json','objects/source.bin','objects/a.json','objects/b.json'}
 assert calls.count(shared['path'])==1
 assert calls.count('archive.json')==2  # parse authenticated bytes, then final readback

@pytest.mark.parametrize('kind',['corrupt','missing','same_size_restored_mtime'])
def test_every_call_rechecks_bytes(tmp_path,kind):
 manifest,shared,seal=fixture(tmp_path);ref=seal();scope.source_closure(tmp_path,ref['path'],ref['sha256'])
 p=tmp_path/shared['path'];before=p.stat()
 if kind=='missing':p.unlink()
 else:
  raw=p.read_bytes();p.write_bytes(b'X'+raw[1:])
  if kind=='same_size_restored_mtime':
   import os;os.utime(p,ns=(before.st_atime_ns,before.st_mtime_ns))
 with pytest.raises((StNoticeEvidenceError,FileNotFoundError)):
  scope.source_closure(tmp_path,ref['path'],ref['sha256'])

@pytest.mark.parametrize('kind',['conflict','escape'])
def test_bad_declaration_rejected(tmp_path,kind):
 manifest,shared,seal=fixture(tmp_path)
 if kind=='conflict':manifest['symbols']['000002']['source_files'][0]['sha256']='0'*64
 else:manifest['symbols']['000002']['source_files'][0]['path']='../outside.bin'
 ref=seal()
 with pytest.raises(StNoticeEvidenceError):scope.source_closure(tmp_path,ref['path'],ref['sha256'])

def test_manifest_mutation_during_discovery_rejected(tmp_path,monkeypatch):
 manifest,shared,seal=fixture(tmp_path);ref=seal();real=scope.read_bound;first=True
 def read(root,spec):
  nonlocal first
  raw=real(root,spec)
  if spec['path']=='archive.json' and first:
   first=False;(tmp_path/'archive.json').write_bytes(raw+b' ')
  return raw
 monkeypatch.setattr(scope,'read_bound',read)
 with pytest.raises(StNoticeEvidenceError,match='hash mismatch'):scope.source_closure(tmp_path,ref['path'],ref['sha256'])

def test_worker_failure_joins_other_readers(tmp_path,monkeypatch):
 manifest,shared,seal=fixture(tmp_path);ref=seal();real=scope.read_bound;active=0;maximum=0;lock=threading.Lock()
 def read(root,spec):
  nonlocal active,maximum
  with lock:active+=1;maximum=max(maximum,active)
  try:
   if spec['path'].startswith('objects/'):
    time.sleep(.03)
    if spec['path'].endswith('source.bin'):raise OSError('injected read failure')
   return real(root,spec)
  finally:
   with lock:active-=1
 monkeypatch.setattr(scope,'read_bound',read)
 with pytest.raises(OSError,match='injected'):scope.source_closure(tmp_path,ref['path'],ref['sha256'])
 assert active==0 and maximum<=4
