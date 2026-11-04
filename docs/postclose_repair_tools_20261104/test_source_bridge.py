import importlib.util,json,subprocess,hashlib,copy
from pathlib import Path
import pytest
HERE=Path(__file__).parent
spec=importlib.util.spec_from_file_location('bridge',HERE/'source_bridge.py');bridge=importlib.util.module_from_spec(spec);spec.loader.exec_module(bridge)

def test_actual_git_scope_matches_only_reviewed_source_paths():
 root=HERE.parents[1];cfg=json.loads((HERE/'transition.json').read_bytes());bridge.verify_scope(root,cfg)
 assert len(cfg['changes'])==39

@pytest.fixture
def pair(tmp_path):
 subprocess.run(['git','init',str(tmp_path)],check=True,capture_output=True)
 def git(*args):return subprocess.check_output(['git','-C',str(tmp_path),'-c','user.name=repair-test','-c','user.email=repair@example.invalid',*args])
 (tmp_path/'docs').mkdir();(tmp_path/'outputs').mkdir();(tmp_path/'docs/source.txt').write_bytes(b'old');(tmp_path/'outputs/value.csv').write_bytes(b'date,value\n2026-11-03,1\n');(tmp_path/'outputs/top100_delivery_manifest.json').write_text(json.dumps({'status':'complete','expected_date':'2026-11-03','inputs':{'outputs/value.csv':'bound'},'artifacts':{}}));git('add','.');git('commit','-m','old');old=git('rev-parse','HEAD').decode().strip();(tmp_path/'docs/source.txt').write_bytes(b'new');git('add','.');git('commit','-m','new');new=git('rev-parse','HEAD').decode().strip();git('checkout','--detach',old)
 cfg={'old_restore_runtime':old,'new_runtime':new,'original_archive_date':'2026-11-03','changes':{'docs/source.txt':{'old':hashlib.sha256(b'old').hexdigest(),'new':hashlib.sha256(b'new').hexdigest()}}};return tmp_path,cfg,git

def test_source_transition_preserves_all_restored_data(pair):
 root,cfg,git=pair;before=(root/'outputs/value.csv').read_bytes();r=bridge.transition(root,cfg);assert r['protected_state_unchanged'];assert (root/'outputs/value.csv').read_bytes()==before;assert (root/'docs/source.txt').read_bytes()==b'new'

def test_wrong_source_or_git_blob_rejects_before_checkout(pair):
 root,cfg,git=pair;cfg['changes']['docs/source.txt']['new']='0'*64
 with pytest.raises(RuntimeError,match='Exact Git blob'):bridge.transition(root,cfg)
 assert git('rev-parse','HEAD').decode().strip()==cfg['old_restore_runtime']

def test_unapproved_output_change_is_rejected(pair):
 root,cfg,git=pair;git('checkout','--detach',cfg['new_runtime']);(root/'outputs/value.csv').write_bytes(b'changed');git('add','.');git('commit','-m','unapproved data change');cfg['new_runtime']=git('rev-parse','HEAD').decode().strip();git('checkout','--detach',cfg['old_restore_runtime'])
 with pytest.raises(RuntimeError,match='Unreviewed Git source change'):bridge.transition(root,cfg)

def test_post_checkout_data_change_does_not_produce_success(pair,monkeypatch):
 root,cfg,git=pair;real=bridge.subprocess.run
 def run(args,**kw):
  p=real(args,**kw)
  if 'checkout' in args:(root/'outputs/value.csv').write_bytes(b'bad')
  return p
 monkeypatch.setattr(bridge.subprocess,'run',run)
 with pytest.raises(RuntimeError,match='Restored state changed'):bridge.transition(root,cfg)

def test_workflow_is_manual_finite_and_has_no_smtp_or_markers():
 import yaml
 w=yaml.load((HERE.parents[1]/'.github/workflows/native-state-release.yml').read_text(encoding='utf-8-sig'),Loader=yaml.BaseLoader)
 assert set(w['on'])=={'workflow_dispatch'};assert w['defaults']['run']['shell']=='bash';steps=w['jobs']['build']['steps'];names=[s.get('name') for s in steps];assert names.index('Recover verified already-restored workspace from authenticated failed run')<names.index('Verify exact source-only transition and unchanged restored state')<names.index('Official strict incremental whole refresh and three-version generation');raw=str(w);assert 'send_report.py' not in raw and 'MAIL_PASSWORD' not in raw and 'send-intent' not in raw;assert 'allow-full-rebuild' not in raw
 assert names.index('Prepare and verify native current share evidence before whole refresh')<names.index('Official strict incremental whole refresh and three-version generation')
 refresh=next(s['run'] for s in steps if s.get('name')=='Official strict incremental whole refresh and three-version generation');assert 'for attempt' not in refresh


@pytest.mark.parametrize('wrong', ['source', 'date'])
def test_share_preparation_rejects_wrong_source_or_date_before_state_operations(tmp_path, monkeypatch, wrong):
 spec=importlib.util.spec_from_file_location('share_prepare',HERE/'prepare_share_sources.py');module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
 monkeypatch.setattr(module.subprocess,'check_output',lambda *a,**kw: '0'*40 if wrong=='source' else '62ed737527030de1a9c5f2897b58bc8670ce1c99')
 with pytest.raises(RuntimeError,match='reviewed repair source/date'):module.prepare(tmp_path,'2026-11-03' if wrong=='date' else '2026-11-04')
 assert not list(tmp_path.iterdir())


def test_deployed_manifest_hash_binds_git_bytes_instead_of_windows_checkout():
 import re
 root=HERE.parents[1];raw=bridge.git(root,'show','HEAD:docs/postclose_repair_tools_20261104/transition.json')
 workflow=(root/'.github/workflows/native-state-release.yml').read_text(encoding='utf-8-sig')
 recorded=re.search(r'--manifest-sha256 ([a-f0-9]{64})',workflow).group(1)
 assert recorded==hashlib.sha256(raw).hexdigest()


@pytest.fixture
def preserved(tmp_path):
 import tarfile
 spec=importlib.util.spec_from_file_location('recover',HERE/'recover_workspace.py');module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
 root=tmp_path/'repo';root.mkdir();(root/'outputs').mkdir();(root/'strategy.py').write_bytes(b'formal code\n');(root/'outputs/input.csv').write_bytes(b'certified input\n');(root/'outputs/result.csv').write_bytes(b'certified result\n')
 manifest={'status':'complete','expected_date':'2026-11-03','errors':[],'inputs':{'outputs/input.csv':module.sha(root/'outputs/input.csv')},'artifacts':{'outputs/result.csv':module.sha(root/'outputs/result.csv')}}
 (root/'outputs/top100_delivery_manifest.json').write_text(json.dumps(manifest));subprocess.run(['git','init',str(root)],check=True,capture_output=True);subprocess.run(['git','-C',str(root),'add','.'],check=True,capture_output=True)
 archive=tmp_path/'saved.tar.gz'
 def pack():
  with tarfile.open(archive,'w:gz') as stream:
   for path in root.rglob('*'):
    if '.git' not in path.relative_to(root).parts:stream.add(path,arcname='microcap/'+path.relative_to(root).as_posix(),recursive=False)
  return archive
 return module,root,pack


def test_preserved_workspace_requires_git_and_all_native_manifest_bindings(preserved):
 module,root,pack=preserved;manifest,actual,count=module.validate_archive(pack(),root);assert count==4 and manifest['expected_date']=='2026-11-03'
 module.extract_verified(pack(),root,actual);assert (root/'strategy.py').read_bytes()==b'formal code\n'


@pytest.mark.parametrize('name', ['strategy.py','outputs/input.csv','outputs/result.csv'])
def test_preserved_source_or_state_tampering_is_rejected_before_extraction(preserved,name):
 module,root,pack=preserved;(root/name).write_bytes(b'tampered\n')
 with pytest.raises(RuntimeError,match='differs'):module.validate_archive(pack(),root)


@pytest.mark.parametrize('kind', ['escape','symlink','hardlink','git'])
def test_preserved_archive_unsafe_members_are_rejected(preserved,kind):
 import io,tarfile
 module,root,pack=preserved;archive=root.parent/'unsafe.tar.gz';member=tarfile.TarInfo('microcap/../escape' if kind=='escape' else 'microcap/.git/config' if kind=='git' else 'microcap/link')
 if kind in ('symlink','hardlink'):member.type=tarfile.SYMTYPE if kind=='symlink' else tarfile.LNKTYPE;member.linkname='../../escape'
 else:member.size=1
 with tarfile.open(archive,'w:gz') as out:out.addfile(member,io.BytesIO(b'x') if member.isfile() else None)
 with pytest.raises(RuntimeError,match='Unsafe'):module.validate_archive(archive,root)
