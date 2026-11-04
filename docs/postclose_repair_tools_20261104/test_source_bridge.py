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
 assert set(w['on'])=={'workflow_dispatch'};assert w['defaults']['run']['shell']=='bash';steps=w['jobs']['build']['steps'];names=[s.get('name') for s in steps];assert names.index('Restore original approved archive through verified compatible transport')<names.index('Verify exact source-only transition and unchanged restored state')<names.index('Official strict incremental whole refresh and three-version generation');raw=str(w);assert 'send_report.py' not in raw and 'MAIL_PASSWORD' not in raw and 'send-intent' not in raw;assert 'allow-full-rebuild' not in raw
 assert names.index('Prepare and verify native current share evidence before whole refresh')<names.index('Official strict incremental whole refresh and three-version generation')
 refresh=next(s['run'] for s in steps if s.get('name')=='Official strict incremental whole refresh and three-version generation');assert 'for attempt' not in refresh


@pytest.mark.parametrize('wrong', ['source', 'date'])
def test_share_preparation_rejects_wrong_source_or_date_before_state_operations(tmp_path, monkeypatch, wrong):
 spec=importlib.util.spec_from_file_location('share_prepare',HERE/'prepare_share_sources.py');module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
 monkeypatch.setattr(module.subprocess,'check_output',lambda *a,**kw: '0'*40 if wrong=='source' else '62ed737527030de1a9c5f2897b58bc8670ce1c99')
 with pytest.raises(RuntimeError,match='reviewed repair source/date'):module.prepare(tmp_path,'2026-11-03' if wrong=='date' else '2026-11-04')
 assert not list(tmp_path.iterdir())
