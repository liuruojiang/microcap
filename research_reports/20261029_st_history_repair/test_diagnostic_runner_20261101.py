import importlib.util,sys,types,tempfile,pathlib,unittest
ROOT=pathlib.Path(__file__).resolve().parents[2]
p=ROOT/'research_reports/20261029_st_history_repair/run_three_version_diagnostic_20261101.py'
spec=importlib.util.spec_from_file_location('diagnostic_runner',p); runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
class DiagnosticTests(unittest.TestCase):
 def test_path_escape(self):
  root=ROOT/'research_reports/20261029_st_history_repair/replay'
  for bad in ['../../outside','.',str(ROOT/'production.csv')]:
   with self.assertRaises(RuntimeError):runner.contained(root,bad)
 def test_embedded_namespace_shared_disabled(self):
  ns={'ROOT':ROOT,'SHARED_CACHE_DIR':ROOT.parent/'foreign'}
  exec('def check(): return SHARED_CACHE_DIR',ns)
  mod=types.SimpleNamespace(**ns);v=types.SimpleNamespace(**{k:mod for k in ('base_mod','freq_mod','fetch_mod','cost_mod','hedge_mod','overlay_mod')})
  self.assertTrue(runner.isolate_modules(v,ROOT));self.assertIsNone(mod.SHARED_CACHE_DIR);self.assertIsNone(mod.check())
 def test_process_network_and_external_write_block(self):
  import subprocess
  code = """import importlib.util,pathlib,tempfile,socket
spec=importlib.util.spec_from_file_location('r',%r);r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r)
root=pathlib.Path(tempfile.mkdtemp()).resolve();outside=root.parent/(root.name+'-forbidden')
r.offline_guard(root)
(root/'allowed').write_text('controlled test')
for op in [lambda:outside.write_text('must not exist'),lambda:socket.socket().connect_ex(('127.0.0.1',9))]:
 try:op()
 except RuntimeError:pass
 else:raise AssertionError('guard did not fail closed')
assert not outside.exists()
(root/'allowed').unlink();root.rmdir()
""" % str(p)
  run=subprocess.run([sys.executable,'-B','-c',code],capture_output=True,text=True)
  self.assertEqual(run.returncode,0,run.stderr)
 def test_metadata_consumption_is_read_only_and_exact(self):
  with tempfile.TemporaryDirectory() as td:
   root=pathlib.Path(td);meta=root/'meta';meta.mkdir();file=meta/'000001.json';file.write_text('{}')
   with runner.frozen_metadata(meta,['000001']) as hashes:
    self.assertEqual(hashes,{'000001.json':runner.digest(file)})
    self.assertEqual(file.read_text(),'{}')
    for change in [lambda:file.write_text('{"changed":true}'),lambda:file.unlink(),lambda:file.rename(meta/'moved.json'),lambda:(meta/'new.json').write_text('{}')]:
     with self.assertRaisesRegex(RuntimeError,'Frozen metadata update attempted'):change()
   self.assertEqual(file.read_text(),'{}')
   with self.assertRaisesRegex(RuntimeError,'coverage'):
    with runner.frozen_metadata(meta,['000001','000002']):pass
   file.write_text('{"outside_guard":true}')
 def test_frozen_calendar_hash_bounds_and_existing_alias(self):
  import datetime,hashlib
  sys.path.insert(0,str(ROOT))
  import scripts.exchange_calendar as calendar
  previous=calendar.sessions_for_day
  alias=types.ModuleType('controlled_calendar_alias');alias.sessions_for_day=previous;sys.modules[alias.__name__]=alias
  with tempfile.TemporaryDirectory() as td:
   path=pathlib.Path(td)/'sessions.csv';body=b'date\n2026-10-28\n2026-10-29\n2026-10-30\n';path.write_bytes(body);sha=hashlib.sha256(body).hexdigest()
   try:
    bound=runner.bind_frozen_calendar(path,sha,'2026-10-30')
    self.assertEqual(bound['rows'],3)
    self.assertEqual(alias.sessions_for_day(datetime.date(2026,10,29)),[datetime.date(2026,10,d) for d in (28,29,30)])
    for day in [datetime.date(2026,10,27),datetime.date(2026,11,2)]:
     with self.assertRaisesRegex(ValueError,'outside frozen'):calendar.sessions_for_day(day)
    with self.assertRaisesRegex(ValueError,'hash mismatch'):runner.bind_frozen_calendar(path,'0'*64,'2026-10-30')
    path.write_bytes(body.replace(b'2026-10-29\n',b''))
    with self.assertRaisesRegex(ValueError,'source changed'):calendar.sessions_for_day(datetime.date(2026,10,30))
    with self.assertRaisesRegex(ValueError,'hash mismatch'):runner.bind_frozen_calendar(path,sha,'2026-10-30')
    for bad in [b'date\n2026-10-30\n2026-10-29\n',b'date\n2026-10-29\n2026-10-29\n']:
     path.write_bytes(bad)
     with self.assertRaisesRegex(ValueError,'unique and ordered'):runner.bind_frozen_calendar(path,hashlib.sha256(bad).hexdigest(),'2026-10-29')
   finally:
    calendar.sessions_for_day=previous;sys.modules.pop(alias.__name__,None)
 def test_windows_extended_guard(self):
  if sys.platform!='win32':self.skipTest('Windows namespace test')
  import subprocess
  code="""import importlib.util,pathlib,tempfile
spec=importlib.util.spec_from_file_location('r',%r);r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r)
root=pathlib.Path(tempfile.mkdtemp()).resolve();meta=root/'meta';meta.mkdir();(meta/'000001.json').write_text('{}')
ext=lambda p:chr(92)*2+'?'+chr(92)+str(p)
assert r.canonical_path(ext(root))==r.canonical_path(root)
unc=chr(92)*2+'server'+chr(92)+'share'+chr(92)+'folder'
assert r.canonical_path(chr(92)*2+'?'+chr(92)+'UNC'+chr(92)+unc[2:])==r.canonical_path(unc)
r.offline_guard(root)
pathlib.Path(ext(root/'allowed')).write_text('ok')
outside=root.parent/(root.name+'-sibling')
try:pathlib.Path(ext(outside)).write_text('bad')
except RuntimeError:pass
else:raise AssertionError('external extended write allowed')
with r.frozen_metadata(meta,['000001']):
 try:pathlib.Path(ext(meta/'000001.json')).write_text('bad')
 except RuntimeError:pass
 else:raise AssertionError('frozen extended write allowed')
(root/'allowed').unlink();(meta/'000001.json').unlink();meta.rmdir();root.rmdir()
""" % str(p)
  run=subprocess.run([sys.executable,'-B','-c',code],capture_output=True,text=True)
  self.assertEqual(run.returncode,0,run.stderr)
 def test_real_three_version_costed_chain(self):
  sys.path.insert(0,str(ROOT));import pandas as pd,numpy as np
  import microcap_top100_mom16_biweekly_live_v2_0 as v
  import microcap_top100_mom16_biweekly_live_v2_3 as v23
  import microcap_top100_mom16_biweekly_live_v2_5 as v25
  # Controlled numerical fixture tests dispatch and date alignment only, never research returns.
  idx=pd.bdate_range('2010-01-04',periods=140)
  close=pd.DataFrame({'microcap':100*np.exp(np.arange(140)*.002),'hedge':100*np.exp(np.arange(140)*.0002)},index=idx)
  turnover=pd.DataFrame(columns=['rebalance_date','turnover','rebalance_cost'])
  v._sync_embedded_base_config();gross=v.base_mod.apply_momentum_gap_exit_buffer(v.base_mod.run_signal(close).sort_index(),v.V2_0_MOMENTUM_GAP_EXIT_BUFFER)
  out20=v.overlay_mod.apply_v2_0_execution(gross,turnover)
  for mod,ver in [(v23,'3'),(v25,'5')]:
   ix=getattr(mod,'build_v2_'+ver+'_common_index')(close,official_index=out20.index)
   out=getattr(mod,'build_v2_'+ver+'_result')(close,turnover,ix)
   self.assertTrue(out.index.equals(ix));self.assertEqual(out.index.max(),idx.max());self.assertTrue(np.isfinite(out.return_net).all())
  self.assertFalse(v23.OVERHEAT_ENABLED)
if __name__=='__main__':unittest.main()
