"""Offline staged metadata rebuild only. No production migration/approval or mail.
Run only after frozen source_manifest plus portable ST archive are installed.
"""
import argparse,hashlib,importlib,json,os,sys,datetime,re
from pathlib import Path
from run_three_version_diagnostic_20261101 import contained,digest,isolate_modules,offline_guard

def main():
 if sys.flags.optimize:raise RuntimeError('Optimized Python forbidden')
 p=argparse.ArgumentParser();p.add_argument('--runtime',type=Path,required=True);p.add_argument('--execute',action='store_true');p.add_argument('--install',action='store_true');p.add_argument('--attempt',default='initial');p.add_argument('--collect-errors',action='store_true');a=p.parse_args()
 if re.fullmatch('[a-z0-9_]+',a.attempt) is None:raise ValueError('Invalid attempt label')
 research=Path(__file__).resolve().parent;root=a.runtime.resolve()
 if not root.is_relative_to(research/'replay'):raise RuntimeError('Only research replay runtime allowed')
 sm_path=root/'source_manifest.json';sm=json.loads(sm_path.read_text(encoding='utf-8'))
 assert sm.get('approved') is False and sm.get('formal_written') is False
 if sm.get('target_root'):assert Path(sm['target_root']).resolve()==root
 def check_code():
  assert digest(sm_path)==sm_sha,'Source manifest changed'
  for name,h in sm['files'].items():assert digest(contained(root,name))==h,name
 sm_sha=digest(sm_path);check_code()
 pointer=root/'.microcap_index_cache/st_notice_archive_pointer.json';assert pointer.is_file()
 pointer_sha=digest(pointer)
 suffix='' if a.attempt=='initial' else '_'+a.attempt
 stage=root/('st_metadata_stage_20261101'+suffix);report=root/('st_metadata_build_20261101'+suffix+'.json')
 if stage.exists() or report.exists():raise RuntimeError('Keep existing batch; use new dedicated runtime for retry')
 if not a.execute:print('Frozen-code and pointer preflight passed; metadata not built');return
 os.chdir(root);sys.path.insert(0,str(root));sys.dont_write_bytecode=True
 os.environ['PYTHONDONTWRITEBYTECODE']='1';os.environ['MPLCONFIGDIR']=str(root/'.mplconfig');os.environ['MPLBACKEND']='Agg'
 os.environ['TEMP']=str(root/'.tmp');os.environ['TMP']=str(root/'.tmp');(root/'.tmp').mkdir(exist_ok=True)
 offline_guard(root)
 import pandas as pd
 v=importlib.import_module('microcap_top100_mom16_biweekly_live_v2_0')
 assert Path(v.__file__).resolve().is_relative_to(root)
 disabled=isolate_modules(v,root);v._sync_embedded_base_config()
 from scripts.st_history_scope import validated_source_batch,resolve_archive_reference
 from scripts.st_notice_evidence import evidence_fingerprint
 import scripts.st_history_scope as scope_module
 assert Path(scope_module.__file__).resolve().is_relative_to(root)
 archive_path,archive_sha=resolve_archive_reference(root);assert archive_path and archive_sha
 archive_file=contained(root,archive_path);assert digest(archive_file)==archive_sha
 registry_sha=evidence_fingerprint(root)
 ns=v.freq_mod.build_security_meta.__globals__;ns['SECURITY_META_DIR']=stage;v.freq_mod.SECURITY_META_DIR=stage
 symbols=v.freq_mod.load_universe();assert len(symbols)==5438 and len(set(symbols))==5438
 stage.mkdir();records=[]
 result=dict(scope='isolated_diagnostic_metadata_only',approved=False,formal_written=False,status='building',attempt=a.attempt,errors=[],notice_as_of='2026-10-30',raw_price_cap='2026-10-29',source_manifest_sha256=sm_sha,archive_manifest_sha256=archive_sha,registry_sha256=registry_sha,disabled_shared_fallbacks=disabled,records=records)
 def save():
  tmp=report.with_suffix('.tmp');tmp.write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8');os.replace(tmp,report)
 save()
 try:
  with validated_source_batch(root):
   for i,symbol in enumerate(symbols):
    code=str(symbol).zfill(6)
    try:
     meta=v.freq_mod.build_security_meta(code,as_of='2026-10-30')
     assert meta and meta['symbol']==code
     assert meta['notice_query_status']=='ok' and meta['notice_source_complete'] is True
     assert meta['current_st_history_resolved'] is True
     assert meta['st_notice_parser_revision']==v.freq_mod.ST_NOTICE_PARSER_REVISION
     assert meta['st_notice_evidence_sha256']==registry_sha and meta['st_notice_archive_sha256']==archive_sha
     assert contained(root,meta['st_notice_archive_manifest_path'])==archive_file
     assert pd.Timestamp(meta['last_trade_date'])<=pd.Timestamp('2026-10-29')
     horizon=min(pd.Timestamp('2026-10-30'),pd.Timestamp(meta['delist_date'])) if meta.get('delist_date') else pd.Timestamp('2026-10-30')
     assert pd.Timestamp(meta['notice_query_end_date'])==horizon
     assert all('current_name_snapshot' not in str(x.get('source','')) for x in meta['st_intervals'])
     f=stage/(code+'.json');read=json.loads(f.read_text(encoding='utf-8'));assert read==meta
     records.append(dict(symbol=code,sha256=digest(f),last_trade_date=meta['last_trade_date'],notice_query_end_date=meta['notice_query_end_date'],intervals=len(meta['st_intervals'])))
    except Exception as exc:
     result['errors'].append(dict(symbol=code,error=type(exc).__name__+': '+str(exc)));save();print('blocked',code,str(exc),flush=True)
     if not a.collect_errors:raise
    if (i+1)%100==0:save();print('processed',i+1,'/5438; passed',len(records),'blocked',len(result['errors']),flush=True)
  check_code();assert digest(pointer)==pointer_sha and digest(archive_file)==archive_sha and evidence_fingerprint(root)==registry_sha
  assert not result['errors'], 'Full pool has blocked symbols; no metadata installation allowed'
  assert len(records)==5438 and len(list(stage.glob('*.json')))==5438
  assert max(x['last_trade_date'] for x in records)=='2026-10-29'
  result.update(status='stage_verified',completed_at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),stage_path=str(stage));save()
  if a.install:
   dest=root/'.microcap_index_cache/security_meta';backup=root/('st_metadata_previous_20261101'+suffix)
   if backup.exists():raise RuntimeError('Existing backup; installation refused')
   if dest.exists():os.replace(dest,backup)
   try:os.replace(stage,dest)
   except BaseException:
    if backup.exists() and not dest.exists():os.replace(backup,dest)
    raise
   for row in records:assert digest(dest/(row['symbol']+'.json'))==row['sha256']
   result.update(status='installed_verified',installed_path=str(dest),previous_backup=str(backup) if backup.exists() else None);save()
  print(json.dumps({k:v for k,v in result.items() if k!='records'},ensure_ascii=False))
 except BaseException as exc:
  result.update(status='failed_not_approved',error=type(exc).__name__+':'+str(exc));save();raise
if __name__=='__main__':main()
