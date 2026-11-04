"""Install verified 100-symbol tail inputs into research runtime only, with exact backup."""
import argparse,hashlib,json,shutil,os
from pathlib import Path

def digest(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def main():
 p=argparse.ArgumentParser(description=__doc__)
 for key in ('runtime','stage','panel'):p.add_argument('--'+key,type=Path,required=True)
 p.add_argument('--execute',action='store_true');a=p.parse_args()
 research=Path(__file__).resolve().parent;root=a.runtime.resolve();stage=a.stage.resolve();panel=a.panel.resolve()
 if not root.is_relative_to(research/'replay') or not stage.is_relative_to(research) or stage.is_relative_to(root):raise ValueError('Research-only paths required')
 report_path=stage/'daily_inputs_verified.json';report=json.loads(report_path.read_text(encoding='utf-8'))
 if report.get('approved') is not False or report.get('formal_written') is not False or report.get('target')!='2026-10-30' or len(set(report['symbols']))!=100:raise ValueError('Incomplete stage')
 for rel,expected in report['frozen_source_identity'].items():
  if digest(root/rel)!=expected:raise ValueError('Source authority changed')
 if digest(panel)!='50fed6d2031d9b09a1c06dd9ad5f6f7ca1045a54eb9ca1d81d25c8b1d6b3e3c2':raise ValueError('Wrong verified panel candidate')
 files={Path(rel).as_posix():sha for rel,sha in report['portable_files'].items()};items=[]
 for rel,expected in files.items():
  src=(stage/rel).resolve();dst=(root/rel).resolve()
  if not src.is_relative_to(stage) or not dst.is_relative_to(root) or not rel.startswith('.microcap_index_cache/') or digest(src)!=expected:raise ValueError('Invalid portable closure')
  if dst.exists() and digest(dst)==expected:continue
  items.append((src,dst,rel,expected))
 out=root/'tail30_input_install_20261101'
 if out.exists():raise ValueError('Installation evidence already exists; inspect before retry')
 receipt={'approved':False,'formal_written':False,'stage_report_sha256':digest(report_path),'source_manifest_sha256':digest(root/'source_manifest.json'),'changes':[],'panel_sha256':digest(panel)}
 if not a.execute:print(json.dumps({'files_to_install':len(items)}));return
 out.mkdir()
 # Finish all backups and input checks before mutation.
 for src,dst,rel,expected in items:
  old=digest(dst) if dst.exists() else None
  if old:
   backup=out/'before'/rel;backup.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(dst,backup)
   if digest(backup)!=old:raise ValueError('Backup verification failed')
  receipt['changes'].append({'path':rel,'before':old,'after':expected})
 (out/'plan.json').write_text(json.dumps(receipt,indent=2),encoding='utf-8')
 for src,dst,rel,expected in items:
  dst.parent.mkdir(parents=True,exist_ok=True);tmp=dst.with_name(dst.name+'.tail-install.tmp');shutil.copy2(src,tmp)
  if digest(tmp)!=expected:raise ValueError('Copy changed')
  os.replace(tmp,dst)
 panel_dest=out/'panel_20261030.csv';shutil.copy2(panel,panel_dest)
 for src,dst,rel,expected in items:
  if digest(dst)!=expected:raise ValueError('Installed readback failed')
 receipt.update(status='installed_verified',panel_path=str(panel_dest))
 (out/'receipt.json').write_text(json.dumps(receipt,indent=2),encoding='utf-8');print(json.dumps({'status':receipt['status'],'files':len(items),'panel':str(panel_dest)}))
if __name__=='__main__':main()
