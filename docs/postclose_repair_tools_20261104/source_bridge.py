"""Finite source-only bridge after the original archive passes official restore."""
import argparse,hashlib,json,subprocess
from pathlib import Path
from datetime import datetime,timezone

def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def git(root,*args):return subprocess.check_output(['git','-C',str(root),*args])
def verify_scope(root,cfg):
    old,new=cfg['old_restore_runtime'],cfg['new_runtime']
    changed=git(root,'diff','--name-only',old,new).decode().splitlines()
    if set(changed)!=set(cfg['changes']):raise RuntimeError('Unreviewed Git source change')
    for name,spec in cfg['changes'].items():
        if not (name.startswith(('tests/','docs/','data/share_source_sse_review_20261104/')) or name in ('.gitattributes','scripts/share_change_source.py','scripts/share_unchanged_tail.py','scripts/top100_delivery.py','scripts/sse_share_daily_unchanged_20261104.py')):
            raise RuntimeError('Protected source/data change: '+name)
        for ref,key in ((old,'old'),(new,'new')):
            q=subprocess.run(['git','-C',str(root),'show',ref+':'+name],stdout=subprocess.PIPE,stderr=subprocess.DEVNULL)
            actual=hashlib.sha256(q.stdout).hexdigest() if q.returncode==0 else None
            if actual!=spec[key] or spec['new'] is None:raise RuntimeError('Exact Git blob differs: '+name)

def transition(root,cfg):
    if git(root,'rev-parse','HEAD').decode().strip()!=cfg['old_restore_runtime']:raise RuntimeError('Restore source identity changed')
    verify_scope(root,cfg)
    manifest=json.loads((root/'outputs/top100_delivery_manifest.json').read_bytes())
    if manifest['status']!='complete' or manifest['expected_date']!=cfg['original_archive_date']:raise RuntimeError('Missing original complete delivery')
    names=sorted({n for key in ('inputs','artifacts') for n in manifest[key] if n.startswith(('outputs/','.microcap_index_cache/'))}|{'outputs/top100_delivery_manifest.json'})
    before={n:sha(root/n) for n in names}
    for name,spec in cfg['changes'].items():
        path=root/name
        if (sha(path) if path.exists() else None)!=spec['old']:raise RuntimeError('Source checkout differs before transition: '+name)
    subprocess.run(['git','-C',str(root),'checkout','--detach',cfg['new_runtime']],check=True)
    for name,spec in cfg['changes'].items():
        if sha(root/name)!=spec['new']:raise RuntimeError('Source checkout differs after transition: '+name)
    after={n:sha(root/n) for n in names}
    if before!=after:raise RuntimeError('Restored state changed during source transition')
    return {'ok':True,'old_restore_runtime':cfg['old_restore_runtime'],'new_runtime':cfg['new_runtime'],'protected_files':before,'protected_state_unchanged':True,'reviewed_changed_paths':sorted(cfg['changes']),'recorded_at':datetime.now(timezone.utc).isoformat(),'scope':'source-only; no market refresh, no data manifest rewrite, no SMTP'}

def main():
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--manifest',type=Path,required=True);p.add_argument('--manifest-sha256',required=True);p.add_argument('--audit',type=Path,required=True);a=p.parse_args()
    if sha(a.manifest)!=a.manifest_sha256:raise RuntimeError('Source transition approval changed')
    cfg=json.loads(a.manifest.read_bytes());root=a.root.resolve()
    origin=git(root,'remote','get-url','origin').decode().strip()
    if origin not in ('https://github.com/liuruojiang/microcap','https://github.com/liuruojiang/microcap.git'):raise RuntimeError('Not official microcap origin')
    subprocess.run(['git','-C',str(root),'fetch','--no-tags','origin',cfg['new_runtime']],check=True)
    result=transition(root,cfg);a.audit.parent.mkdir(parents=True,exist_ok=True);a.audit.write_text(json.dumps(result,indent=2),encoding='utf8');print(json.dumps({k:v for k,v in result.items() if k!='protected_files'}))
if __name__=='__main__':main()
