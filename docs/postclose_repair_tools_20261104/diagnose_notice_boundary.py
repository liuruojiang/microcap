"""Source-only diagnosis; never changes a pointer, metadata, economic output or send state."""
import argparse,hashlib,json,subprocess,sys,tempfile,zipfile,shutil
from datetime import datetime,timezone
from pathlib import Path,PurePosixPath

R='14b896a0915f700e5b999ff6c47676e6f8acaca1';SHA='df235b54c0d90cab49bed1c963cd6498f9a97e07d4a15ed83587c03841abc8d6'
def digest(raw):return hashlib.sha256(raw).hexdigest()
def main(source,out):
    source=source.resolve();out=out.resolve();out.mkdir(parents=True,exist_ok=False)
    assert subprocess.check_output(['git','-C',str(source),'rev-parse','HEAD']).decode().strip()==R
    sys.path.insert(0,str(source))
    from scripts.st_history_scope import resolve_archive_reference,source_closure
    from scripts.st_notice_refresh import _native
    from scripts.st_notice_incremental import collect_days
    import microcap_top100_mom16_biweekly_live_v2_0 as base
    from scripts import st_history_scope as scope
    from scripts.st_notice_evidence import _registry
    started=datetime.now(timezone.utc)
    with tempfile.TemporaryDirectory(prefix='notice-boundary-only-') as tmp:
        archive=Path(tmp)/'approved.zip';print('Read approved ZIP only for prior notice sources, not full restoration',flush=True)
        with archive.open('xb') as stream:subprocess.run(['gh','api','repos/liuruojiang/microcap/releases/assets/709979415','-H','Accept: application/octet-stream'],stdout=stream,check=True,timeout=900)
        h=hashlib.sha256()
        with archive.open('rb') as stream:
            for chunk in iter(lambda:stream.read(1024*1024),b''):h.update(chunk)
        assert archive.stat().st_size==612766808 and h.hexdigest()==SHA
        extracted={}
        with zipfile.ZipFile(archive) as z:
            for info in z.infolist():
                p=PurePosixPath(info.filename)
                if not info.filename.startswith('.microcap_index_cache/st_notice'):continue
                assert not p.is_absolute() and '..' not in p.parts and '\\' not in info.filename
                if info.is_dir():continue
                raw=z.read(info);dest=source.joinpath(*p.parts);dest.parent.mkdir(parents=True,exist_ok=True)
                assert not dest.exists() or dest.read_bytes()==raw
                dest.write_bytes(raw);extracted[info.filename]=digest(raw)
        reference=resolve_archive_reference(source);source_closure(source)
        previous=json.loads((source/reference[0]).read_bytes());assert previous['daily_increments'][-1]['date']=='2026-11-04'
        ref=previous['daily_increments'][-1]['manifest'];oldpath=source/ref['path'];assert digest(oldpath.read_bytes())==ref['sha256']
        old=_native(oldpath.parent,'2026-11-04');(out/'prior_native_rows.json').write_text(json.dumps(old,ensure_ascii=False,indent=2),encoding='utf8')
        (out/'prior_day_manifest.json').write_bytes(oldpath.read_bytes())
        print('Collecting only the failed November4 native notice catalog',flush=True)
        capture=out/'current_capture';collect_days(capture,'2026-11-04','2026-11-04',required_asof=datetime.now(timezone.utc))
        current=_native(capture/'2026-11-04','2026-11-04')
        registered,_=_registry(source)
        def select(row):
            symbols=sorted({c['stock_code'] for c in row.get('codes',[])})
            return {s:scope._daily_notice_row(row,s,'2026-11-04',registered) for s in symbols}
        def risk(row):return base.freq_mod.classify_st_notice_risk_actions(row['title'])
        added=sorted(set(current)-set(old));removed=sorted(set(old)-set(current));changed={}
        precision=[]
        for key in sorted(set(old)&set(current)):
            before,after=old[key],current[key];fields={k:{'old':before.get(k),'new':after.get(k)} for k in before.keys()|after.keys() if before.get(k)!=after.get(k)}
            if set(fields)=={'display_time'}:precision.append(key);continue
            if fields:
                changed[key]={'fields':fields,'old_row':before,'new_row':after,'old_risk_actions':risk(before),'new_risk_actions':risk(after),'old_native_selectors':select(before),'new_native_selectors':select(after),'native_selectors_equal':select(before)==select(after)}
        result={'approved':False,'scope':'source-only comparison; no production adoption, refresh, history, tests, SMTP or marker','runtime':R,'approved_package_sha256':SHA,'approved_package_asset_id':709979415,'original_native_manifest_reference':list(reference),'old_boundary_manifest':ref,'source_files_extracted_sha256':extracted,'prior_count':len(old),'current_count':len(current),'added':{k:{'row':current[k],'risk_actions':risk(current[k]),'native_selectors':select(current[k])} for k in added},'removed':{k:old[k] for k in removed},'changed':changed,'display_time_only_ids':precision,'started_at':started.isoformat(),'completed_at':datetime.now(timezone.utc).isoformat()}
        (out/'boundary_comparison.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf8')
        print(json.dumps({'prior_count':len(old),'current_count':len(current),'added':added,'removed':removed,'changed_ids':list(changed),'display_time_only':len(precision),'approved':False}),flush=True)
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--source',type=Path,required=True);p.add_argument('--out',type=Path,required=True);a=p.parse_args();main(a.source,a.out)
