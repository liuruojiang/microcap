"""Prepare/execute an exact-hash shared-base install; never install final NAVs.

Default is plan only. This does not grant historical migration approval.
The caller must supply an actually regenerated base summary for the target root.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

PRIMARY = Path(r'D:\动量策略\微盘股对冲策略').resolve()
RESEARCH = PRIMARY / 'research_reports/20261029_st_history_repair'
PREFIX = 'microcap_top100_mom16_biweekly_live_v2_0_base_'
BASE = {
    'proxy.csv': 'wind_microcap_top_100_biweekly_thursday_16y_cached.csv',
    'proxy_meta.json': PREFIX + 'proxy_meta.json',
    'turnover.csv': PREFIX + 'proxy_turnover.csv',
    'effective_members.csv': PREFIX + 'proxy_effective_members.csv',
    'base_costed.csv': 'microcap_top100_mom16_hedge_zz1000_0p8x_biweekly_thursday_16y_v2_0_base_costed_nav.csv',
}
CACHE_DIRS = ('prices_raw', 'share_change', 'security_meta', 'trade_status',
              'share_sources', 'st_notice_sources')
CACHE_ROOTS = ('security_master.csv', 'active_universe.csv', 'current_st.csv',
               'st_notice_archive_pointer.json')


def native(p):
    value = str(Path(p).resolve())
    return Path('\\\\?\\' + value) if os.name == 'nt' and not value.startswith('\\\\?\\') else Path(value)


def sha(p):
    result = hashlib.sha256()
    with native(p).open('rb') as stream:
        for data in iter(lambda: stream.read(1024 * 1024), b''):
            result.update(data)
    return result.hexdigest()


def read(p):
    return json.loads(native(p).read_bytes())


def inside(path, root):
    path = Path(path).resolve()
    if not path.is_relative_to(root.resolve()) or path.is_symlink():
        raise ValueError('Path outside allowed root: ' + str(path))
    return path


def allowed_destination(relative):
    p = Path(relative)
    if p.is_absolute() or '..' in p.parts:
        raise ValueError('Unsafe destination')
    if p.parts[0] == '.microcap_index_cache':
        if len(p.parts) < 2 or p.parts[1] not in (*CACHE_DIRS, *CACHE_ROOTS, 'return_basis'):
            raise ValueError('Unexpected cache destination')
    elif p.as_posix() not in {'outputs/' + s for s in (*BASE.values(), PREFIX+'proxy_members.csv', PREFIX+'panel_refreshed.csv', PREFIX+'summary.json')}:
        raise ValueError('Final strategy outputs and authority are forbidden')
    return inside(PRIMARY / p, PRIMARY)


def make_plan(args):
    runtime = inside(args.runtime, RESEARCH / 'replay')
    candidate = inside(args.candidate, runtime)
    old = inside(args.candidate29, runtime)
    summary = inside(args.summary, RESEARCH)
    report = read(candidate / 'diagnostic_result.json')
    prior = read(old / 'diagnostic_result.json')
    if report.get('as_of') != '2026-10-30' or report.get('approved') is not False or prior.get('as_of') != '2026-10-29':
        raise ValueError('Wrong diagnostic identity')
    if report['source_manifest_sha256'] != sha(runtime / 'source_manifest.json'):
        raise ValueError('Code source generation mismatch')
    sources = []
    for name, dest in BASE.items():
        path = candidate / name
        if sha(path) != report[name]['sha256']:
            raise ValueError('Candidate hash mismatch: ' + name)
        sources.append((path, 'outputs/' + dest))
    member = old / 'members.csv'
    if sha(member) != prior['members.csv']['sha256'] or report['source_candidate_hashes']['members.csv'] != sha(member):
        raise ValueError('Historical member identity mismatch')
    sources.append((member, 'outputs/' + PREFIX + 'proxy_members.csv'))
    panel = inside(report['panel']['path'], runtime)
    if sha(panel) != report['panel']['sha256']:
        raise ValueError('Panel identity mismatch')
    sources.append((panel, 'outputs/' + PREFIX + 'panel_refreshed.csv'))
    summary_value = read(summary)
    summary_receipt=read(summary.with_name('base_summary_receipt.json'))
    if (summary_receipt.get('summary_sha256')!=sha(summary)
            or summary_receipt.get('generator')!='scripts.realtime_state_bundle.persist_reference_summary'
            or summary_receipt.get('source_manifest_sha256')!=sha(runtime/'source_manifest.json')
            or summary_receipt.get('formal_written') is not False
            or summary_receipt.get('inputs',{}).get(str((candidate/'diagnostic_result.json').resolve()))!=sha(candidate/'diagnostic_result.json')):
        raise ValueError('Summary lacks exact real-generator/candidate receipt')
    if str(summary_value.get('latest_trade_date') or summary_value.get('target_end_date'))[:10] != '2026-10-30':
        raise ValueError('Need genuine regenerated 2026-10-30 base summary')
    def strings(value):
        if isinstance(value, dict):
            for item in value.values():yield from strings(item)
        elif isinstance(value, list):
            for item in value:yield from strings(item)
        elif isinstance(value,str):yield value
    if any('research_reports/' in value.replace('\\','/') for value in strings(summary_value)):
        raise ValueError('Summary contains research path; regenerate for formal target')
    sources.append((summary, 'outputs/' + PREFIX + 'summary.json'))
    cache = runtime / '.microcap_index_cache'
    for name in CACHE_ROOTS:
        sources.append((cache/name, '.microcap_index_cache/'+name))
    # Follow live identities, never sweep runtime backups, locks or failed attempts.
    symbols=sorted(p.stem for p in (cache/'security_meta').glob('??????.json')
                   if p.stem.isascii() and p.stem.isdigit())
    if len(symbols)!=5438:raise ValueError('Expected exact rebuilt 5438 metadata universe')
    collected={}
    def add_bound(path, expected=None):
        path=inside(path, runtime)
        digest=sha(path)
        if expected is not None and digest!=expected:raise ValueError('Closure hash mismatch: '+str(path))
        relative=path.relative_to(runtime).as_posix()
        collected[relative]=path
    def references(value, base):
        if isinstance(value,dict):
            if isinstance(value.get('path'),str) and isinstance(value.get('sha256'),str):
                add_bound(base/value['path'],value['sha256'])
            for child in value.values():references(child,base)
        elif isinstance(value,list):
            for child in value:references(child,base)
    for symbol in symbols:
        for folder,suffix in (('security_meta','.json'),('prices_raw','.csv'),('share_change','.csv'),('trade_status','.json')):
            path=cache/folder/(symbol+suffix);add_bound(path)
        references(read(cache/'trade_status'/(symbol+'.json')),cache/'trade_status')
    sys.path.insert(0,str(PRIMARY))
    from scripts.st_history_scope import source_closure
    for relative,digest in source_closure(runtime).items():
        if relative.startswith('.microcap_index_cache/'):
            add_bound(runtime/relative,digest)
        elif sha(PRIMARY/relative)!=digest:
            raise ValueError('Source evidence must already match committed primary evidence: '+relative)
    from scripts.share_change_source import verify_share_cache
    for symbol in symbols:
        path=cache/'share_sources'/(symbol+'.json')
        if path.is_file():
            add_bound(path)
            proof=verify_share_cache(root=runtime,symbol=symbol,as_of=read(path)['query_end'])
            for relative,digest in proof['source_files'].items():add_bound(cache/'share_sources'/relative,digest)
    basis = cache/'return_basis'
    for symbol in symbols:
        manifest=basis/'manifests'/(symbol+'.json')
        add_bound(manifest)
        add_bound(basis/'prices_adj'/(symbol+'.csv'),read(manifest)['candidate_sha256'])
        identity = hashlib.sha256(json.dumps(read(manifest), sort_keys=True).encode()).hexdigest()
        for name in ('raw.csv','candidate.csv','manifest.json','qfq.js','qfq.json','hfq.js','hfq.json'):
            add_bound(basis/'bundles'/symbol/identity/name)
        for name in ('qfq.js','qfq.json','hfq.js','hfq.json'):
            add_bound(basis/'sources'/symbol/name)
    already={relative for _,relative in sources}
    sources.extend((path,relative) for relative,path in sorted(collected.items()) if relative not in already)
    entries = []
    seen = set()
    for path, relative in sources:
        inside(path, RESEARCH)
        target = allowed_destination(relative)
        if relative in seen:
            raise ValueError('Duplicate destination: '+relative)
        seen.add(relative)
        digest = sha(path)
        before = sha(target) if target.is_file() else None
        entries.append({'source':str(path.resolve()), 'destination':relative,
                        'source_sha256':digest, 'previous_sha256':before,
                        'source_bytes':native(path).stat().st_size,
                        'previous_bytes':native(target).stat().st_size if before is not None else 0,
                        'changed':digest != before})
    return {'schema_version':1, 'target_root':str(PRIMARY), 'approved':False,
            'scope':'shared_base_install_only_not_final_history_approval',
            'candidate_report_sha256':sha(candidate/'diagnostic_result.json'),
            'source_manifest_sha256':sha(runtime/'source_manifest.json'),
            'space_estimate':{
                'changed_source_bytes':sum(r['source_bytes'] for r in entries if r['changed']),
                'backup_previous_bytes':sum(r['previous_bytes'] for r in entries if r['changed']),
                'net_installed_growth_bytes':sum(r['source_bytes']-r['previous_bytes'] for r in entries if r['changed']),
                'max_atomic_temp_bytes':max((r['source_bytes'] for r in entries if r['changed']),default=0),
                'pack_reserved_bytes':1024**3,
                'free_bytes_at_plan':shutil.disk_usage(PRIMARY).free},
            'files':entries}


def execute(plan_path, expected_sha, workers=8):
    if sha(plan_path) != expected_sha:
        raise ValueError('Exact install plan hash mismatch')
    plan = read(plan_path)
    if plan.get('target_root') != str(PRIMARY) or plan.get('scope') != 'shared_base_install_only_not_final_history_approval':
        raise ValueError('Wrong install plan scope')
    backup = RESEARCH/'formal_base_install_backups'/expected_sha
    if backup.exists():
        raise ValueError('Existing transaction: inspect receipt; do not blindly retry')
    if not 1<=workers<=8:raise ValueError('Workers must be 1..8')
    files = plan['files'];prepared=[];destinations=set()
    for row in files:
        source = inside(row['source'], RESEARCH)
        target = allowed_destination(row['destination'])
        identity=str(target).casefold()
        if identity in destinations:raise ValueError('Duplicate destination')
        destinations.add(identity)
        if row['changed'] != (row['source_sha256']!=row['previous_sha256']):raise ValueError('Inconsistent plan changed flag')
        prepared.append((row,source,target))
    backup.mkdir(parents=True)
    receipt = {'complete':False, 'status':'running', 'formal_final_nav_written':False,
               'plan_sha256':expected_sha, 'replaced':[], 'verified_unchanged':0,'errors':[],'workers':workers}
    receipt_path = backup/'receipt.json'
    receipt_path.write_text(json.dumps(receipt, indent=2), encoding='utf-8')
    def copy_one(item):
        row,source,target=item
        try:
            if (sha(target) if target.is_file() else None) != row['previous_sha256']:
                raise ValueError('Destination changed during install')
            if not row['changed']:return row,None
            if target.is_file():
                saved = backup/row['destination']
                native(saved.parent).mkdir(parents=True, exist_ok=True)
                shutil.copy2(native(target), native(saved))
                if sha(saved) != row['previous_sha256']:raise ValueError('Backup verification failed')
            native(target.parent).mkdir(parents=True, exist_ok=True)
            temp = target.with_name(target.name + '.install-' + expected_sha[:12])
            if temp.exists():raise ValueError('Unexpected transaction temp')
            shutil.copy2(native(source), native(temp))
            # Hash the actual copied source bytes before they become formal state.
            if sha(temp) != row['source_sha256']:raise ValueError('Source/copy verification failed')
            os.replace(native(temp), native(target))
            if sha(target) != row['source_sha256']:raise ValueError('Install readback failed')
            return row,None
        except Exception as exc:return row,type(exc).__name__+': '+str(exc)
    iterator=iter(prepared);processed=0
    with ThreadPoolExecutor(max_workers=workers) as executor, (backup/'completed_files.jsonl').open('a',encoding='utf-8') as journal:
        pending={}
        def fill():
            while not receipt['errors'] and len(pending)<workers*2:
                item=next(iterator,None)
                if item is None:break
                pending[executor.submit(copy_one,item)]=item[0]['destination']
        fill()
        while pending:
            done,_=wait(pending,return_when=FIRST_COMPLETED)
            for future in done:
                pending.pop(future)
                row,error=future.result();processed+=1
                if error:receipt['errors'].append({'destination':row['destination'],'error':error})
                elif row['changed']:receipt['replaced'].append(row['destination'])
                else:receipt['verified_unchanged']+=1
                journal.write(json.dumps({'file':row,'error':error},ensure_ascii=False)+'\n');journal.flush()
                if processed%2000==0:print(json.dumps({'processed':processed,'total':len(files),'errors':len(receipt['errors'])}),flush=True)
            fill()
    receipt['complete'] = not receipt['errors'] and processed==len(files)
    receipt['status']='complete' if receipt['complete'] else 'partial'
    receipt_path.write_text(json.dumps(receipt, indent=2), encoding='utf-8')
    print(json.dumps({'complete':receipt['complete'],'status':receipt['status'],'replaced':len(receipt['replaced']),'receipt':str(receipt_path)}))
    if not receipt['complete']:raise RuntimeError('Partial installation; inspect receipt/journal before recovery')


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--runtime',type=Path);p.add_argument('--candidate',type=Path)
    p.add_argument('--candidate29',type=Path);p.add_argument('--summary',type=Path)
    p.add_argument('--plan',type=Path,required=True);p.add_argument('--execute',action='store_true')
    p.add_argument('--plan-sha256')
    p.add_argument('--workers',type=int,default=8)
    a=p.parse_args();a.plan=inside(a.plan,RESEARCH)
    if a.execute:
        if not a.plan_sha256:p.error('--execute requires --plan-sha256')
        execute(a.plan,a.plan_sha256,a.workers)
    else:
        if not all((a.runtime,a.candidate,a.candidate29,a.summary)):p.error('planning requires runtime, candidate, candidate29, summary')
        if a.plan.exists():raise ValueError('Do not overwrite an existing plan')
        result=make_plan(a);a.plan.parent.mkdir(parents=True,exist_ok=True)
        a.plan.write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
        print(json.dumps({'plan':str(a.plan),'sha256':sha(a.plan),'files':len(result['files']),'changed':sum(r['changed'] for r in result['files']),'space_estimate':result['space_estimate'],'formal_written':False}))


if __name__=='__main__':main()
