"""Read-only research comparison of frozen official history and completed candidates.

No strategy imports, refresh, network, approval creation, or migration operations.
Prepare a contract first; execute only after the real 2026-10-30 tail completes.
"""
import argparse,csv,hashlib,json,math
from pathlib import Path
from datetime import date

VERSIONS=('v2_0','v2_3','v2_5')
RESEARCH=Path(__file__).resolve().parent


def sha(path):
    digest=hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda:stream.read(1024*1024),b''):digest.update(chunk)
    return digest.hexdigest()


def bound(spec, observed):
    path=Path(spec['path']).resolve()
    if not path.is_file() or sha(path)!=spec['sha256']:
        raise ValueError('Missing or changed exact-hash input: '+str(path))
    observed[str(path)]=spec['sha256'];return path


def read_json(spec, observed):return json.loads(bound(spec,observed).read_bytes())


def csv_rows(path):
    with path.open(encoding='utf-8-sig',newline='') as stream:
        reader=csv.DictReader(stream);rows=list(reader)
    if not rows or any(None in r or any(v is None for v in r.values()) for r in rows):
        raise ValueError('Empty or malformed CSV: '+str(path))
    return rows


def day(text):
    if not isinstance(text,str) or len(text)!=10 or date.fromisoformat(text).isoformat()!=text:
        raise ValueError('Non-session date: '+str(text))
    return text


def stream(path, expected=None):
    rows=csv_rows(path);dates=[day(r['date']) for r in rows]
    if dates!=sorted(set(dates)) or (expected and dates[-1]!=expected):
        raise ValueError('Unsorted, duplicate, or stale stream: '+str(path))
    for row in rows:
        for name in ('return_net','nav_net'):
            value=float(row[name])
            if not math.isfinite(value) or (name=='nav_net' and value<=0):raise ValueError('Invalid stream value')
    return {r['date']:r for r in rows}


def members(path):
    rows=csv_rows(path);groups={}
    for row in rows:
        d=day(row['rebalance_date']);symbol=row['symbol'].zfill(6)
        if len(symbol)!=6 or not symbol.isascii() or not symbol.isdigit():raise ValueError('Invalid member code')
        groups.setdefault(d,[]).append((int(row['rank']),symbol))
    if not groups:raise ValueError('Empty members cannot pass an audit')
    for d,values in groups.items():
        if len(values)!=100 or len({s for _,s in values})!=100 or sorted(r for r,_ in values)!=list(range(1,101)):
            raise ValueError('Incomplete/duplicate/rank-invalid member period: '+d)
        groups[d]=[s for _,s in sorted(values)]
    return groups


def equal(a,b):
    if a==b:return True
    try:
        aa,bb=float(a),float(b)
        return math.isfinite(aa) and math.isfinite(bb) and abs(aa-bb)<=1e-12
    except (ValueError,TypeError):return False


def compare(old,new):
    changes=[]
    for d in sorted(set(old)|set(new)):
        a,b=old.get(d),new.get(d)
        if a is None or b is None:
            changes.append(dict(date=d,column='__row__',old='absent' if a is None else 'present',new='absent' if b is None else 'present'))
            continue
        for name in sorted((set(a)|set(b))-{'date'}):
            if not equal(a.get(name),b.get(name)):
                changes.append(dict(date=d,column=name,old=a.get(name),new=b.get(name)))
    return changes


def run(contract_path, output):
    contract_path=contract_path.resolve();output=output.resolve()
    if not output.is_relative_to(RESEARCH) or output.exists():raise ValueError('Use a new research output directory')
    c=json.loads(contract_path.read_bytes());observed={str(contract_path):sha(contract_path)}
    if c.get('schema')!='microcap_readonly_migration_comparison_v1':raise ValueError('Wrong contract schema')
    # Every source artifact is explicitly supplied; never search/refresh formal state.
    roles={}
    for item in c['source_evidence']:
        if item['role'] in roles:raise ValueError('Duplicate source evidence role')
        roles[item['role']]=str(bound(item,observed))
    required={'source_snapshot','st_install','metadata_install','ratio_install','base_assembly','independent_sessions'}
    if not required<=set(roles):raise ValueError('Missing source provenance role')
    # Existing installation manifests bind source bytes, not merely report claims.
    if not c.get('source_file_manifests'):raise ValueError('Source file hash manifest is required')
    for item in c['source_file_manifests']:
        manifest=read_json(item,observed);base=Path(item['root']).resolve()
        files=manifest[item.get('files_key','files')]
        if not isinstance(files,dict) or not files:raise ValueError('Empty source file manifest')
        for relative,digest in files.items():
            path=(base/relative).resolve()
            if not path.is_relative_to(base):raise ValueError('Source manifest escapes root')
            bound(dict(path=str(path),sha256=digest),observed)
    report29=read_json(c['candidate29_result'],observed);report30=read_json(c['candidate30_result'],observed)
    root29=Path(c['candidate29_result']['path']).resolve().parent
    root30=Path(c['candidate30_result']['path']).resolve().parent
    if report29.get('as_of')!='2026-10-29' or report29.get('formal_written') is not False or report30.get('formal_written') is not False:
        raise ValueError('Candidate must be completed diagnostic output')
    calendar=[day(r['date']) for r in csv_rows(Path(roles['independent_sessions']))]
    if calendar!=sorted(set(calendar)) or '2026-10-30' not in calendar:raise ValueError('Incomplete independent calendar')
    members29=bound(dict(path=str(root29/'members.csv'),sha256=report29['members.csv']['sha256']),observed)
    current_members=members(members29);old_members=members(bound(c['old_official_members'],observed))
    if max(current_members)>'2026-10-29':raise ValueError('Future target members in 29 candidate')
    for name,digest in report30['source_candidate_hashes'].items():
        bound(dict(path=str(root29/name),sha256=digest),observed)
    # Tail membership must have an actual dated, complete effective set.
    effective=csv_rows(bound(c['effective_members30'],observed))
    codes=[r['symbol'].zfill(6) for r in effective]
    if len(codes)!=100 or len(set(codes))!=100 or any(day(r['as_of_date']) not in ('2026-10-29','2026-10-30') for r in effective):raise ValueError('Incomplete tail effective members')
    comparisons={};diffs={}
    for version in VERSIONS:
        p29=bound(dict(path=str(root29/(version+'.csv')),sha256=report29['streams'][version]['sha256']),observed)
        p30=bound(dict(path=str(root30/(version+'.csv')),sha256=report30['streams'][version]['sha256']),observed)
        a,b=stream(p29,'2026-10-29'),stream(p30,'2026-10-30')
        for mapping in (a,b):
            if list(mapping)!=[d for d in calendar if min(mapping)<=d<=max(mapping)]:raise ValueError('Candidate session gap: '+version)
        # Actual daily extension may not rewrite the completed 29 diagnostic stream.
        for d,row in a.items():
            if d not in b or any(not equal(row[k],b[d].get(k)) for k in ('return_net','nav_net')):raise ValueError('30 tail rewrites 29 stream: '+version)
        oldpath=bound(c['old_official_streams'][version],observed);old=stream(oldpath)
        if max(old)>'2026-10-30':raise ValueError('Old formal input is newer than candidate')
        diffs[version]=compare(old,b)
        changed_dates=sorted({r['date'] for r in diffs[version] if r['column']!='__row__'})
        comparisons[version]=dict(old_rows=len(old),old_latest=max(old),candidate_rows=len(b),candidate_latest=max(b),historical_changed_dates=len(changed_dates),first_change=changed_dates[0] if changed_dates else None,last_change=changed_dates[-1] if changed_dates else None,changed_cells=len(diffs[version]))
    share=read_json(c['share_qualification'],observed)
    if share['candidate_members_sha256']!=sha(members29):raise ValueError('Share review belongs to another candidate')
    share_root=Path(c['share_cache_root']).resolve()
    for symbol,digest in share['installed_share_sha256'].items():
        if len(symbol)!=6 or not symbol.isascii() or not symbol.isdigit():raise ValueError('Invalid share review symbol')
        bound(dict(path=str(share_root/(symbol+'.csv')),sha256=digest),observed)
    intervals=bound(c['share_intervals'],observed)
    if share['intervals_sha256']!=sha(intervals) or share['calendar_sha256']!=observed[roles['independent_sessions']]:raise ValueError('Share review input mismatch')
    uncertainty=sorted({r['symbol'].zfill(6) for r in csv_rows(intervals)})
    # Qualification is an impact diagnostic, not issuer-source adjudication.
    share_status=dict(source_dispute_symbols=uncertainty,source_status='not_resolved_by_qualification_report',changed_target_dates=share['changed_dates'],actual_target_changes=share['changes'],approval_status='not_approved',qualification_scope=share['scope'])
    if c.get('share_resolution'):
        resolution=read_json(c['share_resolution'],observed)
        if resolution['candidate_members_sha256']!=sha(members29) or resolution['intervals_sha256']!=sha(intervals):raise ValueError('Share resolution identity mismatch')
        unresolved=sorted(resolution['unresolved_symbols'])
        if not set(unresolved)<=set(uncertainty):raise ValueError('Unknown unresolved share symbol')
        share_status.update(source_dispute_symbols=unresolved,source_status=resolution['source_status'])
    member_changes=[]
    for d in sorted(set(old_members)|set(current_members)):
        a,b=old_members.get(d,[]),current_members.get(d,[])
        if a!=b:member_changes.append(dict(date=d,old_count=len(a),new_count=len(b),entered=sorted(set(b)-set(a)),exited=sorted(set(a)-set(b)),rank_changed=a!=b and set(a)==set(b)))
    # Recheck all inputs before writing research output; no mutations are authorized.
    if any(sha(Path(p))!=h for p,h in observed.items()):raise ValueError('Inputs changed during read-only audit')
    report=dict(schema=c['schema'],scope='research_comparison_only',approved=False,formal_written=False,ready_for_review=True,ready_for_migration=False,migration_status='blocked_unresolved_share_source' if share_status['source_dispute_symbols'] else 'awaiting_user_review',share=share_status,versions=comparisons,member_changes=member_changes,input_hashes=observed,limitations=['Qualification changed_dates=0 is not absolute historical source certification','This report does not install authority, migrate NAV, run refresh, or approve any history','Current-source, actual member impact, and user approval are separate statuses'])
    output.mkdir()
    for version,rows in diffs.items():
        with (output/(version+'_differences.csv')).open('w',encoding='utf-8',newline='') as stream_out:
            writer=csv.DictWriter(stream_out,fieldnames=['date','column','old','new']);writer.writeheader();writer.writerows(rows)
    (output/'comparison.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    return report


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--contract',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    report=run(a.contract,a.output)
    print(json.dumps({'ready_for_review':report['ready_for_review'],'ready_for_migration':False,'migration_status':report['migration_status'],'report':str(a.output/'comparison.json')},ensure_ascii=False))

if __name__=='__main__':main()
