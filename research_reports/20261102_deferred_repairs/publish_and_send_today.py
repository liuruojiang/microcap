"""Continue the authorized release and normal daily send once pack succeeds."""
import datetime, json, subprocess, sys, time
from pathlib import Path
root=Path.cwd(); evidence=root/'research_reports/20261102_deferred_repairs'
automation=Path('D:/Codex/workspaces/microcap-no-overheat-daily')
def run(args,cwd=root):
    print(datetime.datetime.now().isoformat(), args[:4], flush=True)
    return subprocess.check_output(args,cwd=cwd,text=True,encoding='utf-8').strip()
def gh(*args):return run(['gh',*args])
def git(*args,cwd=root):return run(['git','-c','maintenance.auto=false','-c','gc.auto=0',*args],cwd)
def wait_run(repo,workflow,sha):
    for _ in range(240):
        rows=json.loads(gh('run','list','--repo',repo,'--workflow',workflow,'--limit','10','--json','databaseId,headSha,status,conclusion,event'))
        matches=[x for x in rows if x['headSha']==sha and x['event']=='workflow_dispatch']
        if matches:
            item=matches[0]
            if item['status']=='completed':
                assert item['conclusion']=='success',item
                return item['databaseId']
        time.sleep(30)
    raise RuntimeError('Workflow completion timeout; inspect before any retry')
for _ in range(240):
    if (evidence/'current_whole_pack_receipt.json').exists():break
    time.sleep(30)
else:raise RuntimeError('Pack receipt not ready; no publication attempted')
packed=json.loads((evidence/'current_whole_pack_receipt.json').read_bytes())
delta=evidence/'scoped-state-delta.zip'
assert not delta.exists(),'Inspect existing delta/publication before retry'
output=run([sys.executable,'-B','-X','utf8',str(evidence/'make_exact_zip_delta.py'),str(evidence/'microcap-native-whole-delivery-20261030.zip'),packed['bundle'],str(delta)])
(evidence/'current_delta_receipt.json').write_text(output,encoding='utf-8')
run([sys.executable,'-B','-X','utf8',str(evidence/'prepare_final_publication.py')])
tag='top100-tencent-daily-20261102';repo='liuruojiang/microcap'
release=json.loads(gh('release','view',tag,'--repo',repo,'--json','isDraft,assets'))
assert release['isDraft'] and not release['assets'],release
gh('release','upload',tag,str(delta),'--repo',repo)
gh('release','edit',tag,'--repo',repo,'--notes-file',str(evidence/'release-notes-final.txt'))
git('diff','--check','--','.github/workflows/native-state-release.yml','docs/deferred_repairs_20261102.md')
git('add','.github/workflows/native-state-release.yml','docs/deferred_repairs_20261102.md')
git('commit','-m','Publish verified November 2 daily state and delivery record')
git('push','origin','main')
release_sha=git('rev-parse','HEAD')
gh('workflow','run','native-state-release.yml','--repo',repo,'--ref','main')
assembly=wait_run(repo,'native-state-release.yml',release_sha)
published=json.loads(gh('release','view',tag,'--repo',repo,'--json','isDraft,assets'))
assert not published['isDraft']
asset=[x for x in published['assets'] if x['name']==Path(packed['bundle']).name]
assert len(asset)==1 and asset[0]['size']==packed['bytes']
git('fetch','origin','main',cwd=automation)
git('merge','--ff-only','origin/main',cwd=automation)
run([sys.executable,'-B','-X','utf8','-m','pytest','-p','no:cacheprovider','tests/test_microcap_workflow_refresh_gate.py','tests/test_microcap_validation_only.py','tests/test_delivery_regression_contract.py','tests/test_restore_approved_microcap_state.py','-q'],automation)
git('diff','--check',cwd=automation)
git('add','.github/workflows/microcap-realtime-digest.yml','.github/workflows/delivery-regression.yml','config/microcap_approved_state.json',cwd=automation)
git('commit','-m','Synchronize November 2 Microcap state and Tencent daily source',cwd=automation)
git('push','origin','main',cwd=automation)
automation_sha=git('rev-parse','HEAD',cwd=automation)
repo2='liuruojiang/codex-daily-automation-probe'
gh('workflow','run','microcap-realtime-digest.yml','--repo',repo2,'--ref','main','-f','validation_only=false','-f','publication_mode=close_confirmed')
receipt=dict(strategy_sha=packed['release_sha'],release_commit=release_sha,automation_sha=automation_sha,bundle_sha256=packed['sha256'],expected_date='2026-11-02',assembly_run=assembly,status='daily_workflow_dispatched',send_authorized=True)
(evidence/'final_publication_receipt.json').write_text(json.dumps(receipt,indent=2),encoding='utf-8')
daily=wait_run(repo2,'microcap-realtime-digest.yml',automation_sha)
receipt.update(daily_run=daily,status='workflow_success_pending_inbox_and_csv_readback')
(evidence/'final_publication_receipt.json').write_text(json.dumps(receipt,indent=2),encoding='utf-8')
gh('run','download',str(daily),'--repo',repo2,'--name','microcap-realtime-digest','--dir',str(evidence/f'cloud_final_{daily}'))
print('DAILY_WORKFLOW_COMPLETE',daily,flush=True)
