"""No refresh/test/pack/SMTP: publish one authenticated, already-validated ZIP."""
import argparse,csv,hashlib,io,json,os,subprocess,tempfile,urllib.request,zipfile
from datetime import datetime,timezone
from pathlib import Path,PurePosixPath

REPO='liuruojiang/microcap';RUN=41845742478;ART=13338785106;RELEASE=434089707
R='14b896a0915f700e5b999ff6c47676e6f8acaca1';HEAD='4f6a416477ac06a707d36446805b82bc19590c12'
TAG='microcap-postclose-repair-20261104-14b896a0';NAME='microcap-postclose-repaired-whole-20261104.zip'

def gh(*args):return subprocess.check_output(['gh',*args],timeout=1200)
def api(path):return json.loads(gh('api',f'repos/{REPO}/'+path))
def sha(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda:f.read(1024*1024),b''):h.update(block)
    return h.hexdigest()
def native_identity():
    run=api(f'actions/runs/{RUN}');assert run['head_sha']==HEAD and run['conclusion']=='failure'
    jobs=api(f'actions/runs/{RUN}/jobs?per_page=100');steps={s['name']:s['conclusion'] for j in jobs['jobs'] for s in j['steps']}
    assert [n for n,v in steps.items() if v=='failure']==['Publish exact completed matching state and verify remote bytes']
    for name in ['Recover verified already-restored workspace from authenticated failed run','Verify exact source-only transition and unchanged restored state','Adopt already-verified 31 native source successors with exact prefix','Prepare and verify native current share evidence before whole refresh','Official strict incremental whole refresh and three-version generation','Independent official whole delivery check','Query official close-confirmed v2.0','Query official close-confirmed v2.3','Query official close-confirmed v2.5','Official whole pack with original validations','Verify actual source authority disclosures from native chain','Render and validate normal consumer without SMTP','Preserve exact results and diagnostics']:assert steps[name]=='success',name
    artifact=api(f'actions/artifacts/{ART}');assert artifact['workflow_run']['id']==RUN and artifact['name']=='microcap-postclose-repair-20261104' and not artifact['expired']
    return run,jobs,artifact

class NoUploadRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,req,fp,code,msg,headers,newurl):raise RuntimeError('Upload redirect refused; inspect original release before retry')

def main(out):
    out.mkdir(parents=True,exist_ok=False);started=datetime.now(timezone.utc).isoformat()
    run,jobs,artifact=native_identity()
    for name,value in [('native_run.json',run),('native_jobs.json',jobs),('native_artifact.json',artifact)]:
        (out/name).write_text(json.dumps(value,indent=2))
    with tempfile.TemporaryDirectory(prefix='verified-publication-') as tmp:
        temp=Path(tmp);archive=temp/'source-artifact.zip'
        print('Downloading authenticated original artifact; no computation or pack',flush=True)
        with archive.open('xb') as stream:subprocess.run(['gh','api',f'repos/{REPO}/actions/artifacts/{ART}/zip'],stdout=stream,check=True,timeout=1200)
        assert archive.stat().st_size==artifact['size_in_bytes']
        evidence=['metadata.json','share_source_disclosures.json','share_preparation.json','source_capsule_adoption.json','source_transition.json','top100_delivery_manifest.json','whole_pack.log']
        evidence+=['microcap_top100_mom16_biweekly_live_'+v+'_latest_signal.csv' for v in ['v2_0','v2_3','v2_5']]
        with zipfile.ZipFile(archive) as z:
            names=z.namelist();assert len(names)==len(set(names))
            packages=[n for n in names if PurePosixPath(n).name==NAME];assert len(packages)==1
            q=PurePosixPath(packages[0]);assert not q.is_absolute() and '..' not in q.parts and '\\' not in str(q)
            package=temp/NAME
            with z.open(packages[0]) as source,package.open('xb') as dest:
                import shutil;shutil.copyfileobj(source,dest)
            for basename in evidence:
                matches=[n for n in names if PurePosixPath(n).name==basename and (n.startswith('repair/') or n.startswith('microcap/outputs/') or n.startswith('outputs/'))]
                assert len(matches)==1,(basename,matches)
                info=z.getinfo(matches[0]);assert info.file_size<=50*1024*1024
                (out/basename).write_bytes(z.read(info))
        expected=sha(package);size=package.stat().st_size;assert size<2*1024**3
        (out/'package_identity.json').write_text(json.dumps({'sha256':expected,'bytes':size,'native_run':RUN,'native_artifact':ART,'strategy_sha':R,'native_pack_step':'success','not_repacked':True},indent=2))
        manifest=json.loads((out/'top100_delivery_manifest.json').read_bytes());assert manifest['status']=='complete' and manifest['expected_date']=='2026-11-04' and not manifest['errors']
        signals={}
        for version,revision in {'v2_0':'plain_mom16_fixed1_20260904','v2_3':'plain_lb25_hl2p5_r2off_no_overheat_20261029','v2_5':'plain_lb20_hl3_entry0_exit0_20260905'}.items():
            p=out/('microcap_top100_mom16_biweekly_live_'+version+'_latest_signal.csv');rows=list(csv.DictReader(io.StringIO(p.read_text(encoding='utf-8-sig'))));assert len(rows)==1
            row=rows[0];assert row['date']=='2026-11-04' and row['strategy_revision']==revision and row['official_close_confirmed_signal'].casefold()=='true'
            assert hashlib.sha256(p.read_bytes().replace(b'\r\n',b'\n')).hexdigest()==manifest['artifacts']['outputs/'+p.name]
            signals[version]=row
        metadata=json.loads((out/'metadata.json').read_bytes());assert metadata['status']=='OK' and metadata['signal_date']=='2026-11-04' and metadata['publication_mode']=='close_confirmed'
        disclosure=json.loads((out/'share_source_disclosures.json').read_bytes());assert disclosure['native_verified'] is True and len(disclosure['disclosures'])==1 and metadata['share_source_disclosures']==disclosure['disclosures']
        for text in ['股本来源分歧','603028','28514.52','28483.52','不据此宣称注销已完成']:assert text in metadata['body']
        assert len(json.loads((out/'share_preparation.json').read_bytes())['records'])==102
        release=api(f'releases/{RELEASE}');assert release['tag_name']==TAG and release['target_commitish']==R
        assets=[a for a in release['assets'] if a['name']==NAME]
        if not assets:
            assert release['draft'] is True and not release['assets']
            print('Uploading the existing native ZIP into original draft by immutable release ID',flush=True)
            url=f'https://uploads.github.com/repos/{REPO}/releases/{RELEASE}/assets?name={NAME}'
            req=urllib.request.Request(url,data=package.read_bytes(),method='POST',headers={'Authorization':'Bearer '+os.environ['GH_TOKEN'],'Content-Type':'application/zip','Accept':'application/vnd.github+json','User-Agent':'verified-native-package-publication'})
            with urllib.request.build_opener(NoUploadRedirect()).open(req,timeout=1200) as response:
                uploaded=json.loads(response.read());assert response.status==201
        else:
            assert len(assets)==1;uploaded=assets[0]
        assert uploaded['name']==NAME and uploaded['size']==size and uploaded.get('digest') in (None,'','sha256:'+expected)
        remote=temp/'remote-readback.zip'
        print('Verifying exact remote asset bytes by immutable asset ID',flush=True)
        with remote.open('xb') as stream:subprocess.run(['gh','api',f'repos/{REPO}/releases/assets/{uploaded["id"]}','-H','Accept: application/octet-stream'],stdout=stream,check=True,timeout=1200)
        assert remote.stat().st_size==size and sha(remote)==expected
        release=api(f'releases/{RELEASE}')
        if release['draft']:gh('api',f'repos/{REPO}/releases/{RELEASE}','--method','PATCH','-F','draft=false')
        release=api(f'releases/tags/{TAG}');assets=[a for a in release['assets'] if a['name']==NAME]
        assert release['id']==RELEASE and not release['draft'] and release['target_commitish']==R and len(assets)==1 and assets[0]['size']==size
        receipt={'ok':True,'state_url':assets[0]['browser_download_url'],'state_sha256':expected,'state_bytes':size,'state_date':'2026-11-04','strategy_sha':R,'release_api':release,'native_run':RUN,'native_run_conclusion':'failure','original_artifact_id':ART,'all_native_data_and_consumer_gates_passed':True,'only_failed_publication_step_recovered':True,'remote_readback_sha256':expected,'smtp_invoked':False,'normal_markers_modified':False,'publication_recovery_started_at':started,'publication_recovery_completed_at':datetime.now(timezone.utc).isoformat(),'source_transition':json.loads((out/'source_transition.json').read_bytes()),'artifact_package_sha256':expected,'signals':signals}
        (out/'new_state_release.json').write_text(json.dumps(receipt,indent=2));print(json.dumps({k:receipt[k] for k in ['ok','state_url','state_sha256','state_bytes','state_date','only_failed_publication_step_recovered']}),flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--out',type=Path,required=True);main(p.parse_args().out)
