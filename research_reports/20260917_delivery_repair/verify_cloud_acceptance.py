import csv,io,json,zipfile,hashlib,sys
from pathlib import Path
import pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from scripts import realtime_state_bundle as state, top100_delivery as delivery
root=Path.cwd(); folder=root/'research_reports/20260917_delivery_repair/cloud_run_35207480791'
run=json.loads((folder/'run.json').read_text(encoding='utf-8-sig'))
assert run['status']=='completed' and run['conclusion']=='success'
assert run['headSha']=='d67de5703d050f689acf1aeb0b15a2f89894e24f'
steps={x['name']:x for j in run['jobs'] for x in j['steps']}
for name in ['Send Gmail','Persist normal-send intent before SMTP','Preserve accepted SMTP receipt','Prepare delivery marker','Mark digest delivered','Persist validated state bundle for same-day recovery','Save refreshed full rebalance cache']:
 assert steps[name]['conclusion']=='skipped',name
assert steps['Restore approved release fallback']['conclusion']=='success'
metadata=json.loads((folder/'digest/artifacts/metadata.json').read_text(encoding='utf-8'))
assert metadata['status']=='OK' and metadata['signal_date']=='2026-09-17'
assert metadata['publication_mode']=='close_confirmed'
p=folder/'whole/microcap-whole-delivery-state.zip'; result={}
with zipfile.ZipFile(p) as z:
 m=state._verify_bundle_manifest(z)
 manifest=json.loads(z.read('outputs/top100_delivery_manifest.json'))
 assert manifest['status']=='complete' and manifest['expected_date']=='2026-09-17'
 for n,h in manifest['artifacts'].items():
  assert hashlib.sha256(z.read(n).replace(b'\r\n',b'\n')).hexdigest()==h,n
 for v in ['0','3','5']:
  n=f'outputs/microcap_top100_mom16_biweekly_live_v2_{v}_latest_signal.csv'
  rows=list(csv.DictReader(io.StringIO(z.read(n).decode('utf-8-sig'))))
  assert len(rows)==1
  row=rows[0]; local=list(csv.DictReader((root/n).open(encoding='utf-8-sig',newline='')))[0]
  keys=['date','version','strategy_revision','signal_timing','current_holding','next_holding','member_rebalance_signal_date','member_rebalance_execution_date','member_rebalance_actionable']
  assert {k:row[k] for k in keys}=={k:local[k] for k in keys}
  assert row['version']==f'2.{v}' and row['date']=='2026-09-17' and row['signal_timing']=='close_confirmed'
  nav_name='outputs/'+delivery.COSTED[v]
  cloud=pd.read_csv(io.BytesIO(z.read(nav_name))); nav_local=pd.read_csv(root/nav_name)
  pd.testing.assert_frame_equal(cloud,nav_local,check_dtype=False,check_exact=False,rtol=1e-12,atol=1e-12)
  result[f'v2.{v}']={'signal':{k:row[k] for k in keys},'costed_rows':len(cloud),'all_costed_columns_match_local':True,'costed_sha256':hashlib.sha256(z.read(nav_name)).hexdigest()}
 report={'ok':True,'run':35207480791,'automation_sha':run['headSha'],'strategy_sha':'e10e6fc1a3ec0105f2282fb3c1290914c879a09e','default_release_recovery_passed':True,'bundle_files_verified':len(m['files']),'final_artifact_hashes_verified':len(manifest['artifacts']),'bundle_sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'streams':result,'extra_email_sent':False,'production_cache_and_delivery_markers_written':False}
(folder/'final_artifact_verification.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
print(json.dumps(report,ensure_ascii=False,indent=2))
