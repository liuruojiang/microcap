from pathlib import Path
import json,hashlib
B=Path(__file__).resolve().parent;D=B/'old4975_shares_tail_20261029'
r=next(x for x in json.loads((D/'final_independent_manifest.json').read_text(encoding='utf-8'))['conflicts'] if x['symbol']=='605006')
p=Path(r['input_path']);raw=p.read_bytes();assert hashlib.sha256(raw).hexdigest()==r['input_sha256']
text=raw.decode('utf-8-sig');lines=text.splitlines();target=[i for i,x in enumerate(lines) if x.startswith('2026-01-19,')];assert len(target)==1
i=target[0];parts=lines[i].split(',');assert float(parts[1])==60001.779;before=lines[i];parts[1]='60001.7880';lines[i]=','.join(parts)
out=D/'issuer_corrected_anchors/605006.csv';out.parent.mkdir(exist_ok=True);out.write_text('\n'.join(lines)+'\n',encoding='utf-8',newline='')
e=json.loads((D/'issuer_evidence/605006_completion20260119.json').read_text(encoding='utf-8'))
report=dict(formal_written=False,approved=False,symbol='605006',input_path=str(p),input_sha256=r['input_sha256'],candidate_path=str(out),candidate_sha256=hashlib.sha256(out.read_bytes()).hexdigest(),source_url=e['url'],source_sha256=e['sha256'],changes=[dict(date='2026-01-19',old_shares=600017790,new_shares=600017880,old_csv_row=before,new_csv_row=lines[i])],scope='anchor correction only; later vendor tail must be merged separately')
(out.parent/'605006_migration_diff.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8');assert p.read_bytes()==raw;print(report)
