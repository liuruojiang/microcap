"""One proven two-event correction candidate, leaves formal input untouched."""
from pathlib import Path
import hashlib,json
B=Path(__file__).resolve().parent;ROOT=B.parents[1];D=B/'old4975_shares_tail_20261029'
source=ROOT/'.microcap_index_cache/share_change/002869.csv';payload=source.read_bytes()
assert hashlib.sha256(payload).hexdigest()=='04bfa388daaca48d17339765ffb8c259fd7f792bb40c85ba1525c7b285e77a0e'
text=payload.decode('utf-8-sig');old='2025-11-21,17691.4991,股份回购';assert text.count(old)==1
new=text.replace(old,'2025-11-21,17756.2991,股份回购\n2025-11-24,17691.4991,股份回购')
out=D/'issuer_corrected_anchors/002869.csv';out.parent.mkdir(exist_ok=True);out.write_text(new,encoding='utf-8',newline='')
evidence=json.loads((D/'issuer_evidence/002869_completion2025.json').read_text(encoding='utf-8'))
report=dict(formal_written=False,approved=False,symbol='002869',input_path=str(source),input_sha256=hashlib.sha256(payload).hexdigest(),candidate_path=str(out),candidate_sha256=hashlib.sha256(out.read_bytes()).hexdigest(),source_url=evidence['url'],source_sha256=evidence['sha256'],changes=[dict(date='2025-11-21',old_shares=176914991,new_shares=177562991),dict(date='2025-11-24',old_event_absent=True,new_shares=176914991)],reason='Issuer explicitly states buyback1993350 cancelledNovember21, restricted648000 cancelledNovember24; old combined both early.',scope='anchor correction only; later vendor tail must be merged separately')
(out.parent/'002869_migration_diff.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
assert source.read_bytes()==payload
print(report)
