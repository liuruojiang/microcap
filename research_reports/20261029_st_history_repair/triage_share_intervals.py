from pathlib import Path
import json,csv
from decimal import Decimal
B=Path(__file__).resolve().parent;F=B/'new463_cninfo_shares_20261029';m=json.loads((F/'final_independent_manifest.json').read_text());intervals=[];summary=[]
for r in m['records']:
 a={x['change_date']:Decimal(x['total_shares_10k']) for x in csv.DictReader(Path(r['em_path']).open(encoding='utf-8-sig'))};b={x['change_date']:Decimal(x['total_shares_10k']) for x in csv.DictReader(Path(r['candidate_path']).open(encoding='utf-8-sig'))}
 dates=sorted(set(a)|set(b));va=vb=None;changes=[]
 for i,d in enumerate(dates):
  va=a.get(d,va);vb=b.get(d,vb)
  end=dates[i+1] if i+1<len(dates) else '2026-10-30'
  if d>'2026-10-29' or end<='2010-01-01' or va is None or vb is None or va==vb:continue
  row=dict(symbol=r['symbol'],start_inclusive=max(d,'2010-01-01'),end_exclusive=min(end,'2026-10-30'),em_shares=str(va*10000),cninfo_shares=str(vb*10000),relative_difference=float(abs(va-vb)/va),same_event_day_conflict=d in a and d in b and a[d]!=b[d]);intervals.append(row);changes.append(row)
 if changes:summary.append(dict(symbol=r['symbol'],intervals=len(changes),first_difference=changes[0]['start_inclusive'],last_difference_end=changes[-1]['end_exclusive'],max_relative_difference=max(x['relative_difference'] for x in changes),same_event_day_conflicts=sum(x['same_event_day_conflict'] for x in changes)))
for name,rows in [('historical_difference_intervals.csv',intervals),('historical_difference_priority.csv',sorted(summary,key=lambda r:-r['max_relative_difference']))]:
 with (F/name).open('w',newline='',encoding='utf-8') as f:
  w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
print({'symbols_since2010':len(summary),'intervals':len(intervals),'same_date_conflict_symbols':sum(x['same_event_day_conflicts']>0 for x in summary)})
