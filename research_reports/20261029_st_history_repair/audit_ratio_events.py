"""Read-only real corporate-action and rounding reconciliation; research only."""
import hashlib,json
from pathlib import Path
import pandas as pd
B=Path(__file__).resolve().parent
O=B/'return_basis_candidate_20261030'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def read(p):return json.loads(p.read_text(encoding='utf-8'))
cases=[]
for symbol,date,div,bonus,rights,price,evidence in [
 ('000418','2012-06-11',.2,0,0,0,'000418_cash2012'),
 ('000559','2011-05-25',.3,.3,0,0,'000559_bonus2011'),
 ('600036','2013-09-05',0,0,.174,9.29,'600036_rights2013')]:
 r=read(O/'results'/f'{symbol}.json');raw=pd.read_csv(r['raw_path']).set_index('date')
 adj=pd.read_csv(O/'prices_adj'/f'{symbol}.csv').set_index('date')
 i=raw.index.get_loc(date);prev=float(raw.iloc[i-1].close_raw);close=float(raw.loc[date,'close_raw'])
 ref=(prev-div+rights*price)/(1+bonus+rights)
 expected=close/ref-1;actual=float(adj.loc[date,'close_adj']/adj.iloc[adj.index.get_loc(date)-1].close_adj-1)
 row=dict(symbol=symbol,date=date,raw_previous=prev,raw_close=close,cash_per_old_share=div,bonus_per_old_share=bonus,planned_rights_per_old_share=rights,rights_price=price,reference_price=ref,expected_ratio_return=expected,candidate_return=actual,absolute_error=abs(expected-actual),primary_pdf_sha256=sha(O/'event_evidence'/f'{evidence}.bin'),primary_text_sha256=sha(O/'event_evidence'/f'{evidence}.txt'))
 if not rights:row['same_day_pretax_entitlement_return_not_strategy_definition']=((1+bonus)*close+div)/prev-1
 else:
  actual_rights=2962813544/17666130885
  row.update(actual_subscription_ratio=actual_rights,reference_using_actual_subscription=(prev+price*actual_rights)/(1+actual_rights),wealth_accounting_not_validated=True)
 assert abs(expected-actual)<1e-10
 cases.append(row)
expected=cases[0]['candidate_return'];cross=[]
for folder,pattern in [('20261030T031659243602Z','004*parsed.json'),('20261030T031659243602Z','005*parsed.json'),('20261030T032047628964Z','002*parsed.json')]:
 p=next((B/'wind_probe'/folder).glob(pattern));data=json.loads(read(p)['result']['content'][0]['text'])['data'];index=[c['name'] for c in data['columns']].index('MATCH')
 v=[row[index] for row in data['rows'][:2]];x,y=map(float,v);eps=[.5*10**(-len(s.split('.')[1])) for s in v]
 lo=(y-eps[1])/(x+eps[0])-1;hi=(y+eps[1])/(x-eps[0])-1
 cross.append(dict(source='Wind',path=str(p),sha256=sha(p),prices=v,return_value=y/x-1,rounding_interval=[lo,hi],candidate_in_rounding_interval=lo<=expected<=hi))
 assert lo<=expected<=hi
p=B/'baostock_probe_20261030/000418_cash_qfq.json';d=read(p);idx=d['fields'].index('close');x,y=[float(row[idx]) for row in d['rows'][:2]]
cross.append(dict(source='BaoStock',path=str(p),sha256=sha(p),prices=[x,y],return_value=y/x-1,difference_from_candidate=y/x-1-expected,precision_difference_not_silently_zeroed=True))
pilot=read(O/'pilot12_snapshot/summary.json')
out=dict(formal_approved=False,return_basis='ratio_adjusted_ex_reference_price_return',cases=cases,independent_sources=cross,pilot12_prefix_max=max(r['prefix_invariance_max_abs_difference'] for r in pilot['results']),pilot12_qfq_hfq_max=max(r['qfq_hfq_return_max_abs_difference'] for r in pilot['results']),limitations=['Frozen-source prefix invariance is not historical vendor PIT certification.','Cash/share receivables, taxes, reinvestment, rights funding and actual subscription are not accounted.','Full pool source coverage and current raw tails are audited separately.','Wind aftype=1 with afdate yielding raw is rejected as HFQ validation.','Wind Q2 FY2012 event query is not FY2011 dividend evidence.'])
(O/'event_reconciliation.json').write_text(json.dumps(out,ensure_ascii=False,indent=2),encoding='utf-8')
print(json.dumps(out,ensure_ascii=False,indent=2))
