from pathlib import Path
import sys,json,hashlib,datetime,csv,io
import pandas as pd
R=Path.cwd();sys.path.insert(0,str(R));from scripts import index_history_preflight as H
B=R/'research_reports/20261029_st_history_repair';D=B/'panel_tail_20261030_20261101';D.mkdir(exist_ok=True);base=B/'replay/runtime_20261029/mnt_strategy_data_cn.csv';raw=base.read_bytes();before=hashlib.sha256(raw).hexdigest();panel=pd.read_csv(io.BytesIO(raw));assert panel.date.iloc[-1]=='2026-10-29';calendar=B/'replay/independent_sessions_20261030.csv';sessions=tuple(pd.to_datetime(pd.read_csv(calendar).date).dt.date);start=datetime.date(2026,10,1);end=datetime.date(2026,10,30);original=H._get;provider='';attempts=[];frames={}
def capture(url,**kwargs):
 began=datetime.datetime.now(datetime.timezone.utc).isoformat()
 try:
  response=original(url,**kwargs);payload=response.content;(D/(provider+'.raw')).write_bytes(payload);receipt=dict(url=url,params=kwargs.get('params'),status=response.status_code,started_at=began,finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),sha256=hashlib.sha256(payload).hexdigest());(D/(provider+'.receipt.json')).write_text(json.dumps(receipt,indent=2),encoding='utf8');return response
 except Exception as exc:
  (D/(provider+'.error.json')).write_text(json.dumps(dict(url=url,started_at=began,error=str(exc)),indent=2),encoding='utf8');raise
H._get=capture
try:
 for provider,func in [('sina_static',H.sina_static),('tencent',H.tencent)]:
  try:
   f=H.validate_history(func(start,end),start,end,sessions);f.to_csv(D/(provider+'.csv'),index=False);frames[provider]=f;attempts.append(dict(provider=provider,status='PASS',rows=len(f),last_date=str(f.date.max().date())))
  except Exception as exc:attempts.append(dict(provider=provider,status='BLOCKED',error=str(exc)))
finally:H._get=original
(D/'attempts.json').write_text(json.dumps(attempts,ensure_ascii=False,indent=2),encoding='utf8');print(attempts,flush=True)
assert len(frames)==2,'Need independent two-provider corroboration before tail candidate'
a,b=frames.values();joined=a.merge(b,on='date',suffixes=('_sina','_tencent'));delta=(joined.close_sina-joined.close_tencent).abs();assert len(joined)==len(a)==len(b) and delta.max()<=0.010000001
history=H.preserve_existing_closes(a,panel,'1.000852');tail=history.loc[history.date==pd.Timestamp(end)];assert len(tail)==1;close=float(tail.close.iloc[0]);assert raw.endswith(b'\n');row=['']*len(panel.columns);row[panel.columns.get_loc('date')]=str(end);row[panel.columns.get_loc('1.000852')]=str(close);buffer=io.StringIO(newline='');csv.writer(buffer,lineterminator='\n').writerow(row);candidate=D/'panel_20261030_candidate.csv';candidate.write_bytes(raw+buffer.getvalue().encode('utf8'));read=pd.read_csv(candidate);pd.testing.assert_frame_equal(read.iloc[:-1].reset_index(drop=True),panel,check_dtype=False);assert candidate.read_bytes().startswith(raw);assert base.read_bytes()==raw
report=dict(status='isolated_tail_candidate_verified',base_path=str(base.relative_to(R)),base_sha256=before,base_rows=len(panel),base_last_date=panel.date.iloc[-1],candidate_path=str(candidate.relative_to(R)),candidate_sha256=hashlib.sha256(candidate.read_bytes()).hexdigest(),candidate_rows=len(read),candidate_last_date=read.date.iloc[-1],columns=list(read.columns),new_row_nonmissing=read.iloc[-1].dropna().to_dict(),historical_prefix_bytes_identical=True,other_new_columns='NaN unchanged existing hedge-only refresh contract',cross_provider_rows=len(joined),cross_provider_max_close_difference=float(delta.max()),canonical_overlap_rows=history.attrs['canonical_overlap_rows'],canonical_max_precision_difference=history.attrs['canonical_max_precision_difference'],calendar_sha256=hashlib.sha256(calendar.read_bytes()).hexdigest(),actual_provider_functions='scripts.index_history_preflight.sina_static/tencent/validate_history/preserve_existing_closes',formal_or_runtime_written=False)
(D/'report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf8');print(report)
