from pathlib import Path
import sys,json
ROOT=Path(__file__).resolve().parents[2];sys.path[:0]=[str(ROOT),str(ROOT/'tests')]
import pytest,pandas as pd,requests,socket,subprocess
from test_frozen_tail_chain_20260917 import chain
B=ROOT/'research_reports/20261029_st_history_repair/resume_audit/producer_execution_counterexample_20261101';B.mkdir(exist_ok=True)
mp=pytest.MonkeyPatch()
def no(*a,**kw):raise AssertionError('network and subprocess disabled in audit')
mp.setattr(requests.sessions.Session,'request',no);mp.setattr(socket.socket,'connect',no);mp.setattr(subprocess,'run',no)
try:
 st=chain.__wrapped__(B,mp);df=pd.read_csv(st.paths['proxy_turnover'])
 for col in ['execution_date','effective_date','return_start_date']:df[col]='2026-09-18'
 df.to_csv(st.paths['proxy_turnover'],index=False)
 before=st.args.index_csv.read_bytes()
 result=st.base.try_extend_proxy_index_without_rebalance(st.args,st.paths,st.paths['panel_shadow'],pd.Timestamp('2026-09-18'))
 report=dict(returned=result,proxy_written=st.args.index_csv.read_bytes()!=before,tail_crosses_recorded_execution=True,scope='controlled execution-boundary fault; not measured market performance')
 (B/'result.json').write_text(json.dumps(report,indent=2));print(report)
finally:mp.undo()
