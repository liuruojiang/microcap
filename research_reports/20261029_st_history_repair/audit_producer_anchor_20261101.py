from pathlib import Path
import sys,json
ROOT=Path(__file__).resolve().parents[2];sys.path[:0]=[str(ROOT),str(ROOT/'tests')]
import pytest,pandas as pd,requests,socket,subprocess
from test_frozen_tail_chain_20260917 import chain
B=ROOT/'research_reports/20261029_st_history_repair/resume_audit/producer_anchor_counterexample_20261101';B.mkdir(exist_ok=True)
mp=pytest.MonkeyPatch()
def no(*a,**kw):raise AssertionError('network and subprocess disabled in audit')
mp.setattr(requests.sessions.Session,'request',no);mp.setattr(socket.socket,'connect',no);mp.setattr(subprocess,'run',no)
try:
 st=chain.__wrapped__(B,mp);df=pd.read_csv(st.args.index_csv);df.loc[0,'close']=float('inf');df.to_csv(st.args.index_csv,index=False)
 result=st.base.try_extend_proxy_index_without_rebalance(st.args,st.paths,st.paths['panel_shadow'],pd.Timestamp('2026-09-18'))
 out=pd.read_csv(st.args.index_csv);report=dict(returned=result,rows=len(out),new_close=str(out.close.iloc[-1]),new_return=float(out.daily_return.iloc[-1]),scope='controlled boundary fault; not measured market performance')
 (B/'result.json').write_text(json.dumps(report,indent=2));print(report)
finally:mp.undo()
