import importlib.util, pathlib, pandas as pd, json
from pathlib import Path
p = Path(r"trade_journal_v6.1 plus.py")
spec = importlib.util.spec_from_file_location("tj", p)
mod = importlib.util.module_from_spec(spec)
class _DummyMsg:
    def write(self, *a, **k): pass
    def attach_file(self, *a, **k): pass
class _DummyPoe:
    class BotError(Exception): pass
    def start_message(self): return _DummyMsg()
    def update_settings(self, *a, **k): pass
    class query:
        text = ""
        attachments = []
    default_chat = None
mod.__dict__["poe"] = _DummyPoe()
spec.loader.exec_module(mod)
text = Path(r"C:/Users/Administrator.DESKTOP-95I7VVU/Desktop/动量策略/组合策略测试/trade_log_20260326.csv").read_text(encoding="utf-8")
records, errors = mod._parse_trade_csv_text(text)
subc = sorted([r for r in records if r.get("strategy") == "Sub-C" and r.get("action") not in ("skip","_deleted","hold")], key=lambda r: (r.get("ts", ""), r.get("trade_date", ""), r.get("id", "")))
print('errors', errors)
print('actions', [r['action'] for r in subc])
idx = pd.to_datetime(["2026-03-23","2026-03-25","2026-03-28"])
price_df = pd.DataFrame({
    "QQQM": [240,241,250],
    "VGIT": [59.4,59.4,60],
    "GLDM": [86.9,90.4,91],
    "IBIT": [39.8,40.6,41],
    "VEA": [63.6,64.2,65],
    "VTI": [325.7,326.0,327],
    "DBMF": [29.6,29.79,30],
}, index=idx)
summary = mod._build_us_holdings_pnl(subc, price_df, {k:k for k in price_df.columns}, pd.Timestamp('2026-03-28'))
out = {
    'total_cost': round(summary['total_cost'], 6),
    'realized_pnl': round(summary['realized_pnl'], 6),
    'legs': [{k:(round(v,6) if isinstance(v,float) else v) for k,v in leg.items() if k in ('etf','shares','avg_cost')} for leg in summary['legs']]
}
print(json.dumps(out, ensure_ascii=False, indent=2))
