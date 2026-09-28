# /// script
# requires-python = ">=3.11"
# dependencies = ["akshare", "pandas", "numpy", "requests"]
# ///
"""Read-only independent live provider probe, storing evidence outside formal outputs."""
from datetime import date, datetime
import json
from pathlib import Path
import sys
import time
from concurrent.futures import ThreadPoolExecutor

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from scripts import index_history_preflight as h

start, end = date(2026, 8, 15), date(2026, 9, 4)
sessions = h.exchange_calendar.sessions_for_day(end)
results = {"observed_at": datetime.now().astimezone().isoformat(), "instrument": "sh000852",
           "purpose": "preflight only, never overwrite formal prices", "sources": []}
valid = {}
for name, provider in (("sina_static", h.sina_static), ("tencent", h.tencent)):
    began = time.monotonic()
    try:
        frame = h.validate_history(provider(start, end), start, end, sessions)
        frame.to_csv(OUT / f"{name}.csv", index=False)
        valid[name] = frame.set_index("date").close
        result = dict(source=name, ok=True, rows=len(frame), first=str(frame.date.iloc[0].date()),
                      last=str(frame.date.iloc[-1].date()))
    except Exception as exc:
        result = dict(source=name, ok=False, error=f"{type(exc).__name__}: {exc}")
    result["elapsed_seconds"] = round(time.monotonic()-began, 3)
    results["sources"].append(result)
    print(json.dumps(result), flush=True)
if len(valid) == 2:
    results["max_close_difference_points"] = float((valid["sina_static"]-valid["tencent"]).abs().max())
def probe_other(item):
    name, url, params = item
    began = time.monotonic()
    result = dict(source=name, selected=False)
    try:
        response = h._get(url, params=params)
        payload = response.json()
        (OUT / f"{name}_response.json").write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        result.update(http_status=response.status_code, received=True)
        if name == "csindex":
            days = [datetime.strptime(row["tradeDate"], "%Y%m%d").date() for row in payload["data"]]
            result["non_session_dates"] = [str(day) for day in days if day not in sessions]
            result["rows"] = len(days)
            result["reason_not_selected"] = "Non-session rows present; quarantine rather than silently clean"
    except Exception as exc:
        result.update(received=False, error=f"{type(exc).__name__}: {exc}")
    result["elapsed_seconds"] = round(time.monotonic()-began, 3)
    return result
others = [
    ("eastmoney", "https://push2his.eastmoney.com/api/qt/stock/kline/get",
     dict(secid="1.000852", fields1="f1,f2,f3,f4,f5", fields2="f51,f52,f53,f54,f55,f56,f57,f58", klt=101, fqt=0, beg="20260815", end="20260904")),
    ("sina_legacy", "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData",
     dict(symbol="sh000852", scale=240, ma="no", datalen=40)),
    ("csindex", "https://www.csindex.com.cn/csindex-home/perf/index-perf",
     dict(indexCode="000852", startDate="20260815", endDate="20260904")),
]
with ThreadPoolExecutor(max_workers=3) as pool:
    for result in pool.map(probe_other, others):
        results["sources"].append(result)
        print(json.dumps(result), flush=True)
(OUT / "source_probe.json").write_text(json.dumps(results, indent=2), encoding="utf-8")
