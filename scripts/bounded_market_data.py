"""Bound slow SDK calls in a disposable process, including SDK-internal retries."""
from __future__ import annotations

import json
import math
import subprocess
import sys

import pandas as pd

PROVIDERS = frozenset({"stock_zh_a_spot", "stock_zh_a_spot_em", "stock_zh_a_st_em"})
WORKER = r'''
import contextlib,json,sys
with contextlib.redirect_stdout(sys.stderr):
    import requests
    original=requests.sessions.Session.request
    def bounded(self,method,url,**kwargs):
        if kwargs.get("timeout") is None:
            kwargs["timeout"]=(5,15)
        return original(self,method,url,**kwargs)
    requests.sessions.Session.request=bounded
    import akshare
    frame=getattr(akshare,sys.argv[1])()
sys.stdout.write(frame.to_json(orient="split",force_ascii=False))
'''


def fetch_akshare_frame(provider: str, *, timeout: float = 90) -> pd.DataFrame:
    if provider not in PROVIDERS or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("Invalid bounded market-data provider or deadline")
    try:
        result = subprocess.run([sys.executable, "-B", "-X", "utf8", "-c", WORKER, provider],
                                capture_output=True, text=True, encoding="utf-8", timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        raise TimeoutError(f"{provider} exceeded its {timeout:g}s total deadline") from exc
    if result.returncode:
        raise RuntimeError(f"{provider} failed: {result.stderr[-1200:]}")
    payload = json.loads(result.stdout)
    frame = pd.DataFrame(payload["data"], columns=payload["columns"], index=payload["index"])
    if frame.empty or frame.columns.duplicated().any():
        raise ValueError(f"{provider} returned an empty or invalid table")
    return frame
