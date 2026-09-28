import json
from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
import analyze_top100_rebalance_frequency as f

day = pd.Timestamp("2026-09-17")
for symbol in ["000995", "301192"]:
    path = f.resolve_cache_path(f.PRICE_DIR, f.SHARED_PRICE_DIR, symbol)
    price = pd.read_csv(path)
    price["date"] = pd.to_datetime(price["date"])
    price = price.sort_values("date")
    prior = price.loc[price["date"].lt(day)].iloc[-1]
    today = price.loc[price["date"].eq(day)].iloc[0]
    meta = json.loads(f.resolve_security_meta_path(symbol).read_text(encoding="utf-8"))
    is_st = bool(f.build_st_status_series(meta, pd.DatetimeIndex([day])).iloc[0])
    print(symbol, "prior", prior["date"], prior["close_raw"], "today", today["close_raw"], "st", is_st, "ratio", f.get_price_limit_ratio(symbol, day, is_st=is_st), "blocks", f.detect_close_limit_blocks(symbol, day, prior["close_raw"], today["close_raw"], is_st=is_st))
    print(price.loc[price["date"].between(pd.Timestamp("2026-09-14"), pd.Timestamp("2026-09-24"))].to_string(index=False))
