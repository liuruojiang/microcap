"""Explicit synthetic provider clocks for unrelated realtime contract tests.

These are not downloaded quotes or freshness evidence. Real quote validation is
left enabled; callers freeze their own test clock at the declared snapshot.
"""
import pandas as pd


def quote_integrity_fixture(base, snapshot_time, count=100):
    stamp = pd.Timestamp(snapshot_time)
    stamp = stamp.tz_localize("Asia/Shanghai") if stamp.tz is None else stamp.tz_convert("Asia/Shanghai")
    quoted = stamp - pd.Timedelta(seconds=30)
    codes = [f"{number:06d}" for number in range(1, count + 1)]
    frame = pd.DataFrame({"code": codes, "rt_price": 10.0,
                          "trade_date": str(stamp.date()), "quoted_at": quoted.isoformat(),
                          "fetched_at": stamp.isoformat(), "price_origin": "provider_latest"}).set_index("code")
    return {**base.extract_member_quote_integrity_stats(frame, codes, stamp),
            "hedge_quoted_at": quoted.isoformat(), "hedge_fetched_at": stamp.isoformat(),
            "hedge_price_origin": "provider_latest"}
