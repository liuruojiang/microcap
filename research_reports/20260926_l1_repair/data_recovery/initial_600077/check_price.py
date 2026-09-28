"""Independent two-date 600077 unadjusted-price and paper limit check."""

import csv
import hashlib
import json
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import requests


HERE = Path(__file__).resolve().parent
DATA = HERE.parent
SHARES = Decimal(159_118_417)  # issuer 2008 annual and 2009 Q3 originals
URL = "https://q.stock.sohu.com/hisHq?code=cn_600077&start=20091228&end=20100115&stat=1&order=A&period=d"


def main():
    response = requests.get(URL, timeout=20)
    response.raise_for_status()
    body = response.content
    parsed = json.loads(body.decode("gbk"))[0]
    if parsed["status"] != 0:
        raise RuntimeError(parsed)
    bars = {bar[0]: bar for bar in parsed["hq"]}
    dates = sorted(bars)
    old_seed = list(csv.DictReader((DATA / "initial_formal_seed_caps.csv").open(encoding="utf-8-sig")))
    members = list(csv.DictReader(Path("outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv").open(encoding="utf-8-sig")))
    cutoffs = {
        "2010-01-04": Decimal(old_seed[-1]["market_cap"]),
        "2010-01-14": Decimal(next(r["market_cap"] for r in members if r["rebalance_date"] == "2010-01-14" and r["rank"] == "100")),
    }
    rows = []
    for date in cutoffs:
        b = bars[date]
        previous = bars[dates[dates.index(date) - 1]]
        close = Decimal(b[2])
        prior = Decimal(previous[2])
        low, high, volume = Decimal(b[5]), Decimal(b[6]), Decimal(b[7])
        lower = (prior * Decimal("0.9")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        upper = (prior * Decimal("1.1")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        cap = SHARES * close
        rows.append({
            "symbol": "600077", "date": date, "shares": SHARES,
            "previous_traded_date": previous[0], "previous_close": prior,
            "open": b[1], "close": close, "low": low, "high": high,
            "volume_lots": volume, "limit_low": lower, "limit_high": upper,
            "market_cap": cap, "old_rank100_cap": cutoffs[date],
            "below_old_rank100": cap < cutoffs[date],
            "paper_tradable": volume > 0 and high > low and lower < close < upper,
            "source_url": URL, "response_sha256": hashlib.sha256(body).hexdigest(),
        })
    with (HERE / "price_check.csv").open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    for row in rows:
        print(row["date"], row["market_cap"], row["old_rank100_cap"], row["paper_tradable"])


if __name__ == "__main__":
    main()
