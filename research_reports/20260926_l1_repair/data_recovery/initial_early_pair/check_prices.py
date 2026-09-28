"""Independently query Sohu unadjusted bars and recompute two-date caps."""

import csv
import hashlib
import json
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import requests


HERE = Path(__file__).resolve().parent
DATA = HERE.parent
SHARES_FROM_ISSUER_PDFS = {
    "002071": 122_680_000,
    "600070": 140_675_760,
    "600355": 246_044_600,
    "600856": 234_831_569,
    "600466": 175_602_342,
    "000502": 184_819_607,
}
DATES = ("2010-01-04", "2010-01-14")


def main():
    seed = list(csv.DictReader((DATA / "initial_formal_seed_caps.csv").open(encoding="utf-8-sig")))
    cutoff = {DATES[0]: Decimal(seed[-1]["market_cap"])}
    member_path = Path("outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv")
    members = list(csv.DictReader(member_path.open(encoding="utf-8-sig")))
    cutoff[DATES[1]] = Decimal(next(row["market_cap"] for row in members if row["rebalance_date"] == DATES[1] and row["rank"] == "100"))
    out = []
    for symbol, shares in SHARES_FROM_ISSUER_PDFS.items():
        url = f"https://q.stock.sohu.com/hisHq?code=cn_{symbol}&start=20091228&end=20100115&stat=1&order=A&period=d"
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        raw = response.content
        parsed = json.loads(raw.decode("gbk"))[0]
        if parsed["status"] != 0:
            raise RuntimeError((symbol, parsed))
        bars = {bar[0]: bar for bar in parsed["hq"]}
        dates = sorted(bars)
        for date in DATES:
            bar = bars[date]
            previous = bars[dates[dates.index(date)-1]]
            prev_close = Decimal(previous[2])
            close = Decimal(bar[2])
            low, high, volume = Decimal(bar[5]), Decimal(bar[6]), Decimal(bar[7])
            limit_low = (prev_close * Decimal("0.9")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            limit_high = (prev_close * Decimal("1.1")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            cap = close * shares
            out.append({
                "symbol": symbol, "date": date, "issuer_shares": shares,
                "previous_traded_date": previous[0], "previous_close": previous[2],
                "open": bar[1], "close": bar[2], "low": bar[5], "high": bar[6], "volume_lots": bar[7],
                "limit_low": limit_low, "limit_high": limit_high,
                "market_cap": cap, "old_rank100_cap": cutoff[date],
                "below_old_rank100": cap < cutoff[date],
                "paper_tradable": volume > 0 and high > low and limit_low < close < limit_high,
                "sohu_url": url, "sohu_response_sha256": hashlib.sha256(raw).hexdigest(),
            })
    with (HERE / "independent_price_check.csv").open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(out[0]))
        writer.writeheader()
        writer.writerows(out)
    for row in out:
        print(row["symbol"], row["date"], row["market_cap"], row["below_old_rank100"], row["paper_tradable"])


if __name__ == "__main__":
    main()
