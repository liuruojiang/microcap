from __future__ import annotations

import ssl
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parent
CACHE_DIR = ROOT / ".cboe_cache"

SOURCES = {
    "VIX_History.csv": "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv",
    "VIX9D_History.csv": "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX9D_History.csv",
}


def main() -> None:
    CACHE_DIR.mkdir(exist_ok=True)
    ssl_ctx = ssl.create_default_context()
    for name, url in SOURCES.items():
        target = CACHE_DIR / name
        with urllib.request.urlopen(url, context=ssl_ctx) as resp:
            target.write_bytes(resp.read())
        print(f"saved {target}")


if __name__ == "__main__":
    main()
