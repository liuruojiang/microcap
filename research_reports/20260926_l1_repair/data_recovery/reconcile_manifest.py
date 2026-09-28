"""Replace earlier price columns with 2010-01-04 refresh and validate readback."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd


HERE = Path(__file__).resolve().parent


def main() -> None:
    combined_path = HERE / "source_manifest_both.csv"
    combined = pd.read_csv(combined_path, dtype={"symbol": str})
    refreshed = pd.read_csv(HERE / "source_manifest_prices_raw.csv", dtype={"symbol": str})
    assert len(combined) == len(refreshed) == 254
    assert combined["symbol"].is_unique and refreshed["symbol"].is_unique
    assert set(combined["symbol"]) == set(refreshed["symbol"])
    combined = combined.set_index("symbol")
    refreshed = refreshed.set_index("symbol")
    for field in [c for c in refreshed.columns if c.startswith("prices_raw_")]:
        combined[field] = refreshed[field]
    combined = combined.reset_index().sort_values("symbol")
    matched = 0
    for row in combined.itertuples(index=False):
        for kind in ("prices_raw", "share_change"):
            if getattr(row, kind + "_status") != "fetched":
                continue
            path = HERE / kind / f"{row.symbol}.csv"
            assert path.exists()
            expected = getattr(row, kind + "_sha256")
            assert hashlib.sha256(path.read_bytes()).hexdigest() == expected, path
            matched += 1
    temp = combined_path.with_suffix(".tmp")
    combined.to_csv(temp, index=False, encoding="utf-8")
    temp.replace(combined_path)
    result = {"symbols": len(combined), "price_fetched": int(combined["prices_raw_status"].eq("fetched").sum()), "share_fetched": int(combined["share_change_status"].eq("fetched").sum()), "file_hashes_matched": matched, "price_start": "2010-01-04", "price_failures": combined.loc[combined["prices_raw_status"].ne("fetched"), "symbol"].tolist()}
    (HERE / "manifest_verification.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
