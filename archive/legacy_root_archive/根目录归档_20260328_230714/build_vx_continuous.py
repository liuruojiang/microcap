from __future__ import annotations

import json
import ssl
import urllib.request
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parent
CACHE_DIR = ROOT / ".cboe_cache"
VX_DIR = CACHE_DIR / "vx_contracts"
PRODUCT_LIST_JSON = CACHE_DIR / "vx_product_list.json"
VX1_CSV = ROOT / "vx_continuous.csv"

PRODUCT_LIST_URL = "https://www-api.cboe.com/us/futures/market_statistics/historical_data/product/list/VX/"
CDN_PREFIX = "https://cdn.cboe.com/"


def download_text(url: str) -> bytes:
    ssl_ctx = ssl.create_default_context()
    with urllib.request.urlopen(url, context=ssl_ctx) as resp:
        return resp.read()


def ensure_product_list() -> dict:
    CACHE_DIR.mkdir(exist_ok=True)
    if PRODUCT_LIST_JSON.exists():
        return json.loads(PRODUCT_LIST_JSON.read_text(encoding="utf-8"))
    payload = download_text(PRODUCT_LIST_URL)
    PRODUCT_LIST_JSON.write_bytes(payload)
    return json.loads(payload.decode("utf-8"))


def fetch_contract_csv(path: str) -> Path:
    VX_DIR.mkdir(parents=True, exist_ok=True)
    name = Path(path).name
    target = VX_DIR / name
    if not target.exists():
        target.write_bytes(download_text(CDN_PREFIX + path))
    return target


def load_contract(contract_meta: dict) -> pd.DataFrame:
    path = fetch_contract_csv(contract_meta["path"])
    df = pd.read_csv(path)
    df["Trade Date"] = pd.to_datetime(df["Trade Date"])
    df["Settle"] = pd.to_numeric(df["Settle"], errors="coerce")
    df["Open Interest"] = pd.to_numeric(df["Open Interest"], errors="coerce")
    df["Total Volume"] = pd.to_numeric(df["Total Volume"], errors="coerce")
    df = df[["Trade Date", "Settle", "Open Interest", "Total Volume"]].copy()
    df["contract_dt"] = pd.to_datetime(contract_meta["contract_dt"])
    df["contract_name"] = contract_meta["product_display"]
    return df


def main() -> None:
    payload = ensure_product_list()
    contracts = []
    for year, items in payload.items():
        for item in items:
            if item.get("duration_type") == "M":
                contracts.append(item)
    contracts = sorted(contracts, key=lambda x: x["contract_dt"])

    contract_frames = [load_contract(item) for item in contracts]
    panel = pd.concat(contract_frames, ignore_index=True)
    panel = panel.dropna(subset=["Trade Date", "Settle", "contract_dt"])
    panel = panel[panel["Settle"] > 0]
    panel = panel.sort_values(["Trade Date", "contract_dt"])
    panel = panel[panel["Trade Date"] <= panel["contract_dt"]]

    rows = []
    for trade_date, grp in panel.groupby("Trade Date"):
        elig = grp.sort_values("contract_dt")
        if len(elig) < 2:
            continue
        front = elig.iloc[0]
        second = elig.iloc[1]
        rows.append({
            "date": trade_date,
            "VX1": float(front["Settle"]),
            "VX2": float(second["Settle"]),
            "VX1_contract": front["contract_name"],
            "VX2_contract": second["contract_name"],
            "VX1_expiry": pd.Timestamp(front["contract_dt"]).strftime("%Y-%m-%d"),
            "VX2_expiry": pd.Timestamp(second["contract_dt"]).strftime("%Y-%m-%d"),
            "VX1_oi": float(front["Open Interest"]) if pd.notna(front["Open Interest"]) else None,
            "VX2_oi": float(second["Open Interest"]) if pd.notna(second["Open Interest"]) else None,
            "VX1_vol": float(front["Total Volume"]) if pd.notna(front["Total Volume"]) else None,
            "VX2_vol": float(second["Total Volume"]) if pd.notna(second["Total Volume"]) else None,
            "ratio_vx1_vx2": float(front["Settle"]) / float(second["Settle"]) if float(second["Settle"]) > 0 else None,
            "spread_vx2_vx1": float(second["Settle"]) - float(front["Settle"]),
        })

    out = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
    out.to_csv(VX1_CSV, index=False)
    print(f"saved {VX1_CSV}")
    print(out.head(5).to_string(index=False))
    print(out.tail(5).to_string(index=False))


if __name__ == "__main__":
    main()
