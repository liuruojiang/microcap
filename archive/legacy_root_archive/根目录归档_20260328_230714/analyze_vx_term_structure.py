from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent
US_DATA = ROOT / "mnt_strategy_data_us.csv"
VX_CONT_CSV = ROOT / "vx_continuous.csv"
VX_CONTRACT_DIR = ROOT / ".cboe_cache" / "vx_contracts"
OUT_CSV = ROOT / "vx_term_structure_scan_results.csv"
OUT_MD = ROOT / "mnt_vx_term_structure_scan_20260328.md"

TRADING_DAYS = 252
FEE_BPS = 2.0


def calc_daily_metrics(ret: pd.Series) -> dict[str, float]:
    ret = pd.Series(ret).dropna()
    if len(ret) == 0:
        return {"annual": np.nan, "vol": np.nan, "sharpe": np.nan, "max_dd": np.nan, "total": np.nan}
    nav = (1.0 + ret).cumprod()
    total = nav.iloc[-1] - 1.0
    years = len(ret) / TRADING_DAYS
    annual = nav.iloc[-1] ** (1.0 / years) - 1.0 if years > 0 else np.nan
    vol = ret.std(ddof=0) * math.sqrt(TRADING_DAYS)
    sharpe = annual / vol if vol and not np.isnan(vol) and vol > 0 else np.nan
    max_dd = ((nav - nav.cummax()) / nav.cummax()).min()
    return {"annual": annual, "vol": vol, "sharpe": sharpe, "max_dd": max_dd, "total": total}


def load_contract_prices() -> dict[str, pd.Series]:
    mapping: dict[str, pd.Series] = {}
    for path in VX_CONTRACT_DIR.glob("VX_*.csv"):
        expiry = path.stem.replace("VX_", "")
        df = pd.read_csv(path, usecols=["Trade Date", "Settle"])
        df["Trade Date"] = pd.to_datetime(df["Trade Date"])
        df["Settle"] = pd.to_numeric(df["Settle"], errors="coerce")
        series = df.set_index("Trade Date")["Settle"].sort_index()
        series = series[series > 0]
        mapping[expiry] = series
    return mapping


def one_day_contract_return(contract_prices: dict[str, pd.Series], expiry: str, dt0: pd.Timestamp, dt1: pd.Timestamp) -> float | None:
    series = contract_prices.get(expiry)
    if series is None or dt0 not in series.index or dt1 not in series.index:
        return None
    p0 = float(series.loc[dt0])
    p1 = float(series.loc[dt1])
    if not np.isfinite(p0) or not np.isfinite(p1) or p0 <= 0:
        return None
    return p1 / p0 - 1.0


def run_directional(vx: pd.DataFrame,
                    contract_prices: dict[str, pd.Series],
                    bil_ret: pd.Series,
                    low_ratio: float,
                    high_ratio: float,
                    use_slope: bool) -> tuple[pd.Series, pd.Series]:
    raw = pd.Series(0.0, index=vx.index)
    slope = vx["VX1"].diff(3)
    long_mask = vx["ratio_vx1_vx2"] >= high_ratio
    short_mask = vx["ratio_vx1_vx2"] <= low_ratio
    if use_slope:
        long_mask &= slope > 0
        short_mask &= slope < 0
    raw = raw.mask(long_mask, 1.0)
    raw = raw.mask(short_mask, -1.0)
    signal = raw.shift(1).fillna(0.0)

    out = pd.Series(0.0, index=vx.index, dtype=float)
    bil_aligned = bil_ret.reindex(vx.index).fillna(0.0)
    for i in range(len(vx.index) - 1):
        dt0 = vx.index[i]
        dt1 = vx.index[i + 1]
        sig = float(signal.iloc[i])
        if sig == 0.0:
            out.iloc[i + 1] = bil_aligned.iloc[i + 1]
            continue
        ret = one_day_contract_return(contract_prices, str(vx["VX1_expiry"].iloc[i]), dt0, dt1)
        if ret is None:
            out.iloc[i + 1] = bil_aligned.iloc[i + 1]
            continue
        out.iloc[i + 1] = bil_aligned.iloc[i + 1] + sig * ret
    trade_change = (raw != raw.shift(1)).fillna(False).astype(float) * (FEE_BPS / 10000.0)
    out = out - trade_change
    return out, raw


def run_calendar(vx: pd.DataFrame,
                 contract_prices: dict[str, pd.Series],
                 bil_ret: pd.Series,
                 low_ratio: float,
                 high_ratio: float,
                 use_slope: bool) -> tuple[pd.Series, pd.Series]:
    raw = pd.Series(0.0, index=vx.index)
    spread = vx["VX2"] - vx["VX1"]
    spread_slope = spread.diff(3)
    long_mask = vx["ratio_vx1_vx2"] >= high_ratio
    short_mask = vx["ratio_vx1_vx2"] <= low_ratio
    if use_slope:
        long_mask &= spread_slope < 0
        short_mask &= spread_slope > 0
    raw = raw.mask(long_mask, 1.0)   # long VX1 short VX2 in backwardation
    raw = raw.mask(short_mask, -1.0) # short VX1 long VX2 in contango
    signal = raw.shift(1).fillna(0.0)

    out = pd.Series(0.0, index=vx.index, dtype=float)
    bil_aligned = bil_ret.reindex(vx.index).fillna(0.0)
    for i in range(len(vx.index) - 1):
        dt0 = vx.index[i]
        dt1 = vx.index[i + 1]
        sig = float(signal.iloc[i])
        if sig == 0.0:
            out.iloc[i + 1] = bil_aligned.iloc[i + 1]
            continue
        ret1 = one_day_contract_return(contract_prices, str(vx["VX1_expiry"].iloc[i]), dt0, dt1)
        ret2 = one_day_contract_return(contract_prices, str(vx["VX2_expiry"].iloc[i]), dt0, dt1)
        if ret1 is None or ret2 is None:
            out.iloc[i + 1] = bil_aligned.iloc[i + 1]
            continue
        out.iloc[i + 1] = bil_aligned.iloc[i + 1] + sig * (ret1 - ret2)
    trade_change = (raw != raw.shift(1)).fillna(False).astype(float) * (FEE_BPS / 10000.0)
    out = out - trade_change
    return out, raw


def main() -> None:
    vx = pd.read_csv(VX_CONT_CSV, parse_dates=["date"]).set_index("date").sort_index()
    us = pd.read_csv(US_DATA, parse_dates=["date"]).set_index("date").sort_index()
    bil_ret = us["BIL"].pct_change(fill_method=None).fillna(0.0)
    contract_prices = load_contract_prices()
    data = vx.join(bil_ret.rename("BIL_RET"), how="inner").dropna(subset=["VX1", "VX2", "ratio_vx1_vx2"])

    rows = []
    for mode in ["directional", "calendar"]:
        for low_ratio, high_ratio in [(0.93, 0.98), (0.94, 0.99), (0.95, 1.00), (0.96, 1.01)]:
            for use_slope in [False, True]:
                if mode == "directional":
                    strat_ret, signal = run_directional(data, contract_prices, data["BIL_RET"], low_ratio, high_ratio, use_slope)
                else:
                    strat_ret, signal = run_calendar(data, contract_prices, data["BIL_RET"], low_ratio, high_ratio, use_slope)
                m = calc_daily_metrics(strat_ret)
                rows.append({
                    "mode": mode,
                    "low_ratio": low_ratio,
                    "high_ratio": high_ratio,
                    "use_slope": use_slope,
                    "annual": m["annual"],
                    "vol": m["vol"],
                    "sharpe": m["sharpe"],
                    "max_dd": m["max_dd"],
                    "total": m["total"],
                    "active_days": int((signal != 0).sum()),
                    "long_days": int((signal > 0).sum()),
                    "short_days": int((signal < 0).sum()),
                    "trades": int((signal != signal.shift(1)).sum()),
                })

    res = pd.DataFrame(rows).sort_values(["sharpe", "annual"], ascending=[False, False]).reset_index(drop=True)
    res.to_csv(OUT_CSV, index=False)

    top = res.head(15)
    with OUT_MD.open("w", encoding="utf-8") as f:
        f.write("# VX1/VX2 Term Structure Scan\n\n")
        f.write("Data source: official Cboe VX monthly contract CSVs, stitched into daily VX1/VX2 continuous series.\n")
        f.write("Two prototypes are tested: directional VX1 and VX1-VX2 calendar spread.\n\n")
        f.write("## Top 15\n\n")
        f.write("| rank | mode | low_ratio | high_ratio | use_slope | annual | vol | sharpe | max_dd | active_days | long_days | short_days |\n")
        f.write("|---:|:---:|---:|---:|:---:|---:|---:|---:|---:|---:|---:|---:|\n")
        for i, row in top.iterrows():
            f.write(
                f"| {i+1} | {row['mode']} | {row['low_ratio']:.2f} | {row['high_ratio']:.2f} | "
                f"{bool(row['use_slope'])} | {row['annual']:.2%} | {row['vol']:.2%} | {row['sharpe']:.2f} | "
                f"{row['max_dd']:.2%} | {int(row['active_days'])} | {int(row['long_days'])} | {int(row['short_days'])} |\n"
            )

    print(f"saved {OUT_CSV}")
    print(f"saved {OUT_MD}")
    print(res.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
