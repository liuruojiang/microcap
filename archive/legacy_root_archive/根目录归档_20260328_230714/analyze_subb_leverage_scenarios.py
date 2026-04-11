import importlib.util
import json
import sys
import types
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent
MAIN_SCRIPT = ROOT / "mnt_bot plus 1 .py"
US_DATA_CSV = ROOT / "mnt_strategy_data_us.csv"
CN_DATA_CSV = ROOT / "mnt_strategy_data_cn.csv"


def load_main_module():
    class BotError(Exception):
        pass

    class DummySettingsResponse:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

    poe = types.SimpleNamespace(
        BotError=BotError,
        start_message=lambda: None,
        default_chat=None,
        query=types.SimpleNamespace(text="", attachments=[]),
        call=lambda *args, **kwargs: None,
        update_settings=lambda *args, **kwargs: None,
    )

    fastapi_poe = types.ModuleType("fastapi_poe")
    fastapi_poe_types = types.ModuleType("fastapi_poe.types")
    fastapi_poe_types.SettingsResponse = DummySettingsResponse
    sys.modules["fastapi_poe"] = fastapi_poe
    sys.modules["fastapi_poe.types"] = fastapi_poe_types

    spec = importlib.util.spec_from_file_location("mntbot", MAIN_SCRIPT)
    module = importlib.util.module_from_spec(spec)
    module.poe = poe
    spec.loader.exec_module(module)
    return module


def load_us_rotation_close():
    us = pd.read_csv(US_DATA_CSV, parse_dates=["date"]).set_index("date").sort_index()
    emxc = us["EMXC_spliced"].combine_first(us["EMXC"]).combine_first(us["EEM"])

    close = pd.DataFrame(index=us.index)
    close["QQQ"] = us["QQQ"]
    close["EMXC"] = emxc
    close["EFA"] = us["EFA"]
    close["GLD"] = us["GLD"]
    close["TLT"] = us["TLT"]
    close["DBC"] = us["DBC"]
    close["BTC-USD"] = us["BTC-USD"]
    close["BIL"] = us["BIL"]
    close["SPY"] = us["SPY"]

    required = ["QQQ", "EMXC", "EFA", "GLD", "TLT", "DBC", "BIL", "SPY"]
    close = close.ffill().dropna(subset=required)

    stock_cols = ["QQQ", "EMXC", "EFA", "GLD", "TLT", "DBC", "SPY"]
    last_stock_date = max(close[col].dropna().index[-1] for col in stock_cols)
    return close.loc[:last_stock_date]


def _load_close_csv(path):
    return pd.read_csv(path, parse_dates=["date"]).set_index("date").sort_index()["close"]


def load_cn_data():
    cn_raw = pd.read_csv(CN_DATA_CSV, parse_dates=["date"]).set_index("date").sort_index()

    cn_close = pd.concat(
        [
            _load_close_csv(ROOT / ".cn_official_cache" / "1_H20955.csv").rename("1.H20955"),
            _load_close_csv(ROOT / ".cn_official_cache" / "0_399606.csv").rename("0.399606"),
            _load_close_csv(ROOT / ".cn_official_cache" / "1_H00016.csv").rename("1.H00016"),
            _load_close_csv(ROOT / ".cn_official_cache" / "1_H00852.csv").rename("1.H00852"),
            _load_close_csv(ROOT / ".cn_official_cache" / "1_H00905.csv").rename("1.H00905"),
        ],
        axis=1,
    ).ffill().dropna()

    bond = _load_close_csv(ROOT / ".cn_official_cache" / "1_H11077.csv").rename("1.H11077")
    cn_close_with_bond = pd.concat([cn_close, bond], axis=1).ffill().dropna()

    cn_dk_close = pd.DataFrame(index=cn_raw.index)
    cn_dk_close["DK_ZZ1000"] = cn_raw["1.000852"]
    cn_dk_close["DK_SZ50"] = cn_raw["1.000016"]
    cn_dk_close["DK_HS300"] = cn_raw["1.000300"]
    cn_dk_close["DK_ZZ500"] = cn_raw["1.000905"]
    cn_dk_close["DK_CYB"] = cn_raw["0.399006"]
    cn_dk_close = cn_dk_close.ffill().dropna()

    return cn_close, cn_close_with_bond, cn_dk_close


def load_us_prod_daily(mod):
    us = pd.read_csv(US_DATA_CSV, parse_dates=["date"]).set_index("date").sort_index()
    emxc = us["EMXC_spliced"].combine_first(us["EMXC"]).combine_first(us["EEM"])

    us_raw = {}
    for ticker in [
        "QQQ",
        "EMXC",
        "EFA",
        "GLD",
        "TLT",
        "DBC",
        "BTC-USD",
        "BIL",
        "SPY",
        "VTI",
        "VGIT",
        "DBMF",
        "VEA",
        "QQQM",
        "GLDM",
        "VGLT",
        "PDBC",
        "IBIT",
        "EEM",
    ]:
        if ticker == "EMXC":
            ser = emxc
        elif ticker in us.columns:
            ser = us[ticker]
        else:
            continue
        us_raw[ticker] = pd.DataFrame({"close": ser}).dropna()

    rot_tickers = list(mod.US_ROT_POOL)
    late_rot = {"BTC-USD", "EMXC"}
    rot_core = [t for t in rot_tickers if t not in late_rot]
    if "EMXC" in mod.US_ROT_POOL and mod.US_ROT_EMXC_BT_PROXY not in rot_core and mod.US_ROT_EMXC_BT_PROXY in us_raw:
        rot_core.append(mod.US_ROT_EMXC_BT_PROXY)

    us_rot_close = pd.concat(
        [us_raw[t][["close"]].rename(columns={"close": t}) for t in rot_core if t in us_raw],
        axis=1,
    ).ffill().dropna()

    if "EMXC" in mod.US_ROT_POOL and mod.US_ROT_EMXC_BT_PROXY in us_raw:
        eem_col = us_rot_close[mod.US_ROT_EMXC_BT_PROXY].copy() if mod.US_ROT_EMXC_BT_PROXY in us_rot_close.columns else None
        emxc_raw = us_raw.get("EMXC")
        if eem_col is not None:
            hybrid = eem_col.rename("EMXC")
            if emxc_raw is not None and len(emxc_raw) > 0:
                emxc_ser = emxc_raw["close"].reindex(hybrid.index)
                switch_idx = hybrid.index >= mod.US_ROT_EMXC_BT_START
                first_emxc_date = emxc_ser.loc[switch_idx].first_valid_index() if switch_idx.any() else None
                if first_emxc_date is not None:
                    scale_factor = hybrid.loc[first_emxc_date] / emxc_ser.loc[first_emxc_date]
                    hybrid.loc[switch_idx] = emxc_ser.loc[switch_idx] * scale_factor
            us_rot_close["EMXC"] = hybrid
            if mod.US_ROT_EMXC_BT_PROXY in us_rot_close.columns and mod.US_ROT_EMXC_BT_PROXY not in mod.US_ROT_POOL:
                us_rot_close = us_rot_close.drop(columns=[mod.US_ROT_EMXC_BT_PROXY])

    for t in late_rot:
        if t == "EMXC":
            continue
        if t in us_raw:
            us_rot_close = us_rot_close.join(us_raw[t][["close"]].rename(columns={"close": t}), how="left")

    prod_proxies = list(set([cfg["proxy"] for cfg in mod.PROD_PORTFOLIO.values()] + [mod.PROD_CASH]))
    late_prod = {"BTC-USD", "DBMF"}
    prod_core = [t for t in prod_proxies if t not in late_prod]
    us_prod_daily = pd.concat(
        [us_raw[t][["close"]].rename(columns={"close": t}) for t in prod_core if t in us_raw],
        axis=1,
    ).ffill().dropna()
    for t in late_prod:
        if t in us_raw:
            us_prod_daily = us_prod_daily.join(us_raw[t][["close"]].rename(columns={"close": t}), how="left")

    if "SPY" not in us_rot_close.columns and "SPY" in us_raw:
        us_rot_close["SPY"] = us_raw["SPY"]["close"].reindex(us_rot_close.index)

    stock_rot = [t for t in rot_tickers if t in us_raw and t != "BTC-USD"]
    if stock_rot:
        last_stock_date = max(us_raw[t].index[-1] for t in stock_rot)
        us_rot_close = us_rot_close.loc[:last_stock_date]

    stock_prod = [t for t in prod_proxies if t in us_raw and t != "BTC-USD"]
    if stock_prod:
        last_prod_date = max(us_raw[t].index[-1] for t in stock_prod)
        us_prod_daily = us_prod_daily.loc[:last_prod_date]

    return us_rot_close, us_prod_daily


def run_rotation_variant(
    mod,
    close_df,
    *,
    target_vol,
    max_lev,
    leverage_mode,
    finance_spread_bps=0.0,
):
    df = close_df.copy()
    btc_ticker = mod.US_ROT_BTC_TICKER
    btc_start = mod.US_ROT_BTC_START
    btc_max_w = mod.US_ROT_BTC_MAX_W

    if btc_ticker and btc_start is not None and btc_ticker in df.columns:
        df.loc[df.index < btc_start, btc_ticker] = np.nan

    ranking_codes = list(mod.US_ROT_POOL)
    momentum = df.div(df.shift(mod.US_ROT_LB)).sub(1.0)
    vol_df = df.pct_change().rolling(mod.US_ROT_VOL_LB).std() * np.sqrt(mod.US_TRADING_DAYS)
    start_idx = max(mod.US_ROT_LB, mod.US_ROT_VOL_LB, mod.US_ROT_VOL_WINDOW) + 1
    signal_days = mod._us_signal_days(df, start_idx)

    act = {"BIL": 1.0}
    rows = []
    hist = []
    spread_daily = finance_spread_bps / 10000.0 / mod.US_TRADING_DAYS
    w_assets = ranking_codes + (["BIL"] if "BIL" not in ranking_codes else [])

    for i in range(start_idx, len(df)):
        is_sig = i in signal_days
        scale = 1.0
        comm = 0.0
        rebalanced = False

        if len(hist) >= mod.US_ROT_VOL_WINDOW:
            rv = np.std(hist[-mod.US_ROT_VOL_WINDOW :], ddof=1) * np.sqrt(mod.US_TRADING_DAYS)
            scale = min(max(target_vol / rv, 0.05), max_lev) if rv > 0.001 else max_lev

        old_act = dict(act)

        if is_sig:
            prev_risky = set()
            if rows:
                for asset in w_assets:
                    if asset != "BIL" and rows[-1].get(f"w_{asset}", 0.0) > 0.001:
                        prev_risky.add(asset)

            raw_w = mod._us_raw_weights(
                momentum.iloc[i],
                vol_df.iloc[i],
                ranking_codes,
                3,
                mod.US_ROT_ABS_THRESHOLD,
                prev_risky=prev_risky if prev_risky else None,
                threshold=mod.US_ROT_REBALANCE_THRESHOLD,
            )

            if leverage_mode == "futures_only":
                new_act = mod._us_model_b(raw_w, scale)
            elif leverage_mode == "all_assets":
                new_act = {
                    asset: weight * scale
                    for asset, weight in raw_w.items()
                    if asset != "BIL"
                }
                risky = sum(new_act.values())
                new_act["BIL"] = max(1.0 - risky, 0.0)
            else:
                raise ValueError(f"Unknown leverage_mode: {leverage_mode}")

            if btc_max_w is not None and btc_ticker:
                new_act = mod._apply_btc_cap(new_act, btc_ticker, btc_max_w)

            prev_a = {asset: rows[-1].get(f"w_{asset}", 0.0) for asset in w_assets} if rows else {"BIL": 1.0}
            all_assets = set(new_act) | set(prev_a)
            turnover = sum(abs(new_act.get(asset, 0.0) - prev_a.get(asset, 0.0)) for asset in all_assets if asset != "BIL")
            if turnover >= mod.US_ROT_MIN_TURNOVER:
                if turnover > 0:
                    comm = turnover * mod.US_ROT_COMMISSION
                act = new_act
                rebalanced = True

        port_ret = 0.0
        for asset, weight in old_act.items():
            prev_px = df.iloc[i - 1].get(asset, np.nan)
            curr_px = df.iloc[i].get(asset, np.nan)
            if pd.notna(prev_px) and pd.notna(curr_px):
                port_ret += weight * (curr_px / prev_px - 1.0)

        gross = sum(weight for asset, weight in old_act.items() if asset != "BIL")
        if leverage_mode == "all_assets" and gross > 1.0:
            prev_bil = df.iloc[i - 1].get("BIL", np.nan)
            curr_bil = df.iloc[i].get("BIL", np.nan)
            bil_ret = curr_bil / prev_bil - 1.0 if pd.notna(prev_bil) and pd.notna(curr_bil) and prev_bil != 0 else 0.0
            port_ret -= (gross - 1.0) * (bil_ret + spread_daily)

        adj_ret = (1.0 + port_ret) * (1.0 - comm) - 1.0
        hist.append(adj_ret)

        row = {
            "date": df.index[i],
            "return": adj_ret,
            "is_signal": is_sig,
            "rebalanced": rebalanced,
            "gross_old": gross,
            "scale": scale,
        }
        for asset in w_assets:
            row[f"w_{asset}"] = act.get(asset, 0.0)
        rows.append(row)

    result = pd.DataFrame(rows).set_index("date")
    result["nav"] = (1.0 + result["return"]).cumprod()
    if mod.US_ROT_VOLREG_ENABLED:
        result = mod.apply_vol_regime_overlay(result, df["SPY"])
    return result


def summarize_result(mod, result):
    ret = result["return"].dropna()
    nav = result["nav"].dropna()
    years = (ret.index[-1] - ret.index[0]).days / 365.25
    annual = nav.iloc[-1] ** (1.0 / years) - 1.0 if years > 0 else np.nan
    vol = ret.std(ddof=1) * np.sqrt(mod.US_TRADING_DAYS)
    sharpe = annual / vol if pd.notna(vol) and vol > 0 else np.nan
    max_dd = ((nav - nav.cummax()) / nav.cummax()).min()

    gross = result[[col for col in result.columns if col.startswith("w_") and col != "w_BIL"]].sum(axis=1)
    summary = {
        "annual_return": float(annual),
        "annual_vol": float(vol),
        "sharpe": float(sharpe),
        "max_drawdown": float(max_dd),
        "avg_gross": float(gross.mean()),
        "median_gross": float(gross.median()),
        "pct_gt_1_5": float((gross > 1.5).mean()),
        "pct_gt_1_8": float((gross > 1.8).mean()),
    }

    segments = {}
    for label, start, end in [
        ("2010_2015", "2010-01-01", "2015-12-31"),
        ("2016_2020", "2016-01-01", "2020-12-31"),
        ("2021_2026", "2021-01-01", "2026-12-31"),
    ]:
        seg = result.loc[(result.index >= start) & (result.index <= end)]
        if len(seg) < 10:
            continue
        seg_ret = seg["return"].dropna()
        seg_nav = (1.0 + seg_ret).cumprod()
        seg_years = (seg_ret.index[-1] - seg_ret.index[0]).days / 365.25
        seg_gross = seg[[col for col in seg.columns if col.startswith("w_") and col != "w_BIL"]].sum(axis=1)
        segments[label] = {
            "annual_return": float(seg_nav.iloc[-1] ** (1.0 / seg_years) - 1.0) if seg_years > 0 else None,
            "annual_vol": float(seg_ret.std(ddof=1) * np.sqrt(mod.US_TRADING_DAYS)),
            "max_drawdown": float(((seg_nav - seg_nav.cummax()) / seg_nav.cummax()).min()),
            "avg_gross": float(seg_gross.mean()),
            "pct_gt_1_5": float((seg_gross > 1.5).mean()),
        }

    summary["segments"] = segments
    return summary


def compute_combined_summary(mod, *, us_rot_result, cn_result, cn_dk_result, us_prod_daily):
    prod_monthly = us_prod_daily.resample("ME").last()
    last_daily = us_prod_daily.index[-1]
    last_monthly_period = prod_monthly.index[-1].to_period("M")
    today_period = pd.Timestamp.now().to_period("M")
    if last_daily.to_period("M") == last_monthly_period == today_period:
        prod_monthly = prod_monthly.iloc[:-1]

    prod_sig_a = mod.make_abs_mom_signals(prod_monthly, mod.PROD_ABS_MOM_LB)
    prod_sig_b = mod.make_sma_signals(prod_monthly, mod.PROD_SMA_WINDOW, mod.PROD_SMA_BAND)
    if not mod.PROD_USE_TIMING:
        prod_sig_a = pd.DataFrame(1.0, index=prod_sig_a.index, columns=prod_sig_a.columns)
        prod_sig_b = prod_sig_a.copy()

    if mod.PROD_VS_ENABLED:
        subc_daily = mod._get_subc_daily_ret(us_prod_daily, prod_sig_a, prod_sig_b=prod_sig_b)
        prod_monthly_ret = subc_daily.groupby(subc_daily.index.to_period("M")).apply(lambda x: (1 + x).prod() - 1)
    else:
        prod_nav, _ = mod.simulate_prod_btc_phased(
            prod_monthly.pct_change().dropna(how="all"),
            prod_sig_a,
            prod_monthly.pct_change().dropna(how="all")[mod.PROD_CASH],
            rebal_month=mod.PROD_REBAL_MONTH,
            sig_b=prod_sig_b,
            blend_a=mod.PROD_BLEND_A,
            commission=mod.PROD_COMMISSION,
        )
        prod_monthly_ret = prod_nav.pct_change().dropna()
        prod_monthly_ret.index = prod_monthly_ret.index.to_period("M")
        subc_daily = prod_monthly_ret

    cn_monthly = cn_result["return"].groupby(cn_result.index.to_period("M")).apply(lambda x: (1 + x).prod() - 1)
    dk_monthly = cn_dk_result["return"].groupby(cn_dk_result.index.to_period("M")).apply(lambda x: (1 + x).prod() - 1)
    us_monthly = us_rot_result["return"].groupby(us_rot_result.index.to_period("M")).apply(lambda x: (1 + x).prod() - 1)

    all_periods = cn_monthly.index.intersection(dk_monthly.index).intersection(us_monthly.index).intersection(prod_monthly_ret.index)
    aligned = pd.DataFrame(
        {
            "Sub-A": cn_monthly.reindex(all_periods),
            "Sub-A-DK": dk_monthly.reindex(all_periods),
            "Sub-B": us_monthly.reindex(all_periods),
            "Sub-C": prod_monthly_ret.reindex(all_periods),
        }
    ).dropna()

    w = mod.COMBINED_WEIGHTS
    strat_cols = ["Sub-A", "Sub-A-DK", "Sub-B", "Sub-C"]
    nav_monthly = (1 + aligned[strat_cols]).cumprod()
    nav_comb = sum(nav_monthly[name] * w[name] for name in strat_cols)
    nav_comb = nav_comb / nav_comb.iloc[0]
    aligned["Combined"] = nav_comb.pct_change()
    aligned.loc[aligned.index[0], "Combined"] = nav_comb.iloc[0] - 1

    monthly_metrics = mod.calc_monthly_metrics(aligned["Combined"])

    nav_parts = {
        "Sub-A": (1 + cn_result["return"]).cumprod(),
        "Sub-A-DK": (1 + cn_dk_result["return"]).cumprod(),
        "Sub-B": (1 + us_rot_result["return"]).cumprod(),
        "Sub-C": (1 + subc_daily).cumprod(),
    }
    nav_parts = {name: series / series.iloc[0] for name, series in nav_parts.items() if len(series) > 1}
    common_start = max(series.index[0] for series in nav_parts.values())
    all_daily_dates = sorted(set().union(*(series.index for series in nav_parts.values())))
    all_daily_dates = [date for date in all_daily_dates if date >= common_start]
    nav_df = pd.DataFrame({name: series.reindex(pd.DatetimeIndex(all_daily_dates)).ffill() for name, series in nav_parts.items()})
    wdf = nav_df.notna().astype(float)
    for col in wdf.columns:
        wdf[col] *= w.get(col, 0.0)
    ws = wdf.sum(axis=1).replace(0, np.nan)
    wdf = wdf.div(ws, axis=0)
    nav_comb_daily = (nav_df.fillna(0.0) * wdf).sum(axis=1)
    nav_comb_daily = nav_comb_daily / nav_comb_daily.iloc[0]
    comb_daily = nav_comb_daily.pct_change().dropna()

    total_return = nav_comb_daily.iloc[-1] / nav_comb_daily.iloc[0] - 1
    ndays = (comb_daily.index[-1] - comb_daily.index[0]).days
    annual = (nav_comb_daily.iloc[-1] / nav_comb_daily.iloc[0]) ** (365.25 / ndays) - 1 if ndays > 0 else np.nan
    max_dd = ((nav_comb_daily - nav_comb_daily.cummax()) / nav_comb_daily.cummax()).min()

    return {
        "annual_return": float(annual),
        "annual_vol": float(monthly_metrics["vol"] / 100.0),
        "sharpe": float(monthly_metrics["sharpe"]),
        "max_drawdown": float(max_dd),
        "total_return": float(total_return),
    }


def main():
    mod = load_main_module()
    close_df = load_us_rotation_close()
    cn_close, cn_close_with_bond, cn_dk_close = load_cn_data()
    us_rot_close, us_prod_daily = load_us_prod_daily(mod)

    cn_result = mod.run_cn_strategy(cn_close_with_bond, mod.CN_EQUITY_CODES)
    cn_dk_result = mod.run_dk_strategy(cn_close, cn_dk_close)

    scenarios = {
        "baseline_20_1.5_futures_only": {
            "target_vol": 0.20,
            "max_lev": 1.5,
            "leverage_mode": "futures_only",
            "finance_spread_bps": 0.0,
        },
        "aggressive_25_2.0_futures_only": {
            "target_vol": 0.25,
            "max_lev": 2.0,
            "leverage_mode": "futures_only",
            "finance_spread_bps": 0.0,
        },
        "aggressive_25_2.0_all_assets_0bps": {
            "target_vol": 0.25,
            "max_lev": 2.0,
            "leverage_mode": "all_assets",
            "finance_spread_bps": 0.0,
        },
        "aggressive_25_2.0_all_assets_100bps": {
            "target_vol": 0.25,
            "max_lev": 2.0,
            "leverage_mode": "all_assets",
            "finance_spread_bps": 100.0,
        },
        "aggressive_25_2.0_all_assets_150bps": {
            "target_vol": 0.25,
            "max_lev": 2.0,
            "leverage_mode": "all_assets",
            "finance_spread_bps": 150.0,
        },
        "aggressive_25_2.0_all_assets_200bps": {
            "target_vol": 0.25,
            "max_lev": 2.0,
            "leverage_mode": "all_assets",
            "finance_spread_bps": 200.0,
        },
    }

    output = {}
    for name, cfg in scenarios.items():
        result = run_rotation_variant(mod, close_df, **cfg)
        output[name] = {
            "sub_b": summarize_result(mod, result),
            "combined": compute_combined_summary(
                mod,
                us_rot_result=result,
                cn_result=cn_result,
                cn_dk_result=cn_dk_result,
                us_prod_daily=us_prod_daily,
            ),
        }

    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
