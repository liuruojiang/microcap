from __future__ import annotations

import importlib.util
import json
import sys
import types
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent
MAIN_SCRIPT = ROOT / "PLUS 6" / "新建文件夹" / "mnt_bot V 6.1 plus.py"
US_DATA_CSV = ROOT / "mnt_strategy_data_us.csv"
CN_DATA_CSV = ROOT / "mnt_strategy_data_cn.csv"
OUT_CSV = ROOT / "subb_risk_switch_scan_results.csv"
OUT_MD = ROOT / "mnt_subb_risk_switch_scan_20260328.md"


@dataclass
class VariantResult:
    name: str
    category: str
    subb_annual: float
    subb_vol: float
    subb_sharpe: float
    subb_max_dd: float
    combined_annual: float
    combined_vol: float
    combined_sharpe: float
    combined_max_dd: float
    params: dict


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

    spec = importlib.util.spec_from_file_location("mntbot_v61", MAIN_SCRIPT)
    module = importlib.util.module_from_spec(spec)
    module.poe = poe
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _load_close_csv(path: Path) -> pd.Series:
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


def load_us_data(mod):
    us = pd.read_csv(US_DATA_CSV, parse_dates=["date"]).set_index("date").sort_index()
    emxc = us["EMXC_spliced"].combine_first(us["EMXC"]).combine_first(us["EEM"])

    us_raw = {}
    for ticker in [
        "QQQ", "EMXC", "EFA", "GLD", "TLT", "DBC", "BTC-USD", "BIL", "SPY",
        "VTI", "VGIT", "DBMF", "VEA", "QQQM", "GLDM", "IBIT", "EEM"
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
    if "BIL" in us_raw and "BIL" not in us_rot_close.columns:
        us_rot_close = us_rot_close.join(us_raw["BIL"][["close"]].rename(columns={"close": "BIL"}), how="left")

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


def calc_daily_metrics(ret: pd.Series, trading_days: int) -> dict[str, float]:
    ret = pd.Series(ret).dropna()
    nav = (1 + ret).cumprod()
    years = (ret.index[-1] - ret.index[0]).days / 365.25
    annual = nav.iloc[-1] ** (1.0 / years) - 1.0 if years > 0 else np.nan
    vol = ret.std(ddof=1) * np.sqrt(trading_days)
    sharpe = annual / vol if pd.notna(vol) and vol > 0 else np.nan
    max_dd = ((nav - nav.cummax()) / nav.cummax()).min()
    return {"annual": float(annual), "vol": float(vol), "sharpe": float(sharpe), "max_dd": float(max_dd)}


def run_raw_subb(mod, us_rot_close: pd.DataFrame) -> pd.DataFrame:
    return mod.run_us_rotation(
        us_rot_close,
        mod.US_ROT_POOL,
        top_n=3,
        abs_threshold=mod.US_ROT_ABS_THRESHOLD,
        min_turnover=mod.US_ROT_MIN_TURNOVER,
        threshold=mod.US_ROT_REBALANCE_THRESHOLD,
        btc_ticker=mod.US_ROT_BTC_TICKER,
        btc_start=mod.US_ROT_BTC_START,
        btc_max_w=mod.US_ROT_BTC_MAX_W,
    )


def apply_current_volreg_clone(mod, raw_result: pd.DataFrame, spy_close: pd.Series) -> pd.DataFrame:
    return mod.apply_vol_regime_overlay(raw_result, spy_close)


def overlay_partial_vol(raw_result: pd.DataFrame, spy_close: pd.Series, bil_close: pd.Series,
                        low_th: float, high_th: float, partial_scale: float) -> pd.DataFrame:
    ret = raw_result["return"].copy()
    bil_ret = bil_close.pct_change(fill_method=None).reindex(ret.index).fillna(0.0)
    spy_ret = spy_close.pct_change(fill_method=None)
    short_vol = spy_ret.rolling(10).std() * np.sqrt(252)
    long_vol = spy_ret.rolling(250).std() * np.sqrt(252)
    ratio = (short_vol / long_vol).reindex(ret.index).ffill()
    shifted = ratio.shift(1)
    scale = pd.Series(1.0, index=ret.index)
    scale = scale.mask(shifted > low_th, partial_scale)
    scale = scale.mask(shifted > high_th, 0.0)
    out = bil_ret + scale * (ret - bil_ret)
    result = raw_result.copy()
    result["return"] = out
    result["nav"] = (1 + out).cumprod()
    result["risk_scale"] = scale
    result["gate_ratio"] = ratio
    return result


def overlay_trend_vol(raw_result: pd.DataFrame, spy_close: pd.Series, bil_close: pd.Series,
                      sma_w: int, vol_th: float) -> pd.DataFrame:
    ret = raw_result["return"].copy()
    bil_ret = bil_close.pct_change(fill_method=None).reindex(ret.index).fillna(0.0)
    spy_ret = spy_close.pct_change(fill_method=None)
    short_vol = spy_ret.rolling(10).std() * np.sqrt(252)
    long_vol = spy_ret.rolling(250).std() * np.sqrt(252)
    ratio = (short_vol / long_vol).reindex(ret.index).ffill()
    sma = spy_close.rolling(sma_w).mean().reindex(ret.index).ffill()
    risk_off = ((ratio.shift(1) > vol_th) & (spy_close.reindex(ret.index).shift(1) < sma.shift(1))).fillna(False)
    out = ret.copy()
    out.loc[risk_off] = bil_ret.loc[risk_off]
    result = raw_result.copy()
    result["return"] = out
    result["nav"] = (1 + out).cumprod()
    result["risk_scale"] = (~risk_off).astype(float)
    result["gate_ratio"] = ratio
    return result


def overlay_dd_cooldown(raw_result: pd.DataFrame, bil_close: pd.Series, dd_trigger: float, cooldown_days: int) -> pd.DataFrame:
    ret = raw_result["return"].copy()
    bil_ret = bil_close.pct_change(fill_method=None).reindex(ret.index).fillna(0.0)
    nav = (1 + ret).cumprod()
    dd = nav / nav.cummax() - 1.0
    out = pd.Series(index=ret.index, dtype=float)
    scale = pd.Series(1.0, index=ret.index)
    remaining = 0
    for i, dt in enumerate(ret.index):
        if i == 0:
            out.iloc[i] = ret.iloc[i]
            continue
        prev_dt = ret.index[i - 1]
        if dd.loc[prev_dt] <= -dd_trigger and remaining == 0:
            remaining = cooldown_days
        if remaining > 0:
            out.iloc[i] = bil_ret.iloc[i]
            scale.iloc[i] = 0.0
            remaining -= 1
        else:
            out.iloc[i] = ret.iloc[i]
    result = raw_result.copy()
    result["return"] = out.fillna(0.0)
    result["nav"] = (1 + result["return"]).cumprod()
    result["risk_scale"] = scale
    result["drawdown_ref"] = dd.reindex(result.index)
    return result


def overlay_combo(raw_result: pd.DataFrame, spy_close: pd.Series, bil_close: pd.Series,
                  low_th: float, high_th: float, partial_scale: float,
                  sma_w: int, trend_vol_th: float,
                  dd_trigger: float, cooldown_days: int) -> pd.DataFrame:
    ret = raw_result["return"].copy()
    bil_ret = bil_close.pct_change(fill_method=None).reindex(ret.index).fillna(0.0)
    spy_ret = spy_close.pct_change(fill_method=None)
    short_vol = spy_ret.rolling(10).std() * np.sqrt(252)
    long_vol = spy_ret.rolling(250).std() * np.sqrt(252)
    ratio = (short_vol / long_vol).reindex(ret.index).ffill()
    sma = spy_close.rolling(sma_w).mean().reindex(ret.index).ffill()
    base_scale = pd.Series(1.0, index=ret.index)
    base_scale = base_scale.mask(ratio.shift(1) > low_th, partial_scale)
    base_scale = base_scale.mask(ratio.shift(1) > high_th, 0.0)
    hard_stop = ((ratio.shift(1) > trend_vol_th) & (spy_close.reindex(ret.index).shift(1) < sma.shift(1))).fillna(False)

    nav = (1 + ret).cumprod()
    dd = nav / nav.cummax() - 1.0
    remaining = 0
    out = pd.Series(index=ret.index, dtype=float)
    scale = pd.Series(1.0, index=ret.index)
    for i, dt in enumerate(ret.index):
        if i == 0:
            out.iloc[i] = ret.iloc[i]
            continue
        prev_dt = ret.index[i - 1]
        if dd.loc[prev_dt] <= -dd_trigger and remaining == 0:
            remaining = cooldown_days
        live_scale = float(base_scale.iloc[i])
        if hard_stop.iloc[i] or remaining > 0:
            live_scale = 0.0
        out.iloc[i] = bil_ret.iloc[i] + live_scale * (ret.iloc[i] - bil_ret.iloc[i])
        scale.iloc[i] = live_scale
        if remaining > 0:
            remaining -= 1
    result = raw_result.copy()
    result["return"] = out.fillna(0.0)
    result["nav"] = (1 + result["return"]).cumprod()
    result["risk_scale"] = scale
    result["gate_ratio"] = ratio
    return result


def compute_subc_daily(mod, us_prod_daily: pd.DataFrame) -> pd.Series:
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
    return mod._get_subc_daily_ret(us_prod_daily, prod_sig_a, prod_sig_b=prod_sig_b)


def compute_combined_metrics(mod, *, cn_result, cn_dk_result, subb_result, subc_daily, subd_daily):
    daily_series = {
        "Sub-A": cn_result["return"].dropna(),
        "Sub-A-DK": cn_dk_result["return"].dropna(),
        "Sub-B": subb_result["return"].dropna(),
        "Sub-C": subc_daily.dropna(),
        "Sub-D": subd_daily.dropna(),
    }
    weights = dict(mod.COMBINED_WEIGHTS)

    monthly = {}
    for name, ser in daily_series.items():
        monthly[name] = ser.groupby(ser.index.to_period("M")).apply(lambda x: (1 + x).prod() - 1)
    all_periods = None
    for ser in monthly.values():
        all_periods = ser.index if all_periods is None else all_periods.intersection(ser.index)
    aligned = pd.DataFrame({name: ser.reindex(all_periods) for name, ser in monthly.items()}).dropna()
    nav_monthly = (1 + aligned).cumprod()
    comb_nav_monthly = sum(nav_monthly[name] * weights[name] for name in aligned.columns)
    comb_nav_monthly = comb_nav_monthly / comb_nav_monthly.iloc[0]
    comb_monthly_ret = comb_nav_monthly.pct_change()
    comb_monthly_ret.iloc[0] = comb_nav_monthly.iloc[0] - 1

    monthly_metrics = mod.calc_monthly_metrics(comb_monthly_ret)

    common_start = max(ser.index[0] for ser in daily_series.values())
    nav_parts = {}
    for name, ser in daily_series.items():
        ser = ser[ser.index >= common_start]
        nav = (1 + ser).cumprod()
        nav_parts[name] = nav / nav.iloc[0]

    all_dates = sorted(set().union(*[nav.index for nav in nav_parts.values()]))
    all_dates = [d for d in all_dates if d >= common_start]
    combo_nav = pd.Series(0.0, index=all_dates, dtype=float)
    for name, nav in nav_parts.items():
        combo_nav = combo_nav.add(nav.reindex(all_dates).ffill() * weights[name], fill_value=0.0)
    combo_nav = combo_nav / combo_nav.iloc[0]
    combo_max_dd = ((combo_nav - combo_nav.cummax()) / combo_nav.cummax()).min()

    return {
        "annual": float(monthly_metrics["annual"] / 100.0),
        "vol": float(monthly_metrics["vol"] / 100.0),
        "sharpe": float(monthly_metrics["sharpe"]),
        "max_dd": float(combo_max_dd),
    }


def main():
    mod = load_main_module()
    cn_close, cn_close_with_bond, cn_dk_close = load_cn_data()
    us_rot_close, us_prod_daily = load_us_data(mod)

    cn_result = mod.run_cn_strategy(cn_close_with_bond, mod.CN_EQUITY_CODES)
    cn_dk_result = mod.run_dk_strategy(cn_close, cn_dk_close)
    raw_subb = run_raw_subb(mod, us_rot_close)
    subc_daily = compute_subc_daily(mod, us_prod_daily)
    subd_daily, _ = mod.run_spy_bull_put_spread(us_rot_close)

    variants: list[tuple[str, str, pd.DataFrame, dict]] = []

    baseline = apply_current_volreg_clone(mod, raw_subb, us_rot_close["SPY"])
    variants.append(("baseline_current_volreg", "baseline", baseline, {}))

    for low_th in [1.3, 1.4, 1.5]:
        for high_th in [1.8, 2.0]:
            for partial_scale in [0.35, 0.5, 0.65]:
                name = f"partial_vol_l{low_th:.1f}_h{high_th:.1f}_p{partial_scale:.2f}"
                res = overlay_partial_vol(raw_subb, us_rot_close["SPY"], us_rot_close["BIL"], low_th, high_th, partial_scale)
                variants.append((name, "partial_vol", res, {"low_th": low_th, "high_th": high_th, "partial_scale": partial_scale}))

    for sma_w in [150, 200]:
        for vol_th in [1.3, 1.4, 1.5]:
            name = f"trend_vol_ma{sma_w}_v{vol_th:.1f}"
            res = overlay_trend_vol(raw_subb, us_rot_close["SPY"], us_rot_close["BIL"], sma_w, vol_th)
            variants.append((name, "trend_vol", res, {"sma_w": sma_w, "vol_th": vol_th}))

    for dd_trigger in [0.08, 0.10, 0.12]:
        for cooldown_days in [5, 10, 15]:
            name = f"ddcool_dd{dd_trigger:.2f}_cd{cooldown_days}"
            res = overlay_dd_cooldown(raw_subb, us_rot_close["BIL"], dd_trigger, cooldown_days)
            variants.append((name, "dd_cooldown", res, {"dd_trigger": dd_trigger, "cooldown_days": cooldown_days}))

    for low_th, high_th, partial_scale, sma_w, trend_vol_th, dd_trigger, cooldown_days in [
        (1.3, 1.8, 0.50, 200, 1.4, 0.10, 10),
        (1.4, 2.0, 0.50, 200, 1.4, 0.10, 10),
        (1.4, 1.8, 0.35, 200, 1.4, 0.08, 10),
        (1.5, 2.0, 0.65, 150, 1.5, 0.12, 15),
    ]:
        name = f"combo_l{low_th:.1f}_h{high_th:.1f}_p{partial_scale:.2f}_ma{sma_w}_v{trend_vol_th:.1f}_dd{dd_trigger:.2f}_cd{cooldown_days}"
        res = overlay_combo(raw_subb, us_rot_close["SPY"], us_rot_close["BIL"], low_th, high_th, partial_scale, sma_w, trend_vol_th, dd_trigger, cooldown_days)
        variants.append((name, "combo", res, {
            "low_th": low_th, "high_th": high_th, "partial_scale": partial_scale,
            "sma_w": sma_w, "trend_vol_th": trend_vol_th, "dd_trigger": dd_trigger, "cooldown_days": cooldown_days
        }))

    rows = []
    for name, category, subb_res, params in variants:
        subb_metrics = calc_daily_metrics(subb_res["return"], mod.US_TRADING_DAYS)
        combined_metrics = compute_combined_metrics(
            mod,
            cn_result=cn_result,
            cn_dk_result=cn_dk_result,
            subb_result=subb_res,
            subc_daily=subc_daily,
            subd_daily=subd_daily,
        )
        rows.append(VariantResult(
            name=name,
            category=category,
            subb_annual=subb_metrics["annual"],
            subb_vol=subb_metrics["vol"],
            subb_sharpe=subb_metrics["sharpe"],
            subb_max_dd=subb_metrics["max_dd"],
            combined_annual=combined_metrics["annual"],
            combined_vol=combined_metrics["vol"],
            combined_sharpe=combined_metrics["sharpe"],
            combined_max_dd=combined_metrics["max_dd"],
            params=params,
        ).__dict__)

    df = pd.DataFrame(rows)
    base = df.loc[df["name"] == "baseline_current_volreg"].iloc[0]
    df["delta_combined_annual_bp"] = (df["combined_annual"] - float(base["combined_annual"])) * 10000.0
    df["delta_combined_sharpe"] = df["combined_sharpe"] - float(base["combined_sharpe"])
    df["delta_combined_max_dd_bp"] = (df["combined_max_dd"] - float(base["combined_max_dd"])) * 10000.0
    df["delta_subb_sharpe"] = df["subb_sharpe"] - float(base["subb_sharpe"])
    df = df.sort_values(["delta_combined_sharpe", "delta_combined_annual_bp"], ascending=[False, False]).reset_index(drop=True)
    df.to_csv(OUT_CSV, index=False)

    top = df.head(15)
    report = {
        "baseline": base.to_dict(),
        "top_15": top.to_dict(orient="records"),
    }
    OUT_MD.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(df.head(15).to_string(index=False))
    print(f"saved {OUT_CSV}")
    print(f"saved {OUT_MD}")


if __name__ == "__main__":
    main()
