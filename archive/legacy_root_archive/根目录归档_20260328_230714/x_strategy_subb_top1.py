import json

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from suba_cross_asset_momentum import build_close_matrix


COMMISSION = 0.001
TRADING_DAYS = 252
LB = 200
VOL_LB = 20
TARGET_VOL = 0.15
VOL_WINDOW = 40
MAX_LEV = 1.5
MIN_SCALE = 0.05
MIN_TURNOVER = 0.0
ABS_THRESHOLD = 0.05

RISKY_CODES = ["QQQ", "GLD", "TLT", "CN_CHINEXT_TR", "CN_DIVLOWVOL_TR"]
CASH_CODE = "BIL"
LEVERABLE_CODES = {"QQQ", "GLD", "TLT"}

DEFAULT_PARAMS = {
    "commission": COMMISSION,
    "trading_days": TRADING_DAYS,
    "lb": LB,
    "vol_lb": VOL_LB,
    "target_vol": TARGET_VOL,
    "vol_window": VOL_WINDOW,
    "max_lev": MAX_LEV,
    "min_scale": MIN_SCALE,
    "min_turnover": MIN_TURNOVER,
    "abs_threshold": ABS_THRESHOLD,
    "signal_mode": "weekly",
}


def load_close_matrix():
    for name in ("suba_cross_asset_close_fixed.csv", "suba_cross_asset_close.csv"):
        try:
            df = pd.read_csv(name, parse_dates=["date"]).set_index("date").sort_index()
            if set(RISKY_CODES + [CASH_CODE]).issubset(df.columns):
                return df
        except Exception:
            pass
    close_df, _ = build_close_matrix()
    return close_df


def signal_days(close_df, start_idx, mode="weekly"):
    if mode == "daily":
        return set(range(start_idx, len(close_df)))
    if mode == "monthly":
        month_best = {}
        for i in range(start_idx, len(close_df)):
            dt = close_df.index[i]
            key = (dt.year, dt.month)
            if key not in month_best or dt > close_df.index[month_best[key]]:
                month_best[key] = i
        return set(month_best.values())
    week_best = {}
    for i in range(start_idx, len(close_df)):
        dt = close_df.index[i]
        dow = dt.dayofweek
        if dow > 3:
            continue
        yr, wk, _ = dt.isocalendar()
        key = (yr, wk)
        if key not in week_best or dow > week_best[key][1]:
            week_best[key] = (i, dow)
    return {v[0] for v in week_best.values()}


def raw_top1_weight(mom_row, abs_threshold):
    available = {
        asset: mom_row[asset]
        for asset in RISKY_CODES
        if asset in mom_row.index and pd.notna(mom_row[asset])
    }
    if not available:
        return {CASH_CODE: 1.0}, None, None
    best = max(available, key=available.get)
    best_mom = available[best]
    if best_mom <= abs_threshold:
        return {CASH_CODE: 1.0}, best, best_mom
    return {best: 1.0}, best, best_mom


def apply_model_b(raw_w, scale, max_lev):
    act = {}
    if scale <= 1.0:
        for asset, weight in raw_w.items():
            if asset == CASH_CODE:
                continue
            act[asset] = weight * scale
    else:
        fut_sum = sum(weight for asset, weight in raw_w.items() if asset in LEVERABLE_CODES)
        nf_sum = sum(weight for asset, weight in raw_w.items() if asset not in LEVERABLE_CODES and asset != CASH_CODE)
        total = fut_sum + nf_sum
        if total > 0:
            target = total * scale
            fut_target = target - nf_sum
            fut_scale = fut_target / fut_sum if fut_sum > 0 and fut_target > 0 else 1.0
            for asset, weight in raw_w.items():
                if asset == CASH_CODE:
                    continue
                act[asset] = weight * fut_scale if asset in LEVERABLE_CODES else weight
    risky = sum(act.values())
    act[CASH_CODE] = max(1.0 - risky, 0.0)
    return act


def run_strategy(close_df, params=None):
    cfg = DEFAULT_PARAMS.copy()
    if params:
        cfg.update(params)
    momentum = close_df.div(close_df.shift(cfg["lb"])).sub(1.0)
    start_idx = max(cfg["lb"], cfg["vol_window"], cfg["vol_lb"]) + 1
    sig_days = signal_days(close_df, start_idx, cfg["signal_mode"])

    act = {CASH_CODE: 1.0}
    hist = []
    rows = []
    for i in range(start_idx, len(close_df)):
        is_sig = i in sig_days
        comm = 0.0
        scale = 1.0
        if len(hist) >= cfg["vol_window"]:
            rv = np.std(hist[-cfg["vol_window"]:], ddof=1) * np.sqrt(cfg["trading_days"])
            scale = min(max(cfg["target_vol"] / rv, cfg["min_scale"]), cfg["max_lev"]) if rv > 0.001 else cfg["max_lev"]

        old_act = dict(act)
        chosen_asset = None
        chosen_mom = None
        rebalanced = False
        target_act = dict(act)
        if is_sig:
            raw_w, chosen_asset, chosen_mom = raw_top1_weight(momentum.iloc[i], cfg["abs_threshold"])
            target_act = apply_model_b(raw_w, scale, cfg["max_lev"])
            all_assets = set(target_act) | set(old_act)
            turnover = sum(abs(target_act.get(asset, 0.0) - old_act.get(asset, 0.0)) for asset in all_assets if asset != CASH_CODE)
            if turnover >= cfg["min_turnover"]:
                if turnover > 0:
                    comm = turnover * cfg["commission"]
                act = target_act
                rebalanced = True

        port_ret = 0.0
        for asset, weight in old_act.items():
            if asset not in close_df.columns:
                continue
            prev_px = close_df.iloc[i - 1].get(asset, np.nan)
            curr_px = close_df.iloc[i].get(asset, np.nan)
            if pd.notna(prev_px) and pd.notna(curr_px):
                port_ret += weight * (curr_px / prev_px - 1.0)
        adj_ret = (1.0 + port_ret) * (1.0 - comm) - 1.0
        hist.append(adj_ret)
        row = {
            "date": close_df.index[i],
            "return": adj_ret,
            "is_signal": is_sig,
            "rebalanced": rebalanced,
            "chosen_asset": chosen_asset,
            "chosen_momentum": chosen_mom,
            "scale": scale,
        }
        for asset in RISKY_CODES + [CASH_CODE]:
            row[f"w_{asset}"] = act.get(asset, 0.0)
            if is_sig:
                row[f"hypo_w_{asset}"] = target_act.get(asset, 0.0)
        rows.append(row)
    result = pd.DataFrame(rows).set_index("date")
    result["nav"] = (1.0 + result["return"]).cumprod()
    holdings = []
    for _, row in result.iterrows():
        risky_weights = {asset: row.get(f"w_{asset}", 0.0) for asset in RISKY_CODES}
        risky_weights = {asset: weight for asset, weight in risky_weights.items() if weight > 1e-6}
        if not risky_weights:
            holdings.append(CASH_CODE)
        else:
            holdings.append(max(risky_weights, key=risky_weights.get))
    result["holding"] = holdings
    return result


def calc_summary(result, params=None):
    cfg = DEFAULT_PARAMS.copy()
    if params:
        cfg.update(params)
    ret = result["return"].dropna()
    nav = result["nav"].dropna()
    monthly = ret.groupby(ret.index.to_period("M")).apply(lambda x: (1.0 + x).prod() - 1.0)
    annual_return = (nav.iloc[-1] ** (cfg["trading_days"] / len(ret)) - 1.0) if len(ret) else np.nan
    annual_vol = ret.std(ddof=1) * np.sqrt(cfg["trading_days"]) if len(ret) > 1 else np.nan
    max_dd = (nav / nav.cummax() - 1.0).min() if len(nav) else np.nan
    sharpe = annual_return / annual_vol if pd.notna(annual_vol) and annual_vol > 0 else np.nan
    calmar = annual_return / abs(max_dd) if pd.notna(max_dd) and max_dd < 0 else np.nan
    return {
        "start": str(result.index[0].date()),
        "end": str(result.index[-1].date()),
        "holding": result["holding"].iloc[-1],
        "nav": float(nav.iloc[-1]),
        "total_return": float(nav.iloc[-1] - 1.0),
        "annual_return": float(annual_return),
        "annual_vol": float(annual_vol),
        "sharpe": float(sharpe),
        "max_drawdown": float(max_dd),
        "calmar": float(calmar),
        "monthly_win_rate": float((monthly > 0).mean()) if len(monthly) else None,
        "months": int(len(monthly)),
    }


def main():
    close_df = load_close_matrix()
    result = run_strategy(close_df, DEFAULT_PARAMS)
    summary = calc_summary(result, DEFAULT_PARAMS)

    close_df.to_csv("x_strategy_subb_top1_close.csv", encoding="utf-8-sig")
    result.to_csv("x_strategy_subb_top1_nav.csv", encoding="utf-8-sig")
    with open("x_strategy_subb_top1_summary.json", "w", encoding="utf-8") as fh:
        json.dump(summary, fh, ensure_ascii=False, indent=2)

    latest_signal = pd.DataFrame(
        {
            "asset": RISKY_CODES,
            f"momentum_{DEFAULT_PARAMS['lb']}d": [
                float(close_df[asset].iloc[-1] / close_df[asset].shift(DEFAULT_PARAMS["lb"]).iloc[-1] - 1.0)
                for asset in RISKY_CODES
            ],
            "is_current_holding": [summary["holding"] == asset for asset in RISKY_CODES],
        }
    )
    latest_signal.to_csv("x_strategy_subb_top1_latest_signal.csv", index=False, encoding="utf-8-sig")

    plt.figure(figsize=(12, 6))
    plt.plot(result.index, result["nav"], color="#1b5e20", linewidth=1.6)
    plt.axhline(1.0, color="gray", linestyle="--", linewidth=0.8, alpha=0.7)
    plt.title("X Strategy (Sub-B Absolute Momentum Top1)")
    plt.xlabel("Date")
    plt.ylabel("NAV")
    plt.grid(alpha=0.25)
    plt.tight_layout()
    plt.savefig("x_strategy_subb_top1_curve.png", dpi=160, bbox_inches="tight")

    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
