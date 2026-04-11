import importlib.util
import json
import sys
import types
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent
MAIN_SCRIPT = ROOT / "mnt_bot plus 1 .py"
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

    cn_dk_close = pd.DataFrame(index=cn_raw.index)
    cn_dk_close["DK_ZZ1000"] = cn_raw["1.000852"]
    cn_dk_close["DK_SZ50"] = cn_raw["1.000016"]
    cn_dk_close["DK_HS300"] = cn_raw["1.000300"]
    cn_dk_close["DK_ZZ500"] = cn_raw["1.000905"]
    cn_dk_close["DK_CYB"] = cn_raw["0.399006"]
    cn_dk_close = cn_dk_close.ffill().dropna()
    return cn_close, cn_dk_close


def calc_base_metrics(mod, ret):
    ret = ret.dropna()
    nav = (1.0 + ret).cumprod()
    years = (ret.index[-1] - ret.index[0]).days / 365.25
    annual = nav.iloc[-1] ** (1.0 / years) - 1.0 if years > 0 else np.nan
    vol = ret.std(ddof=1) * np.sqrt(mod.CN_DK_TRADING_DAYS)
    sharpe = annual / vol if pd.notna(vol) and vol > 0 else np.nan
    max_dd = ((nav - nav.cummax()) / nav.cummax()).min()
    return {
        "annual_return": float(annual),
        "annual_vol": float(vol),
        "sharpe": float(sharpe),
        "max_drawdown": float(max_dd),
    }


def build_health_metrics(mod, dk_result, cn_dk_close):
    signals_df = dk_result.attrs["signals_df"].copy()
    common_idx = dk_result.index
    signals_shifted = signals_df.reindex(common_idx).shift(1)

    rolling_median = signals_shifted.rolling(252, min_periods=126).median()
    signal_breadth = (signals_shifted > rolling_median).sum(axis=1)

    top_pair = dk_result["top_pair"].replace("none", np.nan)
    window = 60
    concentration_hhi = pd.Series(np.nan, index=common_idx)
    top_pair_max_share = pd.Series(np.nan, index=common_idx)
    for i in range(window - 1, len(common_idx)):
        sample = top_pair.iloc[i - window + 1 : i + 1].dropna()
        if sample.empty:
            continue
        shares = sample.value_counts(normalize=True)
        concentration_hhi.iloc[i] = float((shares**2).sum())
        top_pair_max_share.iloc[i] = float(shares.iloc[0])

    small = cn_dk_close[["DK_ZZ1000", "DK_CYB"]].mean(axis=1)
    large = cn_dk_close[["DK_SZ50", "DK_HS300"]].mean(axis=1)
    style_gap = small.pct_change(60) - large.pct_change(60)
    gap_mean = style_gap.rolling(252, min_periods=126).mean()
    gap_std = style_gap.rolling(252, min_periods=126).std()
    style_gap_z = (style_gap - gap_mean) / gap_std
    style_gap_abs_z = style_gap_z.abs().reindex(common_idx)

    next_20d = (1.0 + dk_result["return"]).rolling(20).apply(np.prod, raw=True).shift(-19) - 1.0

    metrics = pd.DataFrame(
        {
            "signal_breadth": signal_breadth,
            "concentration_hhi": concentration_hhi,
            "top_pair_max_share": top_pair_max_share,
            "style_gap_abs_z": style_gap_abs_z,
            "next_20d_return": next_20d,
            "base_return": dk_result["return"],
            "base_weight": dk_result["weight"],
        },
        index=common_idx,
    )

    health_scale = pd.Series(1.0, index=common_idx)
    severe = (
        (metrics["signal_breadth"] <= 2)
        | (metrics["concentration_hhi"] >= 0.50)
        | (metrics["style_gap_abs_z"] >= 2.0)
    )
    moderate = (
        (metrics["signal_breadth"] <= 3)
        | (metrics["concentration_hhi"] >= 0.35)
        | (metrics["style_gap_abs_z"] >= 1.5)
    )
    health_scale.loc[moderate] = 0.75
    health_scale.loc[severe] = 0.50

    metrics["health_scale_raw"] = health_scale
    metrics["health_scale"] = health_scale.shift(1).fillna(1.0)
    metrics["health_state"] = np.where(metrics["health_scale"] <= 0.5, "severe", np.where(metrics["health_scale"] < 1.0, "moderate", "normal"))
    return metrics


def apply_health_overlay(mod, dk_result, metrics):
    scaled = metrics["health_scale"]
    prev_scaled = scaled.shift(1).fillna(scaled.iloc[0])
    delta = (scaled - prev_scaled).abs()
    tc = mod.CN_COMMISSION * delta
    adj_ret = (1.0 + dk_result["return"] * scaled) * (1.0 - tc) - 1.0
    return adj_ret


def summarize_health(metrics, overlaid_ret, mod):
    state_table = {}
    for state in ["normal", "moderate", "severe"]:
        mask = metrics["health_state"] == state
        subset = metrics.loc[mask]
        if subset.empty:
            continue
        state_table[state] = {
            "days": int(mask.sum()),
            "share": float(mask.mean()),
            "avg_signal_breadth": float(subset["signal_breadth"].mean()),
            "avg_concentration_hhi": float(subset["concentration_hhi"].mean()),
            "avg_style_gap_abs_z": float(subset["style_gap_abs_z"].mean()),
            "avg_next_20d_return": float(subset["next_20d_return"].mean()),
        }

    return {
        "base": calc_base_metrics(mod, metrics["base_return"]),
        "overlay": calc_base_metrics(mod, overlaid_ret),
        "health_states": state_table,
        "health_scale_avg": float(metrics["health_scale"].mean()),
        "health_scale_lt_1_share": float((metrics["health_scale"] < 1.0).mean()),
        "health_scale_le_0_5_share": float((metrics["health_scale"] <= 0.5).mean()),
    }


def main():
    mod = load_main_module()
    cn_close, cn_dk_close = load_cn_data()
    dk_result = mod.run_dk_strategy(cn_close, cn_dk_close)
    metrics = build_health_metrics(mod, dk_result, cn_dk_close)
    overlaid_ret = apply_health_overlay(mod, dk_result, metrics)
    summary = summarize_health(metrics, overlaid_ret, mod)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
