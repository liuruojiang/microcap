from __future__ import annotations

from contextlib import contextmanager
from itertools import combinations
from pathlib import Path

import numpy as np
import pandas as pd

import analyze_subb_risk_switch_enhancement as helpers


ROOT = Path(__file__).resolve().parent
OUT_CSV = ROOT / "adk_universe_variant_results.csv"
OUT_MD = ROOT / "mnt_adk_universe_variants_20260328.md"


def calc_adk_metrics(mod, ret: pd.Series) -> dict[str, float]:
    ret = ret.dropna()
    nav = (1.0 + ret).cumprod()
    years = (ret.index[-1] - ret.index[0]).days / 365.25
    annual = nav.iloc[-1] ** (1.0 / years) - 1.0 if years > 0 else np.nan
    vol = ret.std(ddof=1) * np.sqrt(mod.CN_DK_TRADING_DAYS)
    sharpe = annual / vol if pd.notna(vol) and vol > 0 else np.nan
    max_dd = ((nav - nav.cummax()) / nav.cummax()).min()
    return {"annual": float(annual), "vol": float(vol), "sharpe": float(sharpe), "max_dd": float(max_dd)}


def summarize_pair_behavior(dk_result: pd.DataFrame) -> dict[str, float]:
    pair_series = dk_result["top_pair"].replace("none", np.nan).dropna()
    if pair_series.empty:
        return {"pair_change_days": 0, "structural_trade_days_per_year": 0.0, "median_pair_spell": 0.0}
    pair_changed = pair_series.ne(pair_series.shift(1))
    pair_change_days = int(pair_changed.sum())
    years = (pair_series.index[-1] - pair_series.index[0]).days / 365.25
    spells = pair_series.ne(pair_series.shift(1)).cumsum()
    spell_lengths = pair_series.groupby(spells).size()
    return {
        "pair_change_days": pair_change_days,
        "structural_trade_days_per_year": float(pair_change_days / years) if years > 0 else np.nan,
        "median_pair_spell": float(spell_lengths.median()) if len(spell_lengths) > 0 else 0.0,
    }


@contextmanager
def patched_dk_universe(mod, names: list[str]):
    old_indices = mod.CN_DK_INDICES
    old_index_names = mod.CN_DK_INDEX_NAMES
    new_indices = {k: v for k, v in old_indices.items() if k in names}
    new_index_names = {k: old_index_names[k] for k in names}
    mod.CN_DK_INDICES = new_indices
    mod.CN_DK_INDEX_NAMES = new_index_names
    try:
        yield
    finally:
        mod.CN_DK_INDICES = old_indices
        mod.CN_DK_INDEX_NAMES = old_index_names


def main():
    mod = helpers.load_main_module()
    cn_close, cn_close_with_bond, cn_dk_close = helpers.load_cn_data()
    us_rot_close, us_prod_daily = helpers.load_us_data(mod)
    cn_result = mod.run_cn_strategy(cn_close_with_bond, mod.CN_EQUITY_CODES)
    raw_subb = helpers.run_raw_subb(mod, us_rot_close)
    subb_result = helpers.apply_current_volreg_clone(mod, raw_subb, us_rot_close["SPY"])
    subc_daily = helpers.compute_subc_daily(mod, us_prod_daily)
    subd_daily, _ = mod.run_spy_bull_put_spread(us_rot_close)

    variants = {
        "baseline_5": ["SZ50", "HS300", "ZZ500", "ZZ1000", "CYB"],
        "drop_cyb": ["SZ50", "HS300", "ZZ500", "ZZ1000"],
        "drop_zz1000": ["SZ50", "HS300", "ZZ500", "CYB"],
        "drop_sz50": ["HS300", "ZZ500", "ZZ1000", "CYB"],
        "core_3": ["SZ50", "HS300", "ZZ500"],
        "growth_3": ["ZZ500", "ZZ1000", "CYB"],
        "balanced_4": ["SZ50", "HS300", "ZZ500", "CYB"],
    }

    rows = []
    for name, members in variants.items():
        with patched_dk_universe(mod, members):
            dk_result = mod.run_dk_strategy(cn_close, cn_dk_close)
        adk_metrics = calc_adk_metrics(mod, dk_result["return"])
        pair_stats = summarize_pair_behavior(dk_result)
        combined_metrics = helpers.compute_combined_metrics(
            mod,
            cn_result=cn_result,
            cn_dk_result=dk_result,
            subb_result=subb_result,
            subc_daily=subc_daily,
            subd_daily=subd_daily,
        )
        rows.append({
            "variant": name,
            "members": ",".join(members),
            "n_indices": len(members),
            "n_pairs": len(list(combinations(members, 2))),
            "adk_annual": adk_metrics["annual"],
            "adk_vol": adk_metrics["vol"],
            "adk_sharpe": adk_metrics["sharpe"],
            "adk_max_dd": adk_metrics["max_dd"],
            "pair_change_days": pair_stats["pair_change_days"],
            "trade_days_per_year": pair_stats["structural_trade_days_per_year"],
            "median_pair_spell": pair_stats["median_pair_spell"],
            "combined_annual": combined_metrics["annual"],
            "combined_vol": combined_metrics["vol"],
            "combined_sharpe": combined_metrics["sharpe"],
            "combined_max_dd": combined_metrics["max_dd"],
        })

    df = pd.DataFrame(rows)
    base = df.loc[df["variant"] == "baseline_5"].iloc[0]
    df["delta_combined_annual_bp"] = (df["combined_annual"] - float(base["combined_annual"])) * 10000.0
    df["delta_combined_sharpe"] = df["combined_sharpe"] - float(base["combined_sharpe"])
    df["delta_combined_max_dd_bp"] = (df["combined_max_dd"] - float(base["combined_max_dd"])) * 10000.0
    df["delta_trade_days_per_year"] = df["trade_days_per_year"] - float(base["trade_days_per_year"])
    df = df.sort_values(["delta_combined_sharpe", "delta_combined_annual_bp"], ascending=[False, False]).reset_index(drop=True)
    df.to_csv(OUT_CSV, index=False)
    OUT_MD.write_text(df.to_json(orient="records", force_ascii=False, indent=2), encoding="utf-8")
    print(df.to_string(index=False))
    print(f"saved {OUT_CSV}")
    print(f"saved {OUT_MD}")


if __name__ == "__main__":
    main()
