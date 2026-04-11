import json

import numpy as np
import pandas as pd

import analyze_subb_leverage_scenarios as lev


# Map Sub-C strategic proxies into the closest Sub-B tradable sleeves.
SUBC_TO_SUBB_PROXY = {
    "QQQ": "QQQ",
    "VEA": "EFA",
    "GLD": "GLD",
    "VGIT": "TLT",
    "BTC-USD": "BTC-USD",
}


def build_anchor_frame(mod, index):
    risky_assets = [asset for asset in mod.US_ROT_POOL if asset != "BIL"]
    anchor = pd.DataFrame(0.0, index=index, columns=risky_assets)
    meta = pd.DataFrame(index=index, columns=["phase", "mapped_share"], dtype=object)

    phases = [
        ("pre_dbmf", None, mod.DBMF_BT_START, mod.PROD_PORTFOLIO_PRE_DBMF),
        ("dbmf_no_btc", mod.DBMF_BT_START, mod.BTC_BT_START, mod.PROD_PORTFOLIO_BT),
        ("full", mod.BTC_BT_START, None, mod.PROD_PORTFOLIO),
    ]

    for phase_name, start, end, portfolio in phases:
        mapped = {asset: 0.0 for asset in risky_assets}
        for cfg in portfolio.values():
            proxy = cfg["proxy"]
            target = SUBC_TO_SUBB_PROXY.get(proxy)
            if target is not None:
                mapped[target] += cfg["w"]

        mapped_share = float(sum(mapped.values()))
        if mapped_share > 0:
            dist = {asset: mapped[asset] / mapped_share for asset in risky_assets}
        else:
            dist = {asset: 0.0 for asset in risky_assets}

        mask = pd.Series(True, index=index)
        if start is not None:
            mask &= index >= start
        if end is not None:
            mask &= index < end

        phase_idx = index[mask]
        if len(phase_idx) == 0:
            continue

        anchor.loc[phase_idx] = pd.DataFrame([dist], index=phase_idx)
        meta.loc[phase_idx, "phase"] = phase_name
        meta.loc[phase_idx, "mapped_share"] = mapped_share

    meta["mapped_share"] = meta["mapped_share"].astype(float)
    return anchor, meta


def build_overlay_targets(mod, base_result, *, mode):
    risky_assets = [asset for asset in mod.US_ROT_POOL if asset != "BIL"]
    anchor, meta = build_anchor_frame(mod, base_result.index)

    rows = []
    for dt in base_result.index:
        risky = pd.Series(
            {asset: max(float(base_result.at[dt, f"w_{asset}"]), 0.0) for asset in risky_assets}
        )
        full_gross = float(risky.sum())
        if full_gross > 0:
            dist = risky / full_gross
            anchor_dist = anchor.loc[dt].astype(float)
            positive_tilt = (dist - anchor_dist).clip(lower=0.0)
            active_share = float(positive_tilt.sum())
            if mode == "cash_residual":
                overlay_risky = positive_tilt * full_gross
            elif mode == "rescaled_active":
                if active_share > 0:
                    overlay_risky = positive_tilt / active_share * full_gross
                else:
                    overlay_risky = pd.Series(0.0, index=risky_assets)
            else:
                raise ValueError(f"Unknown overlay mode: {mode}")
            overlap_share = float(np.minimum(dist.values, anchor_dist.values).sum())
        else:
            positive_tilt = pd.Series(0.0, index=risky_assets)
            overlay_risky = pd.Series(0.0, index=risky_assets)
            active_share = 0.0
            overlap_share = np.nan

        overlay_gross = float(overlay_risky.sum())
        row = {
            "date": dt,
            "overlay_mode": mode,
            "phase": meta.at[dt, "phase"],
            "anchor_mapped_share": float(meta.at[dt, "mapped_share"]),
            "full_gross": full_gross,
            "overlay_gross_target": overlay_gross,
            "distribution_overlap_with_anchor": overlap_share,
            "positive_tilt_share": float(positive_tilt.sum()),
            "active_tilt_share": active_share,
        }
        for asset in risky_assets:
            row[f"w_{asset}"] = float(overlay_risky.get(asset, 0.0))
        row["w_BIL"] = float(max(1.0 - overlay_gross, 0.0))
        rows.append(row)

    return pd.DataFrame(rows).set_index("date"), anchor, meta


def replay_from_targets(mod, close_df, targets):
    risky_assets = [asset for asset in mod.US_ROT_POOL if asset != "BIL"]
    all_assets = risky_assets + ["BIL"]
    current_act = {"BIL": 1.0}
    rows = []

    for dt in targets.index:
        i = close_df.index.get_loc(dt)
        old_act = dict(current_act)
        target = {asset: float(max(targets.at[dt, f"w_{asset}"], 0.0)) for asset in risky_assets}
        target["BIL"] = float(max(targets.at[dt, "w_BIL"], 0.0))

        turnover = sum(abs(target.get(asset, 0.0) - old_act.get(asset, 0.0)) for asset in risky_assets)
        comm = 0.0
        rebalanced = False
        if turnover >= mod.US_ROT_MIN_TURNOVER:
            if turnover > 0:
                comm = turnover * mod.US_ROT_COMMISSION
            current_act = target
            rebalanced = True

        port_ret = 0.0
        for asset, weight in old_act.items():
            prev_px = close_df.iloc[i - 1].get(asset, np.nan)
            curr_px = close_df.iloc[i].get(asset, np.nan)
            if pd.notna(prev_px) and pd.notna(curr_px):
                port_ret += weight * (curr_px / prev_px - 1.0)

        adj_ret = (1.0 + port_ret) * (1.0 - comm) - 1.0
        row = {
            "date": dt,
            "return": adj_ret,
            "rebalanced": rebalanced,
            "overlay_turnover": turnover,
            "full_gross": float(targets.at[dt, "full_gross"]),
            "overlay_gross_target": float(targets.at[dt, "overlay_gross_target"]),
            "distribution_overlap_with_anchor": float(targets.at[dt, "distribution_overlap_with_anchor"])
            if pd.notna(targets.at[dt, "distribution_overlap_with_anchor"])
            else np.nan,
            "phase": targets.at[dt, "phase"],
            "anchor_mapped_share": float(targets.at[dt, "anchor_mapped_share"]),
        }
        for asset in all_assets:
            row[f"w_{asset}"] = current_act.get(asset, 0.0)
        rows.append(row)

    result = pd.DataFrame(rows).set_index("date")
    result["nav"] = (1.0 + result["return"]).cumprod()
    if mod.US_ROT_VOLREG_ENABLED:
        result = mod.apply_vol_regime_overlay(result, close_df["SPY"])
    return result


def compute_subc_daily(mod, us_prod_daily):
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


def summarize_relationship(full_result, overlay_result, subc_daily, target_df):
    full_ret = full_result["return"].dropna()
    overlay_ret = overlay_result["return"].dropna()
    common_full = full_ret.index.intersection(subc_daily.index)
    common_overlay = overlay_ret.index.intersection(subc_daily.index)

    diagnostics = {
        "full_vs_subc_corr": float(full_ret.reindex(common_full).corr(subc_daily.reindex(common_full))),
        "overlay_vs_subc_corr": float(overlay_ret.reindex(common_overlay).corr(subc_daily.reindex(common_overlay))),
        "avg_full_gross": float(target_df["full_gross"].mean()),
        "avg_overlay_target_gross": float(target_df["overlay_gross_target"].mean()),
        "median_overlay_target_gross": float(target_df["overlay_gross_target"].median()),
        "overlay_target_gt_0_25_share": float((target_df["overlay_gross_target"] > 0.25).mean()),
        "overlay_target_gt_0_50_share": float((target_df["overlay_gross_target"] > 0.50).mean()),
        "avg_distribution_overlap_with_anchor": float(target_df["distribution_overlap_with_anchor"].dropna().mean()),
        "avg_positive_tilt_share": float(target_df["positive_tilt_share"].mean()),
        "avg_target_to_full_gross_ratio": float(
            (target_df["overlay_gross_target"] / target_df["full_gross"].replace(0.0, np.nan)).dropna().mean()
        ),
    }

    phase_stats = {}
    for phase_name, phase_df in target_df.groupby("phase"):
        phase_stats[phase_name] = {
            "days": int(len(phase_df)),
            "avg_anchor_mapped_share": float(phase_df["anchor_mapped_share"].mean()),
            "avg_full_gross": float(phase_df["full_gross"].mean()),
            "avg_overlay_target_gross": float(phase_df["overlay_gross_target"].mean()),
            "avg_distribution_overlap_with_anchor": float(
                phase_df["distribution_overlap_with_anchor"].dropna().mean()
            ),
        }
    diagnostics["phase_stats"] = phase_stats
    return diagnostics


def main():
    mod = lev.load_main_module()
    cn_close, cn_close_with_bond, cn_dk_close = lev.load_cn_data()
    us_rot_close, us_prod_daily = lev.load_us_prod_daily(mod)

    cn_result = mod.run_cn_strategy(cn_close_with_bond, mod.CN_EQUITY_CODES)
    cn_dk_result = mod.run_dk_strategy(cn_close, cn_dk_close)

    full_result = lev.run_rotation_variant(
        mod,
        us_rot_close,
        target_vol=mod.US_ROT_TARGET_VOL,
        max_lev=mod.US_ROT_MAX_LEV,
        leverage_mode="futures_only",
        finance_spread_bps=0.0,
    )
    subc_daily = compute_subc_daily(mod, us_prod_daily)

    output = {
        "sub_b_full_rotation": lev.summarize_result(mod, full_result),
        "combined_full_rotation": lev.compute_combined_summary(
            mod,
            us_rot_result=full_result,
            cn_result=cn_result,
            cn_dk_result=cn_dk_result,
            us_prod_daily=us_prod_daily,
        ),
    }

    for mode in ["cash_residual", "rescaled_active"]:
        target_df, _, _ = build_overlay_targets(mod, full_result, mode=mode)
        overlay_result = replay_from_targets(mod, us_rot_close, target_df)
        output[f"sub_b_overlay_{mode}"] = lev.summarize_result(mod, overlay_result)
        output[f"combined_overlay_{mode}"] = lev.compute_combined_summary(
            mod,
            us_rot_result=overlay_result,
            cn_result=cn_result,
            cn_dk_result=cn_dk_result,
            us_prod_daily=us_prod_daily,
        )
        output[f"diagnostics_{mode}"] = summarize_relationship(full_result, overlay_result, subc_daily, target_df)
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
