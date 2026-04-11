from __future__ import annotations

import json
from pathlib import Path

import analyze_microcap_zz1000_hedge as hedge_mod


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"
PRACTICAL_TOP100_CSV = OUTPUT_DIR / "wind_microcap_top_100_biweekly_thursday_16y_cached.csv"
OUTPUT_PREFIX = "bias20_10_top100_practical_live"


def build_args() -> hedge_mod.argparse.Namespace:
    args = hedge_mod.parse_args()
    args.microcap_csv = PRACTICAL_TOP100_CSV
    args.microcap_column = hedge_mod.DEFAULT_MICROCAP_COLUMN
    args.signal_model = "bias_momentum"
    args.bias_n = 20
    args.bias_mom_day = 10
    args.output_prefix = OUTPUT_PREFIX
    args.require_positive_microcap_mom = False
    return args


def main() -> None:
    args = build_args()
    output_paths = hedge_mod.build_output_paths(args.output_prefix)
    close_df = hedge_mod.build_close_df(args)
    result = hedge_mod.run_backtest(
        close_df=close_df,
        signal_model=args.signal_model,
        lookback=args.lookback,
        bias_n=args.bias_n,
        bias_mom_day=args.bias_mom_day,
        futures_drag=args.futures_drag,
        require_positive_microcap_mom=args.require_positive_microcap_mom,
        r2_window=args.r2_window,
        r2_threshold=args.r2_threshold,
        vol_scale_enabled=args.vol_scale_enabled,
        target_vol=args.target_vol,
        vol_window=args.vol_window,
        max_lev=args.max_lev,
        min_lev=args.min_lev,
        scale_threshold=args.scale_threshold,
    )
    latest_signal = hedge_mod.build_latest_signal(result)
    summary = hedge_mod.build_summary(result=result, args=args, close_df=close_df)
    summary["proxy_variant"] = "top100_biweekly_thursday_practical_cached"
    summary["method_note"] = (
        "Uses outputs/wind_microcap_top_100_biweekly_thursday_16y_cached.csv as the microcap proxy, "
        "which is updated to the latest trade date and rebuilt with suspension and limit-lock tradeability handling."
    )

    result.to_csv(output_paths["nav"], index_label="date", encoding="utf-8")
    latest_signal.to_csv(output_paths["signal"], index=False, encoding="utf-8")
    output_paths["summary"].write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    hedge_mod.plot_nav(result["nav"], output_paths["curve"])

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"saved {output_paths['nav'].name}")
    print(f"saved {output_paths['signal'].name}")
    print(f"saved {output_paths['summary'].name}")
    print(f"saved {output_paths['curve'].name}")


if __name__ == "__main__":
    main()
