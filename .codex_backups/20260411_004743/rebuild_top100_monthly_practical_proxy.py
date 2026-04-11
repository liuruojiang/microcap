from __future__ import annotations

import json

import analyze_top100_rebalance_frequency as freq_mod


def main() -> None:
    trading_dates = freq_mod.load_trading_dates()
    monthly_rebalance_dates = freq_mod.build_all_rebalance_dates(trading_dates)["monthly"]
    symbols = freq_mod.load_universe()
    returns_df, caps_by_date, buyable_df, sellable_df = freq_mod.load_cache_panels(
        symbols,
        trading_dates,
        monthly_rebalance_dates,
        max_workers=8,
    )

    index_df, turnover_df = freq_mod.build_index_and_turnover(
        trading_dates,
        returns_df,
        caps_by_date,
        buyable_df,
        sellable_df,
        monthly_rebalance_dates,
    )
    net = freq_mod.run_strategy(index_df, turnover_df)
    summary = freq_mod.summarize("monthly", net, turnover_df)

    freq_mod.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    index_path = freq_mod.OUTPUT_DIR / "wind_microcap_top_100_monthly_16y_cached.csv"
    turnover_path = freq_mod.OUTPUT_DIR / "microcap_top100_monthly_turnover_stats.csv"
    net_path = freq_mod.OUTPUT_DIR / "microcap_top100_mom16_hedge_zz1000_monthly_16y_costed_nav.csv"
    summary_path = freq_mod.OUTPUT_DIR / "microcap_top100_monthly_practical_proxy_summary.json"

    index_df.to_csv(index_path, index=False, encoding="utf-8")
    turnover_df.to_csv(turnover_path, index=False, encoding="utf-8")
    net.to_csv(net_path, index_label="date", encoding="utf-8")
    summary_path.write_text(
        json.dumps(
            {
                "strategy": "top100_monthly_practical_proxy",
                "end_date": str(index_df["date"].iloc[-1].date()),
                "index_rows": int(len(index_df)),
                "turnover_rows": int(len(turnover_df)),
                "summary": summary,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    print(json.dumps(json.loads(summary_path.read_text(encoding="utf-8")), ensure_ascii=False, indent=2))
    print(f"saved {index_path.name}")
    print(f"saved {turnover_path.name}")
    print(f"saved {net_path.name}")
    print(f"saved {summary_path.name}")


if __name__ == "__main__":
    main()
