import json
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"

SERIES_FILES = {
    "v1.0": [
        (OUTPUT_DIR / "microcap_top100_mom16_biweekly_live_nav.csv", "return"),
        (OUTPUT_DIR / "microcap_top100_mom16_hedge_zz1000_biweekly_thursday_16y_costed_nav.csv", "return_net"),
    ],
    "v1.1": [
        (OUTPUT_DIR / "microcap_top100_mom16_biweekly_live_v1_1_nav.csv", "return"),
        (OUTPUT_DIR / "microcap_top100_mom16_hedge_zz1000_0p8x_biweekly_thursday_16y_costed_nav.csv", "return_net"),
    ],
    "v1.2": [
        (OUTPUT_DIR / "microcap_top100_mom16_biweekly_live_v1_2_nav.csv", "return_net_v1_2"),
        (OUTPUT_DIR / "microcap_top100_mom16_hedge_zz1000_0p8x_nav4_8_biweekly_thursday_16y_costed_nav.csv", "return_net_v1_2"),
    ],
}

SEGMENTS = [
    ("2010-2015", "2010-01-01", "2015-12-31"),
    ("2016-2020", "2016-01-01", "2020-12-31"),
    ("2021-2026YTD", "2021-01-01", "2026-04-10"),
    ("2024-2026YTD", "2024-01-01", "2026-04-10"),
]

ROLLING_WINDOWS = [1, 3, 5]

SUMMARY_JSON = OUTPUT_DIR / "microcap_top100_mom16_versions_overfit_check_summary.json"
YEARLY_CSV = OUTPUT_DIR / "microcap_top100_mom16_versions_overfit_check_yearly.csv"
SEGMENT_CSV = OUTPUT_DIR / "microcap_top100_mom16_versions_overfit_check_segments.csv"
ROLLING_DETAIL_CSV = OUTPUT_DIR / "microcap_top100_mom16_versions_overfit_check_rolling_detail.csv"
ROLLING_SUMMARY_CSV = OUTPUT_DIR / "microcap_top100_mom16_versions_overfit_check_rolling_summary.csv"


def load_series(path: Path, column: str) -> pd.Series:
    df = pd.read_csv(path, parse_dates=["date"]).sort_values("date").set_index("date")
    return df[column].astype(float).fillna(0.0)


def load_latest_series(candidates: list[tuple[Path, str]]) -> pd.Series:
    best_series = None
    best_latest = None
    for path, column in candidates:
        if not path.exists():
            continue
        series = load_series(path, column)
        latest = pd.Timestamp(series.index[-1])
        if best_latest is None or latest > best_latest:
            best_series = series
            best_latest = latest
    if best_series is None:
        raise FileNotFoundError(f"No usable series source found: {candidates}")
    return best_series


def calc_metrics(ret: pd.Series) -> dict[str, float | int | str]:
    ret = ret.dropna()
    nav = (1.0 + ret).cumprod()
    years = (ret.index[-1] - ret.index[0]).days / 365.25
    annual = nav.iloc[-1] ** (1.0 / years) - 1.0 if years > 0 else 0.0
    vol = ret.std(ddof=1) * (252**0.5)
    sharpe = annual / vol if vol > 0 else 0.0
    dd = nav / nav.cummax() - 1.0
    return {
        "start_date": str(pd.Timestamp(ret.index[0]).date()),
        "end_date": str(pd.Timestamp(ret.index[-1]).date()),
        "days": int(len(ret)),
        "annual_return": float(annual),
        "annual_vol": float(vol),
        "sharpe": float(sharpe),
        "max_drawdown": float(dd.min()),
        "total_return": float(nav.iloc[-1] - 1.0),
    }


def compare_to_baseline(candidate: dict, baseline: dict) -> dict[str, bool | int]:
    better_return = candidate["annual_return"] > baseline["annual_return"]
    better_sharpe = candidate["sharpe"] > baseline["sharpe"]
    better_drawdown = candidate["max_drawdown"] > baseline["max_drawdown"]
    improved_count = int(better_return) + int(better_sharpe) + int(better_drawdown)
    return {
        "better_return": better_return,
        "better_sharpe": better_sharpe,
        "better_drawdown": better_drawdown,
        "improved_metric_count": improved_count,
        "improved_majority": improved_count >= 2,
        "improved_all_three": improved_count == 3,
    }


def build_yearly_table(series_map: dict[str, pd.Series]) -> pd.DataFrame:
    rows = []
    years = sorted(set().union(*[set(series.index.year) for series in series_map.values()]))
    for year in years:
        metrics_by_version = {}
        for version, series in series_map.items():
            part = series[series.index.year == year]
            if len(part) < 20:
                continue
            metrics_by_version[version] = calc_metrics(part)

        if "v1.0" not in metrics_by_version:
            continue
        baseline = metrics_by_version["v1.0"]

        for version, metrics in metrics_by_version.items():
            row = {"year": str(year), "version": version, **metrics}
            if version != "v1.0":
                row.update(compare_to_baseline(metrics, baseline))
            rows.append(row)
    return pd.DataFrame(rows)


def build_segment_table(series_map: dict[str, pd.Series]) -> pd.DataFrame:
    rows = []
    for label, start, end in SEGMENTS:
        metrics_by_version = {}
        for version, series in series_map.items():
            part = series.loc[(series.index >= pd.Timestamp(start)) & (series.index <= pd.Timestamp(end))]
            if len(part) < 20:
                continue
            metrics_by_version[version] = calc_metrics(part)

        baseline = metrics_by_version.get("v1.0")
        if baseline is None:
            continue

        for version, metrics in metrics_by_version.items():
            row = {"segment": label, "version": version, **metrics}
            if version != "v1.0":
                row.update(compare_to_baseline(metrics, baseline))
            rows.append(row)
    return pd.DataFrame(rows)


def monthly_checkpoints(index: pd.DatetimeIndex) -> pd.DatetimeIndex:
    frame = pd.DataFrame(index=index)
    frame["month"] = frame.index.to_period("M")
    return pd.DatetimeIndex(frame.groupby("month").tail(1).index)


def build_rolling_tables(series_map: dict[str, pd.Series]) -> tuple[pd.DataFrame, pd.DataFrame]:
    baseline = series_map["v1.0"]
    checkpoints = monthly_checkpoints(baseline.index)
    detail_rows = []

    for years in ROLLING_WINDOWS:
        for end_dt in checkpoints:
            start_dt = end_dt - pd.DateOffset(years=years)
            metrics_by_version = {}
            for version, series in series_map.items():
                part = series.loc[(series.index >= start_dt) & (series.index <= end_dt)]
                if len(part) < 120:
                    continue
                metrics_by_version[version] = calc_metrics(part)

            if "v1.0" not in metrics_by_version:
                continue
            baseline_metrics = metrics_by_version["v1.0"]

            for version, metrics in metrics_by_version.items():
                row = {
                    "window_years": years,
                    "end_date": str(pd.Timestamp(end_dt).date()),
                    "version": version,
                    **metrics,
                }
                if version != "v1.0":
                    row.update(compare_to_baseline(metrics, baseline_metrics))
                detail_rows.append(row)

    detail_df = pd.DataFrame(detail_rows)
    summary_rows = []
    for years in ROLLING_WINDOWS:
        for version in ("v1.1", "v1.2"):
            part = detail_df[(detail_df["window_years"] == years) & (detail_df["version"] == version)]
            if part.empty:
                continue
            summary_rows.append(
                {
                    "window_years": years,
                    "version": version,
                    "checkpoints": int(len(part)),
                    "share_better_return": float(part["better_return"].mean()),
                    "share_better_sharpe": float(part["better_sharpe"].mean()),
                    "share_better_drawdown": float(part["better_drawdown"].mean()),
                    "share_improved_majority": float(part["improved_majority"].mean()),
                    "share_improved_all_three": float(part["improved_all_three"].mean()),
                }
            )
    return detail_df, pd.DataFrame(summary_rows)


def summarize_win_rates(df: pd.DataFrame, group_col: str) -> dict:
    out = {}
    for version in ("v1.1", "v1.2"):
        part = df[df["version"] == version]
        if part.empty:
            continue
        out[version] = {
            "samples": int(len(part)),
            "better_return_share": float(part["better_return"].mean()),
            "better_sharpe_share": float(part["better_sharpe"].mean()),
            "better_drawdown_share": float(part["better_drawdown"].mean()),
            "improved_majority_share": float(part["improved_majority"].mean()),
            "improved_all_three_share": float(part["improved_all_three"].mean()),
            f"{group_col}_wins": part[[group_col, "improved_majority"]].to_dict(orient="records"),
        }
    return out


def main() -> None:
    series_map = {version: load_latest_series(candidates) for version, candidates in SERIES_FILES.items()}

    yearly_df = build_yearly_table(series_map)
    segment_df = build_segment_table(series_map)
    rolling_detail_df, rolling_summary_df = build_rolling_tables(series_map)

    yearly_df.to_csv(YEARLY_CSV, index=False, encoding="utf-8-sig")
    segment_df.to_csv(SEGMENT_CSV, index=False, encoding="utf-8-sig")
    rolling_detail_df.to_csv(ROLLING_DETAIL_CSV, index=False, encoding="utf-8-sig")
    rolling_summary_df.to_csv(ROLLING_SUMMARY_CSV, index=False, encoding="utf-8-sig")

    payload = {
        "as_of_date": str(pd.Timestamp(max(s.index.max() for s in series_map.values())).date()),
        "method_note": "This is a robustness-style overfit check, not a formal statistical proof. It asks whether v1.1/v1.2 beat v1.0 broadly across years, regimes, and rolling windows on return, Sharpe, and max drawdown.",
        "full_sample": {version: calc_metrics(series) for version, series in series_map.items()},
        "yearly_consistency_vs_v1_0": summarize_win_rates(yearly_df, "year"),
        "segment_consistency_vs_v1_0": summarize_win_rates(segment_df, "segment"),
        "rolling_consistency_vs_v1_0": rolling_summary_df.to_dict(orient="records"),
        "artifacts": {
            "yearly_csv": str(YEARLY_CSV),
            "segment_csv": str(SEGMENT_CSV),
            "rolling_detail_csv": str(ROLLING_DETAIL_CSV),
            "rolling_summary_csv": str(ROLLING_SUMMARY_CSV),
        },
    }
    SUMMARY_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
