from pathlib import Path
import json

import matplotlib.pyplot as plt
import pandas as pd


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"

SERIES_SOURCES = {
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

SUMMARY_CSV = OUTPUT_DIR / "microcap_top100_mom16_versions_1_0_1_1_1_2_windows.csv"
SUMMARY_JSON = OUTPUT_DIR / "microcap_top100_mom16_versions_1_0_1_1_1_2_summary.json"
PLOT_PNG = OUTPUT_DIR / "microcap_top100_mom16_versions_1_0_1_1_1_2_recent10y_compare.png"
REBASed_CSV = OUTPUT_DIR / "microcap_top100_mom16_versions_1_0_1_1_1_2_recent10y_rebased_nav.csv"


def summarize_returns(ret: pd.Series) -> dict:
    ret = ret.fillna(0.0)
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


def summarize_window(ret: pd.Series, start: pd.Timestamp, label: str) -> dict | None:
    seg = ret.loc[ret.index >= start].dropna()
    if len(seg) < 20:
        return None
    out = summarize_returns(seg)
    out["window"] = label
    return out


def load_series(path: Path, column: str) -> pd.Series:
    df = pd.read_csv(path, parse_dates=["date"]).sort_values("date").set_index("date")
    return df[column].astype(float)


def load_latest_series(candidates: list[tuple[Path, str]]) -> pd.Series:
    best_series = None
    best_latest = None
    best_path = None
    for path, column in candidates:
        if not path.exists():
            continue
        series = load_series(path, column)
        latest = pd.Timestamp(series.index[-1])
        if best_latest is None or latest > best_latest:
            best_series = series
            best_latest = latest
            best_path = path
    if best_series is None:
        raise FileNotFoundError(f"No usable series source found: {candidates}")
    return best_series


def main() -> None:
    series_map = {version: load_latest_series(candidates) for version, candidates in SERIES_SOURCES.items()}

    latest = max(series.index[-1] for series in series_map.values())
    windows = [
        ("ytd", pd.Timestamp(year=latest.year, month=1, day=1)),
        ("1y", latest - pd.DateOffset(years=1)),
        ("3y", latest - pd.DateOffset(years=3)),
        ("5y", latest - pd.DateOffset(years=5)),
        ("10y", latest - pd.DateOffset(years=10)),
        ("15y", latest - pd.DateOffset(years=15)),
    ]

    rows = []
    payload = {"as_of_date": str(pd.Timestamp(latest).date()), "versions": {}}
    for version, ret in series_map.items():
        payload["versions"][version] = {"full_sample": summarize_returns(ret)}
        for label, start in windows:
            item = summarize_window(ret, start, label)
            if item is None:
                continue
            item["version"] = version
            rows.append(item)
            payload["versions"][version][label] = item

    summary_df = pd.DataFrame(rows)
    summary_df.to_csv(SUMMARY_CSV, index=False, encoding="utf-8-sig")

    start = latest - pd.DateOffset(years=10)
    rebased = {}
    for version, ret in series_map.items():
        nav = (1.0 + ret.fillna(0.0)).cumprod()
        seg = nav.loc[nav.index >= start].copy()
        seg = seg / seg.iloc[0]
        rebased[version] = seg
    pd.DataFrame(rebased).to_csv(REBASed_CSV, index_label="date")

    plt.figure(figsize=(13, 7))
    for version, nav in rebased.items():
        plt.plot(nav.index, nav.values, linewidth=2.0, label=version)
    plt.title("Top100 Mom16 Versions 1.0 vs 1.1 vs 1.2 - Recent 10Y")
    plt.ylabel("Rebased NAV")
    plt.grid(alpha=0.25)
    plt.legend()
    plt.tight_layout()
    plt.savefig(PLOT_PNG, dpi=160)
    plt.close()

    payload["artifacts"] = {
        "summary_csv": str(SUMMARY_CSV),
        "plot_png": str(PLOT_PNG),
        "rebased_nav_csv": str(REBASed_CSV),
    }
    SUMMARY_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(str(SUMMARY_CSV))
    print(str(PLOT_PNG))


if __name__ == "__main__":
    main()
