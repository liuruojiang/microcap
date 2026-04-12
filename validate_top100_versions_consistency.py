from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"

VALIDATION_JSON = OUTPUT_DIR / "microcap_top100_versions_validation_summary.json"

VERSION_FILES = {
    "v1.0": {
        "gross_path": OUTPUT_DIR / "microcap_top100_mom16_biweekly_live_nav.csv",
        "gross_return_col": "return",
        "costed_path": OUTPUT_DIR / "microcap_top100_mom16_hedge_zz1000_biweekly_thursday_16y_costed_nav.csv",
        "costed_return_col": "return_net",
    },
    "v1.1": {
        "gross_path": OUTPUT_DIR / "microcap_top100_mom16_biweekly_live_v1_1_nav.csv",
        "gross_return_col": "return",
        "costed_path": OUTPUT_DIR / "microcap_top100_mom16_hedge_zz1000_0p8x_biweekly_thursday_16y_costed_nav.csv",
        "costed_return_col": "return_net",
    },
}


def load_return_series(path: Path, return_col: str) -> pd.Series:
    frame = pd.read_csv(path, parse_dates=["date"]).sort_values("date").drop_duplicates(subset="date", keep="last")
    if return_col not in frame.columns:
        raise KeyError(f"Column {return_col!r} not found in {path.name}.")
    return frame.set_index("date")[return_col].astype(float)


def summarize_returns(ret: pd.Series) -> dict[str, float | int | str]:
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


def validate_version_pair(
    version: str,
    gross_path: Path,
    gross_return_col: str,
    costed_path: Path,
    costed_return_col: str,
) -> dict[str, object]:
    gross = load_return_series(gross_path, gross_return_col)
    costed = load_return_series(costed_path, costed_return_col)

    if not gross.index.equals(costed.index):
        gross_only = sorted(set(gross.index) - set(costed.index))
        costed_only = sorted(set(costed.index) - set(gross.index))
        raise ValueError(
            f"{version} gross/costed date mismatch: "
            f"gross_only={[str(pd.Timestamp(x).date()) for x in gross_only[:5]]}, "
            f"costed_only={[str(pd.Timestamp(x).date()) for x in costed_only[:5]]}"
        )

    return {
        "version": version,
        "gross_path": str(gross_path),
        "costed_path": str(costed_path),
        "latest_date": str(pd.Timestamp(costed.index.max()).date()),
        "days": int(len(costed)),
        "gross_summary": summarize_returns(gross),
        "costed_summary": summarize_returns(costed),
    }


def build_recent_window_summary(
    series_map: dict[str, pd.Series],
    years_list: list[int],
    as_of_date: pd.Timestamp | None = None,
) -> dict[str, object]:
    latest = max(pd.Timestamp(series.index.max()) for series in series_map.values()) if as_of_date is None else pd.Timestamp(as_of_date)
    out: dict[str, object] = {}
    for years in years_list:
        start = latest - pd.DateOffset(years=years)
        common_index = None
        for series in series_map.values():
            seg_index = series.loc[(series.index >= start) & (series.index <= latest)].index
            common_index = seg_index if common_index is None else common_index.intersection(seg_index)
        if common_index is None or len(common_index) < 20:
            raise ValueError(f"Not enough common rows across versions for recent {years}Y validation.")
        out[f"recent{years}y"] = {
            "common_start_date": str(pd.Timestamp(common_index.min()).date()),
            "common_end_date": str(pd.Timestamp(common_index.max()).date()),
            "versions": {
                version: summarize_returns(series.loc[common_index].fillna(0.0)) for version, series in series_map.items()
            },
        }
    return out


def run_validation() -> dict[str, object]:
    pair_results = {}
    costed_series_map: dict[str, pd.Series] = {}
    for version, cfg in VERSION_FILES.items():
        result = validate_version_pair(
            version=version,
            gross_path=Path(cfg["gross_path"]),
            gross_return_col=str(cfg["gross_return_col"]),
            costed_path=Path(cfg["costed_path"]),
            costed_return_col=str(cfg["costed_return_col"]),
        )
        pair_results[version] = result
        costed_series_map[version] = load_return_series(Path(cfg["costed_path"]), str(cfg["costed_return_col"]))

    recent_windows = build_recent_window_summary(costed_series_map, years_list=[2, 3, 5])
    latest_dates = {version: result["latest_date"] for version, result in pair_results.items()}
    unique_latest_dates = sorted(set(latest_dates.values()))
    if len(unique_latest_dates) != 1:
        raise ValueError(f"Latest dates do not align across versions: {latest_dates}")

    payload = {
        "status": "ok",
        "as_of_date": unique_latest_dates[0],
        "validated_versions": pair_results,
        "recent_windows": recent_windows,
    }
    VALIDATION_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def main() -> None:
    payload = run_validation()
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
