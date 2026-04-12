from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

import microcap_top100_mom16_biweekly_live as v1_0_mod
import validate_top100_versions_consistency as validate_mod


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"

VERSION_CONFIGS = {
    "v1.0": {
        "script": "microcap_top100_mom16_biweekly_live.py",
        "path": OUTPUT_DIR / "microcap_top100_mom16_hedge_zz1000_biweekly_thursday_16y_costed_nav.csv",
        "return_col": "return_net",
        "nav_col": "nav_net",
        "source": "costed",
    },
    "v1.1": {
        "script": "microcap_top100_mom16_biweekly_live_v1_1.py",
        "path": OUTPUT_DIR / "microcap_top100_mom16_hedge_zz1000_0p8x_biweekly_thursday_16y_costed_nav.csv",
        "return_col": "return_net",
        "nav_col": "nav_net",
        "source": "costed",
    },
    "v1.2": {
        "script": "microcap_top100_mom16_biweekly_live_v1_2.py",
        "path": OUTPUT_DIR / "microcap_top100_mom16_hedge_zz1000_0p8x_nav4_8_biweekly_thursday_16y_costed_nav.csv",
        "return_col": "return_net_v1_2",
        "nav_col": "nav_net_v1_2",
        "source": "costed_v1_2",
    },
}

OUTPUT_STEM = "microcap_top100_mom16_versions_1_0_1_1_1_2"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh v1.0/v1.1/v1.2 and build a recent-window costed NAV compare chart.")
    parser.add_argument("--years", type=int, default=2, help="Recent trailing years window to plot.")
    parser.add_argument("--skip-refresh", action="store_true", help="Reuse existing version outputs without refreshing.")
    parser.add_argument("--run-validation", action="store_true", help="Also run version consistency validation before rendering.")
    return parser.parse_args()


def build_artifact_paths(years: int) -> dict[str, Path]:
    suffix = f"recent{years}y"
    return {
        "summary_csv": OUTPUT_DIR / f"{OUTPUT_STEM}_{suffix}_windows.csv",
        "summary_json": OUTPUT_DIR / f"{OUTPUT_STEM}_{suffix}_summary.json",
        "plot_png": OUTPUT_DIR / f"{OUTPUT_STEM}_{suffix}_compare.png",
        "rebased_csv": OUTPUT_DIR / f"{OUTPUT_STEM}_{suffix}_rebased_nav.csv",
    }


def build_v1_0_args(max_workers: int = 8) -> argparse.Namespace:
    return argparse.Namespace(
        query_tokens=[],
        panel_path=v1_0_mod.hedge_mod.DEFAULT_PANEL,
        index_csv=v1_0_mod.DEFAULT_INDEX_CSV,
        costed_nav_csv=v1_0_mod.DEFAULT_COSTED_NAV_CSV,
        output_prefix=v1_0_mod.DEFAULT_OUTPUT_PREFIX,
        capital=None,
        max_workers=max_workers,
        realtime_cache_seconds=30,
        rebuild_index_if_missing=True,
        force_refresh=False,
        max_stale_anchor_days=v1_0_mod.DEFAULT_MAX_STALE_ANCHOR_DAYS,
        allow_stale_realtime=False,
    )


def detect_target_end_date() -> pd.Timestamp:
    args = build_v1_0_args()
    paths = v1_0_mod.build_output_paths(args.output_prefix)
    _, target_end_date = v1_0_mod.build_refreshed_panel_shadow(args, paths)
    return pd.Timestamp(target_end_date).normalize()


def build_refresh_query(years: int) -> str:
    if years <= 1:
        return "净值图 最近一年"
    return f"净值图 最近{years}年"


def read_csv_last_date(path: Path, date_col: str = "date") -> pd.Timestamp | None:
    if not path.exists():
        return None
    frame = pd.read_csv(path, usecols=[date_col])
    if frame.empty:
        return None
    return pd.Timestamp(frame[date_col].iloc[-1]).normalize()


def version_is_fresh(version: str, target_end_date: pd.Timestamp) -> bool:
    last_date = read_csv_last_date(Path(VERSION_CONFIGS[version]["path"]))
    return last_date is not None and last_date >= pd.Timestamp(target_end_date).normalize()


def refresh_version_outputs(version: str, years: int) -> None:
    script = str(VERSION_CONFIGS[version]["script"])
    query = build_refresh_query(years)
    subprocess.run([sys.executable, script, *query.split()], cwd=ROOT, check=True)


def ensure_all_versions_refreshed(years: int) -> None:
    target_end_date = detect_target_end_date()
    for version in ("v1.0", "v1.1", "v1.2"):
        if version_is_fresh(version, target_end_date):
            continue
        refresh_version_outputs(version, years)


def load_version_series(version: str) -> tuple[pd.Series, dict[str, str]]:
    config = VERSION_CONFIGS[version]
    path = Path(config["path"])
    if not path.exists():
        raise FileNotFoundError(f"Missing required {version} costed NAV file: {path}")
    frame = pd.read_csv(path, parse_dates=["date"]).sort_values("date").set_index("date")
    ret_col = str(config["return_col"])
    nav_col = str(config["nav_col"])
    if ret_col not in frame.columns:
        raise KeyError(f"Column {ret_col!r} not found in {path.name}.")
    if nav_col not in frame.columns:
        raise KeyError(f"Column {nav_col!r} not found in {path.name}.")
    return frame[ret_col].astype(float), {
        "path": str(path),
        "return_col": ret_col,
        "nav_col": nav_col,
        "source": str(config["source"]),
    }


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


def build_common_recent_index(series_map: dict[str, pd.Series], years: int) -> tuple[pd.DatetimeIndex, pd.Timestamp]:
    latest = max(pd.Timestamp(series.index.max()) for series in series_map.values())
    start = latest - pd.DateOffset(years=years)
    common_index = None
    for version, ret in series_map.items():
        seg_index = ret.loc[(ret.index >= start) & (ret.index <= latest)].index
        if len(seg_index) < 20:
            raise ValueError(f"Not enough rows for {version} in recent {years}Y window.")
        common_index = seg_index if common_index is None else common_index.intersection(seg_index)
    if common_index is None or len(common_index) < 20:
        raise ValueError(f"Not enough common rows across versions for recent {years}Y compare window.")
    return pd.DatetimeIndex(common_index), latest


def build_recent_rebased(series_map: dict[str, pd.Series], years: int) -> tuple[pd.DataFrame, pd.Timestamp]:
    common_index, latest = build_common_recent_index(series_map, years)
    rebased: dict[str, pd.Series] = {}
    for version, ret in series_map.items():
        seg = ret.loc[common_index].fillna(0.0)
        nav = (1.0 + seg).cumprod()
        rebased[version] = nav / float(nav.iloc[0])
    return pd.DataFrame(rebased), latest


def render_compare_chart(rebased: pd.DataFrame, output_path: Path, years: int, as_of_date: pd.Timestamp) -> Path:
    plt.figure(figsize=(13, 7))
    for version in ("v1.0", "v1.1", "v1.2"):
        plt.plot(rebased.index, rebased[version], linewidth=2.0, label=version)
    plt.title(f"Top100 Mom16 Versions 1.0 vs 1.1 vs 1.2 - Recent {years}Y (costed, as of {as_of_date.date()})")
    plt.ylabel("Rebased NAV")
    plt.grid(alpha=0.25)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=160)
    plt.close()
    return output_path


def main() -> None:
    args = parse_args()
    if not args.skip_refresh:
        ensure_all_versions_refreshed(args.years)
    validation_payload = validate_mod.run_validation() if args.run_validation else None

    series_map: dict[str, pd.Series] = {}
    source_meta: dict[str, dict[str, str]] = {}
    for version in ("v1.0", "v1.1", "v1.2"):
        ret, meta = load_version_series(version)
        series_map[version] = ret
        source_meta[version] = meta

    rebased, latest = build_recent_rebased(series_map, years=args.years)
    artifacts = build_artifact_paths(args.years)

    summary_rows = []
    common_index, _ = build_common_recent_index(series_map, years=args.years)
    for version, ret in series_map.items():
        seg = ret.loc[common_index]
        row = summarize_returns(seg)
        row["version"] = version
        row["window"] = f"recent{args.years}y"
        row["source"] = source_meta[version]["source"]
        summary_rows.append(row)
    pd.DataFrame(summary_rows).to_csv(artifacts["summary_csv"], index=False, encoding="utf-8-sig")

    rebased.to_csv(artifacts["rebased_csv"], index_label="date", encoding="utf-8-sig")
    render_compare_chart(rebased, artifacts["plot_png"], args.years, latest)

    payload = {
        "as_of_date": str(latest.date()),
        "window_years": int(args.years),
        "source_policy": "refresh stale versions via each script's fast performance query, then compare costed NAV only",
        "validation_summary_json": str(validate_mod.VALIDATION_JSON) if validation_payload is not None else None,
        "validation_as_of_date": validation_payload["as_of_date"] if validation_payload is not None else None,
        "versions": {version: {**source_meta[version], **summarize_returns(series_map[version].fillna(0.0))} for version in series_map},
        "recent_window": summary_rows,
        "artifacts": {key: str(path) for key, path in artifacts.items()},
    }
    artifacts["summary_json"].write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(str(artifacts["summary_csv"]))
    print(str(artifacts["plot_png"]))


if __name__ == "__main__":
    main()
