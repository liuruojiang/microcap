from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt

import microcap_top100_mom16_biweekly_live_v1_1 as v1_1_mod


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"

MAINLINE_PREFIX = "microcap_top100_mom16_biweekly_live_v1_1"
MAINLINE_NAV_FILENAME = f"{MAINLINE_PREFIX}_nav.csv"
MAINLINE_SUMMARY_FILENAME = f"{MAINLINE_PREFIX}_summary.json"
RECENT3Y_CHART_FILENAME = f"{MAINLINE_PREFIX}_recent3y_curve.png"
COSTED_NAV_FILENAME = "microcap_top100_mom16_hedge_zz1000_0p8x_biweekly_thursday_16y_costed_nav.csv"
INDEX_CACHE_FILENAME = "wind_microcap_top_100_biweekly_thursday_16y_cached.csv"
PROXY_TURNOVER_FILENAME = f"{MAINLINE_PREFIX}_proxy_turnover.csv"


def build_mainline_args(max_workers: int = 8) -> argparse.Namespace:
    base_mod = v1_1_mod.base_mod
    return argparse.Namespace(
        query_tokens=[],
        panel_path=base_mod.hedge_mod.DEFAULT_PANEL,
        index_csv=base_mod.DEFAULT_INDEX_CSV,
        costed_nav_csv=base_mod.DEFAULT_COSTED_NAV_CSV,
        output_prefix=base_mod.DEFAULT_OUTPUT_PREFIX,
        capital=None,
        max_workers=max_workers,
        realtime_cache_seconds=30,
        rebuild_index_if_missing=True,
        force_refresh=False,
        max_stale_anchor_days=base_mod.DEFAULT_MAX_STALE_ANCHOR_DAYS,
        allow_stale_realtime=False,
    )


def refresh_mainline_outputs() -> dict[str, object]:
    base_mod = v1_1_mod.base_mod
    args = build_mainline_args()
    paths = base_mod.build_output_paths(args.output_prefix)
    resolved_panel_path, target_end_date = base_mod.build_refreshed_panel_shadow(args, paths)
    base_mod.ensure_strategy_files(args, paths, resolved_panel_path, target_end_date)
    close_df = base_mod.load_close_df(resolved_panel_path, args.index_csv)
    result = base_mod.run_signal(close_df)
    result.to_csv(paths["nav"], index_label="date", encoding="utf-8")
    synchronize_costed_nav_dates(result.index, args.costed_nav_csv)
    return {
        "args": args,
        "paths": paths,
        "resolved_panel_path": resolved_panel_path,
        "target_end_date": pd.Timestamp(target_end_date),
        "result": result,
    }


def synchronize_costed_nav_dates(expected_index: pd.Index, costed_path: Path) -> Path:
    frame = pd.read_csv(costed_path, parse_dates=["date"]).sort_values("date").drop_duplicates(subset="date", keep="last")
    expected_dates = pd.DatetimeIndex(pd.to_datetime(expected_index))
    if not set(expected_dates).issubset(set(frame["date"])):
        missing = sorted(set(expected_dates) - set(frame["date"]))
        raise RuntimeError(f"Costed NAV is missing expected dates: {[str(pd.Timestamp(x).date()) for x in missing[:5]]}")
    synced = frame.loc[frame["date"].isin(expected_dates)].copy()
    if len(synced) != len(frame):
        synced.to_csv(costed_path, index=False, encoding="utf-8")
    return costed_path


def resolve_performance_source(output_dir: Path = OUTPUT_DIR) -> tuple[Path, str, str]:
    candidates = [
        (output_dir / COSTED_NAV_FILENAME, "return_net", "costed"),
        (output_dir / MAINLINE_NAV_FILENAME, "return", "gross_fallback"),
    ]
    for path, ret_col, source_label in candidates:
        if path.exists():
            return path, ret_col, source_label
    raise FileNotFoundError("No usable v1.1 performance source found in outputs.")


def load_returns_frame(path: Path, ret_col: str) -> pd.DataFrame:
    frame = pd.read_csv(path, parse_dates=["date"]).sort_values("date")
    if ret_col not in frame.columns:
        raise KeyError(f"Column {ret_col!r} not found in {path.name}.")
    return frame.set_index("date")


def build_recent_window_nav(
    frame: pd.DataFrame,
    ret_col: str,
    years: int = 3,
    end_date: pd.Timestamp | None = None,
) -> pd.DataFrame:
    if frame.empty:
        raise ValueError("Performance source is empty.")
    if end_date is None:
        end_date = pd.Timestamp(frame.index.max())
    start_date = pd.Timestamp(end_date) - pd.DateOffset(years=years)
    window = frame.loc[(frame.index >= start_date) & (frame.index <= end_date)].copy()
    if window.empty:
        raise ValueError(f"No rows found in the last {years} years.")
    returns = window[ret_col].astype(float).fillna(0.0)
    window["nav_rebased"] = (1.0 + returns).cumprod()
    return window


def render_recent_window_chart(
    window: pd.DataFrame,
    output_path: Path,
    title: str,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(window.index, window["nav_rebased"], linewidth=2.0, color="#1f4e79")
    ax.set_title(title)
    ax.set_ylabel("Rebased NAV")
    ax.grid(True, alpha=0.25)
    fig.tight_layout()
    fig.savefig(output_path, dpi=160)
    plt.close(fig)
    return output_path


def generate_recent3y_chart(output_dir: Path = OUTPUT_DIR) -> Path:
    refresh_state = refresh_mainline_outputs()
    source_path, ret_col, source_label = resolve_performance_source(output_dir)
    if source_label != "costed":
        raise RuntimeError("v1.1 recent NAV chart requires refreshed costed NAV data.")
    frame = load_returns_frame(source_path, ret_col)
    if pd.Timestamp(frame.index.max()).normalize() < pd.Timestamp(refresh_state["target_end_date"]).normalize():
        raise RuntimeError(
            "Refreshed v1.1 costed NAV is still stale after sync: "
            f"{pd.Timestamp(frame.index.max()).date()} < {pd.Timestamp(refresh_state['target_end_date']).date()}"
        )
    window = build_recent_window_nav(frame, ret_col=ret_col, years=3)
    latest_date = pd.Timestamp(window.index.max()).date()
    title = f"Top100 Microcap Mom16 Biweekly v1.1 Recent 3Y ({source_label}, as of {latest_date})"
    return render_recent_window_chart(window, output_dir / RECENT3Y_CHART_FILENAME, title)


def keep_output_filenames() -> set[str]:
    keep = {
        INDEX_CACHE_FILENAME,
        COSTED_NAV_FILENAME,
        MAINLINE_NAV_FILENAME,
        MAINLINE_SUMMARY_FILENAME,
        PROXY_TURNOVER_FILENAME,
        RECENT3Y_CHART_FILENAME,
    }
    keep.update(
        {
            f"{MAINLINE_PREFIX}_latest_signal.csv",
            f"{MAINLINE_PREFIX}_panel_refreshed.csv",
            f"{MAINLINE_PREFIX}_proxy_members.csv",
            f"{MAINLINE_PREFIX}_proxy_meta.json",
            f"{MAINLINE_PREFIX}_realtime_rebalance_changes.csv",
            f"{MAINLINE_PREFIX}_realtime_signal.csv",
            f"{MAINLINE_PREFIX}_realtime_target_members.csv",
            f"{MAINLINE_PREFIX}_rebalance_changes.csv",
            f"{MAINLINE_PREFIX}_target_members.csv",
        }
    )
    return keep


def plan_output_cleanup(output_dir: Path = OUTPUT_DIR) -> list[Path]:
    keep = keep_output_filenames()
    deletions: list[Path] = []
    for path in output_dir.rglob("*"):
        if not path.is_file():
            continue
        rel_name = path.relative_to(output_dir).as_posix()
        if "/" not in rel_name and path.name in keep:
            continue
        deletions.append(path)
    return sorted(deletions)


def delete_planned_outputs(output_dir: Path = OUTPUT_DIR) -> list[Path]:
    deletions = plan_output_cleanup(output_dir)
    for path in deletions:
        path.unlink(missing_ok=True)
    for path in sorted(output_dir.rglob("*"), reverse=True):
        if path.is_dir():
            try:
                path.rmdir()
            except OSError:
                pass
    return deletions


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Utilities for the Top100 v1.1 mainline outputs.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("chart", help="Generate the v1.1 recent-3-year NAV chart.")

    clean_parser = subparsers.add_parser("clean", help="Delete non-mainline files under outputs.")
    clean_parser.add_argument("--execute", action="store_true", help="Actually delete files instead of dry-run.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.command == "chart":
        chart_path = generate_recent3y_chart()
        print(chart_path)
        return
    if args.command == "clean":
        deletions = delete_planned_outputs() if args.execute else plan_output_cleanup()
        for path in deletions:
            print(path)
        return
    raise ValueError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    main()
