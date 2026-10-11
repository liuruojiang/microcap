"""Exercise the real no-rebalance extension on disposable copies of frozen data."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import shutil
import sys
import tempfile
from types import SimpleNamespace

import pandas as pd


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
sys.path.insert(0, str(ROOT))
import microcap_top100_mom16_biweekly_live_v2_0 as runtime


INDEX = "wind_microcap_top_100_biweekly_thursday_16y_cached.csv"
MEMBERS = "microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv"
EFFECTIVE = "microcap_top100_mom16_biweekly_live_v2_0_base_proxy_effective_members.csv"
TURNOVER = "microcap_top100_mom16_biweekly_live_v2_0_base_proxy_turnover.csv"
META = "microcap_top100_mom16_biweekly_live_v2_0_base_proxy_meta.json"
PANEL = "microcap_top100_mom16_biweekly_live_v2_0_base_panel_refreshed.csv"


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def make_case(folder: Path, bridge: pd.Timestamp) -> tuple[SimpleNamespace, dict[str, Path]]:
    folder.mkdir(parents=True)
    for name in (MEMBERS, EFFECTIVE, TURNOVER, META):
        shutil.copyfile(ROOT / "outputs" / name, folder / name)
    official = pd.read_csv(ROOT / "outputs" / INDEX)
    official[pd.to_datetime(official["date"]).le(bridge)].to_csv(folder / INDEX, index=False)
    args = SimpleNamespace(index_csv=folder / INDEX, max_workers=4, force_refresh=False)
    paths = {
        "proxy_members": folder / MEMBERS,
        "proxy_effective_members": folder / EFFECTIVE,
        "proxy_turnover": folder / TURNOVER,
        "proxy_meta": folder / META,
    }
    return args, paths


def main() -> None:
    source = ROOT / "outputs" / INDEX
    protected = {name: sha(ROOT / "outputs" / name) for name in (INDEX, MEMBERS, EFFECTIVE, TURNOVER, META, PANEL)}
    official = pd.read_csv(source)
    official["date"] = pd.to_datetime(official["date"])
    bridge = pd.Timestamp("2026-09-17")
    midway = pd.Timestamp("2026-09-23")
    terminal = pd.Timestamp("2026-09-30")
    panel = ROOT / "outputs" / PANEL
    base = runtime.base_mod
    # The entrypoint dynamically loads the base implementation; patch the
    # function's own globals, not an alias on a different module object.
    extension_globals = base.try_extend_proxy_index_without_rebalance.__globals__
    original_refresh = extension_globals["refresh_price_cache_tail"]
    extension_globals["refresh_price_cache_tail"] = lambda *args, **kwargs: None
    try:
        with tempfile.TemporaryDirectory(prefix="l7_tail_", dir=HERE) as temp:
            temp_root = Path(temp)
            batch_args, batch_paths = make_case(temp_root / "batch", bridge)
            split_args, split_paths = make_case(temp_root / "split", bridge)
            batch_first = base.try_extend_proxy_index_without_rebalance(
                batch_args, batch_paths, panel, terminal, max_tail_calendar_days=20
            )
            split_first = base.try_extend_proxy_index_without_rebalance(
                split_args, split_paths, panel, midway, max_tail_calendar_days=20
            )
            split_second = base.try_extend_proxy_index_without_rebalance(
                split_args, split_paths, panel, terminal, max_tail_calendar_days=20
            )
            before_repeat = sha(split_args.index_csv)
            repeated = base.try_extend_proxy_index_without_rebalance(
                split_args, split_paths, panel, terminal, max_tail_calendar_days=20
            )
            after_repeat = sha(split_args.index_csv)
            batch = pd.read_csv(batch_args.index_csv)
            split = pd.read_csv(split_args.index_csv)
            assert all([batch_first, split_first, split_second]) and repeated is False
            assert before_repeat == after_repeat
            assert len(batch) == len(split) == len(official)
            for frame in (batch, split):
                frame["date"] = pd.to_datetime(frame["date"])
                assert frame["date"].equals(official["date"])
            comparison = pd.DataFrame({"date": official["date"],
                                       "saved_return": official["daily_return"],
                                       "batch_return": batch["daily_return"],
                                       "split_return": split["daily_return"],
                                       "saved_close": official["close"],
                                       "batch_close": batch["close"],
                                       "split_close": split["close"]})
            comparison = comparison.loc[comparison["date"].gt(bridge)].copy()
            metrics = {
                "batch_return": float((comparison["saved_return"] - comparison["batch_return"]).abs().max()),
                "split_return": float((comparison["saved_return"] - comparison["split_return"]).abs().max()),
                "batch_close": float((comparison["saved_close"] - comparison["batch_close"]).abs().max()),
                "split_close": float((comparison["saved_close"] - comparison["split_close"]).abs().max()),
                "batch_vs_split_close": float((comparison["batch_close"] - comparison["split_close"]).abs().max()),
            }
    finally:
        extension_globals["refresh_price_cache_tail"] = original_refresh
    unchanged = all(sha(ROOT / "outputs" / name) == digest for name, digest in protected.items())
    report = {
        "status": "PASS" if unchanged and metrics["batch_return"] < 1e-12 and
                              metrics["split_return"] < 1e-12 and
                              max(metrics["batch_close"], metrics["split_close"], metrics["batch_vs_split_close"]) < 1e-7 else "FAIL",
        "actual_function": "try_extend_proxy_index_without_rebalance",
        "network_price_refresh": "suppressed on frozen local cache; no source files changed",
        "bridge": bridge.date().isoformat(),
        "midway": midway.date().isoformat(),
        "terminal": terminal.date().isoformat(),
        "batch_first": batch_first,
        "split_first": split_first,
        "split_second": split_second,
        "repeat_returned": repeated,
        "repeat_file_unchanged": before_repeat == after_repeat,
        "protected_sources_unchanged": unchanged,
        "tail_rows": len(comparison),
        "max_abs_error": metrics,
    }
    (HERE / "isolated_incremental.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if report["status"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
