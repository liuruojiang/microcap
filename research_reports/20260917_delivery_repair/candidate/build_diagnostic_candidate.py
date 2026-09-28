"""Build unapproved diagnostic candidates; never write formal strategy paths."""
import hashlib
import importlib.util
import io
import json
import sys
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
import microcap_top100_mom16_biweekly_live_v2_0 as v20
import microcap_top100_mom16_biweekly_live_v2_3 as v23
import microcap_top100_mom16_biweekly_live_v2_5 as v25

v20._sync_embedded_base_config()
base, freq, overlay = v20.base_mod, v20.base_mod.freq_mod, v20.overlay_mod
NEW_SOURCE = Path("D:/Codex/worktrees/microcap-continuation-20260917/microcap_top100_mom16_biweekly_live_v2_0.py")
spec = importlib.util.spec_from_file_location("continuation_candidate_v20", NEW_SOURCE)
patched = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = patched
spec.loader.exec_module(patched)
ARCHIVE = ROOT / "outputs/repair_20260917_cloud_evidence/cloud_20260916_whole.zip"
PFX = "microcap_top100_mom16_biweekly_live_v2_0_base_"
TARGET = pd.Timestamp("2026-09-17")
BRIDGE = pd.Timestamp("2026-09-03")
report = {"approved": False, "diagnostic_only": True, "formal_state_modified": False,
          "migration_authorized": False, "source_hashes": {}, "parity": {}, "candidates": {}}


def sha(data):
    return hashlib.sha256(data).hexdigest()


def read_local(path):
    report["source_hashes"][str(path)] = sha(path.read_bytes())
    return pd.read_csv(path)


def dated(frame):
    frame = frame.copy()
    frame["date"] = pd.to_datetime(frame.date)
    return frame.set_index("date").sort_index()


def calculate(proxy, panel, turnover):
    close = pd.concat([proxy.close.rename("microcap"), panel["1.000852"].rename("hedge")], axis=1).dropna()
    # Official loader exposes the first base signal's valid index to consumers.
    close = close.loc[base.run_signal(close).index]
    gross = base.apply_momentum_gap_exit_buffer(base.run_signal(close), overlay.V2_0_MOMENTUM_GAP_EXIT_BUFFER)
    a = overlay.apply_v2_0_execution(gross, turnover)
    b = v23.build_v2_3_result(close, turnover, v23.build_v2_3_common_index(close, a.index))
    c = v25.build_v2_5_result(close, turnover, v25.build_v2_5_common_index(close, a.index))
    return {"v2_0": a, "v2_3": b, "v2_5": c}


def compare(left, right, all_columns=False):
    columns = [c for c in left.columns.intersection(right.columns)
               if c in {"return_net", "nav", "nav_net", "holding", "next_holding", "return_raw", "signal_on",
                        "microcap_mom", "hedge_mom", "momentum_gap", "execution_scale", "leverage",
                        "total_cost", "member_cost", "switch_cost", "target_leverage"}]
    if all_columns:
        columns = list(left.columns.intersection(right.columns))
    rows = []
    for day in left.index.intersection(right.index):
        for col in columns:
            x, y = left.at[day, col], right.at[day, col]
            if pd.isna(x) and pd.isna(y):
                continue
            if isinstance(x, (float, int, np.number)) and isinstance(y, (float, int, np.number)):
                same = bool(np.isclose(x, y, rtol=1e-10, atol=1e-12, equal_nan=True))
            elif col.endswith("_date"):
                dx, dy = pd.to_datetime(x, errors="coerce"), pd.to_datetime(y, errors="coerce")
                same = (pd.isna(dx) and pd.isna(dy)) or dx == dy
            else:
                same = str(x) == str(y)
            if not same:
                rows.append({"date": str(day.date()), "field": col, "previous": x, "candidate": y})
    return {"same_dates": left.index.equals(right.index), "checked_columns": columns, "different_cells": len(rows)}, rows


def persist_report():
    (OUT / "exact_hash_migration_review.json").write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str, allow_nan=False), encoding="utf-8")


report["source_hashes"][str(ARCHIVE)] = sha(ARCHIVE.read_bytes())
report["source_hashes"][str(NEW_SOURCE)] = sha(NEW_SOURCE.read_bytes())
for module in [v20, v23, v25]:
    source = Path(module.__file__)
    report["source_hashes"][str(source)] = sha(source.read_bytes())
report["source_hashes"][str(Path(__file__).resolve())] = sha(Path(__file__).read_bytes())
with zipfile.ZipFile(ARCHIVE) as archive:
    def cloud(name):
        raw = archive.read("outputs/" + name)
        report["source_hashes"]["cloud_20260916:" + name] = sha(raw)
        return pd.read_csv(io.BytesIO(raw))
    proxy = dated(cloud("wind_microcap_top_100_biweekly_thursday_16y_cached.csv"))
    panel = dated(cloud(PFX + "panel_refreshed.csv"))
    turnover = cloud(PFX + "proxy_turnover.csv")
    turnover.rebalance_date = pd.to_datetime(turnover.rebalance_date)
    seed = cloud(PFX + "proxy_effective_members.csv")
    references = {key: dated(cloud("microcap_top100_mom16_biweekly_live_" + key + "_nav.csv")) for key in ["v2_0", "v2_3", "v2_5"]}
seed_key = "cloud_20260916:" + PFX + "proxy_effective_members.csv"
assert report["source_hashes"][seed_key] == "9558082806163587d9813100bd19cafe92337a2c086e3fc8ac6aa8a71e5825ee"
baseline = calculate(proxy, panel, turnover)
for key in baseline:
    parity, diffs = compare(references[key], baseline[key])
    report["parity"][key] = parity
if any(not x["same_dates"] or x["different_cells"] for x in report["parity"].values()):
    report["status"] = "BLOCKED_PURE_FUNCTION_PARITY"
    persist_report()
    raise SystemExit(report["status"])

local_panel = dated(read_local(ROOT / "outputs" / (PFX + "panel_refreshed.csv")))
panel = pd.concat([panel, local_panel.loc[(local_panel.index > panel.index.max()) & (local_panel.index <= TARGET)]])
dates = pd.DatetimeIndex(panel.loc[(panel.index >= BRIDGE) & (panel.index <= TARGET)].dropna(subset=["1.000852"]).index)
members_frame = read_local(ROOT / "outputs" / (PFX + "proxy_members.csv"))
members_frame.rebalance_date = pd.to_datetime(members_frame.rebalance_date)
target_rows = members_frame.loc[members_frame.rebalance_date == TARGET].sort_values("rank")
seed_members = seed.sort_values("rank").symbol.astype(str).str.zfill(6).tolist()
target_members = target_rows.symbol.astype(str).str.zfill(6).tolist()
assert len(set(seed_members)) == len(set(target_members)) == 100
symbols = sorted(set(seed_members + target_members))
returns, buys, sells = {}, {}, {}
for symbol in symbols:
    loaded = freq.load_symbol_cache(symbol, dates, pd.DatetimeIndex([]), base.TRADE_CONSTRAINT_MODE, False)
    if loaded is None:
        raise RuntimeError("Missing actual cache: " + symbol)
    returns[symbol], buys[symbol], sells[symbol] = loaded[1], loaded[3], loaded[4]
    metadata_path = ROOT / ".microcap_index_cache/security_meta" / (symbol + ".json")
    if metadata_path.exists():
        report["source_hashes"][str(metadata_path)] = sha(metadata_path.read_bytes())
    for local, shared in [(freq.PRICE_DIR, freq.SHARED_PRICE_DIR), (freq.ADJ_PRICE_DIR, freq.SHARED_ADJ_PRICE_DIR), (freq.SHARE_DIR, freq.SHARED_SHARE_DIR)]:
        path = freq.resolve_cache_path(local, shared, symbol)
        if path:
            report["source_hashes"][str(path)] = sha(path.read_bytes())
recent, new_turnover, effective = patched.base_mod.freq_mod.simulate_rebalance_path(
    trading_dates=dates, returns_df=pd.DataFrame(returns, index=dates),
    target_members_map={TARGET: target_members}, rebalance_dates=pd.DatetimeIndex([TARGET]),
    buyable_df=pd.DataFrame(buys, index=dates), sellable_df=pd.DataFrame(sells, index=dates),
    one_side_cost_rate=0.003, top_n=100, execution_timing=freq.EXECUTION_TIMING_CLOSE,
    initial_members=seed_members)
recent["holding_effective"] = recent.holding_count.gt(0)
candidate_proxy = dated(base.splice_recent_proxy_extension(proxy.loc[proxy.index <= BRIDGE].reset_index(), recent, BRIDGE))
candidate_turnover = pd.concat([turnover.loc[turnover.rebalance_date <= BRIDGE], new_turnover], ignore_index=True)
candidate_streams = calculate(candidate_proxy, panel, candidate_turnover)
pd.testing.assert_frame_equal(candidate_proxy.loc[candidate_proxy.index <= BRIDGE], proxy.loc[proxy.index <= BRIDGE], check_dtype=False, check_exact=True)
report["proxy_frozen_prefix_unchanged"] = True
report["target_members_source"] = "local 2026-09-17 proxy target file; see exact source hash"
report["final_effective_count"] = len(effective[TARGET])
report["status"] = "DIAGNOSTIC_CANDIDATE_REQUIRES_EXACT_HASH_APPROVAL"
candidate_proxy.to_csv(OUT / "candidate_proxy.csv", index_label="date")
candidate_turnover.to_csv(OUT / "candidate_turnover.csv", index=False)
pd.DataFrame({"as_of_date": TARGET.date(), "rank": range(1, len(effective[TARGET]) + 1), "symbol": effective[TARGET]}).to_csv(OUT / "candidate_effective_members.csv", index=False)
all_diffs = []
for key, frame in candidate_streams.items():
    prefix_check, prefix_diff = compare(references[key].loc[:BRIDGE], frame.loc[:BRIDGE], all_columns=True)
    if not prefix_check["same_dates"] or prefix_diff:
        pd.DataFrame(prefix_diff).to_csv(OUT / (key + "_blocked_prefix_diff.csv"), index=False)
        report["status"] = "BLOCKED_FROZEN_PREFIX_PARITY"
        report["blocked_prefix"] = {"version": key, "comparison": prefix_check}
        persist_report()
        raise SystemExit(report["status"])
    path = OUT / (key + "_candidate_nav.csv")
    frame.to_csv(path, index_label="date")
    check, diffs = compare(references[key], frame, all_columns=True)
    for row in diffs:
        row["version"] = key
    all_diffs.extend(diffs)
    report["candidates"][key] = {"path": str(path), "sha256": sha(path.read_bytes()), "rows": len(frame),
        "first_date": str(frame.index.min().date()), "last_date": str(frame.index.max().date()),
        "frozen_through_20260903_unchanged": True, "frozen_checked_columns": prefix_check["checked_columns"],
        "different_cells_on_existing_dates": len(diffs)}
proxy_diff = pd.DataFrame({"previous_close": proxy.close, "candidate_close": candidate_proxy.close,
                          "previous_daily_return": proxy.daily_return, "candidate_daily_return": candidate_proxy.daily_return})
proxy_diff.loc[proxy_diff.index > BRIDGE].to_csv(OUT / "candidate_proxy_daily_differences.csv", index_label="date")
pd.DataFrame(all_diffs).to_csv(OUT / "candidate_cell_differences.csv", index=False)
for path in OUT.glob("candidate_*.csv"):
    report.setdefault("diagnostic_artifact_hashes", {})[path.name] = sha(path.read_bytes())
persist_report()
print(json.dumps({"status": report["status"], "approved": False, "parity": report["parity"], "candidates": report["candidates"]}, ensure_ascii=False, indent=2))

