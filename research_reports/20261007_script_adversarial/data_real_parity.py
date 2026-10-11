"""Read-only before/after and official-path parity on the refreshed real inputs."""
from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import microcap_top100_mom16_biweekly_live_v2_0 as current
import microcap_top100_mom16_biweekly_live_v2_3 as v23
import microcap_top100_mom16_biweekly_live_v2_5 as v25

before_path = ROOT / ".codex_backups/20261007_143135/microcap_top100_mom16_biweekly_live_v2_0.py"
spec = importlib.util.spec_from_file_location("_data_parity_before", before_path)
assert spec is not None and spec.loader is not None
before = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = before
spec.loader.exec_module(before)

panel_path = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_panel_refreshed.csv"
proxy_path = ROOT / "outputs/wind_microcap_top_100_biweekly_thursday_16y_cached.csv"
turnover_path = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_turnover.csv"
turnover = pd.read_csv(turnover_path)
turnover["rebalance_date"] = pd.to_datetime(turnover["rebalance_date"], errors="raise")
turnover = turnover.sort_values("rebalance_date")
old_close = before.base_mod.load_close_df(panel_path, proxy_path, max_date=pd.Timestamp("2026-09-30"))
new_close = current.base_mod.load_close_df(panel_path, proxy_path, max_date=pd.Timestamp("2026-09-30"))
pd.testing.assert_frame_equal(old_close, new_close, check_exact=True)
old_base = before.base_mod.run_signal(old_close).sort_index()
new_base = current.base_mod.run_signal(new_close).sort_index()
pd.testing.assert_frame_equal(old_base, new_base, check_exact=True)
old_base_net = before.overlay_mod.apply_v2_0_execution(old_base, turnover)
new_base_net = current.overlay_mod.apply_v2_0_execution(new_base, turnover)
pd.testing.assert_frame_equal(old_base_net, new_base_net, check_exact=True)

# generate_v2_0_outputs consumes the base's already-warmed-up price rows and
# calls run_signal again; preserve that real call chain rather than shortening it.
official_prices = new_base[["microcap_close", "hedge_close"]].rename(
    columns={"microcap_close": "microcap", "hedge_close": "hedge"}
)
old_gross = before.base_mod.apply_momentum_gap_exit_buffer(
    before.base_mod.run_signal(official_prices).sort_index(), before.V2_0_MOMENTUM_GAP_EXIT_BUFFER
)
new_gross = current.base_mod.apply_momentum_gap_exit_buffer(
    current.base_mod.run_signal(official_prices).sort_index(), current.V2_0_MOMENTUM_GAP_EXIT_BUFFER
)
old_net = before.overlay_mod.apply_v2_0_execution(old_gross, turnover)
new_net = current.overlay_mod.apply_v2_0_execution(new_gross, turnover)
pd.testing.assert_frame_equal(old_net, new_net, check_exact=True)
official = pd.read_csv(current.COSTED_NAV_CSV)
current.base_mod.assert_no_historical_rewrite(
    official, new_net, current.V2_0_REWRITE_AUDIT_KEY_COLUMNS, 0,
    "read-only actual official v2.0 parity", atol=1e-9, rtol=1e-7,
)
official_indexed = official.assign(date=pd.to_datetime(official["date"])).set_index("date")
nav_delta = float((new_net["nav_net"] - official_indexed["nav_net"]).abs().max())

streams = {}
for version, module in (("2.0", current), ("2.3", v23), ("2.5", v25)):
    path = module.COSTED_NAV_CSV
    frame = current.base_mod.validate_performance_frame(pd.read_csv(path), "return_net", "nav_net", version)
    streams[version] = {"path": str(path.relative_to(ROOT)), "rows": len(frame),
                        "latest_date": str(frame.index.max().date()),
                        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                        "finite_positive_consistent_nav": True}

result = {
    "caliber": "script correctness and unchanged official-path parity; not PIT historical certification",
    "cutoff": "2026-09-30", "close_rows": len(new_close), "base_rows": len(new_base),
    "formal_v20_rows": len(new_net), "old_new_close_exact": True, "old_new_base_gross_exact": True,
    "old_new_base_costed_exact": True, "old_new_formal_v20_exact": True,
    "formal_v20_published_frozen_key_parity": True, "formal_v20_max_abs_nav_delta": nav_delta,
    "cost_timing_counts": turnover["execution_timing"].value_counts().to_dict(),
    "formal_stream_validations": streams,
    "inputs": {str(path.relative_to(ROOT)): {"sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
               for path in (panel_path, proxy_path, turnover_path)},
    "before_source_sha256": hashlib.sha256(before_path.read_bytes()).hexdigest(),
    "after_source_sha256": hashlib.sha256((ROOT / "microcap_top100_mom16_biweekly_live_v2_0.py").read_bytes()).hexdigest(),
}
output_path = Path(__file__).with_suffix(".json")
output_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
print(json.dumps(result, ensure_ascii=False, indent=2))
