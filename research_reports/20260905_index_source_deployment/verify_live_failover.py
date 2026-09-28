# /// script
# requires-python = ">=3.11"
# dependencies = ["pandas", "numpy", "requests", "akshare"]
# ///
"""Real backup data through the official history entrypoint; only outages are injected."""
from contextlib import ExitStack
from datetime import datetime
import hashlib
import json
from pathlib import Path
import sys
import time
from unittest.mock import patch

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
import microcap_top100_mom16_biweekly_live_v2_0 as v20
from scripts import index_history_preflight as h
from scripts import top100_delivery as delivery

v20._sync_embedded_base_config()
base = v20.base_mod
panel = pd.read_csv(ROOT / "outputs" / delivery.BASE_PANEL)
source_panel = pd.read_csv(base.DEFAULT_PANEL_PATH)
start = pd.to_datetime(source_panel.date).max() - pd.Timedelta(days=base.HEDGE_HISTORY_LOOKBACK_BUFFER_DAYS)
before = delivery.input_hashes(ROOT)
nav_before = {name: hashlib.sha256((ROOT / "outputs" / name).read_bytes()).hexdigest() for name in delivery.COSTED.values()}
report = dict(started_at=datetime.now().astimezone().isoformat(), injected="transport errors only; real provider responses", cases=[])
for winner, failed in [("sina_static", ["eastmoney", "sina_legacy"]), ("tencent", ["eastmoney", "sina_legacy", "sina_static"])]:
    began = time.monotonic()
    with ExitStack() as stack:
        for name in failed:
            stack.enter_context(patch.object(h, name, side_effect=TimeoutError("explicit acceptance outage injection")))
        actual = base.fetch_eastmoney_index_history("1.000852", start)
    assert actual.attrs["independent_history_source"] == winner
    stable = h.preserve_existing_closes(actual, panel, base.HEDGE_COLUMN)
    assert stable.attrs["canonical_overlap_rows"] == len(stable)
    actual.to_csv(OUT / f"{winner}_live.csv", index=False)
    report["cases"].append(dict(source=winner, rows=len(stable), start=str(stable.date.min().date()),
                                 end=str(stable.date.max().date()), elapsed_seconds=round(time.monotonic()-began, 3), **stable.attrs))
    print(json.dumps(report["cases"][-1]), flush=True)
report["base_inputs_unchanged"] = delivery.input_hashes(ROOT) == before
report["nav_bytes_unchanged"] = all(hashlib.sha256((ROOT / "outputs" / name).read_bytes()).hexdigest() == digest for name, digest in nav_before.items())
report["ok"] = report["base_inputs_unchanged"] and report["nav_bytes_unchanged"]
(OUT / "live_failover.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
assert report["ok"]
