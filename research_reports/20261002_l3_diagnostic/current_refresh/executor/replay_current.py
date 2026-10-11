"""Read-only current-close rerun of the frozen L3 v2.0 baseline replay.

Loads the previously reviewed algorithm, but directs outputs to this separate
directory and extends the close cutoff to 2026-09-30. Never rewrites its 09-24
evidence or project outputs/.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd


HERE = Path(__file__).resolve().parent
BASE = HERE.parents[1] / "executor" / "replay_baseline.py"
spec = importlib.util.spec_from_file_location("l3_frozen_executor", BASE)
assert spec is not None and spec.loader is not None
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
module.HERE = HERE
module.CUTOFF = pd.Timestamp("2026-09-30")

if __name__ == "__main__":
    module.run()
