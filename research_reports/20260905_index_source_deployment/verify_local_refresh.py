# /// script
# requires-python = ">=3.11"
# dependencies = ["pandas", "numpy"]
# ///
"""Whole-workspace real refresh/check and unchanged-history acceptance, bounded per process."""
from datetime import datetime
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from scripts import top100_delivery as delivery

before = {version: pd.read_csv(ROOT / "outputs" / name) for version, name in delivery.COSTED.items()}
hash_before = {name: hashlib.sha256((ROOT / "outputs" / name).read_bytes()).hexdigest() for name in delivery.COSTED.values()}
report = dict(started_at=datetime.now().astimezone().isoformat(), steps=[], mode="close-confirmed refresh; no realtime publication")
for name in ["refresh-all", "check"]:
    began = time.monotonic()
    run = subprocess.run([sys.executable, "-X", "utf8", "scripts/top100_delivery.py", name], cwd=ROOT,
                         capture_output=True, text=True, encoding="utf-8", timeout=600)
    (OUT / f"local_{name}.txt").write_text(run.stdout+run.stderr, encoding="utf-8")
    step = dict(command=name, exit_code=run.returncode, elapsed_seconds=round(time.monotonic()-began, 3))
    report["steps"].append(step)
    print(json.dumps(step), flush=True)
    if run.returncode:
        report["ok"] = False
        (OUT / "local_refresh.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        raise SystemExit(run.returncode)
report["parity"] = {}
for version, name in delivery.COSTED.items():
    after = pd.read_csv(ROOT / "outputs" / name)
    original = before[version]
    assert original.date.equals(after.date)
    for field in ["holding", "next_holding", "return_net", "nav_net", "total_cost"]:
        assert original[field].equals(after[field]), (version, field)
    report["parity"][version] = dict(rows=len(after), last_date=str(after.date.iloc[-1]),
                                    max_nav_delta=float((after.nav_net-original.nav_net).abs().max()),
                                    file_bytes_unchanged=hash_before[name] == hashlib.sha256((ROOT / "outputs" / name).read_bytes()).hexdigest())
report["ok"] = True
report["finished_at"] = datetime.now().astimezone().isoformat()
(OUT / "local_refresh.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
print(json.dumps(report), flush=True)
