"""Read-only same-batch L3 arithmetic fingerprint after a source-code change.

Reuses the reviewed independent replay in 20261002_l3_diagnostic/executor,
but writes every result into this date-isolated folder. It never refreshes or
modifies formal strategy files or outputs/.
"""
from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys

import numpy as np
import pandas as pd


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
PREVIOUS = ROOT / "research_reports/20261002_l3_diagnostic/current_refresh/executor"
REPLAY = ROOT / "research_reports/20261002_l3_diagnostic/executor/replay_baseline.py"


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def metrics(ret: pd.Series, dates: pd.Series) -> dict:
    r = ret.to_numpy(float)
    nav = np.r_[1.0, np.cumprod(1.0 + r)]
    high = np.maximum.accumulate(nav)
    dd = nav / high - 1.0
    trough = int(np.argmin(dd))
    peak = int(np.argmax(nav[:trough + 1]))
    return {
        "rows": int(len(r)),
        "start_date": str(dates.iloc[0].date()),
        "end_date": str(dates.iloc[-1].date()),
        "annual_return_244d_diagnostic": float(nav[-1] ** (244.0 / len(r)) - 1.0),
        "max_drawdown_diagnostic": float(dd[trough]),
        "mdd_peak": "initial_capital" if peak == 0 else str(dates.iloc[peak - 1].date()),
        "mdd_trough": "initial_capital" if trough == 0 else str(dates.iloc[trough - 1].date()),
        "final_nav_window_diagnostic": float(nav[-1]),
    }


def run() -> None:
    old = json.loads((PREVIOUS / "summary.json").read_text(encoding="utf-8"))
    spec = importlib.util.spec_from_file_location("l3_independent_replay", REPLAY)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.HERE = HERE

    # Historical proxy, panel, base, and final v2.0 CSV must share the current
    # completed close; turnover is an event stream and can end earlier.
    file_state = {}
    for name, path in module.FILES.items():
        info = {"path": str(path), "sha256": sha(path)}
        if name != "source":
            frame = pd.read_csv(path, usecols=["rebalance_date" if name == "turnover" else "date"])
            dates = pd.to_datetime(frame.iloc[:, 0], format="mixed")
            info.update({"rows": int(len(frame)), "first_date": str(dates.min().date()),
                         "last_date": str(dates.max().date())})
        file_state[name] = info
    closes = {file_state[k]["last_date"] for k in ("proxy", "panel", "base", "official")}
    assert len(closes) == 1, closes
    module.CUTOFF = pd.Timestamp(next(iter(closes)))
    new = module.run()

    old_daily = pd.read_csv(PREVIOUS / "diagnostic_daily.csv")
    new_daily = pd.read_csv(HERE / "diagnostic_daily.csv")
    old_daily.date = pd.to_datetime(old_daily.date)
    new_daily.date = pd.to_datetime(new_daily.date)
    shared = old_daily.merge(new_daily, on="date", suffixes=("_old", "_new"), validate="one_to_one")
    old_cols = [c for c in old_daily.columns if c != "date"]
    common_diffs = {}
    for col in old_cols:
        a, b = shared[f"{col}_old"], shared[f"{col}_new"]
        if pd.api.types.is_numeric_dtype(a):
            delta = (a.astype(float) - b.astype(float)).abs()
            wrong = delta.gt(1e-10) | a.isna().ne(b.isna())
            maximum = float(delta.max())
        else:
            wrong = a.astype(str).ne(b.astype(str))
            maximum = None
        hits = shared.loc[wrong, "date"]
        common_diffs[col] = {"mismatch_count": int(len(hits)),
                             "first_date": str(hits.iloc[0].date()) if len(hits) else None,
                             "max_abs": maximum}

    official = pd.read_csv(module.FILES["official"])
    official.date = pd.to_datetime(official.date)
    replay = new_daily
    windows = {"full": None, "10Y": 10, "5Y": 5, "3Y": 3, "1Y": 1}
    records = []
    for label, years in windows.items():
        lower = None if years is None else module.CUTOFF - pd.DateOffset(years=years)
        for stream, frame in (("replay", replay), ("saved_official_old_stream", official)):
            part = frame if lower is None else frame.loc[frame.date.ge(lower)]
            assert len(part) and part.date.iloc[-1] == module.CUTOFF
            records.append({"window": label, "stream": stream, "cutoff_rule": "full" if lower is None else f"date >= {lower.date()}",
                            **metrics(part.return_net, part.date)})
    pd.DataFrame(records).to_csv(HERE / "window_metrics.csv", index=False, encoding="utf-8")

    checked = subprocess.run(
        [sys.executable, "-X", "utf8", "scripts/top100_delivery.py", "check"],
        cwd=ROOT, capture_output=True, text=True, encoding="utf-8", errors="replace", check=False,
    )
    delivery_check = {"command": "python -X utf8 scripts/top100_delivery.py check",
                      "exit_code": checked.returncode, "stdout": checked.stdout, "stderr": checked.stderr}
    (HERE / "delivery_check.json").write_text(json.dumps(delivery_check, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    meta = {
        "scope": "old frozen-input arithmetic only; source/remote authority mismatch, formal historical metrics N/A",
        "previous_snapshot": str(PREVIOUS / "summary.json"),
        "latest_common_close": str(module.CUTOFF.date()),
        "source_sha_changed_since_previous": file_state["source"]["sha256"] != old["file_sha256"]["source"],
        "previous_source_sha256": old["file_sha256"]["source"],
        "current_source_sha256": file_state["source"]["sha256"],
        "file_state": file_state,
        "previous_vs_current_file_sha_match": {k: file_state[k]["sha256"] == old["file_sha256"][k] for k in file_state},
        "previous_vs_current_file_rows_match": {k: file_state[k]["rows"] == old["file_full_rows"][k] for k in old["file_full_rows"]},
        "previous_vs_current_replay_common_rows": int(len(shared)),
        "previous_vs_current_replay_diffs": common_diffs,
        "replay_field_parity": new["parity"],
        "delivery_check": {"exit_code": checked.returncode,
                           "ok": (lambda v: v.get("ok") if isinstance(v, dict) else None)(json.loads(checked.stdout)) if checked.stdout.strip().startswith("{") else None},
    }
    (HERE / "recheck_manifest.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"latest_common_close": meta["latest_common_close"],
                      "source_sha_changed": meta["source_sha_changed_since_previous"],
                      "old_csv_hashes_unchanged": all(v for k, v in meta["previous_vs_current_file_sha_match"].items() if k != "source"),
                      "replay_fields_bad": [k for k, v in new["parity"].items() if v["mismatch_count"]],
                      "common_old_new_bad": [k for k, v in common_diffs.items() if v["mismatch_count"]]}, ensure_ascii=False))


if __name__ == "__main__":
    run()
