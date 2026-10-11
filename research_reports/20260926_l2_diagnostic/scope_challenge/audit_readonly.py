"""Read-only L2 scope fingerprint and arithmetic checks on existing formal streams.

These checks certify file-internal arithmetic only. They do not certify L1 data,
historical fills, executable closing prices, or strategy performance.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
STREAMS = {
    "v2.0": "microcap_top100_mom16_plain_fixed1_v2_0_costed_nav.csv",
    "v2.3": "microcap_top100_mom16_lb25_hl2p5_r2off_eb0p08_vol10_oh26_recovery20_exec0p8_v2_3_costed_nav.csv",
    "v2.5": "microcap_top100_mom16_lb20_hl3_entry0_exit0_no_targetvol_v2_5_costed_nav.csv",
}


def inspect(name: str, filename: str) -> dict[str, object]:
    path = ROOT / "outputs" / filename
    frame = pd.read_csv(path, encoding="utf-8-sig")
    current = frame["holding"].astype(str)
    next_holding = frame["next_holding"].astype(str)
    first_entry = frame.loc[current.eq("cash") & next_holding.ne("cash")].iloc[0]
    lag_mismatch = current.iloc[1:].reset_index(drop=True).ne(
        next_holding.iloc[:-1].reset_index(drop=True)
    )
    net = pd.to_numeric(frame["return_net"], errors="coerce")
    nav = pd.to_numeric(frame["nav_net"], errors="coerce")
    cost = pd.to_numeric(frame["total_cost"], errors="coerce")
    pre_cost = pd.to_numeric(frame["overlay_pre_cost_return"], errors="coerce")
    return {
        "version": name,
        "path": str(path),
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "rows": int(len(frame)),
        "first_date": str(frame.iloc[0]["date"]),
        "last_date": str(frame.iloc[-1]["date"]),
        "first_entry": {
            key: first_entry[key]
            for key in (
                "date", "holding", "next_holding", "entry_exit_cost",
                "total_cost", "return_net", "microcap_ret",
            )
        },
        "holding_lag_mismatch_count": int(lag_mismatch.sum()),
        "max_nav_compounding_abs_error": float(np.abs(nav - (1.0 + net).cumprod()).max()),
        "max_cost_identity_abs_error": float(
            np.abs(net - ((1.0 + pre_cost) * (1.0 - cost) - 1.0)).max()
        ),
    }


def main() -> None:
    print(json.dumps([inspect(name, filename) for name, filename in STREAMS.items()], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
