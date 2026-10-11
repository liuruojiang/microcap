"""Add window peak/trough evidence for the frozen L6 overheat scan."""

from pathlib import Path

import numpy as np
import pandas as pd


HERE = Path(__file__).resolve().parent
SUMMARY = HERE / "scan_summary.csv"
OUTPUT = HERE / "peak_trough.csv"
WINDOWS = {"full": None, "last_10y": 10, "last_5y": 5, "last_3y": 3, "last_1y": 1}


def main() -> None:
    summary = pd.read_csv(SUMMARY)
    records = []
    for candidate in summary["candidate"].drop_duplicates():
        file = HERE / "daily" / f"{candidate}.csv.gz"
        frame = pd.read_csv(file, parse_dates=["date"]).set_index("date")
        end = frame.index[-1]
        for segment, years in WINDOWS.items():
            part = frame if years is None else frame.loc[frame.index >= end - pd.DateOffset(years=years)]
            returns = pd.to_numeric(part["return_net"], errors="raise").to_numpy(dtype=float)
            if not np.isfinite(returns).all():
                raise ValueError(f"nonfinite:{candidate}:{segment}")
            nav = np.cumprod(1.0 + returns)
            running = np.maximum.accumulate(np.r_[1.0, nav])[1:]
            drawdown = nav / running - 1.0
            trough_pos = int(np.argmin(drawdown))
            peak_pos = int(np.argmax(nav[: trough_pos + 1]))
            peak = str(part.index[peak_pos].date()) if nav[peak_pos] >= 1.0 else "window_initial_nav"
            trough = str(part.index[trough_pos].date())
            recorded = summary.loc[
                summary["candidate"].eq(candidate) & summary["segment"].eq(segment)
            ]
            if len(recorded) != 1 or len(part) != int(recorded.iloc[0]["rows"]):
                raise AssertionError(f"window_rows:{candidate}:{segment}")
            if abs(float(drawdown[trough_pos]) - float(recorded.iloc[0]["max_dd"])) > 1e-12:
                raise AssertionError(f"drawdown_parity:{candidate}:{segment}")
            records.append({"candidate": candidate, "segment": segment,
                            "peak_date": peak, "trough_date": trough,
                            "max_dd": float(drawdown[trough_pos])})
    pd.DataFrame(records).to_csv(OUTPUT, index=False, encoding="utf-8")
    print(f"PASS: {len(records)} peak/trough windows; {OUTPUT}")


if __name__ == "__main__":
    main()
