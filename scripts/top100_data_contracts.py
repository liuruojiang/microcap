"""Shared daily-file and fee-event contracts for the Top100 entrypoints."""
from __future__ import annotations

import math
import csv
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd


def daily_session_date(value: object) -> date:
    stamp = pd.Timestamp(value)
    if pd.isna(stamp) or stamp.tz is not None or stamp != stamp.normalize():
        raise ValueError(f"Expected a timezone-naive daily session date: {value!r}")
    return stamp.date()


def read_single_csv_row(path: Path) -> dict[str, str]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fields = reader.fieldnames or []
        if len(fields) != len(set(fields)):
            raise ValueError("Duplicate CSV column names")
        rows = list(reader)
    if len(rows) != 1 or None in rows[0] or any(value is None for value in rows[0].values()):
        raise ValueError("Signal CSV requires one complete row matching its header")
    return rows[0]


def normalise_turnover_table(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    if out.empty:
        return out
    required = {"rebalance_date", "two_side_cost_rate"}
    missing = required.difference(out.columns)
    if missing:
        raise ValueError(f"turnover table missing columns: {sorted(missing)}")
    for field in ("rebalance_date", "execution_date", "effective_date", "return_start_date"):
        if field not in out:
            continue
        raw = out[field]
        parsed = pd.to_datetime(raw, format="mixed", errors="coerce")
        invalid = parsed.isna() if field == "rebalance_date" else (raw.notna() & parsed.isna())
        if invalid.any():
            raise ValueError(f"turnover event has an invalid {field}")
        for value in parsed.dropna():
            daily_session_date(value)
        out[field] = parsed
    if out["rebalance_date"].duplicated().any():
        raise ValueError("duplicate rebalance event in turnover table")
    rates = pd.to_numeric(out["two_side_cost_rate"], errors="coerce")
    if (~np.isfinite(rates) | rates.lt(0.0) | rates.ge(1.0)).any():
        raise ValueError("turnover event cost rate must be finite and in [0, 1)")
    out["two_side_cost_rate"] = rates
    if "execution_timing" not in out:
        out["execution_timing"] = "next_open"
    if (not out["execution_timing"].isin(["close", "next_open"]).all()
            or out["execution_timing"].nunique() != 1):
        raise ValueError("turnover stream requires one uniform close or next_open execution timing")
    if "execution_date" in out:
        explicit = out["execution_date"].notna()
        if out.loc[explicit, "execution_date"].lt(out.loc[explicit, "rebalance_date"]).any():
            raise ValueError("rebalance execution date cannot precede its rebalance date")
    return out.sort_values("rebalance_date").reset_index(drop=True)


def expected_rebalance_costs(index: pd.Index, turnover: pd.DataFrame) -> pd.Series:
    dates = pd.DatetimeIndex(index)
    if dates.hasnans or dates.has_duplicates or not dates.is_monotonic_increasing:
        raise ValueError("rebalance cost index requires unique increasing valid dates")
    if dates.tz is not None or not dates.equals(dates.normalize()):
        raise ValueError("rebalance cost index requires timezone-naive daily session dates")
    events = normalise_turnover_table(turnover)
    costs = pd.Series(0.0, index=index, dtype=float)
    for row in events.itertuples(index=False):
        rebalance = pd.Timestamp(row.rebalance_date)
        explicit = getattr(row, "execution_date", None)
        has_execution = explicit is not None and pd.notna(explicit)
        paid_date = pd.Timestamp(explicit) if has_execution else rebalance
        side = "left" if has_execution or row.execution_timing == "close" else "right"
        if len(dates):
            if dates[0] <= rebalance <= dates[-1] and rebalance not in dates:
                raise ValueError("rebalance signal date is not a session in the result window")
            if paid_date < dates[0]:
                if side == "right":
                    raise ValueError("pre-window next_open cost requires an explicit execution_date")
                continue
            if paid_date <= dates[-1] and paid_date not in dates:
                raise ValueError("rebalance cost date is not a session in the result window")
        position = dates.searchsorted(paid_date, side=side)
        if position < len(dates):
            costs.iloc[position] += float(row.two_side_cost_rate)
    return costs


def validate_mapped_rebalance_costs(index: pd.Index, turnover: pd.DataFrame, mapped: pd.Series) -> None:
    expected = expected_rebalance_costs(index, turnover)
    if not mapped.index.equals(expected.index):
        raise ValueError("mapped rebalance costs changed the requested session index")
    actual = pd.to_numeric(mapped, errors="coerce")
    if not np.allclose(actual, expected, rtol=0.0, atol=1e-12):
        raise ValueError("mapped rebalance costs differ from dated turnover events")


def realtime_row_is_consistent(row: object, active_holding: str) -> bool:
    """Validate a dated snapshot; age alone does not invalidate an archived row."""
    try:
        from scripts.exchange_calendar import sessions_for_day, latest_completed_session

        quote = daily_session_date(row.get("quote_trade_date"))
        anchor = daily_session_date(row.get("latest_anchor_trade_date"))
        if daily_session_date(row.get("date")) != quote:
            return False
        sessions = sessions_for_day(quote)
        earlier = [day for day in sessions if day < quote]
        if quote not in sessions or not earlier or anchor != earlier[-1]:
            return False
        snapshot = pd.Timestamp(row.get("snapshot_time"))
        if pd.isna(snapshot) or snapshot.tz is None or snapshot.tz_convert("Asia/Shanghai").date() != quote:
            return False
        if latest_completed_session(snapshot.to_pydatetime()) != anchor:
            return False
        if row.get("signal_timing") != "intraday_hypothetical_if_now_close":
            return False
        if str(row.get("official_close_confirmed_signal")).lower() not in {"false", "0", "0.0"}:
            return False
        current, nxt = row.get("current_holding"), row.get("next_holding")
        if current not in {"cash", active_holding} or nxt not in {"cash", active_holding}:
            return False
        scales = {"current_execution_scale": 0.0 if current == "cash" else 1.0,
                  "next_session_actionable_scale": 0.0 if nxt == "cash" else 1.0}
        for field, expected in scales.items():
            actual = float(row.get(field))
            if not math.isfinite(actual) or actual != expected:
                return False
        action = "hold" if current == nxt else ("open" if current == "cash" else "close")
        if row.get("trade_state") != action or row.get("signal_label") != nxt:
            return False
        for field in ("effective_trade_state", "holding_trade_state", "momentum_trade_state"):
            if field in row and row[field] != action:
                return False
        if "snapshot_row_appended" in row and str(row["snapshot_row_appended"]).lower() not in {"true", "1", "1.0"}:
            return False
        next_scale, current_scale = scales["next_session_actionable_scale"], scales["current_execution_scale"]
        aliases = {field: next_scale for field in ("target_position_scale", "next_session_target_scale",
                   "raw_next_target_scale", "target_vol_scale_next_session")}
        aliases["execution_scale"] = current_scale
        aliases.update({field: next_scale - current_scale for field in (
            "raw_scale_delta", "actionable_scale_delta", "scale_delta", "position_scale_delta")})
        for field, expected in aliases.items():
            if field in row and (not math.isfinite(float(row[field]))
                                 or not math.isclose(float(row[field]), expected, rel_tol=0.0, abs_tol=1e-12)):
                return False
        member_actionable = str(row.get("member_rebalance_actionable", "False")).lower()
        if member_actionable not in {"true", "1", "1.0", "false", "0", "0.0"}:
            return False
        if member_actionable in {"true", "1", "1.0"}:
            if (daily_session_date(row.get("member_rebalance_execution_date")) != quote
                    or daily_session_date(row.get("member_rebalance_signal_date")) != anchor
                    or str(row.get("member_rebalance_required")).lower() not in {"true", "1", "1.0"}
                    or str(row.get("member_rebalance_official")).lower() not in {"true", "1", "1.0"}):
                return False
        return True
    except (ValueError, TypeError, KeyError, OverflowError):
        return False
