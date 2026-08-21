from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = Path(__file__).resolve().parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))


LAUNCH_DATE = pd.Timestamp("2022-07-22")
TRADING_DAYS = 244


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        value = float(value)
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, np.bool_):
        return bool(value)
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(_json_safe(payload), ensure_ascii=False, indent=2), encoding="utf-8"
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _git(args: list[str]) -> str:
    try:
        return subprocess.check_output(
            ["git", *args], cwd=ROOT, text=True, stderr=subprocess.STDOUT
        ).strip()
    except Exception as exc:  # pragma: no cover - diagnostics only
        return f"unavailable: {exc}"


def _metrics(returns: pd.Series) -> dict[str, float]:
    values = pd.to_numeric(returns, errors="raise").astype(float)
    rows = int(len(values))
    nav = (1.0 + values).cumprod()
    final_nav = float(nav.iloc[-1])
    ann_return = final_nav ** (TRADING_DAYS / rows) - 1.0 if final_nav > 0 else np.nan
    ann_vol = float(values.std(ddof=1) * math.sqrt(TRADING_DAYS)) if rows > 1 else 0.0
    sharpe = ann_return / ann_vol if ann_vol > 0 else np.nan
    max_dd = float((nav / nav.cummax() - 1.0).min())
    return {
        "rows": rows,
        "ann_return": float(ann_return),
        "ann_vol": ann_vol,
        "sharpe_repo": float(sharpe),
        "max_dd": max_dd,
        "final_nav": final_nav,
    }


def _windows(index: pd.DatetimeIndex) -> dict[str, tuple[pd.Timestamp, pd.Timestamp, str]]:
    first = pd.Timestamp(index.min())
    end = pd.Timestamp(index.max())
    result = {"full": (first, end, "available")}
    for years in (10, 5, 3, 1):
        requested = end - pd.DateOffset(years=years)
        start = max(first, requested)
        status = "available" if requested >= first else "truncated_to_option_history"
        result[f"last_{years}y"] = (pd.Timestamp(start), end, status)
    return result


def _freshness_files(summary: dict[str, Any]) -> dict[str, Any]:
    files = summary["data_freshness_proof"].get("files", {})
    required = ("base_panel_shadow", "base_index_csv", "base_proxy_turnover", "base_costed_nav")
    result: dict[str, Any] = {}
    for key in required:
        item = files.get(key)
        if not item:
            raise RuntimeError(f"Freshness proof missing {key}")
        result[key] = {
            "path": item["path"],
            "latest_date": item["latest_date"],
            "row_count": int(item["row_count"]),
        }
    return result


class _CommonNamespace:
    LAUNCH_DATE = LAUNCH_DATE
    _write_json = staticmethod(_write_json)
    _sha256 = staticmethod(_sha256)
    _git = staticmethod(_git)
    _metrics = staticmethod(_metrics)
    _windows = staticmethod(_windows)
    _freshness_files = staticmethod(_freshness_files)


common = _CommonNamespace()


DEFAULT_RUN_FOLDER = (
    ROOT
    / "quant_param_scan_runs"
    / "20260820_microcap_top100_v2_3_csi1000_short_atm_call_fixed_contracts_short_call_contract_count"
)
DEFAULT_RAW_DIR = (
    ROOT
    / "quant_param_scan_runs"
    / "_shared_data"
    / "cffex_mo_daily_202207_202608"
)
REFERENCE_STOCK_NOTIONAL = 1_500_000.0
CONTRACT_COUNTS = (2, 3)
OPTION_MULTIPLIER = 100.0
OPTION_FEE_PER_CONTRACT_PER_SIDE = 15.0
OPTION_SLIPPAGE_POINTS_PER_SIDE = 1.0
OPTION_EXPIRY_FEE_PER_CONTRACT = 2.0
CALL_RE = re.compile(r"^MO(?P<yy>\d{2})(?P<mm>\d{2})-C-(?P<strike>\d+(?:\.\d+)?)$")


def _read_cffex_call_file(path: Path) -> pd.DataFrame:
    match = re.match(r"(?P<date>\d{8})", path.stem)
    if not match:
        return pd.DataFrame()
    date = pd.Timestamp(match.group("date"))
    raw = pd.read_csv(path, encoding="gb18030", dtype=str)
    if raw.shape[1] < 14:
        raise ValueError(f"Unexpected CFFEX schema in {path}: {raw.columns.tolist()}")
    raw = raw.iloc[:, :14].copy()
    raw.columns = [
        "contract",
        "open",
        "high",
        "low",
        "volume",
        "amount",
        "open_interest",
        "open_interest_change",
        "close",
        "settle",
        "prev_settle",
        "change_close",
        "change_settle",
        "delta",
    ]
    raw["contract"] = raw["contract"].astype(str).str.strip()
    parsed = raw["contract"].str.extract(CALL_RE)
    raw = raw.loc[parsed["yy"].notna()].copy()
    parsed = parsed.loc[raw.index]
    if raw.empty:
        return raw
    raw["date"] = date
    raw["expiry_code"] = "20" + parsed["yy"] + parsed["mm"]
    raw["strike"] = pd.to_numeric(parsed["strike"], errors="coerce")
    for column in ("volume", "open_interest", "close", "settle", "prev_settle", "delta"):
        raw[column] = pd.to_numeric(raw[column].replace("--", np.nan), errors="coerce")
    return raw[
        [
            "date",
            "contract",
            "expiry_code",
            "strike",
            "volume",
            "open_interest",
            "close",
            "settle",
            "prev_settle",
            "delta",
        ]
    ]


def _load_calls(raw_dir: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    files = sorted(raw_dir.glob("20????/20??????_1.csv"))
    if not files:
        raise FileNotFoundError(f"No extracted CFFEX history files under {raw_dir}")
    parts: list[pd.DataFrame] = []
    manifest: list[dict[str, Any]] = []
    for path in files:
        frame = _read_cffex_call_file(path)
        if frame.empty:
            continue
        parts.append(frame)
        manifest.append(
            {
                "path": str(path),
                "date": str(pd.Timestamp(frame["date"].iloc[0]).date()),
                "mo_call_rows": int(len(frame)),
                "sha256": common._sha256(path),
            }
        )
    if not parts:
        raise RuntimeError("CFFEX history contained no MO call rows.")
    chain = pd.concat(parts, ignore_index=True)
    chain = chain.drop_duplicates(["date", "contract"], keep="last").sort_values(
        ["date", "expiry_code", "strike"]
    )
    chain = chain.loc[chain["settle"].notna() & chain["settle"].ge(0)].copy()
    info = {
        "raw_files_scanned": int(len(files)),
        "raw_files_with_mo_calls": int(len(manifest)),
        "first_date": str(chain["date"].min().date()),
        "latest_date": str(chain["date"].max().date()),
        "rows": int(len(chain)),
        "contracts": int(chain["contract"].nunique()),
        "manifest": manifest,
    }
    return chain, info


def _expiry_last_dates(chain: pd.DataFrame) -> dict[str, pd.Timestamp]:
    grouped = chain.groupby("expiry_code", observed=True)["date"].max()
    data_latest = pd.Timestamp(chain["date"].max())
    expiry_dates: dict[str, pd.Timestamp] = {}
    for key, value in grouped.items():
        expiry_code = str(key)
        last_observed = pd.Timestamp(value)
        year = int(expiry_code[:4])
        month = int(expiry_code[4:6])
        month_start = pd.Timestamp(year=year, month=month, day=1)
        month_end = month_start + pd.offsets.MonthEnd(0)
        scheduled_fridays = pd.date_range(month_start, month_end, freq="W-FRI")
        if len(scheduled_fridays) < 3:
            raise RuntimeError(f"Cannot resolve scheduled expiry for {expiry_code}")
        scheduled_expiry = pd.Timestamp(scheduled_fridays[2])
        # A contract that is still quoted on the dataset endpoint has not necessarily
        # expired there. Preserve its scheduled third-Friday expiry when that date is
        # still in the future; historical contracts use their last observed trading
        # date, which also captures holiday postponements in the official files.
        expiry_dates[expiry_code] = (
            scheduled_expiry
            if last_observed == data_latest and scheduled_expiry > data_latest
            else last_observed
        )
    return expiry_dates


def _contract_row(day: pd.DataFrame, contract: str) -> pd.Series:
    rows = day.loc[day["contract"].eq(contract)]
    if len(rows) != 1:
        raise RuntimeError(f"Expected one CFFEX row for {contract}, got {len(rows)}")
    row = rows.iloc[0]
    if not (pd.notna(row["settle"]) and float(row["settle"]) >= 0):
        raise RuntimeError(f"Invalid settlement for {contract} on {row['date']}")
    return row


def _select_front_atm_call(
    date: pd.Timestamp,
    spot: float,
    day: pd.DataFrame,
    expiry_last: dict[str, pd.Timestamp],
) -> pd.Series:
    expiries = sorted(
        expiry
        for expiry in day["expiry_code"].astype(str).unique()
        if expiry in expiry_last and expiry_last[expiry] > pd.Timestamp(date)
    )
    if not expiries:
        raise RuntimeError(f"No non-expired MO call month available on {date.date()}")
    expiry = expiries[0]
    candidates = day.loc[
        day["expiry_code"].astype(str).eq(expiry)
        & day["settle"].gt(0)
        & day["delta"].between(0.001, 1.0, inclusive="both")
        & day["volume"].gt(0)
        & day["open_interest"].gt(0)
    ].copy()
    if candidates.empty:
        raise RuntimeError(f"No liquid ATM-call candidate for expiry {expiry} on {date.date()}")
    candidates["atm_distance"] = (candidates["strike"] - float(spot)).abs()
    return candidates.sort_values(
        ["atm_distance", "volume", "open_interest"], ascending=[True, False, False]
    ).iloc[0]


def _simulate(
    v23: pd.DataFrame,
    chain: pd.DataFrame,
    contract_count: int,
) -> pd.DataFrame:
    test = v23.loc[v23.index >= common.LAUNCH_DATE].copy()
    if test.empty or test.index.min() != common.LAUNCH_DATE:
        raise RuntimeError("Official v2.3 NAV does not contain 2022-07-22.")
    day_groups = {pd.Timestamp(dt): part.copy() for dt, part in chain.groupby("date", sort=True)}
    missing_days = [dt for dt in test.index if dt not in day_groups]
    if missing_days:
        raise RuntimeError(
            f"MO history misses {len(missing_days)} strategy dates; first={missing_days[0].date()}"
        )
    expiry_last = _expiry_last_dates(chain)
    contract: str | None = None
    previous_settle: float | None = None
    previous_expiry = ""
    records: list[dict[str, Any]] = []
    nav = 1.0
    standard_side_cost = (
        OPTION_FEE_PER_CONTRACT_PER_SIDE
        + OPTION_SLIPPAGE_POINTS_PER_SIDE * OPTION_MULTIPLIER
    )

    for row_number, (date, strategy_row) in enumerate(test.iterrows()):
        day = day_groups[pd.Timestamp(date)]
        current_active = str(strategy_row["holding"]) != "cash"
        next_active = str(strategy_row["next_holding"]) != "cash"
        option_pnl_cny = 0.0
        stock_return = 0.0
        stock_cost_rate = 0.0
        held_row: pd.Series | None = None
        expired_today = False
        early_close = False
        opened_today = False
        rolled_today = False
        option_fee_cost = 0.0
        option_slippage_cost = 0.0
        option_expiry_cost = 0.0

        if row_number > 0:
            if current_active != (contract is not None):
                raise RuntimeError(
                    f"Strategy/option holding mismatch on {date.date()}: "
                    f"strategy_active={current_active}, contract={contract}"
                )
            stock_return = float(strategy_row["microcap_ret"]) if current_active else 0.0
            stock_cost_rate = float(strategy_row["total_cost"])
            if current_active:
                assert contract is not None and previous_settle is not None
                held_row = _contract_row(day, contract)
                current_settle = float(held_row["settle"])
                option_pnl_cny = (
                    -float(contract_count)
                    * (current_settle - previous_settle)
                    * OPTION_MULTIPLIER
                )
                expired_today = expiry_last[str(held_row["expiry_code"])] == pd.Timestamp(date)

        gross_return = stock_return + option_pnl_cny / REFERENCE_STOCK_NOTIONAL
        after_stock_cost = (1.0 + gross_return) * (1.0 - stock_cost_rate) - 1.0

        next_contract: str | None = contract
        next_settle: float | None = (
            float(held_row["settle"]) if held_row is not None else previous_settle
        )
        next_expiry = previous_expiry
        entry_delta = np.nan
        entry_delta_ratio = np.nan
        entry_strike = np.nan
        entry_premium = np.nan
        entry_volume = np.nan
        entry_open_interest = np.nan

        if contract is not None and expired_today:
            if held_row is not None and float(held_row["settle"]) > 0:
                option_expiry_cost += OPTION_EXPIRY_FEE_PER_CONTRACT * contract_count
            next_contract = None
            next_settle = None
            next_expiry = ""
        elif contract is not None and not next_active:
            early_close = True
            option_fee_cost += OPTION_FEE_PER_CONTRACT_PER_SIDE * contract_count
            option_slippage_cost += (
                OPTION_SLIPPAGE_POINTS_PER_SIDE * OPTION_MULTIPLIER * contract_count
            )
            next_contract = None
            next_settle = None
            next_expiry = ""

        if next_active and next_contract is None:
            selected = _select_front_atm_call(
                pd.Timestamp(date), float(strategy_row["hedge_close"]), day, expiry_last
            )
            next_contract = str(selected["contract"])
            next_settle = float(selected["settle"])
            next_expiry = str(selected["expiry_code"])
            entry_delta = float(selected["delta"])
            entry_delta_ratio = (
                contract_count
                * entry_delta
                * float(strategy_row["hedge_close"])
                * OPTION_MULTIPLIER
                / REFERENCE_STOCK_NOTIONAL
            )
            entry_strike = float(selected["strike"])
            entry_premium = next_settle * OPTION_MULTIPLIER * contract_count
            entry_volume = float(selected["volume"])
            entry_open_interest = float(selected["open_interest"])
            option_fee_cost += OPTION_FEE_PER_CONTRACT_PER_SIDE * contract_count
            option_slippage_cost += (
                OPTION_SLIPPAGE_POINTS_PER_SIDE * OPTION_MULTIPLIER * contract_count
            )
            opened_today = True
            rolled_today = bool(expired_today and contract is not None)

        option_trade_cost = option_fee_cost + option_slippage_cost + option_expiry_cost
        daily_return = after_stock_cost - option_trade_cost / REFERENCE_STOCK_NOTIONAL
        nav *= 1.0 + daily_return
        if nav <= 0:
            raise RuntimeError(f"Candidate NAV became non-positive on {date.date()}")

        held_delta = float(held_row["delta"]) if held_row is not None and pd.notna(held_row["delta"]) else np.nan
        held_delta_ratio = (
            contract_count
            * held_delta
            * float(strategy_row["hedge_close"])
            * OPTION_MULTIPLIER
            / REFERENCE_STOCK_NOTIONAL
            if pd.notna(held_delta)
            else np.nan
        )
        records.append(
            {
                "date": pd.Timestamp(date),
                "candidate": f"short_atm_call_{contract_count}_contracts",
                "reference_stock_notional_cny": REFERENCE_STOCK_NOTIONAL,
                "contract_count": int(contract_count),
                "holding": strategy_row["holding"],
                "next_holding": strategy_row["next_holding"],
                "stock_return": stock_return,
                "stock_cost_rate": stock_cost_rate,
                "option_pnl_cny": option_pnl_cny,
                "option_pnl_return": option_pnl_cny / REFERENCE_STOCK_NOTIONAL,
                "option_fee_cost_cny": option_fee_cost,
                "option_fee_cost_return": option_fee_cost / REFERENCE_STOCK_NOTIONAL,
                "option_slippage_cost_cny": option_slippage_cost,
                "option_slippage_cost_return": option_slippage_cost / REFERENCE_STOCK_NOTIONAL,
                "option_expiry_cost_cny": option_expiry_cost,
                "option_expiry_cost_return": option_expiry_cost / REFERENCE_STOCK_NOTIONAL,
                "option_trade_cost_cny": option_trade_cost,
                "option_trade_cost_return": option_trade_cost / REFERENCE_STOCK_NOTIONAL,
                "daily_return": daily_return,
                "nav": nav,
                "contract": next_contract or "",
                "expiry_code": next_expiry,
                "settle": next_settle if next_settle is not None else np.nan,
                "opened_today": opened_today,
                "early_close": early_close,
                "expired_today": expired_today,
                "rolled_today": rolled_today,
                "entry_strike": entry_strike,
                "entry_exchange_delta": entry_delta,
                "entry_delta_ratio": entry_delta_ratio,
                "entry_premium_cny": entry_premium,
                "entry_volume": entry_volume,
                "entry_open_interest": entry_open_interest,
                "held_exchange_delta": held_delta,
                "held_delta_ratio": held_delta_ratio,
                "hedge_close": float(strategy_row["hedge_close"]),
            }
        )
        contract = next_contract
        previous_settle = next_settle
        previous_expiry = next_expiry

    return pd.DataFrame(records).set_index("date")


def _baseline(v23: pd.DataFrame) -> pd.DataFrame:
    test = v23.loc[v23.index >= common.LAUNCH_DATE].copy()
    returns = pd.to_numeric(test["return_net"], errors="raise").astype(float)
    returns.iloc[0] = 0.0
    out = pd.DataFrame(index=test.index)
    out["candidate"] = "original_v2.3_futures_0.8"
    out["contract_count"] = 0
    out["daily_return"] = returns
    out["nav"] = (1.0 + returns).cumprod()
    out["holding"] = test["holding"]
    out["next_holding"] = test["next_holding"]
    out["option_pnl_return"] = 0.0
    out["option_trade_cost_return"] = 0.0
    out["option_fee_cost_return"] = 0.0
    out["option_slippage_cost_return"] = 0.0
    out["option_expiry_cost_return"] = 0.0
    out["opened_today"] = False
    out["early_close"] = False
    out["expired_today"] = False
    out["rolled_today"] = False
    out["entry_delta_ratio"] = np.nan
    out["held_delta_ratio"] = np.nan
    return out


def _summarize(
    paths: dict[str, pd.DataFrame], run_folder: Path
) -> tuple[pd.DataFrame, pd.DataFrame]:
    daily_dir = run_folder / "daily_outputs"
    daily_dir.mkdir(parents=True, exist_ok=True)
    summary_rows: list[dict[str, Any]] = []
    wide_rows: list[dict[str, Any]] = []
    for label, path in paths.items():
        path.reset_index().to_csv(daily_dir / f"{label}.csv", index=False, encoding="utf-8")
        wide: dict[str, Any] = {
            "candidate": label,
            "contract_count": int(path["contract_count"].iloc[0]),
            "reference_stock_notional_cny": REFERENCE_STOCK_NOTIONAL,
        }
        for segment, (start, end, status) in common._windows(pd.DatetimeIndex(path.index)).items():
            part = path.loc[(path.index >= start) & (path.index <= end)]
            metrics = common._metrics(part["daily_return"])
            valid_entry_delta = pd.to_numeric(part["entry_delta_ratio"], errors="coerce").dropna()
            valid_held_delta = pd.to_numeric(part["held_delta_ratio"], errors="coerce").dropna()
            row = {
                "candidate": label,
                "segment": segment,
                "start": str(start.date()),
                "end": str(end.date()),
                "window_status": status,
                **metrics,
                "contract_count": int(path["contract_count"].iloc[0]),
                "reference_stock_notional_cny": REFERENCE_STOCK_NOTIONAL,
                "holding_days": int(part["holding"].astype(str).ne("cash").sum()),
                "holding_day_ratio": float(part["holding"].astype(str).ne("cash").mean()),
                "option_pnl_return_sum": float(part["option_pnl_return"].sum()),
                "option_trade_cost_return_sum": float(part["option_trade_cost_return"].sum()),
                "entry_count": int(part["opened_today"].sum()),
                "early_close_count": int(part["early_close"].sum()),
                "expiry_count": int(part["expired_today"].sum()),
                "expiry_roll_count": int(part["rolled_today"].sum()),
                "avg_entry_delta_ratio": float(valid_entry_delta.mean()) if len(valid_entry_delta) else 0.0,
                "min_entry_delta_ratio": float(valid_entry_delta.min()) if len(valid_entry_delta) else 0.0,
                "max_entry_delta_ratio": float(valid_entry_delta.max()) if len(valid_entry_delta) else 0.0,
                "avg_held_delta_ratio": float(valid_held_delta.mean()) if len(valid_held_delta) else 0.0,
            }
            summary_rows.append(row)
            wide[f"ann_return_{segment}"] = metrics["ann_return"]
            wide[f"max_dd_{segment}"] = metrics["max_dd"]
            wide[f"sharpe_repo_{segment}"] = metrics["sharpe_repo"]
            wide[f"window_status_{segment}"] = status
        wide["entry_count_full"] = int(path["opened_today"].sum())
        wide["early_close_count_full"] = int(path["early_close"].sum())
        wide["expiry_roll_count_full"] = int(path["rolled_today"].sum())
        wide["avg_entry_delta_ratio_full"] = float(
            pd.to_numeric(path["entry_delta_ratio"], errors="coerce").mean()
        ) if label.startswith("short_atm_call") else 0.0
        wide["decision_hint"] = "pending"
        wide["stability_label"] = "pending"
        wide_rows.append(wide)
    summary = pd.DataFrame(summary_rows)
    wide = pd.DataFrame(wide_rows)
    summary.to_csv(run_folder / "scan_summary.csv", index=False, encoding="utf-8")
    wide.to_csv(run_folder / "window_metrics.csv", index=False, encoding="utf-8")
    return summary, wide


def _write_cost_sensitivity(paths: dict[str, pd.DataFrame], run_folder: Path) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for candidate, path in paths.items():
        if not candidate.startswith("short_atm_call"):
            continue
        variants = {
            "fee_15_expiry_2_plus_slippage_1pt": path["daily_return"],
            "fee_15_expiry_2_settlement_execution": (
                path["daily_return"] + path["option_slippage_cost_return"]
            ),
            "zero_option_trading_cost_settlement_execution": (
                path["daily_return"] + path["option_trade_cost_return"]
            ),
        }
        for cost_variant, returns in variants.items():
            rows.append({"candidate": candidate, "cost_variant": cost_variant, **common._metrics(returns)})
    out = pd.DataFrame(rows)
    out.to_csv(run_folder / "cost_sensitivity.csv", index=False, encoding="utf-8")
    return out


def _write_record(run_folder: Path, wide: pd.DataFrame, meta: dict[str, Any]) -> None:
    rows = wide.set_index("candidate")
    lines = [
        "# Quant Parameter Scan Record",
        "",
        "## Run Metadata",
        "",
        f"- Run id: `{run_folder.name}`",
        f"- Created at: {meta['created_at']}",
        "- Project: microcap Top100",
        "- Strategy or version: v2.3",
        "- Sleeve or subsystem: fixed-count short ATM MO calls",
        "- Parameter group: `short_call_contract_count`",
        "- Scan type: `single_parameter`",
        f"- Repo: `{ROOT}`",
        f"- Git branch / commit: `{meta['git_branch']}` / `{meta['git_commit']}`",
        "- Source-change rule: `research_only_no_production_source_change`.",
        "",
        "## Research Question",
        "",
        "- For each RMB 1.5m long microcap unit, compare selling two versus three fixed ATM MO calls.",
        "- Keep the selected strike and contract count unchanged until option expiry or v2.3 stock exit.",
        "- At expiry, cash-settle and sell the next front-month ATM call only if the stock position remains active.",
        "- Do not rebalance Delta, quantity, or strike between entry and expiry.",
        "- Compare with original v2.3 0.8x futures hedge on the same post-MO-listing window.",
        "",
        "## Implementation Anchor",
        "",
        "- Official v2.3 entrypoint: `microcap_top100_mom16_biweekly_live_v2_3.py`.",
        "- Research helper: `scripts/csi1000_short_atm_call_research.py`.",
        "- Shared helper use is limited to official v2.3 loading, baseline parity, metrics, hashing, and freshness proof.",
        f"- Baseline pre-cost parity max absolute error: {meta['parity_check']['max_abs_pre_cost_return_diff']:.3g}.",
        "",
        "## Data Snapshot",
        "",
        f"- Test: {meta['data_snapshot']['metrics_start']} to {meta['data_snapshot']['latest_date']}.",
        f"- MO calls: {meta['option_data']['rows']:,} rows / {meta['option_data']['contracts']:,} contracts.",
        "- Source: official CFFEX daily settlement, volume, open interest, and Delta fields.",
        "- Calendar/timezone: official v2.3 A-share close dates, Asia/Shanghai.",
        "- 10Y/5Y artifact windows are truncated to post-listing history and are not full 10Y/5Y evidence.",
        "",
        "## Cost and Execution Assumptions",
        "",
        "- Long microcap notional: RMB 1.5m reference unit; daily option P&L is divided by this fixed unit.",
        "- Stock signals, vol10 defense, stock returns, and stock costs are unchanged from official v2.3.",
        "- Short-call fee: RMB 15 per contract per open/early-close side; expiry exercise fee RMB 2 when settlement is positive.",
        "- Slippage: one index point per contract on opening and early close; expiry uses cash settlement with no bid/ask slippage.",
        "- Premium received is not recognized as entry profit; only subsequent mark-to-market P&L is recognized.",
        "- No short-option margin funding cost, premium cash yield, or broker-specific margin multiplier is modeled.",
        "",
        "## Runtime Override Plan",
        "",
        "- No production constants changed; official baseline plus both contract-count candidates are in the same run.",
        "",
        "## Commands",
        "",
        "```powershell",
        "python microcap_top100_mom16_biweekly_live_v2_3.py",
        f"python scripts/csi1000_short_atm_call_research.py --run-folder \"{run_folder}\"",
        "```",
        "",
        "## Output Files",
        "",
        "- `scan_summary.csv`, `window_metrics.csv`, `cost_sensitivity.csv`.",
        "- `daily_outputs/`: held contract, settlement P&L, entry Delta, expiry rolls, and early exits.",
        "- `scan_meta.json`, `command_log.txt`.",
        "",
        "## Full-Sample Results",
        "",
    ]
    for candidate, row in rows.iterrows():
        lines.append(
            f"- `{candidate}`: annual return {row['ann_return_full']:.2%}, "
            f"max drawdown {row['max_dd_full']:.2%}, Sharpe {row['sharpe_repo_full']:.3f}."
        )
    lines.extend(
        [
            "",
            "## Window Results",
            "",
            "- See `window_metrics.csv`; full and 3Y/1Y windows are decision-useful, while 10Y/5Y are truncated.",
            "",
            "## Stability Classification",
            "",
            f"- Label: `{meta['stability_label']}`.",
            "- Cost sensitivity: see `cost_sensitivity.csv`.",
            "- Risk caveat: a short call is a negative-Delta overlay with asymmetric upside loss and requires margin.",
            "",
            "## Decision",
            "",
            f"- Decision: `{meta['decision']}`.",
            "- Production v2.3 remains unchanged.",
            "",
            "## User-Facing Summary",
            "",
            "- This run implements the user-confirmed fixed two/three short-ATM-call lifecycle, not daily ATM recentering.",
        ]
    )
    (run_folder / "record.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(run_folder: Path, raw_dir: Path) -> None:
    started = time.time()
    run_folder.mkdir(parents=True, exist_ok=True)
    with (run_folder / "command_log.txt").open("a", encoding="utf-8") as fh:
        fh.write(
            f"\n[{pd.Timestamp.now().isoformat()}] cwd={ROOT}\n"
            f"python scripts/csi1000_short_atm_call_research.py "
            f"--run-folder \"{run_folder}\" --raw-dir \"{raw_dir}\"\n"
        )

    v23, v23_summary = common._load_v23()
    parity = common._assert_baseline_parity(v23)
    chain, option_info = _load_calls(raw_dir)
    latest = pd.Timestamp(v23.index.max())
    if pd.Timestamp(chain["date"].max()) != latest:
        raise RuntimeError(
            f"Latest date mismatch: v2.3={latest.date()}, MO={chain['date'].max().date()}"
        )
    if pd.Timestamp(chain["date"].min()) != common.LAUNCH_DATE:
        raise RuntimeError("MO call history does not start on the 2022-07-22 listing date.")

    paths: dict[str, pd.DataFrame] = {"original_v2.3_futures_0.8": _baseline(v23)}
    for count in CONTRACT_COUNTS:
        paths[f"short_atm_call_{count}_contracts"] = _simulate(v23, chain, count)
    _summary, wide = _summarize(paths, run_folder)
    cost_sensitivity = _write_cost_sensitivity(paths, run_folder)

    baseline = wide.loc[wide["candidate"].eq("original_v2.3_futures_0.8")].iloc[0]
    candidates = wide.loc[wide["candidate"].str.startswith("short_atm_call")].copy()
    best = candidates.sort_values(["sharpe_repo_full", "ann_return_full"], ascending=False).iloc[0]
    beats_baseline = bool(
        best["ann_return_full"] > baseline["ann_return_full"]
        and best["max_dd_full"] >= baseline["max_dd_full"]
    )
    return_up_risk_worse = bool(
        best["ann_return_full"] > baseline["ann_return_full"]
        and best["max_dd_full"] < baseline["max_dd_full"]
    )
    if beats_baseline:
        decision = "watchlist_best_short_call"
        stability = "narrow_stable"
    elif return_up_risk_worse:
        decision = "watchlist_2_contracts_return_up_risk_worse"
        stability = "return_up_drawdown_worse"
    else:
        decision = "keep_original_v2.3_futures_hedge"
        stability = "short_call_replacement_not_superior"
    wide["decision_hint"] = decision
    wide["stability_label"] = stability
    wide.to_csv(run_folder / "window_metrics.csv", index=False, encoding="utf-8")

    freshness = common._freshness_files(v23_summary)
    data_snapshot = {
        "metrics_start": str(common.LAUNCH_DATE.date()),
        "latest_date": str(latest.date()),
        "rows": int((v23.index >= common.LAUNCH_DATE).sum()),
        "freshness_files": freshness,
        "v2_3_candidate": v23_summary["data_freshness_proof"]["candidate"],
    }
    meta = {
        "run_id": run_folder.name,
        "created_at": pd.Timestamp.now(tz="Asia/Shanghai").isoformat(),
        "phase": "analysis_written",
        "project": "microcap_top100",
        "strategy": "v2.3",
        "subsystem": "csi1000_short_atm_call_fixed_contracts",
        "repo_root": str(ROOT),
        "entrypoint": "microcap_top100_mom16_biweekly_live_v2_3.py",
        "research_harness": "scripts/csi1000_short_atm_call_research.py",
        "shared_helper": "scripts/csi1000_short_atm_call_research.py",
        "git_branch": common._git(["branch", "--show-current"]),
        "git_commit": common._git(["rev-parse", "HEAD"]),
        "git_status_before": common._git(["status", "--short"]),
        "git_status_after": common._git(["status", "--short"]),
        "scan_type": "single_parameter",
        "parameter_group": "short_call_contract_count",
        "baseline": {
            "candidate": "original_v2.3_futures_0.8",
            "execution_hedge_ratio": 0.8,
        },
        "candidate_grid": wide["candidate"].tolist(),
        "data_snapshot": data_snapshot,
        "option_data": {key: value for key, value in option_info.items() if key != "manifest"},
        "option_data_manifest_sha256": hashlib.sha256(
            json.dumps(option_info["manifest"], sort_keys=True).encode("utf-8")
        ).hexdigest(),
        "option_data_source": {
            "publisher": "China Financial Futures Exchange (CFFEX)",
            "url_pattern": "http://www.cffex.com.cn/sj/historysj/YYYYMM/zip/YYYYMM.zip",
            "raw_dir": str(raw_dir),
            "fields": ["settlement", "volume", "open_interest", "Delta"],
        },
        "cost_model": {
            "reference_stock_notional_cny": REFERENCE_STOCK_NOTIONAL,
            "stock_costs": "official_v2.3_total_cost_unchanged",
            "option_fee_per_contract_per_side_cny": OPTION_FEE_PER_CONTRACT_PER_SIDE,
            "option_slippage_points_per_side": OPTION_SLIPPAGE_POINTS_PER_SIDE,
            "option_expiry_fee_per_contract_cny": OPTION_EXPIRY_FEE_PER_CONTRACT,
            "margin_funding_rate": 0.0,
            "premium_cash_yield": 0.0,
        },
        "execution_assumptions": {
            "direction": "short_call",
            "contract_count_grid": list(CONTRACT_COUNTS),
            "entry_selection": "front_month_ATM_call_at_stock_entry_or_option_expiry_roll",
            "between_entry_and_exit": "hold_exact_contract_and_fixed_count_no_delta_rebalance",
            "stock_exit": "buy_back_call_same_close",
            "option_expiry": "cash_settle_then_open_next_front_month_ATM_if_stock_remains_active",
            "mark_price": "official_daily_settlement",
            "execution_timing": "close",
            "timezone": "Asia/Shanghai",
        },
        "parity_check": parity,
        "source_hashes": {
            "official_v2_3_nav_sha256": common._sha256(common.V23_NAV),
            "official_v2_3_summary_sha256": common._sha256(common.V23_SUMMARY),
            "research_harness_sha256": common._sha256(Path(__file__)),
            "shared_helper_sha256": common._sha256(Path(common.__file__)),
        },
        "outputs": {
            "record": str(run_folder / "record.md"),
            "scan_summary": str(run_folder / "scan_summary.csv"),
            "window_metrics": str(run_folder / "window_metrics.csv"),
            "cost_sensitivity": str(run_folder / "cost_sensitivity.csv"),
            "scan_meta": str(run_folder / "scan_meta.json"),
            "command_log": str(run_folder / "command_log.txt"),
            "daily_outputs": str(run_folder / "daily_outputs"),
        },
        "decision": decision,
        "stability_label": stability,
        "best_short_call_candidate": str(best["candidate"]),
        "cost_sensitivity_best_zero_cost": cost_sensitivity.loc[
            cost_sensitivity["cost_variant"].eq("zero_option_trading_cost_settlement_execution")
        ].sort_values(["sharpe_repo", "ann_return"], ascending=False).iloc[0].to_dict(),
        "warnings": [
            "The official v2.3 microcap series is the refreshed local/public proxy, not Wind 868008.WI.",
            "Short-call margin funding cost and broker-specific margin rules are excluded.",
            "Historical daily CFFEX data lack bid/ask; one-point slippage is imposed on opens and early closes.",
            "The earlier daily-ATM protective-put interpretation is superseded by this user-confirmed fixed short-call lifecycle.",
            "10Y and 5Y windows are truncated to post-listing history.",
        ],
        "elapsed_sec": round(time.time() - started, 3),
    }
    common._write_json(run_folder / "scan_meta.json", meta)
    _write_record(run_folder, wide, meta)
    print(wide.to_string(index=False))
    print(f"\nrun_folder={run_folder}")
    print(f"decision={decision}")
    print(f"stability_label={stability}")
