from __future__ import annotations

import argparse
import hashlib
import io
import json
import math
import re
import sys
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import matplotlib
import numpy as np
import pandas as pd
import requests

matplotlib.use("Agg")
import matplotlib.pyplot as plt


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import run_microcap_v2_3_v2_5_combo50_comparison as freshness_common


IM_REPO = Path(r"D:\动量策略\IC和IM滚动套利")
if str(IM_REPO) not in sys.path:
    sys.path.insert(0, str(IM_REPO))

import im_monthly_discount_roll_v1 as im_roll
import im_mo_call_overwrite_delta_tenor_v19 as call_common


V25_NAV = (
    ROOT
    / "outputs"
    / "microcap_top100_mom16_lb20_hl3_entry0_exit0_no_targetvol_v2_5_costed_nav.csv"
)
FROZEN_IM = IM_REPO / "data" / "im_monthly_roll_3m_lowest_put_v1" / "cffex_im_contracts.csv"
FROZEN_CALLS = IM_REPO / "data" / "im_mo_call_data_build_v1" / "cffex_mo_calls.csv"
RUN = (
    ROOT
    / "quant_param_scan_runs"
    / "20260905_microcap_v25_short_atm_call_same_strike_v1"
)

VERSION = "2.5"
REVISION = "plain_lb20_hl3_entry0_exit0_20260905"
REAL_START = pd.Timestamp("2022-07-22")
MO_QTY = 2.0
MO_MULTIPLIER = 100.0
IM_MULTIPLIER = 200.0
CALL_BASKET_ONE_WAY_COST = 0.0001
TRADING_DAYS = 244
CFFEX_URLS = (
    "https://www.cffex.com.cn/sj/historysj/{ym}/zip/{ym}.zip",
    "http://www.cffex.com.cn/sj/historysj/{ym}/zip/{ym}.zip",
)
WINDOWS: tuple[tuple[str, int | None], ...] = (
    ("full", None),
    ("last_10y", 10),
    ("last_5y", 5),
    ("last_3y", 3),
    ("last_1y", 1),
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def download_month(month: str, raw_dir: Path) -> tuple[Path, dict[str, object]]:
    raw_dir.mkdir(parents=True, exist_ok=True)
    errors: list[str] = []
    for template in CFFEX_URLS:
        url = template.format(ym=month)
        try:
            response = requests.get(
                url,
                headers={"User-Agent": "Mozilla/5.0 microcap-v25-short-call-research/1.0"},
                timeout=30,
            )
            response.raise_for_status()
            path = raw_dir / f"{month}.zip"
            path.write_bytes(response.content)
            if not zipfile.is_zipfile(path):
                raise RuntimeError("response is not a ZIP archive")
            with zipfile.ZipFile(path) as archive:
                members = sorted(
                    Path(name).name
                    for name in archive.namelist()
                    if re.fullmatch(r"\d{8}_1\.csv", Path(name).name)
                )
            if not members:
                raise RuntimeError("archive has no daily futures/options CSV members")
            return path, {
                "month": month,
                "url": url,
                "bytes": int(path.stat().st_size),
                "sha256": sha256(path),
                "member_count": len(members),
                "first_member": members[0],
                "last_member": members[-1],
            }
        except Exception as exc:
            errors.append(f"{url}: {type(exc).__name__}: {exc}")
    raise RuntimeError(f"Could not refresh official CFFEX month {month}: {errors}")


def parse_calls(zip_paths: list[Path], end: pd.Timestamp) -> pd.DataFrame:
    fields = {
        0: "contract",
        1: "open",
        2: "high",
        3: "low",
        4: "volume",
        5: "turnover",
        6: "open_interest",
        8: "close",
        9: "settle",
        10: "pre_settle",
    }
    frames: list[pd.DataFrame] = []
    for path in zip_paths:
        with zipfile.ZipFile(path) as archive:
            for member in archive.namelist():
                name = Path(member).name
                match = re.fullmatch(r"(?P<day>\d{8})_1\.csv", name)
                if not match:
                    continue
                day = pd.Timestamp(match.group("day"))
                if day < REAL_START or day > end:
                    continue
                raw = pd.read_csv(
                    io.StringIO(im_roll._decode_cffex_csv(archive.read(member))),
                    header=None,
                    skiprows=1,
                    dtype=str,
                    on_bad_lines="error",
                )
                if raw.shape[1] < 11:
                    raise RuntimeError(f"Unexpected CFFEX schema: {path.name}/{member}")
                frame = raw[list(fields)].rename(columns=fields)
                frame["contract"] = frame["contract"].str.strip()
                frame = frame[frame["contract"].str.fullmatch(r"MO\d{4}-C-\d+", na=False)].copy()
                if frame.empty:
                    continue
                frame.insert(1, "date", day)
                for column in fields.values():
                    if column != "contract":
                        frame[column] = pd.to_numeric(
                            frame[column].replace({"--": np.nan, "null": np.nan}), errors="coerce"
                        )
                parsed = frame["contract"].str.extract(r"^MO\d{4}-C-(?P<strike>\d+)$")
                frame["strike"] = pd.to_numeric(parsed["strike"], errors="raise")
                frames.append(frame)
    if not frames:
        raise RuntimeError("No official MO Call rows parsed")
    calls = pd.concat(frames, ignore_index=True).sort_values(["date", "contract"])
    if calls.duplicated(["date", "contract"]).any():
        raise RuntimeError("Duplicate official MO Call quotes")
    if calls["settle"].isna().any():
        raise RuntimeError("Missing official MO Call settlement")
    return calls.reset_index(drop=True)


def overlap_errors(
    frozen: pd.DataFrame,
    refreshed: pd.DataFrame,
    numeric: list[str],
) -> tuple[int, dict[str, float]]:
    overlap = frozen.merge(
        refreshed,
        on=["date", "contract"],
        how="inner",
        suffixes=("_frozen", "_refresh"),
        validate="one_to_one",
    )
    errors = {
        column: float(
            (overlap[f"{column}_frozen"] - overlap[f"{column}_refresh"]).abs().max()
        )
        for column in numeric
    }
    if overlap.empty or any(value > 1e-10 for value in errors.values()):
        raise RuntimeError(f"Official overlap mismatch: rows={len(overlap)}, errors={errors}")
    return int(len(overlap)), errors


def load_market(end: pd.Timestamp) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, object]]:
    frozen_im = pd.read_csv(FROZEN_IM, parse_dates=["date"])
    frozen_calls = pd.read_csv(FROZEN_CALLS, parse_dates=["date"])
    frozen_end = min(frozen_im["date"].max(), frozen_calls["date"].max())
    refresh_start = pd.Timestamp(frozen_end).replace(day=1)
    zip_paths: list[Path] = []
    download_log: list[dict[str, object]] = []
    for month_start in pd.date_range(refresh_start, end.replace(day=1), freq="MS"):
        path, evidence = download_month(month_start.strftime("%Y%m"), RUN / "cffex_raw")
        zip_paths.append(path)
        download_log.append(evidence)

    refreshed_im = im_roll.parse_cffex_im(zip_paths, end)
    refreshed_im = refreshed_im[refreshed_im["date"] >= refresh_start].copy()
    refreshed_calls = parse_calls(zip_paths, end)
    refreshed_calls = refreshed_calls[refreshed_calls["date"] >= refresh_start].copy()
    if refreshed_im["date"].max() != end or refreshed_calls["date"].max() != end:
        raise RuntimeError("Official IM/MO refresh did not reach requested end date")

    numeric = [
        "open", "high", "low", "volume", "turnover", "open_interest", "close", "settle", "pre_settle"
    ]
    im_overlap_rows, im_errors = overlap_errors(frozen_im, refreshed_im, numeric)
    call_overlap_rows, call_errors = overlap_errors(
        frozen_calls, refreshed_calls, [*numeric, "strike"]
    )
    im = pd.concat(
        [frozen_im[frozen_im["date"] < refresh_start], refreshed_im], ignore_index=True
    ).sort_values(["date", "contract"]).reset_index(drop=True)
    calls = pd.concat(
        [frozen_calls[frozen_calls["date"] < refresh_start], refreshed_calls], ignore_index=True
    ).sort_values(["date", "contract"]).reset_index(drop=True)
    if im.duplicated(["date", "contract"]).any() or calls.duplicated(["date", "contract"]).any():
        raise RuntimeError("Duplicate rows after official refresh merge")
    if not im.groupby("date").size().eq(4).all():
        raise RuntimeError("Official IM chain does not contain exactly four contracts per day")
    return im, calls, {
        "frozen_im_path": str(FROZEN_IM),
        "frozen_im_sha256": sha256(FROZEN_IM),
        "frozen_call_path": str(FROZEN_CALLS),
        "frozen_call_sha256": sha256(FROZEN_CALLS),
        "refresh_start": str(refresh_start.date()),
        "end": str(end.date()),
        "download_log": download_log,
        "im_rows": int(len(im)),
        "im_dates": int(im["date"].nunique()),
        "im_contracts": int(im["contract"].nunique()),
        "call_rows": int(len(calls)),
        "call_dates": int(calls["date"].nunique()),
        "call_contracts": int(calls["contract"].nunique()),
        "im_overlap_rows": im_overlap_rows,
        "im_overlap_max_abs_errors": im_errors,
        "call_overlap_rows": call_overlap_rows,
        "call_overlap_max_abs_errors": call_errors,
    }


def load_v25() -> pd.DataFrame:
    frame = pd.read_csv(V25_NAV, parse_dates=["date"]).sort_values("date")
    required = {
        "date", "holding", "next_holding", "hedge_close", "overlay_pre_cost_return",
        "total_cost", "return_net", "nav_net", "version", "strategy_revision",
    }
    missing = required.difference(frame.columns)
    if missing:
        raise RuntimeError(f"v2.5 missing columns: {sorted(missing)}")
    if frame["date"].duplicated().any() or not frame["date"].is_monotonic_increasing:
        raise RuntimeError("v2.5 dates must be ordered and unique")
    if set(frame["version"].astype(str).unique()) != {VERSION}:
        raise RuntimeError("v2.5 version identity mismatch")
    if set(frame["strategy_revision"].astype(str).unique()) != {REVISION}:
        raise RuntimeError("v2.5 strategy revision mismatch")
    parity = (1.0 + frame["overlay_pre_cost_return"]) * (1.0 - frame["total_cost"]) - 1.0
    error = float((parity - frame["return_net"]).abs().max())
    if error > 1e-12:
        raise RuntimeError(f"v2.5 return parity failed: {error}")
    nav_error = float(((1.0 + frame["return_net"]).cumprod() - frame["nav_net"]).abs().max())
    if nav_error > 1e-10:
        raise RuntimeError(f"v2.5 NAV parity failed: {nav_error}")
    return frame


def add_call_metadata(calls: pd.DataFrame, end: pd.Timestamp) -> pd.DataFrame:
    result = calls.copy()
    parsed = result["contract"].str.extract(r"^MO(?P<yymm>\d{4})-C-(?P<strike>\d+)$")
    if parsed.isna().any().any():
        raise RuntimeError("Invalid MO Call identifier")
    result["contract_month"] = pd.to_datetime(
        "20" + parsed["yymm"] + "01", format="%Y%m%d"
    )
    end_month = end.replace(day=1)
    expiries: list[dict[str, object]] = []
    for month, group in result.groupby("contract_month"):
        month = pd.Timestamp(month)
        if month < end_month:
            expiry = pd.Timestamp(group["date"].max())
        else:
            expiry = im_roll.third_friday(month)
        expiries.append({"contract_month": month, "actual_expiry": expiry})
    result = result.merge(pd.DataFrame(expiries), on="contract_month", validate="many_to_one")
    return result.sort_values(["date", "actual_expiry", "strike", "contract"]).reset_index(drop=True)


def tradable_chain(calls: pd.DataFrame, day: pd.Timestamp, expiry: pd.Timestamp) -> pd.DataFrame:
    return calls[
        calls["date"].eq(day)
        & calls["actual_expiry"].eq(expiry)
        & calls["close"].gt(0)
        & calls["settle"].gt(0)
        & calls["volume"].gt(0)
        & calls["open_interest"].gt(0)
    ].copy()


def choose_entry(calls: pd.DataFrame, day: pd.Timestamp, spot: float) -> pd.Series:
    expiries = sorted(calls.loc[calls["date"].eq(day) & calls["actual_expiry"].gt(day), "actual_expiry"].unique())
    if not expiries:
        raise RuntimeError(f"No later listed MO expiry on entry {day.date()}")
    expiry = pd.Timestamp(expiries[0])
    chain = tradable_chain(calls, day, expiry)
    if chain.empty:
        raise RuntimeError(f"No tradable front MO Call on entry {day.date()}")
    chain["target_error"] = (chain["strike"] - spot).abs()
    chain["abs_moneyness"] = (chain["strike"] / spot - 1.0).abs()
    return chain.sort_values(
        ["target_error", "abs_moneyness", "open_interest", "volume", "strike", "contract"],
        ascending=[True, True, False, False, True, True],
    ).iloc[0]


def choose_roll(
    calls: pd.DataFrame,
    day: pd.Timestamp,
    old_expiry: pd.Timestamp,
    prior_strike: float,
) -> tuple[pd.Series, bool]:
    expiries = sorted(calls.loc[calls["date"].eq(day) & calls["actual_expiry"].gt(old_expiry), "actual_expiry"].unique())
    if not expiries:
        raise RuntimeError(f"No next MO expiry on roll {day.date()}")
    expiry = pd.Timestamp(expiries[0])
    chain = tradable_chain(calls, day, expiry)
    if chain.empty:
        raise RuntimeError(f"No tradable next-month MO Call on roll {day.date()}")
    chain["strike_error"] = (chain["strike"] - prior_strike).abs()
    selected = chain.sort_values(
        ["strike_error", "open_interest", "volume", "strike", "contract"],
        ascending=[True, False, False, True, True],
    ).iloc[0]
    return selected, bool(float(selected["strike"]) == float(prior_strike))


@dataclass
class Position:
    contract: str
    strike: float
    expiry: pd.Timestamp
    prior_settle: float
    episode: int


def simulate(
    v25: pd.DataFrame,
    im: pd.DataFrame,
    calls: pd.DataFrame,
    end: pd.Timestamp,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, object]]:
    cycles = im_roll.build_cycles(im, end)
    im_daily, _ = im_roll.build_futures_daily(im, cycles)
    merged = v25.merge(
        im_daily[["date", "contract", "settle"]].rename(
            columns={"contract": "im_contract", "settle": "im_settle"}
        ),
        on="date",
        how="inner",
        validate="one_to_one",
    )
    entry_mask = merged["holding"].eq("cash") & merged["next_holding"].ne("cash")
    eligible_entries = merged.loc[merged["date"].ge(REAL_START) & entry_mask, "date"]
    if eligible_entries.empty:
        raise RuntimeError("No observable v2.5 entry after MO listing")
    start = pd.Timestamp(eligible_entries.iloc[0])
    frame = merged[merged["date"].between(start, end)].copy().reset_index(drop=True)
    if frame.empty or frame["date"].max() != end:
        raise RuntimeError("Common v2.5/IM sample did not reach requested end")
    if not set(frame["date"]).issubset(set(calls["date"])):
        raise RuntimeError("MO Call chain misses strategy trading dates")

    prior_im = frame["im_settle"].shift(1)
    prior_im.iloc[0] = frame.iloc[0]["im_settle"]
    active: Position | None = None
    episode = 0
    rows: list[dict[str, object]] = []
    trades: list[dict[str, object]] = []
    exact_rolls = 0
    nearest_rolls = 0

    for index, base in frame.iterrows():
        day = pd.Timestamp(base["date"])
        holding_active = str(base["holding"]) != "cash"
        next_active = str(base["next_holding"]) != "cash"
        open_event = not holding_active and next_active
        close_event = holding_active and not next_active
        denominator = float(prior_im.iloc[index])
        pnl = 0.0
        cost = 0.0
        action = ""

        if active is not None:
            quote = calls.loc[
                calls["date"].eq(day) & calls["contract"].eq(active.contract)
            ]
            if len(quote) != 1:
                raise RuntimeError(f"Missing/duplicate active Call quote {active.contract} {day.date()}")
            quote = quote.iloc[0]
            if day >= active.expiry:
                settle = float(quote["settle"])
                if settle < 0:
                    raise RuntimeError(f"Invalid expiry Call settlement {active.contract} {day.date()}")
                pnl += MO_QTY * MO_MULTIPLIER / IM_MULTIPLIER * (
                    active.prior_settle - settle
                ) / denominator
                old = active
                cost += CALL_BASKET_ONE_WAY_COST
                active = None
                if next_active:
                    selected, exact = choose_roll(calls, day, old.expiry, old.strike)
                    exact_rolls += int(exact)
                    nearest_rolls += int(not exact)
                    pnl += MO_QTY * MO_MULTIPLIER / IM_MULTIPLIER * (
                        float(selected["close"]) - float(selected["settle"])
                    ) / denominator
                    cost += CALL_BASKET_ONE_WAY_COST
                    active = Position(
                        contract=str(selected["contract"]),
                        strike=float(selected["strike"]),
                        expiry=pd.Timestamp(selected["actual_expiry"]),
                        prior_settle=float(selected["settle"]),
                        episode=old.episode,
                    )
                    trades.append({
                        "date": day, "action": "expiry_roll", "episode": old.episode,
                        "old_contract": old.contract, "new_contract": active.contract,
                        "old_strike": old.strike, "new_strike": active.strike,
                        "old_expiry": old.expiry, "new_expiry": active.expiry,
                        "trade_price": float(selected["close"]), "volume": float(selected["volume"]),
                        "open_interest": float(selected["open_interest"]),
                        "exact_same_strike": exact,
                    })
                    action = "expiry_roll"
                else:
                    trades.append({
                        "date": day, "action": "expire_with_microcap", "episode": old.episode,
                        "old_contract": old.contract, "new_contract": "",
                        "old_strike": old.strike, "new_strike": np.nan,
                        "old_expiry": old.expiry, "new_expiry": pd.NaT,
                        "trade_price": settle, "volume": float(quote["volume"]),
                        "open_interest": float(quote["open_interest"]),
                        "exact_same_strike": np.nan,
                    })
                    action = "expire_with_microcap"
            elif close_event:
                if not (
                    float(quote["close"]) > 0
                    and float(quote["volume"]) > 0
                    and float(quote["open_interest"]) > 0
                ):
                    raise RuntimeError(f"v2.5 exit Call is not executable {active.contract} {day.date()}")
                pnl += MO_QTY * MO_MULTIPLIER / IM_MULTIPLIER * (
                    active.prior_settle - float(quote["close"])
                ) / denominator
                cost += CALL_BASKET_ONE_WAY_COST
                trades.append({
                    "date": day, "action": "close_with_microcap", "episode": active.episode,
                    "old_contract": active.contract, "new_contract": "", "old_strike": active.strike,
                    "new_strike": np.nan, "old_expiry": active.expiry, "new_expiry": pd.NaT,
                    "trade_price": float(quote["close"]), "volume": float(quote["volume"]),
                    "open_interest": float(quote["open_interest"]), "exact_same_strike": np.nan,
                })
                active = None
                action = "close_with_microcap"
            else:
                settle = float(quote["settle"])
                if settle <= 0:
                    raise RuntimeError(f"Invalid active Call settlement {active.contract} {day.date()}")
                pnl += MO_QTY * MO_MULTIPLIER / IM_MULTIPLIER * (
                    active.prior_settle - settle
                ) / denominator
                active.prior_settle = settle

        if open_event:
            if active is not None:
                raise RuntimeError(f"Call already active on v2.5 entry {day.date()}")
            selected = choose_entry(calls, day, float(base["hedge_close"]))
            episode += 1
            pnl += MO_QTY * MO_MULTIPLIER / IM_MULTIPLIER * (
                float(selected["close"]) - float(selected["settle"])
            ) / denominator
            cost += CALL_BASKET_ONE_WAY_COST
            active = Position(
                contract=str(selected["contract"]),
                strike=float(selected["strike"]),
                expiry=pd.Timestamp(selected["actual_expiry"]),
                prior_settle=float(selected["settle"]),
                episode=episode,
            )
            trades.append({
                "date": day, "action": "open_with_microcap", "episode": episode,
                "old_contract": "", "new_contract": active.contract, "old_strike": np.nan,
                "new_strike": active.strike, "old_expiry": pd.NaT, "new_expiry": active.expiry,
                "trade_price": float(selected["close"]), "volume": float(selected["volume"]),
                "open_interest": float(selected["open_interest"]), "exact_same_strike": np.nan,
                "entry_spot": float(base["hedge_close"]),
                "entry_moneyness": active.strike / float(base["hedge_close"]) - 1.0,
            })
            action = "open_with_microcap"

        if (active is not None) != next_active:
            raise RuntimeError(f"EOD Call/v2.5 state mismatch {day.date()}")
        contract = active.contract if active is not None else ""
        strike = active.strike if active is not None else np.nan
        expiry = active.expiry if active is not None else pd.NaT
        margin = 0.0
        mark_fraction = 0.0
        itm = False
        if active is not None:
            quote = calls.loc[
                calls["date"].eq(day) & calls["contract"].eq(active.contract)
            ].iloc[0]
            mark = float(quote["settle"])
            equivalent_units = MO_QTY * MO_MULTIPLIER / IM_MULTIPLIER
            mark_fraction = equivalent_units * mark / float(base["im_settle"])
            margin = call_common.call_margin_fraction(
                mark,
                float(base["hedge_close"]),
                active.strike,
                equivalent_units,
                float(base["im_settle"]),
            )
            itm = float(base["hedge_close"]) > active.strike

        candidate_return = (
            (1.0 + float(base["overlay_pre_cost_return"]) + pnl)
            * (1.0 - float(base["total_cost"]))
            * (1.0 - cost)
            - 1.0
        )
        rows.append({
            "date": day,
            "holding": base["holding"],
            "next_holding": base["next_holding"],
            "baseline_pre_cost_return": float(base["overlay_pre_cost_return"]),
            "stock_total_cost": float(base["total_cost"]),
            "baseline_return_net": float(base["return_net"]),
            "call_pnl_ret": pnl,
            "call_cost_rate": cost,
            "candidate_return_net": candidate_return,
            "call_contract_eod": contract,
            "call_strike_eod": strike,
            "call_expiry_eod": expiry,
            "call_margin_fraction": margin,
            "call_mark_fraction": mark_fraction,
            "call_itm": itm,
            "action": action,
            "im_contract": base["im_contract"],
            "im_settle": float(base["im_settle"]),
            "csi1000_close": float(base["hedge_close"]),
        })

    daily = pd.DataFrame(rows)
    daily["baseline_nav"] = (1.0 + daily["baseline_return_net"]).cumprod()
    daily["candidate_nav"] = (1.0 + daily["candidate_return_net"]).cumprod()
    trades_frame = pd.DataFrame(trades)
    integrity = {
        "start": str(start.date()),
        "end": str(end.date()),
        "rows": int(len(daily)),
        "observable_entry_episodes": int(episode),
        "microcap_entry_events": int(
            ((daily["holding"] == "cash") & (daily["next_holding"] != "cash")).sum()
        ),
        "microcap_exit_events": int(
            ((daily["holding"] != "cash") & (daily["next_holding"] == "cash")).sum()
        ),
        "expiry_rolls": int((daily["action"] == "expiry_roll").sum()),
        "exact_same_strike_rolls": int(exact_rolls),
        "nearest_strike_fallback_rolls": int(nearest_rolls),
        "eod_state_mismatch_rows": int(
            (daily["call_contract_eod"].ne("") != daily["next_holding"].ne("cash")).sum()
        ),
        "baseline_slice_nav_parity_max_abs": float(
            ((1.0 + daily["baseline_return_net"]).cumprod() - daily["baseline_nav"]).abs().max()
        ),
        "candidate_formula_max_abs": float(
            (
                (1.0 + daily["baseline_pre_cost_return"] + daily["call_pnl_ret"])
                * (1.0 - daily["stock_total_cost"])
                * (1.0 - daily["call_cost_rate"])
                - 1.0
                - daily["candidate_return_net"]
            ).abs().max()
        ),
        "call_pnl_sum": float(daily["call_pnl_ret"].sum()),
        "call_cost_sum": float(daily["call_cost_rate"].sum()),
        "call_active_days": int(daily["call_contract_eod"].ne("").sum()),
        "call_itm_days": int(daily["call_itm"].sum()),
        "average_margin_fraction_active": float(
            daily.loc[daily["call_contract_eod"].ne(""), "call_margin_fraction"].mean()
        ),
        "maximum_margin_fraction": float(daily["call_margin_fraction"].max()),
        "maximum_mark_fraction": float(daily["call_mark_fraction"].max()),
        "open_end_position": bool(active is not None),
        "open_end_contract": active.contract if active is not None else "",
    }
    return daily, trades_frame, integrity


def metric(returns: pd.Series) -> dict[str, float]:
    values = returns.astype(float)
    nav = (1.0 + values).cumprod()
    elapsed_years = (values.index[-1] - values.index[0]).days / 365.25
    ann_return = float(nav.iloc[-1] ** (1.0 / elapsed_years) - 1.0)
    ann_vol = float(values.std(ddof=1) * math.sqrt(TRADING_DAYS))
    nav0 = np.r_[1.0, nav.to_numpy(dtype=float)]
    max_dd = float((nav0 / np.maximum.accumulate(nav0) - 1.0).min())
    return {
        "ann_return": ann_return,
        "max_dd": max_dd,
        "ann_vol": ann_vol,
        "sharpe_repo": float(ann_return / ann_vol) if ann_vol > 0 else np.nan,
        "total_return": float(nav.iloc[-1] - 1.0),
    }


def build_metrics(daily: pd.DataFrame) -> pd.DataFrame:
    end = pd.Timestamp(daily["date"].max())
    start = pd.Timestamp(daily["date"].min())
    rows: list[dict[str, object]] = []
    for window, years in WINDOWS:
        requested = start if years is None else end - pd.DateOffset(years=years)
        available = years is None or start <= requested
        for candidate, column in (
            ("v2_5_official", "baseline_return_net"),
            ("v2_5_short_atm_call_same_strike", "candidate_return_net"),
        ):
            if not available:
                rows.append({
                    "window": window, "candidate": candidate, "available": False,
                    "na_reason": f"real MO observable-entry sample is {(end-start).days/365.25:.2f} years, shorter than requested {years} years",
                    "start": "", "end": str(end.date()), "rows": 0,
                    "ann_return": np.nan, "max_dd": np.nan, "ann_vol": np.nan,
                    "sharpe_repo": np.nan, "total_return": np.nan,
                })
                continue
            part = daily[daily["date"].ge(requested)].set_index("date")[column]
            rows.append({
                "window": window, "candidate": candidate, "available": True, "na_reason": "",
                "start": str(part.index.min().date()), "end": str(part.index.max().date()),
                "rows": int(len(part)), **metric(part),
            })
    return pd.DataFrame(rows)


def build_annual(daily: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for year, group in daily.groupby(daily["date"].dt.year):
        for candidate, column in (
            ("v2_5_official", "baseline_return_net"),
            ("v2_5_short_atm_call_same_strike", "candidate_return_net"),
        ):
            returns = group.set_index("date")[column]
            rows.append({"year": int(year), "candidate": candidate, "rows": len(group), **metric(returns)})
    return pd.DataFrame(rows)


def write_chart(daily: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(12, 6.5))
    ax.plot(daily["date"], daily["baseline_nav"], label="v2.5 official long-only", linewidth=1.8)
    ax.plot(
        daily["date"], daily["candidate_nav"],
        label="v2.5 + short ATM MO Call; same strike monthly roll", linewidth=1.8,
    )
    ax.set_title("Microcap v2.5: short ATM MO Call overlay (real CFFEX IM/MO)")
    ax.grid(alpha=0.25)
    ax.legend()
    fig.tight_layout()
    fig.savefig(RUN / "nav_comparison.png", dpi=170)
    plt.close(fig)


def main(official_latest_close_date: str) -> None:
    target_end = pd.Timestamp(official_latest_close_date)
    freshness = freshness_common.validate_formal_freshness(
        expected_latest_date=official_latest_close_date
    )
    RUN.mkdir(parents=True, exist_ok=True)
    v25 = load_v25()
    if v25["date"].max() != target_end:
        raise RuntimeError("v2.5 formal stream is not current")
    im, calls, market_meta = load_market(target_end)
    calls = add_call_metadata(calls, target_end)
    daily, trades, integrity = simulate(v25, im, calls, target_end)
    metrics = build_metrics(daily)
    annual = build_annual(daily)

    daily.to_csv(RUN / "daily_returns_and_nav.csv.gz", index=False, compression="gzip")
    trades.to_csv(RUN / "call_trades.csv", index=False)
    metrics.to_csv(RUN / "window_metrics.csv", index=False)
    annual.to_csv(RUN / "annual_metrics.csv", index=False)
    write_chart(daily)
    meta = {
        "run_id": RUN.name,
        "created_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "status": "research_only_not_production_authority",
        "command": (
            "python -X utf8 scripts/run_microcap_v25_short_atm_call_same_strike_backtest.py "
            f"--official-latest-close-date {official_latest_close_date}"
        ),
        "v2_5": {
            "path": str(V25_NAV), "sha256": sha256(V25_NAV), "version": VERSION,
            "revision": REVISION, "rows": int(len(v25)), "latest_date": str(v25["date"].max().date()),
        },
        "microcap_freshness": freshness,
        "market": market_meta,
        "rule": {
            "entry": "on each v2.5 cash-to-long close, sell front-expiry ATM MO Call",
            "quantity": "2 MO Calls per 1 IM-equivalent notional",
            "holding": "hold to expiry; same-day roll to nearest later month at same strike",
            "roll_fallback": "nearest strike only if exact prior strike is not tradable/listed",
            "exit": "buy back Call at the same close when v2.5 exits microcap",
            "option_marking": "official settlement; trade at official close",
            "call_basket_one_way_cost": CALL_BASKET_ONE_WAY_COST,
            "premium_accounting": "premium receipt and liability established together; no immediate income",
            "margin_financing": "excluded; margin fraction reported separately",
            "bid_ask_and_impact": "excluded",
        },
        "integrity": integrity,
    }
    (RUN / "scan_meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    (RUN / "integrity_checks.json").write_text(
        json.dumps(integrity, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    record = [
        "# 微盘2.5卖出平值MO Call并同执行价逐月换仓",
        "",
        "- 状态：research-only；不修改正式2.5。",
        f"- 真实同窗：{integrity['start']}—{integrity['end']}，{integrity['rows']}个交易日。",
        "- 样本从MO上市后第一笔可观察的2.5新开仓开始，避免倒推上市前开仓对应的未知平值执行价。",
        "- 交易规则：2.5开仓收盘卖出当月平值Call；每1手IM名义卖2手MO；到期仍持有微盘则同日换到下月相同执行价；微盘平仓则Call同日买回。",
        "- 真实成交使用官方收盘，逐日盯市使用官方结算；收到权利金不直接计入收益。",
        "- 两张MO合计每边1bp；保证金只报告占比，未计额外融资、盘口滑点和冲击。",
        "",
        "## Metrics",
        "",
        metrics.to_markdown(index=False),
        "",
        "## Integrity",
        "",
        "```json",
        json.dumps(integrity, ensure_ascii=False, indent=2),
        "```",
    ]
    (RUN / "record.md").write_text("\n".join(record) + "\n", encoding="utf-8")
    print(f"wrote {RUN}")
    print(json.dumps(integrity, ensure_ascii=False))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--official-latest-close-date", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    main(args.official_latest_close_date)
