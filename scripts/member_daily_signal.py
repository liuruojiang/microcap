"""Read-only member-basket daily signals, independent of market-wide maintenance.

The input checkpoint is immutable. A confirmed close may produce a separate
candidate checkpoint; publication/adoption belongs to the calling coordinator.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime
import hashlib
import importlib
import json
import math
from pathlib import Path
import shutil
import sys
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

SCHEMA = "member_daily_checkpoint_v1"
RETURN_BASIS = "sina_ratio_ex_reference_v1"
VERSIONS = ("v2.0", "v2.3", "v2.5")
BJ = ZoneInfo("Asia/Shanghai")


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")


def clean(value):
    """Represent unavailable native presentation values as JSON null."""
    return json.loads(pd.Series(value).to_json(date_format="iso", force_ascii=False))


def bound_file(checkpoint_path, specification):
    path = (checkpoint_path.parent / specification["path"]).resolve()
    if not path.is_relative_to(checkpoint_path.parent.resolve()):
        raise ValueError("Checkpoint dependency escapes checkpoint directory")
    raw = path.read_bytes()
    if digest(raw) != specification["sha256"]:
        raise ValueError("Checkpoint dependency hash mismatch: " + str(path))
    return path, raw


def load_checkpoint(path):
    cp = json.loads(path.read_bytes())
    if cp.get("schema") != SCHEMA:
        raise ValueError("Unsupported member daily checkpoint")
    files = {key: bound_file(path, cp[key]) for key in ("period", "history", "turnover", "version_state", "prices")}
    period = json.loads(files["period"][1])
    members = period["effective_members"]
    symbols = [str(row["symbol"]).zfill(6) for row in members]
    if len(symbols) != 100 or len(set(symbols)) != 100 or any(len(s) != 6 or not s.isascii() or not s.isdigit() for s in symbols):
        raise ValueError("Effective period must contain 100 unique symbols")
    weights = [float(row.get("target_weight", 0.01)) for row in members]
    if any(abs(weight - 0.01) > 1e-12 for weight in weights):
        raise ValueError("Daily basket must retain original equal-weight member contract")
    history = pd.read_csv(files["history"][0]).set_index("date")
    history.index = pd.to_datetime(history.index, errors="raise")
    if history.empty or history.index.has_duplicates or not history.index.is_monotonic_increasing:
        raise ValueError("Checkpoint history must be nonempty, unique and increasing")
    if any(stamp != stamp.normalize() or stamp.tzinfo for stamp in history.index):
        raise ValueError("History requires plain session dates")
    if str(history.index[-1].date()) != cp["as_of_date"]:
        raise ValueError("Checkpoint date does not match history")
    price_basis = json.loads(files["prices"][1])
    price_rows = price_basis["rows"]
    if (price_basis.get("schema") != "member_price_basis_v1" or price_basis.get("as_of_date") != cp["as_of_date"]
            or len(price_rows) != 100 or {str(row["symbol"]).zfill(6) for row in price_rows} != set(symbols)
            or any(row.get("previous_raw_date") != cp["as_of_date"] for row in price_rows)):
        raise ValueError("Checkpoint member price basis differs from its effective basket or date")
    numeric = history[["microcap"]].apply(pd.to_numeric, errors="raise")
    if not np.isfinite(numeric).all().all() or not numeric.gt(0).all().all():
        raise ValueError("Invalid microcap checkpoint history")
    return cp, files, period, symbols, history, pd.read_csv(files["turnover"][0]), json.loads(files["version_state"][1])


def validate_clock(document, day, mode, now):
    stamp = datetime.fromisoformat(document["collected_at"])
    if stamp.tzinfo is None or stamp > now:
        raise ValueError("Market observation clock is missing or future")
    if document.get("market_date") != day:
        raise ValueError("Wrong market date")
    local = stamp.astimezone(BJ)
    if mode == "intraday" and (local.date().isoformat() != day or local.hour >= 15):
        raise ValueError("Intraday collection completed on wrong day or after cutoff")
    market_close = datetime.fromisoformat(day + "T15:00:00+08:00")
    if mode == "close_confirmed" and (local < market_close or document.get("close_confirmed") is not True):
        raise ValueError("Unconfirmed closing market observation")


def member_level(quotes, symbols, anchor, previous_level, day, mode, now, previous_raw):
    validate_clock(quotes, day, mode, now)
    rows = quotes["rows"]
    keys = [str(row["symbol"]).zfill(6) for row in rows]
    if len(keys) != 100 or len(set(keys)) != 100 or set(keys) != set(symbols):
        raise ValueError("Member prices must cover the exact effective 100, with no duplicate or substituted member")
    returns = []
    for row in rows:
        if row.get("trade_date") != day or row.get("previous_date") != anchor:
            raise ValueError("Member return dates do not bridge checkpoint to signal day")
        if row.get("return_basis_revision") != RETURN_BASIS or not row.get("source"):
            raise ValueError("Missing or incompatible member return basis/source")
        raw_before = float(row["previous_raw_close"])
        bound_before = previous_raw[str(row["symbol"]).zfill(6)]
        if not math.isfinite(raw_before) or raw_before <= 0 or not math.isclose(raw_before, bound_before, rel_tol=1e-10, abs_tol=1e-10):
            raise ValueError("Member price return does not bind the prior checkpoint raw price")
        current, previous = float(row["close_adj"]), float(row["previous_close_adj"])
        if not math.isfinite(current) or not math.isfinite(previous) or current <= 0 or previous <= 0:
            raise ValueError("Invalid adjusted member price")
        returns.append(current / previous - 1)
    return previous_level * (1 + float(np.mean(returns)))


def native_modules():
    return {version: importlib.import_module("microcap_top100_mom16_biweekly_live_v2_" + version[-1]) for version in VERSIONS}


def calculate_native(version, module, history, turnover):
    """Use original full-history state machines and original row renderers."""
    if version == "v2.0":
        gross = module.embedded_context.base_mod.run_signal(history[["microcap", "hedge"]])
        out = module.overlay_mod.apply_v2_0_execution(gross, turnover, snapshot=False)
    elif version == "v2.3":
        gross = module.build_spread_log_wls_gross(history[["microcap", "hedge"]])
        out = module.apply_overheat_defense(gross, turnover, enabled=module.OVERHEAT_ENABLED)
    else:
        # No synthetic/stale hedge price is supplied to the unhedged strategy.
        gross = module.build_microcap_log_wls_gross(history[["microcap"]])
        out = module.apply_no_target_vol(module.apply_cost(gross, turnover))
    renderer = module.overlay_mod if version == "v2.0" else module
    return renderer._build_signal_row(out, {}), out


def build(root, checkpoint_path, quotes, hedge, day, mode, out, *, now=None, calendar_sessions=None):
    now = now or datetime.now(BJ)
    sys.path.insert(0, str(root.resolve()))
    cp, files, period, symbols, history, turnover, prior_states = load_checkpoint(checkpoint_path)
    if pd.Timestamp(day) <= history.index[-1]:
        raise ValueError("Daily signal must extend the checkpoint, never replay or overwrite it")
    if cp.get("next_trade_date") and day != cp["next_trade_date"]:
        raise ValueError("Signal skips checkpoint's next exchange session")
    if period.get("effective_from", cp["as_of_date"]) > day:
        raise ValueError("Period members are not effective on signal day")
    out.mkdir(parents=True, exist_ok=False)
    result = dict(schema="member_daily_result_v1", market_date=day, publication_mode=mode,
                  generated_at=now.isoformat(), status="PARTIAL", versions=[], warnings=[],
                  member_count=100, quote_coverage="0/100", checkpoint=None,
                  checkpoint_sha256=digest(checkpoint_path.read_bytes()),
                  period_sha256=cp["period"]["sha256"], anchor_date=cp["as_of_date"])
    extended = history.copy()
    member_error = None
    try:
        if calendar_sessions is None:
            from scripts.exchange_calendar import sessions_for_day
            calendar_sessions = sessions_for_day(date.fromisoformat(day))
        sessions = sorted(date.fromisoformat(str(value)) for value in calendar_sessions)
        target = date.fromisoformat(day)
        earlier = [value for value in sessions if value < target]
        if target not in sessions or not earlier or earlier[-1].isoformat() != cp["as_of_date"]:
            raise ValueError("Signal does not follow the checkpoint's immediately preceding exchange session")
        previous_raw = {str(row["symbol"]).zfill(6): float(row["previous_raw_close"])
                        for row in json.loads(files["prices"][1])["rows"]}
        level = member_level(quotes, symbols, cp["as_of_date"], float(history.microcap.iloc[-1]), day, mode, now, previous_raw)
        extended.loc[pd.Timestamp(day), "microcap"] = level
        result["quote_coverage"] = "100/100"
    except (ValueError, KeyError, TypeError, RuntimeError) as exc:
        member_error = str(exc)
    hedge_error = None
    try:
        if hedge is None:
            raise ValueError("Current hedge quote unavailable")
        validate_clock(hedge, day, mode, now)
        value = float(hedge["close"])
        if not hedge.get("source") or not math.isfinite(value) or value <= 0:
            raise ValueError("Invalid current hedge quote/source")
        extended.loc[pd.Timestamp(day), "hedge"] = value
    except (ValueError, KeyError, TypeError) as exc:
        hedge_error = str(exc)
    sys.path.insert(0, str(root.resolve()))
    modules = native_modules()
    prior_states = prior_states.get("versions", prior_states)
    period_overdue = False
    if calendar_sessions is not None and period.get("anchor_day"):
        rebalance = modules["v2.0"].embedded_context.base_mod.build_biweekly_rebalance_dates(pd.DatetimeIndex(calendar_sessions))
        cut = pd.Timestamp(day)
        due = [value for value in rebalance if pd.Timestamp(period["anchor_day"]) < value <= cut]
        period_overdue = bool(due and (due[-1] < cut or mode == "close_confirmed"))
    result["period_status"] = "overdue_reference" if period_overdue else "current"
    if period_overdue:
        result["warnings"].append("New rebalance handoff unavailable; signals describe the previous basket only, not a confirmed new-period target")
    signals = {}
    for version, module in modules.items():
        entry = dict(version=version, revision=module.STRATEGY_REVISION, status="partial", signal=None, reason=None)
        error = member_error or (hedge_error if version != "v2.5" else None)
        try:
            if error:
                raise ValueError(error)
            # Full-history parity protects path-dependent threshold state. Never
            # silently restart a shortened window from cash.
            previous, _ = calculate_native(version, module, history, turnover)
            saved = prior_states.get(version, {})
            if "signal" in saved:
                saved = saved["signal"]
            for field in ("next_holding", "strategy_revision"):
                if field in saved and str(saved[field]) != str(previous.iloc[0][field]):
                    raise ValueError("Checkpoint model-state mismatch: " + field)
            row, _ = calculate_native(version, module, extended, turnover)
            row["date"] = day
            row["signal_timing"] = "close_confirmed" if mode == "close_confirmed" else "intraday_hypothetical_if_now_close"
            row["official_close_confirmed_signal"] = mode == "close_confirmed"
            row["quote_trade_date"] = day
            row["quote_coverage"] = "100/100"
            row["member_daily_revision"] = "fixed_effective_members_v1"
            row.to_csv(out / (version.replace(".", "_") + "_signal.csv"), index=False)
            entry.update(status="reference" if period_overdue else "verified", signal=clean(row.iloc[0].to_dict()))
            signals[version] = entry["signal"]
        except (ValueError, KeyError, RuntimeError, TypeError) as exc:
            entry["reason"] = str(exc)
        result["versions"].append(entry)
    if len(signals) == 3 and not period_overdue:
        result["status"] = "OK"
    price_rows = None
    if mode == "close_confirmed" and len(signals) == 3:
        try:
            price_rows = [{"symbol": str(row["symbol"]).zfill(6), "previous_raw_close": float(row["raw_close"]),
                           "previous_raw_date": day} for row in quotes["rows"]]
            if any(not math.isfinite(row["previous_raw_close"]) or row["previous_raw_close"] <= 0 for row in price_rows):
                raise ValueError("Invalid raw member basis for next checkpoint")
        except (ValueError, TypeError, KeyError) as exc:
            price_rows = None
            result["warnings"].append("Signals retained; next checkpoint unavailable: " + str(exc))
    if price_rows is not None:
        # Partial observations never advance the shared confirmed checkpoint.
        for key in ("period", "turnover"):
            shutil.copyfile(files[key][0], out / (key + files[key][0].suffix))
        extended.rename_axis("date").to_csv(out / "history.csv", date_format="%Y-%m-%d")
        write_json(out / "version_state.json", dict(schema="member_version_state_v1", versions=signals))
        write_json(out / "member_prices.json", dict(schema="member_price_basis_v1", as_of_date=day, rows=price_rows))
        next_cp = dict(cp, as_of_date=day, period_status="awaiting_handoff" if period_overdue else "current")
        next_days = [value.isoformat() for value in sessions if value.isoformat() > day]
        if next_days:
            next_cp["next_trade_date"] = next_days[0]
        else:
            next_cp.pop("next_trade_date", None)
        for key, name in (("period", "period.json"), ("turnover", "turnover.csv"), ("history", "history.csv"), ("version_state", "version_state.json"), ("prices", "member_prices.json")):
            next_cp[key] = dict(path=name, sha256=digest((out / name).read_bytes()))
        write_json(out / "checkpoint.json", next_cp)
        result["checkpoint"] = "checkpoint.json"
    write_json(out / "quotes.json", quotes)
    if hedge is not None:
        write_json(out / "hedge.json", hedge)
    write_json(out / "result.json", result)
    return result


def saved_close_result(checkpoint_path, day, out, *, now=None):
    """Render an already confirmed checkpoint; never collect or recalculate."""
    from scripts.member_period_handoff import verify
    now = now or datetime.now(BJ)
    verified = verify(checkpoint_path)
    close = datetime.fromisoformat(day + "T15:00:00+08:00")
    if close > now or verified["as_of_date"] != day:
        raise ValueError("Saved close checkpoint is future, unclosed or belongs to another market date")
    cp, files, period, symbols, history, turnover, states = load_checkpoint(checkpoint_path)
    states = states.get("versions", states)
    entries = []
    for version in VERSIONS:
        row = states[version]
        confirmed = row.get("official_close_confirmed_signal")
        if str(confirmed).lower() != "true" or row.get("signal_timing") not in {"close_confirmed", "close_confirmed_anchor"}:
            raise ValueError("Saved state is not an original confirmed-close signal: " + version)
        if not all(key in row and row[key] is not None for key in ("current_holding", "next_holding", "strategy_revision")):
            raise ValueError("Saved close state lacks original signal fields")
        entries.append(dict(version=version, revision=row["strategy_revision"],
                            status="reference" if cp.get("period_status") == "awaiting_handoff" else "verified",
                            signal=row, reason=None))
    # The bootstrap binds the original whole-manifest identity. Subsequent
    # checkpoints inherit that source closure and bind their daily history,
    # member prices and complete native signal rows by immutable file hashes.
    if not period.get("source_manifest_sha256") or not period.get("source_files"):
        raise ValueError("Saved close checkpoint lacks its original bound source closure")
    out.mkdir(parents=True, exist_ok=False)
    exported_cp = {key: value for key, value in cp.items() if key != "_directory"}
    for key, (source, raw) in files.items():
        name = key + source.suffix if key != "prices" else "member_prices.json"
        (out / name).write_bytes(raw)
        exported_cp[key] = dict(path=name, sha256=digest(raw))
    write_json(out / "checkpoint.json", exported_cp)
    for entry in entries:
        pd.DataFrame([entry["signal"]]).to_csv(out / (entry["version"].replace(".", "_") + "_signal.csv"), index=False)
    reference = cp.get("period_status") == "awaiting_handoff"
    result = dict(schema="member_daily_result_v1", market_date=day, publication_mode="close_confirmed",
                  generated_at=now.isoformat(), reused_close_checkpoint=True,
                  collection_performed=False, status="PARTIAL" if reference else "OK", versions=entries,
                  warnings=["Reused previously confirmed close calculation; no new market collection or signal calculation"]
                  + (["Rebalance handoff pending; previous basket reference only"] if reference else []),
                  member_count=100, quote_coverage="100/100", quote_coverage_basis="saved_checkpoint_member_prices",
                  period_status="overdue_reference" if reference else "current", checkpoint="checkpoint.json",
                  checkpoint_sha256=digest(checkpoint_path.read_bytes()), period_sha256=cp["period"]["sha256"],
                  anchor_date=cp["as_of_date"])
    write_json(out / "result.json", result)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("root", "checkpoint", "out"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--quotes", type=Path)
    parser.add_argument("--hedge", type=Path)
    parser.add_argument("--from-checkpoint", action="store_true")
    parser.add_argument("--date", required=True)
    parser.add_argument("--mode", choices=("intraday", "close_confirmed"), required=True)
    args = parser.parse_args(argv)
    sys.path.insert(0, str(args.root.resolve()))
    if args.from_checkpoint:
        if args.mode != "close_confirmed" or args.quotes or args.hedge:
            parser.error("--from-checkpoint requires close_confirmed without new quotes or hedge")
        result = saved_close_result(args.checkpoint, args.date, args.out)
    else:
        if not args.quotes:
            parser.error("--quotes is required for a new daily calculation")
        result = build(args.root, args.checkpoint, json.loads(args.quotes.read_bytes()),
                       json.loads(args.hedge.read_bytes()) if args.hedge else None,
                       args.date, args.mode, args.out)
    print(json.dumps({"status": result["status"], "result": str(args.out / "result.json")}))
    return 0  # Valid partial reports are deliverable; each component has status.


if __name__ == "__main__":
    raise SystemExit(main())
