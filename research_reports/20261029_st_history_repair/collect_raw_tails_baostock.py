"""Resumable single-connection raw tail evidence. Research only, no shared writes."""
from pathlib import Path
import argparse
import csv
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import shutil
import socket
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
RUN = Path(__file__).resolve().parent
OUT = RUN / "raw_tail_20261029_baostock"
TARGET = "2026-10-29"
FIELDS = "date,code,close,preclose,volume,tradestatus,isST,adjustflag"
sys.path.insert(0, str(ROOT))


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def save(path, obj):
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def read_csv(path):
    with path.open(encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


def positive(value, field):
    number = Decimal(value)
    if not number.is_finite() or number <= 0:
        raise ValueError("Invalid finite positive " + field)
    return number


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbols", nargs="+")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--revalidate-blocked", action="store_true", help="Revalidate retained raw evidence after a reviewed parser/input repair; never refetch")
    args = parser.parse_args()
    if bool(args.symbols) == bool(args.all):
        raise ValueError("Choose explicit canary symbols or --all")
    for part in ["raw", "receipts", "normalized_raw", "suspension_evidence"]:
        (OUT / part).mkdir(parents=True, exist_ok=True)
    source_rows = read_csv(RUN / "resume_audit/delivery_old4975_input_coverage.csv")
    sources = {r["symbol"]: Path(r["raw_path"]) for r in source_rows}
    for path in (RUN / "cross_source_prices/normalized_raw").glob("*.csv"):
        if path.stem in sources:
            raise ValueError("New-candidate overlap with old universe")
        sources[path.stem] = path
    sources["688287"] = RUN / "cross_source_prices/missing_session_review/688287_observed_trade_prices_candidate.csv"
    assert len(sources) == 5438, len(sources)
    master = {r["symbol"]: r for r in read_csv(RUN / "cross_source_official/security_master_combined_candidate.csv")}
    from scripts.exchange_calendar import sessions_for_day, SOURCE
    calpath = OUT / "calendar.json"
    if calpath.exists():
        calendar = json.loads(calpath.read_text())
        assert calendar["source"] == SOURCE and calendar["target"] == TARGET
        dates = calendar["sessions"]
    else:
        dates = [d.isoformat() for d in sessions_for_day(date.fromisoformat(TARGET))]
        save(calpath, {"source": SOURCE, "target": TARGET, "sessions": dates})
    assert TARGET in dates
    sessions = set(dates)
    selected = sorted(sources) if args.all else args.symbols
    assert len(set(selected)) == len(selected) and set(selected) <= set(sources)
    import baostock as bs
    socket.setdefaulttimeout(15)
    login = None if args.revalidate_blocked else bs.login()
    if login is not None and login.error_code != "0":
        raise RuntimeError("BaoStock login failed: " + login.error_code)
    failures = 0
    records = []
    started = time.monotonic()
    try:
        for symbol in selected:
            receipt_path = OUT / "receipts" / (symbol + ".json")
            source = sources[symbol]
            source_sha = sha(source)
            retained = None
            if receipt_path.exists():
                previous = json.loads(receipt_path.read_text())
                revalidate = args.revalidate_blocked and previous["status"] == "blocked" and previous.get("raw_path")
                if previous["input_sha256"] != source_sha and not revalidate:
                    raise RuntimeError("Source changed after saved receipt: " + symbol)
                for kind in ["raw", "candidate"]:
                    if previous.get(kind + "_path"):
                        assert sha(Path(previous[kind + "_path"])) == previous[kind + "_sha256"]
                if not revalidate:
                    records.append(previous)
                    continue
                if sha(Path(previous["input_path"])) != previous["input_sha256"]:
                    raise RuntimeError("Original input was modified, rather than a separate reviewed candidate")
                history = OUT / "validation_history"
                history.mkdir(exist_ok=True)
                shutil.copyfile(receipt_path, history / (symbol + "_" + sha(receipt_path) + ".json"))
                retained = json.loads(Path(previous["raw_path"]).read_text())
            rows = read_csv(source)
            assert rows and set(rows[0]) == {"date", "close_raw"}, str(source)
            old_dates = [date.fromisoformat(r["date"]).isoformat() for r in rows]
            if old_dates != sorted(set(old_dates)):
                raise ValueError("Old raw dates not sorted and unique: " + symbol)
            start = old_dates[max(0, len(old_dates) - 10)]
            delist = master.get(symbol, {}).get("delist_date", "")
            end = min(TARGET, (date.fromisoformat(delist) - timedelta(days=1)).isoformat()) if delist else TARGET
            # Do not read future input rows into this fixed-session candidate.
            if old_dates[-1] > TARGET:
                raise ValueError("Input extends past target; requires a separately reviewed slice: " + symbol)
            code = ("sh." if symbol.startswith("6") else "sz.") + symbol
            if retained:
                assert retained["request"]["code"] == code and retained["request"]["end"] >= end
                assert retained["request"]["fields"] == FIELDS and retained["request"]["adjustflag"] == "3"
                start = retained["request"]["start"]
            rec = {"symbol": symbol, "provider": "baostock", "approved": False,
                   "input_path": str(source), "input_sha256": source_sha, "target_date": TARGET,
                   "query_start": start, "query_end": end, "official_delist_date": delist,
                   "code": code, "adjustflag": "3", "calendar_sha256": sha(calpath),
                   "retrieved_at_utc": datetime.now(timezone.utc).isoformat()}
            provider_failure = False
            try:
                if start > end:
                    raise ValueError("Input dates conflict with official delisting date")
                if retained:
                    raw = retained
                    rec["revalidated_saved_raw_without_query"] = True
                else:
                    if args.revalidate_blocked:
                        raise ValueError("Offline validation has no retained raw evidence")
                    result = bs.query_history_k_data_plus(code, FIELDS, start_date=start, end_date=end, frequency="d", adjustflag="3")
                    values = []
                    while result.error_code == "0" and result.next():
                        values.append(result.get_row_data())
                    raw = {"request": {"code": code, "fields": FIELDS, "start": start, "end": end, "frequency": "d", "adjustflag": "3"},
                           "error_code": result.error_code, "error_msg": result.error_msg, "fields": result.fields, "rows": values}
                values = raw["rows"]
                rawpath = OUT / "raw" / (symbol + ".json")
                if not retained:
                    save(rawpath, raw)
                rec.update(raw_path=str(rawpath), raw_sha256=sha(rawpath), provider_rows=len(values))
                rec["actual_request"] = raw["request"]
                rec["evaluation_start"] = start
                rec["evaluation_end"] = end
                if raw["error_code"] != "0" or not values:
                    provider_failure = True
                    raise RuntimeError("Provider query failed or empty: " + raw["error_code"] + ": " + raw["error_msg"])
                if raw["fields"] != FIELDS.split(","):
                    raise ValueError("Provider field identity mismatch")
                data = [dict(zip(raw["fields"], r, strict=True)) for r in values]
                reviewbase = RUN / "raw_status_corrections"
                reviewpath = reviewbase / (symbol + ".json")
                if reviewpath.exists():
                    from scripts.tradeability_evidence import reviewed_provider_status
                    reviews = json.loads(reviewpath.read_text(encoding="utf-8"))
                    corrections = reviewed_provider_status(reviewbase, reviews, symbol)
                    used = set()
                    for row in data:
                        if row["date"] not in corrections:
                            continue
                        correction = corrections[row["date"]]
                        if (correction["provider_raw_sha256"] != sha(rawpath)
                                or correction["original_status"] != int(row["tradestatus"])
                                or abs(correction["independently_observed_close"] - float(row["close"])) > 1e-8
                                or (correction["corrected_status"] == 1 and abs(float(row["volume"]) - correction["independently_observed_volume"]) > 1e-8)):
                            raise ValueError("Reviewed correction does not bind original raw row")
                        row["tradestatus"] = str(correction["corrected_status"])
                        used.add(row["date"])
                    if used != set(corrections):
                        raise ValueError("Unused status correction")
                    rec["provider_status_review_bundle"] = str(reviewpath)
                    rec["provider_status_review_bundle_sha256"] = sha(reviewpath)
                if any(not raw["request"]["start"] <= r["date"] <= raw["request"]["end"] for r in data):
                    raise ValueError("Original response outside actual request interval")
                data = [r for r in data if start <= r["date"] <= end]
                actual_dates = [r["date"] for r in data]
                if actual_dates != sorted(set(actual_dates)):
                    raise ValueError("Provider dates not sorted and unique")
                for row in data:
                    if row["code"] != code or row["adjustflag"] != "3" or not start <= row["date"] <= end or row["date"] not in sessions:
                        raise ValueError("Provider issuer/adjustment/date/calendar mismatch")
                    positive(row["close"], "close")
                    positive(row["preclose"], "preclose")
                    if row["tradestatus"] not in {"0", "1"} or row["isST"] not in {"0", "1"}:
                        raise ValueError("Provider trading/ST status malformed")
                    if row["tradestatus"] == "0" and row["volume"] == "":
                        # Real BaoStock suspended rows use missing volume. Keep that N/A,
                        # do not invent a traded volume or include the row as a trade.
                        continue
                    volume = Decimal(row["volume"])
                    if not volume.is_finite() or volume < 0:
                        raise ValueError("Invalid volume")
                    if row["tradestatus"] == "0" and volume != 0:
                        raise ValueError("Suspended row has nonzero volume")
                expected_sessions = {d for d in sessions if start <= d <= end}
                missing = sorted(expected_sessions - set(actual_dates))
                if missing:
                    rec["missing_sessions"] = missing
                    raise ValueError("Provider omitted exchange sessions; suspension cannot be inferred")
                old_by_date = {r["date"]: r for r in rows}
                overlap = [r for r in data if r["date"] in old_by_date and r["tradestatus"] == "1"]
                rec["overlap_trade_days"] = len(overlap)
                if len(overlap) < 5:
                    raise ValueError("Fewer than five real traded overlapping sessions")
                differences = [{"date": r["date"], "old": old_by_date[r["date"]]["close_raw"], "new": r["close"]}
                               for r in data if r["date"] in old_by_date and
                               abs(Decimal(old_by_date[r["date"]]["close_raw"]) - Decimal(r["close"])) > Decimal("0.00000001")]
                if differences:
                    rec["overlap_conflicts"] = differences
                    raise ValueError("Existing raw close conflicts; no historical values were overwritten")
                suspended = [r for r in data if r["tradestatus"] == "0"]
                save(OUT / "suspension_evidence" / (symbol + ".json"), suspended)
                additions = [{"date": r["date"], "close_raw": r["close"]} for r in data
                             if r["date"] > old_dates[-1] and r["tradestatus"] == "1"]
                candidate = OUT / "normalized_raw" / (symbol + ".csv")
                if additions:
                    with candidate.open("w", encoding="utf-8", newline="") as stream:
                        writer = csv.DictWriter(stream, fieldnames=["date", "close_raw"], lineterminator="\n")
                        writer.writeheader()
                        writer.writerows(rows + additions)
                else:
                    shutil.copyfile(source, candidate)
                if sha(source) != source_sha:
                    raise RuntimeError("Source changed during collection")
                rec.update(status="candidate_tail_verified", candidate_path=str(candidate), candidate_sha256=sha(candidate),
                           added_real_trade_rows=len(additions), suspended_rows=len(suspended), old_rows=len(rows),
                           candidate_rows=len(rows) + len(additions), candidate_last_date=(additions or rows)[-1]["date"],
                           evidence_through=end, suspended_rows_not_added_as_trades=True)
            except Exception as exc:
                rec.update(status="blocked", error_type=type(exc).__name__, error=str(exc))
                if isinstance(exc, (socket.timeout, OSError)):
                    provider_failure = True
            save(receipt_path, rec)
            records.append(rec)
            failures = failures + 1 if provider_failure else 0
            save(OUT / "progress.json", {"approved": False, "selected": len(selected), "processed": len(records),
                 "candidate_count": sum(r["status"] == "candidate_tail_verified" for r in records),
                 "blocked_count": sum(r["status"] == "blocked" for r in records),
                 "elapsed_seconds": time.monotonic() - started, "stopped_provider_failure": failures >= 3})
            if len(records) % 50 == 0 or not args.all:
                print(symbol, rec["status"], rec.get("error", ""), "processed", len(records), flush=True)
            if failures >= 3:
                break
    finally:
        if login is not None:
            bs.logout()
        save(OUT / ("batch_summary.json" if args.all else "canary_summary.json"),
             {"approved": False, "selected": selected, "records": records, "elapsed_seconds": time.monotonic() - started,
              "stopped_provider_failure": failures >= 3, "formal_written": False})


if __name__ == "__main__":
    main()
