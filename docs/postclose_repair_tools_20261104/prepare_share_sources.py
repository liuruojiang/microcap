"""Establish native current share-source evidence before the long whole refresh."""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import sys


def prepare(root, target):
    root = root.resolve()
    expected = "14b896a0915f700e5b999ff6c47676e6f8acaca1"
    actual = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
    if actual != expected or target != "2026-11-04":
        raise RuntimeError("Not the reviewed repair source/date")
    sys.path.insert(0, str(root))
    from scripts.share_change_source import refresh_share_change, verify_share_cache
    from scripts.realtime_state_bundle import current_notice_member_symbols
    directory = root / ".microcap_index_cache/share_sources"
    # The source directory retains the whole ranking universe. Only the native
    # actual-position/current-target union is this repair's daily share scope.
    symbols = current_notice_member_symbols(root)
    if len(symbols) != 102:
        raise RuntimeError("Native 102-stock actual-position/target union differs")
    if any(not (directory / (symbol + ".json")).is_file() for symbol in symbols):
        raise RuntimeError("Native member share-source state missing")
    def one(symbol):
        cache = root / ".microcap_index_cache/share_change" / (symbol + ".csv")
        before = cache.read_bytes()
        # All successful sources are preserved or adopted as exact successors.
        # This final pass is deliberately offline; never repeat successful HTTP.
        refresh_share_change(root=root, symbol=symbol, cache_path=cache, as_of=target, allow_network=False)
        verify_share_cache(root=root, symbol=symbol, as_of=target)
        if cache.read_bytes() != before:
            raise RuntimeError("Native unchanged-tail unexpectedly changed share history: " + symbol)
        print("[native-share-preparation] verified " + symbol + " " + target, flush=True)
        return {"symbol": symbol, "query_end": target, "share_csv_sha256": hashlib.sha256(before).hexdigest()}
    complete = []
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {pool.submit(one, symbol): symbol for symbol in symbols}
        try:
            for future in as_completed(futures):
                complete.append(future.result())
        except BaseException:
            for pending in futures:
                pending.cancel()
            raise
    return {"ok": True, "records": sorted(complete, key=lambda row: row["symbol"]),
            "native_member_union_sha256": hashlib.sha256("\n".join(symbols).encode()).hexdigest(),
            "recorded_at": datetime.now(timezone.utc).isoformat(), "native_verification": True,
            "scope": "official per-symbol share-source freshness; no panel, NAV, manifest, SMTP or marker operation"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--target", required=True)
    parser.add_argument("--audit", type=Path, required=True)
    args = parser.parse_args()
    result = prepare(args.root, args.target)
    args.audit.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps({"ok": result["ok"], "verified_stocks": len(result["records"])}))
