"""Refresh exactly ten legacy metadata records with real official source functions."""
import concurrent.futures
import hashlib
import json
import shutil
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
sys.path.insert(0, str(ROOT))
import microcap_top100_mom16_biweekly_live_v2_0 as v
v._sync_embedded_base_config()
freq = v.base_mod.freq_mod
if "--https" in sys.argv:
    original_post = freq.requests.post
    def official_https_post(url, *args, **kwargs):
        if url == "http://www.cninfo.com.cn/new/hisAnnouncement/query":
            url = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
        return original_post(url, *args, **kwargs)
    freq.requests.post = official_https_post
SYMBOLS = ["300876", "300928", "301135", "301192", "301353", "301520", "301578", "603585", "603836", "688592"]
BACKUP = HERE / "legacy_ten_metadata_backup"
BACKUP.mkdir(exist_ok=True)
old = {}
for symbol in SYMBOLS:
    path = freq.resolve_security_meta_path(symbol)
    raw = path.read_bytes()
    backup = BACKUP / (symbol + ".json")
    if backup.exists() and backup.read_bytes() != raw:
        raise RuntimeError("Backup already records another state: " + symbol)
    backup.write_bytes(raw)
    old[symbol] = (path, raw, json.loads(raw))


def refresh(symbol):
    path, before, previous = old[symbol]
    # load_security_meta accepts legacy meta_version=2 without a policy key.
    # Explicit official rebuild is therefore required to gather missing evidence.
    candidate = freq.build_security_meta(symbol)
    valid = bool(candidate and candidate.get("st_notice_policy_version") == freq.ST_NOTICE_POLICY_VERSION
                 and candidate.get("notice_query_status") == "ok"
                 and candidate.get("name_history_status") in {"ok", "not_applicable"}
                 and candidate.get("current_st_history_resolved") is True)
    generated = path.read_bytes()
    result = {"symbol": symbol, "before_sha256": hashlib.sha256(before).hexdigest(),
              "generated_sha256": hashlib.sha256(generated).hexdigest(), "source_evidence_valid": valid,
              "cninfo_protocol": "https" if "--https" in sys.argv or "https://www.cninfo.com.cn/new/hisAnnouncement/query" in v.FREQ_SOURCE else "http",
              "changed_fields": {key: {"before": previous.get(key), "after": candidate.get(key)}
                                 for key in set(previous) | set(candidate or {}) if previous.get(key) != (candidate or {}).get(key)}}
    if not valid:
        (HERE / (symbol + "_failed_metadata_refresh.json")).write_bytes(generated)
        path.write_bytes(before)
        result["restored_original"] = True
    result["after_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    print(json.dumps({"symbol": symbol, "valid": valid}, ensure_ascii=False), flush=True)
    return result


results = []
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
    futures = {pool.submit(refresh, symbol): symbol for symbol in SYMBOLS}
    for future in concurrent.futures.as_completed(futures):
        symbol = futures[future]
        try:
            results.append(future.result())
        except Exception as exc:
            path, raw, previous = old[symbol]
            path.write_bytes(raw)
            results.append({"symbol": symbol, "source_evidence_valid": False, "restored_original": True, "error": str(exc)})
        (HERE / "ten_metadata_refresh_evidence.json").write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
if not all(item.get("source_evidence_valid") for item in results):
    raise SystemExit("Incomplete source evidence; failed records restored")
