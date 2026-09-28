"""Independent read-only challenge of selected ST evidence rows."""

from __future__ import annotations

import csv
import hashlib
import json
import re
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from pathlib import Path

import fitz
import requests


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]


def read_csv(name: str) -> list[dict[str, str]]:
    with (HERE / name).open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


# Dates below were transcribed from the PDF body, not inferred from title or
# posting date. An end on an ST -> ST reclassification is conservative here:
# all affected target dates precede that date.
ENTRY_EFFECTIVE = {
    ("600080", "2010-04-26"): "2010-04-27",
    ("600080", "2020-06-01"): "2020-06-02",
    ("600080", "2026-04-29"): "2026-04-30",
    ("600082", "2026-04-11"): "2026-04-14",
    ("600119", "2019-04-30"): "2019-05-06",
    ("600119", "2026-04-27"): "2026-04-28",
    ("600180", "2008-06-26"): "2008-06-27",
    ("600180", "2026-04-29"): "2026-04-30",
    ("600187", "2026-04-30"): "2026-05-06",
    ("600423", "2017-04-29"): "2017-05-03",
    ("600423", "2026-04-25"): "2026-04-28",
    ("600476", "2026-04-28"): "2026-04-29",
    ("600491", "2026-04-30"): "2026-05-06",
    ("600543", "2023-04-28"): "2023-05-04",
    ("600543", "2026-04-24"): "2026-04-27",
    ("600678", "2009-06-29"): "2009-06-30",
    ("600678", "2026-04-29"): "2026-04-30",
    ("600818", "2026-04-28"): "2026-04-29",
    ("603272", "2026-04-28"): "2026-04-29",
    ("603359", "2026-04-29"): "2026-04-30",
    ("603378", "2026-04-30"): "2026-05-06",
    ("603729", "2021-04-30"): "2021-05-06",
    ("603729", "2026-04-29"): "2026-04-30",
    ("688121", "2026-07-06"): "2026-07-07",
    ("688189", "2026-06-10"): "2026-06-11",
}

EXIT_EFFECTIVE = {
    ("600080", "2012-06-11"): "2012-06-12",
    ("600080", "2021-05-11"): "2021-05-12",
    ("600119", "2021-05-14"): "2021-05-17",
    ("600180", "2011-12-14"): "2011-12-15",  # *ST -> ST, not fully clean
    ("600423", "2021-05-19"): "2021-05-20",
    ("600543", "2024-07-05"): "2024-07-08",
    ("600678", "2013-02-01"): "2013-02-04",
    ("603729", "2022-05-30"): "2022-05-31",
}


def check_pdf(row: dict[str, str], notice_field: str, effect: str) -> dict[str, str | bool]:
    path = Path(row["local_path"])
    data = path.read_bytes()
    body = "".join(page.get_text() for page in fitz.open(stream=data, filetype="pdf"))
    compact = re.sub(r"\s+", "", body)
    y, m, d = (int(v) for v in effect.split("-"))
    date_present = bool(re.search(rf"{y}年0?{m}月0?{d}日", compact))
    code_present = bool(re.search(r"(?:证券|股票)代码[：:]?" + row["symbol"], compact[:500]))
    st_body = bool(re.search(r"(?:风险警示|特别处理)", compact))
    return {
        "symbol": row["symbol"],
        "notice_date": row[notice_field],
        "effective_date": effect,
        "source_url": row["source_url"],
        "sha256": row["sha256"],
        "local_sha_match": hashlib.sha256(data).hexdigest() == row["sha256"],
        "local_size_match": len(data) == int(row["bytes"]),
        "code_in_pdf_header": code_present,
        "effective_date_in_pdf": date_present,
        "st_text_in_pdf": st_body,
    }


def check_remote(row: dict[str, str]) -> dict[str, str | bool]:
    out = {"source_url": row["source_url"], "remote_status": "", "remote_sha_match": False, "remote_error": ""}
    try:
        response = requests.get(row["source_url"], timeout=30)
        out["remote_status"] = str(response.status_code)
        out["remote_sha_match"] = response.status_code == 200 and hashlib.sha256(response.content).hexdigest() == row["sha256"]
    except Exception as exc:
        out["remote_error"] = repr(exc)
    return out


def main() -> None:
    entries = read_csv("entry_notice_pdf_evidence.csv")
    exits = read_csv("exit_notice_pdf_evidence.csv")
    targets = [x for x in read_csv("strict_target_status.csv") if x["strict_title_status"] == "explicit_entry_interior"]
    assert len(entries) == len(ENTRY_EFFECTIVE) == 25
    assert len(exits) == len(EXIT_EFFECTIVE) == 8
    assert len(targets) == 236
    pdf_rows = [check_pdf(x, "entry_notice_date", ENTRY_EFFECTIVE[(x["symbol"], x["entry_notice_date"])]) for x in entries]
    pdf_rows += [check_pdf(x, "exit_notice_date", EXIT_EFFECTIVE[(x["symbol"], x["exit_notice_date"])]) for x in exits]
    with ThreadPoolExecutor(max_workers=6) as pool:
        remote_rows = list(pool.map(check_remote, entries + exits))
    remote_by_url = {x["source_url"]: x for x in remote_rows}
    for x in pdf_rows:
        x.update(remote_by_url[x["source_url"]])
    with (HERE / "challenge_pdf_checks.csv").open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(pdf_rows[0]))
        writer.writeheader()
        writer.writerows(pdf_rows)

    intervals = []
    for row in entries:
        symbol = row["symbol"]
        start = ENTRY_EFFECTIVE[(symbol, row["entry_notice_date"])]
        later_exits = sorted(
            [EXIT_EFFECTIVE[(x["symbol"], x["exit_notice_date"])] for x in exits
             if x["symbol"] == symbol and x["exit_notice_date"] > row["entry_notice_date"]]
        )
        later_entries = sorted(
            [ENTRY_EFFECTIVE[(x["symbol"], x["entry_notice_date"])] for x in entries
             if x["symbol"] == symbol and x["entry_notice_date"] > row["entry_notice_date"]]
        )
        end = min(later_exits[0], later_entries[0]) if later_exits and later_entries else (later_exits[0] if later_exits else (later_entries[0] if later_entries else None))
        intervals.append({"symbol": symbol, "entry_notice_date": row["entry_notice_date"], "start": start, "end": end})

    checks = []
    for row in targets:
        match = [i for i in intervals if i["symbol"] == row["symbol"] and i["start"] <= row["target_date"] and (i["end"] is None or row["target_date"] < i["end"])]
        checks.append({**row, "matched_pdf_intervals": len(match),
                       "matched_entry_notice_date": match[0]["entry_notice_date"] if len(match) == 1 else "",
                       "pdf_effective_start": match[0]["start"] if len(match) == 1 else "",
                       "pdf_effective_end": match[0]["end"] if len(match) == 1 else ""})
    with (HERE / "challenge_target_checks.csv").open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(checks[0]))
        writer.writeheader()
        writer.writerows(checks)
    members = read_csv(str(ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv"))
    extra = []
    for row in members:
        symbol = row["symbol"].zfill(6)
        target_date = row["rebalance_date"]
        if symbol == "000010" and "2007-03-21" <= target_date < "2013-10-18":
            source = "https://static.cninfo.com.cn/finalpage/2007-03-20/21601653.PDF"
            sha = "4ad99c019e32e9cd4cf929cf735e874a1e86b298baf99bf75ec1a03a6e5c404e"
        elif symbol == "600080" and "2008-06-06" <= target_date < "2010-04-27":
            source = "https://static.cninfo.com.cn/finalpage/2008-06-05/40271256.PDF"
            sha = "5cb797e7bf1d8182a1a5dbe45f2a88a5efc264ca3cde5a9ecac35a5747192579"
        else:
            continue
        meta = json.loads((ROOT / ".microcap_index_cache/security_meta" / f"{symbol}.json").read_text(encoding="utf-8"))
        cached_st = any(i["start"] <= target_date and (i.get("end") is None or target_date <= i["end"])
                        for i in meta.get("st_intervals") or [])
        extra.append({"symbol": symbol, "target_date": target_date, "rank": row["rank"],
                      "source_url": source, "source_sha256": sha, "cached_meta_st": cached_st})
    with (HERE / "challenge_additional_targets.csv").open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(extra[0]))
        writer.writeheader()
        writer.writerows(extra)
    explicit_keys = {(x["symbol"], x["target_date"], x["rank"]) for x in targets}
    extra_keys = {(x["symbol"], x["target_date"], x["rank"]) for x in extra}
    summary = {
        "pdf_count": len(pdf_rows),
        "pdf_check_failures": {key: [f"{x['symbol']}:{x['notice_date']}" for x in pdf_rows if not x[key]]
                               for key in ("local_sha_match", "local_size_match", "code_in_pdf_header", "effective_date_in_pdf", "st_text_in_pdf", "remote_sha_match")},
        "target_count": len(targets),
        "unique_target_keys": len({(x["symbol"], x["target_date"], x["rank"]) for x in targets}),
        "target_interval_match_counts": dict(Counter(x["matched_pdf_intervals"] for x in checks)),
        "cached_meta_st_counts": dict(Counter(x["cached_meta_st"] for x in checks)),
        "per_entry_counts": dict(Counter(f"{x['symbol']}:{x['matched_entry_notice_date']}" for x in checks)),
        "additional_target_count": len(extra),
        "additional_by_symbol": dict(Counter(x["symbol"] for x in extra)),
        "additional_cached_st_counts": dict(Counter(str(x["cached_meta_st"]) for x in extra)),
        "additional_overlap_explicit": len(explicit_keys & extra_keys),
        "proved_unique_lower_bound": len(explicit_keys | extra_keys),
    }
    (HERE / "challenge_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
