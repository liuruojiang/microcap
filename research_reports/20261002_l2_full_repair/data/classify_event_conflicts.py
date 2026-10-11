"""Classify normalized dividend conflicts without silently selecting a winner."""

import csv
import json
import math
from collections import defaultdict, Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parent
SRC = ROOT / "research_reports/20260926_l1_repair/corporate_actions"


def read(path):
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def num(x):
    try:
        y = float(x or 0)
        return y if math.isfinite(y) else 0
    except ValueError:
        return 0


def dt(x):
    return (x or "")[:10]


def main():
    c, s = defaultdict(list), defaultdict(list)
    for source in (SRC / "cninfo_distribution_events.csv", SRC / "cninfo_distribution_events_initial_seed_extra.csv",
                   OUT / "latest_old_members_cninfo_full_history.csv"):
        for r in read(source):
            key = (r["symbol"].zfill(6), dt(r.get("除权日")))
            if source.name == "latest_old_members_cninfo_full_history.csv" and key[1] < "2026-09-25":
                continue
            c[key].append({"source": source.name, "record": dt(r.get("股权登记日")),
                           "bonus": num(r.get("送股比例")) + num(r.get("转增比例")), "cash": num(r.get("派息比例")),
                           "announcement": dt(r.get("实施方案公告日期"))})
    for source in (SRC / "sina_held_2010_2014_distribution_and_rights.csv",
                   SRC / "sina_recovered_delisted_distribution_and_rights.csv",
                   OUT / "sina_tail_dividend_full_history.csv"):
        for r in read(source):
            if r.get("indicator") != "分红":
                continue
            key = (r["symbol"].zfill(6), dt(r.get("除权除息日")))
            if source.name == "sina_tail_dividend_full_history.csv" and key[1] < "2026-09-25":
                continue
            s[key].append({"source": source.name, "record": dt(r.get("股权登记日")),
                           "bonus": num(r.get("送股")) + num(r.get("转增")), "cash": num(r.get("派息")),
                           "announcement": dt(r.get("公告日期"))})
    target = [r for r in read(OUT / "event_candidates_2010_20260930.csv") if r["source_status"] == "source_conflict_or_duplicate"]
    old_held = {(r["symbol"], r["ex_date"]):r for r in read(OUT / "old_path_event_overlaps.csv")}
    out = []
    for r in target:
        key = (r["symbol"], r["ex_date"])
        ca, sa = c[key], s[key]
        ci = {(x["record"], x["bonus"], x["cash"]) for x in ca}
        si = {(x["record"], x["bonus"], x["cash"]) for x in sa}
        if len(ci) > 1 or len(si) > 1:
            kind = "distinct_rows_same_source"
        elif not ca or not sa:
            kind = "single_source_duplicate"
        else:
            x, y = ca[0], sa[0]
            db, dc = abs(x["bonus"] - y["bonus"]), abs(x["cash"] - y["cash"])
            if x["record"] != y["record"]:
                kind = "record_date_disagreement"
            elif max(db, dc) <= 0.000031:
                kind = "ratio_precision_difference_le_0p000031"
            else:
                kind = "substantive_ratio_disagreement"
        old = old_held.get(key)
        out.append({"symbol":key[0],"ex_date":key[1],"kind":kind,
                    "old_held_at_record_snapshot": old["old_held_at_record_snapshot"] if old else "False",
                    "old_held_at_ex_snapshot": old["old_held_at_ex_snapshot"] if old else "False",
                    "cninfo_rows":json.dumps(ca,ensure_ascii=False),"sina_rows":json.dumps(sa,ensure_ascii=False)})
    path=OUT / "event_conflict_classification.csv"
    with path.open("w",encoding="utf-8",newline="") as f:
        w=csv.DictWriter(f,fieldnames=list(out[0]))
        w.writeheader();w.writerows(out)
    summary={"count":len(out),"kind_counts":dict(Counter(x["kind"] for x in out)),
             "old_path_overlap_count":sum(x["old_held_at_record_snapshot"]=="True" or x["old_held_at_ex_snapshot"]=="True" for x in out),
             "earliest_all":out[0],
             "earliest_old_path":next((x for x in out if x["old_held_at_record_snapshot"]=="True" or x["old_held_at_ex_snapshot"]=="True"),None)}
    (OUT / "event_conflict_summary.json").write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding="utf-8")
    print(json.dumps(summary,ensure_ascii=False,indent=2))

if __name__=="__main__":
    main()
