"""Independent 600633 suspension/relisting share and price continuity check."""

from __future__ import annotations

import csv
import hashlib
import json
from decimal import Decimal
from pathlib import Path

from pypdf import PdfReader


ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
NOTICE = HERE / "600633_20110923_relist.pdf"
RAW = ROOT / ".microcap_index_cache/prices_raw/600633.csv"
BARS = ROOT / "research_reports/20261002_l2_full_repair/engine/queried_baostock_bars.csv"
EVENTS = ROOT / "research_reports/20261002_l2_full_repair/data/event_candidates_2010_20260930.csv"
D = Decimal


def rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    assert NOTICE.read_bytes()[:4] == b"%PDF"
    contents = "".join("".join((page.extract_text() or "").split()) for page in PdfReader(NOTICE).pages)
    assert "2011年9月29日" in contents
    assert "11010.6961万股无限售流通A股股票" in contents
    assert "277,682,917股" in contents
    assert "429,733,729股" in contents
    raw = {r["date"]: D(r["close_raw"]) for r in rows(RAW)}
    assert raw["2010-04-29"] == D("7.28")
    assert not [date for date in raw if "2010-04-29" < date < "2011-09-29"]
    assert raw["2011-09-29"] == D("12.25")
    bars = {r["date"]: r for r in rows(BARS) if r["symbol"] == "600633"
            and "2010-04-30" <= r["date"] <= "2011-09-29"}
    suspended = {date: r for date, r in bars.items() if date < "2011-09-29"}
    assert suspended and all(r["tradestatus"] == "0" and D(r["volume"]) == 0
                             and D(r["close"]) == D("7.28") for r in suspended.values())
    assert bars["2011-09-29"]["tradestatus"] == "1"
    assert D(bars["2011-09-29"]["volume"]) > 0
    assert D(bars["2011-09-29"]["close"]) == D("12.25")
    assert D(bars["2011-09-29"]["preclose"]) == D("7.28")
    candidate = [r for r in rows(EVENTS) if r["symbol"] == "600633"
                 and "2010-04-30" <= r["ex_date"] <= "2011-09-29"]
    assert not candidate
    out = {
        "status": "conditional_paper_valuation_can_continue_no_old_share_conversion_shown",
        "issuer_notice_url": "https://static.cninfo.com.cn/finalpage/2011-09-23/59989011.PDF",
        "source_sha256": {p.name: sha(p) for p in (NOTICE, RAW, BARS, EVENTS)},
        "old_unrestricted_shares_relisted": "110106961",
        "new_shares_issued_to_controller": "277682917",
        "total_shares_before": "152050812", "total_shares_after": "429733729",
        "last_true_trade": "2010-04-29", "last_true_close": "7.28",
        "suspended_bar_count_observed": len(suspended),
        "relist_date": "2011-09-29", "relist_true_close": "12.25",
        "relist_mark_to_market_return_per_existing_share": str(D("12.25")/D("7.28")-1),
        "old_share_quantity_contract": "one old shareholder share remains one share; issuer reports a private issuance to controller and explicitly resumes 110106961 existing unrestricted shares. No shareholder-level split, exchange or cash distribution shown in the cited notice or local event candidates.",
        "paper_account_treatment": "Keep frozen old shares and last genuine traded 7.28 mark during halt. No trades can fill; at 2011-09-29 verified raw close revalue unchanged q at 12.25. The 2010-04-30 and later zero-volume bars are not new executable closes.",
        "scope": "Tax-before economic valuation only. This does not prove a realizable exit during suspension, a complete corporate-action feed, or a formal strategy NAV.",
    }
    (HERE / "relist_600633_audit.json").write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: out[k] for k in ("status", "suspended_bar_count_observed", "relist_mark_to_market_return_per_existing_share")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
