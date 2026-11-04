"""One byte-bound non-risk catalog extension; never authorize new ST history."""
from pathlib import Path
from datetime import datetime, timezone
import copy
import hashlib
import json
import re

DIRECTORY = Path(__file__).resolve().parents[1] / "data/st_notice_boundary_reviews/postclose_20261104"
CERTIFICATE_SHA256 = "8a8d39a226af84f78a285649135b080112cd985337d7361181e1c2cf8e1c8af6"
POLICY = "exact_reviewed_postclose_nonrisk_transport_20261104_v2"


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _stable(rows):
    return {key: {k: v for k, v in row.items() if k != "display_time"} for key, row in rows.items()}


def reviewed_extension(previous, current, precision):
    from scripts import st_notice_boundary_review as original
    from scripts.st_notice_evidence import StNoticeEvidenceError, is_equity_issuer
    from scripts import st_history_scope as scope
    import microcap_top100_mom16_biweekly_live_v2_0 as base

    def require(ok, message):
        if not ok:
            raise StNoticeEvidenceError(message)

    path = DIRECTORY / "candidate.json"
    raw = path.read_bytes()
    require(_sha(raw) == CERTIFICATE_SHA256, "Post-close exact review certificate bytes changed")
    cert = json.loads(raw)
    if _stable(current) != _stable(cert["current_rows"]):
        return None
    old_path = original.DIRECTORY / "reviewed_20261104/registry.json"
    old_raw = old_path.read_bytes()
    require(_sha(old_raw) == cert["base_review_registry_sha256"], "Base boundary review changed")
    old_review = json.loads(old_raw)
    if (_stable(previous) != _stable(old_review["previous_rows"])
            or _stable(current) != _stable(cert["current_rows"])):
        return None
    precision(old_review["previous_rows"], previous)
    precision(cert["current_rows"], current)
    extra = cert["extra_added_ids"]
    changed = cert["extra_metadata_changed_ids"]
    require(extra == ["AN202611041831390838"] and len(changed) == 9,
            "Post-close exact review scope changed")
    require(set(current) - set(old_review["current_rows"]) == set(extra)
            and not set(previous) - set(current), "Unreviewed boundary addition or deletion")
    require(set(cert["documents"]) == set(extra + changed), "Review document closure changed")
    for identity, specs in cert["documents"].items():
        blobs = {}
        for kind, suffix in (("pdf", ".pdf"), ("txt", ".txt"), ("receipt.json", ".receipt.json")):
            spec = specs[kind]
            require(spec["path"] == identity + suffix, "Review document path changed")
            data = (DIRECTORY / spec["path"]).read_bytes()
            require(_sha(data) == spec["sha256"], "Review document bytes changed")
            blobs[kind] = data
        receipt = json.loads(blobs["receipt.json"])
        url = "https://pdf.dfcfw.com/pdf/H2_" + identity + "_1.pdf"
        require(receipt["status"] == 200 and receipt["requested_url"] == receipt["final_url"] == url
                and receipt["tls_verify_configuration"] is True and receipt["redirects_allowed"] is False
                and receipt["raw_sha256"] == specs["pdf"]["sha256"]
                and receipt["raw_bytes"] == len(blobs["pdf"])
                and _stable({identity: receipt["native_row"]}) == _stable({identity: current[identity]}),
                "Review document source binding changed")
        start, end = (datetime.fromisoformat(receipt[k]) for k in ("observation_start", "observation_end"))
        require(start.tzinfo is not None and end.tzinfo is not None and start <= end <= datetime.now(timezone.utc),
                "Review document clock invalid")
        text = blobs["txt"].decode("utf-8")
        require(blobs["pdf"].startswith(b"%PDF-") and bool(text.strip())
                and not re.search("退市风险警示|其他风险警示|实施.{0,12}风险警示|撤销.{0,12}风险警示", text),
                "Extra catalog document now contains an unreviewed risk warning")
    normalized = copy.deepcopy(current)
    for identity in changed:
        before = old_review["current_rows"][identity]
        after = current[identity]
        fields = {key for key in before.keys() | after.keys()
                  if key != "display_time" and before.get(key) != after.get(key)}
        require(fields == {"columns"} and after["columns"] == cert["changes"][identity]["columns"]["new"]
                and before["columns"] == cert["changes"][identity]["columns"]["old"],
                "Unreviewed catalog metadata changed")
        for row in (before, after):
            require(base.freq_mod.classify_st_notice_risk_actions(row["title"]) == {},
                    "Catalog update introduced a risk action")
            for code in row["codes"]:
                symbol = code["stock_code"]
                require(not is_equity_issuer(row, symbol)
                        or scope._daily_notice_row(row, symbol, cert["boundary_day"], {}) is None,
                        "Catalog update introduced a risk selector")
        normalized[identity]["columns"] = before["columns"]
    row = current[extra[0]]
    text = re.sub(r"\s+", "", (DIRECTORY / (extra[0] + ".txt")).read_text(encoding="utf-8"))
    require(row["title"] == "新华都:2026年11月3日投资者关系活动记录表"
            and [c["stock_code"] for c in row["codes"]] == ["002264"]
            and "证券代码：002264" in text and "投资者关系活动记录表" in text
            and base.freq_mod.classify_st_notice_risk_actions(row["title"]) == {}
            and (not is_equity_issuer(row, "002264")
                 or scope._daily_notice_row(row, "002264", cert["boundary_day"], {}) is None),
            "Late investor-relations document introduced a risk action")
    del normalized[extra[0]]
    result = original._review_exact_20261104_transport(previous, normalized, precision)
    require(result is not None, "Original exact ST review did not pass")
    all_changed = sorted(set(current) - set(previous) | {k for k in previous if previous[k] != current[k]})
    result = dict(result, policy=POLICY, changed_event_count=len(all_changed),
                  changed_event_ids_sha256=_sha(json.dumps(all_changed, separators=(",", ":")).encode()),
                  reviewed_non_st_added_ids=sorted(result["reviewed_non_st_added_ids"] + extra),
                  reviewed_metadata_changed_ids=sorted(result["reviewed_metadata_changed_ids"] + changed),
                  postclose_exact_extension_sha256=CERTIFICATE_SHA256,
                  risk_projection_unchanged=True)
    return result
