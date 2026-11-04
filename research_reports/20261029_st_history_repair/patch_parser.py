from pathlib import Path
import ast

ROOT = Path(__file__).resolve().parents[2]
REVISION = 'category-state-body-evidence-20261029-v1'

def replace_once(text, old, new):
    assert text.count(old) == 1, (old[:100], text.count(old))
    return text.replace(old, new, 1)

def transform(source, embedded):
    source = replace_once(source, 'ST_NOTICE_POLICY_VERSION = "cninfo-category-plus-entry-exit-keyword-v3"',
        'ST_NOTICE_POLICY_VERSION = "cninfo-category-plus-entry-exit-keyword-v3"\nST_NOTICE_PARSER_REVISION = "'+REVISION+'"')
    source = replace_once(source, '    actions: dict[str, bool] = {}\n    had_effective_withdrawal = False',
        '''    if any(token in text for token in ("将被", "将于", "问询函", "子公司", "参股公司")):
        from scripts.st_notice_evidence import StNoticeEvidenceError
        raise StNoticeEvidenceError("ST title requires verified issuer/body evidence: " + text)
    actions: dict[str, bool] = {}
    had_effective_withdrawal = False''')
    tree=ast.parse(source)
    fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='fetch_cninfo_st_notices')
    lines=source.splitlines(keepends=True); old=''.join(lines[fn.lineno-1:fn.end_lineno]); new=old
    token = '"title": title}' if embedded else '"title": title,'
    replacement = '''"title": title,
                        "announcement_id": str(item.get("announcementId") or ""),
                        "issuer_symbol": str(item.get("secCode") or symbol).zfill(6),
                        "source_url": ("https://static.cninfo.com.cn/" + str(item.get("adjunctUrl") or "").lstrip("/")),'''
    new=replace_once(new, token, replacement + ('}' if embedded else ''))
    new=new.replace('.drop_duplicates(subset=["notice_date", "title"])', '.drop_duplicates()')
    new=new.replace('columns=["notice_date", "title"]', 'columns=["notice_date", "title", "announcement_id", "issuer_symbol", "source_url"]')
    source=replace_once(source,old,new)
    source=replace_once(source, '''def build_st_intervals_from_notices(
    first_trade_date: pd.Timestamp,
    last_trade_date: pd.Timestamp,
    notices: pd.DataFrame,
)''', '''def build_st_intervals_from_notices(
    first_trade_date: pd.Timestamp,
    last_trade_date: pd.Timestamp,
    notices: pd.DataFrame,
    *, verified_events: dict[str, dict] | None = None,
)''')
    source=replace_once(source, '''            actions, had_withdrawal = _parse_st_notice_risk_transition(getattr(row, "title", ""))''', '''            verified = (verified_events or {}).get(str(getattr(row, "announcement_id", "")))
            if verified is not None:
                if verified["notice_date"] != str(notice_date.date()):
                    from scripts.st_notice_evidence import StNoticeEvidenceError
                    raise StNoticeEvidenceError("Verified ST event date mismatch")
                actions = verified["risk_actions"]
                had_withdrawal = any(value is False for value in actions.values())
            else:
                actions, had_withdrawal = _parse_st_notice_risk_transition(getattr(row, "title", ""))''')
    source=replace_once(source, '''    try:
        notices = fetch_cninfo_st_notices(''', '''    from scripts.st_notice_evidence import StNoticeEvidenceError, load_verified_events, evidence_fingerprint
    try:
        notices = fetch_cninfo_st_notices(''')
    source=replace_once(source, '''        notice_intervals = build_st_intervals_from_notices(first_trade, last_trade, notices)
        notice_query_status = "ok"
    except Exception as exc:''', '''        verified_events = load_verified_events(ROOT, str(symbol).zfill(6), first_trade, last_trade, notices)
        notice_intervals = build_st_intervals_from_notices(first_trade, last_trade, notices, verified_events=verified_events)
        notice_query_status = "ok"
    except StNoticeEvidenceError:
        raise
    except Exception as exc:''')
    source=replace_once(source, '''        "st_notice_policy_version": ST_NOTICE_POLICY_VERSION,
        "symbol":''', '''        "st_notice_policy_version": ST_NOTICE_POLICY_VERSION,
        "st_notice_parser_revision": ST_NOTICE_PARSER_REVISION,
        "st_notice_evidence_sha256": evidence_fingerprint(ROOT),
        "symbol":''')
    if embedded:
        source=replace_once(source, '''            payload = _read_security_meta_payload(meta_path, symbol)
            identity_matches''', '''            payload = _read_security_meta_payload(meta_path, symbol)
            if payload.get("st_intervals") or current_st_name:
                from scripts.st_notice_evidence import evidence_fingerprint
                if (payload.get("st_notice_parser_revision") != ST_NOTICE_PARSER_REVISION
                        or payload.get("st_notice_evidence_sha256") != evidence_fingerprint(ROOT)):
                    raise SecurityMetaHistoryCorrectionRequired(
                        f"Security metadata parser provenance is unverified for {code}; "
                        "an exact-hash lineage review is required before reuse")
            identity_matches''')
    return source

standalone=ROOT/'analyze_top100_rebalance_frequency.py'
backup=ROOT/'.codex_backups/20261029_st_history_repair'
standalone.write_text(transform((backup/standalone.name).read_text(encoding='utf-8-sig'),False),encoding='utf-8')
p=ROOT/'microcap_top100_mom16_biweekly_live_v2_0.py';s=(backup/p.name).read_text(encoding='utf-8-sig');tree=ast.parse(s)
n=next(n for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(x,ast.Name) and x.id=='FREQ_SOURCE' for x in n.targets))
lines=s.splitlines(keepends=True);new=transform(ast.literal_eval(n.value),True)
p.write_text(''.join(lines[:n.lineno-1])+"FREQ_SOURCE = r'''"+new+"'''\n"+''.join(lines[n.end_lineno:]),encoding='utf-8')
print('Patched standalone and real shared embedded parser; formal metadata untouched')
