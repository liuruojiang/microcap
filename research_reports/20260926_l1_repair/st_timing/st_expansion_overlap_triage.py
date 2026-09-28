"""Rank title-led possible ST windows by old target overlap; never certify from titles."""
from pathlib import Path
import pandas as pd

out = Path(__file__).resolve().parent / 'expansion_top30'
root = Path(__file__).resolve().parents[3]
notices = pd.read_csv(out/'unresolved_state_notice_review.csv', dtype={'symbol': str, 'announcementId': str})
targets = pd.read_csv(root/'outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv', dtype={'symbol': str})
rows = []
for symbol, group in notices.groupby('symbol'):
    g = group.sort_values('notice_date')
    entries = g[g.title_class.eq('entry')]
    for entry in entries.itertuples(index=False):
        next_exit = g[(g.notice_date > entry.notice_date) & g.title_class.eq('needs_review')]
        end = next_exit.iloc[0].notice_date if len(next_exit) else '9999-12-31'
        overlap = targets[(targets.symbol.eq(symbol)) & (targets.rebalance_date >= entry.notice_date) & (targets.rebalance_date < end)]
        rows.append(dict(symbol=symbol, entry_notice_date=entry.notice_date, entry_id=entry.announcementId,
                         next_exit_title_date=end, provisional_overlap_rows=len(overlap),
                         first_target_date=overlap.rebalance_date.min() if len(overlap) else '',
                         last_target_date=overlap.rebalance_date.max() if len(overlap) else ''))
df = pd.DataFrame(rows).sort_values(['provisional_overlap_rows', 'symbol'], ascending=[False, True])
df.to_csv(out/'title_overlap_triage_only.csv', index=False, encoding='utf-8-sig')
print(df.head(30).to_string(index=False))
