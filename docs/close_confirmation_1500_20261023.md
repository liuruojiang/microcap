# Close-confirmed research from 15:00 Beijing

User explicitly approved this change on 2026-10-23 after the market close. The prior 15:30 buffer prevented today from being selected despite the exchange having closed.

The common/base loader, embedded v2.0 loader and independent exchange calendar now agree on 15:00 Asia/Shanghai. At 14:59:59 the latest completed session remains the preceding actual exchange session. At 15:00 and later, today is eligible only when it is an actual session and its real complete close data is available. A timestamp alone never establishes data completeness.

No strategy parameter, return model, approved historical lineage or ST/100-member/source/hash validation changes. Intraday snapshot/publication deadlines remain strictly before 15:00. v2.0, v2.3 and v2.5 results must explicitly retain close_confirmed and official_close_confirmed_signal=True, with the real signal day and next exchange session. No SMTP or trading permission is added.
