# Realtime exchange-calendar freshness v1

User authorization (2026-09-04): a long exchange holiday must not by itself block
the first realtime signal after reopening. This supersedes the previous audit's
five-calendar-day restriction for the realtime publication path only.

Pre-registration: use an independently fetched full exchange calendar, with a hard
30-second child-process timeout and explicit coverage. Keep the existing 15:30
close-confirmation boundary. The realtime anchor must exactly equal the latest
completed session; quote day must be a current exchange trading day. A truncated
NAV history cannot serve as the calendar. Missing sessions still fail closed.

Retain historical calculations, costs, momentum parameters, member/ST guards,
100/100 fresh quotes, frozen authority and original formal outputs. No stale-data
override and no trading authorization. Core realtime changes require whole-delivery
refresh/check and fixed-SHA GitHub regression acceptance before deployment claims.
