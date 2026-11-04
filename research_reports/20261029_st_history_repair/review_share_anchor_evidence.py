"""Research adjudication, never source overwrite or official share-cache mutation."""
from pathlib import Path
import hashlib,json
B=Path(__file__).resolve().parent;D=B/'old4975_shares_tail_20261029';E=D/'issuer_evidence'
notes={
'000338':('old_quantity_and_event_date_supported','2026-03-16',None,None,'Wind issuer text confirms cancellation of 1184200 on March16; root preserves later disclosure date separately.'),
'000422':('quantity_supported_issuer_date_conflict',None,'2025-10-21','000422_annual2025','Same issuer annual report states October20 in share-change sections and October21 completion in audited notes. Quantity1088248412 supported; exact effective day unresolved.'),
'000503':('old_quantity_and_precise_processing_date_supported','2025-11-04','2025-11-05','000503_annual2025','Annual report page62 explicitly says November4 CSDC cancellation completed; another passage says November5 completion in disclosure context. Both retained; not silently changing olddate.'),
'000762':('quantity_supported_exact_date_unproven',None,'2025-11-06','000762_cancel2025','354900 cancelled;520819240 resulting shares. Recently completed, signedNovember5/publishedNovember6 is not exact completion proof.'),
'000811':('quantity_supported_exact_date_unproven',None,'2025-11-28',None,'Wind issuer completion notice supports992477985 after160160 cancellation; signedNovember27, exact completion day not stated.'),
'000825':('old_quantity_and_event_date_supported','2025-08-15',None,'000825_annual2025','Issuer annual share-change section explicitly completionAugust15:5706913596 minus10665800 equals5696247796.'),
'000830':('quantity_supported_exact_date_unproven',None,'2025-08-27','000830_completion2025','Completion notice says by disclosure completed; signedAugust26. Quantity1904319011 supported; exact eventdate not established by signing.'),
'000948':('quantity_supported_exact_date_unproven',None,'2025-08-30','000948_annual2025_actual','Annual report confirms3729051 cancellation to389888774 and August30 completion announcement; does not establish August29 actual processing date.'),
'001266':('quantity_supported_exact_date_unproven',None,'2026-01-10','001266_completion2026','Actual notice:103587100 minus6858 equals103580242. Recently completed; January10 is Saturday publication, not proven effective date. EM103587442 differs from actual pre-cancel amount too.'),
'002135':('daily_conversion_timing_unresolved',None,'2026-07-02','002135_q2_conversion2026','Q2 notice confirms March31 total1115521300 and June30 total1115522494; does not resolve March23 vs March24 conversion178 shares.'),
'002317':('quantity_supported_exact_date_unproven',None,'2025-07-31','002317_completion2025','851556477 minus1626000 equals849930477. Auditor validates capital as ofJuly17; CSDC completion only stated completed by announcement, cannot assign July17 or signedJuly30 automatically.'),
'002460':('yearend_quantity_supported_december_day_unresolved',None,'2026-03-31','002460_annual2025','HKEX issuer annual report confirms December31 total2096694404 after H-share conversions; does not prove December10 daily anchor.'),
'002605':('quantity_supported_exact_date_unproven',None,'2026-01-30','002605_completion2026','50000 cancelled, completion statement has no day; signedJanuary29 and January30 publication distinct.417651347 supported in issuer table indexed PDF; downloaded HTML represents table as image.'),
'002646':('quantity_supported_exact_date_unproven',None,'2025-09-03','002646_completion2025','482002974 minus2895000 equals479107974. Recently/by disclosure completed; signedSeptember2, no exact CSDC day.'),
'002678':('quantity_supported_exact_date_unproven',None,'2025-07-02','002678_annual2025','Annual report says July completion and July2 notice;1359808323 minus1488000 equals1358320323. Exact July1 unproven.'),
'002851':('issuance_quantity_supported_intervening_options_unresolved',None,'2026-02-05','002851_issuance2026','Issuance31,325,851; pre-issue base550152641 datedJanuary26, listingFebruary6.581478492 supported at that base; possible16920 option exercise before listing remains unresolved; do not declare vendor wrong.'),
'002869':('old_anchor_proven_wrong_split_events','2025-11-24','2025-11-26','002869_completion2025','Issuer explicitly separates1993350 buyback cancellationNovember21 and648000 restricted cancellationNovember24. November21 total177562991, November24 total176914991; old assigns final quantity too early.'),
}
notes.update({
'603501':('quantity_supported_exact_date_unproven',None,'2026-02-07','603501_placement20260211','Issuer placement table supports1260915524 for February11 listing, but table preparation precedes listing and possible54771 intervening exercise/conversion unverified. Retain legacy date/quantity also supported by current original CNInfo.'),
'603535':('quantity_supported_issuer_date_conflict',None,'2025-08-21','603535_unlock20250826','Issuer unlock notice itself uses510912924 for August26, conflicting with June30 semiannual510918139 after5215 conversion. No issuer cancellation identified; retain legacy pending explicit resolution, do not call exact total independently certified.'),
'605006':('old_anchor_proven_wrong_quantity','2026-01-19','2026-01-22','605006_completion20260119','Issuer completion notice explicitly confirms CSDC January19 cancellation10950200, resulting600017880. Legacy600017790 omits90 shares, must correct candidate at same event day.'),
'688235':('old_quantity_asof_date_supported','2025-06-30','2025-08-30','688235_h1_2025','Issuer semiannual share table and financial note explicitly state ordinary shares1428203304 as ofJune30; supports old quantity, EM1539858562 rejected for this date/caliber.'),
'002135':('old_quantity_asof_date_supported','2026-03-23','2026-03-24','002135_asof20260323','Issuer original March24 notice explicitly states as ofMarch23 total1115520944; original CNInfo current change record agrees. First conversion day not separately claimed.'),
'002460':('quantity_supported_exact_date_unproven',None,'2025-12-10','002460_conversion20251210','Issuer December10 progress notice table supports2096694404 after39501025 H shares, but text says issued or expected to be issued. Retain legacy December10 supported by original CNInfo current records; exact issuance day independently unverified.'),
'002935':('quantity_supported_exact_date_unproven',None,'2025-07-08','002935_completion2025','426522467 minus2419959 equals424102508; completed by publication, signedJuly7. Legacy date retained, exact-day independently unverified.'),
'002989':('old_quantity_asof_date_supported','2026-03-24',None,'002989_asof20260324','Issuer expressly states total201614841 as ofMarch24. This verifies that day quantity, not first conversion effective date.'),
'300145':('quantity_supported_exact_date_unproven',None,'2026-02-04','300145_completion2026','Issuer completion notice:1920900236 minus658200 equals1920242036; recently completed, signedFebruary3. Legacy date retained, exact-day independently unverified.'),
'300253':('old_quantity_asof_date_supported','2026-01-20',None,'300253_asof20260120','Issuer explicitly identifies CSDC January20 share-structure data:2215528951. Proposed future cancellation not confused with actual old shares.'),
'300254':('quantity_supported_exact_date_unproven',None,'2026-03-24','300254_completion2026','256108563 minus50000 equals256058563; completed by disclosure, signedMarch23. Legacy date retained, exact-day independently unverified.'),
'300332':('old_quantity_asof_date_supported','2026-03-18','2026-03-19','300332_asof20260318','March18 first repurchase notice uses current total865159951, confirming old day quantity, not first day of option exercise.'),
'300404':('old_quantity_asof_date_supported','2026-03-24','2026-03-25','300404_completion2026','Issuer March24 repurchase-sale completion table total385863039 before/after; verifies old day quantity, not first option exercise day.'),
'600061':('quantity_supported_exact_date_unproven',None,'2026-03-17','600061_completion2026','March13 repurchase cutoff and resulting table6393985434 supported. Convertible conversion increased total; table does not independently establish first effective conversion day. Retain legacy March13.'),
})
records=[]
for r in json.loads((D/'final_independent_manifest.json').read_text(encoding='utf-8'))['conflicts']:
 s=r['symbol'];item={k:r[k] for k in ['symbol','anchor_date','provider_anchor_date','provider_anchor_shares','old_anchor_shares']}
 if s in notes:
  status,effective,disclosed,name,note=notes[s];item.update(review_status=status,verified_effective_date=effective,disclosure_date=disclosed,assessment=note)
  if name:
   receipt=E/(name+'.json');evidence=json.loads(receipt.read_text(encoding='utf-8'));item.update(source_url=evidence['url'],source_path=str(E/(name+'.bin')),source_sha256=evidence.get('sha256'),body_received=evidence.get('body_received'),text_sha256=hashlib.sha256((E/(name+'.txt')).read_bytes()).hexdigest())
 else:item['review_status']='newly_collected_not_yet_reviewed'
 status=item['review_status']
 item['candidate_disposition']=('correct_two_proven_events' if s=='002869' else 'correct_proven_quantity_same_date' if s=='605006' else 'retain_legacy_pending_specific_conflict' if s in ['000422','002851','603535','603501'] else 'retain_legacy_quantity_and_date')
 item['legacy_date_retained']=s!='002869'
 item['exact_day_independently_unverified']=status in ['quantity_supported_exact_date_unproven','quantity_supported_issuer_date_conflict','daily_conversion_timing_unresolved','yearend_quantity_supported_december_day_unresolved','issuance_quantity_supported_intervening_options_unresolved']
 item.update(formal_approved=False,quantity_and_event_time_are_separate=True);records.append(item)
out=dict(formal_written=False,formal_approved=False,records=records,notes=['000948_annual2025.bin is actually board work report, not annual financial report; not used as share evidence.','Provider conflicts are not resolved by choosing one vendor globally.','A publication/signature date is not an exact registration effective date.','000338/000811 detailed Wind raw-response hashes are in wind_probe/20261030T035335182525Z/review.json.'])
(D/'issuer_anchor_review.json').write_text(json.dumps(out,ensure_ascii=False,indent=2),encoding='utf-8')
print([(r['symbol'],r['review_status']) for r in records])
