# PII detection: LogGate vs regex-only baselines

Dataset `pii.jsonl` (sha256 c20aa3fce0dd…), birth-date bound 2026-10-17, go1.27.0.

- **loggate**: hand-written digit scanner with strict boundaries + ID MOD 11-2/date/province, card BIN+Luhn, phone prefix table, e-mail RE2 when '@' is present.
- **regex_naive**: `\d{17}[\dXx]`, `\d{16,19}`, `1[3-9]\d{9}`, e-mail regex; no boundaries, no validation.
- **regex_bounded**: the same patterns wrapped in `\b`.

Both baselines resolve overlaps with the engine's priority (id_card > bank_card > phone > email, longer first).

## Dataset

10000 records, 2176945 bytes. Templates: ticket 2560; app_json 2522; nginx 2475; sshd 2443; 

Scored positives: id_card 3046; bank_card 3691; phone 6393; email 3417; 

Hard negatives: order_id 6584; trace_id 6487; version 5771; ms_timestamp 4057; snowflake_luhn_pass_no_bin 3272; id_bad_date 738; phone_bad_prefix 717; id_bad_checksum 687; card_luhn_fail 682; random_numeric_id 678; id_bad_province 669; key_fingerprint 601; 

Known-limitation spans (excluded from P/R/F1): urlencoded_at 418; fullwidth_digits 263; adjacent_alnum 107; 

## Span-level exact match

| system | type | P | R | F1 | TP | FP | FN |
|---|---|---|---|---|---|---|---|
| loggate | id_card | 1.0000 | 1.0000 | 1.0000 | 3046 | 0 | 0 |
| loggate | bank_card | 0.9780 | 1.0000 | 0.9889 | 3691 | 83 | 0 |
| loggate | phone | 1.0000 | 1.0000 | 1.0000 | 6393 | 0 | 0 |
| loggate | email | 1.0000 | 1.0000 | 1.0000 | 3417 | 0 | 0 |
| loggate | **micro** | 0.9950 | 1.0000 | 0.9975 | 16547 | 83 | 0 |
| regex_naive | id_card | 0.2013 | 0.9294 | 0.3309 | 2831 | 11234 | 215 |
| regex_naive | bank_card | 0.3633 | 0.4736 | 0.4112 | 1748 | 3063 | 1943 |
| regex_naive | phone | 0.5862 | 1.0000 | 0.7392 | 6393 | 4512 | 0 |
| regex_naive | email | 1.0000 | 1.0000 | 1.0000 | 3417 | 0 | 0 |
| regex_naive | **micro** | 0.4334 | 0.8696 | 0.5785 | 14389 | 18809 | 2158 |
| regex_bounded | id_card | 0.4681 | 0.8004 | 0.5907 | 2438 | 2770 | 608 |
| regex_bounded | bank_card | 0.3345 | 0.6760 | 0.4475 | 2495 | 4965 | 1196 |
| regex_bounded | phone | 0.9364 | 0.9032 | 0.9195 | 5774 | 392 | 619 |
| regex_bounded | email | 1.0000 | 1.0000 | 1.0000 | 3417 | 0 | 0 |
| regex_bounded | **micro** | 0.6348 | 0.8536 | 0.7281 | 14124 | 8127 | 2423 |

## False positives by cause

`inside_<type>` = the prediction overlaps a gold span of another type; other causes are the hard-negative kind it overlaps; `unlabelled_text` = background text (IPs, ports, header fields).

| system | predicted type | cause | n |
|---|---|---|---|
| loggate | bank_card | id_bad_date | 28 |
| loggate | bank_card | id_bad_checksum | 26 |
| loggate | bank_card | id_bad_province | 17 |
| loggate | bank_card | random_numeric_id | 12 |
| regex_naive | id_card | order_id | 4350 |
| regex_naive | id_card | snowflake_luhn_pass_no_bin | 3272 |
| regex_naive | id_card | inside_bank_card | 747 |
| regex_naive | id_card | id_bad_date | 738 |
| regex_naive | id_card | id_bad_checksum | 687 |
| regex_naive | id_card | id_bad_province | 669 |
| regex_naive | id_card | random_numeric_id | 342 |
| regex_naive | id_card | boundary_mismatch | 215 |
| regex_naive | id_card | card_luhn_fail | 210 |
| regex_naive | id_card | trace_id | 3 |
| regex_naive | id_card | key_fingerprint | 1 |
| regex_naive | bank_card | order_id | 2234 |
| regex_naive | bank_card | card_luhn_fail | 472 |
| regex_naive | bank_card | random_numeric_id | 336 |
| regex_naive | bank_card | trace_id | 19 |
| regex_naive | bank_card | key_fingerprint | 2 |
| regex_naive | phone | ms_timestamp | 4057 |
| regex_naive | phone | phone_bad_prefix | 403 |
| regex_naive | phone | trace_id | 45 |
| regex_naive | phone | key_fingerprint | 7 |
| regex_bounded | id_card | id_bad_date | 712 |
| regex_bounded | id_card | id_bad_checksum | 664 |
| regex_bounded | id_card | id_bad_province | 655 |
| regex_bounded | id_card | order_id | 572 |
| regex_bounded | id_card | random_numeric_id | 163 |
| regex_bounded | id_card | card_luhn_fail | 4 |
| regex_bounded | bank_card | snowflake_luhn_pass_no_bin | 3249 |
| regex_bounded | bank_card | card_luhn_fail | 666 |
| regex_bounded | bank_card | order_id | 552 |
| regex_bounded | bank_card | random_numeric_id | 498 |
| regex_bounded | phone | phone_bad_prefix | 392 |

## False negatives by surface variant

| system | type | variant | n |
|---|---|---|---|
| regex_naive | id_card | url_escaped | 215 |
| regex_naive | bank_card | grouped_space | 918 |
| regex_naive | bank_card | plain | 747 |
| regex_naive | bank_card | grouped_hyphen | 278 |
| regex_bounded | id_card | url_escaped | 608 |
| regex_bounded | bank_card | grouped_space | 918 |
| regex_bounded | bank_card | grouped_hyphen | 278 |
| regex_bounded | phone | plus86 | 619 |

## Known limitations (not scored)

| system | limitation | found / total |
|---|---|---|
| loggate | adjacent_alnum | 0 / 107 |
| loggate | fullwidth_digits | 0 / 263 |
| loggate | urlencoded_at | 0 / 418 |
| regex_naive | adjacent_alnum | 107 / 107 |
| regex_naive | fullwidth_digits | 0 / 263 |
| regex_naive | urlencoded_at | 0 / 418 |
| regex_bounded | adjacent_alnum | 0 / 107 |
| regex_bounded | fullwidth_digits | 0 / 263 |
| regex_bounded | urlencoded_at | 0 / 418 |

## Residual PII after LogGate masking

- scored positives: 16547
- still present verbatim after `MaskString` on the whole line: 0
- app_json records also run through `MaskEvent` (decoded JSON tree): 2522; positives still present after re-encoding: 0
- values replaced with `[REDACTED:error]`: 0

## Sample errors (synthetic values)

### loggate

| kind | type | cause/variant | context |
|---|---|---|---|
| FP | bank_card | id_bad_date | `l.example.com","id_no":"⟦531125190002299710⟧"},"card":"6270-0510-703` |
| FP | bank_card | id_bad_province | ` 7634 9099 2186","ref":"⟦471507196010084488⟧","msg":"verify ok","ver` |
| FP | bank_card | id_bad_date | `28eb9dacf3e78276c0e&uid=⟦430605199212326340⟧ HTTP/1.1" 200 452 "-" "` |
| FP | bank_card | id_bad_date | `版本 v2.20.112, device ⟦621820198704318271⟧.` |
| FP | bank_card | id_bad_date | `版本 v1.12.99, device ⟦532030198800007922⟧.` |
| FP | bank_card | id_bad_date | `018195905128467","ref":"⟦230723203001019164⟧","msg":"verify ok","ver` |
| FP | bank_card | id_bad_checksum | `本 v2.4.125，设备号 ⟦232012199305219847⟧。` |
| FP | bank_card | random_numeric_id | `00c9f2b97b95aa4da56&uid=⟦495986943581802023⟧ HTTP/1.1" 404 13747 "-"` |

### regex_naive

| kind | type | cause/variant | context |
|---|---|---|---|
| FP | bank_card | order_id | `00] "GET /api/orders/ORD⟦6215420242276265⟧?trace=3b71f45afce2af9d2` |
| FP | bank_card | random_numeric_id | `f9d25b9590efa5d0716&uid=⟦84718634142077616⟧ HTTP/1.1" 200 6950 "-" ` |
| FP | phone | ms_timestamp | `{"ts":⟦17170973505⟧35,"level":"WARN","svc":` |
| FP | id_card | order_id | `784219a296","order_id":"⟦202612123619526978⟧036","snowflake":9190639` |
| FP | id_card | snowflake_luhn_pass_no_bin | `9526978036","snowflake":⟦919063979865379084⟧8,"user":{"mobile":"1491` |
| FP | bank_card | order_id | `rp.example"},"card":"ORD⟦9911967654330981⟧","ref":"8461f405e692d78` |
| FP | bank_card | order_id | `工单#ORD⟦3717985317002279⟧：客户反馈无法登` |
| FP | phone | phone_bad_prefix | `本 v3.22.166，设备号 ⟦16809622223⟧.` |
| FN | bank_card | plain | `r cannot log in; 卡号:⟦5311123416111770547⟧。tel １８８５０７` |
| FN | bank_card | grouped_space | `17489【投诉】卡号:⟦6232 7596 1068 1155⟧], tel 17705193040 App 版` |
| FN | bank_card | grouped_space | `法登录，银行卡（⟦6239 9727 7753 1997⟧），tel 15214317030, m` |
| FN | bank_card | grouped_space | `628199605276120，card [⟦6221 3908 3605 9159 591⟧。tel 19580633004，邮` |
| FN | bank_card | plain | `not log in; 银行卡（⟦2720683862078761366⟧），手机19252461589 ` |
| FN | id_card | url_escaped | `00] "GET /search?q=id%20⟦311113197211244551⟧&card=620764212875817557` |
| FN | bank_card | plain | `311113197211244551&card=⟦6207642128758175572⟧&email=sun.jie%40dev.cor` |
| FN | bank_card | grouped_space | `7197806268443"},"card":"⟦6287 0106 0450 5206⟧","ref":"v3.5.159","msg"` |

### regex_bounded

| kind | type | cause/variant | context |
|---|---|---|---|
| FP | bank_card | random_numeric_id | `f9d25b9590efa5d0716&uid=⟦84718634142077616⟧ HTTP/1.1" 200 6950 "-" ` |
| FP | bank_card | snowflake_luhn_pass_no_bin | `9526978036","snowflake":⟦9190639798653790848⟧,"user":{"mobile":"14911` |
| FP | phone | phone_bad_prefix | `本 v3.22.166，设备号 ⟦16809622223⟧.` |
| FP | phone | phone_bad_prefix | `版本 v3.16.136, device ⟦16884805711⟧` |
| FP | phone | phone_bad_prefix | `版本 v4.25.138 设备ID=⟦16841648599⟧.` |
| FP | bank_card | order_id | `工单#⟦2026042255452817489⟧【投诉】卡号:6232 ` |
| FP | bank_card | order_id | `9b96f2c376","order_id":"⟦2026091569996546371⟧","snowflake":8710880696` |
| FP | bank_card | snowflake_luhn_pass_no_bin | `9996546371","snowflake":⟦8710880696427124728⟧,"user":{"mobile":"+8613` |
| FN | id_card | url_escaped | `00] "GET /search?q=id%3D⟦420814195612049482⟧&card=6254733044368864&e` |
| FN | bank_card | grouped_space | `17489【投诉】卡号:⟦6232 7596 1068 1155⟧], tel 17705193040 App 版` |
| FN | phone | plus86 | `28,"user":{"mobile":"+86⟦13082514526⟧","email":"zhao_yan@exam` |
| FN | bank_card | grouped_space | `法登录，银行卡（⟦6239 9727 7753 1997⟧），tel 15214317030, m` |
| FN | bank_card | grouped_space | `628199605276120，card [⟦6221 3908 3605 9159 591⟧。tel 19580633004，邮` |
| FN | id_card | url_escaped | `00] "GET /search?q=id%20⟦311113197211244551⟧&card=620764212875817557` |
| FN | bank_card | grouped_space | `7197806268443"},"card":"⟦6287 0106 0450 5206⟧","ref":"v3.5.159","msg"` |
| FN | bank_card | grouped_space | `1196203211284"},"card":"⟦6280 1545 0604 7771⟧","ref":"831119197005297` |

## Notes

- Synthetic data only; the synthetic distribution is not a real-world log distribution.
- Exact match: start, end and type must all agree. Phone spans are the 11 national digits (a +86/86 prefix is context).
- Known-limitation spans (full-width digits, URL-encoded '@', digits glued to ASCII letters) are excluded from P/R/F1 and listed separately.
- Labels are technical detections, not legal compliance determinations.
