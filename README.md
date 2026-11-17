Perfect Game CDP, Identity Graph & Meta CAPI Engine
> **AI & Agent Context Prompt:** This repository powers the centralized Customer Data Platform (CDP), real-time identity resolution graph, demographic enrichment waterfall, and server-side Meta Conversions API (CAPI) engine for **Perfect Game** (`35000 Grand River Ave, Farmington Hills, MI 48335`, Coordinates: `42.4705, -83.3932`). Read this specification carefully before modifying database schemas, scoring logic, geographic exclusion rules, or webhook handlers.
---
1. Executive Summary & Core Objectives
Perfect Game operates a multi-attraction family entertainment center (bowling, laser tag, arcade, bar/kitchen, kids' birthday parties, and corporate events). Customer transactions and inquiries originate across four disconnected platforms:
Square POS: In-store card swipes, food/beverage tabs, walk-in bowling, and deposits/payments for events.
WebRes (BowlNow): Online lane reservations.
Tripleseat: Corporate events, banquets, and large group inquiries/bookings.
Partywirks: Kids' and teen birthday party package bookings (`BIRTHDAY EXTRAVAGANZA`, `BOWLING PARTY`, `TEEN PARTY`).
What This System Solves
Identity Resolution & Deduplication: Stitches fragmented customer records across Square, WebRes, Tripleseat, and Partywirks into a single golden household record (`master_customers.cluster_id`) without double-counting revenue when online bookings or Partywirks/Tripleseat deposits settle through Square POS.
Real-Time Progressive Enrichment: Automatically enriches incoming leads and POS customers via Melissa Personator API (verifying physical street address, latitude/longitude, distance from the venue, household income, and presence of children) and Versium REACH API (appending up to 5 alternate emails/phones per person, net worth, home market value, and B2B corporate employment).
Hard Geographic Ad Exclusion Gate: Strictly blocks Meta CAPI optimization events (`Qualified_Bowler`, `Purchase`) and Custom Audience syncs for excluded geographic areas (Detroit, Southfield, Redford, Pontiac, Oak Park, Inkster, etc.) so Meta's ad delivery algorithm never learns to prospect in non-target areas.
Algorithmic Lead Scoring & Thermostat Pacing: Assigns every household to a Perfect Game Mosaic Persona (`PG-1` through `PG-6`) and calculates a `bowler_value_score` (`0–100`). A nightly thermostat (`cdp_thermostat_config`) dynamically adjusts the minimum score threshold (default `70`) to maintain ~50 `Qualified_Bowler` conversion events per 7-day rolling window—keeping Meta Ad Sets out of the "Learning Limited" phase while optimizing purely for high-value suburban families and corporate event planners.
---
2. Infrastructure & Tech Stack
Application Server: FastAPI (`main.py`) hosted on Render (`https://pg-cdp-webhook.onrender.com`)
Database: Serverless PostgreSQL hosted on Neon (`customer_database`)
Enrichment Providers:
Melissa Data Personator API (`/v3/WEB/ContactVerify/doContactVerify`): Address verification, geocoding (`GrpGeo`), household demographics (`GrpDemographicBasic`), and persistent identity keys (`MelissaIdentityKey` / `md_mik` and `MelissaAddressKey` / `md_mak`).
Versium REACH API (`/v2/contact`, `/v2/demographic`): Multi-email/phone identity graph (`email_1..5`, `phone_1..5`), wealth/home value signals, and B2B company/title matching.
Ad Destination: Meta Graph API `v21.0` (Server-Side Conversions API + Real-Time Custom Audience API).
Required Environment Variables (Render Dashboard)
Variable	Purpose
`DATABASE_URL`	PostgreSQL connection string for Neon (`customer_database`)
`SQUARE_ACCESS_TOKEN`	Production Square API token (used for Customer Profile lookups & Catalog sync)
`MELISSA_API_KEY`	License key for Melissa Personator Consumer Verification & Append
`VERSIUM_API_KEY`	API key for Versium REACH Contact, Demographic & B2B Append
`META_PIXEL_ID`	Meta Dataset / Pixel ID (`Perfect Game Facebook Data`)
`META_ACCESS_TOKEN`	Meta System User Access Token with CAPI & Custom Audience permissions
`AUDIENCE_PG1_CORP`	Meta Custom Audience ID (`120246555016470358`) - Corporate & High-LTV
`AUDIENCE_PG2_FAMILIES`	Meta Custom Audience ID (`120246555006040358`) - Affluent Suburban Party Families
`AUDIENCE_PRIME_SUBURBS`	Meta Custom Audience ID (`120246555019320358`) - Prime Western/Northern Suburbs
`AUDIENCE_ALL_NONDETROIT`	Meta Custom Audience ID (`120246555027130358`) - All Qualified Non-Excluded Customers
`SQUARE_WEBHOOK_SIGNATURE_KEY`	Signature key of the Square webhook subscription. When set, `/webhooks/square` rejects requests without a valid `x-square-hmacsha256-signature`.
`SQUARE_WEBHOOK_URL`	The exact notification URL registered with Square (default `https://pg-cdp-webhook.onrender.com/webhooks/square`). Square signs this URL + the body.
`WEBHOOK_SECRET`	Shared key for `/webhooks/webres`, `/webhooks/tripleseat`, `/webhooks/partywirks`. Senders pass it as header `X-CDP-Key` or query `?key=`.
`CRON_SECRET`	Separate key for every `/cron/*` endpoint (header `X-CDP-Key` or query `?key=`).
`PARTYWIRKS_WEBHOOK_SECRET`	Partywirks webhook signature key (HMAC-SHA256 in `X-Partywirks-Signature`). When set, every delivery is checked and the result logged.
`PARTYWIRKS_SIGNATURE_ENFORCE`	Set to `true` to reject (401) deliveries whose signature doesn't match. Only turn on after the logs show `Partywirks signature verified` for a real booking.
`PARTYWIRKS_API_KEY`	Partywirks REST API key (`reservations` and `customers` scopes). When set, every Partywirks webhook is verified and completed by reading the reservation back from the API.
Each authentication check switches on as soon as its variable is set. `GET /health` shows which are on.
---
3. PostgreSQL Database Schema (Neon)
3.1 `master_customers` (Golden Household Record)
One row per resolved customer/household (`cluster_id` primary key).
Identity & Contact: `cluster_id`, `first_name`, `last_name`, `full_name`, `email`, `phone`, `sq_id`, `source`, `updated_at`
Source Flags: `in_square` (`0/1`), `in_webres` (`0/1`), `in_tripleseat` (`0/1`), `in_partywirks` (`0/1`)
Revenue & Activity (De-duplicated):
`sq_spend`: Cumulative dollar spend processed through Square POS.
`wr_spend`: Cumulative dollar spend booked online via WebRes.
`true_lifetime_spend`: `GREATEST(sq_spend, wr_spend)` — prevents double-counting when WebRes/Partywirks/Tripleseat payments clear through Square.
`webres_online_spend`: Online lane reservation spend.
`instore_pos_spend`: `GREATEST(0, sq_spend - wr_spend)` — incremental in-center F&B, arcade, and walk-in spend.
`sq_visits`, `wr_bookings`, `ts_events`, `pw_bookings`: Activity counts by channel.
Tripleseat Event Tracking: `ts_account` (company name), `ts_lead_status` (`PROSPECT`, `TENTATIVE`, `DEFINITE`, `CLOSED`, `LOST`, `TURNED_DOWN`), `ts_max_guests`, `ts_last_event_date`, `ts_turned_down_reason`
Partywirks Birthday Tracking: `pw_bookings`, `pw_last_package` (`BIRTHDAY EXTRAVAGANZA`, `BOWLING PARTY`, `TEEN PARTY`), `pw_last_party_date` (`DATE`)
Melissa Demographic & Geo Columns: `md_address1`, `md_city`, `md_state`, `zip5`, `md_plus4`, `md_carrier_route`, `md_latitude`, `md_longitude`, `distance_miles` (Haversine distance from `42.4705, -83.3932`), `md_household_income`, `md_presence_of_children`, `md_children_age_range`, `md_own_rent`, `md_marital_status`, `md_household_size`, `md_length_of_residence`, `md_education`, `md_occupation`, `md_date_of_birth`, `md_gender`, `md_delivery_indicator` (`B` = Business, `R` = Residential), `md_mik` (Melissa Identity Key), `md_mak` (Melissa Address Key), `md_results`
Versium REACH Columns: `versium_net_worth`, `versium_home_market_value`, `versium_auto_make_model`, `versium_b2b_company`, `versium_alt_emails` (`JSONB` array), `versium_alt_phones` (`JSONB` array), `versium_lifestyle_flags` (`JSONB`)
Segmentation & Scoring: `pg_mosaic_persona` (`TEXT`), `bowler_value_score` (`INT` `0–100`)
3.2 `customer_identities` (Cross-Channel Identity Graph)
Maps every known identifier (`email`, `phone`, `sq_id`, `md_mik`) to a `cluster_id`.
Primary/Unique constraint on `(id_type, id_value)`.
Populated with primary contact info plus up to 5 alternate emails and 5 alternate phone numbers discovered by Versium REACH.
Enables instant stitching when a customer books online with a personal email (`yahoo.com`) and later swipes a credit card at Square POS tied to a work email or spouse's phone number.
3.3 `square_catalog_items` (POS Price Key & Department Dictionary)
Synced automatically from Square Catalog API (`657` variations across `108` categories).
Columns: `variation_id` (`PK`), `item_id`, `item_name`, `variation_name`, `category_id`, `category_name`, `price_dollars`, `sku`, `pg_department`, `is_deleted`, `updated_at`
Automated Department Classification (`pg_department`):
`Parties & Events`
`Bowling & Lanes`
`Laser Tag & Arcade`
`Bar & Alcohol`
`Food & Kitchen`
`Shoes & Socks`
`General / Other`
3.4 `capi_events_log` & `cdp_thermostat_config`
`capi_events_log`: Audit trail of every `Purchase` and `Qualified_Bowler` event sent to Meta CAPI, storing `event_id` (for deduplication), `cluster_id`, `event_name`, `event_value`, `bowler_value_score`, `threshold_at_send`, `meta_response_status`, and `meta_response_body`.
`cdp_thermostat_config`: Single-row configuration table (`id = 1`) holding `current_score_threshold` (default `70`), `target_weekly_events` (`50`), and `last_adjusted_at`.
---
4. Geographic Rules, Personas & Scoring Model
4.1 Hard Geographic Exclusion Gate (`is_detroit_excluded`)
To prevent Meta's ad delivery AI from optimizing toward lower-converting or out-of-market areas, any non-corporate customer matching the following verified cities or ZIP codes is hard-capped at a `bowler_value_score` of `25`, assigned to `PG-5: Excluded Geo`, and completely blocked from firing Meta CAPI events or entering Meta Custom Audiences:
Excluded Cities: `detroit`, `pontiac`, `redford`, `redford charter township`, `redford twp`, `inkster`, `highland park`, `hamtramck`, `southfield`, `lathrup village`, `oak park`, `ferndale`, `hazel park`, `eastpointe`, `harper woods`, `ecorse`, `river rouge`
Excluded ZIP Prefixes / Codes: All `482xx` ZIP codes except the Grosse Pointes (`48230`, `48236`, see `ALLOWED_482_ZIPS`), plus `48033`, `48034`, `48037`, `48075`, `48076`, `48340`, `48341`, `48342`, `48141`
Grosse Pointe: `48230` (Grosse Pointe, Farms, Park) and `48236` (Woods, Shores) are marketable. The city check runs first, so a `48236` address in Harper Woods or Detroit is still excluded.
Important Guardrail: Unmatched cell-phone area code cities (where `md_address1 IS NULL`) are stripped during enrichment so a Novi resident with a `313` or `248` cell phone number is never falsely flagged as Detroit or Southfield unless a verified street address confirms it.
4.2 Prime Suburban Target ZIPs (`PRIME_SUBURB_ZIPS`)
Customers in these core western/northern Oakland and Wayne County suburbs are synced to the `AUDIENCE_PRIME_SUBURBS` (`120246555019320358`) seed list:
Farmington / Farmington Hills: `48331`, `48334`, `48335`, `48336`
Novi: `48374`, `48375`, `48377`
Northville: `48167`, `48168`
Livonia: `48150`, `48152`, `48154`
West Bloomfield: `48322`, `48323`, `48324`
Bloomfield Hills / Birmingham / Franklin: `48301`, `48302`, `48304`, `48009`, `48025`
Plymouth / Canton: `48170`, `48187`, `48188`
Commerce Twp / Walled Lake / Wixom / South Lyon: `48382`, `48390`, `48393`, `48178`
4.3 Mosaic Personas (`pg_mosaic_persona`)
Evaluated in priority order inside `score_and_assign_persona(cust)`:
`PG-1: Corporate & Org Event Planners`: `ts_account` present, `versium_b2b_company` present, `md_delivery_indicator == 'B'`, or `ts_max_guests >= 25`.
`PG-5: Excluded Geo (Detroit / Southfield / Downriver)`: Matches `is_detroit_excluded(md_city, zip5)` and is not `PG-1`. Hard-capped at score `25`.
`PG-2: Affluent Suburban Party Families`: Verified children (`md_presence_of_children` or `in_partywirks == 1`) AND `$100,000+` household income.
`PG-3: Core Suburban Family Bowlers`: Verified children (`md_presence_of_children` or `in_partywirks == 1`) with `<$100k` or unknown income.
`PG-4: Adult Social, Date Night & League`: Verified street address (`md_address1`), no children on file.
`PG-6: Digital-Only (Need Versium Append)`: Email/phone only (`md_address1 IS NULL`); awaiting Versium REACH address append.
4.4 Bowler Value Score Calculation (`0–100`)
(Note: Subject to refinement once Versium REACH net worth, home value, and B2B columns are batch-merged.)
Base Score: `30` points
Persona / Family Bonus:
`PG-1: Corporate & Org Event Planners`: `+35` points
`PG-2: Affluent Suburban Party Families`: `+25` points
`PG-3: Core Suburban Family Bowlers` (`has_kids`): `+15` points
Household Income Bonus:
High Income (`$100,000+`): `+15` points
Mid Income (`$60,000–$99,999`): `+10` points
Proximity Bonus (`<= 12.0` Haversine miles from alley): `+15` points (includes Farmington Hills, Novi, Northville, Livonia, West Bloomfield, Walled Lake, Commerce Twp, Wixom, Plymouth).
High Lifetime Spend Bonus (`true_lifetime_spend >= $400`): `+15` points
Cap: `min(100, score)` (or locked at `25` if `is_detroit_excluded` and not `PG-1`).
---
5. Live API Endpoints (`main.py`)
5.1 Real-Time Ingestion Webhooks
`POST /webhooks/square`
Handles `payment.updated` (`COMPLETED`), `customer.created`, `customer.updated`, and `catalog.version.updated`.
When a POS payment only contains `customer_id`, calls Square's `/v2/customers/{id}` API to fetch full name, email, phone, and address.
Stitches customer, enriches via Melissa/Versium if `md_address1` is missing, and fires a server-side `Purchase` CAPI event (with actual dollar spend) if not in an excluded geography.
When `catalog.version.updated` is received, queues `run_square_catalog_sync()` in the background so POS item/price changes update in Neon automatically.
`POST /webhooks/webres`
Ingests online lane bookings. Fires `Purchase` (actual booking amount) AND, if `bowler_value_score >= current_score_threshold`, fires `Qualified_Bowler` with synthetic LTV `max(amount, $250.00)`.
`POST /webhooks/tripleseat`
Unwraps native Tripleseat payloads (`lead`, `event`, `booking`, `contact`). Extracts company name, guest count, event date, and lead status.
If not `LOST`/`TURNED_DOWN`, not in an excluded geo, and `bowler_value_score >= threshold`, fires `Qualified_Bowler` with synthetic value `$1,000.00` (`PG-1` Corporate) or `$450.00` (Social/Birthday).
`POST /webhooks/partywirks`
Ingests real-time Partywirks birthday bookings, extracts package name and party date, marks `in_partywirks = 1` and `md_presence_of_children = 'Children Present'` (without double-counting Square payment dollars), enriches via Melissa, and fires `Qualified_Bowler` (`$450.00`) for qualified suburban households.
5.2 Maintenance & Cron Endpoints
`POST /cron/adjust-thermostat`
Calls PostgreSQL function `adjust_meta_capi_thermostat()` nightly at 3:00 AM via `cron-job.org` to keep 7-day rolling `Qualified_Bowler` volume near `50` events/week.
`POST /cron/sync-square-catalog`
Paginates through Square `/v2/catalog/list` (`ITEM,CATEGORY`), classifies items into Perfect Game departments, and upserts all variations into `square_catalog_items`.
`POST /cron/enrich-partywirks`
Batch-enriches any `in_partywirks = 1` rows missing `md_address1` via Melissa Personator and recalculates their persona and `bowler_value_score`.
`POST /cron/recover-square-avs-zips`
Finds card billing ZIPs for Square profiles with no address and enriches them via Melissa.
All `/cron/*` endpoints require `CRON_SECRET` (when set). The cron-job.org jobs must send it.
`POST /cron/sync-partywirks?days_back=3`
Nightly catch-up: saves Partywirks reservations (party date from `days_back` days ago onward) that no webhook delivered. Runs in the background and logs its totals.
`GET /health`
Liveness check; reports which authentication checks are switched on.
---
6. Meta CAPI & Custom Audience Architecture
Multi-Identifier Hashing (`build_meta_user_identifiers`):
Every CAPI event sends SHA-256 hashed arrays of primary + Versium alternate emails (`em`), primary + Versium alternate phones (`ph`), first name (`fn`), last name (`ln`), city (`ct`), state (`st`), 5-digit ZIP (`zp`), country (`us`), and `external_id` (`cluster_id`). Sending multiple emails/phones per person allows Meta to match customers who use different emails for Facebook vs. online lane booking.
Dual-Event Strategy:
`Purchase`: Tracks actual cash register/online checkout revenue for true ROAS reporting.
`Qualified_Bowler`: Synthetic conversion event gated by geography (`not is_detroit_excluded`) and `bowler_value_score >= threshold`. Used as the primary Optimization Goal in Meta Ad Sets so Meta's bidding AI hunts exclusively for high-scoring suburban families and corporate planners.
Live Custom Audience Sync (`sync_customer_to_meta_custom_audiences`):
Whenever a qualified customer is enriched or transacts, FastAPI's `BackgroundTasks` pushes their hashed identity and `LOOKALIKE_VALUE` directly into up to 4 live Meta Value-Based Custom Audiences (`PG-1 Corporate`, `PG-2 Affluent Families`, `Prime Suburbs`, and `All Non-Detroit`).
---
7. Reliability Rules (Nov 2026)
Every webhook is safe to receive more than once. `unified_transactions` is the ledger: one row per Square payment (`sq_pay_<id>`), WebRes booking (`wr_<id>`), Tripleseat lead/event (`ts_<kind>_<id>`) and Partywirks booking (`pw_<id>`). Spend and visit/booking counts only change the first time an ID is seen; a later notice for the same Square payment or WebRes booking only applies the change in amount (e.g. a refund).
Square spend is `total_money` (includes tips) minus `refunded_money`.
Square payments with no customer ID, no email and no name on the card are recorded in the ledger but create no customer. Payments from a Square customer ID whose profile has no contact details are parked (`external_customer_id`, no `cluster_id`) and credited automatically once that Square customer is identified.
`Qualified_Bowler` event IDs are stable: WebRes per booking, Tripleseat per customer per event date, Partywirks per booking. Repeat webhooks never create a second conversion.
Melissa is called at most once per customer for the same inputs: `md_results` records that Melissa answered. It is only retried if Melissa was unreachable or the customer gains a new email/phone.
Custom Audience uploads log Meta's answer; a rejected upload is logged as an ERROR (`Meta audience upload to <audience> rejected`). `scripts/backfill_audiences.py` (dry run by default, `--apply` to upload) pushes qualifying customers, or with `--webhook-customers-only` just those a webhook has touched (imported customers arrive via the seed uploads); run it where `META_ACCESS_TOKEN` is set, e.g. `render ssh <service> --ephemeral`.
Meta failures never roll back a customer update. `capi_events_log.sent_to_meta` reflects Meta's actual response, and a failed event is re-sent if it comes through again.
New `cluster_id`s come from the `master_cluster_id_seq` sequence; a new customer is a plain INSERT, so a numbering clash fails loudly instead of merging two people.
Webhook routes read the body asynchronously and run all blocking work in a worker thread, so a slow Melissa/Meta call doesn't hold up other webhooks.

7.1 Rollout order (Render auto-deploys `main`)
1. Run `migrations/001_transactions_ledger_and_cluster_sequence.sql` on production Neon.
2. Merge the PR (deploys).
3. `DATABASE_URL=... python scripts/repair_after_deploy.py` (dry run), then again with `--apply`.
4. Set `SQUARE_WEBHOOK_SIGNATURE_KEY` in Render.
5. Set `WEBHOOK_SECRET` and `CRON_SECRET` in Render at the same time as adding the key to the WebRes/Tripleseat/Partywirks webhook URLs and the cron-job.org jobs.

7.2 Tests
`pip install -r requirements-dev.txt`, then `pytest tests/test_unit.py`. Integration tests also need `TEST_DATABASE_URL` set to a disposable Neon branch with migration 001 applied (they write test customers into it); all outside services are faked.

7.3 Partywirks
With `PARTYWIRKS_API_KEY` set, a Partywirks webhook is treated as a notification: the reservation (`GET /reservations/{order_id}`) and the parent (`GET /customers/{id}`, including street address) are read from the API. An order ID Partywirks doesn't know is ignored. If the API is down, the webhook body is used (flat `customer.address/city/state/zip_code`), but only for a real 10–20 digit order ID, so Partywirks' sample/test delivery (`PW-YYYYMM-NNNN`, John Doe) is never saved. Package = `activity.name`; party date = `event_date`. Cancelled reservations update contact details only: no booking count and no `Qualified_Bowler`. Children's details (`event_name`, `customer_responses`, waivers) are never stored.
`scripts/backfill_partywirks.py --count-since "<YYYY-MM-DD HH:MM:SS>"` records every reservation; bookings created before the cutoff (already counted by the original import) aren't counted again or sent to Meta.

7.4 Square items & history
Every live Square payment also reads its order (`GET /v2/orders/{order_id}`) and stores what was bought on its ledger row: `unified_transactions.line_items` (JSONB: `variation_id`, `item`, `variation`, `qty`, `gross`, `total`), `variation_ids` (price keys, joinable to `square_catalog_items` for departments) and `square_customer_id`. If the order lookup fails, the payment is still processed.
`scripts/backfill_square_history.py --since 2023-12-01` (dry run by default, `--apply` to write) records every historical payment and its items. It never changes spend or visits, because the import already holds Square's lifetime totals. Historical rows are never auto-credited later. The dry run also compares those totals with the real payment history. `--credit-between START END` credits payments in a window the system never counted, such as between the Square export and the live webhooks going live.
Every `/cron/*` endpoint shows a `key` box on the `/docs` page; enter `CRON_SECRET` there to use "Try it out".
