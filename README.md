**Perfect Game CDP, Identity Graph & Meta CAPI Engine**
> **AI & Agent Context Prompt:** This repository powers the centralized Customer Data Platform (CDP), real-time identity resolution graph, demographic enrichment waterfall, and server-side Meta Conversions API (CAPI) engine for **Perfect Game** (`35000 Grand River Ave, Farmington Hills, MI 48335`, Coordinates: `42.4705, -83.3932`). Read this specification carefully before modifying database schemas, scoring logic, geographic exclusion rules, or webhook handlers.
---
**1. Executive Summary & Core Objectives:**
Perfect Game is a single-location family entertainment center (bowling, two-story laser tag, arcade, bar/kitchen, kids' birthday parties, leagues and corporate events) in Farmington Hills, MI. This system has two jobs. The first one drives everything else:
1. **An online ads engine.** Teach Meta (and later Google) to find more of Perfect Game's best customers, meaning affluent suburban families and corporate event planners close to the venue, and to measure what those ads actually earn, online and in person. It is a legal, local version of the "predictive signal" playbook: track first-party behavior, enrich it, score every household, and send the ad platform only the signals worth learning from.
2. **The foundation of a Perfect Game CRM.** One record per household that unifies every system's data and what each customer buys (Square price keys). It grows into a CRM tied directly to Square: quotes, payments and segments.

**Strategic direction (decided by the owner; treat as rules):**
- **Hyperlocal.** Shift the customer base toward the immediate western/northern suburbs (Farmington Hills, Novi, Northville, West Bloomfield, Livonia, Bloomfield Hills, Plymouth, Commerce/Walled Lake …; see `PRIME_SUBURB_ZIPS`).
- **Detroit and Southfield (and the other excluded areas in `is_detroit_excluded`) are removed from Meta entirely.** No Meta events, no audience uploads, no lookalikes seeded from them. They are excluded from *advertising*, not from data collection: their records are still stored and Melissa-enriched (Versium is skipped for them). The Grosse Pointes (48230/48236) are allowed.
- **Only great customers go to Meta.** The 4 curated value-based seed audiences, never the full customer list.
- **Maximum information.** Enrich every customer with every useful Melissa field, and every customer *eligible for advertising* with every useful Versium field. Cost is secondary.
- **Versium spend follows ad eligibility.** No Versium calls for excluded areas (`is_detroit_excluded`); enriching people we will never advertise to is wasted money. Versium *online audience append* (Meta/Google/LinkedIn/TikTok IDs) additionally requires a Michigan or unknown state (`versium_online_append_eligible`): out-of-state customers can't be reached by local ads and would skew lookalike seeds toward visitors.
- **Never store sensitive Versium fields.** Credit rating / credit lines, ethnic group, religion, party affiliation and political donations are dropped before saving (`strip_blocked_versium_fields`) and must never feed scoring, personas or audiences.
- **Legal and within platform rules.** The privacy policy and SMS terms disclose enrichment and hashed ad-platform sharing. Never use credit scores, scraping or exclusion lists built from broker financial data. Low-value prospects are handled by withholding the signal, not by excluding people. Promotional SMS goes only to people who opted in.
- **Scoring is provisional.** The rule-based `bowler_value_score` / personas will be re-tuned once Versium data is in. Don't change weights without the owner.

**Where customer data comes from:**
- Square POS & Square Online: in-store card payments (lanes, bar, kitchen, arcade, shoes), online packages, event deposits. Live webhooks (payments, customers, catalog) plus a full payment and item history since Dec 2023. WebRes and Partywirks payments also settle through Square.
- Qubica WebRes (QubicaAMF): online lane reservations. Qubica has no webhooks; each confirmation email (with a `W#####.pdf`) arrives at reservations@myperfectgame.com and a Google Apps Script posts it to `/webhooks/webres`.
- Tripleseat: corporate events, banquets and large-group leads and bookings, via native webhooks.
- Partywirks: kids'/teen birthday party bookings (`BIRTHDAY EXTRAVAGANZA`, `BOWLING PARTY`, `TEEN PARTY`), via webhooks verified against the Partywirks REST API, plus a nightly catch-up.
- Possible future source: Intercard (arcade tellers and kiosks). Its revenue API has no customer data; registered game cards would.

**How the ads engine works:**
- **Identity:** every Square, WebRes, Tripleseat and Partywirks touch is resolved to one household (`master_customers.cluster_id`) through emails, phones, Square customer IDs and Melissa keys (`customer_identities`), without double-counting revenue that settles through Square.
- **Enrichment:** Melissa first (verified address, geocode, distance, household demographics), then Versium (alternate emails/phones for Meta match rates, wealth, B2B employment).
- **Scoring and gating:** each household gets a persona (`PG-1`…`PG-5`) and a `bowler_value_score` (behaviour + best goal fit, section 4.4). A `Qualified_Bowler` event goes to Meta only for households outside the excluded areas that score at or above a nightly thermostat threshold, paced to ~50/week so ad sets exit "Learning Limited". Low scorers send nothing, so Meta learns not to chase them. Campaigns should optimize on the `Qualified_Bowler` custom conversion.
- **Attribution:** real `Purchase` events (WebRes bookings, Square payments including in-store bar/arcade/kitchen spend) are sent with the same hashed identifiers. Meta can then attribute both the booking *and* later in-venue spend to the ad that drove it.
- **Audiences:** non-excluded households are pushed automatically into the value-based Custom Audiences that seed the lookalikes, by who they are (section 4.5), whatever their score.

**Roadmap (in rough order):** Versium enrichment (alternate IDs, wealth, C2B) → recover more identities (card fingerprints, receipt emails, register phone/email capture) → re-tune the score on real spend and item data → the owner's own "Perfect Game Mosaic" (cluster households on demographics + spend; find look-alike Census tracts and non-customer households for lookalikes, prospect lists and carrier-route mail) → item-based segments (e.g. open-play families who never booked a party, the bar/league crowd, "book next year's birthday" reminders) → CRM features on Square price keys → Google Ads (offline conversions + Customer Match) → Intercard arcade data.

**What this system does today:**
- Identity Resolution & Deduplication: Stitches fragmented customer records across Square, WebRes, Tripleseat, and Partywirks into a single golden household record (`master_customers.cluster_id`) without double-counting revenue when online bookings or Partywirks/Tripleseat deposits settle through Square POS.
- Real-Time Progressive Enrichment: Automatically enriches incoming leads and POS customers via Melissa Personator API (verifying physical street address, latitude/longitude, distance from the venue, household income, and presence of children) and, once the Versium key is set, Versium REACH API (appending up to 5 alternate emails/phones per person, net worth, home market value, and B2B corporate employment).
- Hard Geographic Ad Exclusion Gate: Strictly blocks Meta CAPI optimization events (`Qualified_Bowler`, `Purchase`) and Custom Audience syncs for excluded geographic areas (Detroit, Southfield, Redford, Pontiac, Oak Park, Inkster, etc.) so Meta's ad delivery algorithm never learns to prospect in non-target areas.
- Algorithmic Lead Scoring & Thermostat Pacing: Assigns every household to a Perfect Game Mosaic Persona (`PG-1` through `PG-5`) and calculates a `bowler_value_score` (`0–100`). A nightly thermostat (`cdp_thermostat_config`) dynamically adjusts the minimum score threshold (started at `70`; moves in steps of 5 between `45` and `90`) to maintain ~50 `Qualified_Bowler` conversion events per 7-day rolling window—keeping Meta Ad Sets out of the "Learning Limited" phase while optimizing purely for high-value suburban families and corporate event planners.
---
2. Infrastructure & Tech Stack
Application Server: FastAPI (`main.py`) hosted on Render (`https://pg-cdp-webhook.onrender.com`)
Database: Serverless PostgreSQL hosted on Neon (`customer_database`)
Enrichment Providers:
Melissa Data Personator API (`/v3/WEB/ContactVerify/doContactVerify`): street-address verification, geocoding (`GrpGeocode`), carrier route and household/address keys via `act=Check` (credit license: no Append/Move, no demographics; see `MELISSA_ACT` / `MELISSA_COLS`).
Versium REACH API (`/v2/contact`, `/v2/demographic`, `/v2/onlineAudience`): Multi-email/phone identity graph (`email_1..5`, `phone_1..5`), wealth/home value signals, B2B company/title matching, and platform-ready online audience IDs for Meta, Google, LinkedIn and TikTok (only for customers passing `versium_online_append_eligible`). Versium's hashed Facebook emails/phones are added to every CAPI event's `em`/`ph`.
Ad Destination: Meta Graph API `v21.0` (Server-Side Conversions API + Real-Time Custom Audience API).
Required Environment Variables (Render Dashboard)
Variable	Purpose
`DATABASE_URL`	PostgreSQL connection string for Neon (`customer_database`)
`SQUARE_ACCESS_TOKEN`	Production Square API token (used for Customer Profile lookups & Catalog sync)
`MELISSA_API_KEY`	License key for Melissa Personator Consumer (credit license: address Check only)
`ENRICH_DELAY_MINUTES`	Minutes between a customer's first webhook and their enrichment (default `15`). A WebRes/Partywirks booking's Square payment arrives first and the booking details follow a few minutes later (the Gmail script runs every 5 minutes), so webhooks only save and merge the customer; a worker inside the server (every minute) then runs Versium → Census → scoring → the held Meta events, with no time limits. `0` enriches inside the webhook as before (switch-back). Requires `migrations/008`.
`MELISSA_LIVE`	`true` runs Melissa on new customers during live enrichment. Default `false`: on the credit license Melissa can only check a street the customer already gave, which the free Census Geocoder covers; turn on for a direct-mail campaign that needs verified addresses and carrier routes. Finished lookups without Melissa are marked `md_results = MELISSA_OFF`. `/cron/melissa-geocode` works either way.
`MELISSA_ACT` / `MELISSA_COLS`	Optional overrides for the Melissa actions/column groups (defaults `Check` / `GrpAddressDetails,GrpGeocode`; a subscription license can add `Verify,Append` and `GrpDemographicBasic`)
`VERSIUM_API_KEY`	API key for Versium REACH Contact, Demographic & B2B Append
`META_PIXEL_ID`	Meta Dataset / Pixel ID (`Perfect Game Facebook Data`)
`META_ACCESS_TOKEN`	Meta System User Access Token with CAPI & Custom Audience permissions
`AUDIENCE_PG1_CORP`	Meta Custom Audience ID (`120246555016470358`) - Corporate & High-LTV
`AUDIENCE_PG2_FAMILIES`	Meta Custom Audience ID (`120246555006040358`) - Affluent Suburban Party Families
`AUDIENCE_PRIME_SUBURBS`	Meta Custom Audience ID (`120246555019320358`) - Prime Western/Northern Suburbs
`AUDIENCE_ALL_NONDETROIT`	Meta Custom Audience ID (`120246555027130358`) - All Qualified Non-Excluded Customers
`SQUARE_WEBHOOK_SIGNATURE_KEY`	Signature key of the Square webhook subscription. When set, `/webhooks/square` rejects requests without a valid `x-square-hmacsha256-signature`.
`SQUARE_WEBHOOK_URL`	The exact notification URL registered with Square (default `https://pg-cdp-webhook.onrender.com/webhooks/square`). Square signs this URL + the body.
`WEBHOOK_SECRET`	Shared key for `/webhooks/webres`, `/webhooks/tripleseat`, `/webhooks/partywirks`. Senders pass it as header `X-CDP-Key` or query `?key=`. Unset = every webhook is refused (401). The app's access log redacts any `key=` value, but the header form keeps it out of URLs entirely.
`WEBHOOK_SECRET_PREVIOUS`	Only during a rotation: the old shared key, still accepted (logged as `Accepted … with WEBHOOK_SECRET_PREVIOUS`) until every sender is on the new one. Remove it afterwards.
`TRIPLESEAT_WEBHOOK_SECRET`	The signing secret of the Tripleseat webhook endpoint (Settings > API/Webhooks). When set, every Tripleseat delivery's `X-Signature` (`t=<timestamp>,v1=<HMAC-SHA256>`) is checked and the result logged (`Tripleseat signature verified (scheme …)` / `MISMATCH` / `MISSING`); the shared key is still required.
`TRIPLESEAT_SIGNATURE_SCHEME`	The signed-payload construction real deliveries verified with, copied from the `verified (scheme …)` log line (e.g. `t.body:hex`). If that line says `AMBIGUOUS, also matches …`, the delivery couldn't tell the named schemes apart (e.g. a base64 signature with none of `+ / - _`): wait for a delivery that matches only one before pinning. Tripleseat doesn't document it, so the check tries each candidate until this is set.
`TRIPLESEAT_SIGNATURE_ENFORCE`	`true` (with a timestamp-signed scheme set: any of the payloads marked as signing the timestamp in `TRIPLESEAT_SIGNATURE_PAYLOADS` in `main.py`) = a valid signature whose signed timestamp is within ±300 s is required and replaces the shared key, so `?key=` can be removed from the Tripleseat URL. The ±300 s replay check only exists for those timestamp-signed formats: a body-only format (`body`, `body-rstrip`, `url.body`, `urlnoq.body`, `bodyhash`) signs no timestamp, can't stop a replay, and therefore can never be enforced (it's logged for diagnosis only, and the shared key stays required). Without a scheme it also stays check-only.
`CRON_SECRET`	Separate key for every `/cron/*` endpoint (header `X-CDP-Key` or query `?key=`).
`PARTYWIRKS_WEBHOOK_SECRET`	Partywirks webhook signature key (HMAC-SHA256 in `X-Partywirks-Signature`). When set, every delivery is checked and the result logged.
`PARTYWIRKS_SIGNATURE_ENFORCE`	Set to `true` to reject (401) deliveries whose signature doesn't match. Only turn on after the logs show `Partywirks signature verified` for a real booking.
`PARTYWIRKS_API_KEY`	Partywirks REST API key (`reservations` and `customers` scopes). When set, every Partywirks webhook is verified and completed by reading the reservation back from the API.
`TRIPLESEAT_CLIENT_ID` / `TRIPLESEAT_CLIENT_SECRET`	Tripleseat OAuth 2 app (Settings → API). The API connection stays off until both are set.
`TRIPLESEAT_REDIRECT_URI`	Must match the app's registered Redirect URI (default `https://pg-cdp-webhook.onrender.com/oauth/tripleseat/callback`).
`TRIPLESEAT_SCOPES`	Optional space-separated scopes; blank uses the scopes configured on the Tripleseat app.
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
`true_lifetime_spend`: `GREATEST(sq_spend, wr_spend, pw_spend)` — a max, never a sum, because WebRes/Partywirks/Tripleseat payments also clear through Square. It undercounts a customer with different parties in different systems; that approximation is accepted.
`webres_online_spend`: Online lane reservation spend.
`instore_pos_spend`: `GREATEST(0, sq_spend - wr_spend)` — incremental in-center F&B, arcade, and walk-in spend.
`sq_visits`, `wr_bookings`, `ts_events`, `pw_bookings`: Activity counts by channel.
Tripleseat Event Tracking: `ts_account` (company name), `ts_lead_status` (`PROSPECT`, `TENTATIVE`, `DEFINITE`, `CLOSED`, `LOST`, `TURNED_DOWN`), `ts_max_guests`, `ts_last_event_date`, `ts_turned_down_reason`
Partywirks Birthday Tracking: `pw_bookings`, `pw_spend` (sum of the customer's Partywirks order totals, `migrations/011`), `pw_last_package` (`BIRTHDAY EXTRAVAGANZA`, `BOWLING PARTY`, `TEEN PARTY`), `pw_last_party_date` (`DATE`)
Melissa Demographic & Geo Columns: `md_address1`, `md_city`, `md_state`, `zip5`, `md_plus4`, `md_carrier_route`, `md_latitude`, `md_longitude`, `distance_miles` (Haversine distance from `42.4705, -83.3932`), `md_household_income`, `md_presence_of_children`, `md_children_age_range`, `md_own_rent`, `md_marital_status`, `md_household_size`, `md_length_of_residence`, `md_education`, `md_occupation`, `md_date_of_birth`, `md_gender`, `md_delivery_indicator` (`B` = Business, `R` = Residential), `md_mik` (Melissa Identity Key), `md_mak` (Melissa Address Key), `md_results`
Versium REACH Columns: `versium_net_worth`, `versium_home_market_value`, `versium_auto_make_model`, `versium_b2b_company`, `versium_alt_emails` (`JSONB` array), `versium_alt_phones` (`JSONB` array), `versium_lifestyle_flags` (`JSONB`), `versium_online_audience` (`JSONB`: Versium online audience append, `{"facebook"|"google"|"linkedin"|"tiktok": {field: [values]}}`; Facebook/TikTok emails and phones are SHA-256 hashed by Versium; requires `migrations/004`)
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
`capi_events_log`: Audit trail of every `Purchase` and `Qualified_Bowler` event sent to Meta CAPI, storing `event_id` (for deduplication), `cluster_id`, `event_name`, `event_value`, `bowler_value_score`, `threshold_at_send`, `meta_response_status`, and `meta_response_body`. `event_time` (migrations/014) is the event_time sent to Meta, stored at first log and reused by retries and the release after the enrichment hold.
`cdp_thermostat_config`: Single-row configuration table (`id = 1`) holding `current_score_threshold` (default `70`), `target_weekly_events` (`50`), and `last_adjusted_at`.
---
4. Geographic Rules, Personas & Scoring Model
4.1 Hard Geographic Exclusion Gate (`is_detroit_excluded`)
To prevent Meta's ad delivery AI from optimizing toward lower-converting or out-of-market areas, any customer matching the following verified cities or ZIP codes is assigned to `PG-4: Excluded Geo` (unless an earlier persona matches) and completely blocked from firing Meta CAPI events or entering Meta Custom Audiences:
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
Labels for reporting, first match wins (`assign_persona`). Seeds overlap and don't use the persona order (4.5).
`PG-1: Corporate & Group Organisers`: a real organisation (`is_real_organisation`: a Tripleseat account name that is not the contact's own name, unless it has a business word like "Services"/"Foundation"; or a business mailing address, `md_delivery_indicator == 'B'`), or the contact on a `DEFINITE`/`CLOSED` Tripleseat event. Guest count alone no longer counts (it labelled large family parties as corporate).
`PG-2: Birthday Party Families`: a Partywirks booking, or kids aged 3-10 + age 35-55 + within 12 miles + affluence.
`PG-3: High-Value Open Bowlers / Celebrations`: lifetime spend `$279+` (top quarter), or 3+ separate visit days within 15 miles.
`PG-4: Excluded Geo (Detroit / Southfield / Downriver)`: `is_detroit_excluded`. Never sends events or joins audiences (whatever the label, every send checks the geography).
`PG-5: Not Enough Data Yet`: everyone else. Not a target; customers graduate by spending or by enrichment.
4.4 Bowler Value Score (`0–100`)
Recomputed from current facts on every rescore, so it goes down as well as up (no ratchet), with no base points. Score = behaviour + the best of three goal fits, capped at 100. Verified on the customer base on 2026-11-19 (spend-correlators-2026-11-19.md); behaviour predicts far better than demographics and covers everyone.
Behaviour (shared): party/event booker (Partywirks booking or `DEFINITE`/`CLOSED` Tripleseat) `+25`; lifetime spend `$279+` `+15` or `$600+` `+20`; bought or booked in the last 90 days `+10`; 3+ separate visit days `+5`.
Recency: Square payments by their payment date; WebRes, Partywirks and Tripleseat by the booking's own date (reservation / party / event date in the past 90 days or upcoming). Their ledger rows carry the date they were first recorded, and the Nov 16-17 backfill made them all look recent. Several payments on one day are one visit.
Birthday fit: age 35-55 `+10`, kids 3-10 `+10`, affluence (income `$150k+` or home `$450k+`) `+5`, 5-14 years at the address `+5`, female contact `+5`, within 5 miles `+10` / 12 miles `+5`, married `+5`, household of 2 `+5`, young adult (18-24) at home `-10`.
Corporate fit: real organisation `+15`, age 35-65 `+10`, female contact `+5`, within 5 / 12 / 15 miles `+10` / `+5` / `+2`, married `+5`. Income and home value don't count.
Celebration fit: within 5 / 12 miles `+10` / `+5`, age 35-55 `+5`, affluence `+5`, 5-14 years at the address `+5`, household of 2 `+5`, married `+5`.
Sources (`_fit_facts`): Versium `versium_lifestyle_flags` first (`age_range`, `household_income`, `home_value`, `length_of_residence`, `household_size`, `young_adult_in_household`, `gender`, `marital_status`), then `versium_home_market_value`; Melissa only for kids' ages (`md_children_age_range`), and as a fallback for income (`md_household_income`) and tenure (`md_length_of_residence`). Bands are read by their lower bound. Never ethnicity, religion, politics or credit; kids present alone, own/rent, education and net worth are not scored (verified weak).
The winning goal (`winning_goal`) is sent on every `Qualified_Bowler` as `custom_data.qb_goal` (`birthday` / `corporate` / `celebration`) for per-goal custom conversions.
4.5 Seed audiences (`seed_memberships`, `audience_targets`)
Built from who the customer is, not from the score threshold and not from distance (the ad set's radius does the geography). `LOOKALIKE_VALUE` is real lifetime spend, no floor. Corporate (`AUDIENCE_PG1_CORP`): real organisation or a `DEFINITE`/`CLOSED` Tripleseat contact. Birthday (`AUDIENCE_PG2_FAMILIES`): a Partywirks booking, or kids 3-10 + age 35-55 + affluence. Broad Local (`AUDIENCE_BROAD_LOCAL`): spend `$279+`, 3+ Square visit days, or age 35-55 + affluence. It replaces the old PG-3 Celebrations seed and contains all of it. No seed reads the stored persona label. Prime Suburbs and Master (All Non-Detroit) unchanged. Each customer is uploaded as their own email/phone plus one extra row per additional identifier (Versium alternate and Facebook-linked emails/phones, up to 10 extra rows), so Meta can match people whose Facebook account uses another email or phone. Excluded areas join none. Uploads only add people; removing members who no longer belong means replacing the audience.
---
5. Live API Endpoints (`main.py`)
5.1 Real-Time Ingestion Webhooks
`POST /webhooks/square`
Handles `payment.updated` (`COMPLETED`), `customer.created`, `customer.updated`, and `catalog.version.updated`.
When a POS payment only contains `customer_id`, calls Square's `/v2/customers/{id}` API to fetch full name, email, phone, and address.
Stitches customer, enriches via Melissa/Versium if `md_address1` is missing, and fires a server-side `Purchase` CAPI event (with actual dollar spend) if not in an excluded geography.
When `catalog.version.updated` is received, queues `run_square_catalog_sync()` in the background so POS item/price changes update in Neon automatically.
`POST /webhooks/webres`
Ingests online lane bookings. Fires `Purchase` (actual booking amount) unless the same money already went to Meta as a Square payment (see below), AND, if `bowler_value_score >= current_score_threshold`, fires `Qualified_Bowler` worth the same real booking amount (no floor).
`POST /webhooks/tripleseat`
Unwraps native Tripleseat payloads (`lead`, `event`, `booking`, `contact`). Extracts company name, guest count, event date, and lead status.
If not `LOST`/`TURNED_DOWN`, not in an excluded geo, and `bowler_value_score >= threshold`: a definite booking (an event or booking with status `DEFINITE`/`CLOSED`) fires `Qualified_Bowler` worth its contracted `grand_total` (a booking's `total_grand_total`), or no value if Tripleseat gives none. A prospect or tentative lead fires a standard `Lead` with no value, never a `Qualified_Bowler`.
`GET /oauth/tripleseat/login?key=<CRON_SECRET>` → `GET /oauth/tripleseat/callback`
Connects the server to the Tripleseat API (requires `migrations/006`). Open the login link in a browser and approve access at Tripleseat; the callback stores the tokens in `oauth_tokens`. Access tokens last 2 hours and are renewed automatically with the refresh token, so the login is needed once (or again if Tripleseat revokes access). `GET /cron/tripleseat-status` shows whether it's connected; `check=true` also makes one API call.
`POST /cron/sync-tripleseat?days_back=3` (nightly) / `?history=true&apply=false` (one-time import, dry run first)
Reads Tripleseat bookings (with events and payments, `show_financial=true`) updated in the window and leads created in it, links each to a customer (`in_tripleseat`, `ts_*` fields; new people are enriched like any other) and stores them in `tripleseat_bookings`, `tripleseat_events` and `tripleseat_leads` (requires `migrations/007`). It **never changes lifetime spend and never sends Meta events**: about 99% of Tripleseat payments settle through Square. `paid_not_card` holds what was paid by check/ACH/cash for the later scoring rebuild. A booking adds 1 to `ts_events` only when it's new, not lost, and no webhook already recorded it or one of its events; `history=true` adds nothing (old bookings aren't new events). Leads never count. Runs in the background; totals are logged as `Tripleseat sync: {...}`, along with the booking's field names (no values) the first time. Search dates are sent as `TRIPLESEAT_DATE_FORMAT` (default `%m/%d/%Y`).
`POST /webhooks/partywirks`
Ingests real-time Partywirks birthday bookings, extracts package name and party date, marks `in_partywirks = 1` and `md_presence_of_children = 'Children Present'` (without double-counting Square payment dollars), enriches via Melissa, and fires `Qualified_Bowler` worth the real order total (`totals.total`, or no value without one) for qualified suburban households. No Partywirks `Purchase`: the money arrives as Square payments, so one would first need the same twin check as WebRes.
5.2 Maintenance & Cron Endpoints
`POST /cron/adjust-thermostat`
Calls PostgreSQL function `adjust_meta_capi_thermostat()` nightly at 3:00 AM via `cron-job.org` to keep 7-day rolling `Qualified_Bowler` volume near `50` events/week. The function and its config table are versioned in `migrations/012_thermostat.sql` (copied exactly from production): it counts `Qualified_Bowler` events Meta accepted in the last 7 days; under 40 it steps the threshold down 5 (floor 45), over 65 up 5 (ceiling 90), otherwise it holds. The migration never changes the live row; its seed (threshold 55) only applies to an empty database.
`POST /cron/sync-square-catalog`
Paginates through Square `/v2/catalog/list` (`ITEM,CATEGORY`), classifies items into Perfect Game departments, and upserts all variations into `square_catalog_items`.
`POST /cron/enrich-partywirks`
Batch-enriches any `in_partywirks = 1` rows missing `md_address1` via Melissa Personator and recalculates their persona and `bowler_value_score`.
`POST /cron/census-geocode?limit=200`
Free US Census Geocoder (no key; only the street address is sent) for customers with a street address but no census tract or no coordinates: census tract/block, county, school districts, plus coordinates and `distance_miles` when Melissa had none; then rescores. Each address is asked once (`census_checked_at`, `migrations/005`). Live enrichment does the same right after Melissa.
`POST /cron/melissa-geocode?limit=100`
Runs customers with a street address + ZIP but no Melissa address work (no coordinates or no carrier route, e.g. addresses from a Versium CSV) through Melissa: verified address, coordinates/`distance_miles`, carrier route, household key; then rescores them. Melissa is address-only on the current credit license (`MELISSA_ACT`, default `Check`; `MELISSA_COLS`, default `GrpAddressDetails,GrpGeocode`): credit licenses answer GE29 to the Append/Move actions and to demographic fields, so Melissa only checks a street address we already have (customers without one are marked `NO_STREET_TO_CHECK` and not charged), while email/phone→address and demographics come from Versium. Highest scores first, at most `limit` paid lookups per call (max 500). Each customer is tried once (`md_results` gets `GEO_TRIED`); a Melissa outage leaves them for the next run.
**Deferred enrichment (migrations/008).** A webhook sets `enrich_after` (first webhook + `ENRICH_DELAY_MINUTES`; later webhooks add their details to `enrich_inputs` without restarting the window). Meta events for a waiting customer are logged as `DEFERRED_ENRICHMENT` instead of sent; Qualified_Bowler is held whatever the score, since the score can change. The worker claims due customers with a 10-minute lease, enriches them, rescores, and releases the held events: Purchase unless the customer is in an excluded area, Qualified_Bowler and Lead also need the threshold; others become `DROPPED_AFTER_ENRICHMENT` (never retried). Then the customer is added to their Meta audiences. A Versium failure (including a missing `VERSIUM_API_KEY` while `VERSIUM_REQUIRED`, default true) is retried 15/30/45/60 minutes later. A held `Qualified_Bowler`/`Lead` is only dropped as `below_threshold` once enrichment finished (Versium answered, match or no match, or the customer is in an excluded area); until then it stays held and the customer is re-queued. After the 5th failed attempt, an event that would be dropped is dropped as `enrichment_exhausted`, and `/cron/versium-enrich` judges those again (sent or dropped) once Versium answers for the customer, while Meta still accepts them. `master_customers.enrich_outcome` (migrations/015) records each provider's answer on the last run, the attempt and how it ended. Provider calls (Melissa 10 s, Versium 8 s timeouts) retry timeouts, connection errors, 5xx and 429 (honouring Retry-After) up to 3 attempts in the worker (1 inside a webhook); any other 4xx or a Melissa TransmissionResults rejection is not retried. Failures are logged with provider, endpoint, HTTP status, time and a redacted body. `/cron/melissa-geocode` stops after 3 failures in a row. `/health` shows `versium_key_set`. `scripts/reevaluate_dropped_events.py` audits past drops and re-releases those made before enrichment finished. A customer Versium found nothing for gets one retry when a later webhook brings an email, phone or street we didn't have (`_versium_retried`); a matched customer is never bought twice. The WebRes webhook also accepts `address`/`city`/`state`/`zip` if the Gmail script sends them.
`POST /cron/versium-enrich?limit=100`
Versium contact + demographic append for advertisable customers Versium has never answered for (no CSV row, no live answer). Live enrichment order is Melissa (only if `MELISSA_LIVE`) → Versium → Census: Versium's contact append returns alternate emails/phones and, when the customer has no street, the postal address (used only if it doesn't contradict a known ZIP); the Census Geocoder then adds coordinates, `distance_miles` and census fields. Demographics use outputs `demographic`, `financial`, `lifestyle` (never `political`); Versium's display-name fields are stored in `versium_lifestyle_flags` under snake_case names (the CSV import's), merged with what's there, with blocked categories removed. A Versium address in an excluded area stops further paid lookups. Answered customers get `_versium_checked` (today) and are never bought again; a rejected call is logged as `Versium <endpoint> rejected (HTTP …)` and retried by the next run. About 2 paid lookups per customer (3 when the online audience is also missing); highest scores first; stops after 3 rejections in a row.
`POST /cron/versium-online-audience?limit=100`
Versium online audience append (Meta / Google / LinkedIn / TikTok IDs) for eligible customers (`versium_online_append_eligible`) that don't have it yet: never checked, or the live call was skipped (slow lookups) or failed. Highest scores first, at most `limit` paid lookups per call (max 500); stops after 3 rejections in a row. No-match customers are marked `_checked` so they aren't bought again.
`POST /cron/melissa-batch` / `POST /cron/melissa-batch/{id}/emailed`
Manual Melissa.com uploads (requires `migrations/009`). Customers with a first and last name and an email or phone, whom Versium answered for but found no demographics, with no Melissa demographics, not waiting for deferred enrichment and never uploaded (`melissa_upload_status` NULL), are queued. Once `MELISSA_BATCH_SIZE` (default `100`) are waiting, the call makes a batch (`melissa_batches`; oldest customers first; their status becomes `sent` with `melissa_batch_id`) and returns `{ready, batch_id, customers, filename, csv}`; the CSV columns are `RecordID` (the customer number), `First Name`, `Last Name`, `Address`, `City`, `State`, `Zip`, `Email`, `Phone` (10 digits). Until it's confirmed with `/emailed`, the same batch is returned again; otherwise `{ready: false, waiting, batch_size}`. The Gmail Apps Script `apps-script/melissa_batch.gs` calls it hourly and emails the CSV (setup steps are in the file). The migration marks the customers Melissa had already answered for as `prior`, so they aren't uploaded again; set them back to NULL to include them.
`POST /cron/retry-webhooks?limit=50` (hourly)
Failed-webhook retry (requires `migrations/010`). A Tripleseat webhook that fails still answers 500, but its full payload is kept in `webhook_dead_letters` (the same payload re-sent is stored once). Each is tied to its Tripleseat record (`record_key`, e.g. `event:123`): when a newer webhook for that record succeeds, older failed copies are marked `superseded` and never replayed, and the retry only replays the newest failed copy of a record. This call processes the open ones again, oldest first: a success is marked `resolved_at` with the result; a failure counts an attempt and stops being retried after 5 (it stays in the table with its last error). Returns `{checked, resolved, failed}`.
`POST /cron/recover-square-avs-zips`
Finds card billing ZIPs for Square profiles with no address and enriches them via Melissa.
All `/cron/*` endpoints and `/oauth/tripleseat/login` require `CRON_SECRET`, sent as the `X-CDP-Key` header (preferred) or `?key=`. If `CRON_SECRET` isn't set in Render they all refuse every request (401, logged as `CRON_SECRET is not set: refusing …`), so the cron-job.org jobs stop until it's set: before deploying, check `/health` shows `cron_key_check: true`. A refused call is logged with its path and reason (`no key` / `wrong key`), never the key.
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
**Event time = transaction time (migrations/014).** `event_time` is when the transaction happened, never when it's sent: Square = the payment's `created_at`; WebRes = the Gmail script's `booked_at` / `created_at` / `email_received_at` if it sends one, else the time the webhook arrives (the script posts within ~5 minutes), never the reservation date; Partywirks = the order's `created_at` (venue local time, America/Detroit), never the party date; Tripleseat = a booking's change to its current status (`status_changes`), else the record's `created_at`, never the event date. A future time is clamped to now, and a time older than Meta's 7 days (e.g. an old record that became definite today) becomes now. Retries age by `event_time`, so Meta never gets an event older than it accepts.

**Browser identifiers (fbp / fbc / user agent).** Only the Tripleseat Dynamic lead form (7252) on `/groups-and-parties` passes them: `site-snippets/tripleseat_lead_form_meta_ids.html` fills hidden custom fields `fbp`, `fbc` and `user_agent` from the visitor's `_fbp`/`_fbc` cookies (or `fb.1.<ms>.<fbclid>` from the URL) and the browser. Each is stored on the customer with its own capture time (`meta_fbp` / `meta_fbp_at`, `meta_fbc` / `meta_fbc_at`, `meta_client_user_agent` / `meta_client_user_agent_at`). Per field, the newest capture wins, and a newer capture that lacks a field (a visit not from an ad click has no fbc) leaves the stored value and its capture time alone. Each value is added to that customer's Tripleseat `Lead` / `Qualified_Bowler` events while its own capture is under 90 days old; otherwise it's left out. Square (in-store, no browser session; Square Online already forwards its own events with fbp), WebRes (Gmail confirmation emails), Partywirks (its API has no browser fields) and Tripleseat's hosted request page carry none, so their events send none. No source gives the client IP address.

**One Purchase per payment (WebRes + Square).** A WebRes booking is paid through Square online, so the same money arrives as a Square payment (`sq_pay_…`) and, 1–6 minutes later, as the WebRes booking (`wr_…`). Meta can't dedupe the two IDs. The Square payment is the source of truth for the money. A WebRes Purchase is logged as `DUPLICATE_PURCHASE` (not sent, never retried; `meta_response_body` = the Square event ID) when the same customer has a Square Purchase of the same amount recorded within `PURCHASE_TWIN_HOURS` (6) of the booking (times are when each was first recorded in `unified_transactions`, so a retry days later pairs the same way), whether it was sent, is waiting for a retry, or is held for enrichment. Each Square payment pairs with at most one booking. A Square Purchase is only skipped that way if a matching WebRes Purchase already reached Meta. A WebRes booking whose Square payment couldn't be tied to the customer still sends its Purchase. Lifetime spend already counted the money once (`true_lifetime_spend` = greater of Square and WebRes).
**Meta values are real money or nothing.** Every event carries the actual money of that transaction, or no `value`/`currency` at all; never an estimate, since Meta treats value as revenue in bidding and reporting. Events sent Nov 14–19, 2026 carried estimated Qualified_Bowler values ($250 floor, $450/$1,000), so value reporting for that window is inflated.

`Qualified_Bowler`: Custom conversion event gated by geography (`not is_detroit_excluded`) and `bowler_value_score >= threshold`. Carries `qb_goal` (the winning goal). Used as the primary Optimization Goal in Meta Ad Sets so Meta's bidding AI hunts exclusively for high-scoring suburban families and corporate planners.
Live Custom Audience Sync (`sync_customer_to_meta_custom_audiences`):
Whenever a non-excluded customer is enriched or transacts, FastAPI's `BackgroundTasks` pushes their hashed identity and `LOOKALIKE_VALUE` (real lifetime spend) into the Meta Value-Based Custom Audiences they belong in (`Corporate`, `Birthday`, `Broad Local`, `Prime Suburbs`, `Master (All Non-Detroit)`), whatever their score.
---
7. Reliability Rules (Nov 2026)
Every webhook is safe to receive more than once. `unified_transactions` is the ledger: one row per Square payment (`sq_pay_<id>`), WebRes booking (`wr_<id>`), Tripleseat lead/event (`ts_<kind>_<id>`) and Partywirks booking (`pw_<id>`). Spend and visit/booking counts only change the first time an ID is seen; a later notice for the same Square payment or WebRes booking only applies the change in amount (e.g. a refund).
Square spend is `total_money` (includes tips) minus `refunded_money`.
Square payments with no customer ID, no email and no name on the card are recorded in the ledger but create no customer. Payments from a Square customer ID whose profile has no contact details are parked (`external_customer_id`, no `cluster_id`) and credited automatically once that Square customer is identified.
`Qualified_Bowler` event IDs are stable: WebRes per booking, Tripleseat per customer per event date (`qb_ts_` for a definite booking, `lead_ts_` for a prospect/tentative lead), Partywirks per booking. Repeat webhooks never create a second conversion.
Melissa is called at most once per customer for the same inputs: `md_results` records that Melissa answered. It is only retried if Melissa was unreachable or the customer gains a new email/phone.
Custom Audience uploads log Meta's answer; a rejected upload is logged as an ERROR (`Meta audience upload to <audience> rejected`). `scripts/backfill_audiences.py` (dry run by default, `--apply` to upload) pushes every non-excluded customer with an email/phone into their seeds (no score cutoff), or with `--webhook-customers-only` just those a webhook has touched (imported customers arrive via the seed uploads); run it where `META_ACCESS_TOKEN` is set, e.g. `render ssh <service> --ephemeral`.
Meta failures never roll back a customer update. `capi_events_log.sent_to_meta` reflects Meta's actual response, and a failed event is re-sent if it comes through again.
New `cluster_id`s come from the `master_cluster_id_seq` sequence; a new customer is a plain INSERT, so a numbering clash fails loudly instead of merging two people.
Webhook routes read the body asynchronously and run all blocking work in a worker thread, so a slow Melissa/Meta call doesn't hold up other webhooks.

7.1 Rollout order (Render auto-deploys `main`)
1. Run `migrations/001_transactions_ledger_and_cluster_sequence.sql` on production Neon.
2. Merge the PR (deploys).
3. `DATABASE_URL=... python scripts/repair_after_deploy.py` (dry run), then again with `--apply`.
4. Set `SQUARE_WEBHOOK_SIGNATURE_KEY` in Render.
5. Set `WEBHOOK_SECRET` and `CRON_SECRET` in Render at the same time as adding the key to the WebRes/Tripleseat/Partywirks webhook URLs and the cron-job.org jobs. Both fail closed: with either unset, its endpoints refuse every request. Before a deploy goes live, `/health` must show `webhook_key_check: true` and `cron_key_check: true`.

**Rotating `WEBHOOK_SECRET` (no 401s).** `CRON_SECRET` is separate and not part of this.
1. In Render set `WEBHOOK_SECRET_PREVIOUS` = the current key and `WEBHOOK_SECRET` = a new random key (one save = one deploy).
2. Move each sender to the new key: the WebRes Gmail Apps Script (not in this repo: change its URL `?key=`, or better send header `X-CDP-Key` with `UrlFetchApp`'s `headers`), the Partywirks webhook URL (Partywirks settings), and the Tripleseat URL unless its signature is already enforced (then the URL has no key at all).
3. Watch the logs for a day: `Accepted … with WEBHOOK_SECRET_PREVIOUS` names any path still on the old key; `Rejected … wrong key` means a sender has neither.
4. When no `PREVIOUS` lines appear, delete `WEBHOOK_SECRET_PREVIOUS` in Render.
Rotation doesn't erase history: the old key stays readable in Render's log history until the 30-day retention expires; it just no longer authenticates anything.

**Moving Tripleseat off `?key=`.** Tripleseat can't send custom headers, so its own signature replaces the key: set `TRIPLESEAT_WEBHOOK_SECRET`; wait for `Tripleseat signature verified (scheme X)` on real deliveries; set `TRIPLESEAT_SIGNATURE_SCHEME=X` and `TRIPLESEAT_SIGNATURE_ENFORCE=true`; then remove `?key=…` from the webhook URL in Tripleseat.

7.2 Tests
`pip install -r requirements-dev.txt`, then `pytest tests/test_unit.py`. Integration tests also need `TEST_DATABASE_URL` set to a disposable Neon branch with migration 001 applied (they write test customers into it); all outside services are faked.

7.3 Partywirks
With `PARTYWIRKS_API_KEY` set, a Partywirks webhook is treated as a notification: the reservation (`GET /reservations/{order_id}`) and the parent (`GET /customers/{id}`, including street address) are read from the API. An order ID Partywirks doesn't know is ignored. If the API is down, the webhook body is used (flat `customer.address/city/state/zip_code`), but only for a real 10–20 digit order ID, so Partywirks' sample/test delivery (`PW-YYYYMM-NNNN`, John Doe) is never saved. Package = `activity.name`; party date = `event_date`. Each booking's real order total (`totals.total`) goes on its ledger row (`pw_<id>`, once per order) and into the customer's `pw_spend`: the Square deposit is usually not linked to the booker, so this is often their only recorded spend. Cancelled reservations update contact details only (no booking count, no `Qualified_Bowler`) and take a recorded order total back off `pw_spend`. `scripts/backfill_partywirks_spend.py` (dry run by default, `--apply` to write) adds the totals of bookings recorded before totals were stored, from the API's reservation list; an order without a total from the API is skipped, never estimated. Children's details (`event_name`, `customer_responses`, waivers) are never stored. The ledger row's `party_date` (migrations/013; NULL while unknown, moved when the party is rescheduled) is the party date, so a Tripleseat re-entry of the same party (staff copy birthday bookings into Tripleseat for BEOs) gets no second `Qualified_Bowler`: one QB per customer per party date, the first one wins, the other is logged as `DUPLICATE_QUALIFIED_BOWLER` (never retried). `scripts/backfill_partywirks_party_dates.py` (dry run by default) sets `party_date` on rows recorded before this.
`scripts/rescore_all.py` (dry run by default, `--apply` to write) recomputes every customer's persona and score with the current model through `rescore_customer`. It sends no Meta events and uploads no audiences. Run it after a scoring change.

`scripts/backfill_partywirks.py --count-since "<YYYY-MM-DD HH:MM:SS>"` records every reservation; bookings created before the cutoff (already counted by the original import) aren't counted again or sent to Meta.

7.4 Square items & history
Every live Square payment also reads its order (`GET /v2/orders/{order_id}`) and stores what was bought on its ledger row: `unified_transactions.line_items` (JSONB: `variation_id`, `item`, `variation`, `qty`, `gross`, `total`), `variation_ids` (price keys, joinable to `square_catalog_items` for departments) and `square_customer_id`. If the order lookup fails, the payment is still processed.
`scripts/backfill_square_history.py --since 2023-12-01` (dry run by default, `--apply` to write) records every historical payment and its items. It never changes spend or visits, because the import already holds Square's lifetime totals. Historical rows are never auto-credited later. The dry run also compares those totals with the real payment history. `--credit-between START END` credits payments in a window the system never counted, such as between the Square export and the live webhooks going live.
Every `/cron/*` endpoint shows a `key` box on the `/docs` page; enter `CRON_SECRET` there to use "Try it out".
When a brand-new customer is first linked to a Square customer ID (their next payment, or a Square profile update), their earlier payments under that ID, recorded by the history backfill and never counted, are credited to them: spend, visits and first-visit date. Existing customers gaining another Square ID get no such credit, because their imported total may already include those payments.
