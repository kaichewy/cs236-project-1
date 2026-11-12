# CS 236 Project — Phase 1 Report: Data Preparation & EDA in Spark

**Team:** Kai Chew, Jordan Baltodano

**Video presentation:** https://drive.google.com/file/d/1eZ_vqKZeBLBBI6oPymPjIj5ZDjU6FVpF/view?usp=sharing

**Contributions:**
- **Kai Chew:** prepared, recorded and presented the 5-minute video presentation, including the
  walkthrough of the environment setup, the EDA findings, the schema comparison, the cleaning and
  standardization decisions, and the validation results. Co-wrote this report.
- **Jordan Baltodano:** co-wrote this report, covering the exploratory data analysis, the
  data-quality findings, the schema comparison and mappings, and the process used to build the
  unified dataset.
- **Both members:** discussed and agreed on every part of the phase together, including which EDA
  analyses to run and what they showed, how to handle each data-quality issue, how to map and
  standardize the status and channel values of the two cities, which attributes to derive, and
  how to validate the final datasets.

All numbers in this report come from `src/eda.py` (tables in `output/eda/eda_results.md`,
charts in `output/eda/figures/`) and `src/process.py` (`output/processed/validation_results.md`).

---

## 1. Environment

| Component | Version | Note |
|---|---|---|
| PySpark | 3.5.1 | `pip install pyspark==3.5.1`, run locally (`local[*]`) |
| Java | OpenJDK 17 | Spark 3.5 does not run on Java 21+; the machine's default Java 24 failed |
| Python | 3.11 (venv) | Python 3.13 is newer than PySpark 3.5.1 supports |
| Docker | 28.2.2 | installed and verified for Phase 2 |

The Spark driver is bound to `127.0.0.1`; without this Spark fails on macOS with
`BindException: Can't assign requested address`. Scripts are run with `./run.sh <script>`.

Both CSVs are read with `multiLine=True, escape='"'` because free-text fields contain quoted
commas/newlines, and with `inferSchema=False` so that every column starts as a string and we
control (and can count failures of) every type conversion.

---

## 2. Exploratory Data Analysis

### 2.1 Load & scan

| | NYC 311 | LA MyLA311 |
|---|---|---|
| Rows | 800,000 | 800,000 |
| Columns | 44 | 34 |
| Created range | 2026-07-22 → 2026-10-05 | 2024-01-01 → 2024-07-24 |
| Closed range | 2026-07-08 → 2026-10-05 | 2024-01-01 → 2025-10-16 |
| Unparseable timestamps | 0 | 0 |
| Numeric-looking columns | unique_key, incident_zip, council_district, bbl, x/y state plane, lat, lon | housenumber, zipcode, lat, lon, tbmpage, tbmrow, cd, nc |

**Insights.**
- **The two cities cover different, non-overlapping periods** (NYC ≈ 2.5 months in 2026, LA ≈ 7
  months in 2024). Any comparison between cities is a comparison of different seasons/years,
  and absolute volumes per month are not comparable. This is noted for Phase 2.
- NYC's minimum *closed* date (2026-07-08) is before its minimum *created* date → there are
  records closed before they were created (see §2.2).
- Timestamps in both files have the same ISO format (`yyyy-MM-ddTHH:mm:ss.SSS`, no time zone)
  and parse 100%, so a single parser can be used. They are kept as local wall-clock time of
  each city; no time-zone shift is applied because neither source provides an offset.
- Zip codes, council districts and coordinates look numeric. Zip codes are nevertheless kept as
  strings (they are labels, and leading zeros would be lost); districts become integers and
  coordinates doubles.

### 2.2 Data quality

**Identifiers & duplicates**

| | NYC | LA |
|---|---|---|
| Distinct IDs / rows | 800,000 / 800,000 | 800,000 / 800,000 |
| Null IDs | 0 | 0 |
| Exact duplicate rows | 0 | 0 |
| Duplicates ignoring the ID | 261 | 0 |
| ID collisions between cities | 0 (formats differ: `70643465` vs `1-4601720641`) | |

IDs are unique within each city. The 261 NYC rows identical in every column except the ID are
most likely the same issue reported twice by different people; since they are distinct service
requests in the city system, we **keep** them. Even though no collisions exist today, the two ID
spaces are independent, so the unified ID is prefixed with the city.

**Missing values** (nulls + blank strings + placeholder words such as `Unspecified`, `N/A`)

- NYC: many columns are specific to a few complaint types and are almost always empty:
  `park_facility_name` (100% `Unspecified`), taxi/bridge/road-ramp fields (>99%), `due_date`
  (99.6%), `facility_type` (99.7%), `vehicle_type` (95%). Core fields are well populated:
  `closed_date` 10.5% missing (open requests), `incident_zip` 0.9%, coordinates 1.8%.
  `open_data_channel_type` is `UNKNOWN` for 8.2%; `borough` is `Unspecified` for 940 rows.
- LA: `mobileos` 65% null (only app requests have it), `approximateaddress` 41%,
  `housenumber`/`streetname` 14% (intersections), `closeddate` 4.5%. Zip and coordinates are
  missing for < 0.1%.

**Invalid / malformed values**

| Check | NYC | LA |
|---|---|---|
| Zip not 5 digits | 1 (`No`) | 39 (`0` ×38, `VE, 0`) |
| 5-digit zip outside the city range | 16 (e.g. `06611`, `22222`, `00083`) | 0 |
| Coordinates outside city bounding box | 0 | 0 |

**Temporal and status consistency**

| Check | NYC | LA |
|---|---|---|
| Closed before created | 626 | 0 |
| Closed exactly at creation | 11,257 | 748 |
| Open > 1 year | 0 | 3,072 |
| Status `Closed` but no close date | 4 | 1,991 |
| Status not `Closed` but has a close date | 11,109 | 35,643 |

Drilling into the 626 negative NYC durations: 604 are DEP "Water Maintenance" requests, and
**591 of the 626 differ by less than 60 seconds**. DEP stores close times truncated to the
minute (`15:39:00` vs created `15:39:04`), so these are rounding artifacts of requests closed in
the same minute. Only 35 rows are genuinely inconsistent.

The "not closed but has a close date" rows are mostly LA `Cancelled` (34,711) and
`Referred Out` (932) requests, and NYC `In Progress`/`Open`/`Assigned` requests that were
probably reopened. Their close date does not mean the request was resolved.

**Consistency of categorical values**: no whitespace or case variants in status, channel or
agency in either city. NYC `complaint_type` has 184 distinct values but 182 after upper-casing:
`PLUMBING`/`Plumbing` and `ELEVATOR`/`Elevator` (HPD uses upper case, other agencies title case).

### 2.3 Distributions

Figures: `output/eda/figures/{nyc,la}_*.png`.

**Categorical**
- *Status*: NYC 88.1% Closed, 6.8% In Progress, 4.6% Open. LA 91.3% Closed, 4.3% Cancelled,
  2.3% Pending, 2.0% Open. The vocabularies differ (see §2.4).
- *Channel*: NYC ONLINE 52.4%, PHONE 20.4%, MOBILE 18.8%, UNKNOWN 8.2%. LA Call 39.3%,
  Mobile App 34.7%, Self Service 16.1%, Driver Self Report 9.5%, plus 11 rare values.
- *Agency*: NYC is spread over 14 agencies (NYPD 46.5%, HPD 17.0%, DSNY 9.0%, …). LA is
  dominated by sanitation: LASAN 74.2%, OCB (graffiti) 22.2%.
- *Request type*: NYC has 184 types; top are Illegal Parking 16.0%, Noise – Residential 9.7%,
  Noise – Street/Sidewalk 6.8%. LA has only 12 types; Bulky Items 47.3% and Graffiti Removal
  22.2% make up ~70%. → The type taxonomies are not comparable one-to-one; we keep each city's
  own types (Phase 2 analyses request types *within* a city).

**Temporal**
- NYC ≈ 10,500 requests/day, flat across the week (Fri highest at 122k, Sun lowest at 109k).
- LA ≈ 3,900 requests/day with a strong weekday pattern: Mon–Wed ≈ 148k each, Sat 64k.
  LA sanitation pickups are scheduled by phone/app during the work week.
- Month periods (preview of Phase 2): NYC days 22–end = 308k vs ≈150–190k for other periods.
  This is an artifact of coverage — NYC data starts on July 22 and ends on October 5 — so
  Phase 2 should interpret the NYC period comparison with this in mind. LA (full months
  Jan–Jun) is much more even: 183k / 193k / 188k / 236k (days 22–end has 8–10 days).

**Numerical — resolution time** (closed ≥ created, before cleaning)

| | NYC | LA |
|---|---|---|
| n | 715,268 | 763,778 |
| mean (h) | 89.3 | 229.0 |
| median (h) | 4.8 | 46.1 |
| p90 (h) | 287.7 | 172.3 |
| p99 (h) | 1,043.7 | 5,749.0 |

Distributions are extremely right-skewed (mean ≫ median), so Phase 2 should report medians
alongside means. LA streetlight requests have medians of 2,346 h (multiple lights) and 3,170 h
(single light), i.e. months; LA homeless encampments 475 h. NYC is fast mostly because NYPD
complaints (noise, parking) are closed within hours.

**Geographic**
- NYC: Brooklyn 32%, Queens 25%, Manhattan 20%, Bronx 19%, Staten Island 4%; 272 distinct zips,
  top zip 11226 (1.7%).
- LA: requests spread over 7 Area Planning Commissions (South LA, South Valley and Central ≈ 19%
  each); 142 distinct zips, top 90026 (2.5%).
- Scatter plots of coordinates reproduce the shapes of the two cities and no point is outside the
  bounding boxes, so coordinates are reliable.

### 2.4 Schema comparison

Only four column names are shared (`status`, `latitude`, `longitude`, `location`), but most core
concepts exist in both datasets under different names:

| Concept | NYC | LA | Difference / decision |
|---|---|---|---|
| Record ID | `unique_key` | `srnumber` | different formats → prefix with city |
| Created / closed | `created_date` / `closed_date` | `createddate` / `closeddate` | same format |
| Last update | `resolution_action_updated_date` | `updateddate` | similar meaning |
| Status | `status` (7 values) | `status` (6 values) | only Closed/Open/Pending shared → standardize |
| Channel | `open_data_channel_type` (5) | `requestsource` (15) | no value shared → standardize |
| Agency | `agency` + `agency_name` | `owner` (code only) | add LA name lookup |
| Request type | `complaint_type` (+`descriptor`) | `requesttype` | different taxonomies → keep original |
| Zip | `incident_zip` | `zipcode` | same meaning; validate |
| Address / street | `incident_address`, `street_name` | `address`, `streetname` | LA also splits house #/direction/suffix |
| Larger area | `borough` | `apc` | both ≈ 5–7 sub-regions of the city |
| Council district | `council_district` | `cd` | both integer |
| Police precinct | `police_precinct` (`Precinct 60`) | `policeprecinct` (`SOUTHWEST`) | kept as text |
| Coordinates | `latitude`, `longitude` | same | both decimal |
| Point text | `location` | `location` | redundant with lat/lon → dropped |

*NYC only*: descriptor(s), location_type, address_type, city, landmark, cross/intersection
streets, bbl, due_date, resolution_description, x/y state-plane coordinates, park/vehicle/taxi/
bridge fields.
*LA only*: actiontaken, anonymous, mobileos, assignto, servicedate, addressverified,
approximateaddress, house number/direction/suffix, Thomas Bros. map grid (tbm*), neighborhood
council (nc/ncname), council member, createdbyuserorganization.

---

## 3. Dataset Processing (`src/process.py`)

### 3.1 Cleaning rules

| # | Issue (from EDA) | Rule | Rows affected |
|---|---|---|---|
| 1 | Blank / whitespace strings | trim every string; `''` → NULL | — |
| 2 | Timestamps as strings | parse to `timestamp` (all 4 NYC and 4 LA date columns) | 0 failures |
| 3 | `Unspecified` used as missing | → NULL in NYC `borough`, `park_borough`, `park_facility_name` | 940 / 940 / 799,256 |
| 4 | Invalid zips | not matching the city's range (NYC `1[01]xxx`, LA `9[0-3]xxx`) → NULL | NYC 17, LA 39 |
| 5 | Coordinates as strings | cast to double; outside bounding box → NULL | 0 outside |
| 6 | Types | IDs/districts/map grid → int/long; LA Y/N flags → boolean | — |
| 7 | Duplicated IDs | keep the most recently updated row per ID (defensive; none found) | 0 |
| 8 | Redundant `location` text | dropped (same as lat/lon) | — |
| 9 | Case variants of NYC complaint types | new `complaint_type_std` = most frequent spelling; original kept | 1,538 |
| 10 | Closed < 60 s before created | minute truncation → resolution = 0 h | NYC 591 |
| 11 | Closed > 60 s before created | record kept, resolution = NULL | NYC 35 |
| 12 | Close date on non-closed requests | record kept, no resolution computed | NYC 11,109, LA 35,643 |

Rows are never dropped: all problems found affect individual attributes, not the validity of
the whole request, and dropping would bias counts in Phase 2.

### 3.2 Standardization

Original values are always kept in `*_raw` columns (unified) or original columns (cleaned), so
no information is lost.

**Status → `status`**

| Standard | NYC | LA |
|---|---|---|
| OPEN | Open | Open |
| IN_PROGRESS | In Progress, Assigned, Started | — |
| PENDING | Pending | Pending |
| CLOSED | Closed | Closed |
| CANCELLED | — | Cancelled |
| REFERRED | — | Referred Out, Forward |
| UNKNOWN | Unspecified | — |

Cancelled and referred requests were *not* merged into CLOSED: they were not resolved by the
city agency, and merging them would distort resolution-time statistics.

**Channel → `channel`**: LA's 15 sources are rolled up into NYC's coarser categories.

| Standard | NYC | LA |
|---|---|---|
| PHONE | PHONE | Call, Voicemail, Queue Initiated Customer Call |
| ONLINE | ONLINE | Self Service (website), Web Form, Social |
| MOBILE | MOBILE | Mobile App |
| EMAIL | — | Email |
| INTERNAL | — | Driver Self Report, Council's Office, City Attorney, Radio |
| OTHER | OTHER | Fax, Letter, Walk-in |
| UNKNOWN | UNKNOWN | — |

EMAIL and INTERNAL were added because they do not fit any NYC category; in particular
"Driver Self Report" (9.5% of LA) means city staff created the request, not a resident.

**Agency**: LA has only codes, so a lookup adds `agency_name` (LASAN → LA Sanitation and
Environment, OCB → Office of Community Beautification, BSL → Bureau of Street Lighting, …).

### 3.3 Derived attributes

| Attribute | Definition |
|---|---|
| `record_id` | `NYC-<unique_key>` or `LA-<srnumber>` — unique across both cities |
| `source_city` | `NYC` / `LA` |
| `resolution_hours` | `(closed − created)` in hours, only when status = CLOSED and a close time exists (rules 10–12) |
| `has_valid_resolution` | `resolution_hours IS NOT NULL` |
| `created_day`, `created_year`, `created_month`, `created_day_of_month` | from created timestamp |
| `month_period` | 1 = days 1–7, 2 = 8–14, 3 = 15–21, 4 = 22–end (for Phase 2) |

### 3.4 Unified schema

Each cleaned city dataset is projected onto this schema and combined with `unionByName`.

| Column | Type | NYC source | LA source |
|---|---|---|---|
| record_id | string (PK) | derived | derived |
| source_city | string | 'NYC' | 'LA' |
| source_record_id | string | unique_key | srnumber |
| created_at | timestamp | created_date | createddate |
| closed_at | timestamp | closed_date | closeddate |
| updated_at | timestamp | resolution_action_updated_date | updateddate |
| status / status_raw | string | standardized / status | standardized / status |
| channel / channel_raw | string | standardized / open_data_channel_type | standardized / requestsource |
| agency_code | string | agency | owner |
| agency_name | string | agency_name | lookup |
| request_type | string | complaint_type_std | requesttype |
| request_descriptor | string | descriptor | NULL |
| address | string | incident_address | address |
| street_name | string | street_name | streetname |
| zip_code | string | incident_zip | zipcode |
| area | string | borough | apc |
| council_district | int | council_district | cd |
| police_precinct | string | police_precinct | policeprecinct |
| latitude, longitude | double | latitude, longitude | latitude, longitude |
| resolution_hours | double | derived | derived |
| has_valid_resolution | boolean | derived | derived |
| created_day | date | derived | derived |
| created_year, created_month, created_day_of_month, month_period | int | derived | derived |

Source-only attributes stay in the cleaned city datasets rather than being added as mostly-NULL
columns to the unified table.

### 3.5 Persisted outputs

`output/processed/{nyc_cleaned, la_cleaned, unified}/` — each as Parquet (keeps types, used by
Phase 2) and a single CSV. Cleaned city datasets keep all original columns (minus `location`)
plus the standardized and derived columns.

### 3.6 Validation

The script re-reads the written Parquet files and checks them; all 28 checks pass
(`output/processed/validation_results.md`):

- Record counts: NYC 800,000, LA 800,000, unified 1,600,000 (= sum, 800,000 per city).
- `record_id` unique and non-null in all three datasets.
- No NULLs in unified `record_id, source_city, source_record_id, created_at, status, channel,
  agency_code, request_type`.
- `resolution_hours ≥ 0`, equals `closed_at − created_at`, and exists only for CLOSED requests.
- `month_period ∈ {1..4}`; zip codes are 5 digits or NULL.

Resolution time after cleaning (valid, CLOSED requests): NYC n = 704,767, mean 88.9 h,
median 4.8 h; LA n = 728,135, mean 229.4 h, median 46.5 h.

---

## 4. Implications for Phase 2

- City periods differ (NYC Jul–Oct 2026, LA Jan–Jul 2024): compare *patterns*, not volumes.
- NYC's partial first and last months inflate "days 22–end" in the weekly-volume analysis.
- Use `zip_code` (NULL for 0.9% NYC / 0.1% LA) for hotspots; use `has_valid_resolution` to
  select completed requests; resolution times are heavily skewed, so report medians too.
- The unified schema maps directly onto PostgreSQL types (`varchar`, `timestamp`, `int`,
  `double precision`, `boolean`, `date`) with `record_id` as primary key.
