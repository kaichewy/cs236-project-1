# Phase 1 Video Script (~5 minutes)

🖥 = what to show on screen · quoted text = what to say

## Setup before recording

- **Terminal** open in the project folder: `cd /Users/kaichew/Downloads/CS236-Project`
- **Editor tabs:** `run.sh`, `output/eda/eda_results.md`, `docs/Phase1_Report.md` (Markdown preview), `src/process.py`, `output/processed/validation_results.md`
- **Finder:** `output/eda/figures/` (select a PNG, press **Space** to preview)
- **Docker Desktop** running
- Increase font size in Terminal and editor (**Cmd +**)
- Record with **Cmd + Shift + 5** → Record Entire Screen → Options → choose microphone
- In VS Code, jump to a line with **Ctrl + G**

---

## Part 1 — Intro (0:00–0:25)

🖥 **Finder → `CS236-Project` folder** (shows `src`, `output`, `docs`, dataset folder)
> "Hi, I'm [name]. This is Phase 1 of the CS 236 project."

🖥 **Finder → open `CS236-Fall26-Project-Dataset`** (the two CSVs)
> "We have two 311 datasets, 800,000 rows from New York City and 800,000 from Los Angeles. They describe similar requests but have different schemas and values."

🖥 **`docs/Phase1_Report.md` → top of the file (title)**
> "In this phase I set up Spark, explored both datasets, then cleaned and merged them into one unified dataset."

## Part 2 — Environment (0:25–0:55)

🖥 **Terminal → `docker --version`**
> "Docker is installed and ready for Phase 2."

🖥 **Terminal → `.venv/bin/pip show pyspark`**
> "PySpark 3.5.1 is installed locally in a Python virtual environment."

🖥 **Editor → `run.sh`**
> "Spark 3.5 needs Java 17, so this script sets Java 17, uses the project's Python, and binds Spark to localhost to avoid a networking error on macOS. I run every script through it."

## Part 3 — Running the EDA (0:55–1:15)

🖥 **Terminal → `./run.sh src/eda.py`** (let progress lines scroll ~10 seconds)
> "This is the EDA script. Spark reads both full CSVs, splits them into partitions and processes them in parallel. I read every column as a string first so I can count values that fail to convert."

🖥 **Terminal → Ctrl + C to stop**
> "It takes a few minutes, so here are the results from a completed run."

## Part 4 — EDA findings (1:15–2:35)

🖥 **`output/eda/eda_results.md` → line 200** (NYC "Timestamp parse check & date range")
> "All timestamps parsed. NYC covers July 22 to October 5, 2026…"

🖥 **`output/eda/eda_results.md` → line 666** (LA, same table)
> "…and LA covers January to July 2024. Since the periods are different, we compare patterns, not raw volumes."

🖥 **`output/eda/eda_results.md` → line 263** ("Identifier uniqueness and duplicates")
> "IDs are unique in both datasets and there are no duplicate rows."

🖥 **Finder → `nyc_missing.png`**
> "Many NYC columns, like taxi and bridge fields, only apply to a few complaint types, so they're almost always empty. The core fields are well populated."

🖥 **`output/eda/eda_results.md` → line 298** ("Temporal / status consistency"), then **line 316** (sample records)
> "626 NYC requests look like they were closed before they were created. In these samples, the close times all end in zero seconds. They're from DEP, which rounds close times down to the minute. 591 of the 626 are less than a minute off, so only 35 are actually wrong."

🖥 **Finder → `la_dow.png`, then `nyc_dow.png`**
> "LA requests peak Monday to Wednesday and drop on weekends. NYC is flat across the week."

🖥 **Finder → `la_resolution_hist.png`**
> "Resolution time is very skewed. In NYC the median is under 5 hours but the mean is almost 90. LA streetlight requests take months, so medians matter here."

🖥 **Finder → `nyc_geo.png`, then `la_geo.png`**
> "All coordinates fall inside each city, so the location data is reliable."

## Part 5 — Schema comparison (2:35–3:15)

🖥 **`docs/Phase1_Report.md` → line 162** (section 2.4 table, scroll slowly)
> "Only four column names are the same, but most concepts exist in both under different names, like `unique_key` versus `srnumber`, `complaint_type` versus `requesttype`, and `borough` versus `apc`."

> "Status values only share three words, and the submission channels share none. NYC uses ONLINE and PHONE, while LA has 15 values like 'Call' and 'Mobile App'. LA also only has agency codes, not names. This table is what I used to design the unified schema."

## Part 6 — Cleaning and standardizing (3:15–4:20)

🖥 **`src/process.py` → line 32** (`STATUS_MAP`)
> "Here's the processing script. Both cities' statuses map to one set: OPEN, IN_PROGRESS, PENDING, CLOSED, CANCELLED, REFERRED and UNKNOWN. I kept Cancelled and Referred separate from Closed because those requests weren't actually resolved."

🖥 **`src/process.py` → line 56** (`CHANNEL_MAP`)
> "LA's 15 channels are grouped into NYC's categories, plus EMAIL and INTERNAL. INTERNAL is for requests created by city staff, like 'Driver Self Report'. The original values are kept in raw columns, so nothing is lost."

🖥 **`docs/Phase1_Report.md` → line 195** (section 3.1, Cleaning rules table)
> "The other cleaning rules are here. Blanks and 'Unspecified' become NULL, invalid zip codes become NULL, which is 17 in NYC and 39 in LA, and dates and numbers get proper types. I also merged duplicate spellings like 'PLUMBING' and 'Plumbing'."

🖥 **`docs/Phase1_Report.md` → line 253** (section 3.3, Derived attributes)
> "I added a unique record ID with a city prefix, a source city column, and resolution time in hours, only for closed requests, with the minute-rounding cases set to zero. I didn't drop any rows, because every problem affected single fields, not whole requests."

## Part 7 — Outputs and validation (4:20–5:00)

🖥 **Terminal → `ls output/processed`**
> "The script saves three datasets: cleaned NYC, cleaned LA, and the unified dataset with 1.6 million rows, each as Parquet and CSV."

🖥 **`output/processed/validation_results.md` → "Checks" table**
> "It then reads the saved files back and runs 28 checks. Record counts match, IDs are unique, required fields have no missing values, and resolution times are valid. All 28 pass."

🖥 **`docs/Phase1_Report.md` → line 320** (section 4, Implications for Phase 2)
> "The unified dataset is ready for Phase 2, where I'll run the analyses and load it into PostgreSQL. Thanks for watching."

**Stop recording** (stop button in the menu bar; the video saves to the Desktop).

---

## Tips

- Do one practice run. If you're over 5:00, trim Part 4 first.
- Glance at the script rather than reading word for word.
- If you stumble, keep going.
- Line numbers are for the current files. If you edit `Phase1_Report.md` above those sections, they'll shift.
