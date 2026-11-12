"""Phase 1.3 - Clean both datasets, build the unified dataset, and validate it.

Run:  ./run.sh src/process.py

Outputs (each as Parquet, plus a single CSV file for easy inspection):
    output/processed/nyc_cleaned/
    output/processed/la_cleaned/
    output/processed/unified/
    output/processed/validation_results.md

Every cleaning rule below is motivated by a finding in src/eda.py; the
comment above each rule names the issue it fixes. Counts of rows affected by
each rule are collected in CLEANING_LOG and written to the validation report.
"""
import shutil

from pyspark.sql import DataFrame, Window, functions as F, types as T

from common import LA_RAW, NYC_RAW, PROCESSED_DIR, get_spark, read_raw

TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS"
NYC_BBOX = (40.47, 40.93, -74.27, -73.68)
LA_BBOX = (33.68, 34.35, -118.70, -118.13)
NYC_ZIP_RE = r"^1[01]\d{3}$"  # NYC zips are 100xx-119xx
LA_ZIP_RE = r"^9[0-3]\d{3}$"  # LA zips are 90xxx-93xxx

CLEANING_LOG = []  # (city, rule, rows affected)

# ----------------------------------------------------------------- standardization maps
# Status: the two cities use different words for the same lifecycle stage.
# We map them to a small shared set and keep the original in status_raw.
STATUS_MAP = {
    "NYC": {
        "Closed": "CLOSED",
        "Open": "OPEN",
        "In Progress": "IN_PROGRESS",
        "Assigned": "IN_PROGRESS",  # assigned to a crew = work has started
        "Started": "IN_PROGRESS",
        "Pending": "PENDING",
        "Unspecified": "UNKNOWN",
    },
    "LA": {
        "Closed": "CLOSED",
        "Open": "OPEN",
        "Pending": "PENDING",
        "Cancelled": "CANCELLED",
        "Referred Out": "REFERRED",  # handed to another agency
        "Forward": "REFERRED",
    },
}

# Submission channel: NYC already has 5 coarse values; LA has 15 fine-grained
# sources. LA values are rolled up into the NYC-style categories, with EMAIL
# and INTERNAL (requests created by city staff) added because they do not fit
# any NYC category. channel_raw keeps the original value.
CHANNEL_MAP = {
    "NYC": {"ONLINE": "ONLINE", "PHONE": "PHONE", "MOBILE": "MOBILE", "OTHER": "OTHER", "UNKNOWN": "UNKNOWN"},
    "LA": {
        "Call": "PHONE",
        "Voicemail": "PHONE",
        "Queue Initiated Customer Call": "PHONE",
        "Mobile App": "MOBILE",
        "Self Service": "ONLINE",  # MyLA311 website
        "Web Form": "ONLINE",
        "Social": "ONLINE",
        "Email": "EMAIL",
        "Driver Self Report": "INTERNAL",  # sanitation drivers logging issues
        "Council's Office": "INTERNAL",
        "City Attorney": "INTERNAL",
        "Radio": "INTERNAL",
        "Fax": "OTHER",
        "Letter": "OTHER",
        "Walk-in": "OTHER",
    },
}

# LA only provides agency codes, NYC provides code + full name. A lookup gives
# LA the same two attributes.
LA_AGENCY_NAMES = {
    "LASAN": "LA Sanitation and Environment",
    "OCB": "Office of Community Beautification",
    "BSL": "Bureau of Street Lighting",
    "ITA": "Information Technology Agency",
    "LADWP": "Department of Water and Power",
    "BSS": "Bureau of Street Services",
    "LADOT": "Department of Transportation",
    "RAP": "Department of Recreation and Parks",
    "LADBS": "Department of Building and Safety",
    "LAAS": "Department of Animal Services",
    "BOE": "Bureau of Engineering",
}


# ----------------------------------------------------------------- generic helpers
def map_values(col: str, mapping: dict, default: str):
    """Build a CASE WHEN expression from a python dict."""
    expr = None
    for k, v in mapping.items():
        expr = F.when(F.col(col) == k, v) if expr is None else expr.when(F.col(col) == k, v)
    return expr.otherwise(F.lit(default) if default is not None else F.col(col))


def log(city: str, rule: str, n: int) -> None:
    CLEANING_LOG.append((city, rule, n))
    print(f"[{city}] {rule}: {n:,}")


def trim_and_blank_to_null(df: DataFrame) -> DataFrame:
    """Issue: some string fields contain only whitespace. Trim all strings and
    turn empty strings into real NULLs so missingness is represented one way."""
    return df.select(
        [F.when(F.trim(F.col(c)) == "", None).otherwise(F.trim(F.col(c))).alias(c) for c in df.columns]
    )


def placeholders_to_null(df: DataFrame, city: str, cols: list, values: list) -> DataFrame:
    """Issue: 'Unspecified' / '0' are used as 'missing' markers in some columns."""
    for c in cols:
        n = df.filter(F.col(c).isin(values)).count()
        if n:
            log(city, f"placeholder {values} -> NULL in {c}", n)
        df = df.withColumn(c, F.when(F.col(c).isin(values), None).otherwise(F.col(c)))
    return df


def parse_timestamps(df: DataFrame, cols: list) -> DataFrame:
    """Issue: timestamps are stored as ISO strings. Convert to TimestampType."""
    for c in cols:
        df = df.withColumn(c, F.to_timestamp(c, TS_FORMAT))
    return df


def clean_zip(df: DataFrame, city: str, col: str, regex: str) -> DataFrame:
    """Issue: a few zip codes are malformed ('No', '0', 'VE, 0') or outside the
    city's zip range. They cannot be repaired reliably, so they become NULL.
    Zip stays a string: leading zeros matter and zips are labels, not numbers."""
    bad = F.col(col).isNotNull() & ~F.col(col).rlike(regex)
    log(city, f"invalid {col} -> NULL", df.filter(bad).count())
    return df.withColumn(col, F.when(bad, None).otherwise(F.col(col)))


def clean_coordinates(df: DataFrame, city: str, bbox: tuple) -> DataFrame:
    """Issue: coordinates are strings and could be outside the city. Cast to
    double; if either value is outside the bounding box, NULL both."""
    la0, la1, lo0, lo1 = bbox
    df = df.withColumn("latitude", F.col("latitude").cast("double")).withColumn(
        "longitude", F.col("longitude").cast("double")
    )
    bad = F.col("latitude").isNotNull() & (
        ~F.col("latitude").between(la0, la1) | ~F.col("longitude").between(lo0, lo1)
    )
    log(city, "coordinates outside city bounding box -> NULL", df.filter(bad).count())
    for c in ["latitude", "longitude"]:
        df = df.withColumn(c, F.when(bad, None).otherwise(F.col(c)))
    return df


def deduplicate(df: DataFrame, city: str, id_col: str, order_col: str) -> DataFrame:
    """Issue check: identifiers must be unique. EDA found none duplicated, but the
    rule is kept so the pipeline stays correct on new extracts: for a repeated
    id we keep the most recently updated version."""
    before = df.count()
    w = Window.partitionBy(id_col).orderBy(F.col(order_col).desc_nulls_last())
    df = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    df = df.filter(F.col(id_col).isNotNull())
    log(city, "duplicate / null ids removed", before - df.count())
    return df


def add_derived(df: DataFrame, city: str, created: str, closed: str) -> DataFrame:
    """Derived attributes needed in Phase 2.

    resolution_hours: (closed - created) in hours, defined only for requests
      whose standardized status is CLOSED and that have a close time. A
      resolution time is not meaningful for open/cancelled/referred requests
      even if they carry a close date (e.g. NYC 'In Progress' rows that were
      reopened after closing).
      NYC DEP truncates close times to the minute, so a request closed within
      the same minute it was created can appear up to 59 s "before" creation.
      Those gaps are rounding artifacts and are set to 0 hours. Larger negative
      gaps are genuinely inconsistent: the record is kept (other attributes
      are fine) but resolution_hours is NULL.
    has_valid_resolution: TRUE when resolution_hours is defined.
    month_period: 1 = days 1-7, 2 = 8-14, 3 = 15-21, 4 = 22-end of month.
    """
    gap_s = F.unix_timestamp(closed) - F.unix_timestamp(created)
    truncated = gap_s.between(-59, -1)
    invalid = gap_s < -59
    log(city, "closed < 60 s before created (minute truncation, resolution = 0)", df.filter(truncated).count())
    log(city, "closed > 60 s before created (resolution set NULL)", df.filter(invalid).count())
    completed = (F.col("status_std") == "CLOSED") & F.col(closed).isNotNull()
    log(city, "close date present but status not CLOSED (no resolution)",
        df.filter(F.col(closed).isNotNull() & (F.col("status_std") != "CLOSED")).count())
    dom = F.dayofmonth(created)
    return (
        df.withColumn(
            "resolution_hours",
            F.when(~completed | invalid, None)
            .when(truncated, 0.0)
            .otherwise(F.round(gap_s / 3600.0, 4)),
        )
        .withColumn("has_valid_resolution", F.col("resolution_hours").isNotNull())
        .withColumn("created_day", F.to_date(created))
        .withColumn("created_year", F.year(created))
        .withColumn("created_month", F.month(created))
        .withColumn("created_day_of_month", dom)
        .withColumn(
            "month_period",
            F.when(dom <= 7, 1).when(dom <= 14, 2).when(dom <= 21, 3).otherwise(4).cast("int"),
        )
    )


# ----------------------------------------------------------------- city-specific cleaning
def clean_nyc(spark) -> DataFrame:
    city = "NYC"
    df = trim_and_blank_to_null(read_raw(spark, NYC_RAW))
    log(city, "raw rows", df.count())

    df = parse_timestamps(df, ["created_date", "closed_date", "due_date", "resolution_action_updated_date"])
    # 'Unspecified' is used as a missing marker in these location columns.
    df = placeholders_to_null(df, city, ["borough", "park_borough", "park_facility_name"], ["Unspecified"])
    df = clean_zip(df, city, "incident_zip", NYC_ZIP_RE)
    df = clean_coordinates(df, city, NYC_BBOX)
    df = (
        df.withColumn("unique_key", F.col("unique_key").cast("long"))
        .withColumn("council_district", F.col("council_district").cast("int"))
        .withColumn("x_coordinate_state_plane", F.col("x_coordinate_state_plane").cast("int"))
        .withColumn("y_coordinate_state_plane", F.col("y_coordinate_state_plane").cast("int"))
        # 'location' is the POINT(lon lat) text of latitude/longitude -> redundant.
        .drop("location")
    )
    df = deduplicate(df, city, "unique_key", "resolution_action_updated_date")

    # Issue: complaint_type has spellings that differ only by case
    # ('PLUMBING'/'Plumbing', 'ELEVATOR'/'Elevator'). complaint_type_std uses the
    # most frequent spelling within each case-insensitive group; the original
    # complaint_type is kept unchanged.
    spellings = df.groupBy(F.upper("complaint_type").alias("_u"), "complaint_type").count()
    w = Window.partitionBy("_u").orderBy(F.desc("count"), "complaint_type")
    canonical = spellings.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1") \
        .select("_u", F.col("complaint_type").alias("complaint_type_std"))
    df = df.withColumn("_u", F.upper("complaint_type")).join(F.broadcast(canonical), "_u", "left").drop("_u")
    log(city, "complaint_type case variants merged in complaint_type_std",
        df.filter(F.col("complaint_type") != F.col("complaint_type_std")).count())

    df = (
        df.withColumn("status_std", map_values("status", STATUS_MAP[city], "UNKNOWN"))
        .withColumn("channel_std", map_values("open_data_channel_type", CHANNEL_MAP[city], "UNKNOWN"))
        .withColumn("source_city", F.lit(city))
        .withColumn("record_id", F.concat(F.lit("NYC-"), F.col("unique_key").cast("string")))
    )
    return add_derived(df, city, "created_date", "closed_date")


def clean_la(spark) -> DataFrame:
    city = "LA"
    df = trim_and_blank_to_null(read_raw(spark, LA_RAW))
    log(city, "raw rows", df.count())

    df = parse_timestamps(df, ["createddate", "updateddate", "servicedate", "closeddate"])
    df = clean_zip(df, city, "zipcode", LA_ZIP_RE)
    df = clean_coordinates(df, city, LA_BBOX)
    yn = lambda c: F.when(F.col(c) == "Y", True).when(F.col(c) == "N", False)  # noqa: E731
    df = (
        # Y/N flags become real booleans.
        df.withColumn("anonymous", yn("anonymous"))
        .withColumn("addressverified", yn("addressverified"))
        .withColumn("approximateaddress", yn("approximateaddress"))
        .withColumn("cd", F.col("cd").cast("int"))
        .withColumn("nc", F.col("nc").cast("int"))
        .withColumn("tbmpage", F.col("tbmpage").cast("int"))
        .withColumn("tbmrow", F.col("tbmrow").cast("int"))
        .drop("location")
    )
    df = deduplicate(df, city, "srnumber", "updateddate")

    agency_name = map_values("owner", LA_AGENCY_NAMES, None)
    df = (
        df.withColumn("status_std", map_values("status", STATUS_MAP[city], "UNKNOWN"))
        .withColumn("channel_std", map_values("requestsource", CHANNEL_MAP[city], "OTHER"))
        .withColumn("agency_name", agency_name)
        .withColumn("source_city", F.lit(city))
        .withColumn("record_id", F.concat(F.lit("LA-"), F.col("srnumber")))
    )
    return add_derived(df, city, "createddate", "closeddate")


# ----------------------------------------------------------------- unification
# Unified schema: one row per service request from either city. Column order
# here is the order of the output. (unified column, NYC source, LA source);
# None means the attribute does not exist in that city and is NULL there.
UNIFIED_COLUMNS = [
    ("record_id", "record_id", "record_id"),
    ("source_city", "source_city", "source_city"),
    ("source_record_id", "unique_key", "srnumber"),
    ("created_at", "created_date", "createddate"),
    ("closed_at", "closed_date", "closeddate"),
    ("updated_at", "resolution_action_updated_date", "updateddate"),
    ("status", "status_std", "status_std"),
    ("status_raw", "status", "status"),
    ("channel", "channel_std", "channel_std"),
    ("channel_raw", "open_data_channel_type", "requestsource"),
    ("agency_code", "agency", "owner"),
    ("agency_name", "agency_name", "agency_name"),
    ("request_type", "complaint_type_std", "requesttype"),
    ("request_descriptor", "descriptor", None),
    ("address", "incident_address", "address"),
    ("street_name", "street_name", "streetname"),
    ("zip_code", "incident_zip", "zipcode"),
    ("area", "borough", "apc"),
    ("council_district", "council_district", "cd"),
    ("police_precinct", "police_precinct", "policeprecinct"),
    ("latitude", "latitude", "latitude"),
    ("longitude", "longitude", "longitude"),
    ("resolution_hours", "resolution_hours", "resolution_hours"),
    ("has_valid_resolution", "has_valid_resolution", "has_valid_resolution"),
    ("created_day", "created_day", "created_day"),
    ("created_year", "created_year", "created_year"),
    ("created_month", "created_month", "created_month"),
    ("created_day_of_month", "created_day_of_month", "created_day_of_month"),
    ("month_period", "month_period", "month_period"),
]


def to_unified(df: DataFrame, city: str) -> DataFrame:
    idx = 1 if city == "NYC" else 2
    cols = []
    for spec in UNIFIED_COLUMNS:
        name, src = spec[0], spec[idx]
        expr = F.lit(None).cast("string") if src is None else F.col(src)
        if name == "source_record_id":
            expr = expr.cast("string")  # NYC key is a long, LA's is a string
        cols.append(expr.alias(name))
    return df.select(cols)


def reorder(df: DataFrame) -> DataFrame:
    """Put record_id/source_city first in the cleaned per-city outputs."""
    first = ["record_id", "source_city"]
    return df.select(first + [c for c in df.columns if c not in first])


# ----------------------------------------------------------------- persistence
def write(df: DataFrame, name: str) -> None:
    out = PROCESSED_DIR / name
    shutil.rmtree(out, ignore_errors=True)
    df.write.mode("overwrite").parquet(str(out / "parquet"))
    # Single CSV file for graders / spreadsheets.
    tmp = out / "_csv_tmp"
    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .csv(str(tmp))
    )
    part = next(tmp.glob("part-*.csv"))
    part.rename(out / f"{name}.csv")
    shutil.rmtree(tmp)
    print(f"wrote {out}")


# ----------------------------------------------------------------- validation
def validate(spark, expected: dict) -> str:
    """Re-read the persisted Parquet files and check them."""
    checks = []
    lines = []  # detail sections, appended after the checks table

    def check(name, ok, detail=""):
        checks.append(f"| {'PASS' if ok else 'FAIL'} | {name} | {detail} |")

    for name in ["nyc_cleaned", "la_cleaned", "unified"]:
        df = spark.read.parquet(str(PROCESSED_DIR / name / "parquet"))
        n = df.count()
        check(f"{name}: record count", n == expected[name], f"{n:,} (expected {expected[name]:,})")
        distinct = df.select("record_id").distinct().count()
        check(f"{name}: record_id unique", distinct == n, f"{distinct:,} distinct")
        check(f"{name}: record_id not null", df.filter(F.col("record_id").isNull()).count() == 0)
        bad_res = df.filter(F.col("resolution_hours") < 0).count()
        check(f"{name}: resolution_hours >= 0", bad_res == 0, f"{bad_res} negative")
        bad_period = df.filter(~F.col("month_period").between(1, 4)).count()
        check(f"{name}: month_period in 1..4", bad_period == 0)
        if name == "unified":
            lines += ["\n## Unified schema\n", "```", df._jdf.schema().treeString(), "```"]
            per_city = {r["source_city"]: r["count"] for r in df.groupBy("source_city").count().collect()}
            check("unified: rows per city", per_city == {"NYC": expected["nyc_cleaned"], "LA": expected["la_cleaned"]},
                  str(per_city))
            for c in ["record_id", "source_city", "source_record_id", "created_at", "status", "channel",
                      "agency_code", "request_type"]:
                nulls = df.filter(F.col(c).isNull()).count()
                check(f"unified: {c} has no NULLs", nulls == 0, f"{nulls} nulls")
            mismatch = df.filter(F.col("has_valid_resolution") != F.col("resolution_hours").isNotNull()).count()
            check("unified: has_valid_resolution consistent with resolution_hours", mismatch == 0)
            not_closed = df.filter(F.col("resolution_hours").isNotNull() & (F.col("status") != "CLOSED")).count()
            check("unified: resolution_hours only for CLOSED requests", not_closed == 0, f"{not_closed} violations")
            recompute = df.filter(
                (F.col("resolution_hours") > 0)
                & (F.abs((F.unix_timestamp("closed_at") - F.unix_timestamp("created_at")) / 3600
                         - F.col("resolution_hours")) > 0.001)
            ).count()
            check("unified: resolution_hours equals closed_at - created_at", recompute == 0)
            check("unified: zip_code is 5 digits or NULL",
                  df.filter(F.col("zip_code").isNotNull() & ~F.col("zip_code").rlike(r"^\d{5}$")).count() == 0)
            for c in ["status", "channel"]:
                vals = df.groupBy("source_city", c).count().orderBy("source_city", F.desc("count")).toPandas()
                lines += [f"\n## {c} distribution after standardization\n", "```", vals.to_string(index=False), "```"]
            stats = df.filter("has_valid_resolution").groupBy("source_city").agg(
                F.count("*").alias("n"), F.round(F.avg("resolution_hours"), 2).alias("avg_h"),
                F.round(F.percentile_approx("resolution_hours", 0.5), 2).alias("median_h"),
            ).toPandas()
            lines += ["\n## Resolution time (valid resolutions)\n", "```", stats.to_string(index=False), "```"]
            lines += ["\n## Sample unified rows\n", "```", df._jdf.showString(4, 40, True), "```"]

    head = ["# Validation results (generated by src/process.py)\n", "## Cleaning log\n",
            "| city | rule | rows |", "|---|---|---|"]
    head += [f"| {c} | {r} | {n:,} |" for c, r, n in CLEANING_LOG]
    head += ["\n## Checks\n", "| result | check | detail |", "|---|---|---|"] + checks
    return "\n".join(head + lines)


def main() -> None:
    spark = get_spark("cs236-process")
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    nyc = reorder(clean_nyc(spark)).cache()
    la = reorder(clean_la(spark)).cache()
    counts = {"nyc_cleaned": nyc.count(), "la_cleaned": la.count()}

    # Merge: same column names and types, so unionByName is safe.
    unified = to_unified(nyc, "NYC").unionByName(to_unified(la, "LA"))
    counts["unified"] = counts["nyc_cleaned"] + counts["la_cleaned"]

    write(nyc, "nyc_cleaned")
    write(la, "la_cleaned")
    write(unified, "unified")

    report = validate(spark, counts)
    (PROCESSED_DIR / "validation_results.md").write_text(report)
    print(report)
    spark.stop()


if __name__ == "__main__":
    main()
