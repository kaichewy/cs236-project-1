"""Phase 1.2 - Exploratory Data Analysis of the NYC 311 and LA MyLA311 datasets.

Run:  ./run.sh src/eda.py

Every analysis below is computed with PySpark on the full 800k-row files.
Only small aggregated results are converted to pandas, for printing and for
matplotlib figures. Results are written to:
    output/eda/eda_results.md   - all tables, in the order they are computed
    output/eda/figures/*.png    - charts used in the report

Each section starts with a short "why" note explaining what the analysis is
meant to reveal and how it feeds into the cleaning/integration decisions.
"""
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from pyspark.sql import DataFrame, functions as F  # noqa: E402

from common import EDA_DIR, FIG_DIR, LA_RAW, NYC_RAW, get_spark, read_raw  # noqa: E402

# Per-city settings: which raw columns play which role. Keeping this in one
# place lets every analysis run identically on both datasets.
CITIES = {
    "NYC": {
        "path": NYC_RAW,
        "id": "unique_key",
        "created": "created_date",
        "closed": "closed_date",
        "status": "status",
        "channel": "open_data_channel_type",
        "agency": "agency",
        "type": "complaint_type",
        "zip": "incident_zip",
        "area": "borough",
        # Rough bounding box of the five boroughs.
        "bbox": (40.47, 40.93, -74.27, -73.68),
        "zip_regex": r"^1[01]\d{3}$",
    },
    "LA": {
        "path": LA_RAW,
        "id": "srnumber",
        "created": "createddate",
        "closed": "closeddate",
        "status": "status",
        "channel": "requestsource",
        "agency": "owner",
        "type": "requesttype",
        "zip": "zipcode",
        "area": "apc",
        # Rough bounding box of the City of Los Angeles.
        "bbox": (33.68, 34.35, -118.70, -118.13),
        "zip_regex": r"^9[0-3]\d{3}$",
    },
}

TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS"
_report = []


# --------------------------------------------------------------------------- helpers
def section(title: str, why: str = "") -> None:
    print(f"\n## {title}")
    _report.append(f"\n## {title}\n")
    if why:
        _report.append(f"*Why:* {why}\n")


def emit(label: str, df_or_text, n: int = 50) -> None:
    """Print a small Spark DataFrame (or text) and append it to the report."""
    if isinstance(df_or_text, DataFrame):
        text = df_or_text.limit(n).toPandas().to_string(index=False)
    else:
        text = str(df_or_text)
    print(f"### {label}\n{text}")
    _report.append(f"\n### {label}\n```\n{text}\n```\n")


def value_counts(df: DataFrame, col: str) -> DataFrame:
    total = df.count()
    return (
        df.groupBy(col)
        .count()
        .withColumn("pct", F.round(F.col("count") * 100 / total, 2))
        .orderBy(F.desc("count"))
    )


def bar(pdf, x: str, y: str, title: str, fname: str, horizontal: bool = False) -> None:
    fig, ax = plt.subplots(figsize=(9, 5))
    labels = pdf[x].fillna("NULL").astype(str)
    if horizontal:
        ax.barh(labels[::-1], pdf[y][::-1])
    else:
        ax.bar(labels, pdf[y])
        plt.xticks(rotation=45, ha="right")
    ax.set_title(title)
    fig.tight_layout()
    fig.savefig(FIG_DIR / fname, dpi=120)
    plt.close(fig)


def with_timestamps(df: DataFrame, cfg: dict) -> DataFrame:
    return df.withColumn("_created", F.to_timestamp(cfg["created"], TS_FORMAT)).withColumn(
        "_closed", F.to_timestamp(cfg["closed"], TS_FORMAT)
    )


# --------------------------------------------------------------------------- 1. load & scan
def load_and_scan(city: str, df: DataFrame, cfg: dict) -> None:
    section(
        f"{city}: load & scan",
        "Establish size, columns and raw types before any decision is made. All columns are "
        "read as strings so that type problems show up as parse failures below.",
    )
    emit("Dimensions", f"rows = {df.count():,}   columns = {len(df.columns)}")
    emit("Schema (raw)", "\n".join(f"{f.name}: {f.dataType.simpleString()}" for f in df.schema))
    emit("Representative records (3 rows, vertical)", df._jdf.showString(3, 60, True))

    ts = with_timestamps(df, cfg)
    emit(
        "Timestamp parse check & date range",
        ts.select(
            F.sum((F.col(cfg["created"]).isNotNull() & F.col("_created").isNull()).cast("int")).alias(
                "created_unparseable"
            ),
            F.sum((F.col(cfg["closed"]).isNotNull() & F.col("_closed").isNull()).cast("int")).alias(
                "closed_unparseable"
            ),
            F.min("_created").cast("string").alias("min_created"),
            F.max("_created").cast("string").alias("max_created"),
            F.min("_closed").cast("string").alias("min_closed"),
            F.max("_closed").cast("string").alias("max_closed"),
        ),
    )
    # Which raw string columns actually look numeric? Useful to decide the
    # target SQL/Spark type of each column.
    numeric_checks = [
        F.round(
            F.sum(F.col(c).rlike(r"^-?\d+(\.\d+)?$").cast("int")) * 100 / F.count(c), 1
        ).alias(c)
        for c in df.columns
    ]
    row = df.select(numeric_checks).collect()[0].asDict()
    numeric_like = {k: v for k, v in row.items() if v and v > 95}
    emit("Columns whose non-null values are >95% numeric", numeric_like)


# --------------------------------------------------------------------------- 2. data quality
def data_quality(city: str, df: DataFrame, cfg: dict) -> None:
    section(
        f"{city}: data quality",
        "Quantify missing values, duplicates, invalid values and temporal inconsistencies. "
        "Each issue found here maps to a cleaning rule in process.py.",
    )
    total = df.count()

    # Missing values: true nulls, empty/whitespace strings and placeholder words.
    placeholders = ["unspecified", "n/a", "na", "unknown", "none", "null", "0"]
    rows = []
    for c in df.columns:
        rows.append(
            F.struct(
                F.lit(c).alias("column"),
                F.sum(F.col(c).isNull().cast("int")).alias("null"),
                F.sum((F.trim(F.col(c)) == "").cast("int")).alias("blank"),
                F.sum(F.lower(F.trim(F.col(c))).isin(placeholders).cast("int")).alias("placeholder"),
            )
        )
    missing = df.select(F.explode(F.array(*rows)).alias("r")).select("r.*")
    missing = missing.withColumn(
        "missing_pct", F.round((F.col("null") + F.col("blank") + F.col("placeholder")) * 100 / total, 2)
    ).orderBy(F.desc("missing_pct"))
    emit("Missing / blank / placeholder values per column", missing, n=100)
    mp = missing.toPandas()
    bar(mp, "column", "missing_pct", f"{city}: % missing per column", f"{city.lower()}_missing.png", True)

    # Identifier uniqueness and duplicate records.
    idc = cfg["id"]
    non_id_cols = [c for c in df.columns if c != idc]
    emit(
        "Identifier uniqueness and duplicates",
        f"rows = {total:,}\n"
        f"distinct {idc} = {df.select(idc).distinct().count():,}\n"
        f"null {idc} = {df.filter(F.col(idc).isNull()).count():,}\n"
        f"exact duplicate rows = {total - df.distinct().count():,}\n"
        f"duplicates ignoring {idc} = {total - df.select(non_id_cols).distinct().count():,}",
    )

    # Invalid / malformed values.
    lat, lon = F.col("latitude").cast("double"), F.col("longitude").cast("double")
    la0, la1, lo0, lo1 = cfg["bbox"]
    emit(
        "Invalid or malformed values",
        df.select(
            F.sum((F.col(cfg["zip"]).isNotNull() & ~F.col(cfg["zip"]).rlike(r"^\d{5}$")).cast("int")).alias(
                "zip_not_5_digits"
            ),
            F.sum(
                (F.col(cfg["zip"]).rlike(r"^\d{5}$") & ~F.col(cfg["zip"]).rlike(cfg["zip_regex"])).cast("int")
            ).alias("zip_outside_city_range"),
            F.sum((F.col("latitude").isNotNull() & lat.isNull()).cast("int")).alias("lat_unparseable"),
            F.sum((lat.isNotNull() & ~lat.between(la0, la1)).cast("int")).alias("lat_outside_bbox"),
            F.sum((lon.isNotNull() & ~lon.between(lo0, lo1)).cast("int")).alias("lon_outside_bbox"),
        ),
    )
    emit(
        "Examples of zip codes that are not 5 digits / outside city range",
        df.filter(~F.col(cfg["zip"]).rlike(cfg["zip_regex"]))
        .groupBy(cfg["zip"])
        .count()
        .orderBy(F.desc("count")),
        n=15,
    )

    # Temporal consistency between created/closed timestamps and status.
    ts = with_timestamps(df, cfg)
    is_closed = F.lower(F.col(cfg["status"])) == "closed"
    emit(
        "Temporal / status consistency",
        ts.select(
            F.sum((F.col("_closed") < F.col("_created")).cast("int")).alias("closed_before_created"),
            F.sum((F.col("_closed") == F.col("_created")).cast("int")).alias("closed_equals_created"),
            F.sum(
                ((F.unix_timestamp("_closed") - F.unix_timestamp("_created")) > 365 * 86400).cast("int")
            ).alias("open_more_than_1_year"),
            F.sum((is_closed & F.col("_closed").isNull()).cast("int")).alias("status_closed_no_close_date"),
            F.sum((~is_closed & F.col("_closed").isNotNull()).cast("int")).alias("not_closed_but_has_close_date"),
            F.sum((F.date_format("_created", "HH:mm:ss") == "00:00:00").cast("int")).alias("created_exactly_midnight"),
        ),
    )
    emit(
        "Status vs presence of a close date",
        ts.groupBy(cfg["status"])
        .agg(F.count("*").alias("rows"), F.sum(F.col("_closed").isNotNull().cast("int")).alias("with_close_date"))
        .orderBy(F.desc("rows")),
    )
    emit(
        "Sample of records closed before they were created",
        ts.filter(F.col("_closed") < F.col("_created")).select(
            idc, cfg["created"], cfg["closed"], cfg["status"], cfg["agency"], cfg["type"]
        ),
        n=5,
    )

    # Case / whitespace inconsistencies inside key categorical columns.
    for c in [cfg["status"], cfg["channel"], cfg["agency"], cfg["type"]]:
        raw = df.select(c).distinct().count()
        norm = df.select(F.upper(F.trim(F.col(c)))).distinct().count()
        emit(f"Case/whitespace variants in {c}", f"distinct raw = {raw}, distinct after trim+upper = {norm}")


# --------------------------------------------------------------------------- 3. distributions
def distributions(city: str, df: DataFrame, cfg: dict) -> None:
    section(
        f"{city}: distributions",
        "Understand the categorical, temporal, numerical and geographic shape of the data. This "
        "shows which categories need standardizing and what the Phase 2 analyses will look like.",
    )
    tag = city.lower()

    for key, label, top in [("status", "Status", 20), ("channel", "Submission channel", 20),
                            ("agency", "Agency / owner", 20), ("type", "Request type (top 20)", 20),
                            ("area", "Borough / APC", 20)]:
        vc = value_counts(df, cfg[key])
        emit(f"{label}: {cfg[key]} (distinct = {df.select(cfg[key]).distinct().count()})", vc, n=top)
        bar(vc.limit(top).toPandas(), cfg[key], "count", f"{city}: {label}", f"{tag}_{key}.png", True)

    zips = value_counts(df, cfg["zip"])
    emit(f"Top 15 zip codes (distinct = {df.select(cfg['zip']).distinct().count()})", zips, n=15)

    # Temporal distributions.
    ts = with_timestamps(df, cfg)
    daily = ts.groupBy(F.to_date("_created").alias("day")).count().orderBy("day").toPandas()
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(daily["day"], daily["count"])
    ax.set_title(f"{city}: requests created per day")
    fig.tight_layout()
    fig.savefig(FIG_DIR / f"{tag}_daily.png", dpi=120)
    plt.close(fig)
    emit("Requests per day (summary)", daily["count"].describe().round(1).to_string())

    monthly = ts.groupBy(F.date_format("_created", "yyyy-MM").alias("month")).count().orderBy("month")
    emit("Requests per month", monthly)
    dow = ts.groupBy(F.dayofweek("_created").alias("dow_1sun"), F.date_format("_created", "E").alias("day")) \
        .count().orderBy("dow_1sun")
    emit("Requests by day of week", dow)
    bar(dow.toPandas(), "day", "count", f"{city}: by day of week", f"{tag}_dow.png")
    hour = ts.groupBy(F.hour("_created").alias("hour")).count().orderBy("hour")
    bar(hour.toPandas(), "hour", "count", f"{city}: by hour of creation", f"{tag}_hour.png")
    period = ts.withColumn(
        "period",
        F.when(F.dayofmonth("_created") <= 7, "1: days 1-7")
        .when(F.dayofmonth("_created") <= 14, "2: days 8-14")
        .when(F.dayofmonth("_created") <= 21, "3: days 15-21")
        .otherwise("4: days 22-end"),
    ).groupBy("period").count().orderBy("period")
    emit("Requests by month period (preview of Phase 2 analysis)", period)

    # Numerical: resolution time in hours for rows with a valid close date.
    res = ts.withColumn("hours", (F.unix_timestamp("_closed") - F.unix_timestamp("_created")) / 3600) \
        .filter(F.col("hours") >= 0)
    q = res.approxQuantile("hours", [0.0, 0.25, 0.5, 0.75, 0.9, 0.99, 1.0], 0.001)
    emit(
        "Resolution time in hours (closed >= created only)",
        f"n = {res.count():,}  mean = {res.agg(F.avg('hours')).first()[0]:.1f}\n"
        + "  ".join(f"p{int(p * 100)}={v:.1f}" for p, v in zip([0, .25, .5, .75, .9, .99, 1], q)),
    )
    hp = res.select("hours").filter(F.col("hours") <= q[5]).sample(0.2, seed=1).toPandas()
    fig, ax = plt.subplots(figsize=(9, 4))
    ax.hist(hp["hours"], bins=80)
    ax.set_title(f"{city}: resolution time in hours (up to p99, 20% sample)")
    fig.tight_layout()
    fig.savefig(FIG_DIR / f"{tag}_resolution_hist.png", dpi=120)
    plt.close(fig)
    emit(
        "Median resolution hours for the 10 most common request types",
        res.groupBy(cfg["type"])
        .agg(F.count("*").alias("n"), F.round(F.percentile_approx("hours", 0.5), 1).alias("median_h"))
        .orderBy(F.desc("n")),
        n=10,
    )

    # Geographic: scatter of a sample of coordinates.
    geo = df.select(F.col("latitude").cast("double").alias("lat"), F.col("longitude").cast("double").alias("lon")) \
        .dropna().sample(0.05, seed=1).toPandas()
    fig, ax = plt.subplots(figsize=(6, 6))
    ax.scatter(geo["lon"], geo["lat"], s=0.2, alpha=0.3)
    ax.set_title(f"{city}: request locations (5% sample)")
    fig.tight_layout()
    fig.savefig(FIG_DIR / f"{tag}_geo.png", dpi=120)
    plt.close(fig)


# --------------------------------------------------------------------------- 4. schema comparison
SCHEMA_MAPPING = [
    # (concept, NYC column, LA column, note)
    ("record identifier", "unique_key", "srnumber", "NYC numeric string vs LA '1-XXXXXXXXXX'; ranges may collide -> prefix with city"),
    ("creation timestamp", "created_date", "createddate", "same ISO format"),
    ("closing timestamp", "closed_date", "closeddate", "same ISO format"),
    ("last update", "resolution_action_updated_date", "updateddate", "similar meaning"),
    ("status", "status", "status", "different vocabularies -> standardize"),
    ("submission channel", "open_data_channel_type", "requestsource", "5 vs 15 values -> standardize"),
    ("responsible agency (code)", "agency", "owner", "codes differ per city; keep code"),
    ("agency full name", "agency_name", "-", "LA only has codes -> add lookup table"),
    ("request type", "complaint_type", "requesttype", "~200 NYC types vs 12 LA types; keep original"),
    ("request sub-type", "descriptor", "-", "NYC only"),
    ("postal code", "incident_zip", "zipcode", "both strings; validate 5 digits"),
    ("full address", "incident_address", "address", "LA address also contains ', zip'"),
    ("street name", "street_name", "streetname", "LA splits house/direction/street/suffix"),
    ("larger area", "borough", "apc", "borough vs Area Planning Commission"),
    ("council district", "council_district", "cd", "both integer-like"),
    ("police precinct", "police_precinct", "policeprecinct", "'Precinct 60' vs 'SOUTHWEST'"),
    ("latitude / longitude", "latitude/longitude", "latitude/longitude", "both decimal strings"),
    ("point geometry", "location", "location", "redundant with lat/lon"),
    ("resolution text", "resolution_description", "actiontaken", "different granularity"),
    ("NYC-only", "location_type, address_type, city, landmark, bbl, due_date, x/y state plane, park_*, vehicle/taxi/bridge_*", "-", ""),
    ("LA-only", "-", "anonymous, mobileos, assignto, servicedate, addressverified, approximateaddress, tbm*, nc/ncname, cdmember, createdbyuserorganization", ""),
]


def schema_comparison(dfs: dict) -> None:
    section(
        "Schema comparison",
        "Identify overlapping attributes, naming/type differences and source-only attributes. "
        "This table is the basis of the unified schema built in process.py.",
    )
    nyc, la = dfs["NYC"], dfs["LA"]
    common = sorted(set(nyc.columns) & set(la.columns))
    emit("Columns with identical names", ", ".join(common))
    emit("Column counts", f"NYC = {len(nyc.columns)}, LA = {len(la.columns)}")
    width = max(len(m[0]) for m in SCHEMA_MAPPING)
    emit(
        "Manual concept mapping (NYC -> LA)",
        "\n".join(f"{c:<{width}} | {n} | {l} | {note}" for c, n, l, note in SCHEMA_MAPPING),
    )
    for key in ["status", "channel"]:
        a = sorted(r[0] or "NULL" for r in nyc.select(CITIES["NYC"][key]).distinct().collect())
        b = sorted(r[0] or "NULL" for r in la.select(CITIES["LA"][key]).distinct().collect())
        emit(f"{key} vocabularies", f"NYC: {a}\nLA:  {b}\nshared exactly: {sorted(set(a) & set(b))}")
    # Does any NYC id collide with an LA id once both are treated as strings?
    collide = nyc.select(F.col("unique_key").alias("id")).intersect(la.select(F.col("srnumber").alias("id"))).count()
    emit("Raw identifier collisions between cities", collide)


def main() -> None:
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    spark = get_spark("cs236-eda")
    dfs = {}
    for city, cfg in CITIES.items():
        df = read_raw(spark, cfg["path"]).cache()
        dfs[city] = df
        load_and_scan(city, df, cfg)
        data_quality(city, df, cfg)
        distributions(city, df, cfg)
    schema_comparison(dfs)

    (EDA_DIR / "eda_results.md").write_text("# EDA results (generated by src/eda.py)\n" + "".join(_report))
    print(f"\nWrote {EDA_DIR / 'eda_results.md'} and figures in {FIG_DIR}")
    spark.stop()


if __name__ == "__main__":
    main()
