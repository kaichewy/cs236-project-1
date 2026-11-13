from pyspark.sql import SparkSession, functions as F, Window

spark = SparkSession.builder.appName("phase2-service-request-hotspots").getOrCreate()

df = (spark.read.csv("../output/processed/unified/unified.csv", header=True, inferSchema=True)
      .select("record_id", "source_city", "zip_code", "request_type")
      .dropna(subset=["source_city", "zip_code", "request_type"]))


# the zip codes should be fine from inspection (just in case)
# df = df.withColumn(
#     "zip_code",
#     F.substring(F.regexp_replace(F.col("zip_code").cast("string"), r"\.0$", ""), 1, 5)
# )

# this calculates the top 10 zip codes per city by total requests by
# first aggregating the amount of requests per zip code per city. Afterwards,
# for each city, we simply sort then filter to find the top 10.
zip_counts = (df.groupBy("source_city", "zip_code").agg(F.count("*").alias("zip_total")))

w_zip = Window.partitionBy("source_city").orderBy(F.desc("zip_total"), F.asc("zip_code"))
top_zips = (zip_counts.withColumn("zip_rank", F.row_number().over(w_zip)).filter("zip_rank <= 10"))


# this gives the counts of the request types within the top-10 zips. To avoid ties, (zip_code, request_type)
# were added. It was not specified what to do for ties, so this was done just in case to prevent them and to
# not assume they dont exist.
type_counts = (df.join(top_zips.select("source_city", "zip_code", "zip_total", "zip_rank"), on=["source_city", "zip_code"], how="inner")
                 .groupBy("source_city", "zip_code", "zip_rank", "zip_total", "request_type")
                 .agg(F.count("*").alias("type_count")))


# afterwards, for the top 3 request types per city (city, zip), we calculate the percentages
# (at this point we already have the counts). Totals should be non-zero so no division errors.
w_type = Window.partitionBy("source_city", "zip_code").orderBy(F.desc("type_count"),F.asc("request_type"))

result = (type_counts.withColumn("type_rank", F.row_number().over(w_type))
                     .filter("type_rank <= 3")
                     .withColumn("pct_of_zip", F.round(100 * F.col("type_count") / F.col("zip_total"), 2))
                     .select("source_city", "zip_rank", "zip_code", "zip_total", "type_rank", "request_type", "type_count", "pct_of_zip")
                     .orderBy("source_city", "zip_rank", "type_rank"))

# print
result.show(60, truncate=False)