from pyspark.sql import SparkSession, functions as F, Window

spark = SparkSession.builder.appName("phase2-weekly_request_volume").getOrCreate()

df = (spark.read.csv("../output/processed/unified/unified.csv", header=True, inferSchema=True)
      .select("record_id", "source_city", "month_period")
      .dropna(subset=["source_city", "month_period"]))


# # this calculates the weekly request volume per city, for every (year,month,period) time frame/window
# this isnt needed/used, i misunderstood the analysis questions, kept just in case
# by_window = (df.groupBy("source_city", "created_year", "created_month", "period")
#              .agg(F.count("*").alias("n_requests")))


# this calculates the weekly request volume per city, grouped by period number (1-4) across all months
by_period = (df.groupBy("source_city", "month_period").agg(F.count("*").alias("n_requests")))


"""
Input: The weekly request volume (counts), the associated columns (month_period)
Output: The period with the highest and lowest request volume for each city.
Uses PySpark Window to partition by the two cities then filter to get the highest and lowest by rank.
"""
def extremes(counts, group_cols):
    w_hi = Window.partitionBy("source_city").orderBy(F.desc("n_requests"))
    w_lo = Window.partitionBy("source_city").orderBy(F.asc("n_requests"))
    ranked = (counts.withColumn("rk_hi", F.row_number().over(w_hi))
                    .withColumn("rk_lo", F.row_number().over(w_lo)))
    hi = ranked.filter("rk_hi=1").withColumn("extreme", F.lit("highest"))
    lo = ranked.filter("rk_lo=1").withColumn("extreme", F.lit("lowest"))
    return (hi.unionByName(lo)
            .select("source_city", "extreme", *group_cols, "n_requests")
            .orderBy("source_city", "extreme"))

# print
extremes(by_period, ["month_period"]).show(truncate=False)