from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

# ============================================================
# SP3 - Network Activity Aggregations
# ============================================================

import os
os.environ["HADOOP_HOME"] = r"C:\Users\yuvasree.r\Desktop\python test\hadoop"
os.environ["PATH"] = os.environ["PATH"] + r";C:\Users\yuvasree.r\Desktop\python test\hadoop\bin"
# ------------------------------------------------------------
# 1. Preserve count before aggregation
# ------------------------------------------------------------

spark = SparkSession.builder.appName("SP3_Aggr").master("local[2]").getOrCreate()
clean_network_df= spark.read.parquet(r"parquet_output\clean_parquet")
clean_count = clean_network_df.count()

print("\n========== INPUT ==========")
print("clean_network_df rows:", clean_count)


# ------------------------------------------------------------
# 2. Collapse country-code grain
# ------------------------------------------------------------
# BEFORE:
#   one row = country_code + grid_id + timestamp
#
# AFTER:
#   one row = grid_id + timestamp
#
# All country-code records belonging to the same
# grid and hour are summed together.

hourly_grid_summary = (
    clean_network_df
    .groupBy(
        "grid_id",
        "timestamp"
    )
    .agg(
        F.sum("sms_in").alias("sms_in"),
        F.sum("sms_out").alias("sms_out"),
        F.sum("call_in").alias("call_in"),
        F.sum("call_out").alias("call_out"),
        F.sum("internet_activity").alias("internet_activity")
    )
)


# ------------------------------------------------------------
# 3. Create total SMS, total calls and total activity
# ------------------------------------------------------------

hourly_grid_summary = (
    hourly_grid_summary
    .withColumn(
        "total_sms",
        F.col("sms_in") + F.col("sms_out")
    )
    .withColumn(
        "total_calls",
        F.col("call_in") + F.col("call_out")
    )
    .withColumn(
        "total_activity",
        F.col("total_sms")
        + F.col("total_calls")
        + F.col("internet_activity")
    )
)


# ------------------------------------------------------------
# 4. Internet share of total activity
# ------------------------------------------------------------
# Avoid division by zero.

hourly_grid_summary = hourly_grid_summary.withColumn(
    "internet_share",
    F.when(
        F.col("total_activity") > 0,
        F.col("internet_activity") / F.col("total_activity")
    ).otherwise(F.lit(0.0))
)


# ------------------------------------------------------------
# 5. Derive date
# ------------------------------------------------------------

hourly_grid_summary = hourly_grid_summary.withColumn(
    "date",
    F.to_date("timestamp")
)


# ------------------------------------------------------------
# 6. Make the column order clear
# ------------------------------------------------------------

hourly_grid_summary = hourly_grid_summary.select(
    "timestamp",
    "grid_id",
    "sms_in",
    "sms_out",
    "call_in",
    "call_out",
    "internet_activity",
    "total_sms",
    "total_calls",
    "total_activity",
    "internet_share",
    "date"
)


# ============================================================
# GRAIN VALIDATION
# ============================================================


# ------------------------------------------------------------
# 7. Zero duplicate grid/hour records
# ------------------------------------------------------------
# This assertion MUST remain in the code.
#
# Exactly one row must exist for every:
#       grid_id + timestamp

duplicate_count = (
    hourly_grid_summary
    .groupBy("grid_id", "timestamp")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

assert duplicate_count == 0, (
    f"Grain validation failed: {duplicate_count} "
    "duplicate grid/hour groups found."
)

print("\nGrain validation: PASSED")


# ------------------------------------------------------------
# 8. Check country_code is NOT present
# ------------------------------------------------------------

assert "country_code" not in hourly_grid_summary.columns, (
    "country_code must not appear in hourly_grid_summary"
)

print("country_code validation: PASSED")


# ------------------------------------------------------------
# 9. Check hourly row count is smaller
# ------------------------------------------------------------

hourly_count = hourly_grid_summary.count()

assert hourly_count < clean_count, (
    "hourly_grid_summary row count must be strictly "
    "less than clean_network_df row count."
)

print("Row reduction validation: PASSED")


# ------------------------------------------------------------
# 10. Maximum possible row count
# ------------------------------------------------------------
# Maximum theoretical grain:
#
# D files × 24 hours × 10,000 grids

D = (
    clean_network_df
    .select("input_file_name")
    .distinct()
    .count()
)

maximum_rows = D * 24 * 10000

assert hourly_count <= maximum_rows, (
    f"Row count {hourly_count} exceeds maximum "
    f"allowed count {maximum_rows}"
)

print("Maximum row-count validation: PASSED")


# ============================================================
# HAND CHECK
# ============================================================


# ------------------------------------------------------------
# 11. Hand-check one grid/hour
# ------------------------------------------------------------
# Select one actual grid/hour from the data and reproduce
# the aggregation manually using Spark filtering.
#
# This validates that groupBy + sum produced the exact result.

sample_key = (
    clean_network_df
    .select("grid_id", "timestamp")
    .first()
)

sample_grid = sample_key["grid_id"]
sample_timestamp = sample_key["timestamp"]


print("\n========== HAND CHECK ==========")
print("Grid:", sample_grid)
print("Timestamp:", sample_timestamp)


# Original country-code-level records
original_rows = (
    clean_network_df
    .filter(
        (F.col("grid_id") == sample_grid) &
        (F.col("timestamp") == sample_timestamp)
    )
)


# Manually reproduce aggregation
manual_result = (
    original_rows
    .agg(
        F.sum("sms_in").alias("sms_in"),
        F.sum("sms_out").alias("sms_out"),
        F.sum("call_in").alias("call_in"),
        F.sum("call_out").alias("call_out"),
        F.sum("internet_activity").alias("internet_activity")
    )
    .first()
)


# Aggregated result from hourly_grid_summary
spark_result = (
    hourly_grid_summary
    .filter(
        (F.col("grid_id") == sample_grid) &
        (F.col("timestamp") == sample_timestamp)
    )
    .first()
)


# Compare every aggregated measure
assert manual_result["sms_in"] == spark_result["sms_in"]
assert manual_result["sms_out"] == spark_result["sms_out"]
assert manual_result["call_in"] == spark_result["call_in"]
assert manual_result["call_out"] == spark_result["call_out"]
assert (
    manual_result["internet_activity"]
    == spark_result["internet_activity"]
)

print("Hand-check: PASSED")


# ============================================================
# DAILY TRAFFIC SUMMARY
# ============================================================


# ------------------------------------------------------------
# 12. Daily activity per grid
# ------------------------------------------------------------
# Grain:
#
#       one row = date + grid_id

daily_traffic_summary = (
    hourly_grid_summary
    .groupBy(
        "date",
        "grid_id"
    )
    .agg(
        F.sum("sms_in").alias("sms_in"),
        F.sum("sms_out").alias("sms_out"),
        F.sum("call_in").alias("call_in"),
        F.sum("call_out").alias("call_out"),
        F.sum("internet_activity").alias("internet_activity"),
        F.sum("total_sms").alias("total_sms"),
        F.sum("total_calls").alias("total_calls"),
        F.sum("total_activity").alias("total_activity")
    )
)


# ============================================================
# TOP 10 HOTSPOT GRIDS
# ============================================================


# ------------------------------------------------------------
# 13. Overall top 10 high-activity grids
# ------------------------------------------------------------
# Sum activity across all available dates/hours.

top_10_hotspots = (
    hourly_grid_summary
    .groupBy("grid_id")
    .agg(
        F.sum("total_activity").alias("total_activity")
    )
    .orderBy(
        F.col("total_activity").desc()
    )
    .limit(10)
)

print("\n========== TOP 10 HOTSPOT GRIDS ==========")
top_10_hotspots.show(truncate=False)


# ------------------------------------------------------------
# 14. Top 10 grids for each date
# ------------------------------------------------------------
# This uses a window function.
#
# rank = 1 means the highest-activity grid for that date.

daily_grid_activity = (
    hourly_grid_summary
    .groupBy(
        "date",
        "grid_id"
    )
    .agg(
        F.sum("total_activity").alias("total_activity")
    )
)


ranking_window = Window.partitionBy(
    "date"
).orderBy(
    F.col("total_activity").desc()
)


daily_hotspot_ranking = (
    daily_grid_activity
    .withColumn(
        "rank",
        F.row_number().over(ranking_window)
    )
    .filter(
        F.col("rank") <= 10
    )
    .orderBy(
        "date",
        "rank"
    )
)


print("\n========== DAILY TOP 10 HOTSPOTS ==========")
daily_hotspot_ranking.show(
    100,
    truncate=False
)


# ============================================================
# PEAK ACTIVITY HOUR
# ============================================================


# ------------------------------------------------------------
# 15. Find the peak activity hour
# ------------------------------------------------------------
# Aggregate activity across all grids for each timestamp.

peak_activity_hour = (
    hourly_grid_summary
    .groupBy("timestamp")
    .agg(
        F.sum("total_activity").alias("total_activity")
    )
    .orderBy(
        F.col("total_activity").desc()
    )
    .limit(1)
)

print("\n========== PEAK ACTIVITY HOUR ==========")
peak_activity_hour.show(truncate=False)


# ============================================================
# FINAL OUTPUTS
# ============================================================


print("\n========== HOURLY GRID SUMMARY ==========")

hourly_grid_summary.show(
    10,
    truncate=False
)

print("\nHourly grid summary rows:", hourly_count)
print("Clean network rows:", clean_count)
print("Number of files:", D)

print("\n========== DAILY TRAFFIC SUMMARY ==========")

daily_traffic_summary.show(
    10,
    truncate=False
)

hourly_grid_summary.write.mode("overwrite").parquet(r"parquet_output\hourly_grid_parquet")

spark.stop
print("over")