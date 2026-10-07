from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql import SparkSession

# ============================================================
# SP2 - Cleaning & Standardization
# ============================================================
import os
os.environ["HADOOP_HOME"] = r"C:\Users\yuvasree.r\Desktop\python test\hadoop"
os.environ["PATH"] = os.environ["PATH"] + r";C:\Users\yuvasree.r\Desktop\python test\hadoop\bin"

spark = SparkSession.builder.appName("SP2_Cleaning").master("local[2]").getOrCreate()
raw_network_df =  spark.read.parquet(r"parquet_output\raw_parquet")


clean_network_df = (
    raw_network_df
    .withColumnRenamed("datetime", "timestamp")
    .withColumnRenamed("CellID", "grid_id")
    .withColumnRenamed("countrycode", "country_code")
    .withColumnRenamed("smsin", "sms_in")
    .withColumnRenamed("smsout", "sms_out")
    .withColumnRenamed("callin", "call_in")
    .withColumnRenamed("callout", "call_out")
    .withColumnRenamed("internet", "internet_activity")
)


# ------------------------------------------------------------
# 3. Cast timestamp and activity measures
# ------------------------------------------------------------

clean_network_df = (
    clean_network_df
    .withColumn(
        "timestamp",
        F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss")
    )
    .withColumn(
        "grid_id",
        F.col("grid_id").cast(IntegerType())
    )
    .withColumn(
        "sms_in",
        F.col("sms_in").cast(DoubleType())
    )
    .withColumn(
        "sms_out",
        F.col("sms_out").cast(DoubleType())
    )
    .withColumn(
        "call_in",
        F.col("call_in").cast(DoubleType())
    )
    .withColumn(
        "call_out",
        F.col("call_out").cast(DoubleType())
    )
    .withColumn(
        "internet_activity",
        F.col("internet_activity").cast(DoubleType())
    )
)


# ------------------------------------------------------------
# 4. Profile activity nulls BEFORE replacing them
# ------------------------------------------------------------

activity_columns = [
    "sms_in",
    "sms_out",
    "call_in",
    "call_out",
    "internet_activity"
]

print("\n========== NULL PROFILE BEFORE CLEANING ==========")

null_profile = clean_network_df.select([
    F.sum(
        F.col(c).isNull().cast("int")
    ).alias(c)
    for c in activity_columns
])

null_profile.show()


# Total activity null cells
total_null_activity_cells = 0

for c in activity_columns:
    total_null_activity_cells += (
        clean_network_df
        .filter(F.col(c).isNull())
        .count()
    )

print(
    "Total activity null cells:",
    total_null_activity_cells
)


# ------------------------------------------------------------
# 5. Identify rejected rows
# ------------------------------------------------------------
# A row is rejected if:
#   - grid_id is missing
#   - timestamp is missing
#   - any activity measure is negative
#
# IMPORTANT:
# Rejected rows are counted BEFORE null-to-zero handling.

negative_activity_condition = (

(F.col("sms_in").isNotNull() & (F.col("sms_in") < 0)) |

(F.col("sms_out").isNotNull() & (F.col("sms_out") < 0)) |

(F.col("call_in").isNotNull() & (F.col("call_in") < 0)) |

(F.col("call_out").isNotNull() & (F.col("call_out") < 0)) |

(F.col("internet_activity").isNotNull() & (F.col("internet_activity") < 0))

)

rejected_condition = (
    F.col("grid_id").isNull() |
    F.col("timestamp").isNull() |
    negative_activity_condition
)

rejected_df = clean_network_df.filter(
    rejected_condition
)

rejected_row_count = rejected_df.count()

print("\n========== REJECTED RECORD SUMMARY ==========")
print("Rejected rows:", rejected_row_count)


# ------------------------------------------------------------
# 6. Breakdown of rejected rows
# ------------------------------------------------------------

missing_grid_count = (
    clean_network_df
    .filter(F.col("grid_id").isNull())
    .count()
)

missing_timestamp_count = (
    clean_network_df
    .filter(F.col("timestamp").isNull())
    .count()
)

negative_activity_count = (
    clean_network_df
    .filter(negative_activity_condition)
    .count()
)

print("Missing grid_id:", missing_grid_count)
print("Missing timestamp:", missing_timestamp_count)
print("Rows with negative activity:", negative_activity_count)


# ------------------------------------------------------------
# 7. Remove rejected records
# ------------------------------------------------------------

clean_network_df = clean_network_df.filter(
    ~rejected_condition
)

# ------------------------------------------------------------
# 8. Apply curated-layer NULL -> ZERO rule
# ------------------------------------------------------------
# This is done ONLY after:
#   1. Raw data was preserved
#   2. Null activity values were profiled
#   3. Rejected rows were identified
#
# Missing activity measurements become 0.

for c in activity_columns:
    clean_network_df = clean_network_df.withColumn(
        c,
        F.coalesce(F.col(c), F.lit(0.0))
    )


# ------------------------------------------------------------
# 9. Create composite measures
# ------------------------------------------------------------

clean_network_df = (
    clean_network_df
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
        F.col("sms_in")
        + F.col("sms_out")
        + F.col("call_in")
        + F.col("call_out")
        + F.col("internet_activity")
    )
)


# ------------------------------------------------------------
# 10. Derive date, hour and day_of_week
# ------------------------------------------------------------

clean_network_df = (
    clean_network_df
    .withColumn(
        "date",
        F.to_date("timestamp")
    )
    .withColumn(
        "hour",
        F.hour("timestamp")
    )
    .withColumn(
        "day_of_week",
        F.dayofweek("timestamp")
    )
)


# ------------------------------------------------------------
# 11. Record counts before and after cleaning
# ------------------------------------------------------------

before_count = raw_network_df.count()
after_count = clean_network_df.count()

print("\n========== RECORD COUNT COMPARISON ==========")
print("Before cleaning:", before_count)
print("After cleaning:", after_count)
print("Rejected rows:", rejected_row_count)

print(
    "Count reconciliation:",
    before_count == after_count + rejected_row_count
)


# ------------------------------------------------------------
# 12. Null-handling report
# ------------------------------------------------------------

print("\n========== NULL-HANDLING REPORT ==========")
print(
    "Activity null cells handled by NULL -> 0:",
    total_null_activity_cells
)


# ------------------------------------------------------------
# 13. Verify timestamp cadence
# ------------------------------------------------------------

distinct_timestamps_after = (
    clean_network_df
    .select("timestamp")
    .distinct()
    .count()
)

file_count = (
    raw_network_df
    .select("input_file_name")
    .distinct()
    .count()
)

expected_timestamps = file_count * 24

print("\n========== TIMESTAMP CADENCE ==========")
print("Files:", file_count)
print("Expected timestamps:", expected_timestamps)
print("Actual timestamps after cleaning:", distinct_timestamps_after)

print(
    "Cadence check:",
    distinct_timestamps_after == expected_timestamps
)


# ------------------------------------------------------------
# 14. Verify required original activity columns remain
# ------------------------------------------------------------

required_activity_columns = [
    "sms_in",
    "sms_out",
    "call_in",
    "call_out",
    "internet_activity",
    "total_sms",
    "total_calls",
    "total_activity"
]

print("\n========== REQUIRED COLUMNS ==========")

for c in required_activity_columns:
    print(
        c,
        "->",
        c in clean_network_df.columns
    )


# ------------------------------------------------------------
# 15. Final schema
# ------------------------------------------------------------

print("\n========== CLEAN NETWORK SCHEMA ==========")
clean_network_df.printSchema()


# ------------------------------------------------------------
# 16. Sample output
# ------------------------------------------------------------

print("\n========== CLEAN NETWORK DATA ==========")

clean_network_df.show(
    10,
    truncate=False
)

clean_network_df.write.mode("overwrite").parquet(r"parquet_output\clean_parquet")