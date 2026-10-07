from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import input_file_name, col, count, countDistinct
import os
import glob
import json
from datetime import datetime

os.environ["HADOOP_HOME"] = r"C:\Users\yuvasree.r\Desktop\python test\hadoop"
os.environ["PATH"] = os.environ["PATH"] + r";C:\Users\yuvasree.r\Desktop\python test\hadoop\bin"

# ============================================================
# SP1 - Distributed Ingestion
# ============================================================

DATA_FOLDER = "data/landing"
FILE_PATTERN = os.path.join(DATA_FOLDER, "sms-call-internet-mi-*.csv")

# ------------------------------------------------------------
# 1. Create SparkSession
# ------------------------------------------------------------

spark = SparkSession.builder.appName("SP1_Distributed_Ingestion").master("local[2]").getOrCreate()

# ------------------------------------------------------------
# 2. Identify actual input files
# ------------------------------------------------------------

actual_files = glob.glob(FILE_PATTERN)
print("\nFiles found:")
for file in actual_files:
    print(os.path.basename(file))
print("\nActual file count:", len(actual_files))

# ------------------------------------------------------------
# 3. Trainer-provided manual schema
# ------------------------------------------------------------

# inferred_df = (
#     spark.read
#     .option("header", True)
#     .option("inferSchema", True)
#     .csv(FILE_PATTERN)
# )

manual_schema = StructType([
    StructField("datetime", StringType(), True),
    StructField("CellId", IntegerType(), True),
    StructField("countrycode", StringType(), True),
    StructField("smsin", DoubleType(), True),
    StructField("smsout", DoubleType(), True),
    StructField("callin", DoubleType(), True),
    StructField("callout", DoubleType(), True),
    StructField("internet", DoubleType(), True)
])

# ------------------------------------------------------------
# 4. Read all daily files as ONE distributed DataFrame
# ------------------------------------------------------------

raw_network_df = spark.read.option("header", True).schema(manual_schema).csv(FILE_PATTERN)

# ------------------------------------------------------------
# 5. Add source-file traceability
# ------------------------------------------------------------

raw_network_df = raw_network_df.withColumn("input_file_name", input_file_name())

# ------------------------------------------------------------
# 6. Display schema
# ------------------------------------------------------------

print("\n========== VALIDATED SCHEMA ==========")
raw_network_df.printSchema()

# ------------------------------------------------------------
# 7. Basic row count
# ------------------------------------------------------------

row_count = raw_network_df.count()
print("\n========== BASIC COUNTS ==========")
print("Total rows:", row_count)

# ------------------------------------------------------------
# 8. File count from Spark DataFrame
# ------------------------------------------------------------

spark_file_count = raw_network_df.select("input_file_name").distinct().count()
print("Files reported by Spark:", spark_file_count)
print("Actual files in folder:", len(actual_files))

# ------------------------------------------------------------
# 9. Unique grids
# ------------------------------------------------------------

unique_grids = raw_network_df.select("CellID").distinct().count()
print("Unique grids:", unique_grids)

# ------------------------------------------------------------
# 10. Country-code categories
# ------------------------------------------------------------

country_codes = raw_network_df.select("countrycode").distinct().count()
print("Country-code categories:", country_codes)

# ------------------------------------------------------------
# 11. Distinct hourly intervals
# ------------------------------------------------------------

distinct_timestamps = raw_network_df.select("datetime").distinct().count()
print("Distinct timestamps:", distinct_timestamps)

expected_timestamps = len(actual_files) * 24
print("Expected timestamps (D × 24):", expected_timestamps)

# ------------------------------------------------------------
# 12. Check grid_id range
# ------------------------------------------------------------

invalid_grids = raw_network_df.filter(
    (col("CellID") < 1) |
    (col("CellID") > 10000)
).count()
print("Invalid grid_id rows:", invalid_grids)

# ------------------------------------------------------------
# 13. Check input_file_name population
# ------------------------------------------------------------

missing_file_names = raw_network_df.filter(
    col("input_file_name").isNull() |
    (col("input_file_name") == "")
).count()
print("Rows with missing input_file_name:", missing_file_names)

# ------------------------------------------------------------
# 14. File-level row count report
# ------------------------------------------------------------

file_row_count_report = (
    raw_network_df
    .groupBy("input_file_name")
    .agg(count("*").alias("row_count"))
    .orderBy("input_file_name")
)

print("\n========== FILE-LEVEL ROW COUNT REPORT ==========")
file_row_count_report.show(file_row_count_report.count(), truncate=False)

# ------------------------------------------------------------
# 15. Partition count
# ------------------------------------------------------------

partition_count = raw_network_df.rdd.getNumPartitions()
print("\n========== PARTITIONS ==========")
print("Partition count:", partition_count)

# ------------------------------------------------------------
# 16. Acceptance criteria
# ------------------------------------------------------------

print("\n========== ACCEPTANCE CRITERIA ==========")
print("Timestamp check:", distinct_timestamps == expected_timestamps)
print("File count check:", spark_file_count == len(actual_files))
print("Grid range check:", invalid_grids == 0)
print("input_file_name check:", missing_file_names == 0)

# ------------------------------------------------------------
# 17. Show sample data
# ------------------------------------------------------------

print("\n========== SAMPLE DATA ==========")
raw_network_df.show(10, truncate=False)

# ------------------------------------------------------------
# 18. Stop Spark
# ------------------------------------------------------------
# Save

raw_network_df.write.mode("overwrite").parquet(r"D:\capstone1\parquet_output\raw_parquet")

# # ------------------------------------------------------------
# # Store outputs in JSON
# # ------------------------------------------------------------

# # File-level row count report as list
# file_row_counts = [
#     {
#         "input_file_name": row["input_file_name"],
#         "row_count": row["row_count"]
#     }
#     for row in file_row_count_report.collect()
# ]

# results = {
#     "execution_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),

#     "schema": raw_network_df.schema.simpleString(),

#     "basic_counts": {
#         "total_rows": row_count,
#         "spark_file_count": spark_file_count,
#         "actual_file_count": len(actual_files),
#         "unique_grids": unique_grids,
#         "country_code_categories": country_codes,
#         "distinct_timestamps": distinct_timestamps,
#         "expected_timestamps": expected_timestamps,
#         "partition_count": partition_count
#     },

#     "data_quality_checks": {
#         "invalid_grid_rows": invalid_grids,
#         "missing_input_file_names": missing_file_names
#     },

#     "acceptance_criteria": {
#         "timestamp_check": distinct_timestamps == expected_timestamps,
#         "file_count_check": spark_file_count == len(actual_files),
#         "grid_range_check": invalid_grids == 0,
#         "input_file_name_check": missing_file_names == 0
#     },

#     "files_found": [os.path.basename(f) for f in actual_files],

#     "file_level_row_counts": file_row_counts
# }

# # Save JSON
# json_output_path = "output/sp1_ingestion_output.json"

# with open(json_output_path, "w", encoding="utf-8") as json_file:
#     json.dump(results, json_file, indent=4)

# print(f"\nJSON report saved: {json_output_path}")



spark.stop()
print("stopped")
