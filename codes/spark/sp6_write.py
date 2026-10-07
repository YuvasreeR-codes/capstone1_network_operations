from pyspark.sql import functions as F
import os, glob, shutil, logging
import os
from pyspark.sql import SparkSession
os.environ["HADOOP_HOME"] = r"C:\Users\yuvasree.r\Desktop\python test\hadoop"
os.environ["PATH"] = os.environ["PATH"] + r";C:\Users\yuvasree.r\Desktop\python test\hadoop\bin"

spark =  (
            SparkSession.builder
            .appName("sp5")
            .master("local[*]")
            .getOrCreate()
            )
# ---------- LOG ----------
logging.basicConfig(
    filename=r"logs\sp6_output.log",
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    filemode="w"
)

def log(x):
    print(x)
    logging.info(x)

# ---------- READ EXISTING PARQUETS ----------
clean_df = spark.read.parquet(r"parquet_output\clean_parquet")
hourly_df = spark.read.parquet(r"parquet_output\hourly_grid_parquet")
geo_df = spark.read.parquet(r"parquet_output\grid_activity_geo_parquet")
lookup_df = spark.read.parquet(r"parquet_output\grid_lookup_parquet")

# ---------- COUNTS ----------
clean_n = clean_df.count()
hourly_n = hourly_df.count()

log(f"Clean rows: {clean_n}")
log(f"Hourly rows: {hourly_n}")
log(f"Geo rows: {geo_df.count()}")
log(f"Lookup rows: {lookup_df.count()}")

# ---------- GRAIN CHECK ----------
dupes = (
    hourly_df.groupBy("grid_id", "timestamp")
    .count().filter("count > 1").count()
)

assert dupes == 0
log(f"Duplicate (grid_id,timestamp): {dupes} - PASSED")

# ---------- NO GEOMETRY ----------
assert "geometry" not in hourly_df.columns
log("No geometry in hourly_grid_parquet - PASSED")

# ---------- ROUND TRIP ----------
clean2 = spark.read.parquet(r"parquet_output\clean_parquet")
hourly2 = spark.read.parquet(r"parquet_output\hourly_grid_parquet")

assert clean2.count() == clean_n
assert hourly2.count() == hourly_n
assert clean2.schema == clean_df.schema
assert hourly2.schema == hourly_df.schema

dupes2 = (
    hourly2.groupBy("grid_id", "timestamp")
    .count().filter("count > 1").count()
)

assert dupes2 == 0

log("Clean Parquet row count/schema: PASSED")
log("Hourly Parquet row count/schema: PASSED")
log("Round-trip duplicate check: PASSED")

# ---------- DATE PARTITIONS ----------
import  shutil

partitioned_clean = r"parquet_output\clean_parquet_partitioned_sp6"

if os.path.exists(partitioned_clean):
    shutil.rmtree(partitioned_clean)

(
    clean_df.write
    .mode("overwrite")
    .partitionBy("date")
    .parquet(partitioned_clean)
)

parts = glob.glob(
    os.path.join(partitioned_clean, "date=*")
)

assert parts

log(f"Date partitions: {len(parts)}")
log("Date partition check: PASSED")

log(f"Date partitions: {len(parts)}")
log("Date partition check: PASSED")

# ---------- DASHBOARD CSV ----------
dashboard = (
    hourly_df.groupBy("date")
    .agg(
        F.sum("total_sms").alias("total_sms"),
        F.sum("total_calls").alias("total_calls"),
        F.sum("internet_activity").alias("internet_activity"),
        F.sum("total_activity").alias("total_activity"),
        F.countDistinct("grid_id").alias("active_grids")
    )
    .orderBy("date")
)

temp = "output/dashboard_temp"
output = "output/dashboard_summary_sp6.csv"

if os.path.exists(temp):
    shutil.rmtree(temp)
if os.path.exists(output):
    os.remove(output)

dashboard.coalesce(1).write.mode("overwrite").option(
    "header", True
).csv(temp)

csv = glob.glob(os.path.join(temp, "part-*.csv"))[0]
shutil.copy(csv, output)
shutil.rmtree(temp)

log(f"Dashboard CSV: {output}")

# ---------- FILE SIZES ----------
def size(path):
    return sum(
        os.path.getsize(os.path.join(r, f))
        for r, _, fs in os.walk(path)
        for f in fs
    ) / (1024 * 1024)

parquet_mb = size(r"parquet_output\hourly_grid_parquet")
csv_mb = os.path.getsize(output) / (1024 * 1024)

log(f"Hourly Parquet size: {parquet_mb:.4f} MB")
log(f"Dashboard CSV size: {csv_mb:.4f} MB")
log("Parquet is columnar and generally provides better compression and selective column reads.")

log("========== SP6 COMPLETE ==========")