from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from pyspark.storagelevel import StorageLevel

import time
import logging
from io import StringIO
from contextlib import redirect_stdout
from pyspark.sql import SparkSession

import os
os.environ["HADOOP_HOME"] = r"C:\Users\yuvasree.r\Desktop\python test\hadoop"
os.environ["PATH"] = os.environ["PATH"] + r";C:\Users\yuvasree.r\Desktop\python test\hadoop\bin"

# ============================================================
# SP5 - PERFORMANCE & EXECUTION BEHAVIOUR
# ============================================================

spark =  (
            SparkSession.builder
            .appName("sp5")
            .master("local[*]")
            .getOrCreate()
            )
clean_network_df= spark.read.parquet(r"parquet_output\clean_parquet")
hourly_grid_summary=spark.read.parquet(r"parquet_output\hourly_grid_parquet")
grid_lookup_df=spark.read.parquet(r"parquet_output\grid_lookup_parquet")
# ============================================================
# 1. LOGGING SETUP
# ============================================================

LOG_FILE = "logs/sp5_performance_execution.log"

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="w"
)

logger = logging.getLogger("SP5")


def log_message(message):
    """
    Print output to terminal and save it to the log file.
    """
    print(message)
    logger.info(message)


def log_dataframe(df, title, rows=20):
    """
    Capture DataFrame.show() output in the log file.
    """

    buffer = StringIO()

    with redirect_stdout(buffer):
        df.show(rows, truncate=False)

    output = buffer.getvalue()

    print(f"\n========== {title} ==========")
    print(output)

    logger.info(
        f"\n========== {title} ==========\n{output}"
    )


def log_explain(df, title):
    """
    Capture Spark physical execution plan in the log file.
    """

    buffer = StringIO()

    with redirect_stdout(buffer):
        df.explain(mode="formatted")

    plan = buffer.getvalue()

    print(f"\n========== {title} ==========")
    print(plan)

    logger.info(
        f"\n========== {title} ==========\n{plan}"
    )


log_message("\n========== SP5 START ==========")


# ============================================================
# 2. INPUT INFORMATION
# ============================================================

clean_count = clean_network_df.count()

original_partitions = (
    clean_network_df
    .rdd
    .getNumPartitions()
)

log_message(
    f"\nclean_network_df rows: {clean_count}"
)

log_message(
    f"Original partition count: {original_partitions}"
)


# ============================================================
# 3. HOTSPOT AGGREGATION
# ============================================================

hotspot_df = (
    clean_network_df
    .groupBy("grid_id")
    .agg(
        F.sum("total_activity").alias(
            "total_activity"
        )
    )
    .orderBy(
        F.col("total_activity").desc()
    )
    .limit(10)
)


# ------------------------------------------------------------
# EXPLAIN HOTSPOT PLAN
# ------------------------------------------------------------

log_explain(
    hotspot_df,
    "HOTSPOT AGGREGATION PHYSICAL PLAN"
)


# Execute the aggregation
hotspot_count = hotspot_df.count()

log_dataframe(
    hotspot_df,
    "TOP 10 HOTSPOT GRIDS",
    10
)


# ============================================================
# 4. CACHE PERFORMANCE TEST
# ============================================================

log_message(
    "\n========== CACHE PERFORMANCE TEST =========="
)


# IMPORTANT:
# Do NOT cache the entire clean_network_df.
#
# The previous MEMORY_ONLY cache caused:
#
#     java.lang.OutOfMemoryError: Java heap space
#
# Therefore we create a smaller DataFrame containing only
# the columns needed for this performance test.

cache_test_df = (
    clean_network_df
    .select(
        "grid_id",
        "date",
        "total_activity"
    )
)


log_message(
    "Cache test uses only: "
    "grid_id, date, total_activity"
)


# ------------------------------------------------------------
# Uncached action 1
# ------------------------------------------------------------

start_time = time.perf_counter()

uncached_count_1 = cache_test_df.count()

uncached_time_1 = (
    time.perf_counter() - start_time
)


log_message(
    f"Uncached action 1 count: {uncached_count_1}"
)

log_message(
    f"Uncached action 1 time: "
    f"{uncached_time_1:.4f} seconds"
)


# ------------------------------------------------------------
# Uncached action 2
# ------------------------------------------------------------

start_time = time.perf_counter()

uncached_count_2 = cache_test_df.count()

uncached_time_2 = (
    time.perf_counter() - start_time
)


log_message(
    f"Uncached action 2 count: {uncached_count_2}"
)

log_message(
    f"Uncached action 2 time: "
    f"{uncached_time_2:.4f} seconds"
)


# ------------------------------------------------------------
# Cache the smaller DataFrame
# ------------------------------------------------------------

cached_df = cache_test_df.persist(
    StorageLevel.MEMORY_AND_DISK
)


# MEMORY_AND_DISK is safer than MEMORY_ONLY for this lab.
#
# If the data does not fit completely in memory, Spark can
# store the remaining partitions on disk instead of failing
# with a Java heap-space error.


# ------------------------------------------------------------
# Cached action 1
# ------------------------------------------------------------
# First action materializes the cache.

start_time = time.perf_counter()

cached_count_1 = cached_df.count()

cached_time_1 = (
    time.perf_counter() - start_time
)


log_message(
    f"Cached action 1 count: {cached_count_1}"
)

log_message(
    f"Cached action 1 time: "
    f"{cached_time_1:.4f} seconds"
)


# ------------------------------------------------------------
# Cached action 2
# ------------------------------------------------------------
# This action can reuse the cached data.

start_time = time.perf_counter()

cached_count_2 = cached_df.count()

cached_time_2 = (
    time.perf_counter() - start_time
)


log_message(
    f"Cached action 2 count: {cached_count_2}"
)

log_message(
    f"Cached action 2 time: "
    f"{cached_time_2:.4f} seconds"
)


# ------------------------------------------------------------
# Cache comparison
# ------------------------------------------------------------

log_message(
    "\n========== CACHE COMPARISON =========="
)

log_message(
    f"Uncached action 1: "
    f"{uncached_time_1:.4f} seconds"
)

log_message(
    f"Uncached action 2: "
    f"{uncached_time_2:.4f} seconds"
)

log_message(
    f"Cached action 1: "
    f"{cached_time_1:.4f} seconds"
)

log_message(
    f"Cached action 2: "
    f"{cached_time_2:.4f} seconds"
)


if uncached_time_2 > 0:

    speedup = (
        uncached_time_2 / cached_time_2
    )

    log_message(
        f"Cached second-action speedup: "
        f"{speedup:.2f}x"
    )


# Verify counts
assert uncached_count_1 == cached_count_1
assert uncached_count_2 == cached_count_2


# ============================================================
# 5. REPARTITION BY DATE
# ============================================================

log_message(
    "\n========== REPARTITION TEST =========="
)


# Use a modest number of partitions.
# Four partitions are enough for this local demonstration.

date_repartitioned_df = (
    clean_network_df
    .repartition(
        4,
        "date"
    )
)


repartitioned_count = (
    date_repartitioned_df
    .rdd
    .getNumPartitions()
)


log_message(
    f"Original partitions: "
    f"{original_partitions}"
)

log_message(
    f"After repartition(4, date): "
    f"{repartitioned_count}"
)


# Explain the repartition plan
log_explain(
    date_repartitioned_df,
    "REPARTITION BY DATE PLAN"
)


# ============================================================
# 6. OVER-PARTITIONING DEMONSTRATION
# ============================================================

log_message(
    "\n========== OVER-PARTITIONING TEST =========="
)


# Deliberately create many partitions only to demonstrate
# why blindly increasing partition count can hurt a small
# local workload.

over_partitioned_df = (
    clean_network_df
    .repartition(200)
)


over_partition_count = (
    over_partitioned_df
    .rdd
    .getNumPartitions()
)


log_message(
    f"Over-partitioned count: "
    f"{over_partition_count}"
)


# Execute a simple action to obtain evidence.

start_time = time.perf_counter()

over_partitioned_rows = (
    over_partitioned_df.count()
)

over_partitioned_time = (
    time.perf_counter() - start_time
)


log_message(
    f"Over-partitioned rows: "
    f"{over_partitioned_rows}"
)

log_message(
    f"Over-partitioned count time: "
    f"{over_partitioned_time:.4f} seconds"
)


# ============================================================
# 7. COLUMN PRUNING
# ============================================================

log_message(
    "\n========== COLUMN PRUNING =========="
)


# ------------------------------------------------------------
# Full DataFrame version
# ------------------------------------------------------------

full_column_aggregation = (
    clean_network_df
    .groupBy("grid_id")
    .agg(
        F.sum("total_activity").alias(
            "total_activity"
        )
    )
)


log_explain(
    full_column_aggregation,
    "FULL DATAFRAME AGGREGATION PLAN"
)


start_time = time.perf_counter()

full_column_count = (
    full_column_aggregation.count()
)

full_column_time = (
    time.perf_counter() - start_time
)


log_message(
    f"Full-column aggregation time: "
    f"{full_column_time:.4f} seconds"
)


# ------------------------------------------------------------
# Column-pruned version
# ------------------------------------------------------------

pruned_df = (
    clean_network_df
    .select(
        "grid_id",
        "total_activity"
    )
)


pruned_aggregation = (
    pruned_df
    .groupBy("grid_id")
    .agg(
        F.sum("total_activity").alias(
            "total_activity"
        )
    )
)


log_explain(
    pruned_aggregation,
    "COLUMN-PRUNED AGGREGATION PLAN"
)


start_time = time.perf_counter()

pruned_count = (
    pruned_aggregation.count()
)

pruned_time = (
    time.perf_counter() - start_time
)


log_message(
    f"Column-pruned aggregation time: "
    f"{pruned_time:.4f} seconds"
)


# Verify results are equivalent
assert full_column_count == pruned_count

log_message(
    "Column-pruning result validation: PASSED"
)


# ============================================================
# 8. STANDARD JOIN VS BROADCAST JOIN
# ============================================================

log_message(
    "\n========== STANDARD JOIN =========="
)


standard_join = (
    hourly_grid_summary
    .join(
        grid_lookup_df,
        on="grid_id",
        how="left"
    )
)


log_explain(
    standard_join,
    "STANDARD GRID JOIN PHYSICAL PLAN"
)


log_message(
    "\n========== BROADCAST JOIN =========="
)


broadcast_join = (
    hourly_grid_summary
    .join(
        broadcast(grid_lookup_df),
        on="grid_id",
        how="left"
    )
)


log_explain(
    broadcast_join,
    "BROADCAST GRID JOIN PHYSICAL PLAN"
)


# ============================================================
# 9. JOIN RESULT COMPARISON
# ============================================================

log_message(
    "\n========== JOIN RESULT VALIDATION =========="
)


standard_join_count = (
    standard_join.count()
)

broadcast_join_count = (
    broadcast_join.count()
)


log_message(
    f"Standard join rows: "
    f"{standard_join_count}"
)

log_message(
    f"Broadcast join rows: "
    f"{broadcast_join_count}"
)


assert (
    standard_join_count
    == broadcast_join_count
)


log_message(
    "Standard vs broadcast row-count validation: PASSED"
)


# ============================================================
# 10. BROADCAST REASONING
# ============================================================

grid_lookup_count = (
    grid_lookup_df.count()
)

hourly_count = (
    hourly_grid_summary.count()
)


log_message(
    "\n========== BROADCAST OBSERVATION =========="
)

log_message(
    f"Hourly activity rows: {hourly_count}"
)

log_message(
    f"Grid lookup rows: {grid_lookup_count}"
)

log_message(
    "The grid lookup is much smaller than the activity "
    "DataFrame, making it a broadcast candidate."
)


# ============================================================
# 11. THREE PERFORMANCE OBSERVATIONS
# ============================================================

log_message(
    "\n========== PERFORMANCE OBSERVATIONS =========="
)


# Observation 1
log_message(
    "\nOBSERVATION 1 - CACHE\n"
    f"Evidence:\n"
    f"Uncached action 2 = {uncached_time_2:.4f} sec\n"
    f"Cached action 2 = {cached_time_2:.4f} sec\n"
    "Conclusion:\n"
    "Caching is useful when the same DataFrame is reused "
    "across multiple actions. The first cached action pays "
    "the cache materialization cost, while later actions "
    "can reuse cached data."
)


# Observation 2
log_message(
    "\nOBSERVATION 2 - BROADCAST JOIN\n"
    f"Evidence:\n"
    f"Activity rows = {hourly_count}\n"
    f"Grid lookup rows = {grid_lookup_count}\n"
    "The broadcast physical plan should be compared with "
    "the standard join plan above.\n"
    "Conclusion:\n"
    "The grid lookup is a small static reference dataset, "
    "so broadcasting it can avoid a large shuffle of the "
    "activity side."
)


# Observation 3
log_message(
    "\nOBSERVATION 3 - PARTITIONING\n"
    f"Evidence:\n"
    f"Original partitions = {original_partitions}\n"
    f"Date-repartitioned partitions = "
    f"{repartitioned_count}\n"
    f"Over-partitioned partitions = "
    f"{over_partition_count}\n"
    f"Over-partitioned count time = "
    f"{over_partitioned_time:.4f} sec\n"
    "Conclusion:\n"
    "More partitions are not automatically faster. "
    "Excessive partitions introduce task scheduling and "
    "management overhead, particularly for small local "
    "workloads."
)


# Observation 4
log_message(
    "\nOBSERVATION 4 - COLUMN PRUNING\n"
    f"Evidence:\n"
    f"Full-column aggregation = "
    f"{full_column_time:.4f} sec\n"
    f"Column-pruned aggregation = "
    f"{pruned_time:.4f} sec\n"
    "The physical plans above show how Spark handles the "
    "required columns.\n"
    "Conclusion:\n"
    "Selecting only the fields required by an aggregation "
    "reduces unnecessary data carried through the pipeline."
)


# ============================================================
# 12. REJECT ONE OPTIMIZATION
# ============================================================

log_message(
    "\n========== REJECTED OPTIMIZATION =========="
)

log_message(
    "Rejected suggestion:\n"
    "Blindly increasing the number of partitions to "
    "hundreds or thousands.\n\n"
    "Reason:\n"
    "This is a small/local workload. Excessive partitions "
    "create additional tasks and scheduling overhead. "
    "There is no evidence that blindly increasing the "
    "partition count would improve this workload.\n\n"
    "Evidence:\n"
    f"The deliberate 200-partition test produced "
    f"{over_partition_count} partitions and took "
    f"{over_partitioned_time:.4f} seconds for the count action."
)


# ============================================================
# 13. OPTIMIZED HOTSPOT TRANSFORMATION
# ============================================================

log_message(
    "\n========== OPTIMIZED HOTSPOT TRANSFORMATION =========="
)


optimized_hotspot_df = (
    clean_network_df
    .select(
        "grid_id",
        "total_activity"
    )
    .groupBy("grid_id")
    .agg(
        F.sum("total_activity").alias(
            "total_activity"
        )
    )
    .orderBy(
        F.col("total_activity").desc()
    )
    .limit(10)
)


log_explain(
    optimized_hotspot_df,
    "OPTIMIZED HOTSPOT PHYSICAL PLAN"
)


log_dataframe(
    optimized_hotspot_df,
    "OPTIMIZED TOP 10 HOTSPOTS",
    10
)


# ============================================================
# 14. RELEASE CACHE
# ============================================================

cached_df.unpersist()

log_message(
    "\nCached DataFrame released."
)


# ============================================================
# 15. FINAL SUMMARY
# ============================================================

log_message(
    "\n========== SP5 FINAL SUMMARY =========="
)

log_message(
    f"Input rows: {clean_count}"
)

log_message(
    f"Original partitions: {original_partitions}"
)

log_message(
    f"Date-repartitioned partitions: "
    f"{repartitioned_count}"
)

log_message(
    f"Over-partitioned partitions: "
    f"{over_partition_count}"
)

log_message(
    f"Uncached second action: "
    f"{uncached_time_2:.4f} seconds"
)

log_message(
    f"Cached second action: "
    f"{cached_time_2:.4f} seconds"
)

log_message(
    f"Full-column aggregation: "
    f"{full_column_time:.4f} seconds"
)

log_message(
    f"Column-pruned aggregation: "
    f"{pruned_time:.4f} seconds"
)

log_message(
    f"Standard join rows: "
    f"{standard_join_count}"
)

log_message(
    f"Broadcast join rows: "
    f"{broadcast_join_count}"
)

log_message(
    "\nSP5 completed successfully."
)

log_message(
    f"All SP5 outputs saved to: {LOG_FILE}"
)