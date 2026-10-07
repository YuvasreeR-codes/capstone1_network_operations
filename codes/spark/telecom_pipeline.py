
import os, sys, glob, json, argparse, logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)

from pyspark.sql import functions as F
import shutil

os.environ["HADOOP_HOME"] = r"C:\Users\yuvasree.r\Desktop\python test\hadoop"
os.environ["PATH"] = os.environ["PATH"] + r";C:\Users\yuvasree.r\Desktop\python test\hadoop\bin"
# ============================================================
# LOGGING
# ============================================================

def setup_logger(log_path):
    os.makedirs(log_path, exist_ok=True)
    logger = logging.getLogger("telecom_pipeline")
    logger.handlers.clear()
    logger.setLevel(logging.INFO)

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    fh = logging.FileHandler(
        os.path.join(log_path, "telecom_pipeline.log")
    )
    ch = logging.StreamHandler()

    fh.setFormatter(fmt)
    ch.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger


# ============================================================
# SPARK
# ============================================================

def create_spark():
    return (
        SparkSession.builder
        .appName("Telecom_Network_Pipeline")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


# ============================================================
# 1. READ RAW
# ============================================================

def read_raw(spark, input_path, logger):

    pattern = os.path.join(
        input_path,
        "sms-call-internet-mi-2013-11-01.csv"
    )

    files = glob.glob(pattern)

    if not files:
        raise FileNotFoundError(
            f"No input files found in {input_path}"
        )

    logger.info(f"input_files={len(files)}")

    schema = StructType([
        StructField("datetime", StringType(), True),
        StructField("CellId", IntegerType(), True),
        StructField("countrycode", StringType(), True),
        StructField("smsin", DoubleType(), True),
        StructField("smsout", DoubleType(), True),
        StructField("callin", DoubleType(), True),
        StructField("callout", DoubleType(), True),
        StructField("internet", DoubleType(), True)
    ])

    df = (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(pattern)
    )

    logger.info(f"input_rows={df.count()}")
    return df


# ============================================================
# 2. CLEAN
# ============================================================

def clean(df, logger):

    df = (
        df
        .withColumnRenamed("datetime", "timestamp")
        .withColumnRenamed("CellId", "grid_id")
        .withColumnRenamed("countrycode", "country_code")
        .withColumnRenamed("smsin", "sms_in")
        .withColumnRenamed("smsout", "sms_out")
        .withColumnRenamed("callin", "call_in")
        .withColumnRenamed("callout", "call_out")
        .withColumnRenamed("internet", "internet_activity")
        .withColumn(
            "timestamp",
            F.to_timestamp(
                "timestamp",
                "yyyy-MM-dd HH:mm:ss"
            )
        )
    )

    activity = [
        "sms_in",
        "sms_out",
        "call_in",
        "call_out",
        "internet_activity"
    ]

    bad = (
        F.col("grid_id").isNull()
        | F.col("timestamp").isNull()
        | F.greatest(*[
            F.col(c) < 0 for c in activity
        ])
    )

    rejected = df.filter(bad).count()

    nulls = sum(
        df.filter(F.col(c).isNull()).count()
        for c in activity
    )

    df = df.filter(~bad)

    df = df.fillna(0, subset=activity)

    df = (
        df
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
            sum(F.col(c) for c in activity)
        )
        .withColumn(
            "date",
            F.date_format("timestamp", "yyyy-MM-dd")
        )
        .withColumn(
            "hour",
            F.date_format("timestamp", "HH")
        )
        .withColumn(
            "day_of_week",
            F.dayofweek("timestamp")
        )
    )

    logger.info(f"rejected_rows={rejected}")
    logger.info(f"nulls_handled={nulls}")

    return df, rejected, nulls


# ============================================================
# 3. AGGREGATE
# ============================================================

def aggregate(df, logger):

    activity = [
        "sms_in",
        "sms_out",
        "call_in",
        "call_out",
        "internet_activity"
    ]

    hourly = (
        df.groupBy("grid_id", "timestamp")
        .agg(*[
            F.sum(c).alias(c)
            for c in activity
        ])
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
            sum(F.col(c) for c in activity)
        )
        .withColumn(
            "internet_share_pct",
            F.when(
                F.col("total_activity") > 0,
                F.col("internet_activity")
                / F.col("total_activity") * 100
            ).otherwise(0)
        )
    )

    duplicates = (
        hourly.groupBy("grid_id", "timestamp")
        .count()
        .filter("count > 1")
        .count()
    )

    if duplicates:
        raise ValueError(
            "Duplicate grid_id + timestamp records found"
        )

    daily = (
        hourly
        .withColumn(
            "date",
            F.date_format("timestamp", "yyyy-MM-dd")
        )
        .groupBy("date", "grid_id")
        .agg(
            F.sum("total_activity")
            .alias("daily_activity")
        )
    )

    # hotspots = (
    #     hourly.orderBy(
    #         F.col("total_activity").desc()
    #     ).limit(10)
    # )
    hotspots = (
    hourly
    .groupBy("grid_id")
    .agg(
        F.sum("total_activity").alias("total_activity")
    )
    .orderBy(
        F.col("total_activity").desc()
    )
    .limit(50)
)

    logger.info(f"output_rows={hourly.count()}")

    return hourly, daily, hotspots


# ============================================================
# 4. ENRICH
# ============================================================

def enrich(hourly, reference_path, spark, logger):

    geo_path = os.path.join(
        reference_path,
        "milano-grid.geojson"
    )

    if not os.path.exists(geo_path):
        raise FileNotFoundError(
            f"Reference file not found: {geo_path}"
        )

    with open(geo_path, encoding="utf-8") as f:
        geojson = json.load(f)

    records = [
        (
            int(feature["properties"]["cellId"]),
            json.dumps(feature["geometry"])
        )
        for feature in geojson.get("features", [])
    ]

    schema = StructType([
        StructField("grid_id", IntegerType(), False),
        StructField("geometry", StringType(), True)
    ])

    lookup = spark.createDataFrame(
        records,
        schema
    )

    enriched = (
        hourly
        .join(
            F.broadcast(lookup),
            "grid_id",
            "left"
        )
    )

    if hourly.count() != enriched.count():
        raise ValueError(
            "Enrichment changed the row count"
        )

    logger.info(
        f"enriched_rows={enriched.count()}"
    )

    return enriched


# ============================================================
# 5. WRITE OUTPUTS
# ============================================================



def write_outputs(
    clean_df,
    hourly,
    daily,
    enriched,
    hotspots,
    output_path,
    logger
):

    paths = {
        "processed": os.path.join(
            output_path,
            "processed",
            "activity"
        ),
        "hourly": os.path.join(
            output_path,
            "analytics",
            "hourly_grid_summary"
        ),
        "daily": os.path.join(
            output_path,
            "analytics",
            "daily_traffic_summary"
        ),
        "geo": os.path.join(
            output_path,
            "analytics",
            "grid_activity_geo"
        ),
        # "dashboard": os.path.join(
        #     output_path,
        #     "dashboard_summary.csv"
        # ),
        "hotspots": os.path.join(
                    output_path,
                    "analytics",
                    "hotspots"
                )

    }

    (
        clean_df.write
        .mode("overwrite")
        .partitionBy("date")
        .parquet(paths["processed"])
    )

    hourly.write.mode("overwrite").parquet(
        paths["hourly"]
    )

    daily.write.mode("overwrite").parquet(
        paths["daily"]
    )

    enriched.write.mode("overwrite").parquet(
        paths["geo"]
    )

    hotspots.write.mode("overwrite").parquet(
            paths["hotspots"]
        )

    # # Dashboard CSV
    # dashboard_tmp = os.path.join(
    #     output_path,
    #     "dashboard_summary_tmp"
    # )

    # (
    #     hotspots
    #     .select(
    #         "grid_id",
    #         "timestamp",
    #         "total_activity",
    #         "internet_share_pct"
    #     )
    #     .coalesce(1)
    #     .write
    #     .mode("overwrite")
    #     .option("header", True)
    #     .csv(dashboard_tmp)
    # )

    # part_file = next(
    #     f for f in os.listdir(dashboard_tmp)
    #     if f.startswith("part-") and f.endswith(".csv")
    # )

    # shutil.move(
    #     os.path.join(dashboard_tmp, part_file),
    #     paths["dashboard"]
    # )

    # shutil.rmtree(dashboard_tmp)

    logger.info("outputs_written=SUCCESS")

    return paths

# ============================================================
# MAIN
# ============================================================

def main():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input",
        required=True
    )

    parser.add_argument(
        "--reference",
        required=True
    )

    parser.add_argument(
        "--output",
        required=True
    )

    parser.add_argument(
        "--logs",
        required=True
    )

    args = parser.parse_args()

    logger = setup_logger(args.logs)
    start = datetime.now()

    logger.info("status=STARTED")

    spark = None

    try:

        spark = create_spark()

        raw = read_raw(
            spark,
            args.input,
            logger
        )

        clean_df, rejected, nulls = clean(
            raw,
            logger
        )

        hourly, daily, hotspots = aggregate(
            clean_df,
            logger
        )

        enriched = enrich(
            hourly,
            args.reference,
            spark,
            logger
        )

        write_outputs(
            clean_df,
            hourly,
            daily,
            enriched,
            hotspots,
            args.output,
            logger
        )

        logger.info(
            f"input_rows={raw.count()}"
        )
        logger.info(
            f"rejected_rows={rejected}"
        )
        logger.info(
            f"nulls_handled={nulls}"
        )
        logger.info(
            f"output_rows={hourly.count()}"
        )
        logger.info("status=SUCCESS")
        logger.info(
            f"duration_seconds={(datetime.now()-start).total_seconds():.2f}"
        )

        return 0

    except Exception as e:

        logger.exception(
            f"status=FAILED error={e}"
        )

        return 1

    finally:

        if spark:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())

