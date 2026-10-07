import sys

PROJECT_DIR = "/mnt/d/capstone1"

sys.path.insert(
    0,
    PROJECT_DIR
)
import os
import sys
import json
import glob
import re
import logging
import subprocess

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule


# =========================================================
# PROJECT PATHS
# =========================================================

# Change this ONLY if your project is somewhere else
# PROJECT_DIR = r"D:\capstone1"

DATA_DIR = os.path.join(PROJECT_DIR, "data")

LANDING_DIR = os.path.join(DATA_DIR, "landing")
RAW_DIR = os.path.join(DATA_DIR, "raw")
REJECTED_DIR = os.path.join(DATA_DIR, "rejected")
REFERENCE_DIR = os.path.join(DATA_DIR, "reference")

OUTPUT_DIR = os.path.join(DATA_DIR, "final_outputde7")

ANALYTICS_DIR = os.path.join(
    OUTPUT_DIR,
    "analytics"
)

HOURLY_PATH = os.path.join(
    ANALYTICS_DIR,
    "hourly_grid_summary"
)

DE7_OUTPUT_DIR = os.path.join(
    OUTPUT_DIR,
    "de7_outputs"
)

LOG_DIR = os.path.join(
    PROJECT_DIR,
    "logs"
)

DE2_SCRIPT = os.path.join(
    PROJECT_DIR,
    "de2_ingestion.py"
)

SPARK_SCRIPT = os.path.join(
    PROJECT_DIR,
    "spark",
    "telecom_pipeline.py"
)

DE6_SCRIPT = os.path.join(
    PROJECT_DIR,
    "de6_warehouse.py"
)

REFERENCE_FILE = os.path.join(
    REFERENCE_DIR,
    # "milano-grid.geojson"
)

SPARK_LOG = os.path.join(
    LOG_DIR,
    "telecom_pipeline.log"
)

DE7_LOG = os.path.join(
    DE7_OUTPUT_DIR,
    "de7_pipeline.log"
)

STATUS_FILE = os.path.join(
    DE7_OUTPUT_DIR,
    "pipeline_status.json"
)

# =========================================================
# SETTINGS
# =========================================================

FRESH_THRESHOLD_SECONDS = 60 * 60          # 1 hour
STALE_THRESHOLD_SECONDS = 6 * 60 * 60      # 6 hours


# =========================================================
# DIRECTORY SETUP
# =========================================================

os.makedirs(DE7_OUTPUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)


# =========================================================
# LOGGER
# =========================================================

logger = logging.getLogger("DE7")
logger.setLevel(logging.INFO)

if not logger.handlers:
    file_handler = logging.FileHandler(DE7_LOG)
    console_handler = logging.StreamHandler()

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)


# =========================================================
# HELPER FUNCTIONS
# =========================================================

def safe_run_id(run_id):
    """
    Convert Airflow run_id into a filename-safe string.
    """
    return re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)


def run_state_file(run_id):
    """
    Temporary machine-readable state for one DAG run.
    """
    filename = f"pipeline_run_{safe_run_id(run_id)}.json"

    return os.path.join(
        DE7_OUTPUT_DIR,
        filename
    )


def load_run_state(run_id):
    """
    Load current state of this DAG run.
    """

    path = run_state_file(run_id)

    if os.path.exists(path):

        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    return {
        "run_id": run_id,
        "run_started": datetime.now().isoformat(),
        "tasks": {}
    }


def save_run_state(run_id, state):
    """
    Save run-specific state.
    """

    path = run_state_file(run_id)

    temp_path = path + ".tmp"

    with open(
        temp_path,
        "w",
        encoding="utf-8"
    ) as f:

        json.dump(
            state,
            f,
            indent=4
        )

    os.replace(
        temp_path,
        path
    )


def mark_task(
    context,
    task_id,
    status,
    **metrics
):
    """
    Record task status in machine-readable run state.
    """

    run_id = context["dag_run"].run_id

    state = load_run_state(run_id)

    state["tasks"][task_id] = {
        "status": status,
        "timestamp": datetime.now().isoformat(),
        **metrics
    }

    save_run_state(
        run_id,
        state
    )


def write_status(status):
    """
    Write the final DE7 machine-readable status.
    """

    temp_file = STATUS_FILE + ".tmp"

    with open(
        temp_file,
        "w",
        encoding="utf-8"
    ) as f:

        json.dump(
            status,
            f,
            indent=4
        )

    os.replace(
        temp_file,
        STATUS_FILE
    )

    logger.info(
        "Machine-readable status written: %s",
        STATUS_FILE
    )


def run_script(script, args=None):
    """
    Execute an existing project script.
    """

    command = [
        sys.executable,
        script
    ]

    if args:
        command.extend(args)

    logger.info(
        "Running: %s",
        " ".join(command)
    )

    result = subprocess.run(
        command,
        cwd=PROJECT_DIR,
        check=True
    )

    return result.returncode


def parse_last_metric(
    log_file,
    metric_name
):
    """
    Read the latest metric from Spark log.

    Example:
        input_rows=10000
        rejected_rows=5
        nulls_handled=20
    """

    if not os.path.exists(log_file):
        return 0

    pattern = rf"{metric_name}\s*=\s*(\d+)"

    values = []

    with open(
        log_file,
        "r",
        encoding="utf-8",
        errors="ignore"
    ) as f:

        for line in f:

            match = re.search(
                pattern,
                line
            )

            if match:
                values.append(
                    int(match.group(1))
                )

    if values:
        return values[-1]

    return 0


def latest_output_mtime(path):
    """
    Find the latest modification time of files
    inside the analytics output directory.

    This is used for freshness.

    It is NOT used for AS_OF.
    """

    if not os.path.exists(path):
        return None

    latest = None

    for root, dirs, files in os.walk(path):

        for file in files:

            file_path = os.path.join(
                root,
                file
            )

            try:

                modified = os.path.getmtime(
                    file_path
                )

                if latest is None or modified > latest:
                    latest = modified

            except OSError:
                continue

    return latest


def get_freshness(path):
    """
    Calculate freshness of the analytics layer.

    Freshness = current time - analytics publication time.
    """

    latest_mtime = latest_output_mtime(path)

    if latest_mtime is None:
        return None, "UNKNOWN"

    age_seconds = (
        datetime.now().timestamp()
        - latest_mtime
    )

    if age_seconds <= FRESH_THRESHOLD_SECONDS:

        indicator = "FRESH"

    elif age_seconds <= STALE_THRESHOLD_SECONDS:

        indicator = "STALE"

    else:

        indicator = "VERY_STALE"

    return age_seconds, indicator


# =========================================================
# TASK 1
# INGEST
# =========================================================

def ingest_task(**context):

    logger.info("========== INGEST START ==========")

    try:

        run_script(
            DE2_SCRIPT
        )

        raw_files = glob.glob(
            os.path.join(
                RAW_DIR,
                "sms-call-internet-mi-2013-11-01.csv"
            )
        )

        if not raw_files:

            raise RuntimeError(
                "Ingestion completed but no files exist in data/raw"
            )

        logger.info(
            "Raw files available: %d",
            len(raw_files)
        )

        mark_task(
            context,
            "ingest",
            "SUCCESS",
            rows_in=len(raw_files)
        )

        logger.info("========== INGEST SUCCESS ==========")

    except Exception as e:

        mark_task(
            context,
            "ingest",
            "FAILED",
            error=str(e)
        )

        logger.exception(
            "INGEST FAILED"
        )

        raise


# =========================================================
# TASK 2
# VALIDATE
# =========================================================

def validate_task(**context):

    logger.info("========== VALIDATE START ==========")

    try:

        import pandas as pd

        raw_files = glob.glob(
            os.path.join(
                RAW_DIR,
                "sms-call-internet-mi-2013-11-01.csv"
            )
        )

        if not raw_files:

            raise RuntimeError(
                "No raw files available for validation"
            )

        total_rows = 0

        invalid_files = []

        # Import your existing DE2 validation functions.
        sys.path.insert(
            0,
            PROJECT_DIR
        )

        import de2_ingestion

        for file_path in raw_files:

            schema_ok, row_count, reason = (
                de2_ingestion.validate_schema(
                    file_path
                )
            )

            if not schema_ok:

                invalid_files.append(
                    f"{os.path.basename(file_path)}: {reason}"
                )

                continue

            quality_ok, quality_reason = (
                de2_ingestion.validate_minimum_quality(
                    file_path
                )
            )

            if not quality_ok:

                invalid_files.append(
                    f"{os.path.basename(file_path)}: {quality_reason}"
                )

            total_rows += row_count

        if invalid_files:

            raise RuntimeError(
                "Validation failed:\n"
                + "\n".join(invalid_files)
            )

        logger.info(
            "Validated %d raw files",
            len(raw_files)
        )

        logger.info(
            "Total raw rows: %d",
            total_rows
        )

        mark_task(
            context,
            "validate",
            "SUCCESS",
            rows_in=total_rows,
            rows_rejected=0
        )

        logger.info(
            "========== VALIDATE SUCCESS =========="
        )

    except Exception as e:

        mark_task(
            context,
            "validate",
            "FAILED",
            error=str(e)
        )

        logger.exception(
            "VALIDATE FAILED"
        )

        raise


# =========================================================
# TASK 3
# SPARK PROCESS
# =========================================================

def spark_process_task(**context):

    logger.info(
        "========== SPARK PROCESS START =========="
    )

    try:

        run_script(
            SPARK_SCRIPT,
            [
                "--input",
                RAW_DIR,

                "--reference",
                REFERENCE_FILE,

                "--output",
                OUTPUT_DIR,

                "--logs",
                LOG_DIR
            ]
        )

        if not os.path.exists(
            HOURLY_PATH
        ):

            raise RuntimeError(
                "Spark completed but hourly_grid_summary was not created"
            )

        logger.info(
            "Spark analytics output created"
        )

        rows_in = parse_last_metric(
            SPARK_LOG,
            "input_rows"
        )

        rows_rejected = parse_last_metric(
            SPARK_LOG,
            "rejected_rows"
        )

        nulls_handled = parse_last_metric(
            SPARK_LOG,
            "nulls_handled"
        )

        mark_task(
            context,
            "spark_process",
            "SUCCESS",
            rows_in=rows_in,
            rows_rejected=rows_rejected,
            nulls_handled=nulls_handled
        )

        logger.info(
            "========== SPARK PROCESS SUCCESS =========="
        )

    except Exception as e:

        mark_task(
            context,
            "spark_process",
            "FAILED",
            error=str(e)
        )

        logger.exception(
            "SPARK PROCESS FAILED"
        )

        raise


# =========================================================
# TASK 4
# LOAD WAREHOUSE
# =========================================================

def load_warehouse_task(**context):

    logger.info(
        "========== LOAD WAREHOUSE START =========="
    )

    try:

        run_script(
            DE6_SCRIPT
        )

        mark_task(
            context,
            "load_warehouse",
            "SUCCESS"
        )

        logger.info(
            "========== LOAD WAREHOUSE SUCCESS =========="
        )

    except Exception as e:

        mark_task(
            context,
            "load_warehouse",
            "FAILED",
            error=str(e)
        )

        logger.exception(
            "LOAD WAREHOUSE FAILED"
        )

        raise
# =========================================================
# TASK 5
# QUALITY CHECK
# =========================================================

def quality_check_task(**context):

    logger.info(
        "========== QUALITY CHECK START =========="
    )

    run_id = context["dag_run"].run_id
    run_timestamp = datetime.now().isoformat()

    reasons = []

    spark = None

    try:

        # -------------------------------------------------
        # Check analytics output
        # -------------------------------------------------

        if not os.path.exists(
            HOURLY_PATH
        ):

            reasons.append(
                "hourly_grid_summary does not exist"
            )

            raise RuntimeError(
                "Analytics output missing"
            )

        # -------------------------------------------------
        # Read latest AS_OF from analytics
        # -------------------------------------------------

        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F

        spark = (
            SparkSession.builder
            .appName("DE7_Quality_Check")
            .master("local[2]")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel(
            "WARN"
        )

        hourly_df = spark.read.parquet(
            HOURLY_PATH
        )

        rows_published = hourly_df.count()

        if rows_published == 0:

            reasons.append(
                "hourly_grid_summary contains zero rows"
            )

            raise RuntimeError(
                "No published analytics rows"
            )

        # =================================================
        # IMPORTANT AS_OF FIX
        # =================================================
        #
        # AS_OF = latest timestamp in actual analytics data
        #
        # NOT datetime.now()
        #
        # =================================================

        latest_timestamp = (
            hourly_df
            .select(
                F.max("timestamp").alias(
                    "latest_timestamp"
                )
            )
            .first()
            ["latest_timestamp"]
        )

        if latest_timestamp is None:

            reasons.append(
                "Could not determine AS_OF"
            )

            raise RuntimeError(
                "AS_OF could not be determined"
            )

        # Convert timestamp to ISO string
        if hasattr(
            latest_timestamp,
            "isoformat"
        ):

            as_of = latest_timestamp.isoformat()

        else:

            as_of = str(
                latest_timestamp
            )

        logger.info(
            "AS_OF = %s",
            as_of
        )

        # -------------------------------------------------
        # Check required analytics columns
        # -------------------------------------------------

        required_columns = [
            "timestamp",
            "grid_id",
            "total_activity"
        ]

        missing_columns = [
            column
            for column in required_columns
            if column not in hourly_df.columns
        ]

        if missing_columns:

            reasons.append(
                "Missing columns: "
                + ", ".join(missing_columns)
            )

            raise RuntimeError(
                "Required analytics columns missing"
            )

        # -------------------------------------------------
        # Check nulls
        # -------------------------------------------------

        null_rows = hourly_df.filter(
            F.col("timestamp").isNull()
            | F.col("grid_id").isNull()
            | F.col("total_activity").isNull()
        ).count()

        if null_rows > 0:

            reasons.append(
                f"{null_rows} rows contain required null values"
            )

        # -------------------------------------------------
        # Get Spark metrics
        # -------------------------------------------------

        rows_in = parse_last_metric(
            SPARK_LOG,
            "input_rows"
        )

        rows_rejected = parse_last_metric(
            SPARK_LOG,
            "rejected_rows"
        )

        nulls_handled = parse_last_metric(
            SPARK_LOG,
            "nulls_handled"
        )

        # -------------------------------------------------
        # Freshness
        # -------------------------------------------------

        freshness_seconds, freshness_indicator = (
            get_freshness(
                HOURLY_PATH
            )
        )

        if freshness_seconds is None:

            reasons.append(
                "Could not determine analytics freshness"
            )

        logger.info(
            "Freshness = %s",
            freshness_indicator
        )

        # -------------------------------------------------
        # Warehouse check
        # -------------------------------------------------

        warehouse_rows = 0

        try:

            import mysql.connector

            connection = mysql.connector.connect(
                host="172.29.64.1",
                port=3306,
                user="root",
                password="root",
                database="capstone1airflow"
                
            )

            cursor = connection.cursor()

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM fact_network_activity
                """
            )

            result = cursor.fetchone()

            if result:

                warehouse_rows = result[0]

            cursor.close()
            connection.close()

        except Exception as db_error:

            reasons.append(
                f"Warehouse check failed: {db_error}"
            )

        if warehouse_rows == 0:

            reasons.append(
                "Warehouse fact table contains zero rows"
            )

        # -------------------------------------------------
        # Overall health
        # -------------------------------------------------

        if freshness_indicator == "VERY_STALE":

            reasons.append(
                "Analytics layer is very stale"
            )

        healthy = (
            len(reasons) == 0
        )

        # -------------------------------------------------
        # Get existing task statuses
        # -------------------------------------------------

        state = load_run_state(
            run_id
        )

        tasks = state.get(
            "tasks",
            {}
        )

        # Quality check itself succeeds only if healthy
        quality_status = (
            "SUCCESS"
            if healthy
            else "FAILED"
        )

        tasks["quality_check"] = {
            "status": quality_status,
            "timestamp": datetime.now().isoformat(),
            "rows_in": rows_in,
            "rows_rejected": rows_rejected,
            "nulls_handled": nulls_handled,
            "rows_published": rows_published,
            "warehouse_rows": warehouse_rows
        }

        # notify has not executed yet
        tasks["notify"] = {
            "status": "PENDING"
        }

        # -------------------------------------------------
        # FINAL MACHINE-READABLE STATUS
        # -------------------------------------------------

        status = {
            "last_run_id": run_id,

            "last_run_timestamp": run_timestamp,

            # =================================================
            # CORRECT AS_OF
            # =================================================
            "AS_OF": as_of,

            "current_as_of": as_of,

            "freshness_seconds": (
                round(
                    freshness_seconds,
                    2
                )
                if freshness_seconds is not None
                else None
            ),

            "freshness_indicator": (
                freshness_indicator
            ),

            "rows_in": rows_in,

            "rows_rejected": rows_rejected,

            "nulls_handled": nulls_handled,

            "rows_published": rows_published,

            "warehouse_rows": warehouse_rows,

            "healthy": healthy,

            "reasons": reasons,

            "tasks": tasks
        }

        # -------------------------------------------------
        # Save status
        # -------------------------------------------------

        write_status(
            status
        )

        state["tasks"] = tasks

        save_run_state(
            run_id,
            state
        )

        if not healthy:

            raise RuntimeError(
                "Quality check failed: "
                + "; ".join(reasons)
            )

        logger.info(
            "========== QUALITY CHECK SUCCESS =========="
        )

    except Exception as e:

        logger.exception(
            "QUALITY CHECK FAILED"
        )

        # ---------------------------------------------
        # Make sure API6 still gets a failure record
        # ---------------------------------------------

        state = load_run_state(
            run_id
        )

        tasks = state.get(
            "tasks",
            {}
        )

        tasks["quality_check"] = {
            "status": "FAILED",
            "timestamp": datetime.now().isoformat(),
            "error": str(e)
        }

        status = {
            "last_run_id": run_id,

            "last_run_timestamp": run_timestamp,

            "AS_OF": None,

            "current_as_of": None,

            "freshness_seconds": None,

            "freshness_indicator": "UNKNOWN",

            "rows_in": parse_last_metric(
                SPARK_LOG,
                "input_rows"
            ),

            "rows_rejected": parse_last_metric(
                SPARK_LOG,
                "rejected_rows"
            ),

            "nulls_handled": parse_last_metric(
                SPARK_LOG,
                "nulls_handled"
            ),

            "rows_published": 0,

            "warehouse_rows": 0,

            "healthy": False,

            "reasons": [
                str(e)
            ],

            "tasks": tasks
        }

        write_status(
            status
        )

        save_run_state(
            run_id,
            {
                **state,
                "tasks": tasks
            }
        )

        raise

    finally:

        if spark is not None:

            spark.stop()


# =========================================================
# TASK 6
# NOTIFY
# =========================================================

def notify_task(**context):

    logger.info(
        "========== NOTIFY START =========="
    )

    run_id = context["dag_run"].run_id

    try:

        state = load_run_state(
            run_id
        )

        tasks = state.get(
            "tasks",
            {}
        )

        failed_tasks = []

        for task_name in [
            "ingest",
            "validate",
            "spark_process",
            "load_warehouse",
            "quality_check"
        ]:

            task_info = tasks.get(
                task_name,
                {}
            )

            if task_info.get(
                "status"
            ) != "SUCCESS":

                failed_tasks.append(
                    task_name
                )

        # -------------------------------------------------
        # If everything succeeded
        # -------------------------------------------------

        if not failed_tasks:

            if os.path.exists(
                STATUS_FILE
            ):

                with open(
                    STATUS_FILE,
                    "r",
                    encoding="utf-8"
                ) as f:

                    status = json.load(f)

            else:

                status = {
                    "last_run_id": run_id,
                    "last_run_timestamp": datetime.now().isoformat(),
                    "healthy": True,
                    "reasons": [],
                    "tasks": tasks
                }

            status["tasks"][
                "notify"
            ] = {
                "status": "SUCCESS",
                "timestamp": datetime.now().isoformat()
            }

            status["healthy"] = True

            write_status(
                status
            )

            logger.info(
                "DE7 PIPELINE SUCCESS"
            )

        # -------------------------------------------------
        # If something failed
        # -------------------------------------------------

        else:

            reasons = [
                f"Task failed: {task}"
                for task in failed_tasks
            ]

            status = {
                "last_run_id": run_id,

                "last_run_timestamp": datetime.now().isoformat(),

                "AS_OF": None,

                "current_as_of": None,

                "freshness_seconds": None,

                "freshness_indicator": "UNKNOWN",

                "rows_in": 0,

                "rows_rejected": 0,

                "nulls_handled": 0,

                "rows_published": 0,

                "warehouse_rows": 0,

                "healthy": False,

                "reasons": reasons,

                "tasks": tasks
            }

            status["tasks"][
                "notify"
            ] = {
                "status": "SUCCESS",
                "timestamp": datetime.now().isoformat()
            }

            write_status(
                status
            )

            logger.error(
                "DE7 PIPELINE FAILED: %s",
                ", ".join(failed_tasks)
            )

        logger.info(
            "========== NOTIFY COMPLETE =========="
        )

    except Exception as e:

        logger.exception(
            "NOTIFY FAILED"
        )

        raise


# =========================================================
# AIRFLOW DAG
# =========================================================

with DAG(
    dag_id="de7_end_to_end",
    description="DE7 End-to-End Telecom Data Pipeline",
    start_date=datetime(
        2026,
        1,
        1
    ),
    schedule=None,
    catchup=False,
    tags=[
        "capstone1",
        "DE7",
        "telecom"
    ]
) as dag:

    ingest = PythonOperator(
        task_id="ingest",
        python_callable=ingest_task
    )

    validate = PythonOperator(
        task_id="validate",
        python_callable=validate_task
    )

    spark_process = PythonOperator(
        task_id="spark_process",
        python_callable=spark_process_task
    )

    load_warehouse = PythonOperator(
        task_id="load_warehouse",
        python_callable=load_warehouse_task
    )

    quality_check = PythonOperator(
        task_id="quality_check",
        python_callable=quality_check_task
    )

    notify = PythonOperator(
        task_id="notify",
        python_callable=notify_task,
        trigger_rule=TriggerRule.ALL_DONE
    )

    # =====================================================
    # DEPENDENCIES
    # =====================================================

    ingest >> validate >> spark_process >> load_warehouse >> quality_check >> notify