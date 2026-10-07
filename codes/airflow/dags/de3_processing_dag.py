
import sys

PROJECT_DIR = "/mnt/d/capstone1"

sys.path.insert(
    0,
    PROJECT_DIR
)
import os
import subprocess

from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


# =========================================================
# PATHS
# =========================================================


SPARK_JOB = os.path.join(
    PROJECT_DIR,
    "spark",
    "telecom_pipeline.py"
)

RAW_DIR = os.path.join(
    PROJECT_DIR,
    "data",
    "raw"
)

REFERENCE_DIR = os.path.join(
    PROJECT_DIR,
    "data",
    "reference"
)

OUTPUT_DIR = os.path.join(
    PROJECT_DIR,
    "data",
    "final_outputde7"
)

LOG_DIR = os.path.join(
    PROJECT_DIR,
    "logs"
)


# =========================================================
# INGESTION CHECK
# =========================================================

def check_ingestion():
    import glob

    pattern = os.path.join(
        RAW_DIR,
        "sms-call-internet-mi-2013-11-01.csv"
    )

    files = glob.glob(pattern)

    if not files:
        raise FileNotFoundError(
            f"No valid raw files found in {RAW_DIR}"
        )

    print(f"Ingestion successful: {len(files)} file(s) found")

    for file in files:
        print(f"  {file}")


# =========================================================
# RUN SPARK PIPELINE
# =========================================================

def run_spark_pipeline():

    os.makedirs(LOG_DIR, exist_ok=True)

    command = [
        sys.executable,
        SPARK_JOB,
        "--input",
        RAW_DIR,
        "--reference",
        REFERENCE_DIR,
        "--output",
        OUTPUT_DIR,
        "--logs",
        LOG_DIR,
    ]

    print("Starting Spark processing job...")
    print("Command:", " ".join(command))

    result = subprocess.run(
        command,
        capture_output=True,
        text=True
    )

    print("----- SPARK STDOUT -----")
    print(result.stdout)

    print("----- SPARK STDERR -----")
    print(result.stderr)

    print(f"Spark exit code: {result.returncode}")

    if result.returncode != 0:
        raise RuntimeError(
            f"Spark processing failed with exit code "
            f"{result.returncode}"
        )

    print("Spark processing completed successfully.")


# =========================================================
# DAG
# =========================================================

with DAG(
    dag_id="telecom_de3",
    start_date=datetime(2026, 8, 1),
    schedule=None,
    catchup=False,
    tags=["telecom", "DE3"],
) as dag:

    ingestion = PythonOperator(
        task_id="ingestion_success",
        python_callable=check_ingestion,
    )

    spark_processing = PythonOperator(
        task_id="spark_processing",
        python_callable=run_spark_pipeline,
    )

    # -----------------------------------------------------
    # DEPENDENCY
    # -----------------------------------------------------

    ingestion >> spark_processing
