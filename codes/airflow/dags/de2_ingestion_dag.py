
import sys

PROJECT_DIR = "/mnt/d/capstone1"

sys.path.insert(
    0,
    PROJECT_DIR
)


from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from de2_ingestion import (
    detect_files,
    validate_files,
    route_files,
    log_results
)


# =========================================================
# AIRFLOW TASK FUNCTIONS
# =========================================================

def detect_task():

    files = detect_files()

    if not files:

        print(
            "No files detected in landing."
        )

        return

    print(
        f"Detected {len(files)} file(s)."
    )


def validate_task():

    files = detect_files()

    if not files:

        print(
            "No files available for validation."
        )

        return

    results = validate_files(
        files
    )

    print(
        f"Validated {len(results)} file(s)."
    )


def route_task():

    files = detect_files()

    if not files:

        print(
            "No files available for routing."
        )

        return

    results = validate_files(
        files
    )

    routed_results = route_files(
        results
    )

    print(
        f"Routed {len(routed_results)} file(s)."
    )


def log_task():

    files = detect_files()

    if not files:

        print(
            "No files available for logging."
        )

        return

    results = validate_files(
        files
    )

    results = route_files(
        results
    )

    log_results(
        results
    )


# =========================================================
# DAG
# =========================================================

with DAG(

    dag_id="de2_landing_to_raw",

    start_date=datetime(
        2026,
        8,
        30
    ),

    schedule=None,

    catchup=False,

    tags=[
        "DE2",
        "telecom",
        "ingestion"
    ]

) as dag:


    detect = PythonOperator(

        task_id="detect_files",

        python_callable=detect_task

    )


    validate = PythonOperator(

        task_id="validate_files",

        python_callable=validate_task

    )


    route = PythonOperator(

        task_id="route_files",

        python_callable=route_task

    )


    log = PythonOperator(

        task_id="log_ingestion",

        python_callable=log_task

    )


    detect >> validate >> route >> log

