
import os
import glob
import shutil
import logging
import pandas as pd


# =========================================================
# PROJECT DIRECTORIES
# =========================================================

PROJECT_DIR = os.path.dirname(
    os.path.abspath(__file__)
)

DATA_DIR = os.path.join(
    PROJECT_DIR,
    "data"
)

LANDING_DIR = os.path.join(
    DATA_DIR,
    "landing"
)

RAW_DIR = os.path.join(
    DATA_DIR,
    "raw"
)

REJECTED_DIR = os.path.join(
    DATA_DIR,
    "rejected"
)

LOG_DIR = os.path.join(
    PROJECT_DIR,
    "logs"
)

PATTERN = "sms-call-internet-mi-2013-11-01.csv"


# =========================================================
# REQUIRED COLUMNS
# =========================================================

REQUIRED_COLUMNS = [
    "datetime",
    "CellID",
    "smsin",
    "smsout",
    "callin",
    "callout",
    "internet"
]

ACTIVITY_COLUMNS = [
    "smsin",
    "smsout",
    "callin",
    "callout",
    "internet"
]


# =========================================================
# CREATE DIRECTORIES
# =========================================================

os.makedirs(LANDING_DIR, exist_ok=True)
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(REJECTED_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)


# =========================================================
# LOGGING
# =========================================================

LOG_FILE = os.path.join(
    LOG_DIR,
    "ingestion.log"
)

logger = logging.getLogger("DE2_Ingestion")
logger.setLevel(logging.INFO)

if not logger.handlers:

    file_handler = logging.FileHandler(
        LOG_FILE
    )

    formatter = logging.Formatter(
        "%(asctime)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)


# =========================================================
# 1. DETECT FILES
# =========================================================

def detect_files():

    pattern = os.path.join(
        LANDING_DIR,
        PATTERN
    )

    files = glob.glob(pattern)

    print(
        f"Landing directory: {LANDING_DIR}"
    )

    print(
        f"Detected {len(files)} file(s)."
    )

    for file_path in files:
        print(
            f"  {os.path.basename(file_path)}"
        )

    return files


# =========================================================
# 2. VALIDATE SCHEMA
# =========================================================

def validate_schema(file_path):

    try:

        df = pd.read_csv(
            file_path
        )

    except Exception as e:

        return (
            False,
            0,
            f"Unable to read CSV: {e}"
        )

    missing_columns = [
        column
        for column in REQUIRED_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:

        return (
            False,
            len(df),
            "Schema validation failed: "
            f"missing column(s): {missing_columns}"
        )

    return (
        True,
        len(df),
        "Schema validation passed"
    )


# =========================================================
# 3. VALIDATE MINIMUM QUALITY
# =========================================================

def validate_minimum_quality(file_path):

    try:

        df = pd.read_csv(
            file_path
        )

    except Exception as e:

        return (
            False,
            f"Unable to read CSV: {e}"
        )

    # -----------------------------------------------------
    # Timestamp validation
    # -----------------------------------------------------

    datetimes = pd.to_datetime(
        df["datetime"],
        errors="coerce"
    )

    if datetimes.isna().any():

        return (
            False,
            "Minimum quality validation failed: "
            "malformed datetime"
        )

    # -----------------------------------------------------
    # Null validation
    # -----------------------------------------------------

    # for column in REQUIRED_COLUMNS:

    #     if df[column].isna().any():

    #         return (
    #             False,
    #             "Minimum quality validation failed: "
    #             f"null value found in {column}"
    #         )

    # # -----------------------------------------------------
    # # Numeric activity validation
    # # -----------------------------------------------------

    # for column in ACTIVITY_COLUMNS:

    #     numeric_values = pd.to_numeric(
    #         df[column],
    #         errors="coerce"
    #     )

    #     if numeric_values.isna().any():

    #         return (
    #             False,
    #             "Minimum quality validation failed: "
    #             f"non-numeric value in {column}"
    #         )

    #     if (numeric_values < 0).any():

    #         return (
    #             False,
    #             "Minimum quality validation failed: "
    #             f"negative value found in {column}"
    #         )

    # -----------------------------------------------------
    # CellID validation
    # -----------------------------------------------------

    cell_ids = pd.to_numeric(
        df["CellID"],
        errors="coerce"
    )

    if cell_ids.isna().any():

        return (
            False,
            "Minimum quality validation failed: "
            "invalid CellID"
        )

    return (
        True,
        "Minimum quality validation passed"
    )


# =========================================================
# 4. ROUTE FILE
# =========================================================

def route_file(
    file_path,
    status,
    reason
):

    filename = os.path.basename(
        file_path
    )

    if status == "ACCEPTED":

        destination_dir = RAW_DIR

    else:

        destination_dir = REJECTED_DIR

    destination = os.path.join(
        destination_dir,
        filename
    )

    # -----------------------------------------------------
    # Already processed
    # -----------------------------------------------------

    if os.path.exists(destination):

        duplicate_reason = (
            f"File already exists at "
            f"{destination}; skipped"
        )

        print(
            f"Already processed: {filename}"
        )

        return (
            "ALREADY_PROCESSED",
            duplicate_reason
        )

    # -----------------------------------------------------
    # Copy unchanged
    # -----------------------------------------------------

    shutil.copy2(
        file_path,
        destination
    )

    print(
        f"{filename} -> {destination}"
    )

    return (
        status,
        reason
    )


# =========================================================
# 5. WRITE INGESTION LOG
# =========================================================

def write_ingestion_log(
    filename,
    status,
    row_count,
    reason
):

    logger.info(
        "filename=%s | status=%s | row_count=%s | reason=%s",
        filename,
        status,
        row_count,
        reason
    )


# =========================================================
# 6. VALIDATE ALL FILES
# =========================================================

def validate_files(files):

    results = []

    for file_path in files:

        filename = os.path.basename(
            file_path
        )

        schema_valid, row_count, reason = (
            validate_schema(file_path)
        )

        if not schema_valid:

            results.append({
                "file_path": file_path,
                "filename": filename,
                "status": "REJECTED",
                "row_count": row_count,
                "reason": reason
            })

            continue

        quality_valid, quality_reason = (
            validate_minimum_quality(
                file_path
            )
        )

        if not quality_valid:

            results.append({
                "file_path": file_path,
                "filename": filename,
                "status": "REJECTED",
                "row_count": row_count,
                "reason": quality_reason
            })

            continue

        results.append({
            "file_path": file_path,
            "filename": filename,
            "status": "ACCEPTED",
            "row_count": row_count,
            "reason": "All validation checks passed"
        })

    return results


# =========================================================
# 7. ROUTE ALL FILES
# =========================================================

def route_files(results):

    routed_results = []

    for result in results:

        final_status, final_reason = route_file(
            result["file_path"],
            result["status"],
            result["reason"]
        )

        result["status"] = final_status
        result["reason"] = final_reason

        routed_results.append(
            result
        )

    return routed_results


# =========================================================
# 8. LOG ALL FILES
# =========================================================

def log_results(results):

    for result in results:

        write_ingestion_log(
            result["filename"],
            result["status"],
            result["row_count"],
            result["reason"]
        )

    print(
        f"Logged {len(results)} file(s)."
    )


# =========================================================
# MAIN
# =========================================================

def main():

    files = detect_files()

    if not files:

        print(
            "No files found in landing directory."
        )

        raise FileNotFoundError(
            "No input files found in landing directory."
        )

    results = validate_files(
        files
    )

    results = route_files(
        results
    )

    log_results(
        results
    )

    accepted = sum(
        1
        for result in results
        if result["status"] == "ACCEPTED"
    )

    rejected = sum(
        1
        for result in results
        if result["status"] == "REJECTED"
    )

    already_processed = sum(
        1
        for result in results
        if result["status"] == "ALREADY_PROCESSED"
    )

    rows_in = sum(
        result["row_count"]
        for result in results
    )

    if rejected > 0:

        raise ValueError(
            f"DE2 validation failed: "
            f"{rejected} file(s) rejected."
        )

    return {
        "files_detected": len(files),
        "accepted_files": accepted,
        "rejected_files": rejected,
        "already_processed": already_processed,
        "rows_in": rows_in,
        "rows_rejected": 0,
        "rows_published": rows_in
    }


if __name__ == "__main__":

    main()

