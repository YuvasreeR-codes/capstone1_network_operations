# `de6_warehouse.py`
import os
import json
import logging
from datetime import datetime

import mysql.connector
from mysql.connector import Error

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    date_format,
    dayofweek,
    hour,
    to_timestamp,
    lit
)

import os
os.environ["HADOOP_HOME"] = r"C:\Users\yuvasree.r\Desktop\python test\hadoop"
os.environ["PATH"] = os.environ["PATH"] + r";C:\Users\yuvasree.r\Desktop\python test\hadoop\bin"
# =========================================================
# CONFIGURATION
# =========================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
#hourly gris summary path changed to de7 output
HOURLY_PATH = os.path.join(
    BASE_DIR,
    "data",
    "final_outputde7",
    "analytics",
    "hourly_grid_summary"
)

GEOJSON_PATH = os.path.join(
    BASE_DIR,
    "data",
    "reference",
    "milano-grid.geojson"
)

LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(
    LOG_DIR,
    "de6_warehouse.log"
)


# =========================================================
# MYSQL CONFIGURATION
# =========================================================

MYSQL_HOST = "172.29.64.1"
# MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "root"
MYSQL_DATABASE = "capstone1airflow"

# Batch size for MySQL inserts
BATCH_SIZE = 70000


# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# =========================================================
# SPARK SESSION
# =========================================================

spark = (
    SparkSession.builder
    .appName("DE6_Warehouse_Load")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =========================================================
# MYSQL CONNECTION
# =========================================================

def create_database():

    connection = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD
    )

    cursor = connection.cursor()

    cursor.execute(
        f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DATABASE}`"
    )

    connection.commit()

    cursor.close()
    connection.close()

    logger.info(
        "Database checked/created: %s",
        MYSQL_DATABASE
    )


def get_connection():

    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE
    )


# =========================================================
# CREATE TABLES
# =========================================================

def create_tables():

    connection = get_connection()
    cursor = connection.cursor()

    # -----------------------------------------------------
    # DIM TIME
    # -----------------------------------------------------

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_time (
            time_key VARCHAR(20) PRIMARY KEY,
            timestamp DATETIME NOT NULL,
            date DATE NOT NULL,
            hour INT NOT NULL,
            day_of_week VARCHAR(20)
        )
    """)

    # -----------------------------------------------------
    # DIM GRID
    # -----------------------------------------------------

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_grid (
            grid_key INT AUTO_INCREMENT PRIMARY KEY,
            grid_id VARCHAR(100) NOT NULL UNIQUE,
            latitude DOUBLE,
            longitude DOUBLE,
            geometry_reference VARCHAR(255)
        )
    """)

    # -----------------------------------------------------
    # FACT NETWORK ACTIVITY
    # -----------------------------------------------------

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fact_network_activity (

            activity_key BIGINT AUTO_INCREMENT PRIMARY KEY,

            time_key VARCHAR(20) NOT NULL,
            grid_key INT NOT NULL,

            sms_in DOUBLE,
            sms_out DOUBLE,
            call_in DOUBLE,
            call_out DOUBLE,

            total_sms DOUBLE,
            total_calls DOUBLE,

            internet_activity DOUBLE,
            total_activity DOUBLE,

            CONSTRAINT fk_fact_time
                FOREIGN KEY (time_key)
                REFERENCES dim_time(time_key),

            CONSTRAINT fk_fact_grid
                FOREIGN KEY (grid_key)
                REFERENCES dim_grid(grid_key),

            UNIQUE KEY uq_fact_time_grid
                (time_key, grid_key)
        )
    """)

    connection.commit()

    cursor.close()
    connection.close()

    logger.info("Warehouse tables created.")


# =========================================================
# CREATE INDEXES
# =========================================================

def create_indexes():

    connection = get_connection()
    cursor = connection.cursor()

    indexes = [

        """
        CREATE INDEX idx_fact_time
        ON fact_network_activity(time_key)
        """,

        """
        CREATE INDEX idx_fact_grid
        ON fact_network_activity(grid_key)
        """,

        """
        CREATE INDEX idx_fact_grid_time
        ON fact_network_activity(grid_key, time_key)
        """,

        """
        CREATE INDEX idx_dim_time_date
        ON dim_time(date)
        """,

        """
        CREATE INDEX idx_dim_grid_id
        ON dim_grid(grid_id)
        """
    ]

    for sql in indexes:

        try:

            cursor.execute(sql)

        except Error as e:

            logger.warning(
                "Index may already exist: %s",
                e
            )

    connection.commit()

    cursor.close()
    connection.close()

    logger.info("Indexes created.")


# =========================================================
# LOAD GRID DIMENSION
# =========================================================
# =========================================================
# LOAD GRID DIMENSION
# =========================================================

def load_dim_grid():

    logger.info("Loading dim_grid...")

    if not os.path.exists(GEOJSON_PATH):
        raise FileNotFoundError(
            f"GeoJSON not found: {GEOJSON_PATH}"
        )

    with open(
        GEOJSON_PATH,
        "r",
        encoding="utf-8"
    ) as f:

        geojson = json.load(f)

    features = geojson.get("features", [])

    if not features:
        raise ValueError(
            "No features found in GeoJSON."
        )

    connection = get_connection()
    cursor = connection.cursor()

    batch = []
    total = 0

    for feature in features:

        properties = feature.get(
            "properties",
            {}
        )

        # -------------------------------------------------
        # Grid ID
        # -------------------------------------------------

        grid_id = (
            properties.get("cellId")
            or properties.get("grid_id")
            or properties.get("gridId")
        )

        if grid_id is None:
            logger.warning(
                "Feature has no grid ID. Skipping."
            )
            continue

        # -------------------------------------------------
        # Geometry
        # -------------------------------------------------

        geometry = feature.get(
            "geometry",
            {}
        )

        geometry_type = geometry.get(
            "type"
        )

        coordinates = geometry.get(
            "coordinates"
        )

        latitude = None
        longitude = None

        # -------------------------------------------------
        # Calculate centroid for Polygon
        #
        # GeoJSON coordinate format:
        # [longitude, latitude]
        # -------------------------------------------------

        if (
            geometry_type == "Polygon"
            and coordinates
        ):

            # First ring = outer boundary
            outer_ring = coordinates[0]

            if outer_ring:

                longitudes = [
                    point[0]
                    for point in outer_ring
                    if len(point) >= 2
                ]

                latitudes = [
                    point[1]
                    for point in outer_ring
                    if len(point) >= 2
                ]

                if longitudes and latitudes:

                    longitude = (
                        sum(longitudes)
                        / len(longitudes)
                    )

                    latitude = (
                        sum(latitudes)
                        / len(latitudes)
                    )

        # -------------------------------------------------
        # Handle Point geometry if encountered
        # -------------------------------------------------

        elif (
            geometry_type == "Point"
            and coordinates
            and len(coordinates) >= 2
        ):

            longitude = coordinates[0]
            latitude = coordinates[1]

        # -------------------------------------------------
        # Handle missing geometry
        # -------------------------------------------------

        if latitude is None or longitude is None:

            logger.warning(
                "Could not calculate centroid "
                "for grid_id=%s",
                grid_id
            )

        batch.append(
            (
                str(grid_id),
                latitude,
                longitude,
                "milano-grid.geojson"
            )
        )

        # -------------------------------------------------
        # Batch insert/update
        # -------------------------------------------------

        if len(batch) >= BATCH_SIZE:

            cursor.executemany(
                """
                INSERT INTO dim_grid
                (
                    grid_id,
                    latitude,
                    longitude,
                    geometry_reference
                )
                VALUES
                (
                    %s,
                    %s,
                    %s,
                    %s
                )

                ON DUPLICATE KEY UPDATE

                    latitude =
                        VALUES(latitude),

                    longitude =
                        VALUES(longitude),

                    geometry_reference =
                        VALUES(geometry_reference)
                """,
                batch
            )

            connection.commit()

            total += len(batch)

            logger.info(
                "dim_grid rows processed: %d",
                total
            )

            batch.clear()

    # -----------------------------------------------------
    # Insert remaining rows
    # -----------------------------------------------------

    if batch:

        cursor.executemany(
            """
            INSERT INTO dim_grid
            (
                grid_id,
                latitude,
                longitude,
                geometry_reference
            )
            VALUES
            (
                %s,
                %s,
                %s,
                %s
            )

            ON DUPLICATE KEY UPDATE

                latitude =
                    VALUES(latitude),

                longitude =
                    VALUES(longitude),

                geometry_reference =
                    VALUES(geometry_reference)
            """,
            batch
        )

        connection.commit()

        total += len(batch)

    cursor.close()
    connection.close()

    logger.info(
        "dim_grid loading complete. "
        "Rows processed: %d",
        total
    )
# =========================================================
# READ HOURLY GRID SUMMARY
# =========================================================

def load_hourly_data():

    logger.info(
        "Reading hourly_grid_summary..."
    )

    if not os.path.exists(HOURLY_PATH):

        raise FileNotFoundError(
            f"Hourly output not found: {HOURLY_PATH}"
        )

    df = spark.read.parquet(
        HOURLY_PATH
    )

    logger.info(
        "Input columns: %s",
        df.columns
    )

    input_count = df.count()

    logger.info(
        "Input rows: %d",
        input_count
    )

    # -----------------------------------------------------
    # Required columns
    # -----------------------------------------------------

    if "timestamp" not in df.columns:

        raise ValueError(
            "hourly_grid_summary must contain timestamp."
        )

    if "grid_id" not in df.columns:

        raise ValueError(
            "hourly_grid_summary must contain grid_id."
        )

    # -----------------------------------------------------
    # Convert timestamp
    # -----------------------------------------------------

    df = df.withColumn(
        "timestamp",
        to_timestamp(col("timestamp"))
    )

    # -----------------------------------------------------
    # Derive date
    # -----------------------------------------------------

    df = df.withColumn(
        "date",
        date_format(
            col("timestamp"),
            "yyyy-MM-dd"
        )
    )

    # -----------------------------------------------------
    # Derive hour
    # -----------------------------------------------------

    df = df.withColumn(
        "hour",
        hour(col("timestamp"))
    )

    # -----------------------------------------------------
    # Derive day of week
    # -----------------------------------------------------

    df = df.withColumn(
        "day_of_week",
        date_format(
            col("timestamp"),
            "EEEE"
        )
    )

    # -----------------------------------------------------
    # Time key
    # -----------------------------------------------------

    df = df.withColumn(
        "time_key",
        date_format(
            col("timestamp"),
            "yyyyMMdd_HHmm"
        )
    )

    # -----------------------------------------------------
    # Measures
    # -----------------------------------------------------

    required_measures = [

        "sms_in",
        "sms_out",

        "call_in",
        "call_out",

        "total_sms",
        "total_calls",

        "internet_activity",
        "total_activity"
    ]

    for column_name in required_measures:

        if column_name not in df.columns:

            logger.warning(
                "Column %s not found. "
                "Using 0.",
                column_name
            )

            df = df.withColumn(
                column_name,
                lit(0)
            )

    # -----------------------------------------------------
    # Select required columns
    # -----------------------------------------------------

    df = df.select(

        "time_key",
        "timestamp",
        "date",
        "hour",
        "day_of_week",

        "grid_id",

        "sms_in",
        "sms_out",

        "call_in",
        "call_out",

        "total_sms",
        "total_calls",

        "internet_activity",
        "total_activity"
    )

    return df


# =========================================================
# LOAD DIM TIME
# =========================================================

def load_dim_time(df):

    logger.info(
        "Loading dim_time..."
    )

    time_df = (
        df
        .select(
            "time_key",
            "timestamp",
            "date",
            "hour",
            "day_of_week"
        )
        .dropDuplicates(
            ["time_key"]
        )
    )

    connection = get_connection()
    cursor = connection.cursor()

    batch = []
    total = 0

    # -----------------------------------------------------
    # Process without collecting entire dataset
    # -----------------------------------------------------

    for row in time_df.toLocalIterator():

        timestamp_value = row["timestamp"]

        batch.append(
            (
                row["time_key"],

                timestamp_value
                if timestamp_value is not None
                else None,

                row["date"],
                row["hour"],
                row["day_of_week"]
            )
        )

        # -------------------------------------------------
        # Batch insert
        # -------------------------------------------------

        if len(batch) >= BATCH_SIZE:

            cursor.executemany(
                """
                INSERT INTO dim_time
                (
                    time_key,
                    timestamp,
                    date,
                    hour,
                    day_of_week
                )
                VALUES
                (%s, %s, %s, %s, %s)

                ON DUPLICATE KEY UPDATE

                    timestamp =
                        VALUES(timestamp),

                    date =
                        VALUES(date),

                    hour =
                        VALUES(hour),

                    day_of_week =
                        VALUES(day_of_week)
                """,
                batch
            )

            connection.commit()

            total += len(batch)

            logger.info(
                "dim_time rows processed: %d",
                total
            )

            batch.clear()

    # -----------------------------------------------------
    # Remaining rows
    # -----------------------------------------------------

    if batch:

        cursor.executemany(
            """
            INSERT INTO dim_time
            (
                time_key,
                timestamp,
                date,
                hour,
                day_of_week
            )
            VALUES
            (%s, %s, %s, %s, %s)

            ON DUPLICATE KEY UPDATE

                timestamp =
                    VALUES(timestamp),

                date =
                    VALUES(date),

                hour =
                    VALUES(hour),

                day_of_week =
                    VALUES(day_of_week)
            """,
            batch
        )

        connection.commit()

        total += len(batch)

    cursor.close()
    connection.close()

    logger.info(
        "dim_time loading complete. "
        "Rows processed: %d",
        total
    )


# =========================================================
# LOAD FACT TABLE
# =========================================================

def load_fact(df):

    logger.info(
        "Loading fact_network_activity..."
    )

    # -----------------------------------------------------
    # Load grid mapping
    # -----------------------------------------------------

    connection = get_connection()
    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT
            grid_key,
            grid_id
        FROM dim_grid
        """
    )

    grid_mapping = {
        str(row[1]): row[0]
        for row in cursor.fetchall()
    }

    cursor.close()
    connection.close()

    logger.info(
        "Grid mappings loaded: %d",
        len(grid_mapping)
    )

    # -----------------------------------------------------
    # Process Spark partitions
    # -----------------------------------------------------

    def process_partition(rows):

        connection = get_connection()
        cursor = connection.cursor()

        batch = []
        count = 0

        for row in rows:

            grid_id = str(
                row["grid_id"]
            )

            grid_key = grid_mapping.get(
                grid_id
            )

            # -------------------------------------------------
            # Skip unknown grid
            # -------------------------------------------------

            if grid_key is None:

                logger.warning(
                    "Grid ID not found: %s",
                    grid_id
                )

                continue

            batch.append(
                (

                    row["time_key"],

                    grid_key,

                    row["sms_in"],
                    row["sms_out"],

                    row["call_in"],
                    row["call_out"],

                    row["total_sms"],
                    row["total_calls"],

                    row["internet_activity"],
                    row["total_activity"]
                )
            )

            # -------------------------------------------------
            # Batch insert
            # -------------------------------------------------

            if len(batch) >= BATCH_SIZE:

                cursor.executemany(
                    """
                    INSERT INTO
                    fact_network_activity
                    (
                        time_key,
                        grid_key,

                        sms_in,
                        sms_out,

                        call_in,
                        call_out,

                        total_sms,
                        total_calls,

                        internet_activity,
                        total_activity
                    )
                    VALUES
                    (
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s
                    )

                    ON DUPLICATE KEY UPDATE

                        sms_in =
                            VALUES(sms_in),

                        sms_out =
                            VALUES(sms_out),

                        call_in =
                            VALUES(call_in),

                        call_out =
                            VALUES(call_out),

                        total_sms =
                            VALUES(total_sms),

                        total_calls =
                            VALUES(total_calls),

                        internet_activity =
                            VALUES(internet_activity),

                        total_activity =
                            VALUES(total_activity)
                    """,
                    batch
                )

                connection.commit()

                count += len(batch)

                batch.clear()

        # -----------------------------------------------------
        # Remaining rows
        # -----------------------------------------------------

        if batch:

            cursor.executemany(
                """
                INSERT INTO
                fact_network_activity
                (
                    time_key,
                    grid_key,

                    sms_in,
                    sms_out,

                    call_in,
                    call_out,

                    total_sms,
                    total_calls,

                    internet_activity,
                    total_activity
                )
                VALUES
                (
                    %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s
                )

                ON DUPLICATE KEY UPDATE

                    sms_in =
                        VALUES(sms_in),

                    sms_out =
                        VALUES(sms_out),

                    call_in =
                        VALUES(call_in),

                    call_out =
                        VALUES(call_out),

                    total_sms =
                        VALUES(total_sms),

                    total_calls =
                        VALUES(total_calls),

                    internet_activity =
                        VALUES(internet_activity),

                    total_activity =
                        VALUES(total_activity)
                """,
                batch
            )

            connection.commit()

            count += len(batch)

        cursor.close()
        connection.close()

        yield count

    # -----------------------------------------------------
    # Execute partition processing
    # -----------------------------------------------------

    results = (
        df.rdd
        .mapPartitions(
            process_partition
        )
        .collect()
    )

    total_loaded = sum(results)

    logger.info(
        "fact_network_activity loading "
        "complete. Rows loaded: %d",
        total_loaded
    )


# =========================================================
# VALIDATION
# =========================================================

def validate_warehouse():

    logger.info(
        "Starting warehouse validation..."
    )

    connection = get_connection()
    cursor = connection.cursor()

    queries = {

        # -------------------------------------------------
        # Dimension counts
        # -------------------------------------------------

        "dim_grid_count":
        """
        SELECT COUNT(*)
        FROM dim_grid
        """,

        "dim_time_count":
        """
        SELECT COUNT(*)
        FROM dim_time
        """,

        # -------------------------------------------------
        # Fact count
        # -------------------------------------------------

        "fact_count":
        """
        SELECT COUNT(*)
        FROM fact_network_activity
        """,

        # -------------------------------------------------
        # Top grids
        # -------------------------------------------------

        "top_grids":
        """
        SELECT
            g.grid_id,
            SUM(f.total_activity)
                AS total_activity
        FROM fact_network_activity f

        JOIN dim_grid g
            ON f.grid_key = g.grid_key

        GROUP BY
            g.grid_id

        ORDER BY
            total_activity DESC

        LIMIT 10
        """,

        # -------------------------------------------------
        # Hourly trend
        # -------------------------------------------------

        "hourly_trend":
        """
        SELECT
            t.date,
            t.hour,
            SUM(f.total_activity)
                AS total_activity
        FROM fact_network_activity f

        JOIN dim_time t
            ON f.time_key = t.time_key

        GROUP BY
            t.date,
            t.hour

        ORDER BY
            t.date,
            t.hour

        LIMIT 20
        """,

        # -------------------------------------------------
        # Internet-heavy windows
        # -------------------------------------------------

        "internet_heavy":
        """
        SELECT
            t.date,
            t.hour,
            SUM(f.internet_activity)
                AS internet_activity
        FROM fact_network_activity f

        JOIN dim_time t
            ON f.time_key = t.time_key

        GROUP BY
            t.date,
            t.hour

        ORDER BY
            internet_activity DESC

        LIMIT 10
        """,

        # -------------------------------------------------
        # SMS and call direction validation
        # -------------------------------------------------

        "directional_activity":
        """
        SELECT
            SUM(sms_in) AS sms_in,
            SUM(sms_out) AS sms_out,
            SUM(call_in) AS call_in,
            SUM(call_out) AS call_out,
            SUM(total_sms) AS total_sms,
            SUM(total_calls) AS total_calls
        FROM fact_network_activity
        """,

        # -------------------------------------------------
        # Busiest grid/time combinations
        # -------------------------------------------------

        "top_grid_time":
        """
        SELECT
            g.grid_id,
            t.date,
            t.hour,
            f.total_activity
        FROM fact_network_activity f

        JOIN dim_grid g
            ON f.grid_key = g.grid_key

        JOIN dim_time t
            ON f.time_key = t.time_key

        ORDER BY
            f.total_activity DESC

        LIMIT 10
        """
    }

    # -----------------------------------------------------
    # Execute queries
    # -----------------------------------------------------

    for name, sql in queries.items():

        logger.info(
            "Running validation: %s",
            name
        )

        cursor.execute(sql)

        rows = cursor.fetchall()

        logger.info(
            "%s result: %s",
            name,
            rows
        )

    cursor.close()
    connection.close()

    logger.info(
        "Warehouse validation complete."
    )


# =========================================================
# MAIN
# =========================================================

def main():

    start_time = datetime.now()

    logger.info("=" * 70)
    logger.info(
        "DE6 WAREHOUSE LOAD STARTED"
    )
    logger.info("=" * 70)

    try:

        # -------------------------------------------------
        # 1. Create database
        # -------------------------------------------------

        create_database()

        # -------------------------------------------------
        # 2. Create warehouse tables
        # -------------------------------------------------

        create_tables()

        # -------------------------------------------------
        # 3. Load grid dimension
        # -------------------------------------------------

        load_dim_grid()

        # -------------------------------------------------
        # 4. Read Spark output
        # -------------------------------------------------

        df = load_hourly_data()

        # -------------------------------------------------
        # 5. Load time dimension
        # -------------------------------------------------

        load_dim_time(df)

        # -------------------------------------------------
        # 6. Load fact table
        # -------------------------------------------------

        load_fact(df)

        # -------------------------------------------------
        # 7. Create indexes
        # -------------------------------------------------

        create_indexes()

        # -------------------------------------------------
        # 8. Validate
        # -------------------------------------------------

        validate_warehouse()

        end_time = datetime.now()

        logger.info(
            "DE6 SUCCESS"
        )

        logger.info(
            "Start time: %s",
            start_time
        )

        logger.info(
            "End time: %s",
            end_time
        )

        logger.info(
            "Duration: %s",
            end_time - start_time
        )

        logger.info("=" * 70)

    except Exception as e:

        logger.exception(
            "DE6 FAILED: %s",
            e
        )

        raise

    finally:

        spark.stop()


# =========================================================
# ENTRY POINT
# =========================================================

if __name__ == "__main__":

    main()

