import json
import math
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import broadcast
from pyspark.sql import SparkSession
# ============================================================
# SP4 - Geospatial Enrichment Using the Milan Grid
# ============================================================
import os
os.environ["HADOOP_HOME"] = r"C:\Users\yuvasree.r\Desktop\python test\hadoop"
os.environ["PATH"] = os.environ["PATH"] + r";C:\Users\yuvasree.r\Desktop\python test\hadoop\bin"

spark = SparkSession.builder.appName("SP3_milan").master("local[2]").getOrCreate()
hourly_grid_summary =  spark.read.parquet(r"parquet_output\hourly_grid_parquet")

GEOJSON_PATH = "data/landing/milano-grid.geojson"

# ============================================================
# 1. LOAD AND INSPECT GEOJSON
# ============================================================

print("\n========== GEOJSON INSPECTION ==========")

with open(GEOJSON_PATH, "r", encoding="utf-8") as f:
    geojson_data = json.load(f)

# Top-level structure
print("Top-level type:", geojson_data.get("type"))

# Number of features
features = geojson_data.get("features", [])
print("Number of features:", len(features))

# Inspect first feature
first_feature = features[0]
print("\nFirst feature top-level keys:")
print(first_feature.keys())
print("\nFirst feature ID:")
print(first_feature.get("id"))
print("\nFirst feature properties:")
print(first_feature.get("properties"))
print("\nFirst feature geometry type:")
print(first_feature.get("geometry", {}).get("type"))

# ============================================================
# 2. IMPORTANT IDENTIFIER CHECK
# ============================================================

# IMPORTANT:
# The telecom grid_id MUST come from:
#
#     feature["properties"]["cellId"]
#
# NOT from:
#
#     feature["id"]
#
# GeoJSON feature "id" is 0-based.
# properties.cellId is 1-based and is the correct project key.

assert "cellId" in first_feature["properties"], "GeoJSON does not contain properties.cellId"
print("\nCorrect join key: properties.cellId -> grid_id")

# ============================================================
# 3. FLATTEN FEATURES INTO GRID LOOKUP
# ============================================================

grid_records = []

for feature in features:
    properties = feature.get("properties", {})
    geometry = feature.get("geometry")

    # --------------------------------------------------------
    # IMPORTANT:
    # Map properties.cellId -> grid_id
    # --------------------------------------------------------

    grid_id = properties.get("cellId")
    assert grid_id is not None, "Feature missing properties.cellId"

    # Keep geometry as GeoJSON text.
    # This is convenient for later map/API visualization.

    geometry_json = json.dumps(geometry, separators=(",", ":"))

    grid_records.append((int(grid_id), geometry_json))

# ============================================================
# 4. CREATE SPARK GRID LOOKUP
# ============================================================

grid_schema = StructType([
    StructField("grid_id", IntegerType(), False),
    StructField("geometry", StringType(), False)
])

grid_lookup_df = spark.createDataFrame(grid_records, schema=grid_schema)

print("\n========== GRID LOOKUP ==========")
print("Grid lookup rows:", grid_lookup_df.count())
grid_lookup_df.show(5, truncate=False)

# ============================================================
# 5. VALIDATE GRID LOOKUP
# ============================================================

grid_lookup_count = grid_lookup_df.count()

duplicate_grid_keys = (
    grid_lookup_df
    .groupBy("grid_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print("\nDuplicate grid IDs:", duplicate_grid_keys)

assert duplicate_grid_keys == 0, (
    "Grid lookup contains duplicate grid_id values. "
    "A left join could multiply activity rows."
)

assert grid_lookup_count == 10000, f"Expected 10,000 grid cells, found {grid_lookup_count}"
print("Grid lookup validation: PASSED")

# ============================================================
# 6. COMPARE LOOKUP SIZE WITH ACTIVITY DATA
# ============================================================

activity_row_count = hourly_grid_summary.count()

print("\n========== SIZE COMPARISON ==========")
print("Activity rows:", activity_row_count)
print("Grid lookup rows:", grid_lookup_count)
print("Activity / lookup size ratio:", activity_row_count / grid_lookup_count)

# ============================================================
# 7. DISTINCT ACTIVITY GRIDS BEFORE JOIN
# ============================================================

activity_grid_count_before = (
    hourly_grid_summary
    .select("grid_id")
    .distinct()
    .count()
)

print("\nDistinct activity grids before join:", activity_grid_count_before)

# ============================================================
# 8. STANDARD JOIN EXECUTION PLAN
# ============================================================

print("\n========== STANDARD JOIN PLAN ==========")

standard_join_df = (
    hourly_grid_summary.alias("a")
    .join(
        grid_lookup_df.alias("g"),
        F.col("a.grid_id") == F.col("g.grid_id"),
        "left"
    )
)

standard_join_df.explain(mode="formatted")

# ============================================================
# 9. BROADCAST JOIN EXECUTION PLAN
# ============================================================

print("\n========== BROADCAST JOIN PLAN ==========")

broadcast_join_df = (
    hourly_grid_summary.alias("a")
    .join(
        broadcast(grid_lookup_df).alias("g"),
        F.col("a.grid_id") == F.col("g.grid_id"),
        "left"
    )
)

broadcast_join_df.explain(mode="formatted")

# ============================================================
# 10. USE BROADCAST JOIN
# ============================================================

# The grid lookup contains only 10,000 rows while the activity
# DataFrame can contain millions of rows.
#
# Therefore it is a good broadcast candidate:
#
#     LARGE activity DataFrame
#                  +
#     SMALL static grid lookup
#
# Spark can distribute the small lookup to executors instead
# of performing a large shuffle-based join.

grid_activity_geo_df = (
    hourly_grid_summary.alias("a")
    .join(
        broadcast(grid_lookup_df).alias("g"),
        F.col("a.grid_id") == F.col("g.grid_id"),
        "left"
    )
    .select(
        F.col("a.timestamp"),
        F.col("a.grid_id"),
        F.col("a.sms_in"),
        F.col("a.sms_out"),
        F.col("a.call_in"),
        F.col("a.call_out"),
        F.col("a.internet_activity"),
        F.col("a.total_activity"),
        F.col("g.geometry")
    )
)

# ============================================================
# 11. ROW COUNT VALIDATION
# ============================================================

before_join_count = hourly_grid_summary.count()
after_join_count = grid_activity_geo_df.count()

print("\n========== ROW COUNT VALIDATION ==========")
print("Before join:", before_join_count)
print("After join:", after_join_count)

# A LEFT JOIN must not increase the number of rows when
# grid_lookup_df has unique grid_id values.

assert after_join_count == before_join_count, (
    "Row count changed after join. "
    "The lookup may contain duplicate grid_id values."
)

print("Left-join row-count validation: PASSED")

# ============================================================
# 12. DISTINCT GRID COUNT AFTER JOIN
# ============================================================

activity_grid_count_after = (
    grid_activity_geo_df
    .select("grid_id")
    .distinct()
    .count()
)

print("Distinct activity grids after join:", activity_grid_count_after)

assert activity_grid_count_after == activity_grid_count_before, "Distinct grid count changed after join."

# ============================================================
# 13. FIND UNMATCHED GRIDS
# ============================================================

unmatched_grid_ids = (
    grid_activity_geo_df
    .filter(F.col("geometry").isNull())
    .select("grid_id")
    .distinct()
    .orderBy("grid_id")
)

unmatched_count = unmatched_grid_ids.count()

print("\n========== UNMATCHED GRID IDs ==========")
unmatched_grid_ids.show(100, truncate=False)
print("Unmatched grid count:", unmatched_count)

# Acceptance criterion
assert unmatched_count == 0, (
    f"Found {unmatched_count} activity grids "
    "without geometry."
)

# ============================================================
# 14. ENRICHMENT COVERAGE
# ============================================================

enriched_grid_count = (
    grid_activity_geo_df
    .filter(F.col("geometry").isNotNull())
    .select("grid_id")
    .distinct()
    .count()
)

coverage_percentage = (
    enriched_grid_count / activity_grid_count_before
) * 100

print("\n========== ENRICHMENT COVERAGE ==========")
print("Distinct grids before join:", activity_grid_count_before)
print("Distinct grids with geometry:", enriched_grid_count)
print("Enrichment coverage:", coverage_percentage, "%")

assert coverage_percentage == 100.0, "Enrichment coverage is not 100."
print("Coverage validation: PASSED")

# ============================================================
# 15. GEOJSON CENTROID FUNCTION
# ============================================================

def polygon_centroid(geometry):
    """
    Calculate an approximate centroid from polygon coordinates.

    GeoJSON coordinates use:
        [longitude, latitude]

    Polygon:
        coordinates[0] = outer ring
    """

    if geometry is None:
        return None

    geometry_type = geometry.get("type")
    coordinates = geometry.get("coordinates")

    if geometry_type == "Polygon":
        ring = coordinates[0]
    elif geometry_type == "MultiPolygon":
        # Use the first polygon's outer ring for this
        # simple validation.
        ring = coordinates[0][0]
    else:
        return None

    if not ring:
        return None

    lon_sum = sum(point[0] for point in ring)
    lat_sum = sum(point[1] for point in ring)

    return (lon_sum / len(ring), lat_sum / len(ring))

# ============================================================
# 16. BUILD CENTROID LOOKUP FOR VALIDATION
# ============================================================

centroid_records = []

for feature in features:
    grid_id = int(feature["properties"]["cellId"])
    geometry = feature["geometry"]
    centroid = polygon_centroid(geometry)

    if centroid is not None:
        centroid_records.append((
            grid_id,
            float(centroid[0]),
            float(centroid[1])
        ))

centroid_schema = StructType([
    StructField("grid_id", IntegerType(), False),
    StructField("centroid_lon", StringType(), False),
    StructField("centroid_lat", StringType(), False)
])

# Keep numeric values by creating through tuples and
# converting afterwards.

centroid_lookup = spark.createDataFrame(
    centroid_records,
    ["grid_id", "centroid_lon", "centroid_lat"]
).select(
    F.col("grid_id"),
    F.col("centroid_lon").cast("double"),
    F.col("centroid_lat").cast("double")
)

# ============================================================
# 17. GEOGRAPHIC SPOT CHECK
# ============================================================

print("\n========== GEOGRAPHIC SPOT CHECK ==========")

# Grid 4821 is used as the named/example grid from the
# project scenario. Its centroid should fall within the
# Milan area.

named_grid_id = 4821

named_grid = (
    centroid_lookup
    .filter(F.col("grid_id") == named_grid_id)
    .first()
)

assert named_grid is not None, f"Grid {named_grid_id} was not found."

named_lon = named_grid["centroid_lon"]
named_lat = named_grid["centroid_lat"]

print(
    f"Grid {named_grid_id} centroid:"
    f" longitude={named_lon},"
    f" latitude={named_lat}"
)

# Milan approximate geographic bounds.
#
# Milan is around:
# longitude 9.19 E
# latitude 45.46 N
#
# This is a broad sanity check, not a substitute for
# checking the cell visually on a map.

assert (
    9.0 <= named_lon <= 9.35
    and
    45.35 <= named_lat <= 45.60
), (
    f"Grid {named_grid_id} centroid does not appear "
    "to be within the expected Milan area."
)

print("Named-grid geographic spot-check: PASSED")

# ============================================================
# 18. GRID 1 AND GRID 2 ADJACENCY CHECK
# ============================================================

grid_1 = (
    centroid_lookup
    .filter(F.col("grid_id") == 1)
    .first()
)

grid_2 = (
    centroid_lookup
    .filter(F.col("grid_id") == 2)
    .first()
)

assert grid_1 is not None
assert grid_2 is not None

lon1 = grid_1["centroid_lon"]
lat1 = grid_1["centroid_lat"]
lon2 = grid_2["centroid_lon"]
lat2 = grid_2["centroid_lat"]

print("\nGrid 1 centroid:")
print(f"longitude={lon1}, latitude={lat1}")

print("\nGrid 2 centroid:")
print(f"longitude={lon2}, latitude={lat2}")

# Approximate distance between centroids.
# 1 degree latitude is approximately 111 km.

distance_km = math.sqrt(
    ((lon2 - lon1) * 111 * math.cos(
        math.radians((lat1 + lat2) / 2)
    )) ** 2
    +
    ((lat2 - lat1) * 111) ** 2
)

print(
    "\nApproximate Grid 1 -> Grid 2 centroid distance:",
    distance_km,
    "km"
)

# They must not be identical.
assert not (lon1 == lon2 and lat1 == lat2), "Grid 1 and Grid 2 have identical centroids."

# They should be geographically close.
# 5 km is deliberately generous for a sanity check.
assert distance_km < 5, "Grid 1 and Grid 2 do not appear to be adjacent."

print("Grid 1 / Grid 2 adjacency check: PASSED")

# ============================================================
# 19. TOP HIGH-ACTIVITY GRIDS
# ============================================================

# Select a window.
# Change these dates to whatever window you want to analyse.

START_DATE = "2013-11-01"
END_DATE = "2013-11-03"

top_activity_grids = (
    grid_activity_geo_df
    .filter(
        (F.to_date("timestamp") >= F.lit(START_DATE)) &
        (F.to_date("timestamp") <= F.lit(END_DATE))
    )
    .groupBy("grid_id")
    .agg(
        F.sum("total_activity").alias("window_total_activity"),
        F.first("geometry").alias("geometry")
    )
    .orderBy(F.col("window_total_activity").desc())
    .limit(10)
)

print("\n========== TOP 10 HIGH-ACTIVITY GRIDS ==========")
top_activity_grids.show(10, truncate=False)

# ============================================================
# 20. FINAL OUTPUT CHECK
# ============================================================

required_columns = [
    "timestamp",
    "grid_id",
    "sms_in",
    "sms_out",
    "call_in",
    "call_out",
    "internet_activity",
    "total_activity",
    "geometry"
]

print("\n========== FINAL OUTPUT COLUMNS ==========")

for column_name in required_columns:
    print(column_name, "->", column_name in grid_activity_geo_df.columns)
    assert column_name in grid_activity_geo_df.columns

print("\n========== SP4 COMPLETE ==========")
print("Final enriched rows:", grid_activity_geo_df.count())
print("Enrichment coverage:", coverage_percentage, "%")
print("Unmatched grids:", unmatched_count)
print("Grid lookup rows:", grid_lookup_count)

grid_activity_geo_df.write.mode("overwrite").parquet(r"parquet_output\grid_activity_geo_parquet")
grid_lookup_df.write.mode("overwrite").parquet(r"parquet_output\grid_lookup_parquet")