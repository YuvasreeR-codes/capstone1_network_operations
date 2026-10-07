import json
import math
from datetime import datetime
from pathlib import Path
from typing import Optional

from api.database import get_connection


# ---------------------------------------------------------
# PIPELINE STATUS FILE
# ---------------------------------------------------------

STATUS_FILE = Path("data/final_outputde7/de7_outputs/pipeline_status.json")


# ---------------------------------------------------------
# PIPELINE STATUS
# ---------------------------------------------------------

def get_pipeline_status():

    if not STATUS_FILE.exists():
        return None

    try:
        with open(
            STATUS_FILE,
            "r",
            encoding="utf-8"
        ) as file:
            status = json.load(file)

    except Exception:
        return None

    # -----------------------------------------------------
    # CHECK AS_OF AGAINST WAREHOUSE
    # -----------------------------------------------------

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT MAX(timestamp)
        FROM dim_time
        """
    )

    warehouse_as_of = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    reasons = list(status.get("reasons", []))

    status_as_of = status.get(
        "current_as_of",
        status.get("AS_OF")
    )

    if warehouse_as_of is not None and status_as_of:

        status_as_of_dt = datetime.fromisoformat(
            status_as_of
        )

        if status_as_of_dt != warehouse_as_of:

            reasons.append(
                "Pipeline AS_OF does not match "
                "the warehouse AS_OF."
            )

    # -----------------------------------------------------
    # CHECK TASK STATUS
    # -----------------------------------------------------

    tasks = status.get("tasks", {})

    for task_name, task in tasks.items():

        if task.get("status") != "SUCCESS":

            reasons.append(
                f"Task '{task_name}' status is "
                f"{task.get('status')}."
            )

    healthy = (
        status.get("healthy", False)
        and len(reasons) == 0
    )

    status["healthy"] = healthy
    status["reasons"] = reasons

    return status


# ---------------------------------------------------------
# GRID LOCATION
# ---------------------------------------------------------

def get_grid_location(grid_id: str):

    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute(
        """
        SELECT
            grid_id,
            latitude,
            longitude,
            geometry_reference
        FROM dim_grid
        WHERE grid_id = %s
        """,
        (grid_id,)
    )

    row = cursor.fetchone()

    cursor.close()
    conn.close()

    if row is None:
        return None

    return {
        "grid_id": str(row["grid_id"]),
        "latitude": float(row["latitude"]),
        "longitude": float(row["longitude"]),
        "geometry_reference": (
            row["geometry_reference"]
        )
    }


# ---------------------------------------------------------
# GRID NEIGHBOURS
# ---------------------------------------------------------

def get_grid_neighbours(
    grid_id: str,
    limit: int = 5
):

    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    # Get target grid
    cursor.execute(
        """
        SELECT
            grid_id,
            latitude,
            longitude
        FROM dim_grid
        WHERE grid_id = %s
        """,
        (grid_id,)
    )

    target = cursor.fetchone()

    if target is None:
        cursor.close()
        conn.close()
        return None

    target_lat = float(target["latitude"])
    target_lon = float(target["longitude"])

    # Get all other grids
    cursor.execute(
        """
        SELECT
            grid_id,
            latitude,
            longitude
        FROM dim_grid
        WHERE grid_id != %s
        """,
        (grid_id,)
    )

    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    neighbours = []

    # Simple centroid distance.
    # For nearby Milan grids this is sufficient
    # for ranking neighbouring cells.
    for row in rows:

        lat = float(row["latitude"])
        lon = float(row["longitude"])

        distance = math.sqrt(
            (lat - target_lat) ** 2
            + (lon - target_lon) ** 2
        )

        neighbours.append({
            "grid_id": str(row["grid_id"]),
            "latitude": lat,
            "longitude": lon,
            "distance": distance
        })

    neighbours.sort(
        key=lambda x: x["distance"]
    )

    neighbours = neighbours[:limit]

    return {
        "grid_id": str(grid_id),
        "count": len(neighbours),
        "neighbours": neighbours
    }