from datetime import datetime
from typing import Optional

from api.database import get_connection


def get_grid_features(
    grid_id: str,
    as_of: Optional[datetime] = None
):
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    # Check grid exists
    cursor.execute(
        """
        SELECT grid_id
        FROM dim_grid
        WHERE grid_id = %s
        """,
        (grid_id,)
    )

    grid = cursor.fetchone()

    if grid is None:
        cursor.close()
        conn.close()
        return None

    # Get latest feature for the grid
    query = """
        SELECT
            grid_id,
            feature_timestamp,
            avg_activity,
            median_activity,
            median_avg_ratio,
            activity_growth,
            active_hours,
            peak_ratio,
            variability,
            internet_share,
            hour_of_day,
            day_of_week
        FROM network_feature_new_final
        WHERE grid_id = %s
    """

    params = [grid_id]

    if as_of:
        query += """
            AND feature_timestamp <= %s
        """
        params.append(as_of)

    query += """
        ORDER BY feature_timestamp DESC
        LIMIT 1
    """

    cursor.execute(query, tuple(params))

    row = cursor.fetchone()

    cursor.close()
    conn.close()

    if row is None:
        return None

    # Basic freshness calculation
    now = datetime.now()
    age_hours = (
        now - row["feature_timestamp"]
    ).total_seconds() / 3600

    if age_hours <= 24:
        freshness = "FRESH"
    elif age_hours <= 72:
        freshness = "STALE"
    else:
        freshness = "OLD"

    return {
        "grid_id": str(row["grid_id"]),
        "feature_timestamp": row["feature_timestamp"],

        "avg_activity": float(row["avg_activity"]),
        "median_activity": float(row["median_activity"]),
        "median_avg_ratio": float(row["median_avg_ratio"]),
        "activity_growth": float(row["activity_growth"]),
        "active_hours": int(row["active_hours"]),
        "peak_ratio": float(row["peak_ratio"]),
        "variability": float(row["variability"]),
        "internet_share": float(row["internet_share"]),
        "hour_of_day":int(row["hour_of_day"]),
        "day_of_week":int(row["day_of_week"]),


        "data_quality": "GOOD",
        "feature_freshness": freshness
    }