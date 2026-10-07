from datetime import datetime
from typing import Optional

from api.database import get_connection


# ---------------------------------------------------------
# SEVERITY
# ---------------------------------------------------------

SEVERITY_LEVELS = {
 
    "MEDIUM": 1,
    "HIGH": 2,
  
}


def get_effective_as_of(
    requested_as_of: Optional[datetime] = None
):
    conn = get_connection()
    cursor = conn.cursor()

    if requested_as_of:
        cursor.execute(
            """
            SELECT MAX(timestamp)
            FROM dim_time
            WHERE timestamp <= %s
            """,
            (requested_as_of,)
        )
    else:
        cursor.execute(
            """
            SELECT MAX(timestamp)
            FROM dim_time
            """
        )

    result = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    if result is None:
        raise ValueError("No network data available")

    return result


# ---------------------------------------------------------
# HOTSPOTS
# ---------------------------------------------------------

def get_hotspots(
    limit: int = 10,
    as_of: Optional[datetime] = None
):

    effective_as_of = get_effective_as_of(as_of)

    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    query = """
    SELECT
        g.grid_id,
        t.timestamp,

        f.sms_in,
        f.sms_out,
        f.call_in,
        f.call_out,
        f.total_sms,
        f.total_calls,
        f.internet_activity,
        f.total_activity,

        nas.ml3_risk_score,
        nas.ml3_risk_level,
        nas.model_version

    FROM fact_network_activity f

    JOIN dim_time t
        ON f.time_key = t.time_key

    JOIN dim_grid g
        ON f.grid_key = g.grid_key

    LEFT JOIN network_anomaly_scores nas
        ON nas.grid_id = g.grid_id
        AND nas.timestamp = t.timestamp

    WHERE t.timestamp = %s

    ORDER BY f.total_activity DESC

    LIMIT %s
"""

    cursor.execute(
        query,
        (effective_as_of, limit)
    )

    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    data = []

    for row in rows:

        data.append({
            "grid_id": str(row["grid_id"]),
            "timestamp": row["timestamp"],

            "sms_in": float(row["sms_in"] or 0),
            "sms_out": float(row["sms_out"] or 0),
            "call_in": float(row["call_in"] or 0),
            "call_out": float(row["call_out"] or 0),
            "total_sms": float(row["total_sms"] or 0),
            "total_calls": float(row["total_calls"] or 0),
            "internet_activity": float(
                row["internet_activity"] or 0
            ),
            "total_activity": float(
                row["total_activity"] or 0
            ),

            "status": "HOTSPOT",
            "severity": "HIGH",

            "reason": (
                f"Grid {row['grid_id']} has high activity "
                f"with total activity of "
                f"{float(row['total_activity'] or 0):.0f}."
            ),

            "risk_score": (
    float(row["ml3_risk_score"])
    if row["ml3_risk_score"] is not None
    else None
),

            "risk_level": row["ml3_risk_level"],
            "model_version": row["model_version"]
        })

    return {
        "as_of": effective_as_of,
        "limit": limit,
        "count": len(data),
        "data": data
    }


# ---------------------------------------------------------
# ALERTS
# ---------------------------------------------------------

def get_alerts(
    limit: int = 10,
    severity: Optional[str] = None,
    as_of: Optional[datetime] = None
):

    effective_as_of = get_effective_as_of(as_of)

    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    query = """
    SELECT
        a.alert_key,
        a.time_key,
        a.grid_key,
        a.alert_type,
        a.current_activity,
        a.baseline_activity,
        a.reason,
        a.severity,

        t.timestamp,
        g.grid_id,

        f.sms_in,
        f.sms_out,
        f.call_in,
        f.call_out,
        f.total_sms,
        f.total_calls,
        f.internet_activity,
        f.total_activity,

        nas.ml3_risk_score,
        nas.ml3_risk_level,
        nas.model_version

    FROM fact_network_alerts a

    JOIN dim_time t
        ON a.time_key = t.time_key

    JOIN dim_grid g
        ON a.grid_key = g.grid_key

    LEFT JOIN fact_network_activity f
        ON f.time_key = a.time_key
        AND f.grid_key = a.grid_key

    LEFT JOIN network_anomaly_scores nas
        ON nas.grid_id = g.grid_id
        AND nas.timestamp = t.timestamp

    WHERE t.timestamp <= %s
"""

    params = [effective_as_of]

    # -----------------------------------------------------
    # SEVERITY FILTER
    # -----------------------------------------------------

    if severity:

        severity = severity.upper()

        if severity not in SEVERITY_LEVELS:
            raise ValueError(
                "Invalid severity. "
                "Use MEDIUM, HIGH ."
            )

        query += """
            AND
            CASE a.severity
               
                WHEN 'MEDIUM' THEN 1
                WHEN 'HIGH' THEN 2
              
                ELSE 0
            END >= %s
        """

        params.append(
            SEVERITY_LEVELS[severity]
        )

    query += """
        ORDER BY
            t.timestamp DESC,
            a.current_activity DESC

        LIMIT %s
    """

    params.append(limit)

    cursor.execute(
        query,
        tuple(params)
    )

    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    data = []

    for row in rows:

        data.append({
            "grid_id": str(row["grid_id"]),
            "timestamp": row["timestamp"],

            "alert_type": row["alert_type"],

            "current_activity": float(
                row["current_activity"] or 0
            ),

            "baseline_activity": float(
                row["baseline_activity"] or 0
            ),

            "sms_in": float(row["sms_in"] or 0),
            "sms_out": float(row["sms_out"] or 0),
            "call_in": float(row["call_in"] or 0),
            "call_out": float(row["call_out"] or 0),
            "total_sms": float(row["total_sms"] or 0),
            "total_calls": float(row["total_calls"] or 0),

            "internet_activity": float(
                row["internet_activity"] or 0
            ),

            "total_activity": float(
                row["total_activity"] or 0
            ),

            "status": "ALERT",
            "severity": row["severity"],
            "reason": row["reason"],

            # Reserved for future ML
            "risk_score": (
    float(row["ml3_risk_score"])
    if row["ml3_risk_score"] is not None
    else None
),

            "risk_level": row["ml3_risk_level"],
            "model_version": row["model_version"]
        })

    return {
        "as_of": effective_as_of,
        "limit": limit,
        "count": len(data),
        "data": data
    }