from api.database import get_connection


def get_anomaly_result(grid_id: str, timestamp):
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    query = """
        SELECT
            grid_id,
            timestamp,
            anomaly_score,
            ml4_direction,
            ml4_reason,
            np3_alert,
            np3_alert_count,
            np3_alert_types,
            comparison,
            comparison_reason
        FROM network_anomaly_scores
        WHERE grid_id = %s
          AND timestamp = %s
        LIMIT 1
    """

    cursor.execute(
        query,
        (grid_id, timestamp)
    )

    result = cursor.fetchone()

    cursor.close()
    conn.close()

    return result