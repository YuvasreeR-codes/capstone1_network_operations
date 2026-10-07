from datetime import datetime
from datetime import timedelta
from api.database import get_connection


from datetime import datetime


def get_network_summary(
    requested_as_of: datetime | None = None
):
    connection = None
    cursor = None

    try:
        connection = get_connection()
        cursor = connection.cursor(dictionary=True)

        # -------------------------------------------------
        # 1. Determine effective AS_OF
        # -------------------------------------------------

        if requested_as_of is None:

            cursor.execute("""
                SELECT
                    MAX(timestamp) AS max_timestamp
                FROM dim_time
            """)

            result = cursor.fetchone()

            if not result or result["max_timestamp"] is None:
                raise ValueError(
                    "Analytics warehouse contains no timestamp data."
                )

            as_of = result["max_timestamp"]

        else:
            as_of = requested_as_of

        # -------------------------------------------------
        # 2. Resolve cutoff time_key once
        # -------------------------------------------------

        cursor.execute("""
            SELECT
                MAX(time_key) AS cutoff_time_key
            FROM dim_time
            WHERE timestamp <= %s
        """, (as_of,))

        cutoff_result = cursor.fetchone()

        if (
            cutoff_result is None or
            cutoff_result["cutoff_time_key"] is None
        ):
            return {
                "total_activity": 0.0,
                "active_grids": 0,
                "peak_hour": 0,
                "top_grid": "",
                "as_of": as_of
            }

        cutoff_time_key = cutoff_result[
            "cutoff_time_key"
        ]

        # -------------------------------------------------
        # 3. Total activity + active grids
        #    Single table scan
        # -------------------------------------------------

        cursor.execute("""
            SELECT
                COALESCE(
                    SUM(total_activity),
                    0
                ) AS total_activity,

                COUNT(
                    DISTINCT CASE
                        WHEN total_activity > 0
                        THEN grid_key
                    END
                ) AS active_grids

            FROM fact_network_activity

            WHERE time_key <= %s
        """, (cutoff_time_key,))

        summary_result = cursor.fetchone()

        total_activity = float(
            summary_result["total_activity"]
        )

        active_grids = int(
            summary_result["active_grids"]
        )

        # -------------------------------------------------
        # 4. Peak hour
        # -------------------------------------------------

        cursor.execute("""
            SELECT
                t.hour,
                SUM(f.total_activity) AS activity

            FROM fact_network_activity f

            JOIN dim_time t
                ON f.time_key = t.time_key

            WHERE f.time_key <= %s

            GROUP BY t.hour

            ORDER BY activity DESC

            LIMIT 1
        """, (cutoff_time_key,))

        peak_result = cursor.fetchone()

        peak_hour = (
            int(peak_result["hour"])
            if peak_result
            else 0
        )

        # -------------------------------------------------
        # 5. Top grid
        # -------------------------------------------------

        cursor.execute("""
            SELECT
                g.grid_id,
                SUM(f.total_activity) AS activity

            FROM fact_network_activity f

            JOIN dim_grid g
                ON f.grid_key = g.grid_key

            WHERE f.time_key <= %s

            GROUP BY g.grid_id

            ORDER BY activity DESC

            LIMIT 1
        """, (cutoff_time_key,))

        top_grid_result = cursor.fetchone()

        top_grid = (
            str(top_grid_result["grid_id"])
            if top_grid_result
            else ""
        )

        # -------------------------------------------------
        # Return API response
        # -------------------------------------------------

        return {
            "total_activity": total_activity,
            "active_grids": active_grids,
            "peak_hour": peak_hour,
            "top_grid": top_grid,
            "as_of": as_of
        }

    finally:

        if cursor:
            cursor.close()

        if connection:
            connection.close()

def get_grid_activity(
    grid_id: str,
    requested_date=None,
    requested_hour=None,
    requested_as_of=None
):
    connection = None
    cursor = None

    try:

        connection = get_connection()
        cursor = connection.cursor(dictionary=True)

        # =================================================
        # 1. Validate grid ID
        # =================================================

        try:
            numeric_grid_id = int(grid_id)
        except (ValueError, TypeError):
            return None

        if numeric_grid_id < 1 or numeric_grid_id > 10000:
            return None

        # =================================================
        # 2. Check grid exists
        # =================================================

        cursor.execute(
            """
            SELECT grid_id
            FROM dim_grid
            WHERE grid_id = %s
            LIMIT 1
            """,
            (str(numeric_grid_id),)
        )

        if cursor.fetchone() is None:
            return None

        # =================================================
        # 3. Validate hour if provided
        # =================================================

        if requested_hour is not None:

            try:
                requested_hour = int(requested_hour)
            except (ValueError, TypeError):
                raise ValueError(
                    "Hour must be between 0 and 23."
                )

            if requested_hour < 0 or requested_hour > 23:
                raise ValueError(
                    "Hour must be between 0 and 23."
                )

        # =================================================
        # 4. Determine AS_OF
        # =================================================

        if requested_as_of is None:

            cursor.execute(
                """
                SELECT
                    MAX(timestamp) AS max_timestamp
                FROM dim_time
                """
            )

            result = cursor.fetchone()

            if (
                not result
                or result["max_timestamp"] is None
            ):
                raise ValueError(
                    "Analytics warehouse contains no timestamp data."
                )

            as_of = result["max_timestamp"]

        else:

            as_of = requested_as_of

        # =================================================
        # 5. Determine query window
        # =================================================

        specific_hour_mode = False

        # -----------------------------------------
        # date + hour
        # Return specific hour
        # -----------------------------------------

        if (
            requested_date is not None
            and requested_hour is not None
        ):

            specific_hour_mode = True

            query_start = datetime.combine(
                requested_date,
                datetime.min.time()
            ).replace(
                hour=requested_hour,
                minute=0,
                second=0,
                microsecond=0
            )

            query_end = query_start

        # -----------------------------------------
        # date only
        # Return whole day trend
        # -----------------------------------------

        elif requested_date is not None:

            query_start = datetime.combine(
                requested_date,
                datetime.min.time()
            )

            query_end = query_start + timedelta(hours=23)

        # -----------------------------------------
        # no date supplied
        # Return trailing 24-hour window
        # -----------------------------------------

        else:

            if requested_hour is not None:

                as_of = as_of.replace(
                    hour=requested_hour,
                    minute=0,
                    second=0,
                    microsecond=0
                )

            cursor.execute(
                """
                SELECT
                    MAX(timestamp) AS latest_available
                FROM dim_time
                WHERE timestamp <= %s
                """,
                (as_of,)
            )

            latest_result = cursor.fetchone()

            if (
                not latest_result
                or latest_result["latest_available"] is None
            ):
                return {
                    "grid_id": str(numeric_grid_id),
                    "data": []
                }

            query_end = latest_result["latest_available"]
            query_start = query_end - timedelta(hours=23)

        # =================================================
        # 6. Query activity
        # =================================================

        cursor.execute(
            """
            SELECT

                t.timestamp,
                t.date,
                t.hour,

                COALESCE(f.sms_in, 0) AS sms_in,
                COALESCE(f.sms_out, 0) AS sms_out,

                COALESCE(f.call_in, 0) AS call_in,
                COALESCE(f.call_out, 0) AS call_out,

                COALESCE(f.total_sms, 0) AS total_sms,
                COALESCE(f.total_calls, 0) AS total_calls,

                COALESCE(f.internet_activity, 0)
                    AS internet_activity,

                COALESCE(f.total_activity, 0)
                    AS total_activity

            FROM fact_network_activity f

            JOIN dim_time t
                ON f.time_key = t.time_key

            JOIN dim_grid g
                ON f.grid_key = g.grid_key

            WHERE g.grid_id = %s
              AND t.timestamp >= %s
              AND t.timestamp <= %s

            ORDER BY t.timestamp
            """,
            (
                str(numeric_grid_id),
                query_start,
                query_end
            )
        )

        rows = cursor.fetchall()

        # =================================================
        # 7. Build response
        # =================================================

        data = []

        for row in rows:

            data.append(
                {
                    "timestamp": row["timestamp"],
                    "date": str(row["date"]),
                    "hour": int(row["hour"]),

                    "sms_in": float(
                        row["sms_in"] or 0
                    ),

                    "sms_out": float(
                        row["sms_out"] or 0
                    ),

                    "call_in": float(
                        row["call_in"] or 0
                    ),

                    "call_out": float(
                        row["call_out"] or 0
                    ),

                    "total_sms": float(
                        row["total_sms"] or 0
                    ),

                    "total_calls": float(
                        row["total_calls"] or 0
                    ),

                    "internet_activity": float(
                        row["internet_activity"] or 0
                    ),

                    "total_activity": float(
                        row["total_activity"] or 0
                    )
                }
            )

       
        return {
            "grid_id": str(numeric_grid_id),
            "as_of": query_end,
            "start_time": query_start,
            "end_time": query_end,
            "data": data
        }

    finally:

        if cursor:
            cursor.close()

        if connection:
            connection.close()