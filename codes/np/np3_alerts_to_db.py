import glob
import pandas as pd
import numpy as np
import mysql.connector

from np.np2_UsageProcessor import UsageProcessor


# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------

INPUT_PATTERN = "data/raw/sms-call-internet-mi-*.csv"

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "database": "capstone1"
}

HIGH_ACTIVITY_FACTOR = 2.0
SPIKE_FACTOR = 2.0
DROP_FACTOR = 0.5


# ---------------------------------------------------------
# STEP 1: PROCESS ALL CSV FILES
# ---------------------------------------------------------

files = glob.glob(INPUT_PATTERN)

if not files:
    raise FileNotFoundError(
        f"No input files found: {INPUT_PATTERN}"
    )

print(f"Found {len(files)} CSV files.")

all_grid_hours = []

for file_path in files:
    print(f"Processing: {file_path}")

    processor = UsageProcessor(file_path=file_path)

    processor.load_data()
    processor.clean_data()
    processor.derive_time_features()
    processor.aggregate_to_grid_time()
    processor.derive_activity_features()

    df = processor.grid_hour_df.copy()

    if not df.empty:
        all_grid_hours.append(df)


if not all_grid_hours:
    raise ValueError("No processed data was produced.")


df = pd.concat(all_grid_hours, ignore_index=True)

df["timestamp"] = pd.to_datetime(df["timestamp"])

df = df.sort_values(
    ["grid_id", "timestamp"]
).reset_index(drop=True)

print(f"\nTotal grid-hour records: {len(df):,}")


# ---------------------------------------------------------
# STEP 2: LEAVE-ONE-OUT MEDIAN BASELINE
# ---------------------------------------------------------

def leave_one_out_median(group):

    values = group["total_activity"].values
    baselines = []

    for i in range(len(values)):

        others = np.delete(values, i)

        if len(others) > 0:
            baselines.append(np.median(others))
        else:
            baselines.append(np.nan)

    return pd.Series(
        baselines,
        index=group.index
    )


df["baseline_activity"] = (
    df.groupby(
        ["grid_id", "date"],
        group_keys=False
    )
    .apply(leave_one_out_median)
)


# ---------------------------------------------------------
# STEP 3: ACTIVITY FLOOR
# ---------------------------------------------------------

daily_totals = (
    df.groupby(
        ["grid_id", "date"]
    )["total_activity"]
    .sum()
    .reset_index(
        name="daily_total_activity"
    )
)

activity_floor = daily_totals[
    "daily_total_activity"
].quantile(0.15)

print(
    f"\nActivity floor selected: "
    f"{activity_floor:.2f}"
)

df = df.merge(
    daily_totals,
    on=["grid_id", "date"],
    how="left"
)

df = df[
    df["daily_total_activity"] >= activity_floor
].copy()


# ---------------------------------------------------------
# STEP 4: PREVIOUS HOUR
# ---------------------------------------------------------

df["previous_activity"] = (
    df.groupby("grid_id")["total_activity"]
    .shift(1)
)


# ---------------------------------------------------------
# STEP 5: GENERATE ALERTS
# ---------------------------------------------------------

alerts = []

for _, row in df.iterrows():

    current = row["total_activity"]
    baseline = row["baseline_activity"]
    previous = row["previous_activity"]

    if pd.isna(baseline) or baseline == 0:
        continue

    # HIGH ACTIVITY
    if current >= baseline * HIGH_ACTIVITY_FACTOR:

        alerts.append({
            "grid_id": str(row["grid_id"]),
            "timestamp": row["timestamp"],
            "alert_type": "HIGH_ACTIVITY",
            "current_activity": current,
            "baseline_activity": baseline,
            "severity": "HIGH",
            "reason": (
                f"Current activity ({current:.0f}) is "
                f"{current / baseline:.2f}x baseline."
            )
        })

    # ACTIVITY SPIKE
    if (
        pd.notna(previous)
        and previous > 0
        and current >= previous * SPIKE_FACTOR
    ):

        alerts.append({
            "grid_id": str(row["grid_id"]),
            "timestamp": row["timestamp"],
            "alert_type": "ACTIVITY_SPIKE",
            "current_activity": current,
            "baseline_activity": baseline,
            "severity": "HIGH",
            "reason": (
                f"Activity increased from {previous:.0f} "
                f"to {current:.0f} "
                f"({current / previous:.2f}x increase)."
            )
        })

    # ACTIVITY DROP
    if current <= baseline * DROP_FACTOR:

        alerts.append({
            "grid_id": str(row["grid_id"]),
            "timestamp": row["timestamp"],
            "alert_type": "ACTIVITY_DROP",
            "current_activity": current,
            "baseline_activity": baseline,
            "severity": "MEDIUM",
            "reason": (
                f"Current activity ({current:.0f}) is "
                f"only {current / baseline:.2f}x baseline."
            )
        })


alerts_df = pd.DataFrame(alerts)

print(f"\nGenerated {len(alerts_df):,} alerts.")


# ---------------------------------------------------------
# STEP 6: CONNECT TO MYSQL
# ---------------------------------------------------------

conn = mysql.connector.connect(**DB_CONFIG)

cursor = conn.cursor()


# ---------------------------------------------------------
# STEP 7: REBUILD ALERT TABLE
# ---------------------------------------------------------

cursor.execute(
    "TRUNCATE TABLE fact_network_alerts"
)


# ---------------------------------------------------------
# STEP 8: INSERT ALERTS
# ---------------------------------------------------------

insert_sql = """
INSERT INTO fact_network_alerts (
    time_key,
    grid_key,
    alert_type,
    current_activity,
    baseline_activity,
    reason,
    severity
)
SELECT
    %s,
    grid_key,
    %s,
    %s,
    %s,
    %s,
    %s
FROM dim_grid
WHERE grid_id = %s
"""


inserted = 0

for _, row in alerts_df.iterrows():

    timestamp = pd.Timestamp(
        row["timestamp"]
    ).to_pydatetime()

    time_key = timestamp.strftime(
        "%Y%m%d_%H%M"
    )

    cursor.execute(
        insert_sql,
        (
            time_key,
            row["alert_type"],
            float(row["current_activity"]),
            float(row["baseline_activity"]),
            row["reason"],
            row["severity"],
            str(row["grid_id"])
        )
    )

    if cursor.rowcount > 0:
        inserted += 1


conn.commit()


# ---------------------------------------------------------
# STEP 9: CLOSE CONNECTION
# ---------------------------------------------------------

cursor.close()
conn.close()


# ---------------------------------------------------------
# SUMMARY
# ---------------------------------------------------------

print("\n================================")
print("NP3 COMPLETED")
print("================================")

print(f"CSV files processed : {len(files):,}")
print(f"Grid-hours analyzed : {len(df):,}")
print(f"Alerts generated    : {len(alerts_df):,}")
print(f"Alerts inserted     : {inserted:,}")

if not alerts_df.empty:

    print("\n========== ALERTS BY TYPE ==========")
    print(
        alerts_df["alert_type"]
        .value_counts()
    )

    print("\n========== ALERTS BY SEVERITY ==========")
    print(
        alerts_df["severity"]
        .value_counts()
    )