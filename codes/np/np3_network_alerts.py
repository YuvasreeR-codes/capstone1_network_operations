import pandas as pd
import numpy as np
from np.np2_UsageProcessor import UsageProcessor


OUTPUT_FILE = "network_alerts.csv"

processor = UsageProcessor(
    file_path="data/landing/sms-call-internet-mi-2013-11-01.csv"
)

processor.load_data()
processor.clean_data()
processor.derive_time_features()
processor.aggregate_to_grid_time()
processor.derive_activity_features()

df = processor.grid_hour_df.copy()

df["timestamp"] = pd.to_datetime(df["timestamp"])
df = df.sort_values(["grid_id", "timestamp"])

# =====================================================
# STEP 1: Build within-day leave-one-out median baseline
# =====================================================

def leave_one_out_median(group):
    """
    For each hour:
    baseline = median(total_activity of all other hours
                       in same grid/day)
    """
    values = group["total_activity"].values
    baselines = []

    for i in range(len(values)):
        others = np.delete(values, i)

        if len(others) > 0:
            baselines.append(np.median(others))
        else:
            baselines.append(np.nan)

    return pd.Series(baselines, index=group.index)


df["baseline_activity"] = (
    df.groupby(["grid_id", "date"], group_keys=False)
      .apply(leave_one_out_median)
)

# =====================================================
# STEP 2: Activity Floor
# =====================================================
# Daily activity per grid

daily_totals = (
    df.groupby(["grid_id", "date"])["total_activity"]
      .sum()
      .reset_index(name="daily_total_activity")
)

# Data-driven floor:
# Use 25th percentile of daily totals

activity_floor = daily_totals["daily_total_activity"].quantile(0.15)

print(f"\nActivity floor selected: {activity_floor:.2f}")
print("Rationale: remove lowest 15% of grid-days where ratios are unstable.\n")

# Add daily totals back

df = df.merge(
    daily_totals,
    on=["grid_id", "date"],
    how="left"
)

df = df[df["daily_total_activity"] >= activity_floor].copy()

# =====================================================
# STEP 3: Previous hour activity
# =====================================================

df["previous_activity"] = (
    df.groupby("grid_id")["total_activity"]
      .shift(1)
)

# =====================================================
# STEP 4: Training Rules
# =====================================================

HIGH_ACTIVITY_FACTOR = 2.0
SPIKE_FACTOR = 2.0
DROP_FACTOR = 0.5

alerts = []

for _, row in df.iterrows():

    current = row["total_activity"]
    baseline = row["baseline_activity"]
    prev_hour = row["previous_activity"]

    # Skip if baseline unavailable
    if pd.isna(baseline) or baseline == 0:
        continue

    # -----------------------------------------
    # HIGH_ACTIVITY
    # -----------------------------------------
    if current >= baseline * HIGH_ACTIVITY_FACTOR:

        alerts.append({
            "grid_id": row["grid_id"],
            "timestamp": row["timestamp"],
            "alert_type": "HIGH_ACTIVITY",
            "current_activity": current,
            "baseline_activity": baseline,
            "reason": (
                f"Current activity ({current:.0f}) is "
                f"{current/baseline:.2f}x baseline."
            )
        })

    # -----------------------------------------
    # ACTIVITY_SPIKE
    # -----------------------------------------
    if (
        pd.notna(prev_hour)
        and prev_hour > 0
        and current >= prev_hour * SPIKE_FACTOR
    ):

        alerts.append({
            "grid_id": row["grid_id"],
            "timestamp": row["timestamp"],
            "alert_type": "ACTIVITY_SPIKE",
            "current_activity": current,
            "baseline_activity": baseline,
            "reason": (
                f"Activity increased from {prev_hour:.0f} "
                f"to {current:.0f} "
                f"({current/prev_hour:.2f}x increase)."
            )
        })

    # -----------------------------------------
    # ACTIVITY_DROP
    # -----------------------------------------
    if current <= baseline * DROP_FACTOR:

        alerts.append({
            "grid_id": row["grid_id"],
            "timestamp": row["timestamp"],
            "alert_type": "ACTIVITY_DROP",
            "current_activity": current,
            "baseline_activity": baseline,
            "reason": (
                f"Current activity ({current:.0f}) is "
                f"only {current/baseline:.2f}x baseline."
            )
        })

# =====================================================
# STEP 5: Create alert dataframe
# =====================================================

alerts_df = pd.DataFrame(alerts)

# =====================================================
# STEP 6: Save alerts
# =====================================================

alerts_df.to_csv("output/grid_activity_alerts.csv", index=False)

alerts_df.to_json(
    "output/grid_activity_alerts.json",
    orient="records",
    date_format="iso"
)

print(f"Generated {len(alerts_df):,} alerts.")

# =====================================================
# STEP 7: Operational Summary
# =====================================================

print("\n========== ALERTS BY TYPE ==========")

if len(alerts_df) > 0:
    print(alerts_df["alert_type"].value_counts())

print("\n========== TOP 10 GRIDS ==========")

if len(alerts_df) > 0:

    top_grids = (
        alerts_df["grid_id"]
        .value_counts()
        .head(10)
    )

    print(top_grids)

total_grid_hours = len(df)

alerted_grid_hours = (
    alerts_df[["grid_id", "timestamp"]]
    .drop_duplicates()
    .shape[0]
)

alert_rate = alerted_grid_hours / total_grid_hours

print("\n========== ALERT RATE ==========")
print(f"Total grid-hours: {total_grid_hours:,}")
print(f"Alerted grid-hours: {alerted_grid_hours:,}")
print(f"Proportion alerted: {alert_rate:.2%}")