# ================================================================
# ML3 vs NP3 — RULE-BASED ALERT COMPARISON
# ================================================================
#
# Purpose:
#   Compare ML3 predictions against the rule-based NP3 alerts.
#
# ML3:
#   Predicts HIGH_ACTIVITY for the NEXT hour.
#
# NP3:
#   Rule-based alerts:
#       HIGH_ACTIVITY
#       ACTIVITY_SPIKE
#       ACTIVITY_DROP
#
# Main comparison:
#   ML3 HIGH_ACTIVITY prediction
#              vs
#   NP3 HIGH_ACTIVITY alert
#
# Additional analysis:
#   ML3 prediction vs:
#       - ACTIVITY_SPIKE
#       - ACTIVITY_DROP
#
# Outputs:
#   ml3_np3_comparison/
#       ml3_np3_comparison.csv
#       ml3_np3_summary.csv
#       ml3_np3_alert_type_summary.csv
#       ml3_np3_disagreements.csv
#
# ================================================================


import os
import pickle
import warnings

import numpy as np
import pandas as pd

from sqlalchemy import create_engine

warnings.filterwarnings("ignore")


# ================================================================
# 1. CONFIGURATION
# ================================================================

DB_USER = "root"
DB_PASSWORD = "root"
DB_HOST = "localhost"
DB_PORT = "3306"
DB_NAME = "capstone1"


# ------------------------------------------------
# ML3 model created by ml3_predict.py
# ------------------------------------------------

MODEL_PATH = os.path.join(
    "ml",
    "ml3_output",
    "ml3_best_model.pkl"
)


# ------------------------------------------------
# Output directory
# ------------------------------------------------

OUTPUT_DIR = "ml3_np3_comparison"

os.makedirs(
    OUTPUT_DIR,
    exist_ok=True
)


COMPARISON_OUTPUT = os.path.join(
    OUTPUT_DIR,
    "ml3_np3_comparison.csv"
)

SUMMARY_OUTPUT = os.path.join(
    OUTPUT_DIR,
    "ml3_np3_summary.csv"
)

ALERT_TYPE_OUTPUT = os.path.join(
    OUTPUT_DIR,
    "ml3_np3_alert_type_summary.csv"
)

DISAGREEMENT_OUTPUT = os.path.join(
    OUTPUT_DIR,
    "ml3_np3_disagreements.csv"
)


# ================================================================
# IMPORTANT TIMESTAMP SETTING
# ================================================================
#
# ML3:
#
#   feature time T
#       ↓
#   predicts activity at T+1
#
# NP3:
#
#   If NP3 alert timestamp represents the SAME activity hour
#   that triggered the alert, set this to True.
#
#   Example:
#
#       ML3 feature time = 09:00
#       ML3 predicts     = 10:00
#
#       NP3 alert time   = 10:00
#
#   Therefore NP3 must be shifted back by 1 hour for comparison.
#
# ================================================================

NP3_ALERT_IS_FOR_SAME_HOUR = True


# ================================================================
# 2. DATABASE CONNECTION
# ================================================================

print("=" * 70)
print("ML3 vs NP3 — RULE-BASED ALERT COMPARISON")
print("=" * 70)

print("\nSTEP 1 — CONNECTING TO DATABASE")
print("-" * 70)

connection_string = (
    f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

engine = create_engine(
    connection_string
)

print("Database connection created successfully.")


# ================================================================
# 3. LOAD SAVED ML3 MODEL
# ================================================================

print("\nSTEP 2 — LOADING SAVED ML3 MODEL")
print("-" * 70)

if not os.path.exists(MODEL_PATH):

    raise FileNotFoundError(
        f"ML3 model not found: {MODEL_PATH}\n"
        "Run ml3_predict.py first."
    )


with open(
    MODEL_PATH,
    "rb"
) as file:

    model_package = pickle.load(file)


best_model = model_package["model"]

best_model_name = model_package[
    "model_name"
]
best_threshold = model_package.get(
    "threshold",
    0.50
)
print(
    f"Prediction threshold: "
    f"{best_threshold:.2f}"
)
FEATURE_COLUMNS = model_package[
    "features"
]

# P85_THRESHOLD = model_package[
#     "grid_p85_thresholds"
# ]

TRAIN_START = pd.to_datetime(
    model_package["train_start"]
)

TRAIN_END = pd.to_datetime(
    model_package["train_end"]
)

TEST_START = pd.to_datetime(
    model_package["test_start"]
)

TEST_END = pd.to_datetime(
    model_package["test_end"]
)


print(
    f"Model loaded: {best_model_name}"
)

# print(
#     f"P85 threshold: {P85_THRESHOLD:.6f}"
# )

print(
    "\nML3 features:"
)

for feature in FEATURE_COLUMNS:

    print(
        f"  - {feature}"
    )

print(
    "\nOriginal ML3 test period:"
)

print(
    f"  {TEST_START} → {TEST_END}"
)


# ================================================================
# 4. READ ML2 FEATURE TABLE
# ================================================================

print("\nSTEP 3 — READING ML2 FEATURE TABLE")
print("-" * 70)

feature_query = """
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
"""

features = pd.read_sql(
    feature_query,
    engine
)

print(
    f"Feature rows loaded: {len(features):,}"
)


# ================================================================
# 5. PREPARE ML2 FEATURES
# ================================================================

print("\nSTEP 4 — PREPARING ML2 FEATURES")
print("-" * 70)

features["grid_id"] = (
    features["grid_id"].astype(str)
)

features["feature_timestamp"] = pd.to_datetime(
    features["feature_timestamp"]
)

features = features.sort_values(
    "feature_timestamp"
).reset_index(
    drop=True
)

print(
    "Feature period:"
)

print(
    f"  {features['feature_timestamp'].min()}"
    f" → "
    f"{features['feature_timestamp'].max()}"
)


# ================================================================
# 6. READ ACTUAL ACTIVITY
# ================================================================
#
# We use one SQL query instead of loading:
#
#   dim_time
#   dim_grid
#   fact_network_activity
#
# separately.
#
# ================================================================

print("\nSTEP 5 — READING ACTUAL NETWORK ACTIVITY")
print("-" * 70)

activity_query = """
SELECT
    g.grid_id,
    t.timestamp,
    f.total_activity
FROM fact_network_activity f
INNER JOIN dim_time t
    ON f.time_key = t.time_key
INNER JOIN dim_grid g
    ON f.grid_key = g.grid_key
WHERE f.total_activity IS NOT NULL
ORDER BY
    g.grid_id,
    t.timestamp
"""

activity = pd.read_sql(
    activity_query,
    engine
)

print(
    f"Activity rows loaded: {len(activity):,}"
)

activity["grid_id"] = (
    activity["grid_id"].astype(str)
)

activity["timestamp"] = pd.to_datetime(
    activity["timestamp"]
)


# ================================================================
# 7. CREATE NEXT-HOUR TARGET
# ================================================================

print("\nSTEP 6 — CREATING NEXT-HOUR ACTIVITY")
print("-" * 70)

features["target_timestamp"] = (
    features["feature_timestamp"]
    + pd.Timedelta(hours=1)
)

activity = activity.rename(
    columns={
        "timestamp": "target_timestamp",
        "total_activity": "next_hour_activity"
    }
)

ml_data = features.merge(
    activity,
    on=[
        "grid_id",
        "target_timestamp"
    ],
    how="left"
)

missing_target = (
    ml_data["next_hour_activity"]
    .isna()
    .sum()
)

print(
    f"Rows after next-hour join: "
    f"{len(ml_data):,}"
)

print(
    f"Rows without next-hour activity: "
    f"{missing_target:,}"
)


# ================================================================
# 8. REMOVE MISSING NEXT-HOUR OBSERVATIONS
# ================================================================

ml_data = ml_data.dropna(
    subset=[
        "next_hour_activity"
    ]
).copy()


# ================================================================
# 9. CHRONOLOGICAL SORT
# ================================================================

print("\nSTEP 7 — CHRONOLOGICAL SORT")
print("-" * 70)

ml_data = ml_data.sort_values(
    "feature_timestamp"
).reset_index(
    drop=True
)

print(
    f"Earliest: "
    f"{ml_data['feature_timestamp'].min()}"
)

print(
    f"Latest:   "
    f"{ml_data['feature_timestamp'].max()}"
)


# ================================================================
# 10. RECREATE ML3 CHRONOLOGICAL TEST PERIOD
# ================================================================
#
# IMPORTANT:
#
# We reproduce the timestamp-based split used in ML3.
#
# This ensures the comparison uses exactly the same
# future test period.
#
# ================================================================

print("\nSTEP 8 — SELECTING ML3 TEST PERIOD")
print("-" * 70)

unique_timestamps = (
    ml_data["feature_timestamp"]
    .drop_duplicates()
    .sort_values()
    .reset_index(drop=True)
)


timestamp_split_index = int(
    len(unique_timestamps) * 0.80
)


recreated_train_end = (
    unique_timestamps[
        timestamp_split_index - 1
    ]
)


recreated_test_start = (
    unique_timestamps[
        timestamp_split_index
    ]
)


train_data = ml_data[
    ml_data["feature_timestamp"]
    <= recreated_train_end
].copy()


test_data = ml_data[
    ml_data["feature_timestamp"]
    >= recreated_test_start
].copy()


print(
    f"Train period:"
)

print(
    f"  {train_data['feature_timestamp'].min()}"
    f" → "
    f"{train_data['feature_timestamp'].max()}"
)

print(
    f"\nTest period:"
)

print(
    f"  {test_data['feature_timestamp'].min()}"
    f" → "
    f"{test_data['feature_timestamp'].max()}"
)


# ================================================================
# 11. CHECK SAVED MODEL TEST PERIOD
# ================================================================

print("\nSTEP 9 — VERIFYING TEST PERIOD")
print("-" * 70)

if (
    test_data["feature_timestamp"].min()
    != TEST_START
    or
    test_data["feature_timestamp"].max()
    != TEST_END
):

    print(
        "WARNING: Recreated test period does not exactly "
        "match the period stored in the ML3 model."
    )

    print(
        f"Saved test period:"
    )

    print(
        f"  {TEST_START} → {TEST_END}"
    )

    print(
        f"Recreated test period:"
    )

    print(
        f"  {test_data['feature_timestamp'].min()}"
        f" → "
        f"{test_data['feature_timestamp'].max()}"
    )

else:

    print(
        "✓ Test period matches saved ML3 model."
    )


# ================================================================
# 12. GENERATE ML3 PREDICTIONS
# ================================================================

print("\nSTEP 10 — GENERATING ML3 PREDICTIONS")
print("-" * 70)

X_test = test_data[
    FEATURE_COLUMNS
].copy()


# ------------------------------------------------
# Handle infinite values
# ------------------------------------------------

X_test = X_test.replace(
    [np.inf, -np.inf],
    np.nan
)


# ------------------------------------------------
# Use training medians
#
# IMPORTANT:
# The original model was trained using training medians.
#
# If your saved model does not contain those medians,
# we calculate them from the recreated training period.
#
# ------------------------------------------------

X_train_for_median = train_data[
    FEATURE_COLUMNS
].copy()

X_train_for_median = (
    X_train_for_median
    .replace(
        [np.inf, -np.inf],
        np.nan
    )
)

train_medians = (
    X_train_for_median
    .median()
)

X_test = X_test.fillna(
    train_medians
)


ml3_probabilities = (
    best_model.predict_proba(X_test)[:, 1]
)
test_data["ML3_PROBABILITY"] = (
    ml3_probabilities
)
test_data["ML3_PREDICTION"] = (
    ml3_probabilities >= best_threshold
).astype(int)



print(
    "ML3 predictions generated."
)

print(
    "\nML3 prediction distribution:"
)

print(
    test_data[
        "ML3_PREDICTION"
    ].value_counts()
    .sort_index()
)


# ================================================================
# 13. PREPARE ML3 COMPARISON DATA
# ================================================================

print("\nSTEP 11 — PREPARING ML3 COMPARISON DATA")
print("-" * 70)

comparison_ml3 = test_data[
    [
        "grid_id",
        "feature_timestamp",
        "ML3_PROBABILITY",
        "ML3_PREDICTION"
    ]
].copy()


# ------------------------------------------------
# ML3 feature timestamp T predicts T+1.
#
# Therefore create the actual predicted activity
# timestamp explicitly.
# ------------------------------------------------

comparison_ml3[
    "prediction_timestamp"
] = (
    comparison_ml3[
        "feature_timestamp"
    ]
    + pd.Timedelta(hours=1)
)


print(
    "ML3 prediction timestamps created."
)


# ================================================================
# 14. READ NP3 ALERTS
# ================================================================

print("\nSTEP 12 — READING NP3 ALERTS")
print("-" * 70)

np3_query = """
SELECT
    g.grid_id,
    t.timestamp,
    a.alert_type,
    a.current_activity,
    a.baseline_activity,
    a.reason,
    a.severity
FROM fact_network_alerts a
INNER JOIN dim_time t
    ON a.time_key = t.time_key
INNER JOIN dim_grid g
    ON a.grid_key = g.grid_key
ORDER BY
    g.grid_id,
    t.timestamp
"""

np3_alerts = pd.read_sql(
    np3_query,
    engine
)

print(
    f"NP3 alert rows loaded: "
    f"{len(np3_alerts):,}"
)


# ================================================================
# 15. PREPARE NP3 ALERTS
# ================================================================

print("\nSTEP 13 — PREPARING NP3 ALERTS")
print("-" * 70)

np3_alerts["grid_id"] = (
    np3_alerts["grid_id"].astype(str)
)

np3_alerts["timestamp"] = pd.to_datetime(
    np3_alerts["timestamp"]
)

print(
    "\nNP3 alert types found:"
)

print(
    np3_alerts[
        "alert_type"
    ].value_counts()
)


# ================================================================
# 16. ALIGN NP3 TIMESTAMP WITH ML3
# ================================================================

print("\nSTEP 14 — ALIGNING ML3 AND NP3 TIMESTAMPS")
print("-" * 70)

if NP3_ALERT_IS_FOR_SAME_HOUR:

    # NP3 alert at T+1 corresponds to
    # ML3 feature timestamp T.

    np3_alerts[
        "comparison_timestamp"
    ] = (
        np3_alerts["timestamp"]
        - pd.Timedelta(hours=1)
    )

    print(
        "NP3 timestamps shifted back by 1 hour."
    )

    print(
        "Reason: NP3 alert timestamp represents "
        "the activity hour being detected."
    )

else:

    np3_alerts[
        "comparison_timestamp"
    ] = np3_alerts["timestamp"]

    print(
        "NP3 timestamps NOT shifted."
    )


# ================================================================
# 17. CREATE BINARY NP3 ALERT FLAGS
# ================================================================

print("\nSTEP 15 — CREATING NP3 ALERT FLAGS")
print("-" * 70)


# ------------------------------------------------
# HIGH_ACTIVITY
# ------------------------------------------------

np3_alerts["NP3_HIGH_ACTIVITY"] = (
    np3_alerts["alert_type"]
    == "HIGH_ACTIVITY"
).astype(int)


# ------------------------------------------------
# ACTIVITY_SPIKE
# ------------------------------------------------

np3_alerts["NP3_ACTIVITY_SPIKE"] = (
    np3_alerts["alert_type"]
    == "ACTIVITY_SPIKE"
).astype(int)


# ------------------------------------------------
# ACTIVITY_DROP
# ------------------------------------------------

np3_alerts["NP3_ACTIVITY_DROP"] = (
    np3_alerts["alert_type"]
    == "ACTIVITY_DROP"
).astype(int)


# ================================================================
# 18. COLLAPSE NP3 TO GRID + TIMESTAMP
# ================================================================
#
# A grid/time may theoretically have multiple alert records.
#
# We use MAX so that:
#
#   at least one HIGH_ACTIVITY → 1
#
# ================================================================

np3_binary = (
    np3_alerts
    .groupby(
        [
            "grid_id",
            "comparison_timestamp"
        ],
        as_index=False
    )
    .agg(
        NP3_HIGH_ACTIVITY=(
            "NP3_HIGH_ACTIVITY",
            "max"
        ),
        NP3_ACTIVITY_SPIKE=(
            "NP3_ACTIVITY_SPIKE",
            "max"
        ),
        NP3_ACTIVITY_DROP=(
            "NP3_ACTIVITY_DROP",
            "max"
        )
    )
)


# ================================================================
# 19. MERGE ML3 WITH NP3
# ================================================================

print("\nSTEP 16 — MERGING ML3 AND NP3")
print("-" * 70)

comparison = comparison_ml3.merge(
    np3_binary,
    left_on=[
        "grid_id",
        "feature_timestamp"
    ],
    right_on=[
        "grid_id",
        "comparison_timestamp"
    ],
    how="left"
)


# ------------------------------------------------
# No NP3 alert means all flags = 0
# ------------------------------------------------

for column in [
    "NP3_HIGH_ACTIVITY",
    "NP3_ACTIVITY_SPIKE",
    "NP3_ACTIVITY_DROP"
]:

    comparison[column] = (
        comparison[column]
        .fillna(0)
        .astype(int)
    )


# ------------------------------------------------
# Remove duplicate comparison timestamp column
# ------------------------------------------------

comparison = comparison.drop(
    columns=[
        "comparison_timestamp"
    ]
)


print(
    f"Comparison rows: "
    f"{len(comparison):,}"
)


# ================================================================
# 20. LEAKAGE / TIMESTAMP SANITY CHECK
# ================================================================

print("\nSTEP 17 — TIMESTAMP SANITY CHECK")
print("-" * 70)

train_max = train_data[
    "feature_timestamp"
].max()

test_min = test_data[
    "feature_timestamp"
].min()


if train_max >= test_min:

    raise ValueError(
        "DATA LEAKAGE: train and test timestamps overlap!"
    )

else:

    print(
        "✓ Train and test timestamps do not overlap."
    )


print(
    f"\nTrain ends: "
    f"{train_max}"
)

print(
    f"Test starts: "
    f"{test_min}"
)


# ================================================================
# 21. MAIN HIGH_ACTIVITY COMPARISON
# ================================================================

print("\nSTEP 18 — ML3 vs NP3 HIGH_ACTIVITY")
print("-" * 70)

comparison["HIGH_ACTIVITY_COMPARISON"] = np.select(
    [
        (
            (comparison["ML3_PREDICTION"] == 1)
            &
            (comparison["NP3_HIGH_ACTIVITY"] == 1)
        ),

        (
            (comparison["ML3_PREDICTION"] == 0)
            &
            (comparison["NP3_HIGH_ACTIVITY"] == 0)
        ),

        (
            (comparison["ML3_PREDICTION"] == 1)
            &
            (comparison["NP3_HIGH_ACTIVITY"] == 0)
        ),

        (
            (comparison["ML3_PREDICTION"] == 0)
            &
            (comparison["NP3_HIGH_ACTIVITY"] == 1)
        )
    ],

    [
        "AGREE_HIGH_ACTIVITY",
        "AGREE_NO_HIGH_ACTIVITY",
        "ML3_ONLY",
        "NP3_ONLY"
    ],

    default="UNKNOWN"
)


# ================================================================
# 22. HIGH_ACTIVITY COUNTS
# ================================================================

comparison_counts = (
    comparison[
        "HIGH_ACTIVITY_COMPARISON"
    ]
    .value_counts()
)


total_comparisons = len(
    comparison
)


agreement_count = (
    comparison[
        "HIGH_ACTIVITY_COMPARISON"
    ]
    .isin(
        [
            "AGREE_HIGH_ACTIVITY",
            "AGREE_NO_HIGH_ACTIVITY"
        ]
    )
    .sum()
)


ml3_only_count = (
    comparison[
        "HIGH_ACTIVITY_COMPARISON"
    ]
    .eq("ML3_ONLY")
    .sum()
)


np3_only_count = (
    comparison[
        "HIGH_ACTIVITY_COMPARISON"
    ]
    .eq("NP3_ONLY")
    .sum()
)


agreement_rate = (
    agreement_count
    / total_comparisons
)


ml3_only_rate = (
    ml3_only_count
    / total_comparisons
)


np3_only_rate = (
    np3_only_count
    / total_comparisons
)


print(
    "\nHIGH_ACTIVITY comparison:"
)

print(
    comparison_counts
)


print(
    f"\nTotal comparisons: "
    f"{total_comparisons:,}"
)

print(
    f"Agreement: "
    f"{agreement_count:,} "
    f"({agreement_rate * 100:.2f}%)"
)

print(
    f"ML3-only: "
    f"{ml3_only_count:,} "
    f"({ml3_only_rate * 100:.2f}%)"
)

print(
    f"NP3-only: "
    f"{np3_only_count:,} "
    f"({np3_only_rate * 100:.2f}%)"
)


# ================================================================
# 23. ANALYZE NP3 ALERT TYPES
# ================================================================

print("\nSTEP 19 — ANALYZING ALL NP3 ALERT TYPES")
print("-" * 70)


alert_type_results = []


for alert_type, flag_column in [

    (
        "HIGH_ACTIVITY",
        "NP3_HIGH_ACTIVITY"
    ),

    (
        "ACTIVITY_SPIKE",
        "NP3_ACTIVITY_SPIKE"
    ),

    (
        "ACTIVITY_DROP",
        "NP3_ACTIVITY_DROP"
    )

]:

    ml3_positive = (
        comparison["ML3_PREDICTION"] == 1
    )

    np3_positive = (
        comparison[flag_column] == 1
    )


    both = (
        ml3_positive
        &
        np3_positive
    ).sum()


    ml3_only = (
        ml3_positive
        &
        ~np3_positive
    ).sum()


    np3_only = (
        ~ml3_positive
        &
        np3_positive
    ).sum()


    neither = (
        ~ml3_positive
        &
        ~np3_positive
    ).sum()


    alert_count = (
        np3_positive.sum()
    )


    alert_rate = (
        alert_count
        / total_comparisons
    )


    alert_type_results.append(
        {
            "np3_alert_type": alert_type,
            "np3_alert_count": alert_count,
            "np3_alert_rate": alert_rate,
            "both_ml3_and_np3": both,
            "ml3_only": ml3_only,
            "np3_only": np3_only,
            "neither": neither
        }
    )


    print(
        f"\n{alert_type}"
    )

    print(
        f"  NP3 alerts : {alert_count:,}"
    )

    print(
        f"  Both       : {both:,}"
    )

    print(
        f"  ML3 only   : {ml3_only:,}"
    )

    print(
        f"  NP3 only   : {np3_only:,}"
    )

    print(
        f"  Neither    : {neither:,}"
    )


alert_type_summary = pd.DataFrame(
    alert_type_results
)


# ================================================================
# 24. DETAILED NP3 INFORMATION FOR DISAGREEMENTS
# ================================================================

print("\nSTEP 20 — ANALYZING DISAGREEMENTS")
print("-" * 70)


# ------------------------------------------------
# Keep one representative NP3 record per
# grid + comparison timestamp.
#
# If multiple alerts exist at the same time,
# HIGH_ACTIVITY gets priority, followed by
# ACTIVITY_SPIKE, followed by ACTIVITY_DROP.
# ------------------------------------------------

alert_priority = {
    "HIGH_ACTIVITY": 1,
    "ACTIVITY_SPIKE": 2,
    "ACTIVITY_DROP": 3
}


np3_alerts["alert_priority"] = (
    np3_alerts["alert_type"]
    .map(alert_priority)
    .fillna(99)
)


np3_details = (
    np3_alerts
    .sort_values(
        [
            "grid_id",
            "comparison_timestamp",
            "alert_priority"
        ]
    )
    .drop_duplicates(
        subset=[
            "grid_id",
            "comparison_timestamp"
        ],
        keep="first"
    )
)


np3_details = np3_details[
    [
        "grid_id",
        "comparison_timestamp",
        "alert_type",
        "current_activity",
        "baseline_activity",
        "reason",
        "severity"
    ]
]


# ------------------------------------------------
# Merge detailed information
# ------------------------------------------------

comparison_detailed = comparison.merge(
    np3_details,
    left_on=[
        "grid_id",
        "feature_timestamp"
    ],
    right_on=[
        "grid_id",
        "comparison_timestamp"
    ],
    how="left"
)


comparison_detailed = comparison_detailed.drop(
    columns=[
        "comparison_timestamp"
    ]
)


# ================================================================
# 25. SAVE DISAGREEMENT DATA
# ================================================================

disagreements = comparison_detailed[
    comparison_detailed[
        "HIGH_ACTIVITY_COMPARISON"
    ].isin(
        [
            "ML3_ONLY",
            "NP3_ONLY"
        ]
    )
].copy()


print(
    f"Total disagreements: "
    f"{len(disagreements):,}"
)


print(
    f"  ML3-only: "
    f"{"ML3_ONLY" if False else ""}"
)


print(
    f"  ML3-only count: "
    f"{ml3_only_count:,}"
)

print(
    f"  NP3-only count: "
    f"{np3_only_count:,}"
)


# ================================================================
# 26. CHARACTERIZE ML3-ONLY CASES
# ================================================================

print("\nSTEP 21 — CHARACTERIZING ML3-ONLY CASES")
print("-" * 70)


ml3_only = comparison_detailed[
    comparison_detailed[
        "HIGH_ACTIVITY_COMPARISON"
    ] == "ML3_ONLY"
].copy()


if len(ml3_only) > 0:

    print(
        f"ML3-only cases: "
        f"{len(ml3_only):,}"
    )

    print(
        "\nThese are cases where:"
    )

    print(
        "  ML3 predicted HIGH_ACTIVITY"
    )

    print(
        "  NP3 did NOT produce HIGH_ACTIVITY"
    )

    print(
        "\nExample cases:"
    )

    print(
        ml3_only[
            [
                "grid_id",
                "feature_timestamp",
                "ML3_PREDICTION",
                "alert_type",
                "current_activity",
                "baseline_activity",
                "reason",
                "severity"
            ]
        ]
        .head(20)
        .to_string(index=False)
    )

else:

    print(
        "No ML3-only cases found."
    )


# ================================================================
# 27. CHARACTERIZE NP3-ONLY CASES
# ================================================================

print("\nSTEP 22 — CHARACTERIZING NP3-ONLY CASES")
print("-" * 70)


np3_only = comparison_detailed[
    comparison_detailed[
        "HIGH_ACTIVITY_COMPARISON"
    ] == "NP3_ONLY"
].copy()


if len(np3_only) > 0:

    print(
        f"NP3-only cases: "
        f"{len(np3_only):,}"
    )

    print(
        "\nThese are cases where:"
    )

    print(
        "  NP3 produced HIGH_ACTIVITY"
    )

    print(
        "  ML3 predicted NO HIGH_ACTIVITY"
    )

    print(
        "\nExample cases:"
    )

    print(
        np3_only[
            [
                "grid_id",
                "feature_timestamp",
                "ML3_PREDICTION",
                "alert_type",
                "current_activity",
                "baseline_activity",
                "reason",
                "severity"
            ]
        ]
        .head(20)
        .to_string(index=False)
    )

else:

    print(
        "No NP3-only cases found."
    )


# ================================================================
# 28. NP3 REASON DISTRIBUTION IN DISAGREEMENTS
# ================================================================

print("\nSTEP 23 — NP3 REASONS IN DISAGREEMENTS")
print("-" * 70)


reason_distribution = (
    disagreements[
        "reason"
    ]
    .fillna("NO_NP3_ALERT")
    .value_counts()
)


print(
    reason_distribution
)


# ================================================================
# 29. NP3 SEVERITY DISTRIBUTION IN DISAGREEMENTS
# ================================================================

print("\nSTEP 24 — NP3 SEVERITY IN DISAGREEMENTS")
print("-" * 70)


severity_distribution = (
    disagreements[
        "severity"
    ]
    .fillna("NO_NP3_ALERT")
    .value_counts()
)


print(
    severity_distribution
)


# ================================================================
# 30. GRID-LEVEL DISAGREEMENT ANALYSIS
# ================================================================

print("\nSTEP 25 — GRID-LEVEL DISAGREEMENT ANALYSIS")
print("-" * 70)


grid_summary = (
    comparison
    .groupby("grid_id")
    .agg(
        total_observations=(
            "ML3_PREDICTION",
            "size"
        ),

        ml3_high_activity=(
            "ML3_PREDICTION",
            "sum"
        ),

        np3_high_activity=(
            "NP3_HIGH_ACTIVITY",
            "sum"
        ),

        ml3_only=(
            "HIGH_ACTIVITY_COMPARISON",
            lambda x:
                (x == "ML3_ONLY").sum()
        ),

        np3_only=(
            "HIGH_ACTIVITY_COMPARISON",
            lambda x:
                (x == "NP3_ONLY").sum()
        )
    )
    .reset_index()
)


grid_summary["agreement_rate"] = (
    1
    -
    (
        (
            grid_summary["ml3_only"]
            +
            grid_summary["np3_only"]
        )
        /
        grid_summary["total_observations"]
    )
)


grid_summary = grid_summary.sort_values(
    "agreement_rate"
)


print(
    "\nGrids with lowest agreement:"
)

print(
    grid_summary.head(20)
    .to_string(index=False)
)


# ================================================================
# 31. HOUR-LEVEL DISAGREEMENT ANALYSIS
# ================================================================

print("\nSTEP 26 — HOUR-LEVEL DISAGREEMENT ANALYSIS")
print("-" * 70)


comparison["hour"] = (
    comparison[
        "feature_timestamp"
    ].dt.hour
)


hour_summary = (
    comparison
    .groupby("hour")
    .agg(
        observations=(
            "ML3_PREDICTION",
            "size"
        ),

        ml3_high_activity=(
            "ML3_PREDICTION",
            "sum"
        ),

        np3_high_activity=(
            "NP3_HIGH_ACTIVITY",
            "sum"
        ),

        ml3_only=(
            "HIGH_ACTIVITY_COMPARISON",
            lambda x:
                (x == "ML3_ONLY").sum()
        ),

        np3_only=(
            "HIGH_ACTIVITY_COMPARISON",
            lambda x:
                (x == "NP3_ONLY").sum()
        )
    )
    .reset_index()
)


hour_summary["disagreement_rate"] = (
    (
        hour_summary["ml3_only"]
        +
        hour_summary["np3_only"]
    )
    /
    hour_summary["observations"]
)


print(
    hour_summary.to_string(
        index=False
    )
)


# ================================================================
# 32. OVERALL SUMMARY
# ================================================================

summary = pd.DataFrame(
    [
        {
            "metric": "total_comparisons",
            "value": total_comparisons
        },

        {
            "metric": "agreement_count",
            "value": agreement_count
        },

        {
            "metric": "agreement_rate",
            "value": agreement_rate
        },

        {
            "metric": "ml3_only_count",
            "value": ml3_only_count
        },

        {
            "metric": "ml3_only_rate",
            "value": ml3_only_rate
        },

        {
            "metric": "np3_only_count",
            "value": np3_only_count
        },

        {
            "metric": "np3_only_rate",
            "value": np3_only_rate
        },

        {
            "metric": "ml3_model",
            "value": best_model_name
        },

        # {
        #     "metric": "ml3_p85_threshold",
        #     "value": P85_THRESHOLD
        # },

        {
            "metric": "np3_alert_timestamp_shifted",
            "value": NP3_ALERT_IS_FOR_SAME_HOUR
        }
    ]
)


# ================================================================
# 33. SAVE RESULTS
# ================================================================

print("\nSTEP 27 — SAVING COMPARISON RESULTS")
print("-" * 70)


# ------------------------------------------------
# Main comparison
# ------------------------------------------------

comparison_detailed.to_csv(
    COMPARISON_OUTPUT,
    index=False
)

print(
    f"Saved: "
    f"{COMPARISON_OUTPUT}"
)


# ------------------------------------------------
# Overall summary
# ------------------------------------------------

summary.to_csv(
    SUMMARY_OUTPUT,
    index=False
)

print(
    f"Saved: "
    f"{SUMMARY_OUTPUT}"
)


# ------------------------------------------------
# Alert type summary
# ------------------------------------------------

alert_type_summary.to_csv(
    ALERT_TYPE_OUTPUT,
    index=False
)

print(
    f"Saved: "
    f"{ALERT_TYPE_OUTPUT}"
)


# ------------------------------------------------
# Disagreements
# ------------------------------------------------

disagreements.to_csv(
    DISAGREEMENT_OUTPUT,
    index=False
)

print(
    f"Saved: "
    f"{DISAGREEMENT_OUTPUT}"
)


# ================================================================
# 34. FINAL SUMMARY
# ================================================================

print("\n" + "=" * 70)
print("ML3 vs NP3 FINAL SUMMARY")
print("=" * 70)


print(
    f"\nML3 model:"
)

print(
    f"  {best_model_name}"
)


print(
    f"\nML3 test period:"
)

print(
    f"  {test_data['feature_timestamp'].min()}"
    f" → "
    f"{test_data['feature_timestamp'].max()}"
)


print(
    f"\nML3 P85 threshold:"
)

# print(
#     f"  {P85_THRESHOLD:.6f}"
# )


print(
    f"\nTotal comparisons:"
)

print(
    f"  {total_comparisons:,}"
)


print(
    f"\nHIGH_ACTIVITY agreement:"
)

print(
    f"  {agreement_count:,}"
    f" ({agreement_rate * 100:.2f}%)"
)


print(
    f"\nML3-only:"
)

print(
    f"  {ml3_only_count:,}"
    f" ({ml3_only_rate * 100:.2f}%)"
)


print(
    f"\nNP3-only:"
)

print(
    f"  {np3_only_count:,}"
    f" ({np3_only_rate * 100:.2f}%)"
)


print(
    "\nNP3 alert types:"
)

print(
    np3_alerts[
        "alert_type"
    ]
    .value_counts()
    .to_string()
)


print(
    "\nOutput files:"
)

print(
    f"  1. {COMPARISON_OUTPUT}"
)

print(
    f"  2. {SUMMARY_OUTPUT}"
)

print(
    f"  3. {ALERT_TYPE_OUTPUT}"
)

print(
    f"  4. {DISAGREEMENT_OUTPUT}"
)


print(
    "\nML3 vs NP3 comparison completed successfully."
)

print("=" * 70)