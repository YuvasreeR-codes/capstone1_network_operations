
# ================================================================
# ML3 — TRAIN A SIMPLE RISK CLASSIFIER
# ================================================================
#
# Models:
#   1. Logistic Regression
#   2. Decision Tree
#
# Target:
#   HIGH_ACTIVITY = 1 when next-hour activity >= training P85
#
# Data:
#   network_feature_table -> ML2 features
#   fact_network_activity -> actual next-hour activity
#   dim_time               -> timestamp lookup
#   dim_grid               -> grid lookup
#
# Process:
#   1. Read data from MySQL
#   2. Create next-hour activity
#   3. Chronological train/test split
#   4. Calculate P85 using TRAINING data only
#   5. Create HIGH_ACTIVITY target
#   6. Train Logistic Regression and Decision Tree
#   7. Evaluate both models
#   8. Select the better model
#   9. Inspect model interpretability
#  10. Save model and evaluation report
#
# ================================================================


import os
import pickle
import warnings

import numpy as np
import pandas as pd

from sqlalchemy import create_engine

from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    classification_report,
    confusion_matrix
)

warnings.filterwarnings("ignore")


# ================================================================
# 1. CONFIGURATION
# ================================================================

DB_USER = "root"
DB_PASSWORD = "root"
DB_HOST = "localhost"
DB_PORT = "3306"
DB_NAME = "capstone1"

# Output directory
OUTPUT_DIR = "ml3_output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

MODEL_OUTPUT = os.path.join(
    OUTPUT_DIR,
    "ml3_best_model.pkl"
)

REPORT_OUTPUT = os.path.join(
    OUTPUT_DIR,
    "ml3_evaluation_report.csv"
)

IMPORTANCE_OUTPUT = os.path.join(
    OUTPUT_DIR,
    "ml3_feature_importance.csv"
)


# ================================================================
# 2. DATABASE CONNECTION
# ================================================================

print("=" * 70)
print("ML3 — SIMPLE RISK CLASSIFIER")
print("=" * 70)

print("\nSTEP 1 — CONNECTING TO DATABASE")
print("-" * 70)

connection_string = (
    f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

engine = create_engine(connection_string)

print("Database connection created successfully.")


# ================================================================
# 3. READ ML2 FEATURE TABLE
# ================================================================

print("\nSTEP 2 — READING NETWORK FEATURE TABLE")
print("-" * 70)

feature_query = """
SELECT
    grid_id,
    feature_timestamp,
    avg_activity,
    activity_growth,
    active_hours,
    peak_ratio,
    variability,
    internet_share
FROM network_feature_table
"""

features = pd.read_sql(feature_query, engine)

print(f"Feature rows loaded: {len(features):,}")

print("\nFeature columns:")
print(features.columns.tolist())


# ================================================================
# 4. READ DIM_TIME
# ================================================================
# ================================================================
# 4. READ ACTIVITY LOOKUP (REPLACES DIM_TIME + DIM_GRID + FACT)
# ================================================================

print("\nSTEP 3 — READING ACTIVITY LOOKUP")
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
ORDER BY g.grid_id, t.timestamp
"""

fact = pd.read_sql(activity_query, engine)

print(f"Activity rows loaded: {len(fact):,}")

# Convert timestamp
fact["timestamp"] = pd.to_datetime(
    fact["timestamp"]
)

print(
    "Activity period:",
    fact["timestamp"].min(),
    "to",
    fact["timestamp"].max()
)



# ================================================================
# 7. PREPARE TIMESTAMPS
# ================================================================
# ================================================================
# PREPARE TIMESTAMPS
# ================================================================

print("\nSTEP 4 — PREPARING TIMESTAMPS")
print("-" * 70)

features["feature_timestamp"] = pd.to_datetime(
    features["feature_timestamp"]
)

# ================================================================
# STANDARDIZING GRID IDS
# ================================================================

print("\nSTEP 5 — STANDARDIZING GRID IDS")
print("-" * 70)

features["grid_id"] = features["grid_id"].astype(str)
fact["grid_id"] = fact["grid_id"].astype(str)


# ================================================================
# 10. CREATE NEXT-HOUR ACTIVITY
# ================================================================
print("\nSTEP 6 — CREATING NEXT-HOUR ACTIVITY")
print("-" * 70)

features["target_timestamp"] = (
    features["feature_timestamp"]
    + pd.Timedelta(hours=1)
)

fact = fact.rename(
    columns={
        "timestamp": "target_timestamp",
        "total_activity": "next_hour_activity"
    }
)

ml_data = features.merge(
    fact,
    on=["grid_id", "target_timestamp"],
    how="left"
)

print(f"Rows after next-hour join: {len(ml_data):,}")

missing_target = ml_data["next_hour_activity"].isna().sum()

print(
    f"Rows without next-hour activity: "
    f"{missing_target:,}"
)

# ================================================================
# 11. REMOVE ROWS WITHOUT NEXT-HOUR OBSERVATION
# ================================================================

print("\nSTEP 10 — REMOVING ROWS WITHOUT TARGET")
print("-" * 70)

before = len(ml_data)

ml_data = ml_data.dropna(
    subset=["next_hour_activity"]
).copy()

after = len(ml_data)

print(f"Rows before: {before:,}")
print(f"Rows after : {after:,}")
print(f"Removed    : {before - after:,}")


# ================================================================
# 12. SORT CHRONOLOGICALLY
# ================================================================

print("\nSTEP 11 — CHRONOLOGICAL SORT")
print("-" * 70)

ml_data = ml_data.sort_values(
    "feature_timestamp"
).reset_index(drop=True)

print(
    "Earliest timestamp:",
    ml_data["feature_timestamp"].min()
)

print(
    "Latest timestamp  :",
    ml_data["feature_timestamp"].max()
)


# ================================================================
# 13. CHRONOLOGICAL TRAIN / TEST SPLIT
# ================================================================

# ================================================================
# CHRONOLOGICAL TRAIN / TEST SPLIT BY TIMESTAMP
# ================================================================

print("\nSTEP 12 — CHRONOLOGICAL TRAIN / TEST SPLIT")
print("-" * 70)

# Get unique timestamps
unique_timestamps = (
    ml_data["feature_timestamp"]
    .drop_duplicates()
    .sort_values()
    .reset_index(drop=True)
)

# 80% earlier timestamps -> training
# 20% later timestamps   -> testing
timestamp_split_index = int(
    len(unique_timestamps) * 0.80
)

train_end_timestamp = unique_timestamps[
    timestamp_split_index - 1
]

test_start_timestamp = unique_timestamps[
    timestamp_split_index
]

train_data = ml_data[
    ml_data["feature_timestamp"] <= train_end_timestamp
].copy()

test_data = ml_data[
    ml_data["feature_timestamp"] >= test_start_timestamp
].copy()

print(f"Unique timestamps : {len(unique_timestamps):,}")
print(f"Train rows        : {len(train_data):,}")
print(f"Test rows         : {len(test_data):,}")

print("\nTRAIN PERIOD")
print("Earliest:", train_data["feature_timestamp"].min())
print("Latest  :", train_data["feature_timestamp"].max())

print("\nTEST PERIOD")
print("Earliest:", test_data["feature_timestamp"].min())
print("Latest  :", test_data["feature_timestamp"].max())

# Leakage check
assert (
    train_data["feature_timestamp"].max()
    < test_data["feature_timestamp"].min()
), "DATA LEAKAGE: train/test timestamps overlap!"

print("\n✓ Train/test timestamps do not overlap.")


# ================================================================
# 14. CALCULATE P85 USING TRAINING DATA ONLY
# ================================================================

# print("\nSTEP 13 — CALCULATING TRAINING P85")
# print("-" * 70)

# p85_threshold = train_data[
#     "next_hour_activity"
# ].quantile(0.85)

# print(
#     f"Training P85 threshold: "
#     f"{p85_threshold:.6f}"
# )
# print("\nSTEP 13 — CALCULATING PER-GRID TRAINING P85")
# print("-" * 70)

# grid_p85 = (
#     train_data
#     .groupby("grid_id")["next_hour_activity"]
#     .quantile(0.85)
# )

# print(f"Number of grid thresholds: {len(grid_p85):,}")

# print("\nSample per-grid P85 thresholds:")
# print(grid_p85.head(10))
print("\nSTEP 13 — CALCULATING PER-GRID TRAINING P85")
print("-" * 70)

# Learn thresholds ONLY from training data
grid_p85 = (
    train_data
    .groupby("grid_id")["avg_activity"]
    .quantile(0.85)
)

print(
    f"Number of grid thresholds: "
    f"{len(grid_p85):,}"
)

print("\nSample per-grid P85 thresholds:")
print(grid_p85.head(10))

# ================================================================
# 15. CREATE HIGH_ACTIVITY TARGET
# ================================================================
# print("\nSTEP 14 — CREATING PER-GRID HIGH_ACTIVITY TARGET")
# print("-" * 70)

# train_data["p85_threshold"] = (
#     train_data["grid_id"]
#     .map(grid_p85)
# )

# test_data["p85_threshold"] = (
#     test_data["grid_id"]
#     .map(grid_p85)
# )

# assert train_data["p85_threshold"].notna().all(), \
#     "Missing P85 threshold for a training grid."

# assert test_data["p85_threshold"].notna().all(), \
#     "A test grid has no training P85 threshold."

# train_data["HIGH_ACTIVITY"] = (
#     train_data["next_hour_activity"]
#     >= train_data["p85_threshold"]
# ).astype(int)

# test_data["HIGH_ACTIVITY"] = (
#     test_data["next_hour_activity"]
#     >= test_data["p85_threshold"]
# ).astype(int)

# print("\nPer-grid P85 target created successfully.")

# print("\nPER-GRID P85 SUMMARY")
# print("-" * 70)

# print(grid_p85.describe())

# print(
#     f"\nMinimum P85: {grid_p85.min():.6f}"
# )

# print(
#     f"Maximum P85: {grid_p85.max():.6f}"
# )

# print(
#     f"Median P85: {grid_p85.median():.6f}"
# )
# print("\nSTEP 14 — CREATING HIGH_ACTIVITY TARGET")
# print("-" * 70)

# train_data["HIGH_ACTIVITY"] = (
#     train_data["next_hour_activity"]
#     >= p85_threshold
# ).astype(int)

# test_data["HIGH_ACTIVITY"] = (
#     test_data["next_hour_activity"]
#     >= p85_threshold
# ).astype(int)

# print("\nSTEP 13 — CALCULATING PER-GRID TRAINING P85")
# print("-" * 70)

# # Each grid gets its own P85 threshold.
# # IMPORTANT: thresholds are calculated from TRAINING DATA ONLY.

# grid_p85 = (
#     train_data
#     .groupby("grid_id")["next_hour_activity"]
#     .quantile(0.85)
# )

# print(f"Number of grid thresholds: {len(grid_p85):,}")

# print("\nSample per-grid P85 thresholds:")
# print(grid_p85.head(10))
print("\nSTEP 14 — CREATING HIGH_ACTIVITY TARGET")
print("-" * 70)

train_data["p85_threshold"] = (
    train_data["grid_id"]
    .map(grid_p85)
)

test_data["p85_threshold"] = (
    test_data["grid_id"]
    .map(grid_p85)
)

assert train_data["p85_threshold"].notna().all(), \
    "Missing P85 threshold for a training grid."

assert test_data["p85_threshold"].notna().all(), \
    "A test grid has no training P85 threshold."

# --------------------------------------------------
# TARGET:
# future avg_activity exceeds grid-specific P85
# --------------------------------------------------

train_data["HIGH_ACTIVITY"] = (
    train_data["avg_activity"]
    >= train_data["p85_threshold"]
).astype(int)

test_data["HIGH_ACTIVITY"] = (
    test_data["avg_activity"]
    >= test_data["p85_threshold"]
).astype(int)

print("\nPer-grid P85 target created successfully.")

print("\nPER-GRID P85 SUMMARY")
print("-" * 70)

print(grid_p85.describe())

print(
    f"\nMinimum P85: "
    f"{grid_p85.min():.6f}"
)

print(
    f"Maximum P85: "
    f"{grid_p85.max():.6f}"
)

print(
    f"Median P85: "
    f"{grid_p85.median():.6f}"
)

# # ================================================================
# # 15. CREATE PER-GRID HIGH_ACTIVITY TARGET
# # ================================================================

# print("\nSTEP 14 — CREATING PER-GRID HIGH_ACTIVITY TARGET")
# print("-" * 70)

# # Apply each grid's own training P85 threshold
# train_data["p85_threshold"] = (
#     train_data["grid_id"]
#     .map(grid_p85)
# )

# test_data["p85_threshold"] = (
#     test_data["grid_id"]
#     .map(grid_p85)
# )

# # Safety check:
# # every test grid must have a threshold learned from training data
# assert train_data["p85_threshold"].notna().all(), \
#     "Missing P85 threshold for a training grid."

# assert test_data["p85_threshold"].notna().all(), \
#     "A test grid has no training P85 threshold."


# # Create target
# train_data["HIGH_ACTIVITY"] = (
#     train_data["next_hour_activity"]
#     >= train_data["p85_threshold"]
# ).astype(int)

# test_data["HIGH_ACTIVITY"] = (
#     test_data["next_hour_activity"]
#     >= test_data["p85_threshold"]
# ).astype(int)


# print("\nPer-grid P85 target created successfully.")

# print("\nPER-GRID P85 SUMMARY")
# print("-" * 70)

# print(
#     grid_p85.describe()
# )

# print(
#     f"\nMinimum P85: {grid_p85.min():.6f}"
# )

# print(
#     f"Maximum P85: {grid_p85.max():.6f}"
# )

# print(
#     f"Median P85: {grid_p85.median():.6f}"
# )
# ================================================================
# 16. CLASS BALANCE
# ================================================================

print("\nSTEP 15 — CLASS BALANCE")
print("-" * 70)

train_counts = train_data[
    "HIGH_ACTIVITY"
].value_counts()

test_counts = test_data[
    "HIGH_ACTIVITY"
].value_counts()

train_base_rate = train_data[
    "HIGH_ACTIVITY"
].mean()

test_base_rate = test_data[
    "HIGH_ACTIVITY"
].mean()

print("\nTRAIN")
print(
    train_data["HIGH_ACTIVITY"]
    .value_counts()
    .sort_index()
)

print(
    f"Training positive base rate: "
    f"{train_base_rate:.4f} "
    f"({train_base_rate * 100:.2f}%)"
)

print("\nTEST")
print(
    test_data["HIGH_ACTIVITY"]
    .value_counts()
    .sort_index()
)

print(
    f"Test positive base rate: "
    f"{test_base_rate:.4f} "
    f"({test_base_rate * 100:.2f}%)"
)


# ================================================================
# 17. DEFINE FEATURES AND TARGET
# ================================================================

print("\nSTEP 16 — DEFINING ML FEATURES")
print("-" * 70)

FEATURE_COLUMNS = [
    "avg_activity",
    "activity_growth",
    "active_hours",
    "peak_ratio",
    "variability",
    "internet_share"
]

TARGET_COLUMN = "HIGH_ACTIVITY"

X_train = train_data[
    FEATURE_COLUMNS
].copy()

y_train = train_data[
    TARGET_COLUMN
].copy()

X_test = test_data[
    FEATURE_COLUMNS
].copy()

y_test = test_data[
    TARGET_COLUMN
].copy()

print("Features:")
for feature in FEATURE_COLUMNS:
    print(f"  - {feature}")

print(f"\nTarget: {TARGET_COLUMN}")


# ================================================================
# 18. HANDLE MISSING / INFINITE FEATURE VALUES
# ================================================================

print("\nSTEP 17 — FEATURE QUALITY CHECK")
print("-" * 70)

# Replace infinite values with NaN
X_train = X_train.replace(
    [np.inf, -np.inf],
    np.nan
)

X_test = X_test.replace(
    [np.inf, -np.inf],
    np.nan
)

print("\nMissing values in training:")
print(X_train.isna().sum())

print("\nMissing values in testing:")
print(X_test.isna().sum())


# For a simple baseline, use training medians.
train_medians = X_train.median()

X_train = X_train.fillna(train_medians)
X_test = X_test.fillna(train_medians)


# ================================================================
# 19. MODEL 1 — LOGISTIC REGRESSION
# ================================================================

print("\nSTEP 18 — TRAINING LOGISTIC REGRESSION")
print("-" * 70)

logistic_model = Pipeline(
    steps=[
        (
            "scaler",
            StandardScaler()
        ),
        (
            "classifier",
            LogisticRegression(
                max_iter=1000,
                class_weight="balanced",
                random_state=42
            )
        )
    ]
)

logistic_model.fit(
    X_train,
    y_train
)

logistic_predictions = (
    logistic_model.predict(X_test)
)

print("Logistic Regression training complete.")


# ================================================================
# 20. MODEL 2 — DECISION TREE
# ================================================================

print("\nSTEP 19 — TRAINING DECISION TREE")
print("-" * 70)

decision_tree_model = DecisionTreeClassifier(
    max_depth=5,
    min_samples_leaf=50,
    class_weight="balanced",
    random_state=42
)

decision_tree_model.fit(
    X_train,
    y_train
)

tree_predictions = (
    decision_tree_model.predict(X_test)
)

print("Decision Tree training complete.")


# ================================================================
# 21. EVALUATION FUNCTION
# ================================================================

def evaluate_model(
    model_name,
    y_true,
    predictions,
    base_rate
):

    accuracy = accuracy_score(
        y_true,
        predictions
    )

    precision = precision_score(
        y_true,
        predictions,
        zero_division=0
    )

    recall = recall_score(
        y_true,
        predictions,
        zero_division=0
    )

    cm = confusion_matrix(
        y_true,
        predictions
    )

    print("\n" + "=" * 70)
    print(model_name)
    print("=" * 70)

    print(
        f"Accuracy : {accuracy:.4f}"
    )

    print(
        f"Precision: {precision:.4f}"
    )

    print(
        f"Recall   : {recall:.4f}"
    )

    print(
        f"Base rate: {base_rate:.4f}"
    )

    print("\nConfusion Matrix:")
    print(cm)

    print("\nClassification Report:")
    print(
        classification_report(
            y_true,
            predictions,
            zero_division=0
        )
    )

    return {
        "model": model_name,
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "base_rate": base_rate
    }


# ================================================================
# 22. EVALUATE LOGISTIC REGRESSION
# ================================================================

logistic_results = evaluate_model(
    "Logistic Regression",
    y_test,
    logistic_predictions,
    test_base_rate
)


# ================================================================
# 23. EVALUATE DECISION TREE
# ================================================================

tree_results = evaluate_model(
    "Decision Tree",
    y_test,
    tree_predictions,
    test_base_rate
)


# ================================================================
# 24. COMPARE MODELS
# ================================================================

print("\nSTEP 20 — MODEL COMPARISON")
print("-" * 70)

results = pd.DataFrame(
    [
        logistic_results,
        tree_results
    ]
)

print(
    results.to_string(
        index=False
    )
)


# ================================================================
# 25. SELECT BEST MODEL
# ================================================================

# Primary comparison:
# precision + recall + accuracy.
#
# For this risk classifier, recall is important because
# missing a genuinely high-activity period can be operationally
# costly.
#
# We therefore use F1 as a balanced comparison metric.

from sklearn.metrics import f1_score

logistic_f1 = f1_score(
    y_test,
    logistic_predictions,
    zero_division=0
)

tree_f1 = f1_score(
    y_test,
    tree_predictions,
    zero_division=0
)

results["f1"] = [
    logistic_f1,
    tree_f1
]

print("\nModel comparison including F1:")
print(
    results.to_string(
        index=False
    )
)

if tree_f1 > logistic_f1:

    best_model = decision_tree_model
    best_model_name = "Decision Tree"

else:

    best_model = logistic_model
    best_model_name = "Logistic Regression"


print(
    f"\nSelected final model: "
    f"{best_model_name}"
)


# ================================================================
# 26. INTERPRETABILITY — LOGISTIC REGRESSION
# ================================================================

print("\nSTEP 21 — LOGISTIC REGRESSION COEFFICIENTS")
print("-" * 70)

logistic_classifier = (
    logistic_model.named_steps["classifier"]
)

logistic_coefficients = pd.DataFrame(
    {
        "feature": FEATURE_COLUMNS,
        "coefficient": logistic_classifier.coef_[0]
    }
)

logistic_coefficients[
    "absolute_coefficient"
] = logistic_coefficients[
    "coefficient"
].abs()

logistic_coefficients = (
    logistic_coefficients
    .sort_values(
        "absolute_coefficient",
        ascending=False
    )
)

print(
    logistic_coefficients.to_string(
        index=False
    )
)


# ================================================================
# 27. INTERPRETABILITY — DECISION TREE
# ================================================================

print("\nSTEP 22 — DECISION TREE FEATURE IMPORTANCE")
print("-" * 70)

tree_importance = pd.DataFrame(
    {
        "feature": FEATURE_COLUMNS,
        "importance":
            decision_tree_model.feature_importances_
    }
)

tree_importance = (
    tree_importance
    .sort_values(
        "importance",
        ascending=False
    )
)

print(
    tree_importance.to_string(
        index=False
    )
)


# ================================================================
# 28. SAVE FEATURE IMPORTANCE / COEFFICIENTS
# ================================================================

print("\nSTEP 23 — SAVING INTERPRETABILITY RESULTS")
print("-" * 70)

importance_output = tree_importance.copy()

importance_output[
    "logistic_coefficient"
] = importance_output[
    "feature"
].map(
    logistic_coefficients.set_index(
        "feature"
    )["coefficient"]
)

importance_output.to_csv(
    IMPORTANCE_OUTPUT,
    index=False
)

print(
    f"Saved: {IMPORTANCE_OUTPUT}"
)


# ================================================================
# 29. ADD F1 TO EVALUATION REPORT
# ================================================================

results["f1"] = [
    logistic_f1,
    tree_f1
]

results[
    "selected_final_model"
] = results[
    "model"
].eq(best_model_name)


# ================================================================
# 30. SAVE EVALUATION REPORT
# ================================================================

print("\nSTEP 24 — SAVING EVALUATION REPORT")
print("-" * 70)

results.to_csv(
    REPORT_OUTPUT,
    index=False
)

print(
    f"Saved: {REPORT_OUTPUT}"
)


# ================================================================
# 31. SAVE BEST MODEL
# ================================================================

print("\nSTEP 25 — SAVING BEST MODEL")
print("-" * 70)

model_package = {
    "model": best_model,
    "model_name": best_model_name,
    "features": FEATURE_COLUMNS,
    "target": TARGET_COLUMN,
    "grid_p85_thresholds": grid_p85.to_dict(),
    # "p85_threshold": p85_threshold,
    "train_start": train_data[
        "feature_timestamp"
    ].min(),
    "train_end": train_data[
        "feature_timestamp"
    ].max(),
    "test_start": test_data[
        "feature_timestamp"
    ].min(),
    "test_end": test_data[
        "feature_timestamp"
    ].max()
}

with open(
    MODEL_OUTPUT,
    "wb"
) as file:

    pickle.dump(
        model_package,
        file
    )

print(
    f"Saved: {MODEL_OUTPUT}"
)


# ================================================================
# 32. FINAL ML3 SUMMARY
# ================================================================

print("\n" + "=" * 70)
print("ML3 FINAL SUMMARY")
print("=" * 70)

print(
    f"\nTotal usable observations : "
    f"{len(ml_data):,}"
)

print(
    f"Training observations     : "
    f"{len(train_data):,}"
)

print(
    f"Testing observations      : "
    f"{len(test_data):,}"
)

print(
    "\nTRAIN PERIOD:"
)

print(
    f"  {train_data['feature_timestamp'].min()}"
    f" → "
    f"{train_data['feature_timestamp'].max()}"
)

print(
    "\nTEST PERIOD:"
)

print(
    f"  {test_data['feature_timestamp'].min()}"
    f" → "
    f"{test_data['feature_timestamp'].max()}"
)


print(
    f"\nTraining base rate: "
    f"{train_base_rate * 100:.2f}%"
)

print(
    f"Test base rate: "
    f"{test_base_rate * 100:.2f}%"
)

print(
    f"\nLogistic Regression F1: "
    f"{logistic_f1:.4f}"
)

print(
    f"Decision Tree F1: "
    f"{tree_f1:.4f}"
)

print(
    f"\nFINAL MODEL: "
    f"{best_model_name}"
)

print("\nOutput files:")
print(
    f"  Model  : {MODEL_OUTPUT}"
)
print(
    f"  Report : {REPORT_OUTPUT}"
)
print(
    f"  Feature: {IMPORTANCE_OUTPUT}"
)

print("\nML3 completed successfully.")
print("=" * 70)
