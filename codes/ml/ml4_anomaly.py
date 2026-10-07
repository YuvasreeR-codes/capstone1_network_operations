import os
import pickle
import mysql.connector
import pandas as pd
import numpy as np


# ============================================================
# CONFIG
# ============================================================

DB_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "localhost"),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", "root"),
    "database": "capstone1"
}

ML3_MODEL_PATH = "ml/ml3_output/ml3_best_model.pkl"

# ML4 anomaly threshold
ANOMALY_THRESHOLD_PCT = 30.0

# Minimum number of previous days required
MIN_HISTORY_DAYS = 2

MODEL_VERSION = "ML4_ANOMALY_V1"


# ============================================================
# DATABASE CONNECTION
# ============================================================

def get_connection():
    return mysql.connector.connect(**DB_CONFIG)


# ============================================================
# LOAD NETWORK ACTIVITY
# ============================================================

def load_activity():

    conn = get_connection()

    query = """
        SELECT
            g.grid_id,
            t.timestamp AS timestamp,
            f.total_activity
        FROM fact_network_activity f
        JOIN dim_time t
            ON f.time_key = t.time_key
        JOIN dim_grid g
            ON f.grid_key = g.grid_key
        ORDER BY g.grid_id, t.timestamp
    """

    df = pd.read_sql(query, conn)

    conn.close()

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["grid_id"] = df["grid_id"].astype(str)

    df["total_activity"] = pd.to_numeric(
        df["total_activity"],
        errors="coerce"
    ).fillna(0)

    df["date"] = df["timestamp"].dt.date
    df["hour"] = df["timestamp"].dt.hour

    return df


# ============================================================
# ML4 HISTORICAL BASELINE
# ============================================================

def create_ml4_baseline(df):

    df = df.sort_values(
        ["grid_id", "timestamp"]
    ).copy()

    # --------------------------------------------------------
    # For each grid + hour-of-day:
    #
    # baseline at time t =
    # median activity from PREVIOUS DAYS at same hour
    #
    # Current hour is NOT included.
    # --------------------------------------------------------

    group_cols = ["grid_id", "hour"]

    df["previous_day_count"] = (
        df.groupby(group_cols)
          .cumcount()
    )

    df["_running_sum"] = (
        df.groupby(group_cols)["total_activity"]
          .cumsum()
    )

    # We need previous observations only.
    df["_previous_values"] = (
        df.groupby(group_cols)["total_activity"]
          .transform(
              lambda x: x.shift(1)
                        .expanding()
                        .median()
          )
    )

    df["baseline_activity"] = df["_previous_values"]

    df.drop(
        columns=[
            "_running_sum",
            "_previous_values"
        ],
        inplace=True
    )

    return df


# ============================================================
# ACTIVITY FLOOR
# ============================================================

def apply_activity_floor(df):

    daily_totals = (
        df.groupby(["grid_id", "date"])["total_activity"]
          .sum()
          .reset_index(name="daily_total_activity")
    )

    # Same idea as your NP3:
    # remove lowest 15% of grid-days.
    activity_floor = (
        daily_totals["daily_total_activity"]
        .quantile(0.15)
    )

    print(
        f"\nActivity floor selected: "
        f"{activity_floor:.4f}"
    )

    print(
        "Rationale: remove lowest 15% of grid-days "
        "where ratios can be unstable."
    )

    df = df.merge(
        daily_totals,
        on=["grid_id", "date"],
        how="left"
    )

    df = df[
        df["daily_total_activity"] >= activity_floor
    ].copy()

    return df, activity_floor


# ============================================================
# CALCULATE ML4 ANOMALY
# ============================================================

def calculate_anomaly(df):

    df = df.copy()

    # Percentage deviation
    df["deviation_pct"] = np.where(
        df["baseline_activity"] > 0,
        (
            (
                df["total_activity"]
                - df["baseline_activity"]
            )
            / df["baseline_activity"]
        ) * 100,
        np.nan
    )

    # Absolute deviation = anomaly score
    df["anomaly_score"] = (
        df["deviation_pct"].abs()
    )

    # --------------------------------------------------------
    # HIGH / LOW / NORMAL
    # --------------------------------------------------------

    df["ml4_direction"] = np.select(
        [
            df["deviation_pct"]
            >= ANOMALY_THRESHOLD_PCT,

            df["deviation_pct"]
            <= -ANOMALY_THRESHOLD_PCT
        ],
        [
            "HIGH",
            "LOW"
        ],
        default="NORMAL"
    )

    df["ml4_anomaly"] = (
        df["ml4_direction"] != "NORMAL"
    ).astype(int)

    # --------------------------------------------------------
    # Reason
    # --------------------------------------------------------

    def make_reason(row):

        if pd.isna(row["baseline_activity"]):
            return (
                "Insufficient historical same-grid "
                "same-hour data."
            )

        current = row["total_activity"]
        baseline = row["baseline_activity"]
        deviation = row["deviation_pct"]

        if row["ml4_direction"] == "HIGH":

            return (
                f"Current activity ({current:.0f}) is "
                f"{deviation:.1f}% above historical "
                f"baseline ({baseline:.0f})."
            )

        if row["ml4_direction"] == "LOW":

            return (
                f"Current activity ({current:.0f}) is "
                f"{abs(deviation):.1f}% below historical "
                f"baseline ({baseline:.0f})."
            )

        return (
            f"Current activity ({current:.0f}) is within "
            f"{ANOMALY_THRESHOLD_PCT:.0f}% of historical "
            f"baseline ({baseline:.0f})."
        )

    df["ml4_reason"] = df.apply(
        make_reason,
        axis=1
    )

    return df


# ============================================================
# LOAD ML3 MODEL
# ============================================================

def load_ml3():

    print("\nLoading ML3 model...")

    with open(
        ML3_MODEL_PATH,
        "rb"
    ) as f:

        ml3_package = pickle.load(f)

    print(
        "ML3 package loaded successfully."
    )

    print(
        "Package type:",
        type(ml3_package)
    )

    if isinstance(
        ml3_package,
        dict
    ):

        print(
            "ML3 package keys:",
            list(ml3_package.keys())
        )

    return ml3_package


# ============================================================
# EXTRACT MODEL FROM ML3 PACKAGE
# ============================================================

def extract_ml3_model(package):

    if hasattr(
        package,
        "predict"
    ):
        return package

    if isinstance(
        package,
        dict
    ):

        # Most likely structure from your ML3 code
        if "model" in package:
            return package["model"]

        if "best_model" in package:
            return package["best_model"]
        

    raise ValueError(
        "Could not find trained ML3 model inside .pkl"
    )


# ============================================================
# GET ML3 FEATURES
# ============================================================

ML3_FEATURES = [
    "avg_activity",
    "median_activity",
    "median_avg_ratio",
    "activity_growth",
    "active_hours",
    "peak_ratio",
    "variability",
    "internet_share",
    "hour_of_day",
    "day_of_week"
]


# ============================================================
# LOAD ML3 FEATURES
# ============================================================

def load_ml3_features():

    conn = get_connection()

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
        ORDER BY grid_id, feature_timestamp
    """

    features = pd.read_sql(
        query,
        conn
    )

    conn.close()

    features["grid_id"] = (
        features["grid_id"]
        .astype(str)
    )

    features["feature_timestamp"] = (
        pd.to_datetime(
            features["feature_timestamp"]
        )
    )

    return features


# ============================================================
# GENERATE ML3 PREDICTIONS
# ============================================================

def generate_ml3_predictions(
    package,
    features
):

    model = extract_ml3_model(
        package
    )

    print("\nGenerating ML3 predictions...")

    X = features[
        ML3_FEATURES
    ].copy()

    # Replace invalid values
    X = X.replace(
        [np.inf, -np.inf],
        np.nan
    )

    # Training-style median filling
    X = X.fillna(
        X.median()
    )

    predictions = model.predict(X)

    threshold = package.get(
    "best_threshold",
    0.50
)

    probabilities = (
    model.predict_proba(X)[:, 1]
)

    features["ml3_risk_score"] = (
    probabilities
)

    features["ml3_prediction"] = (
    probabilities >= threshold
).astype(int)

    # --------------------------------------------------------
    # IMPORTANT:
    #
    # ML3 prediction at t means:
    # "risk in t+1"
    #
    # Shift timestamp forward by one hour so it can be
    # compared against ML4 at the actual future hour.
    # --------------------------------------------------------

    features["prediction_timestamp"] = (
        features["feature_timestamp"]
        + pd.Timedelta(hours=1)
    )

    # # Risk score if model supports predict_proba
    # if hasattr(
    #     model,
    #     "predict_proba"
    # ):

    #     probabilities = model.predict_proba(X)

    #     features["ml3_risk_score"] = (
    #         probabilities[:, 1]
    #     )

    # else:

    #     features["ml3_risk_score"] = (
    #         features["ml3_prediction"]
    #     )

    features["ml3_risk_level"] = np.where(
        features["ml3_prediction"] == 1,
        "HIGH",
        "LOW"
    )

    return features[
        [
            "grid_id",
            "prediction_timestamp",
            "ml3_prediction",
            "ml3_risk_score",
            "ml3_risk_level"
        ]
    ]


# ============================================================
# LOAD NP3 ALERTS
# ============================================================

def load_np3_alerts():

    conn = get_connection()

    query = """
        SELECT
            g.grid_id,
            t.timestamp AS timestamp,
            a.alert_type,
            a.current_activity,
            a.baseline_activity,
            a.reason,
            a.severity
        FROM fact_network_alerts a
        JOIN dim_time t
            ON a.time_key = t.time_key
        JOIN dim_grid g
            ON a.grid_key = g.grid_key
        ORDER BY g.grid_id, t.timestamp
    """

    alerts = pd.read_sql(
        query,
        conn
    )

    conn.close()

    alerts["grid_id"] = (
        alerts["grid_id"]
        .astype(str)
    )

    alerts["timestamp"] = (
        pd.to_datetime(
            alerts["timestamp"]
        )
    )

    return alerts


# ============================================================
# PREPARE NP3 FOR COMPARISON
# ============================================================

def prepare_np3(alerts):

    np3 = (
        alerts
        .groupby(
            ["grid_id", "timestamp"]
        )
        .agg(
            np3_alert_count=(
                "alert_type",
                "count"
            ),

            np3_alert_types=(
                "alert_type",
                lambda x:
                ",".join(
                    sorted(
                        set(x)
                    )
                )
            )
        )
        .reset_index()
    )

    np3["np3_alert"] = 1

    return np3


# ============================================================
# THREE-WAY COMPARISON
# ============================================================

def compare_all(
    ml4,
    ml3,
    np3
):

    # --------------------------------------------------------
    # ML4 timestamp = actual observed hour
    # --------------------------------------------------------

    result = ml4.merge(
        ml3,
        left_on=[
            "grid_id",
            "timestamp"
        ],
        right_on=[
            "grid_id",
            "prediction_timestamp"
        ],
        how="left"
    )

    # --------------------------------------------------------
    # Add NP3
    # --------------------------------------------------------

    result = result.merge(
        np3,
        on=[
            "grid_id",
            "timestamp"
        ],
        how="left"
    )

    result["np3_alert"] = (
        result["np3_alert"]
        .fillna(0)
        .astype(int)
    )

    result["np3_alert_count"] = (
        result["np3_alert_count"]
        .fillna(0)
        .astype(int)
    )

    result["np3_alert_types"] = (
        result["np3_alert_types"]
        .fillna("")
    )

    result["ml3_prediction"] = (
        result["ml3_prediction"]
        .fillna(0)
        .astype(int)
    )

    # --------------------------------------------------------
    # Three-way comparison
    # --------------------------------------------------------

    def comparison(row):

        ml3 = row["ml3_prediction"]
        ml4 = row["ml4_anomaly"]
        np3 = row["np3_alert"]

        if ml3 == 1 and ml4 == 1 and np3 == 1:
            return "ALL_AGREE"

        if ml3 == 1 and ml4 == 1 and np3 == 0:
            return "ML3_ML4_AGREE"

        if ml3 == 1 and ml4 == 0 and np3 == 1:
            return "ML3_NP3_AGREE"

        if ml3 == 0 and ml4 == 1 and np3 == 1:
            return "ML4_NP3_AGREE"

        if ml3 == 1 and ml4 == 0 and np3 == 0:
            return "ML3_ONLY"

        if ml3 == 0 and ml4 == 1 and np3 == 0:
            return "ML4_ONLY"

        if ml3 == 0 and ml4 == 0 and np3 == 1:
            return "NP3_ONLY"

        return "NONE"

    result["comparison"] = (
        result.apply(
            comparison,
            axis=1
        )
    )

    # --------------------------------------------------------
    # Explain disagreements
    # --------------------------------------------------------

    def disagreement_reason(row):

        c = row["comparison"]

        if c == "ALL_AGREE":
            return (
                "ML3 predicted high risk, ML4 detected "
                "an anomaly, and NP3 generated an alert."
            )

        if c == "ML3_ONLY":
            return (
                "ML3 predicted next-hour high activity, "
                "but the observed hour was not anomalous "
                "and NP3 produced no alert."
            )

        if c == "ML4_ONLY":
            return (
                "ML4 detected an unusual activity level, "
                "but ML3 did not predict high risk and "
                "NP3 produced no alert."
            )

        if c == "NP3_ONLY":
            return (
                "NP3 triggered a rule-based alert, while "
                "ML3 did not predict high risk and ML4 "
                "did not classify the hour as anomalous."
            )

        if c == "ML3_ML4_AGREE":
            return (
                "ML3 predicted high risk and ML4 observed "
                "an anomaly, but NP3 thresholds did not "
                "trigger an alert."
            )

        if c == "ML3_NP3_AGREE":
            return (
                "ML3 predicted high risk and NP3 generated "
                "an alert, but ML4 did not flag the hour "
                "as anomalous."
            )

        if c == "ML4_NP3_AGREE":
            return (
                "ML4 detected an anomaly and NP3 generated "
                "an alert, but ML3 did not predict high risk."
            )

        return (
            "None of the three intelligence mechanisms "
            "flagged the hour."
        )

    result["comparison_reason"] = (
        result.apply(
            disagreement_reason,
            axis=1
        )
    )

    return result


# ============================================================
# SAVE TO MYSQL
# ============================================================

def save_results(df):

    BATCH_SIZE = 5000

    print(
        f"\nSaving {len(df):,} rows in "
        f"batches of {BATCH_SIZE:,}..."
    )

    # ========================================================
    # CREATE TABLE
    # ========================================================

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS network_anomaly_scores (
            grid_id VARCHAR(50) NOT NULL,
            timestamp DATETIME NOT NULL,

            current_activity DOUBLE,
            baseline_activity DOUBLE,
            deviation_pct DOUBLE,
            anomaly_score DOUBLE,

            ml4_direction VARCHAR(20),
            ml4_anomaly TINYINT,
            ml4_reason VARCHAR(500),

            ml3_prediction TINYINT,
            ml3_risk_score DOUBLE,
            ml3_risk_level VARCHAR(20),

            np3_alert TINYINT,
            np3_alert_count INT,
            np3_alert_types VARCHAR(500),

            comparison VARCHAR(50),
            comparison_reason VARCHAR(1000),

            model_version VARCHAR(50),

            PRIMARY KEY (grid_id, timestamp)
        )
    """)

    conn.commit()

    cursor.execute(
        "TRUNCATE TABLE network_anomaly_scores"
    )

    conn.commit()

    cursor.close()
    conn.close()

    # ========================================================
    # INSERT SQL
    # ========================================================

    insert_sql = """
        INSERT INTO network_anomaly_scores (
            grid_id,
            timestamp,
            current_activity,
            baseline_activity,
            deviation_pct,
            anomaly_score,

            ml4_direction,
            ml4_anomaly,
            ml4_reason,

            ml3_prediction,
            ml3_risk_score,
            ml3_risk_level,

            np3_alert,
            np3_alert_count,
            np3_alert_types,

            comparison,
            comparison_reason,

            model_version
        )
        VALUES (
            %s,%s,%s,%s,%s,%s,
            %s,%s,%s,
            %s,%s,%s,
            %s,%s,%s,
            %s,%s,
            %s
        )
    """

    # ========================================================
    # INSERT IN BATCHES
    # ========================================================

    total = len(df)
    inserted = 0

    for start in range(0, total, BATCH_SIZE):

        end = min(
            start + BATCH_SIZE,
            total
        )

        batch_df = df.iloc[start:end]

        rows = []

        for _, r in batch_df.iterrows():

            def clean(v):
                if pd.isna(v):
                    return None
                return v

            rows.append((
                str(r["grid_id"]),
                r["timestamp"].to_pydatetime(),

                clean(r["total_activity"]),
                clean(r["baseline_activity"]),
                clean(r["deviation_pct"]),
                clean(r["anomaly_score"]),

                r["ml4_direction"],
                int(r["ml4_anomaly"]),
                r["ml4_reason"],

                int(r["ml3_prediction"]),
                clean(r["ml3_risk_score"]),
                r["ml3_risk_level"],

                int(r["np3_alert"]),
                int(r["np3_alert_count"]),
                r["np3_alert_types"],

                r["comparison"],
                r["comparison_reason"],

                MODEL_VERSION
            ))

        # ----------------------------------------------------
        # Connect for each batch
        # ----------------------------------------------------

        try:

            conn = get_connection()
            cursor = conn.cursor()

            cursor.executemany(
                insert_sql,
                rows
            )

            conn.commit()

            cursor.close()
            conn.close()

            inserted += len(rows)

            print(
                f"Inserted "
                f"{inserted:,} / {total:,} rows"
            )

        except Exception as e:

            print(
                f"\nERROR inserting batch "
                f"{start:,} - {end:,}"
            )

            print(e)

            try:
                cursor.close()
                conn.close()
            except:
                pass

            raise

    print(
        f"\nSuccessfully saved "
        f"{inserted:,} rows to "
        f"network_anomaly_scores"
    )
# ============================================================
# PRINT SUMMARY
# ============================================================

def print_summary(df):

    print("\n")
    print("=" * 70)
    print("ML4 SUMMARY")
    print("=" * 70)

    valid = df[
        df["baseline_activity"].notna()
    ]

    print(
        f"Total grid-hours : {len(df):,}"
    )

    print(
        f"Valid baselines  : {len(valid):,}"
    )

    print(
        f"ML4 anomalies     : "
        f"{valid['ml4_anomaly'].sum():,}"
    )

    if len(valid) > 0:

        print(
            f"ML4 anomaly rate  : "
            f"{valid['ml4_anomaly'].mean()*100:.2f}%"
        )

    print("\nML4 directions:")

    print(
        df["ml4_direction"]
        .value_counts()
        .to_string()
    )

    print("\nThree-way comparison:")

    print(
        df["comparison"]
        .value_counts()
        .to_string()
    )

    print("\nNP3 alert types:")

    np3_rows = df[
        df["np3_alert"] == 1
    ]

    if len(np3_rows) > 0:

        print(
            np3_rows[
                "np3_alert_types"
            ]
            .value_counts()
            .to_string()
        )


# ============================================================
# SHOW GENUINE DISAGREEMENTS
# ============================================================

def show_disagreements(df):

    print("\n")
    print("=" * 70)
    print("GENUINE DISAGREEMENT CASES")
    print("=" * 70)

    disagreement_types = [
        "ML3_ONLY",
        "ML4_ONLY",
        "NP3_ONLY",
        "ML3_ML4_AGREE",
        "ML3_NP3_AGREE",
        "ML4_NP3_AGREE"
    ]

    for comparison_type in disagreement_types:

        examples = df[
            df["comparison"]
            == comparison_type
        ].head(3)

        if len(examples) == 0:
            continue

        print(
            f"\n--- {comparison_type} ---"
        )

        print(
            examples[
                [
                    "grid_id",
                    "timestamp",
                    "total_activity",
                    "baseline_activity",
                    "deviation_pct",
                    "ml3_prediction",
                    "ml3_risk_score",
                    "np3_alert_types",
                    "comparison_reason"
                ]
            ].to_string(index=False)
        )


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 70)
    print("ML4 ANOMALY DETECTION")
    print("=" * 70)

    # --------------------------------------------------------
    # 1. Activity
    # --------------------------------------------------------

    print("\n[1] Loading activity...")

    df = load_activity()

    print(
        f"Activity rows: {len(df):,}"
    )

    # --------------------------------------------------------
    # 2. Historical baseline
    # --------------------------------------------------------

    print(
        "\n[2] Creating historical baseline..."
    )

    df = create_ml4_baseline(
        df
    )

    # --------------------------------------------------------
    # 3. Activity floor
    # --------------------------------------------------------

    print(
        "\n[3] Applying activity floor..."
    )

    df, floor = apply_activity_floor(
        df
    )

    # --------------------------------------------------------
    # 4. ML4 anomaly
    # --------------------------------------------------------

    print(
        "\n[4] Calculating ML4 anomaly..."
    )

    df = calculate_anomaly(
        df
    )

    # --------------------------------------------------------
    # 5. ML3 model
    # --------------------------------------------------------

    print(
        "\n[5] Loading ML3 model..."
    )

    ml3_package = load_ml3()

    # --------------------------------------------------------
    # 6. ML3 features
    # --------------------------------------------------------

    print(
        "\n[6] Loading ML3 features..."
    )

    ml3_features = load_ml3_features()

    print(
        f"ML3 feature rows: "
        f"{len(ml3_features):,}"
    )

    # --------------------------------------------------------
    # 7. ML3 predictions
    # --------------------------------------------------------

    print(
        "\n[7] Generating ML3 predictions..."
    )

    ml3_predictions = (
        generate_ml3_predictions(
            ml3_package,
            ml3_features
        )
    )

    # --------------------------------------------------------
    # 8. NP3
    # --------------------------------------------------------

    print(
        "\n[8] Loading NP3 alerts..."
    )

    alerts = load_np3_alerts()

    print(
        f"NP3 alerts: {len(alerts):,}"
    )

    np3 = prepare_np3(
        alerts
    )

    # --------------------------------------------------------
    # 9. Three-way comparison
    # --------------------------------------------------------

    print(
        "\n[9] Comparing ML3 + ML4 + NP3..."
    )

    result = compare_all(
        df,
        ml3_predictions,
        np3
    )

    # --------------------------------------------------------
    # 10. Summary
    # --------------------------------------------------------

    print_summary(
        result
    )

    # --------------------------------------------------------
    # 11. Disagreements
    # --------------------------------------------------------

    show_disagreements(
        result
    )

    # --------------------------------------------------------
    # 12. Save
    # --------------------------------------------------------

    print(
        "\n[10] Saving ML4 results..."
    )

    save_results(
        result
    )

    print(
        "\n" + "=" * 70
    )

    print(
        "ML4 COMPLETE"
    )

    print(
        "=" * 70
    )


if __name__ == "__main__":
    main()