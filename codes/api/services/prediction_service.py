import os
import pickle
import numpy as np

from datetime import datetime, timedelta
from fastapi import HTTPException

from api.services.feature_service import get_grid_features
from api.services.anomaly_service import get_anomaly_result


BASE_DIR = os.path.dirname(
    os.path.dirname(
        os.path.dirname(
            os.path.abspath(__file__)
        )
    )
)

MODEL_PATH = os.path.join(
    BASE_DIR,
    "ml",
    "ml3_output",
    "ml3_best_model.pkl"
)


def load_model_package():

    if not os.path.exists(MODEL_PATH):
        raise FileNotFoundError(
            f"ML3 model not found: {MODEL_PATH}"
        )

    with open(MODEL_PATH, "rb") as file:
        package = pickle.load(file)

    return package
MODEL_PACKAGE = load_model_package()

MODEL = MODEL_PACKAGE["model"]

FEATURE_COLUMNS = MODEL_PACKAGE["features"]

THRESHOLD = MODEL_PACKAGE.get(
    "threshold",
    0.50
)

MODEL_NAME = MODEL_PACKAGE.get(
    "model_name",
    "Unknown"
)




def predict_risk(request):

    # ========================================================
    # 1. GET FEATURES AUTOMATICALLY
    # ========================================================

    features = get_grid_features(
        request.grid_id,
        request.feature_timestamp
    )

    if features is None:
        raise HTTPException(
            status_code=404,
            detail="Features for this timestamp are not available."
        )

    # ========================================================
    # 2. PREPARE ML3 FEATURES
    # ========================================================

    X = np.array([[
    features["avg_activity"],
    features["median_activity"],
    features["median_avg_ratio"],
    features["activity_growth"],
    features["active_hours"],
    features["peak_ratio"],
    features["variability"],
    features["internet_share"],
    features["hour_of_day"],
    features["day_of_week"]
]], dtype=float)

    if not np.isfinite(X).all():
        raise HTTPException(
            status_code=500,
            detail="Feature values contain invalid numbers."
        )

    # ========================================================
    # 3. ML3 PREDICTION
    # ========================================================

    risk_score = float(
    MODEL.predict_proba(X)[0][1]
)

    prediction = int(
    risk_score >= THRESHOLD
)

    # if hasattr(MODEL, "predict_proba"):

    #     risk_score = float(
    #         MODEL.predict_proba(X)[0][1]
    #     )

    # else:

    #     risk_score = float(
    #         prediction
    #     )

    risk_level = (
        "HIGH"
        if prediction == 1
        else "LOW"
    )

    # ========================================================
    # 4. FIND ML4 RESULT FOR t+1
    # ========================================================

    feature_time = datetime.fromisoformat(
        request.feature_timestamp.replace("Z", "")
    )

    # prediction_time = (
    #     feature_time + timedelta(hours=1)
    # )

    anomaly = get_anomaly_result(
        request.grid_id,
        # prediction_time
        feature_time
    )

    # ========================================================
    # 5. ML4 AVAILABLE
    # ========================================================

    if anomaly is not None:

        return {
            "grid_id": request.grid_id,
            "feature_timestamp": request.feature_timestamp,

            # ML3
            "risk_score": round(
                risk_score,
                4
            ),
            "risk_level": risk_level,

            # ML4
            "anomaly_score": (
                float(anomaly["anomaly_score"])
                if anomaly["anomaly_score"] is not None
                else None
            ),

            "anomaly_direction":
                anomaly["ml4_direction"],

            "anomaly_reason":
                anomaly["ml4_reason"],

            # NP3
            "np3_alert":
                int(anomaly["np3_alert"]),

            "np3_alert_count":
                int(anomaly["np3_alert_count"]),

            "np3_alert_types":
                anomaly["np3_alert_types"],

            # Comparison
            "comparison":
                anomaly["comparison"],

            "comparison_reason":
                anomaly["comparison_reason"],

            "model_version":
                "ML3_XGBOOST_V1",

            "explanation_note": (
                "ML3 XGBoost predicts "
                f"{risk_level.lower()} activity for the "
                "next hour. ML4 anomaly detection and "
                "NP3 alert results are available for "
                "that predicted hour."
            )
        }

    # ========================================================
    # 6. ML4 NOT AVAILABLE
    # ========================================================

    return {
        "grid_id": request.grid_id,
        "feature_timestamp": request.feature_timestamp,

        # ML3 still works
        "risk_score": round(
            risk_score,
            4
        ),
        "risk_level": risk_level,

        # ML4 unavailable
        "anomaly_score": None,
        "anomaly_direction": None,
        "anomaly_reason": None,

        # NP3 unavailable through ML4 result
        "np3_alert": None,
        "np3_alert_count": None,
        "np3_alert_types": None,

        "comparison": None,
        "comparison_reason": None,

        "model_version":
            "XGBoost_v1",

        "explanation_note": (
            "ML3 XGBoost predicts "
            f"{risk_level.lower()} activity for the "
            "next hour. Anomaly results for this "
            "timestamp are not available."
        )
    }