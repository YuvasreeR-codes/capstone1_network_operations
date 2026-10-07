from fastapi import APIRouter

from api.models import (
    RiskPredictionRequest,
    RiskPredictionResponse
)

from api.services.prediction_service import predict_risk


router = APIRouter(
    prefix="/network",
    tags=["Risk Prediction"]
)


@router.post(
    "/predict-risk",
    response_model=RiskPredictionResponse
)
def predict_network_risk(
    request: RiskPredictionRequest
):
    return predict_risk(request)