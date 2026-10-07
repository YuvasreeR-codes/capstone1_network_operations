from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from api.models import GridFeatureResponse
from api.services.feature_service import get_grid_features


router = APIRouter(
    prefix="/network",
    tags=["Grid Features"]
)


@router.get(
    "/grid/{grid_id}/features",
    response_model=GridFeatureResponse
)
def grid_features(
    grid_id: str,
    as_of: Optional[datetime] = Query(None)
):
    result = get_grid_features(
        grid_id=grid_id,
        as_of=as_of
    )

    if result is None:
        raise HTTPException(
            status_code=404,
            detail=f"No features found for grid {grid_id}"
        )

    return result