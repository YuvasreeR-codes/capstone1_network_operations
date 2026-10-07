from fastapi import APIRouter, HTTPException, Query

from api.models import (
    PipelineStatusResponse,
    GridLocationResponse,
    GridNeighboursResponse
)

from api.services.operational_service import (
    get_pipeline_status,
    get_grid_location,
    get_grid_neighbours
)


router = APIRouter(
    tags=["Operational Support"]
)


# ---------------------------------------------------------
# PIPELINE STATUS
# ---------------------------------------------------------

@router.get(
    "/pipeline/status",
    response_model=PipelineStatusResponse
)
def pipeline_status():

    result = get_pipeline_status()

    if result is None:
        raise HTTPException(
            status_code=404,
            detail="Pipeline status record not found"
        )

    return result


# ---------------------------------------------------------
# GRID LOCATION
# ---------------------------------------------------------

@router.get(
    "/network/grid/{grid_id}/location",
    response_model=GridLocationResponse
)
def grid_location(grid_id: str):

    result = get_grid_location(grid_id)

    if result is None:
        raise HTTPException(
            status_code=404,
            detail=f"Grid {grid_id} not found"
        )

    return result


# ---------------------------------------------------------
# GRID NEIGHBOURS
# ---------------------------------------------------------

@router.get(
    "/network/grid/{grid_id}/neighbours",
    response_model=GridNeighboursResponse
)
def grid_neighbours(
    grid_id: str,
    limit: int = Query(
        5,
        ge=1,
        le=20
    )
):

    result = get_grid_neighbours(
        grid_id=grid_id,
        limit=limit
    )

    if result is None:
        raise HTTPException(
            status_code=404,
            detail=f"Grid {grid_id} not found"
        )

    return result