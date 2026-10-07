from datetime import date, datetime

from fastapi import (
    APIRouter,
    HTTPException,
    Query
)

from api.models import GridActivityResponse
from api.services.network_service import (
    get_grid_activity
)


router = APIRouter(
    prefix="/network",
    tags=["Network"]
)


@router.get(
    "/grid/{grid_id}",
    response_model=GridActivityResponse
)
def grid_activity(

    grid_id: str,

    date: date | None = Query(
        default=None,
        description="Optional date, e.g. 2013-11-05"
    ),

    hour: int | None = Query(
        default=None,
        ge=0,
        le=23,
        description="Optional hour from 0 to 23"
    ),

    as_of: datetime | None = Query(
        default=None,
        description=(
            "Optional reporting cutoff timestamp. "
            "Defaults to the maximum timestamp "
            "available in the warehouse."
        )
    )
):

    try:

        result = get_grid_activity(
            grid_id=grid_id,
            requested_date=date,
            requested_hour=hour,
            requested_as_of=as_of
        )

        # -------------------------------------------------
        # Unknown grid
        # -------------------------------------------------

        if result is None:

            raise HTTPException(
                status_code=404,
                detail=f"Grid {grid_id} not found."
            )

        return result

    except HTTPException:
        raise

    except Exception as e:

        raise HTTPException(
            status_code=500,
            detail=(
                "Analytics data source unavailable: "
                f"{str(e)}"
            )
        )