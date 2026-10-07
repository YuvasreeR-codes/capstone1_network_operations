from datetime import datetime

from fastapi import APIRouter, HTTPException, Query

from api.models import NetworkSummary
from api.services.network_service import (
    get_network_summary
)


router = APIRouter(
    prefix="/network",
    tags=["Network"]
)


@router.get(
    "/summary",
    response_model=NetworkSummary
)
def network_summary(
    as_of: datetime | None = Query(
        default=None,
        description=(
            "Optional reporting cutoff timestamp. "
            "If omitted, the maximum timestamp "
            "in the analytics warehouse is used."
        )
    )
):

    try:

        result = get_network_summary(
            requested_as_of=as_of
        )

        return result

    except Exception as e:

        raise HTTPException(
            status_code=500,
            detail=f"Analytics data source unavailable: {str(e)}"
        )