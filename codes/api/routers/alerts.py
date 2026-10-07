from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from api.models import (
    HotspotResponse,
    AlertResponse
)

from api.services.alert_service import (
    get_hotspots,
    get_alerts
)


router = APIRouter(
    prefix="/network",
    tags=["Network Alerts"]
)


# ---------------------------------------------------------
# GET HOTSPOTS
# ---------------------------------------------------------

@router.get(
    "/hotspots",
    response_model=HotspotResponse
)
def hotspots(
    limit: int = Query(
        10,
        ge=1,
        le=100
    ),

    as_of: Optional[datetime] = None
):

    try:

        return get_hotspots(
            limit=limit,
            as_of=as_of
        )

    except ValueError as e:

        raise HTTPException(
            status_code=404,
            detail=str(e)
        )

    except Exception:

        raise HTTPException(
            status_code=500,
            detail="Unable to retrieve hotspots"
        )


# ---------------------------------------------------------
# GET ALERTS
# ---------------------------------------------------------

@router.get(
    "/alerts",
    response_model=AlertResponse
)
def alerts(
    limit: int = Query(
        10,
        ge=1,
        le=100
    ),

    severity: Optional[str] = Query(
        None,
        description=(
            "Minimum severity: "
            "LOW, MEDIUM, HIGH or CRITICAL"
        )
    ),

    as_of: Optional[datetime] = None
):

    try:

        return get_alerts(
            limit=limit,
            severity=severity,
            as_of=as_of
        )

    except ValueError as e:

        raise HTTPException(
            status_code=400,
            detail=str(e)
        )

    except Exception:

        raise HTTPException(
            status_code=500,
            detail="Unable to retrieve alerts"
        )