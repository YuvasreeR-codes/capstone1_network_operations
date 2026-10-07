from datetime import datetime

from pydantic import BaseModel, Field
from typing import Optional

class NetworkSummary(BaseModel):

    total_activity: float
    active_grids: int
    peak_hour: int
    top_grid: str
    as_of: datetime

class GridActivityPoint(BaseModel):
    timestamp: datetime
    date: str
    hour: int

    sms_in: float
    sms_out: float
    call_in: float
    call_out: float

    total_sms: float
    total_calls: float
    internet_activity: float
    total_activity: float


class GridActivityResponse(BaseModel):
    grid_id: str
    as_of: datetime
    start_time: datetime
    end_time: datetime
    data: list[GridActivityPoint]

class HotspotItem(BaseModel):
    grid_id: str
    timestamp: datetime

    sms_in: float
    sms_out: float
    call_in: float
    call_out: float
    total_sms: float
    total_calls: float
    internet_activity: float
    total_activity: float

    status: str
    severity: str
    reason: str

    # Future ML fields
    risk_score: Optional[float] = None
    risk_level: Optional[str] = None
    model_version: Optional[str] = None
class HotspotResponse(BaseModel):
    as_of: datetime
    limit: int
    count: int
    data: list[HotspotItem]

class AlertItem(BaseModel):
    grid_id: str
    timestamp: datetime

    alert_type: str
    current_activity: float
    baseline_activity: float

    sms_in: float
    sms_out: float
    call_in: float
    call_out: float
    total_sms: float
    total_calls: float
    internet_activity: float
    total_activity: float

    status: str
    severity: str
    reason: str

    # Future ML fields
    risk_score: Optional[float] = None
    risk_level: Optional[str] = None
    model_version: Optional[str] = None

class AlertResponse(BaseModel):
    as_of: datetime
    limit: int
    count: int
    data: list[AlertItem]

class GridFeatureResponse(BaseModel):
    grid_id: str
    feature_timestamp: datetime

    avg_activity: float
    median_activity:float
    median_avg_ratio:float
    activity_growth: float
    active_hours: int
    peak_ratio: float
    variability: float
    internet_share: float
    hour_of_day:int
    day_of_week:int

    data_quality: str
    feature_freshness: str

# class RiskPredictionRequest(BaseModel):
#     grid_id: str = Field(..., min_length=1)
#     feature_timestamp: str
#     avg_activity: float = Field(..., ge=0)
#     activity_growth: float
#     active_hours: int = Field(..., ge=0, le=24)
#     peak_ratio: float = Field(..., ge=0)
#     variability: float = Field(..., ge=0)
#     internet_share: float = Field(..., ge=0, le=1)
class RiskPredictionRequest(BaseModel):
    grid_id: str = Field(..., min_length=1)
    feature_timestamp: str

# class RiskPredictionResponse(BaseModel):
#     grid_id: str
#     feature_timestamp: str
#     risk_score: float
#     risk_level: str
#     model_version: str
#     explanation_note: str
class RiskPredictionResponse(BaseModel):
    grid_id: str
    feature_timestamp: str

    # ML3
    risk_score: float
    risk_level: str

    # ML4
    anomaly_score: Optional[float] = None
    anomaly_direction: Optional[str] = None
    anomaly_reason: Optional[str] = None

    # NP3
    np3_alert: Optional[int] = None
    np3_alert_count: Optional[int] = None
    np3_alert_types: Optional[str] = None

    # Comparison
    comparison: Optional[str] = None
    comparison_reason: Optional[str] = None

    model_version: str
    explanation_note: str

class PipelineTaskStatus(BaseModel):
    status: str
    timestamp: datetime
    rows_in: Optional[int] = None
    rows_rejected: Optional[int] = None
    nulls_handled: Optional[int] = None
    rows_published: Optional[int] = None
    warehouse_rows: Optional[int] = None


class PipelineStatusResponse(BaseModel):
    last_run_id: str
    last_run_timestamp: datetime

    AS_OF: datetime
    current_as_of: datetime

    freshness_seconds: float
    freshness_indicator: str

    rows_in: int
    rows_rejected: int
    nulls_handled: int
    rows_published: int
    warehouse_rows: int

    healthy: bool
    reasons: list[str]

    tasks: dict[str, PipelineTaskStatus]


# ---------------------------------------------------------
# GRID LOCATION
# ---------------------------------------------------------

class GridLocationResponse(BaseModel):
    grid_id: str
    latitude: float
    longitude: float
    geometry_reference: str


# ---------------------------------------------------------
# GRID NEIGHBOURS
# ---------------------------------------------------------

class GridNeighbour(BaseModel):
    grid_id: str
    latitude: float
    longitude: float
    distance: float


class GridNeighboursResponse(BaseModel):
    grid_id: str
    count: int
    neighbours: list[GridNeighbour]