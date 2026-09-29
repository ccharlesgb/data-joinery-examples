from dataclasses import dataclass
from datetime import date, datetime


@dataclass
class AdvertEvents:
    """One advert interaction; UUID values are stored as canonical strings."""

    event_timestamp: datetime
    event_id: str
    advert_id: str
    postcode: str
    vehicle_make: str
    vehicle_model: str
    event_type: str


@dataclass
class AdvertFeatures:
    snapshot_date: date
    advert_id: str
    postcode: str
    vehicle_make: str
    vehicle_model: str
    number_of_search_views: int
    number_of_advert_views: int


@dataclass
class AdvertPredictions(AdvertFeatures):
    predicted_number_of_search_views: float
    predicted_number_of_advert_views: float


@dataclass
class PredictionMetrics:
    mean_absolute_error: float
    mean_squared_error: float
    r2_score: float
