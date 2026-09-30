from dataclasses import dataclass
from datetime import datetime


@dataclass
class SnowplowEvent:
    """Snowplow atomic fields consumed from the wide enriched event table."""

    collector_tstamp: datetime
    event_name: str | None


@dataclass
class EventWindow:
    start: datetime
    end: datetime


@dataclass
class WindowedEventCount:
    window: EventWindow
    event_name: str | None
    event_count: int
