from dataclasses import dataclass
from datetime import datetime


@dataclass
class VideoView:
    event_time: datetime
    category: str
    video_id: str


@dataclass
class RankedVideo:
    rank: int
    video_id: str
    view_count: int


@dataclass
class Leaderboard:
    window_start: datetime
    window_end: datetime
    category: str
    top_videos: list[RankedVideo]
    is_final: bool
