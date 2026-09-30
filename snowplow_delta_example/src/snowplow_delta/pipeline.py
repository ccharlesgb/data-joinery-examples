from data_joinery import Pipeline

from .context import SnowplowDeltaContext
from .transform import (
    count_events_by_name,
    read_snowplow_delta_stream,
    start_printing_event_counts,
)


def build_pipeline() -> Pipeline[SnowplowDeltaContext]:
    pipeline = Pipeline(SnowplowDeltaContext)
    events = pipeline.add_step(read_snowplow_delta_stream)
    counts = pipeline.add_step(count_events_by_name)
    printing_query = pipeline.add_step(start_printing_event_counts)

    events >> counts >> printing_query
    return pipeline
