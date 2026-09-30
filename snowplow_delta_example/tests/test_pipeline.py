from snowplow_delta.pipeline import build_pipeline


def test_pipeline_connects_stream_read_aggregation_and_sink():
    pipeline = build_pipeline()

    assert [step.name for step in pipeline.get_steps_in_execution_order()] == [
        "read_snowplow_delta_stream",
        "count_events_by_name",
        "start_printing_event_counts",
    ]
