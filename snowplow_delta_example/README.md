# Snowplow Delta streaming example

This example reads the Delta table written by the Snowplow Lake Loader as a
Spark Structured Streaming source. It counts events in one-minute event-time
windows using `collector_tstamp`, groups the counts by `event_name`, and prints
the updated counts from every microbatch.

The input contract intentionally projects only the Snowplow atomic fields used
by the aggregation. Snowplow documents `collector_tstamp` as the required
Collector timestamp and `event_name` as the name populated for self-describing
events. Consequently, `event_name` is nullable so non-self-describing events
are retained in a null group rather than silently dropped.

Run the example from the repository root:

```sh
uv run --package snowplow-delta python -m snowplow_delta \
  s3a://my-bucket/events \
  s3a://my-bucket/events/_checkpoints/event-counts
```

Use the cloud storage connector and credentials appropriate for the Delta
location. The first local run can also download the Delta Lake JVM artifacts.

References:

- [Snowplow atomic event properties](https://docs.snowplow.io/docs/fundamentals/canonical-event/)
- [Snowplow Lake Loader configuration](https://docs.snowplow.io/docs/api-reference/loaders-storage-targets/lake-loader/configuration-reference/)
- [Delta Lake streaming reads](https://docs.delta.io/delta-streaming/)
