from data_joinery import Pipeline

from .context import PipelineContext
from .transform import read_views, update_leaderboards, write_leaderboards


def build_pipeline() -> Pipeline[PipelineContext]:
    pipeline = Pipeline(PipelineContext)
    views = pipeline.add_step(read_views, name="read_views")
    rankings = pipeline.add_step(update_leaderboards, name="update_leaderboards")
    output = pipeline.add_step(write_leaderboards, name="write_leaderboards")
    views >> rankings >> output
    return pipeline
