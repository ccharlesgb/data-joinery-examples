from dataclasses import dataclass

from data_joinery.backends.spark import SparkContext


class HousingPath(str):
    """Marker class for the housing input path context."""


@dataclass(frozen=True)
class PipelineContext(SparkContext):
    housing_path: HousingPath
