import pytest
from video_topk.context import TopKSettings


@pytest.mark.parametrize(
    "kwargs", [{"k": 0}, {"window_minutes": 0}, {"lateness_minutes": -1}]
)
def test_invalid_settings(kwargs):
    with pytest.raises(ValueError, match="must be"):
        TopKSettings(**kwargs)
