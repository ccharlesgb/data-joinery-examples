"""Regenerate the diagrams embedded in each example README.

Run from the repository root with ``uv run python scripts/update_pipeline_visualisations.py``.
No Spark session or input data is needed: only pipeline graphs are constructed.
"""

import importlib
import sys
from pathlib import Path

from matplotlib import pyplot as plt

plt.switch_backend("Agg")


ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    examples = sorted(ROOT.glob("*_example/src/*/pipeline.py"))
    if not examples:
        raise RuntimeError("No example pipelines found")

    for pipeline_file in examples:
        package = pipeline_file.parent.name
        example_dir = pipeline_file.parents[2]
        sys.path.insert(0, str(example_dir / "src"))
        try:
            module = importlib.import_module(f"{package}.pipeline")
            figure = module.build_pipeline().visualize(show=False)
            destination = example_dir / "pipeline.png"
            figure.savefig(destination, dpi=160, bbox_inches="tight")
            plt.close(figure)
            print(destination.relative_to(ROOT))
        finally:
            sys.path.pop(0)


if __name__ == "__main__":
    main()
