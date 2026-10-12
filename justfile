set shell := ["zsh", "-cu"]

default:
    just --list

install:
    uv sync --all-extras --all-groups --all-packages

skills:
    uvx library-skills

test:
    uv run --all-packages pytest

lint:
    uv run --all-packages ruff check . --fix

types:
    uv run --all-packages pyrefly check

format:
    uv run --all-packages ruff format .

check: lint types format test

generate-datasets:
    find datasets/tpch -maxdepth 1 -type f -name '*.parquet' -delete
    uv run tpchgen-cli parquet --compression='ZSTD(1)' -s 30 --output-dir datasets/tpch

run-example example *args:
    uv run --package {{ replace(trim_end_match(trim_end_match(example, "/"), "_example"), "_", "-") }} data-joinery run {{ trim_end_match(trim_end_match(example, "/"), "_example") }}.main {{ args }}
