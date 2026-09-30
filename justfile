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

run-example example:
    uv run --package {{ replace(trim_end_match(example, "_example/"), "_", "-") }} python -m {{ trim_end_match(example, "_example/") }}
