# Documentation Index

Welcome to the **testing** project documentation. This project contains Python
solutions to classic coding challenges, built with modern tooling.

## Guides

| Document | Description |
|----------|-------------|
| [challenges.md](challenges.md) | Detailed write-ups for each coding challenge |
| [../README.md](../README.md) | Project overview, quick start, and contributing guide |

## Tooling Reference

| Tool | Config location | Purpose |
|------|-----------------|---------|
| [uv](https://docs.astral.sh/uv/) | `pyproject.toml` | Package management and virtual environments |
| [ruff](https://docs.astral.sh/ruff/) | `pyproject.toml` `[tool.ruff]` | Linting (E, F, I, W rules) and formatting (line-length 88) |
| [pytest](https://docs.pytest.org/) | `pyproject.toml` `[tool.pytest.ini_options]` | Test runner; test paths in `tests/`, source in `src/` |
| GitHub Actions | `.github/workflows/ci.yml` | CI: lint + test on push/PR to `main` |

## Challenge Index

| Challenge | Module | Tests | Docs |
|-----------|--------|-------|------|
| Merge Intervals | `src/challenges/merge_intervals.py` | `tests/test_merge_intervals.py`, `tests/test_merge_intervals_extended.py` | [challenges.md#merge-intervals](challenges.md#merge-intervals) |
| In-Memory NoSQL Database | `src/challenges/nosql_db.py` | `tests/test_nosql_db.py` | [challenges.md#in-memory-nosql-database](challenges.md#in-memory-nosql-database) |
| LRU Cache | `src/challenges/lru_cache.py` | `tests/test_lru_cache.py` | [challenges.md#lru-cache](challenges.md#lru-cache) |
| MicroGPT | `src/challenges/microgpt.py` | `tests/test_microgpt.py` | [challenges.md#microgpt](challenges.md#microgpt) |
| In-Memory Key-Value Store | `src/challenges/kv_store.py` | `tests/test_kv_store.py` | [challenges.md#in-memory-key-value-store](challenges.md#in-memory-key-value-store) |
