# Testing

A Python project for solving coding challenges with modern tooling and CI.

## Overview

This repository contains solutions to classic coding challenges, including
**Merge Intervals** and a **Simple In-Memory Database**. The project uses modern Python tooling for a
consistent development experience:

- **[uv](https://docs.astral.sh/uv/)** &mdash; fast, reliable package management
- **[ruff](https://docs.astral.sh/ruff/)** &mdash; linting and formatting
- **[pytest](https://docs.pytest.org/)** &mdash; testing
- **GitHub Actions** &mdash; continuous integration

## Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) package manager

## Quick Start

```bash
# Install dependencies (including dev tools)
uv sync --dev

# Run the test suite
uv run pytest

# Run the linter
uv run ruff check .

# Check code formatting
uv run ruff format --check .
```

### Fixing Lint / Format Issues

```bash
# Auto-fix lint violations
uv run ruff check --fix .

# Auto-format code
uv run ruff format .
```

## Project Structure

```
testing/
├── src/
│   └── challenges/
│       ├── __init__.py
│       ├── merge_intervals.py       # Merge Intervals solution
│       └── inmemory_db.py           # In-Memory Database solution
├── tests/
│   ├── __init__.py
│   ├── test_merge_intervals.py      # Core Merge Intervals tests
│   ├── test_merge_intervals_extended.py  # Extended edge-case tests
│   ├── test_inmemory_db.py          # In-Memory Database tests
│   └── test_inmemory_db_gaps.py     # Gap-filling edge-case tests
├── docs/
│   ├── index.md                     # Documentation hub
│   └── challenges.md                # Challenge write-ups
├── .github/
│   └── workflows/
│       └── ci.yml                   # CI pipeline (lint + test)
├── pyproject.toml                   # Project & tool configuration
├── .python-version                  # Python 3.12
└── .gitignore
```

### Key directories

| Directory | Purpose |
|-----------|---------|
| `src/challenges/` | Challenge implementations (importable as `from challenges.<name> import ...`) |
| `tests/` | Pytest test suites; mirrors `src/challenges/` naming |
| `docs/` | Written documentation and challenge explanations |
| `.github/workflows/` | GitHub Actions CI configuration |

## Challenges

### Merge Intervals

Given a list of intervals `[start, end]`, merge all overlapping intervals and
return the result sorted by start time.

```python
from challenges.merge_intervals import merge_intervals

merge_intervals([[1, 3], [2, 6], [8, 10], [15, 18]])
# => [[1, 6], [8, 10], [15, 18]]

merge_intervals([[1, 4], [4, 5]])
# => [[1, 5]]
```

**Algorithm**: Sort by start time, then iterate &mdash; merge when the current
interval overlaps the previous one, otherwise append.

| Metric | Value | Reason |
|--------|-------|--------|
| Time | O(n log n) | Dominated by sort |
| Space | O(n) | Output list (worst case: no overlaps) |

See [docs/challenges.md](docs/challenges.md) for the full write-up including
edge cases and test coverage details.

### In-Memory Database

A simple in-memory relational database supporting typed columns, primary key
constraints, and CRUD operations with flexible where-clause filtering.

```python
from challenges.inmemory_db import InMemoryDB

db = InMemoryDB()
db.create_table("users", {"id": int, "name": str, "age": int}, primary_key="id")
db.insert("users", {"id": 1, "name": "Alice", "age": 30})

# Select with equality filter
db.select("users", where={"name": "Alice"})
# => [{'id': 1, 'name': 'Alice', 'age': 30}]

# Select with callable predicate
db.select("users", where=lambda r: r["age"] > 25)
# => [{'id': 1, 'name': 'Alice', 'age': 30}]

# Update and delete return affected row count
db.update("users", {"age": 31}, where={"id": 1})  # => 1
db.delete("users", where={"id": 1})                # => 1
```

**Features**: Typed column validation, primary key uniqueness enforcement,
dict and callable where-clause filters, copy semantics for data isolation.

See [docs/challenges.md](docs/challenges.md#in-memory-database) for the full
API reference, exception hierarchy, and test coverage details.

## CI

The GitHub Actions workflow (`.github/workflows/ci.yml`) runs on every push to
`main` and on pull requests targeting `main`:

1. **Lint** &mdash; `ruff check .` and `ruff format --check .`
2. **Test** &mdash; `pytest` against the full test suite

## Contributing

1. Create a branch from `main`
2. Add your solution under `src/challenges/`
3. Add tests under `tests/`
4. Ensure `uv run ruff check .` and `uv run pytest` pass
5. Submit a pull request
