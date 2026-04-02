# Testing

A Python project for solving coding challenges with modern tooling and CI.

## Overview

This repository contains solutions to coding challenges, including
**Merge Intervals** and an **In-Memory NoSQL Database**. The project uses modern
Python tooling for a consistent development experience:

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
│       └── nosql_db.py              # In-Memory NoSQL Database
├── tests/
│   ├── __init__.py
│   ├── test_merge_intervals.py      # Core test suite
│   ├── test_merge_intervals_extended.py  # Extended edge-case tests
│   └── test_nosql_db.py             # NoSQL database test suite
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

### In-Memory NoSQL Database

A document-oriented in-memory NoSQL database with a MongoDB-style API. Supports
CRUD operations, rich query operators, secondary indexes, aggregation pipelines,
and transactions with snapshot isolation.

```python
from challenges.nosql_db import Database

db = Database()
users = db.collection("users")

users.insert_many([
    {"name": "Alice", "age": 30, "dept": "eng"},
    {"name": "Bob", "age": 25, "dept": "eng"},
    {"name": "Charlie", "age": 35, "dept": "sales"},
])

# Query with operators
young_engineers = users.find({"$and": [
    {"dept": "eng"},
    {"age": {"$lt": 30}},
]})
# => [{"_id": "...", "name": "Bob", "age": 25, "dept": "eng"}]

# Aggregation pipeline
users.aggregate([
    {"$group": {"_id": "$dept", "avg_age": {"$avg": "$age"}}},
    {"$sort": {"avg_age": -1}},
])
# => [{"_id": "sales", "avg_age": 35}, {"_id": "eng", "avg_age": 27.5}]
```

**Features**: Query engine (comparison, logical, set, existence operators),
dot-notation for nested fields, secondary indexes, aggregation ($match, $group,
$sort, $limit, $skip, $project, $unwind, $count), and transactions with snapshot
isolation and conflict detection.

See [docs/challenges.md](docs/challenges.md) for the full API reference and
usage examples.

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
