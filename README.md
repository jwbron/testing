# Testing

A Python project for solving coding challenges with proper tooling and CI.

## Overview

This repository contains solutions to classic coding challenges, starting with the **Merge Intervals** problem. The project uses modern Python tooling:

- **[uv](https://docs.astral.sh/uv/)** for fast, reliable package management
- **[ruff](https://docs.astral.sh/ruff/)** for linting and formatting
- **[pytest](https://docs.pytest.org/)** for testing
- **GitHub Actions** for continuous integration

## Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) package manager

## Getting Started

```bash
# Install dependencies
uv sync --dev

# Run tests
uv run pytest

# Run linter
uv run ruff check .

# Check formatting
uv run ruff format --check .

# Auto-fix lint issues
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
│       └── merge_intervals.py   # Merge Intervals solution
├── tests/
│   ├── __init__.py
│   └── test_merge_intervals.py  # Comprehensive test suite (30+ cases)
├── docs/
│   └── challenges.md            # Challenge documentation
├── .github/
│   └── workflows/
│       └── ci.yml               # CI pipeline (lint + test)
├── pyproject.toml               # Project config (uv, ruff, pytest)
├── uv.lock                      # Locked dependencies
├── .python-version              # Python 3.12
└── .gitignore
```

## Challenges

### Merge Intervals

Given a list of intervals `[start, end]`, merge all overlapping intervals and return the result sorted by start time.

```python
from challenges.merge_intervals import merge_intervals

merge_intervals([[1, 3], [2, 6], [8, 10], [15, 18]])
# => [[1, 6], [8, 10], [15, 18]]

merge_intervals([[1, 4], [4, 5]])
# => [[1, 5]]
```

**Algorithm**: Sort intervals by start time, then iterate through them. For each interval, either merge it with the previous one (if overlapping) or add it as a new entry.

**Time complexity**: O(n log n) due to sorting.
**Space complexity**: O(n) for the output list.

See [docs/challenges.md](docs/challenges.md) for detailed documentation.

## CI

The GitHub Actions workflow runs on every push and pull request:

1. **Lint**: `ruff check .` and `ruff format --check .`
2. **Test**: `pytest` with the full test suite

## Contributing

1. Create a feature branch
2. Add your solution under `src/challenges/`
3. Add tests under `tests/`
4. Ensure `uv run ruff check .` and `uv run pytest` pass
5. Submit a pull request
