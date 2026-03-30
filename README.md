# Coding Challenges

A Python project containing solutions to classic coding challenges, starting with
the **Merge Intervals** problem. Built with modern Python tooling: `uv` for package
management, `ruff` for linting/formatting, and `pytest` for testing.

## Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) package manager

## Quick Start

```bash
# Install dependencies
uv sync --dev

# Run tests
uv run pytest

# Run tests with verbose output
uv run pytest -v

# Run linter
uv run ruff check .

# Check formatting
uv run ruff format --check .

# Auto-fix lint and formatting issues
uv run ruff check --fix .
uv run ruff format .
```

## Project Structure

```
.
├── src/
│   └── challenges/
│       ├── __init__.py
│       └── merge_intervals.py   # Merge Intervals solution
├── tests/
│   ├── __init__.py
│   └── test_merge_intervals.py  # Comprehensive test suite
├── .github/
│   └── workflows/
│       └── ci.yml               # GitHub Actions CI pipeline
├── pyproject.toml               # Project config, ruff & pytest settings
└── README.md
```

## Challenges

### Merge Intervals

**Problem:** Given a list of intervals `[start, end]`, merge all overlapping
intervals and return the non-overlapping result sorted by start time.

**Example:**

```python
from challenges.merge_intervals import merge_intervals

merge_intervals([[1, 3], [2, 6], [8, 10], [15, 18]])
# => [[1, 6], [8, 10], [15, 18]]
```

**Algorithm:** Sort intervals by start time, then iterate through. For each
interval, either merge it with the previous (if overlapping) or append it as a
new interval.

| Metric | Complexity |
|--------|-----------|
| Time   | O(n log n) — dominated by sorting |
| Space  | O(n) — for sorted copy and result |

**Edge cases handled:**
- Empty input
- Single interval
- No overlapping intervals
- Fully overlapping intervals
- Adjacent intervals (e.g., `[1, 2]` and `[3, 4]` — not merged)
- Unsorted input
- Duplicate intervals
- Single-point intervals (e.g., `[5, 5]`)

## CI/CD

The GitHub Actions workflow runs on every push and pull request:

1. **Lint** — `ruff check .` and `ruff format --check .`
2. **Test** — `uv run pytest`

## Development

### Adding a New Challenge

1. Create `src/challenges/<challenge_name>.py` with the solution function
2. Create `tests/test_<challenge_name>.py` with comprehensive tests
3. Update this README with problem description and usage examples
4. Verify: `uv run ruff check . && uv run pytest`

### Code Standards

- Python 3.12+ with full type annotations
- PEP 8 style enforced by ruff (line length: 88)
- All public functions require docstrings
- Tests cover edge cases and typical inputs
