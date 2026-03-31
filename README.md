# Coding Challenges

A Python project containing solutions to classic coding challenges, starting with
the **Merge Intervals** problem. Built with modern Python tooling:

- [`uv`](https://docs.astral.sh/uv/) for fast, reproducible package management
- [`ruff`](https://docs.astral.sh/ruff/) for linting and formatting
- [`pytest`](https://docs.pytest.org/) for testing
- [GitHub Actions](.github/workflows/ci.yml) for continuous integration

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
│       └── merge_intervals.py          # Merge Intervals solution
├── tests/
│   ├── __init__.py
│   ├── test_merge_intervals.py         # Core test suite (30 tests)
│   └── test_merge_intervals_gaps.py    # Gap/edge-case tests (31 tests)
├── .github/
│   └── workflows/
│       └── ci.yml                      # GitHub Actions CI pipeline
├── pyproject.toml                      # Project config, ruff & pytest settings
├── .python-version                     # Pinned Python version (3.12)
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

merge_intervals([[1, 4], [4, 5]])
# => [[1, 5]]

merge_intervals([])
# => []
```

**Algorithm:** Sort intervals by start time, then iterate and merge adjacent
intervals where `current_start <= previous_end`. Two intervals overlap when one
starts before or at the point where the other ends.

| Metric | Complexity |
|--------|-----------|
| Time   | O(n log n) -- dominated by sorting |
| Space  | O(n) -- for sorted copy and result list |

**Edge cases handled:**

- Empty input
- Single interval
- No overlapping intervals
- Fully overlapping / nested intervals
- Adjacent intervals sharing an endpoint (e.g., `[1, 3]` and `[3, 5]` -- merged)
- Non-adjacent intervals with a gap (e.g., `[1, 2]` and `[3, 4]` -- not merged)
- Unsorted input
- Duplicate intervals
- Single-point intervals (e.g., `[5, 5]`)
- Negative numbers and zero
- Large value ranges

> **Note:** The function creates a sorted copy of the input list, so the outer
> list is not mutated. However, inner sub-lists may be modified in place during
> merging (e.g., `[1, 3]` could become `[1, 6]` if merged). If you need the
> original intervals unchanged, pass deep copies.

## CI/CD

The GitHub Actions workflow (`.github/workflows/ci.yml`) runs on every push and
pull request to `main`:

1. **Lint** -- `ruff check .` and `ruff format --check .`
2. **Test** -- `uv run pytest -v`

Both jobs use [`astral-sh/setup-uv`](https://github.com/astral-sh/setup-uv) to
install `uv` and run on `ubuntu-latest`.

## Development

### Adding a New Challenge

1. Create `src/challenges/<challenge_name>.py` with the solution function
2. Add the import to `src/challenges/__init__.py`
3. Create `tests/test_<challenge_name>.py` with comprehensive tests
4. Update this README with the problem description and usage examples
5. Verify everything passes: `uv run ruff check . && uv run ruff format --check . && uv run pytest`

### Code Standards

- Python 3.12+ with full type annotations
- PEP 8 style enforced by ruff (line length: 88)
- All public functions require docstrings with Args, Returns, and Examples
- Tests organized into classes by category (basic, overlaps, edge cases, etc.)
- Tests cover both typical inputs and boundary conditions
