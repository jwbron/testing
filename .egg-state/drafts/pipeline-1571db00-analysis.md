### Task Analysis

**Problem statement**: Set up a Python repo with proper standards (uv, venv, ruff, pytest, GitHub Actions CI) and solve the Merge Intervals coding challenge as the first piece of code.

**Coding challenge**: Given a list of intervals [start, end], merge all overlapping intervals. E.g., [[1,3],[2,6],[8,10],[15,18]] → [[1,6],[8,10],[15,18]].

**System context**: Repo has an initial commit with .gitignore, pyproject.toml, and uv.lock. uv is installed globally in the container.

**Files to create/modify**:
- `pyproject.toml` — update with ruff and pytest settings, add dev dependencies
- `.python-version` — pin Python version (3.12)
- `src/challenges/__init__.py` — package init
- `src/challenges/merge_intervals.py` — the solution with type annotations
- `tests/__init__.py` — test package init
- `tests/test_merge_intervals.py` — comprehensive tests
- `.github/workflows/ci.yml` — GitHub Actions CI

**Approach**:
- Use `uv` for venv creation and dependency management (uv is installed globally in the container)
- `ruff` for linting and formatting
- `pytest` for testing with src layout
- Single CI workflow with lint and test jobs

**Edge cases for merge_intervals**: empty list, single interval, fully overlapping intervals, partial overlaps, adjacent but non-overlapping [1,2],[3,4], unsorted input, duplicate intervals, single-point intervals [5,5]