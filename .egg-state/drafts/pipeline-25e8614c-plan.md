# Plan: Python Repo Setup + Merge Intervals

## Summary

Configure the Python project with ruff (linting/formatting), pytest (testing), and GitHub Actions CI. Implement the Merge Intervals algorithm as the first coding challenge. The repo will use a src layout and serve as a foundation for future challenges.

**Risks / edge cases**: The existing `uv.lock` is empty/invalid — delete it before running `uv add`. For the algorithm: handle empty input, single interval, adjacent non-overlapping intervals, unsorted input, duplicates, and single-point intervals.

## Implementation

### Phase 1: Implement

Set up the full project skeleton and implement the coding challenge with tests and CI.

**Tasks**:
1. **[TASK-1-1]** Configure Python project: update `pyproject.toml` with ruff and pytest settings (line-length=88, target Python 3.12, src layout for pytest), delete the empty `uv.lock`, add dev dependencies via `uv add --dev ruff pytest`, set `.python-version` to 3.12. Acceptance: `uv run ruff --version` and `uv run pytest --version` work.
2. **[TASK-1-2]** Implement `merge_intervals(intervals: list[list[int]]) -> list[list[int]]` in `src/challenges/merge_intervals.py` — sort by start, iterate and merge where current[0] <= prev[1]. Add type annotations. Create `src/challenges/__init__.py`. Acceptance: function handles all edge cases correctly.
3. **[TASK-1-3]** Write comprehensive pytest tests in `tests/test_merge_intervals.py` covering: empty input, single interval, no overlaps, full overlaps, partial overlaps, adjacent intervals [1,2],[3,4], unsorted input, duplicate intervals, single-point intervals [5,5]. Create `tests/__init__.py`. Acceptance: all tests pass with `uv run pytest`.
4. **[TASK-1-4]** Create `.github/workflows/ci.yml` — trigger on push/PR, use `astral-sh/setup-uv` action, run `uv sync --dev`, then lint job (`uv run ruff check .` + `uv run ruff format --check .`) and test job (`uv run pytest`). Acceptance: valid YAML workflow with correct structure.
5. **[TASK-1-5]** Verify all linting passes (`uv run ruff check .` and `uv run ruff format --check .`) and all tests pass (`uv run pytest`). Fix any issues found. Acceptance: zero lint violations and all tests green.

```yaml
# yaml-tasks
pr:
  title: "Set up Python project with uv, ruff, pytest, CI and solve Merge Intervals"
  description: |
    Initialize the repo as a Python project using uv for package management.
    Set up ruff for linting/formatting, pytest for testing, and GitHub Actions
    for CI. Implement the Merge Intervals coding challenge with comprehensive tests.
  test_plan: |
    - Automated: pytest test suite covers empty input, single interval, overlaps, adjacent intervals, unsorted input, duplicates, single-point intervals
    - Manual: verify CI workflow runs lint and test jobs on push
  manual_steps: |
    Pre-merge: none
    Post-merge: verify GitHub Actions CI runs successfully on main
phases:
  - id: 1
    name: Implement
    goal: "Set up Python project with tooling and implement Merge Intervals with full test coverage"
    tasks:
      - id: TASK-1-1
        description: "Configure Python project: read then update `pyproject.toml` with ruff and pytest settings (line-length=88, target py312, src layout for pytest), delete the empty `uv.lock`, add dev dependencies via `uv add --dev ruff pytest`, set `.python-version` to 3.12"
        acceptance: "`uv run ruff --version` and `uv run pytest --version` both succeed"
        files:
          - pyproject.toml
          - .python-version
      - id: TASK-1-2
        description: "Implement `merge_intervals(intervals: list[list[int]]) -> list[list[int]]` in `src/challenges/merge_intervals.py` — sort by start, iterate and merge where current[0] <= prev[1]. Add type annotations. Create `src/challenges/__init__.py`."
        acceptance: "Function correctly merges overlapping intervals for all edge cases"
        files:
          - src/challenges/__init__.py
          - src/challenges/merge_intervals.py
      - id: TASK-1-3
        description: "Write comprehensive pytest tests in `tests/test_merge_intervals.py` covering: empty input, single interval, no overlaps, full overlaps, partial overlaps, adjacent intervals [1,2],[3,4], unsorted input, duplicate intervals, single-point intervals [5,5]. Create `tests/__init__.py`."
        acceptance: "All tests pass with `uv run pytest`"
        files:
          - tests/__init__.py
          - tests/test_merge_intervals.py
      - id: TASK-1-4
        description: "Create `.github/workflows/ci.yml` — trigger on push/PR, use `astral-sh/setup-uv` action, run `uv sync --dev`, then lint job (`uv run ruff check .` + `uv run ruff format --check .`) and test job (`uv run pytest`)"
        acceptance: "Valid YAML workflow with lint and test jobs"
        files:
          - .github/workflows/ci.yml
      - id: TASK-1-5
        description: "Run `uv run ruff check .`, `uv run ruff format --check .`, and `uv run pytest` to verify everything passes. Fix any issues found."
        acceptance: "Zero lint violations and all tests green"
        files: []
```