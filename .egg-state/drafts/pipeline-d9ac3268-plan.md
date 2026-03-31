# Plan: Simple In-Memory Database

## Summary

Implement a simple in-memory database as a Python module following the repo's existing coding challenge pattern. The database will store tables as dictionaries of rows, support typed columns with primary key constraints, and provide CRUD operations with basic filtering. This is a standalone exercise with no dependencies on existing code.

**Risks / edge cases**: None identified — standalone new module.

## Implementation

### Phase 1: Implement

Build the in-memory database module and comprehensive tests.

**Tasks**:
1. **[task-1-1]** Create `src/challenges/inmemory_db.py` with an `InMemoryDB` class supporting `create_table(name, columns, primary_key)`, `drop_table(name)`, `insert(table, row)`, `select(table, where)`, `update(table, values, where)`, `delete(table, where)`. Tables stored as `dict[str, list[dict]]`. Columns defined as `dict[str, type]`. Primary key enforces uniqueness on insert. Acceptance: All CRUD operations work correctly.
2. **[task-1-2]** Create `tests/test_inmemory_db.py` with pytest tests covering: table creation/dropping, insert with primary key enforcement, select with and without filters, update with filters, delete with filters, error cases (duplicate table, missing table, type mismatches, duplicate primary key). Acceptance: All tests pass via `pytest`.

```yaml
# yaml-tasks
pr:
  title: "Add simple in-memory database coding exercise"
  description: |
    Implements a simple in-memory database module as a coding exercise.
    Supports table creation with typed columns, primary key constraints,
    and full CRUD operations with filtering.
  test_plan: |
    - Automated: pytest tests covering all CRUD operations, constraints, and error cases
    - Manual: Run `pytest tests/test_inmemory_db.py -v` to verify all tests pass
  manual_steps: |
    Pre-merge: None
    Post-merge: None
phases:
  - id: 1
    name: Implement
    goal: "Build the in-memory database module and tests"
    tasks:
      - id: task-1-1
        description: "Create `src/challenges/inmemory_db.py` with an `InMemoryDB` class supporting `create_table(name, columns, primary_key)`, `drop_table(name)`, `insert(table, row)`, `select(table, where)`, `update(table, values, where)`, `delete(table, where)`. Tables stored as `dict[str, list[dict]]`. Columns defined as `dict[str, type]`. Primary key enforces uniqueness on insert. Type validation on insert rejects wrong column types."
        acceptance: "All CRUD operations work correctly — insert stores rows, select retrieves with optional filtering, update modifies matched rows, delete removes matched rows"
        files:
          - src/challenges/inmemory_db.py
      - id: task-1-2
        description: "Create `tests/test_inmemory_db.py` with pytest tests covering: table creation/dropping, insert with primary key enforcement, select with and without filters, update with filters, delete with filters, error cases (duplicate table, missing table, type mismatches, duplicate primary key)."
        acceptance: "All tests pass via pytest, covering CRUD operations, primary key constraints, type validation, and error cases"
        files:
          - tests/test_inmemory_db.py
```