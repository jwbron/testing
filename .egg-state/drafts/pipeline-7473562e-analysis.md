### Task Analysis

**Problem statement**: Build a simple in-memory database as a coding exercise — a self-contained Python module that supports basic CRUD operations, table creation, and querying.

**System context**: The repo is a coding challenges collection. Each challenge lives as a module under `src/challenges/` with corresponding tests in `tests/`. Python 3.12+, pytest for testing, ruff for linting.

**Files affected**:
- `src/challenges/inmemory_db.py` — new module implementing the in-memory database
- `tests/test_inmemory_db.py` — tests covering CRUD, querying, and edge cases

**Scope**: A single-module implementation supporting:
- Create/drop tables with typed columns
- Insert, select, update, delete rows
- Filtering (where clauses) on select/update/delete
- Primary key constraint enforcement

**Risks / edge cases**: None significant — this is a new standalone module with no dependencies on existing code.