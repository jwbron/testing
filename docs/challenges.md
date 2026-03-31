# Coding Challenges

## Merge Intervals

**Module**: `src/challenges/merge_intervals.py`
**Tests**: `tests/test_merge_intervals.py`, `tests/test_merge_intervals_extended.py`

### Problem Statement

Given a collection of intervals where each interval is a pair `[start, end]`,
merge all overlapping intervals and return a list of non-overlapping intervals
that cover all the intervals in the input. The output must be sorted by start
time.

**Example**:

| Input | Output |
|-------|--------|
| `[[1, 3], [2, 6], [8, 10], [15, 18]]` | `[[1, 6], [8, 10], [15, 18]]` |
| `[[1, 4], [4, 5]]` | `[[1, 5]]` |

### API

```python
def merge_intervals(intervals: list[list[int]]) -> list[list[int]]:
    """Merge all overlapping intervals.

    Args:
        intervals: A list of [start, end] pairs representing intervals.

    Returns:
        A list of merged, non-overlapping intervals sorted by start time.
    """
```

### Algorithm

1. **Sort** the intervals by their start value.
2. **Initialize** a result list with the first interval.
3. **Iterate** through the remaining intervals:
   - If the current interval overlaps the last merged interval
     (`current_start <= last_end`), merge by updating the end to
     `max(last_end, current_end)`.
   - Otherwise, append the current interval as a new entry.
4. **Return** the merged list.

### Complexity

| Metric | Value | Reason |
|--------|-------|--------|
| Time | O(n log n) | Dominated by the sort step |
| Space | O(n) | Output list in the worst case (no overlaps) |

### Edge Cases

The test suite covers a comprehensive set of scenarios:

**Core behaviour**

| Case | Input | Expected Output |
|------|-------|-----------------|
| Empty input | `[]` | `[]` |
| Single interval | `[[1, 5]]` | `[[1, 5]]` |
| No overlaps | `[[1, 2], [4, 5], [7, 8]]` | `[[1, 2], [4, 5], [7, 8]]` |
| All overlapping | `[[1, 10], [2, 6], [3, 5], [7, 9]]` | `[[1, 10]]` |
| Partial overlaps | `[[1, 3], [2, 5], [4, 7]]` | `[[1, 7]]` |
| Touching endpoints | `[[1, 2], [2, 3]]` | `[[1, 3]]` |
| Adjacent (no overlap) | `[[1, 2], [3, 4]]` | `[[1, 2], [3, 4]]` |
| Unsorted input | `[[8, 10], [1, 3], [15, 18], [2, 6]]` | `[[1, 6], [8, 10], [15, 18]]` |
| Duplicates | `[[1, 3], [1, 3], [1, 3]]` | `[[1, 3]]` |
| Single-point interval | `[[5, 5]]` | `[[5, 5]]` |
| Nested intervals | `[[1, 10], [3, 5], [6, 8]]` | `[[1, 10]]` |

**Extended scenarios** (from `test_merge_intervals_extended.py`)

| Case | Input | Expected Output |
|------|-------|-----------------|
| Negative numbers | `[[-5, -3], [-4, -1], [-10, -8]]` | `[[-10, -8], [-5, -1]]` |
| Crossing zero | `[[-2, 0], [0, 3]]` | `[[-2, 3]]` |
| Same start, different end | `[[1, 3], [1, 6], [1, 2]]` | `[[1, 6]]` |
| Same end, different start | `[[1, 5], [3, 5], [2, 5]]` | `[[1, 5]]` |
| Reverse-sorted input | `[[15, 18], [8, 10], [2, 6], [1, 3]]` | `[[1, 6], [8, 10], [15, 18]]` |
| Chain merge | `[[1, 3], [2, 5], [4, 7], [6, 9], [8, 11]]` | `[[1, 11]]` |
| Large values | `[[0, 1_000_000], [500_000, 2_000_000]]` | `[[0, 2_000_000]]` |
| Stress (100 overlapping) | `[[i, i+10] for i in range(100)]` | `[[0, 109]]` |

### Known Limitations

The current implementation mutates the inner sub-lists of the input because
`sorted()` produces a shallow copy. The extended test suite documents this via
`xfail` markers. If immutability is required, callers should pass a deep copy.

### Usage Examples

```python
from challenges.merge_intervals import merge_intervals

# Basic usage
result = merge_intervals([[1, 3], [2, 6], [8, 10], [15, 18]])
assert result == [[1, 6], [8, 10], [15, 18]]

# Touching intervals merge
result = merge_intervals([[1, 4], [4, 5]])
assert result == [[1, 5]]

# Adjacent but non-overlapping stay separate
result = merge_intervals([[1, 2], [3, 4]])
assert result == [[1, 2], [3, 4]]

# Handles unsorted input
result = merge_intervals([[15, 18], [1, 3], [2, 6], [8, 10]])
assert result == [[1, 6], [8, 10], [15, 18]]

# Works with negative numbers
result = merge_intervals([[-5, -1], [-3, 2], [4, 8]])
assert result == [[-5, 2], [4, 8]]
```

---

## In-Memory Database

**Module**: `src/challenges/inmemory_db.py`
**Tests**: `tests/test_inmemory_db.py`, `tests/test_inmemory_db_gaps.py`

### Problem Statement

Build a simple in-memory relational database that supports creating and dropping
tables with typed columns, inserting rows with primary key constraint
enforcement, and selecting/updating/deleting rows with optional where-clause
filtering. The entire implementation lives in a single Python module with no
external dependencies.

### API

The main entry point is the `InMemoryDB` class:

```python
from challenges.inmemory_db import InMemoryDB

db = InMemoryDB()
```

#### `create_table(name, columns, primary_key)`

Create a new table with a typed schema and a designated primary key column.

```python
db.create_table(
    "users",
    {"id": int, "name": str, "age": int},
    primary_key="id",
)
```

- **`name`** (`str`): Table name.
- **`columns`** (`dict[str, type]`): Mapping of column names to Python types
  (e.g. `int`, `str`, `float`, `bool`).
- **`primary_key`** (`str`): Must be one of the declared columns.
- **Raises**: `TableExistsError` if the table already exists; `ColumnError` if
  the primary key is not in `columns`.

#### `drop_table(name)`

Remove a table and all its data.

```python
db.drop_table("users")
```

- **Raises**: `TableNotFoundError` if the table does not exist.

#### `insert(table_name, row)`

Insert a single row. The row must supply every declared column with the correct
type.

```python
db.insert("users", {"id": 1, "name": "Alice", "age": 30})
```

- **Raises**: `TableNotFoundError`, `ColumnError` (missing/extra columns),
  `TypeValidationError` (wrong type), `PrimaryKeyViolationError` (duplicate PK).
- Stores a **copy** of the row dict — mutating the original after insert has no
  effect on stored data.

#### `select(table_name, where=None)`

Retrieve rows, optionally filtered.

```python
# All rows
db.select("users")

# Equality filter (dict)
db.select("users", where={"name": "Alice"})

# Predicate filter (callable)
db.select("users", where=lambda row: row["age"] > 25)
```

- **`where`**: `None` (all rows), a `dict` of `{column: value}` for equality
  matching, or a `callable` predicate `(row) -> bool`.
- **Returns**: A `list[dict]` of matching rows (copies — safe to mutate).
- **Raises**: `TableNotFoundError`.

#### `update(table_name, values, where=None)`

Update matched rows with new column values.

```python
count = db.update("users", {"age": 31}, where={"id": 1})
```

- **Returns**: The number of rows updated (`int`).
- **Raises**: `TableNotFoundError`, `ColumnError`, `TypeValidationError`,
  `PrimaryKeyViolationError` (if updating the PK to a value that already exists).

#### `delete(table_name, where=None)`

Delete matched rows.

```python
count = db.delete("users", where={"id": 1})
```

- **Returns**: The number of rows deleted (`int`).
- Deleted rows free their primary key values for reuse.
- **Raises**: `TableNotFoundError`.

### Exception Hierarchy

All exceptions inherit from `DatabaseError`:

| Exception | When raised |
|-----------|-------------|
| `DatabaseError` | Base class for all database errors |
| `TableExistsError` | Creating a table that already exists |
| `TableNotFoundError` | Referencing a table that does not exist |
| `PrimaryKeyViolationError` | Duplicate primary key on insert or update |
| `TypeValidationError` | Column value does not match declared type |
| `ColumnError` | Missing, unknown, or invalid column reference |

### Design

- **Storage**: Each table is stored as a `list[dict]` of rows, wrapped in an
  internal `_Table` class that also holds the schema and a primary key index
  (`set`) for O(1) uniqueness checks.
- **Filtering**: Where clauses are normalised into a callable predicate via
  `_build_filter()`, supporting `None`, `dict`, and arbitrary callables.
- **Copy semantics**: Both `insert()` and `select()` copy row dicts to prevent
  external mutations from corrupting the database state.

### Complexity

| Operation | Time | Notes |
|-----------|------|-------|
| `create_table` / `drop_table` | O(1) | Dict lookup |
| `insert` | O(c) | c = number of columns (validation) |
| `select` | O(n) | Full scan; n = row count |
| `update` | O(n) | Full scan + in-place mutation |
| `delete` | O(n) | Full scan + list rebuild |
| PK uniqueness check | O(1) | Hash set lookup |

### Edge Cases

The test suite covers a comprehensive set of scenarios:

**Core behaviour**

| Case | Tested in |
|------|-----------|
| Create and select from empty table | `test_inmemory_db.py` |
| Duplicate table creation raises | `test_inmemory_db.py` |
| Bad primary key column raises | `test_inmemory_db.py` |
| Drop and recreate table with new schema | `test_inmemory_db.py` |
| Insert with all column types validated | `test_inmemory_db.py` |
| Duplicate PK on insert raises | `test_inmemory_db.py` |
| Missing / extra columns on insert raise | `test_inmemory_db.py` |
| Select with dict filter, callable filter, multi-column filter | `test_inmemory_db.py` |
| Update single row, multiple rows, all rows | `test_inmemory_db.py` |
| Update PK to same value (no-op) succeeds | `test_inmemory_db.py` |
| Update PK to duplicate raises | `test_inmemory_db.py` |
| Delete frees PK for reuse | `test_inmemory_db.py` |
| Independent multi-table operations | `test_inmemory_db.py` |
| Insert/select copy semantics | `test_inmemory_db.py` |

**Gap-filling scenarios** (from `test_inmemory_db_gaps.py`)

| Case | Tested in |
|------|-----------|
| Additional edge cases for insert, select, update, delete | `test_inmemory_db_gaps.py` |

### Known Limitations

- No indexing beyond the primary key — all `select`, `update`, and `delete`
  operations perform a full table scan.
- No support for `NULL` / optional columns — every column must be present in
  every row.
- No transactions or rollback for cross-operation sequences. However,
  `update()` is atomic within a single call — it validates all PK constraints
  before mutating any rows, so a PK violation will not leave partial changes.
- No foreign key or referential integrity constraints.
- Single-threaded — no concurrency protection.

### Usage Examples

```python
from challenges.inmemory_db import InMemoryDB

db = InMemoryDB()

# Create a table with typed columns
db.create_table(
    "products",
    {"sku": str, "name": str, "price": float, "in_stock": bool},
    primary_key="sku",
)

# Insert rows
db.insert("products", {"sku": "A001", "name": "Widget", "price": 9.99, "in_stock": True})
db.insert("products", {"sku": "A002", "name": "Gadget", "price": 24.99, "in_stock": False})

# Select all
all_products = db.select("products")
assert len(all_products) == 2

# Filter with a dict (equality match)
widgets = db.select("products", where={"name": "Widget"})
assert widgets[0]["sku"] == "A001"

# Filter with a callable
affordable = db.select("products", where=lambda r: r["price"] < 20.0)
assert len(affordable) == 1

# Update matching rows
updated = db.update("products", {"in_stock": True}, where={"sku": "A002"})
assert updated == 1

# Delete matching rows
deleted = db.delete("products", where={"in_stock": True})
assert deleted == 2

# Table is now empty
assert db.select("products") == []

# Drop and recreate with a different schema
db.drop_table("products")
db.create_table("products", {"id": int, "name": str}, primary_key="id")
```
