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
**Tests**: `tests/test_inmemory_db.py`

### Problem Statement

Build a simple in-memory relational database that supports creating and dropping
tables with typed columns, primary key constraint enforcement, and full CRUD
operations (insert, select, update, delete) with filtering via where clauses.

### API

```python
from challenges.inmemory_db import InMemoryDB

db = InMemoryDB()
```

#### `create_table(name, columns, primary_key=None)`

Create a new table with typed columns and an optional primary key.

```python
def create_table(
    self,
    name: str,
    columns: dict[str, type],
    primary_key: str | None = None,
) -> None:
```

- `columns`: mapping of column name to Python type (e.g. `{"id": int, "name": str}`)
- `primary_key`: column name to enforce uniqueness, or `None`
- Raises `TableExistsError` if the table already exists
- Raises `ValueError` if the primary key column is not in the column definitions

#### `drop_table(name)`

Drop an existing table and its data.

```python
def drop_table(self, name: str) -> None:
```

- Raises `TableNotFoundError` if the table does not exist

#### `insert(table, row)`

Insert a row into a table. The row must contain exactly the columns defined in
the schema, and each value must match the declared type.

```python
def insert(self, table: str, row: dict[str, Any]) -> None:
```

- Raises `TableNotFoundError` if the table does not exist
- Raises `TypeValidationError` if a value does not match its column type
- Raises `PrimaryKeyViolationError` if a duplicate primary key is inserted
- Raises `ValueError` if the row has unknown or missing columns

#### `select(table, where=None)`

Select rows from a table with optional filtering. Returns copies of rows (not
references to internal data).

```python
def select(
    self,
    table: str,
    where: dict[str, Any] | Callable[[dict[str, Any]], bool] | None = None,
) -> list[dict[str, Any]]:
```

The `where` parameter accepts:
- `None` — return all rows
- A `dict` — equality matching on column values (all conditions must match)
- A `callable` — arbitrary predicate function receiving a row dict

#### `update(table, values, where=None)`

Update matching rows, returning the count of rows modified.

```python
def update(
    self,
    table: str,
    values: dict[str, Any],
    where: dict[str, Any] | Callable[[dict[str, Any]], bool] | None = None,
) -> int:
```

- Raises `TypeValidationError` if a new value does not match its column type
- Raises `ValueError` if values references unknown columns

#### `delete(table, where=None)`

Delete matching rows, returning the count of rows removed.

```python
def delete(
    self,
    table: str,
    where: dict[str, Any] | Callable[[dict[str, Any]], bool] | None = None,
) -> int:
```

### Exception Hierarchy

All database exceptions inherit from `DatabaseError`:

| Exception | Raised when |
|-----------|-------------|
| `DatabaseError` | Base class for all database errors |
| `TableExistsError` | Creating a table that already exists |
| `TableNotFoundError` | Referencing a table that does not exist |
| `PrimaryKeyViolationError` | Inserting a duplicate primary key value |
| `TypeValidationError` | A column value does not match its declared type |

### Design

- **Storage**: Tables are stored as `dict[str, list[dict]]` — each table name
  maps to a list of row dicts.
- **Schemas**: Column types are tracked per-table in a parallel `dict[str, dict[str, type]]`.
- **Primary keys**: Tracked per-table; uniqueness is checked by scanning existing
  rows on each insert.
- **Row isolation**: `select()` returns shallow copies of row dicts, so mutating
  returned rows does not affect stored data. `insert()` also stores a copy.
- **Where clauses**: Polymorphic — accepts `None`, a dict of equality conditions,
  or a callable predicate. Shared across `select`, `update`, and `delete`.

### Complexity

| Operation | Time | Space |
|-----------|------|-------|
| `create_table` | O(1) | O(c) where c = number of columns |
| `drop_table` | O(1) | — |
| `insert` | O(n) | O(c) per row (n = existing rows, for PK check) |
| `select` | O(n) | O(m) where m = matching rows |
| `update` | O(n) | O(1) |
| `delete` | O(n) | O(n) (rebuilds list) |

### Test Coverage

The test suite (`tests/test_inmemory_db.py`) is organized by operation:

| Category | Cases |
|----------|-------|
| **Create table** | Basic creation, without primary key, duplicate table error, invalid PK error |
| **Drop table** | Drop existing, drop nonexistent error, recreate after drop |
| **Insert** | Single row, multiple rows, duplicate PK error, wrong type error, missing column error, extra column error, nonexistent table error, duplicates without PK |
| **Select** | All rows, dict filter, callable filter, no match, returns copies, nonexistent table error, multiple conditions |
| **Update** | Matching rows, all rows, no match, wrong type error, unknown column error, nonexistent table error |
| **Delete** | Matching rows, all rows, no match, callable filter, nonexistent table error |

### Usage Examples

```python
from challenges.inmemory_db import InMemoryDB

db = InMemoryDB()

# Create a table with typed columns and a primary key
db.create_table("users", {"id": int, "name": str, "age": int}, primary_key="id")

# Insert rows
db.insert("users", {"id": 1, "name": "Alice", "age": 30})
db.insert("users", {"id": 2, "name": "Bob", "age": 25})
db.insert("users", {"id": 3, "name": "Charlie", "age": 35})

# Select all rows
all_users = db.select("users")
# => [{"id": 1, "name": "Alice", "age": 30}, ...]

# Filter with a dict (equality matching)
alice = db.select("users", where={"name": "Alice"})
# => [{"id": 1, "name": "Alice", "age": 30}]

# Filter with a callable (arbitrary predicate)
seniors = db.select("users", where=lambda r: r["age"] >= 30)
# => [{"id": 1, ...}, {"id": 3, ...}]

# Update matching rows (returns count of updated rows)
count = db.update("users", {"age": 31}, where={"name": "Alice"})
# => 1

# Delete matching rows (returns count of deleted rows)
count = db.delete("users", where={"name": "Bob"})
# => 1

# Drop the table
db.drop_table("users")
```
