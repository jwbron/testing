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

Build a simple in-memory relational database that supports table creation with
typed columns, primary key constraints, and full CRUD operations (insert,
select, update, delete) with callable-based filtering.

### API

```python
from challenges.inmemory_db import InMemoryDB

db = InMemoryDB()
```

#### `create_table(name, columns, primary_key=None)`

Create a new table with a typed schema.

```python
db.create_table("users", {"id": int, "name": str, "age": int}, primary_key="id")
```

- `name` &mdash; table name (string)
- `columns` &mdash; mapping of column names to Python types (`int`, `str`, `float`, `bool`, etc.)
- `primary_key` &mdash; optional column name to enforce uniqueness

**Raises**: `TableExistsError` if the table already exists; `ColumnError` if the
primary key is not in the column definitions.

#### `drop_table(name)`

Remove a table and all its data.

```python
db.drop_table("users")
```

**Raises**: `TableNotFoundError` if the table does not exist.

#### `insert(table, row)`

Insert a row into a table. The row must include all defined columns with values
matching the expected types.

```python
db.insert("users", {"id": 1, "name": "Alice", "age": 30})
```

**Raises**:
- `TableNotFoundError` &mdash; table does not exist
- `ColumnError` &mdash; missing or extra columns
- `TypeValidationError` &mdash; value type does not match column type
- `PrimaryKeyViolationError` &mdash; duplicate primary key value

#### `select(table, where=None)`

Retrieve rows from a table, optionally filtered by a predicate. Returns copies
of the rows (mutations to results do not affect stored data).

```python
# All rows
db.select("users")

# Filtered
db.select("users", where=lambda r: r["age"] > 25)
```

**Returns**: `list[dict[str, Any]]` &mdash; matching rows.

#### `update(table, values, where=None)`

Update columns on matching rows. Returns the count of updated rows.

```python
count = db.update("users", {"age": 31}, where=lambda r: r["name"] == "Alice")
```

**Raises**: `ColumnError` for unknown columns; `TypeValidationError` for type
mismatches; `PrimaryKeyViolationError` if the update would create duplicate
primary keys.

**Returns**: `int` &mdash; number of rows updated.

#### `delete(table, where=None)`

Delete matching rows (or all rows if no predicate). Returns the count of deleted
rows.

```python
count = db.delete("users", where=lambda r: r["id"] == 1)
```

**Returns**: `int` &mdash; number of rows deleted.

### Exception Hierarchy

All exceptions inherit from `DatabaseError`:

| Exception | When raised |
|-----------|-------------|
| `DatabaseError` | Base class for all database errors |
| `TableExistsError` | Creating a table that already exists |
| `TableNotFoundError` | Referencing a table that does not exist |
| `PrimaryKeyViolationError` | Inserting/updating with a duplicate primary key |
| `TypeValidationError` | Column value does not match the expected type |
| `ColumnError` | Missing or unknown columns in a row |

### Design Decisions

- **Callable-based filtering**: The `where` parameter accepts any
  `Callable[[dict], bool]`, giving callers maximum flexibility without
  implementing a query language.
- **Copy semantics**: `select` returns deep copies of rows so callers cannot
  accidentally mutate stored data.
- **Strict schema enforcement**: Every `insert` must provide exactly the columns
  defined in the schema &mdash; no extra, no missing.
- **Linear primary key checks**: Primary key uniqueness is validated via a linear
  scan of existing rows. Suitable for small datasets; for larger tables an index
  structure would be needed.

### Complexity

| Operation | Time | Space | Notes |
|-----------|------|-------|-------|
| `create_table` | O(1) | O(c) | c = number of columns |
| `drop_table` | O(1) | &mdash; | Frees table storage |
| `insert` | O(n) | O(c) | n = existing rows (PK scan) |
| `select` | O(n) | O(n) | Returns copies of matching rows |
| `update` | O(n) | O(1) | In-place update of matched rows |
| `delete` | O(n) | O(n) | Builds new list of kept rows |

### Usage Examples

```python
from challenges.inmemory_db import InMemoryDB

db = InMemoryDB()

# Create a table with a primary key
db.create_table("products", {"sku": str, "name": str, "price": float}, primary_key="sku")

# Insert rows
db.insert("products", {"sku": "A001", "name": "Widget", "price": 9.99})
db.insert("products", {"sku": "A002", "name": "Gadget", "price": 24.99})

# Select all
db.select("products")
# => [{'sku': 'A001', 'name': 'Widget', 'price': 9.99},
#     {'sku': 'A002', 'name': 'Gadget', 'price': 24.99}]

# Select with filter
db.select("products", where=lambda r: r["price"] < 15.0)
# => [{'sku': 'A001', 'name': 'Widget', 'price': 9.99}]

# Update rows
db.update("products", {"price": 19.99}, where=lambda r: r["sku"] == "A001")

# Delete rows
db.delete("products", where=lambda r: r["sku"] == "A002")

# Drop the table
db.drop_table("products")
```

### Error Handling Examples

```python
from challenges.inmemory_db import (
    InMemoryDB,
    TableExistsError,
    PrimaryKeyViolationError,
    TypeValidationError,
)

db = InMemoryDB()
db.create_table("users", {"id": int, "name": str}, primary_key="id")

# Duplicate table
try:
    db.create_table("users", {"id": int})
except TableExistsError:
    pass  # "Table 'users' already exists"

# Duplicate primary key
db.insert("users", {"id": 1, "name": "Alice"})
try:
    db.insert("users", {"id": 1, "name": "Bob"})
except PrimaryKeyViolationError:
    pass  # "Duplicate primary key 'id' = 1"

# Wrong column type
try:
    db.insert("users", {"id": "not_an_int", "name": "Charlie"})
except TypeValidationError:
    pass  # "Column 'id' expected int, got str"
```
