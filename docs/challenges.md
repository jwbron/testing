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

Build a simple in-memory relational database that supports core SQL-like
operations: creating and dropping tables with typed columns, inserting rows,
querying with filters, updating rows, and deleting rows. The database must
enforce primary key uniqueness constraints.

This is a design exercise that tests data structure choices, constraint
enforcement, and API design without relying on any external database engine.

### API

```python
from challenges.inmemory_db import InMemoryDB

db = InMemoryDB()
```

#### Table Management

```python
# Create a table with typed columns and a primary key
db.create_table("users", {
    "id": "int",
    "name": "str",
    "email": "str",
}, primary_key="id")

# Drop a table
db.drop_table("users")
```

#### Insert

```python
db.insert("users", {"id": 1, "name": "Alice", "email": "alice@example.com"})
db.insert("users", {"id": 2, "name": "Bob", "email": "bob@example.com"})

# Inserting a duplicate primary key raises an error
db.insert("users", {"id": 1, "name": "Duplicate", "email": "dup@example.com"})
# => raises exception (primary key violation)
```

#### Select

```python
# Select all rows
rows = db.select("users")
# => [{"id": 1, "name": "Alice", ...}, {"id": 2, "name": "Bob", ...}]

# Select with a predicate filter
rows = db.select("users", where=lambda r: r["name"] == "Alice")
# => [{"id": 1, "name": "Alice", "email": "alice@example.com"}]

# Select specific columns only
rows = db.select("users", columns=["name", "email"])
# => [{"name": "Alice", "email": "alice@example.com"}, ...]
```

#### Update

```python
# Update rows matching a predicate — returns count of rows updated
count = db.update("users", values={"email": "newalice@example.com"},
                  where=lambda r: r["name"] == "Alice")
```

#### Delete

```python
# Delete rows matching a predicate — returns count of rows deleted
count = db.delete("users", where=lambda r: r["name"] == "Bob")
```

### Design

The database stores tables as an in-memory dictionary mapping table names to
their schema (column definitions, primary key) and row data. Each table's rows
are stored in a list of dictionaries.

Key design decisions:

- **Typed columns**: Column types (`int`, `str`, `float`, `bool`) are declared
  at table creation time. Values are validated on insert and update. Integer
  values are automatically promoted to float when inserted into a float column.
- **Primary key enforcement**: A designated primary key column uses a hash-set
  index for O(1) uniqueness checks on insert. Primary key values must not be
  null.
- **Predicate-based filtering**: Select, update, and delete operations accept
  an optional `where` callable (`Callable[[dict], bool]`) for flexible row
  filtering. This enables arbitrary predicates beyond simple equality.
- **Column projection**: Select supports an optional `columns` parameter to
  return only a subset of columns.
- **Custom exception hierarchy**: Structured exceptions (`TableExistsError`,
  `TableNotFoundError`, `ColumnError`, `PrimaryKeyError`) all inherit from
  `DatabaseError` for easy catch-all handling.

### Supported Operations

| Operation | Method | Description |
|-----------|--------|-------------|
| Create table | `create_table(name, columns, primary_key=None)` | Define a new table with typed columns |
| Drop table | `drop_table(name)` | Remove a table and all its data |
| Insert row | `insert(table, row)` | Add a row, enforcing PK uniqueness and type checks |
| Select rows | `select(table, columns=None, where=None)` | Query rows with optional column projection and predicate filter |
| Update rows | `update(table, values, where=None)` | Modify matching rows; returns count updated |
| Delete rows | `delete(table, where=None)` | Remove matching rows; returns count deleted |

### Complexity

| Operation | Time | Notes |
|-----------|------|-------|
| Create/Drop table | O(1) | Dictionary insertion/deletion |
| Insert | O(1) amortized | PK uniqueness via hash-set index; row validation is O(columns) |
| Select | O(n) | Linear scan with optional filtering |
| Update | O(n) | Linear scan to find and modify matching rows |
| Delete | O(n) | Linear scan to filter out matching rows |

Where *n* is the number of rows in the target table.

### Edge Cases

| Case | Behaviour |
|------|-----------|
| Create duplicate table | Raises `TableExistsError` |
| Drop nonexistent table | Raises `TableNotFoundError` |
| Insert into nonexistent table | Raises `TableNotFoundError` |
| Duplicate primary key | Raises `PrimaryKeyError` |
| Null primary key value | Raises `PrimaryKeyError` |
| Unknown column in row | Raises `ColumnError` |
| Type mismatch on insert/update | Raises `ColumnError` |
| Unsupported column type at creation | Raises `ColumnError` |
| Select from empty table | Returns empty list |
| Update with no matches | Returns 0, no rows modified |
| Delete with no matches | Returns 0, no rows deleted |
| Missing columns on insert | Filled with `None` |

### Usage Examples

```python
from challenges.inmemory_db import InMemoryDB

db = InMemoryDB()

# Set up a table
db.create_table("products", {
    "id": "int",
    "name": "str",
    "price": "float",
}, primary_key="id")

# Populate it
db.insert("products", {"id": 1, "name": "Widget", "price": 9.99})
db.insert("products", {"id": 2, "name": "Gadget", "price": 24.99})
db.insert("products", {"id": 3, "name": "Doohickey", "price": 4.99})

# Query with a predicate
cheap = db.select("products", where=lambda r: r["name"] == "Doohickey")
# => [{"id": 3, "name": "Doohickey", "price": 4.99}]

# Select specific columns
names = db.select("products", columns=["name"])
# => [{"name": "Widget"}, {"name": "Gadget"}, {"name": "Doohickey"}]

# Update a price
db.update("products", values={"price": 19.99}, where=lambda r: r["id"] == 1)

# Remove a product
db.delete("products", where=lambda r: r["id"] == 2)

# Verify
remaining = db.select("products")
# => [{"id": 1, ...}, {"id": 3, ...}]

# Clean up
db.drop_table("products")
```
