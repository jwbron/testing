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

Build a simple in-memory relational database as a single Python module. The
database must support creating and dropping tables with typed columns, inserting
rows with type validation, selecting rows with optional filtering, updating rows
matching a filter, deleting rows matching a filter, and enforcing primary key
uniqueness constraints.

### API

#### `InMemoryDB` class

The main entry point. Manages a collection of named tables stored as
dictionaries internally.

```python
from challenges.inmemory_db import InMemoryDB

db = InMemoryDB()
```

##### `create_table(name, columns, primary_key)`

Create a new table with typed columns and a primary key.

```python
db.create_table(
    "users",
    {"id": int, "name": str, "active": bool},
    primary_key="id",
)
```

- `name`: table name (string)
- `columns`: dict mapping column names to Python types (`int`, `float`, `str`,
  `bool`)
- `primary_key`: column name to enforce as a unique key (required)
- Raises `ValueError` if the table already exists or primary key is not in
  columns

##### `drop_table(name)`

Remove a table and all its data.

- Raises `ValueError` if the table does not exist

##### `insert(table, row)`

Insert a row into a table.

```python
db.insert("users", {"id": 1, "name": "Alice", "active": True})
```

- Row is stored as a copy (internal mutation is prevented)
- Raises `ValueError` if the table does not exist or the primary key value
  duplicates an existing row
- Raises `TypeError` if any value does not match its column's expected type

##### `select(table, where=None)`

Query rows from a table.

```python
# All rows
db.select("users")

# With filter (dict of column=value equality checks)
db.select("users", where={"active": True})
```

- Returns a list of row dicts (copies, not references to internal data)
- `where`: optional dict of column-value pairs; only rows matching **all**
  conditions are returned

##### `update(table, values, where=None)`

Update matching rows in a table.

```python
count = db.update("users", {"active": False}, where={"name": "Bob"})
# count = number of rows updated
```

- Returns the number of rows updated
- Updates all rows if `where` is `None`
- Validates types of incoming values before applying
- Raises `TypeError` for type mismatches

##### `delete(table, where=None)`

Delete matching rows from a table.

```python
count = db.delete("users", where={"id": 1})
# count = number of rows deleted
```

- Returns the number of rows deleted
- Deletes all rows if `where` is `None`

### Column Types

Columns are defined using Python built-in types. Type checking uses
`isinstance`, so subclass relationships apply (e.g., `bool` is accepted
for `int` columns since `bool` is a subclass of `int` in Python).

| Python type | Example values |
|-------------|----------------|
| `int` | `1`, `42`, `-7` |
| `float` | `3.14`, `0.0` |
| `str` | `"hello"`, `""` |
| `bool` | `True`, `False` |

### Error Handling

The implementation uses standard Python exceptions:

| Exception | Raised when |
|-----------|-------------|
| `ValueError` | Table already exists, table not found, duplicate primary key, primary key not in columns |
| `TypeError` | Value does not match expected column type |

### Design Decisions

- **In-memory storage**: Tables stored as `list[dict]` for simplicity. No
  persistence layer.
- **Dict-based where clauses**: Filters use `{column: value}` dicts with
  equality matching. Simple and sufficient for the exercise scope.
- **Copy-on-read**: `select` returns copies of row dicts so callers cannot
  accidentally mutate internal state. `insert` also stores a copy.
- **Standard exceptions**: Uses built-in `ValueError` and `TypeError` rather
  than custom exception classes, keeping the API surface minimal.
- **Required primary key**: Every table must have a primary key column,
  ensuring a clear identity for each row.
- **Linear PK scan**: Primary key uniqueness is checked by scanning existing
  rows (O(n) per insert). Acceptable for a coding exercise; a production
  system would use a hash index.

### Complexity

| Operation | Time | Notes |
|-----------|------|-------|
| `create_table` | O(1) | Dict insertion |
| `drop_table` | O(1) | Dict deletion |
| `insert` | O(n) | Scans rows for PK uniqueness |
| `select` | O(n &times; w) | n = rows, w = where clause size |
| `update` | O(n &times; w) | Scans and updates in place |
| `delete` | O(n) | Rebuilds row list |

### Edge Cases

The test suite covers:

| Category | Cases |
|----------|-------|
| Table lifecycle | Create, drop, duplicate table name, drop non-existent, PK not in columns |
| Insert | Valid rows, type mismatches, duplicate PK, non-existent table |
| Select | All rows, filtered, empty results, from non-existent table |
| Update | All rows, filtered, type validation, return count |
| Delete | All rows, filtered, no match, return count |
| Primary key | Uniqueness enforcement, non-existent column as PK |

### Usage Examples

```python
from challenges.inmemory_db import InMemoryDB

db = InMemoryDB()

# Create a table with typed columns and a primary key
db.create_table(
    "products",
    {"id": int, "name": str, "price": float, "in_stock": bool},
    primary_key="id",
)

# Insert rows
db.insert("products", {"id": 1, "name": "Widget", "price": 9.99, "in_stock": True})
db.insert("products", {"id": 2, "name": "Gadget", "price": 24.99, "in_stock": False})
db.insert("products", {"id": 3, "name": "Doohickey", "price": 4.99, "in_stock": True})

# Select all products
all_products = db.select("products")
# => [{'id': 1, 'name': 'Widget', ...}, {'id': 2, ...}, {'id': 3, ...}]

# Select in-stock products
in_stock = db.select("products", where={"in_stock": True})
# => [{'id': 1, 'name': 'Widget', ...}, {'id': 3, 'name': 'Doohickey', ...}]

# Update a product's price
db.update("products", {"price": 19.99}, where={"name": "Widget"})

# Delete out-of-stock products
deleted = db.delete("products", where={"in_stock": False})
# deleted = 1

# Primary key enforcement
try:
    db.insert("products", {"id": 1, "name": "Duplicate", "price": 0.0, "in_stock": True})
except ValueError as e:
    print(f"Cannot insert: {e}")
    # => Cannot insert: Duplicate primary key value: 1

# Drop the table
db.drop_table("products")
```
