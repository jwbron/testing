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

## In-Memory NoSQL Database

**Module**: `src/challenges/nosql_db.py`
**Tests**: `tests/test_nosql_db.py`

### Problem Statement

Build a thorough in-memory document-oriented NoSQL database inspired by
MongoDB's API. The database stores documents (Python dicts) in named
collections, supports rich queries with `$`-prefixed operators, maintains
secondary indexes for accelerated lookups, provides a multi-stage aggregation
pipeline, and supports transactions with snapshot isolation.

### Architecture Overview

The module is organized into several cooperating classes:

| Class | Responsibility |
|-------|----------------|
| `Database` | Top-level container; manages named `Collection` instances |
| `Collection` | Stores documents; exposes CRUD, aggregation, and index operations |
| `QueryEngine` | Evaluates filter expressions against documents |
| `IndexManager` | Maintains sorted secondary indexes for fast lookups |
| `AggregationPipeline` | Executes multi-stage data transformation pipelines |
| `Transaction` | Provides snapshot isolation with buffered writes and conflict detection |

### API Reference

#### Database

```python
from challenges.nosql_db import Database

db = Database()

# Get or create a collection (returns the same instance for the same name)
users = db.get_collection("users")
orders = db.get_collection("orders")

# List collection names
db.list_collections()  # => ["users", "orders"]

# Drop a collection
db.drop_collection("orders")
```

#### Collection — CRUD Operations

Every inserted document receives an auto-generated `_id` field (a UUID string)
if one is not already present. Documents are stored as independent copies to
prevent external mutation.

```python
users = db.get_collection("users")

# Insert a single document — returns the generated _id
doc_id = users.insert_one({"name": "Alice", "age": 30, "role": "admin"})

# Insert multiple documents — returns a list of _ids
ids = users.insert_many([
    {"name": "Bob", "age": 25, "role": "user"},
    {"name": "Carol", "age": 35, "role": "admin"},
])

# Find one document matching a filter
user = users.find_one({"name": "Alice"})
# => {"_id": "...", "name": "Alice", "age": 30, "role": "admin"}

# Find all documents matching a filter (returns a list)
admins = users.find({"role": "admin"})

# Find all documents (empty filter)
all_users = users.find({})

# Update one document — returns count of modified documents
modified = users.update_one({"name": "Alice"}, {"$set": {"age": 31}})

# Update many documents
modified = users.update_many({"role": "user"}, {"$set": {"active": True}})

# Delete one document — returns count of deleted documents
deleted = users.delete_one({"name": "Bob"})

# Delete many documents
deleted = users.delete_many({"role": "user"})

# Count documents matching a filter
count = users.count({"role": "admin"})
```

#### Query Engine — Filter Operators

The query engine supports MongoDB-style operators for building expressive
filters. Filters are plain dicts where keys are field names and values are
either literal matches or operator expressions.

**Comparison operators:**

| Operator | Description | Example |
|----------|-------------|---------|
| `$eq` | Equal to | `{"age": {"$eq": 30}}` |
| `$ne` | Not equal to | `{"age": {"$ne": 30}}` |
| `$gt` | Greater than | `{"age": {"$gt": 25}}` |
| `$gte` | Greater than or equal | `{"age": {"$gte": 25}}` |
| `$lt` | Less than | `{"age": {"$lt": 35}}` |
| `$lte` | Less than or equal | `{"age": {"$lte": 35}}` |

**Set operators:**

| Operator | Description | Example |
|----------|-------------|---------|
| `$in` | Value is in array | `{"role": {"$in": ["admin", "mod"]}}` |
| `$nin` | Value is not in array | `{"role": {"$nin": ["guest"]}}` |

**Logical operators:**

| Operator | Description | Example |
|----------|-------------|---------|
| `$and` | All conditions must match | `{"$and": [{"age": {"$gt": 20}}, {"role": "admin"}]}` |
| `$or` | At least one condition must match | `{"$or": [{"role": "admin"}, {"age": {"$lt": 25}}]}` |
| `$not` | Negates a condition | `{"age": {"$not": {"$gt": 30}}}` |
| `$nor` | None of the conditions must match | `{"$nor": [{"role": "guest"}, {"age": {"$lt": 18}}]}` |

**Existence operator:**

| Operator | Description | Example |
|----------|-------------|---------|
| `$exists` | Field exists (true) or is absent (false) | `{"email": {"$exists": True}}` |

**Nested field access (dot notation):**

Use dot-separated paths to query nested documents:

```python
users.insert_one({
    "name": "Alice",
    "address": {"city": "Seattle", "state": "WA"}
})

# Query nested fields with dot notation
users.find({"address.city": "Seattle"})
```

#### Indexes

Secondary indexes accelerate equality and range queries. Indexes are
maintained automatically as documents are inserted, updated, or deleted.

```python
users = db.get_collection("users")

# Create an index on the "age" field
users.create_index("age")

# Create an index on a nested field
users.create_index("address.city")

# Drop an index
users.drop_index("age")
```

When an index exists on a queried field, the query engine uses it to narrow
the candidate set before applying the full filter, improving query performance
from O(n) to approximately O(log n + k) where k is the number of matching
documents.

#### Aggregation Pipeline

The `aggregate()` method on `Collection` accepts a list of pipeline stages
that transform the document stream sequentially:

```python
results = users.aggregate([
    {"$match": {"role": "admin"}},
    {"$group": {
        "_id": "$role",
        "avg_age": {"$avg": "$age"},
        "count": {"$count": {}},
    }},
    {"$sort": {"avg_age": -1}},
    {"$limit": 10},
])
```

**Supported pipeline stages:**

| Stage | Description |
|-------|-------------|
| `$match` | Filter documents (uses the same query syntax as `find`) |
| `$group` | Group documents by a key and compute aggregates |
| `$sort` | Sort documents by one or more fields (1 = ascending, -1 = descending) |
| `$limit` | Limit the number of output documents |
| `$skip` | Skip a number of documents |
| `$project` | Include, exclude, or reshape fields |
| `$unwind` | Deconstruct an array field into one document per element |
| `$count` | Replace the document stream with a count |

**Aggregation accumulators (used inside `$group`):**

| Accumulator | Description |
|-------------|-------------|
| `$sum` | Sum of numeric values (use `1` to count) |
| `$avg` | Average of numeric values |
| `$min` | Minimum value |
| `$max` | Maximum value |
| `$push` | Collect all values into an array |
| `$count` | Count of documents in the group |

**Example — Sales report:**

```python
orders = db.get_collection("orders")
orders.insert_many([
    {"product": "Widget", "quantity": 5, "price": 10.00},
    {"product": "Widget", "quantity": 3, "price": 10.00},
    {"product": "Gadget", "quantity": 1, "price": 50.00},
])

report = orders.aggregate([
    {"$group": {
        "_id": "$product",
        "total_qty": {"$sum": "$quantity"},
        "avg_price": {"$avg": "$price"},
        "order_count": {"$count": {}},
    }},
    {"$sort": {"total_qty": -1}},
])
# => [
#     {"_id": "Widget", "total_qty": 8, "avg_price": 10.0, "order_count": 2},
#     {"_id": "Gadget", "total_qty": 1, "avg_price": 50.0, "order_count": 1},
# ]
```

#### Transactions

Transactions provide snapshot isolation — reads within a transaction see a
consistent snapshot taken at `begin()`, and writes are buffered until
`commit()`. Conflict detection at commit time prevents lost updates.

```python
db = Database()
users = db.get_collection("users")
users.insert_one({"name": "Alice", "balance": 100})

# Begin a transaction
txn = db.begin_transaction()

# Reads within the transaction see the snapshot
alice = txn.find_one("users", {"name": "Alice"})

# Writes are buffered (not visible outside the transaction)
txn.update_one("users", {"name": "Alice"}, {"$set": {"balance": 50}})

# Commit applies all buffered writes atomically
txn.commit()

# Rollback discards all buffered writes
txn2 = db.begin_transaction()
txn2.delete_one("users", {"name": "Alice"})
txn2.rollback()  # Alice is still there
```

**Transaction guarantees:**

- **Snapshot isolation**: Reads see a consistent point-in-time view
- **Atomic commit**: All writes in a transaction are applied together or not
  at all
- **Conflict detection**: If another write modifies a document that the
  transaction also modified, commit raises a conflict error
- **Rollback**: Discards all buffered writes and releases the snapshot

### Complexity

| Operation | Average Case | Notes |
|-----------|-------------|-------|
| `insert_one` | O(1) + O(k) index updates | k = number of indexes |
| `find` (no index) | O(n) | Full collection scan |
| `find` (indexed) | O(log n + m) | m = matching documents |
| `update_one` | O(n) find + O(k) index updates | |
| `delete_one` | O(n) find + O(k) index updates | |
| `aggregate` | O(n) per stage | Depends on pipeline stages |
| Transaction commit | O(w) | w = number of buffered writes |

### Design Decisions

1. **Documents are deep-copied** on insert and retrieval to prevent external
   mutation from corrupting the database state.

2. **Auto-generated `_id` fields** use UUIDs to guarantee uniqueness without
   coordination.

3. **Indexes use sorted structures** (via Python's `bisect` module) to support
   both equality and range queries efficiently.

4. **The query engine is decoupled** from `Collection` to allow independent
   testing and potential reuse.

5. **Transactions buffer writes** in memory rather than using write-ahead
   logging, keeping the implementation simple while still providing atomicity.

### Usage Examples

```python
from challenges.nosql_db import Database

# Create a database and collection
db = Database()
users = db.get_collection("users")

# Insert documents
users.insert_many([
    {"name": "Alice", "age": 30, "tags": ["admin", "dev"]},
    {"name": "Bob", "age": 25, "tags": ["dev"]},
    {"name": "Carol", "age": 35, "tags": ["admin"]},
])

# Query with operators
young_admins = users.find({
    "$and": [
        {"age": {"$lt": 35}},
        {"tags": {"$in": ["admin"]}},
    ]
})

# Create an index for faster lookups
users.create_index("age")

# Aggregation pipeline
age_stats = users.aggregate([
    {"$group": {
        "_id": None,
        "avg_age": {"$avg": "$age"},
        "min_age": {"$min": "$age"},
        "max_age": {"$max": "$age"},
    }},
])

# Unwind arrays
users.aggregate([
    {"$unwind": "$tags"},
    {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
])
# => [{"_id": "admin", "count": 2}, {"_id": "dev", "count": 2}]

# Transactions
txn = db.begin_transaction()
txn.update_one("users", {"name": "Alice"}, {"$set": {"age": 31}})
txn.insert_one("users", {"name": "Dave", "age": 28, "tags": ["dev"]})
txn.commit()
```
