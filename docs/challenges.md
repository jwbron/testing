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

Implement a document-oriented in-memory NoSQL database with a MongoDB-style API.
The database stores documents (Python dicts) in named collections, supports rich
querying with comparison/logical operators, secondary indexes for accelerated
lookups, a multi-stage aggregation pipeline, and transactions with snapshot
isolation.

### Architecture

The module is organized into several cooperating classes:

| Class | Responsibility |
|-------|---------------|
| `Database` | Top-level container; manages named `Collection` instances |
| `Collection` | Stores documents as dicts with auto-generated `_id` fields; exposes CRUD and aggregation methods |
| `QueryEngine` | Evaluates query filters with `$`-prefixed operators against documents |
| `IndexManager` | Maintains sorted secondary indexes; auto-updated on write operations |
| `AggregationPipeline` | Processes multi-stage aggregation pipelines over collection data |
| `Transaction` | Provides snapshot isolation with buffered writes and conflict detection |

### API Reference

#### Database

```python
from challenges.nosql_db import Database

db = Database()

# Access or create a collection (creates on first access)
users = db.collection("users")

# List collection names
db.list_collections()  # -> ["users"]

# Drop a collection
db.drop_collection("users")
```

#### Collection &mdash; CRUD Operations

```python
users = db.collection("users")

# Insert a single document (returns the generated _id)
doc_id = users.insert_one({"name": "Alice", "age": 30})

# Insert multiple documents (returns list of _ids)
ids = users.insert_many([
    {"name": "Bob", "age": 25},
    {"name": "Charlie", "age": 35},
])

# Find one document matching a filter
user = users.find_one({"name": "Alice"})
# -> {"_id": "...", "name": "Alice", "age": 30}

# Find all documents matching a filter (returns list)
young = users.find({"age": {"$lt": 30}})

# Find all documents (empty filter)
all_users = users.find({})

# Update one matching document
users.update_one({"name": "Alice"}, {"$set": {"age": 31}})

# Update all matching documents
users.update_many({"age": {"$lt": 30}}, {"$set": {"status": "young"}})

# Delete one matching document
users.delete_one({"name": "Bob"})

# Delete all matching documents
users.delete_many({"age": {"$gt": 50}})

# Count documents matching a filter
count = users.count({"age": {"$gte": 30}})
```

#### Query Operators

The query engine supports the following operators in filter documents:

**Comparison operators**

| Operator | Description | Example |
|----------|-------------|---------|
| `$eq` | Equal to | `{"age": {"$eq": 30}}` |
| `$ne` | Not equal to | `{"age": {"$ne": 30}}` |
| `$gt` | Greater than | `{"age": {"$gt": 25}}` |
| `$gte` | Greater than or equal | `{"age": {"$gte": 25}}` |
| `$lt` | Less than | `{"age": {"$lt": 35}}` |
| `$lte` | Less than or equal | `{"age": {"$lte": 35}}` |

**Set operators**

| Operator | Description | Example |
|----------|-------------|---------|
| `$in` | Matches any value in array | `{"status": {"$in": ["active", "pending"]}}` |
| `$nin` | Matches none of the values | `{"status": {"$nin": ["banned"]}}` |

**Logical operators**

| Operator | Description | Example |
|----------|-------------|---------|
| `$and` | All conditions must match | `{"$and": [{"age": {"$gte": 18}}, {"age": {"$lt": 65}}]}` |
| `$or` | At least one condition matches | `{"$or": [{"age": {"$lt": 18}}, {"age": {"$gte": 65}}]}` |
| `$not` | Negates a condition | `{"age": {"$not": {"$gt": 30}}}` |
| `$nor` | None of the conditions match | `{"$nor": [{"status": "banned"}, {"age": {"$lt": 13}}]}` |

**Existence**

| Operator | Description | Example |
|----------|-------------|---------|
| `$exists` | Field exists (or not) | `{"email": {"$exists": true}}` |

**Dot notation** for nested fields:

```python
# Query nested documents
users.find({"address.city": "New York"})
users.find({"profile.settings.theme": "dark"})
```

#### Indexes

```python
# Create a secondary index on a field
users.create_index("age")

# Queries on indexed fields are accelerated automatically
users.find({"age": {"$gte": 25, "$lte": 35}})  # uses index

# Drop an index
users.drop_index("age")
```

Indexes are automatically maintained when documents are inserted, updated, or
deleted. They accelerate equality and range lookups on the indexed field.

#### Aggregation Pipeline

```python
# Multi-stage aggregation
results = users.aggregate([
    {"$match": {"age": {"$gte": 18}}},
    {"$group": {
        "_id": "$department",
        "count": {"$count": {}},
        "avg_age": {"$avg": "$age"},
        "total_salary": {"$sum": "$salary"},
        "youngest": {"$min": "$age"},
        "oldest": {"$max": "$age"},
        "names": {"$push": "$name"},
    }},
    {"$sort": {"count": -1}},
    {"$limit": 10},
])
```

**Supported stages**

| Stage | Description |
|-------|-------------|
| `$match` | Filter documents (same syntax as `find`) |
| `$group` | Group by `_id` expression with accumulators (`$sum`, `$avg`, `$min`, `$max`, `$push`, `$count`) |
| `$sort` | Sort by fields (1 = ascending, -1 = descending) |
| `$limit` | Limit the number of results |
| `$skip` | Skip a number of results |
| `$project` | Include/exclude or reshape fields |
| `$unwind` | Deconstruct an array field into one document per element |
| `$count` | Count documents and output as a named field |

#### Transactions

```python
# Start a transaction (snapshot isolation)
txn = db.begin_transaction()

# Access collections through the transaction proxy
users = txn.collection("users")

# Reads see a consistent snapshot
user = users.find_one({"name": "Alice"})

# Writes are buffered until commit
users.insert_one({"name": "Dave", "age": 28})
users.update_one({"name": "Alice"}, {"$set": {"age": 32}})
users.delete_one({"name": "Bob"})

# Commit applies all changes atomically
txn.commit()

# Or rollback to discard all buffered changes
# txn.rollback()
```

**Convenience shorthand methods** are also available directly on the transaction:

```python
txn = db.begin_transaction()
txn.insert("users", {"name": "Dave", "age": 28})
txn.find("users", {"name": "Alice"})
txn.update("users", {"name": "Alice"}, {"$set": {"age": 32}})
txn.delete("users", {"name": "Bob"})
txn.commit()
```

**Transaction guarantees**:

- **Snapshot isolation**: Reads see a consistent point-in-time snapshot taken at
  transaction start
- **Buffered writes**: All writes are held in memory until `commit()`
- **Atomic commit**: All changes apply together or not at all
- **Conflict detection**: If another write modifies a document read by the
  transaction, `commit()` raises a conflict error
- **Rollback**: `rollback()` discards all buffered changes and releases the
  snapshot

### Complexity

| Operation | Time | Notes |
|-----------|------|-------|
| `insert_one` | O(1) amortized | O(log n) per index if indexes exist |
| `find` (no index) | O(n) | Full collection scan |
| `find` (indexed) | O(log n + k) | k = number of matching documents |
| `update_one` | O(n) | O(log n) if querying on indexed field |
| `delete_one` | O(n) | O(log n) if querying on indexed field |
| `aggregate` | Varies | Depends on pipeline stages |
| Transaction commit | O(w) | w = number of buffered writes |

### Usage Examples

```python
from challenges.nosql_db import Database

# Create a database and collection
db = Database()
products = db.collection("products")

# Insert product data
products.insert_many([
    {"name": "Laptop", "price": 999, "category": "electronics", "tags": ["computer", "portable"]},
    {"name": "Phone", "price": 699, "category": "electronics", "tags": ["mobile", "portable"]},
    {"name": "Desk", "price": 299, "category": "furniture", "tags": ["office"]},
    {"name": "Chair", "price": 199, "category": "furniture", "tags": ["office", "ergonomic"]},
])

# Query with operators
expensive = products.find({"price": {"$gte": 500}})
# -> [Laptop, Phone]

# Logical operators
electronics_or_cheap = products.find({
    "$or": [
        {"category": "electronics"},
        {"price": {"$lt": 250}},
    ]
})
# -> [Laptop, Phone, Chair]

# Aggregation: average price by category
category_stats = products.aggregate([
    {"$group": {
        "_id": "$category",
        "avg_price": {"$avg": "$price"},
        "count": {"$count": {}},
    }},
    {"$sort": {"avg_price": -1}},
])
# -> [{"_id": "electronics", "avg_price": 849, "count": 2},
#     {"_id": "furniture", "avg_price": 249, "count": 2}]

# Unwind array fields
products.aggregate([
    {"$unwind": "$tags"},
    {"$group": {"_id": "$tags", "count": {"$count": {}}}},
    {"$sort": {"count": -1}},
])
# -> [{"_id": "portable", "count": 2}, {"_id": "office", "count": 2}, ...]

# Transactions
txn = db.begin_transaction()
txn_products = txn.collection("products")
txn_products.update_one({"name": "Laptop"}, {"$set": {"price": 899}})
txn_products.insert_one({"name": "Monitor", "price": 399, "category": "electronics"})
txn.commit()
```
