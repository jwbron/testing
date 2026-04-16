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

---

## LRU Cache

**Module**: `src/challenges/lru_cache.py`
**Tests**: `tests/test_lru_cache.py`

### Problem Statement

Design and implement a data structure for a
[Least Recently Used (LRU) cache](https://en.wikipedia.org/wiki/Cache_replacement_policies#LRU).
The cache has a fixed capacity and supports two operations — `get` and `put` —
both in **O(1)** average time.

When the cache reaches capacity, the **least recently used** entry is evicted
before inserting a new one. Accessing or updating an entry makes it the most
recently used.

This is a classic systems-design building block that appears frequently in
coding interviews (e.g.,
[LeetCode 146](https://leetcode.com/problems/lru-cache/)).

### Architecture Overview

The implementation combines two data structures for O(1) operations:

| Component | Role |
|-----------|------|
| **Hash map** (`dict`) | O(1) key → node lookup |
| **Doubly-linked list** | O(1) insertion, removal, and reordering to track access recency |

Sentinel (dummy) head and tail nodes eliminate edge-case checks for empty-list
or single-element operations, keeping the code clean and the logic uniform.

```
 head <-> node_A <-> node_B <-> node_C <-> tail
 (sentinel)     most recent ... least recent     (sentinel)
```

### API Reference

```python
from challenges.lru_cache import LRUCache

cache = LRUCache(capacity=2)

# put(key, value) — insert or update an entry
cache.put(1, 10)
cache.put(2, 20)

# get(key) — returns the value, or -1 if not found
cache.get(1)   # => 10  (key 1 is now most-recently used)

# Inserting beyond capacity evicts the LRU entry
cache.put(3, 30)   # evicts key 2 (least recently used)
cache.get(2)       # => -1  (evicted)
```

#### `LRUCache(capacity: int)`

Create a new LRU cache with the given maximum capacity.

- **capacity** — positive integer specifying the maximum number of key-value
  pairs the cache can hold.

#### `get(key: int) -> int`

Retrieve the value associated with `key`.

- Returns the value if the key exists, and marks it as **most recently used**.
- Returns **-1** if the key is not in the cache.

#### `put(key: int, value: int) -> None`

Insert or update the key-value pair.

- If the key already exists, its value is updated and it becomes the **most
  recently used** entry. No eviction occurs.
- If the key is new and the cache is at capacity, the **least recently used**
  entry is evicted before the new entry is inserted.

### Complexity

| Operation | Time | Space |
|-----------|------|-------|
| `get` | O(1) | — |
| `put` | O(1) | — |
| Overall space | — | O(capacity) |

Both operations are O(1) because the hash map provides constant-time lookup
and the doubly-linked list provides constant-time insertion and removal.

### Edge Cases

The test suite covers the following scenarios:

| Case | Description | Expected Behavior |
|------|-------------|-------------------|
| Basic get/put | Insert and retrieve values | Returns correct value |
| Missing key | `get` on a key not in cache | Returns -1 |
| Capacity eviction | Insert beyond capacity | LRU entry is evicted |
| Access-order update | `get` updates recency | Accessed key is not evicted next |
| Overwrite existing key | `put` with existing key | Value updated, no eviction |
| Capacity-1 cache | Single-slot cache | Each new `put` evicts the previous entry |

### Usage Examples

```python
from challenges.lru_cache import LRUCache

# --- Basic usage ---
cache = LRUCache(2)
cache.put(1, 1)
cache.put(2, 2)
cache.get(1)       # => 1
cache.put(3, 3)    # evicts key 2 (LRU)
cache.get(2)       # => -1 (evicted)
cache.get(3)       # => 3
cache.put(4, 4)    # evicts key 1 (LRU)
cache.get(1)       # => -1 (evicted)
cache.get(3)       # => 3
cache.get(4)       # => 4

# --- Overwriting a key does not evict ---
cache = LRUCache(2)
cache.put(1, 10)
cache.put(2, 20)
cache.put(1, 100)  # updates key 1, no eviction
cache.get(1)       # => 100
cache.get(2)       # => 20  (still present)

# --- Capacity-1 cache ---
cache = LRUCache(1)
cache.put(1, 1)
cache.put(2, 2)    # evicts key 1
cache.get(1)       # => -1
cache.get(2)       # => 2

# --- get() refreshes recency ---
cache = LRUCache(2)
cache.put(1, 1)
cache.put(2, 2)
cache.get(1)       # refreshes key 1
cache.put(3, 3)    # evicts key 2 (now LRU), not key 1
cache.get(1)       # => 1  (still present)
cache.get(2)       # => -1 (evicted)
```

### Design Decisions

1. **Sentinel nodes** for the doubly-linked list head and tail eliminate
   null-pointer checks and simplify insertion/removal logic.

2. **Returns -1 for missing keys** rather than raising an exception, matching
   the conventional LeetCode-style interface and making it easy to integrate
   into algorithmic workflows.

3. **Integer keys and values** keep the interface simple and focused on the
   core cache-eviction algorithm. The pattern generalizes trivially to
   arbitrary hashable keys and values.

---

## MicroGPT

**Module**: `src/challenges/microgpt.py`
**Tests**: `tests/test_microgpt.py`

### Background

This module is a refactored version of Andrej Karpathy's
[microgpt](https://karpathy.github.io/2026/02/12/microgpt/) — a ~200-line
pure-Python GPT implementation that trains and runs inference with zero
external dependencies. The original demonstrates that the complete algorithm
behind modern language models (autograd, tokenizer, transformer, optimizer,
and inference) fits in a single self-contained script.

The refactored version preserves the original's educational clarity while
restructuring it into a modular, testable library with Pydantic configuration
models, full type annotations, and reusable functions.

**Original source**:
[gist.github.com/karpathy/8627fe009c40f57531cb18360106ce95](https://gist.github.com/karpathy/8627fe009c40f57531cb18360106ce95)

### Architecture Overview

The module is organized into five logical groups:

| Component | Description |
|-----------|-------------|
| **Configuration** | Pydantic models (`GPTConfig`, `AdamConfig`, `SampleConfig`) replacing scattered global constants |
| **Scalar autograd** | `Scalar` class (renamed from `Value`) — a computation-graph node with reverse-mode automatic differentiation |
| **NN primitives** | `linear`, `softmax`, `rmsnorm` — the building blocks of the transformer |
| **Tokenizer** | `build_vocab`, `encode`, `decode`, `load_dataset` — character-level tokenization with no import-time side effects |
| **Model** | `init_state_dict`, `gpt`, `adam_step`, `train`, `sample` — initialization, forward pass, optimization, and inference |

### API Reference

#### Configuration Models

All configuration is managed through Pydantic `BaseModel` subclasses with
defaults matching the original script.

```python
from challenges.microgpt import GPTConfig, AdamConfig, SampleConfig

# GPT model configuration
config = GPTConfig(
    n_layer=1,        # number of transformer layers (default: 1)
    n_embd=16,        # embedding dimension (default: 16)
    block_size=16,    # maximum context length (default: 16)
    n_head=4,         # number of attention heads (default: 4)
    vocab_size=27,    # vocabulary size (set after building vocab)
)
config.head_dim      # => 4 (computed: n_embd // n_head)

# Adam optimizer configuration
adam_config = AdamConfig(
    learning_rate=0.01,  # default: 0.01
    beta1=0.85,          # default: 0.85
    beta2=0.99,          # default: 0.99
    eps=1e-8,            # default: 1e-8
)

# Sampling/inference configuration
sample_config = SampleConfig(
    temperature=0.5,   # default: 0.5
    max_tokens=16,     # maximum tokens to generate (default matches block_size)
    num_samples=20,    # number of samples to generate (default: 20)
)
```

#### Scalar — Autograd Engine

`Scalar` is a scalar-valued node in a computation graph that supports
reverse-mode automatic differentiation (backpropagation). It is renamed from
`Value` in the original script for clarity.

```python
from challenges.microgpt import Scalar

# Create scalar values
a = Scalar(2.0)
b = Scalar(3.0)

# Arithmetic operations build a computation graph
c = a * b + Scalar(1.0)   # c.data == 7.0

# Backpropagation computes gradients
c.backward()
print(a.grad)  # 3.0 (dc/da = b)
print(b.grad)  # 2.0 (dc/db = a)
```

**Supported operations:**

| Operation | Forward | Gradient (w.r.t. self) |
|-----------|---------|----------------------|
| `a + b` | `a.data + b.data` | `1` |
| `a * b` | `a.data * b.data` | `b.data` |
| `a ** n` | `a.data ** n` | `n * a.data^(n-1)` |
| `a.log()` | `log(a.data)` | `1 / a.data` |
| `a.exp()` | `exp(a.data)` | `exp(a.data)` |
| `a.relu()` | `max(0, a.data)` | `1 if a.data > 0 else 0` |
| `-a` | `-a.data` | `-1` |
| `a / b` | via `a * b**-1` | — |
| `a - b` | via `a + (-b)` | — |

Mixed `Scalar`/`float` operations are supported in both directions
(`Scalar + float` and `float + Scalar`) via `__radd__`, `__rmul__`, etc.

**Known limitation:** `__pow__` only supports constant (non-`Scalar`)
exponents. `Scalar(2) ** 3` works, but `Scalar(2) ** Scalar(3)` does not.
This is by design — the gradient formula `n * x^(n-1)` assumes `n` is constant.

#### NN Primitives

```python
from challenges.microgpt import Scalar, linear, softmax, rmsnorm

x = [Scalar(1.0), Scalar(2.0), Scalar(3.0)]

# linear(x, w) — matrix-vector multiply
# w is a list of rows; each row has the same length as x
w = [[Scalar(1.0), Scalar(0.0), Scalar(0.0)],
     [Scalar(0.0), Scalar(1.0), Scalar(0.0)]]
y = linear(x, w)   # => [Scalar(1.0), Scalar(2.0)]

# softmax(logits) — numerically stable softmax
probs = softmax(x)  # sums to 1.0

# rmsnorm(x) — root-mean-square normalization
normed = rmsnorm(x)  # output has approximately unit RMS
```

| Function | Signature | Description |
|----------|-----------|-------------|
| `linear` | `(x: list[Scalar], w: list[list[Scalar]]) -> list[Scalar]` | Matrix-vector multiply (one dot product per row of `w`) |
| `softmax` | `(logits: list[Scalar]) -> list[Scalar]` | Numerically stable softmax (subtracts max before exp) |
| `rmsnorm` | `(x: list[Scalar]) -> list[Scalar]` | Root-mean-square normalization with epsilon 1e-5 |

#### Tokenizer

The tokenizer converts text to integer token sequences using a character-level
vocabulary. Unlike the original script, there are no import-time side effects
— the dataset is loaded explicitly.

```python
from challenges.microgpt import build_vocab, encode, decode, load_dataset

# Load training data (downloads from URL if file doesn't exist)
docs = load_dataset("input.txt", url="https://raw.githubusercontent.com/karpathy/makemore/988aa59/names.txt")

# Build vocabulary from documents
vocab, bos = build_vocab(docs)

# Encode text to token IDs (wraps with BOS tokens)
token_ids = encode("hello", vocab, bos)

# Decode token IDs back to text
text = decode(token_ids, vocab, bos)  # => "hello"

# Unknown characters raise ValueError
encode("@", vocab, bos)  # raises ValueError
```

| Function | Signature | Description |
|----------|-----------|-------------|
| `build_vocab` | `(docs: list[str]) -> tuple[list[str], int]` | Extracts sorted unique characters; returns `(vocab, bos_token_id)` |
| `encode` | `(text: str, vocab: list[str], bos: int) -> list[int]` | Tokenizes text, wrapping with BOS tokens |
| `decode` | `(token_ids: list[int], vocab: list[str], bos: int) -> str` | Converts token IDs back to text (strips BOS tokens) |
| `load_dataset` | `(path: str, url: str \| None = None) -> list[str]` | Reads lines from file; downloads from `url` if file is missing |

#### Model Initialization and Forward Pass

```python
from challenges.microgpt import GPTConfig, init_state_dict, gpt

config = GPTConfig(n_layer=1, n_embd=16, n_head=4, vocab_size=27)

# Initialize all model weights
state_dict = init_state_dict(config)
# Keys: 'wte', 'wpe', 'lm_head', 'layer0.attn_wq', 'layer0.attn_wk',
#        'layer0.attn_wv', 'layer0.attn_wo', 'layer0.mlp_fc1', 'layer0.mlp_fc2'

# Forward pass (single token at a time)
keys = [[] for _ in range(config.n_layer)]
values = [[] for _ in range(config.n_layer)]
logits = gpt(token_id=0, pos_id=0, keys=keys, values=values,
             config=config, state_dict=state_dict)
# logits is a list of vocab_size Scalar values
```

| Function | Description |
|----------|-------------|
| `init_state_dict(config)` | Creates all weight matrices (wte, wpe, lm_head, per-layer attention and MLP weights) with Gaussian initialization (std=0.08) |
| `gpt(token_id, pos_id, keys, values, config, state_dict)` | Single-token transformer forward pass; returns `vocab_size` logits. Mutates `keys`/`values` for KV caching. |

#### Training and Inference

```python
from challenges.microgpt import (
    GPTConfig, AdamConfig, SampleConfig,
    load_dataset, train, sample
)

# Load data and train
docs = load_dataset("input.txt")
config = GPTConfig(n_layer=1, n_embd=16, n_head=4, vocab_size=27)
adam_config = AdamConfig(learning_rate=0.01)

state_dict, vocab, bos = train(docs, config, adam_config, num_steps=1000)

# Generate samples
sample_config = SampleConfig(temperature=0.5, num_samples=10)
samples = sample(state_dict, vocab, bos, config, sample_config)
for s in samples:
    print(s)
```

| Function | Signature | Description |
|----------|-----------|-------------|
| `adam_step` | `(params, m, v, step, config) -> None` | One Adam optimizer update step (in-place) |
| `train` | `(docs, config, adam_config, num_steps) -> (state_dict, vocab, bos)` | Full training loop with linear learning-rate warmdown |
| `sample` | `(state_dict, vocab, bos, config, sample_config) -> list[str]` | Temperature-controlled text generation |

### Complexity

Training is pure-Python scalar autograd — each operation creates a graph node.
This is intentionally slow (educational, not production). Performance
characteristics:

| Operation | Complexity | Notes |
|-----------|-----------|-------|
| `Scalar.backward()` | O(V + E) | V = nodes, E = edges in the computation graph |
| `linear(x, w)` | O(n × m) | n = output dim, m = input dim (all scalar ops) |
| `softmax(logits)` | O(n) | Two passes: max + exp/normalize |
| `gpt` (forward) | O(L × d² × T) | L = layers, d = embedding dim, T = sequence length |
| `train` (one step) | O(B × L × d² × T²) | B = 1 (no batching), quadratic in sequence length |

### Design Decisions

1. **`Scalar` instead of `Value`**: The name `Scalar` better communicates that
   each node holds a single floating-point value and participates in an
   autograd computation graph. The original `Value` was ambiguous.

2. **Pydantic for configuration**: `GPTConfig`, `AdamConfig`, and
   `SampleConfig` use Pydantic `BaseModel` for validation and defaults.
   `Scalar` is _not_ a Pydantic model because operator overloads (`__add__`,
   `__mul__`, etc.) conflict with Pydantic's model semantics.

3. **No import-time side effects**: The original script downloads a dataset and
   initializes global model weights at import time. The refactored version
   defers all I/O and initialization to explicit function calls.

4. **Explicit config passing**: Functions like `gpt()` and `train()` accept
   a `GPTConfig` parameter instead of relying on global variables, making
   them independently testable with different configurations.

5. **`__pow__` limited to constant exponents**: The gradient formula
   `n * x^(n-1)` requires `n` to be a constant. Supporting `Scalar ** Scalar`
   would require computing `x^y * ln(x)` for the exponent gradient, which
   adds complexity with no benefit for the GPT use case.

### Edge Cases

The test suite covers:

**Scalar autograd (9 tests):**

| Test | Input | Expected |
|------|-------|----------|
| `test_scalar_add` | `Scalar(2) + Scalar(3)` | data=5, both grads=1.0 |
| `test_scalar_mul` | `Scalar(3) * Scalar(4)` | data=12, grads=4.0/3.0 |
| `test_scalar_pow` | `Scalar(2) ** 3` | data=8, grad=12.0 |
| `test_scalar_relu_positive` | `Scalar(5).relu()` | data=5, grad=1.0 |
| `test_scalar_relu_negative` | `Scalar(-3).relu()` | data=0, grad=0.0 |
| `test_scalar_log` | `Scalar(1).log()` | data=0, grad=1.0 |
| `test_scalar_exp` | `Scalar(0).exp()` | data=1, grad=1.0 |
| `test_scalar_composite_backward` | multi-op chain | correct chain-rule grads |
| `test_scalar_division` | `Scalar(6) / Scalar(3)` | data=2, correct grads |

**NN primitives (5 tests):**

| Test | Description |
|------|-------------|
| `test_linear_identity` | Identity matrix returns input unchanged |
| `test_softmax_uniform` | Equal logits produce uniform distribution |
| `test_softmax_sums_to_one` | Output probabilities sum to 1.0 |
| `test_softmax_numerical_stability` | Large logits do not produce NaN |
| `test_rmsnorm_unit_scale` | Output has approximately unit RMS |

**Tokenizer (3 tests):**

| Test | Description |
|------|-------------|
| `test_build_vocab` | Known input produces expected vocab and BOS ID |
| `test_encode_decode_roundtrip` | `decode(encode(text)) == text` |
| `test_encode_unknown_char_raises` | Unknown character raises `ValueError` |

**GPT model and end-to-end (5 tests):**

| Test | Description |
|------|-------------|
| `test_gpt_output_shape` | Forward pass returns `vocab_size` logits |
| `test_gpt_deterministic` | Same inputs produce same outputs |
| `test_training_loss_decreases` | 50 steps on tiny data shows loss decrease |
| `test_sample_produces_valid_tokens` | Generated tokens are valid vocab indices |
| `test_sample_respects_bos_termination` | Sampling stops at BOS token |

All model tests use a tiny configuration (`n_embd=4, n_head=2, block_size=4,
n_layer=1`) to keep test runtime under a few seconds.

### Usage Examples

```python
from challenges.microgpt import (
    GPTConfig, AdamConfig, SampleConfig, Scalar,
    build_vocab, encode, decode, load_dataset,
    linear, softmax, rmsnorm,
    init_state_dict, gpt, train, sample,
)

# --- Scalar autograd ---
x = Scalar(3.0)
y = Scalar(4.0)
z = x * y + x ** 2    # z.data == 21.0
z.backward()
print(x.grad)          # 10.0 (dz/dx = y + 2x = 4 + 6)
print(y.grad)          # 3.0  (dz/dy = x)

# --- Tokenizer ---
docs = ["hello", "world"]
vocab, bos = build_vocab(docs)
tokens = encode("hello", vocab, bos)
assert decode(tokens, vocab, bos) == "hello"

# --- Full training pipeline ---
docs = load_dataset("input.txt",
    url="https://raw.githubusercontent.com/karpathy/makemore/988aa59/names.txt")
config = GPTConfig(n_layer=1, n_embd=16, n_head=4)

# vocab_size is determined from data
vocab, bos = build_vocab(docs)
config = GPTConfig(n_layer=1, n_embd=16, n_head=4, vocab_size=len(vocab) + 1)

state_dict, vocab, bos = train(docs, config, AdamConfig(), num_steps=500)
names = sample(state_dict, vocab, bos, config,
               SampleConfig(temperature=0.5, num_samples=5))
for name in names:
    print(name)
```

---

## In-Memory Key-Value Store

**Module**: `src/challenges/kv_store.py`
**Tests**: `tests/test_kv_store.py`

### Problem Statement

Build an in-memory key-value store with progressive feature extensions across
four levels:

1. **Level 1 — Basic CRUD with LRU Eviction**: `get`, `put`, and `delete`
   operations with a fixed capacity and Least Recently Used eviction.
2. **Level 2 — Prefix Scan**: `scan(prefix)` returns all key-value pairs whose
   key starts with the given prefix.
3. **Level 3 — TTL (Time-to-Live)**: Entries can have an expiry time. Expired
   entries are treated as absent and lazily cleaned up on access.
4. **Level 4 — Compact/Restore**: Serialize the entire store state to a
   compressed string and restore it later, preserving LRU order and TTL
   metadata.

All keys and values are strings. Operations accept an optional `timestamp`
keyword argument (default `0.0`) to support TTL logic using logical
timestamps (floats) rather than wall-clock time.

### Architecture Overview

The implementation combines two data structures for O(1) operations:

| Component | Role |
|-----------|------|
| **Hash map** (`dict`) | O(1) key → node lookup |
| **Doubly-linked list** | O(1) insertion, removal, and reordering to track access recency |

Sentinel (dummy) head and tail nodes eliminate edge-case checks. The most
recently used entry is at the front (after head), and the least recently used
is at the back (before tail).

```
 head <-> node_A <-> node_B <-> node_C <-> tail
 (sentinel)     most recent ... least recent     (sentinel)
```

TTL metadata is stored per entry as an absolute expiry timestamp
(`timestamp + ttl`). Entries are lazily evicted — checked for expiry on each
`get`, `scan`, `delete`, and capacity check during `put`.

### API Reference

```python
from challenges.kv_store import KVStore

store = KVStore(capacity=3)
```

#### `KVStore(capacity: int)`

Create a new key-value store with the given maximum capacity.

- **capacity** — positive integer specifying the maximum number of live
  entries the store can hold. Raises `ValueError` if not positive.

#### `get(key: str, *, timestamp: float = 0) -> str | None`

Retrieve the value associated with `key`.

- Returns the value if the key exists and has not expired, and marks it as
  **most recently used**.
- Returns `None` if the key is not present or has expired.
- Expired entries are lazily removed on access.
- `timestamp` is keyword-only.

#### `put(key: str, value: str, ttl: float | None = None, *, timestamp: float = 0) -> None`

Insert or update a key-value pair.

- If the key already exists (and is not expired), its value and TTL are
  updated and it becomes the **most recently used** entry.
- If the key exists but is expired, the expired entry is removed and the
  new entry is inserted as if the key were absent.
- If `ttl` is provided, the entry expires when `timestamp >= timestamp + ttl`
  (i.e., expiry is inclusive).
- If the store is at capacity, all expired entries are purged first. If still
  at capacity, the **least recently used** live entry is evicted.
- `ttl` is positional; `timestamp` is keyword-only.

#### `delete(key: str, *, timestamp: float = 0) -> bool`

Remove the entry for `key`.

- Returns `True` if the key was present and non-expired.
- Returns `False` if the key was not present or was already expired.
  Expired entries are removed as a side effect.
- `timestamp` is keyword-only.

#### `scan(prefix: str, *, timestamp: float = 0) -> list[tuple[str, str]]`

Return all non-expired key-value pairs where the key starts with `prefix`.

- Iterates all entries in **most-recently-used-first** order, skipping (and
  lazily removing) expired ones.
- Returns a list of `(key, value)` tuples.
- `timestamp` is keyword-only.

#### `compact(*, timestamp: float = 0) -> str`

Serialize the store's state to a compressed string.

- Returns a base64-encoded, zlib-compressed JSON string containing the
  store's capacity, all non-expired entries (in MRU-first order), and their
  TTL metadata.
- Useful for snapshotting store state for backup or transfer.
- `timestamp` is keyword-only.

#### `KVStore.restore(data: str) -> KVStore` *(classmethod)*

Reconstruct a `KVStore` from a string produced by `compact()`.

- Returns a **new** `KVStore` instance with the same capacity, entries,
  LRU order, and TTL metadata as the original.
- Does not modify any existing store — creates a fresh instance.

### Complexity

| Operation | Time | Notes |
|-----------|------|-------|
| `get` | O(1) | Hash map lookup + linked list move |
| `put` | O(1) amortized | Hash map insert + linked list ops; O(n) when expired entries are purged |
| `delete` | O(1) | Hash map delete + linked list removal |
| `scan` | O(n) | Iterates all entries to check prefix match |
| `compact` | O(n) | Serializes all live entries |
| `restore` | O(n) | Deserializes and rebuilds the store |
| Space | O(capacity) | Stores up to `capacity` entries |

### Edge Cases

The test suite covers the following scenarios:

**Level 1 — Basic CRUD & LRU Eviction**

| Case | Description | Expected Behavior |
|------|-------------|-------------------|
| Basic get/put | Insert and retrieve values | Returns correct value |
| Missing key | `get` on a key not in store | Returns `None` |
| Capacity eviction | Insert beyond capacity | LRU entry is evicted |
| Access-order update | `get` updates recency | Accessed key is not evicted next |
| Overwrite existing key | `put` with existing key | Value updated, no eviction |
| Delete existing key | `delete` a present key | Returns `True`, key removed |
| Delete missing key | `delete` a non-existent key | Returns `False` |

**Level 2 — Prefix Scan**

| Case | Description | Expected Behavior |
|------|-------------|-------------------|
| Matching prefix | `scan("user:")` with matching keys | Returns matching pairs |
| No matches | `scan("zzz")` with no matching keys | Returns empty list |
| Empty prefix | `scan("")` | Returns all entries |

**Level 3 — TTL**

| Case | Description | Expected Behavior |
|------|-------------|-------------------|
| Expired entry | `get` when `timestamp >= expires_at` | Returns `None` |
| Live entry | `get` when `timestamp < expires_at` | Returns value |
| Expired in scan | `scan` skips expired entries | Only live entries returned; expired lazily removed |
| Expired frees capacity | Expired entries purged before LRU eviction | No unnecessary eviction of live entries |
| Expired overwrite | `put` on an expired key | Treated as new insert |

**Level 4 — Compact/Restore**

| Case | Description | Expected Behavior |
|------|-------------|-------------------|
| Round-trip | `compact` then `KVStore.restore` | All entries preserved |
| Capacity preserved | Restored store has same capacity | Capacity from snapshot |
| LRU order preserved | MRU order matches after restore | Eviction order unchanged |
| TTL preserved | TTL metadata survives round-trip | Expiry still applies |
| Expired excluded | Expired entries not in compact output | Clean snapshot |

### Design Decisions

1. **Logical timestamps (float)** instead of wall-clock time allow
   deterministic testing and make the TTL behavior reproducible. Callers pass
   the current timestamp explicitly as a keyword argument.

2. **Lazy + eager expiry** — expired entries are lazily removed when
   encountered during `get`, `scan`, or `delete`. During `put`, all expired
   entries are eagerly purged before checking capacity, preventing unnecessary
   eviction of live entries.

3. **String keys and values** match common key-value store conventions (e.g.,
   Redis, Memcached) and make serialization for `compact`/`restore`
   straightforward.

4. **Base64 + zlib for compact** produces a single opaque string that is safe
   to store or transmit, while keeping the snapshot compact. The JSON payload
   uses short keys (`"k"`, `"v"`, `"e"`) to minimize size.

5. **Sentinel nodes** for the doubly-linked list (same pattern as the LRU
   Cache challenge) eliminate null-pointer checks and simplify
   insertion/removal logic.

6. **`restore` is a classmethod** rather than an instance method — it returns
   a new `KVStore` with capacity recovered from the snapshot, making it
   impossible to misuse (e.g., restoring into a store with a different
   capacity).

7. **`__slots__` on `_Node`** reduces memory overhead per entry, matching the
   pattern used in the LRU Cache challenge.

### Usage Examples

```python
from challenges.kv_store import KVStore

# --- Level 1: Basic CRUD with LRU eviction ---
store = KVStore(capacity=3)
store.put("name", "Alice")
store.put("age", "30")
store.put("role", "admin")

store.get("name")       # => "Alice"
store.get("missing")    # => None

store.put("city", "Seattle")  # evicts LRU entry ("age", since "name" was accessed)
store.get("age")              # => None (evicted)

store.delete("role")    # => True
store.delete("role")    # => False (already deleted)

# --- Level 2: Prefix scan ---
store = KVStore(capacity=10)
store.put("user:1", "Alice")
store.put("user:2", "Bob")
store.put("order:1", "Widget")

# Results are in most-recently-used-first order
store.scan("user:")     # => [("user:2", "Bob"), ("user:1", "Alice")]
store.scan("order:")    # => [("order:1", "Widget")]
store.scan("missing:")  # => []

# --- Level 3: TTL support (timestamp is keyword-only) ---
store = KVStore(capacity=10)
store.put("session", "abc123", ttl=60, timestamp=100)

store.get("session", timestamp=150)  # => "abc123" (not expired yet)
store.get("session", timestamp=160)  # => None (expired: 100 + 60 = 160, inclusive)

# Scan skips expired entries
store.put("key1", "val1", ttl=10, timestamp=0)
store.put("key2", "val2", ttl=100, timestamp=0)
store.scan("key", timestamp=50)  # => [("key2", "val2")]  (key1 expired at t=10)

# --- Level 4: Compact and restore ---
store = KVStore(capacity=5)
store.put("a", "1")
store.put("b", "2")
store.put("c", "3")

snapshot = store.compact()

# restore() is a classmethod — returns a new KVStore
restored = KVStore.restore(snapshot)
restored.get("a")  # => "1"
restored.get("b")  # => "2"
restored.get("c")  # => "3"
```
