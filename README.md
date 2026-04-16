# Testing

A Python project for solving coding challenges with modern tooling and CI.

## Overview

This repository contains solutions to classic coding challenges, including
**Merge Intervals**, an **In-Memory NoSQL Database**, an **LRU Cache**,
**MicroGPT** (a pure-Python GPT implementation), and an **In-Memory Key-Value
Store** with LRU eviction, prefix scan, TTL, and compact/restore. The project uses modern
Python tooling for a consistent development experience:

- **[uv](https://docs.astral.sh/uv/)** &mdash; fast, reliable package management
- **[ruff](https://docs.astral.sh/ruff/)** &mdash; linting and formatting
- **[pytest](https://docs.pytest.org/)** &mdash; testing
- **GitHub Actions** &mdash; continuous integration

## Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) package manager

## Quick Start

```bash
# Install dependencies (including dev tools)
uv sync --dev

# Run the test suite
uv run pytest

# Run the linter
uv run ruff check .

# Check code formatting
uv run ruff format --check .
```

### Fixing Lint / Format Issues

```bash
# Auto-fix lint violations
uv run ruff check --fix .

# Auto-format code
uv run ruff format .
```

## Project Structure

```
testing/
├── src/
│   └── challenges/
│       ├── __init__.py
│       ├── merge_intervals.py       # Merge Intervals solution
│       ├── nosql_db.py              # In-Memory NoSQL Database
│       ├── lru_cache.py             # LRU Cache implementation
│       ├── microgpt.py              # MicroGPT (pure-Python GPT)
│       └── kv_store.py              # In-Memory Key-Value Store
├── tests/
│   ├── __init__.py
│   ├── test_merge_intervals.py      # Core test suite
│   ├── test_merge_intervals_extended.py  # Extended edge-case tests
│   ├── test_nosql_db.py             # NoSQL database test suite
│   ├── test_lru_cache.py            # LRU Cache test suite
│   ├── test_microgpt.py             # MicroGPT test suite
│   └── test_kv_store.py             # Key-Value Store test suite
├── docs/
│   ├── index.md                     # Documentation hub
│   └── challenges.md                # Challenge write-ups
├── .github/
│   └── workflows/
│       └── ci.yml                   # CI pipeline (lint + test)
├── pyproject.toml                   # Project & tool configuration
├── .python-version                  # Python 3.12
└── .gitignore
```

### Key directories

| Directory | Purpose |
|-----------|---------|
| `src/challenges/` | Challenge implementations (importable as `from challenges.<name> import ...`) |
| `tests/` | Pytest test suites; mirrors `src/challenges/` naming |
| `docs/` | Written documentation and challenge explanations |
| `.github/workflows/` | GitHub Actions CI configuration |

## Challenges

### Merge Intervals

Given a list of intervals `[start, end]`, merge all overlapping intervals and
return the result sorted by start time.

```python
from challenges.merge_intervals import merge_intervals

merge_intervals([[1, 3], [2, 6], [8, 10], [15, 18]])
# => [[1, 6], [8, 10], [15, 18]]

merge_intervals([[1, 4], [4, 5]])
# => [[1, 5]]
```

**Algorithm**: Sort by start time, then iterate &mdash; merge when the current
interval overlaps the previous one, otherwise append.

| Metric | Value | Reason |
|--------|-------|--------|
| Time | O(n log n) | Dominated by sort |
| Space | O(n) | Output list (worst case: no overlaps) |

### In-Memory NoSQL Database

A document-oriented in-memory NoSQL database with a MongoDB-style API.
Supports CRUD operations, rich queries with `$`-prefixed operators, secondary
indexes, a multi-stage aggregation pipeline, and transactions with snapshot
isolation.

```python
from challenges.nosql_db import Database

db = Database()
users = db.get_collection("users")

# Insert documents
users.insert_one({"name": "Alice", "age": 30})
users.insert_many([{"name": "Bob", "age": 25}, {"name": "Carol", "age": 35}])

# Query with operators
users.find({"age": {"$gte": 25, "$lt": 35}})

# Aggregation pipeline
users.aggregate([
    {"$group": {"_id": None, "avg_age": {"$avg": "$age"}}},
])

# Transactions with snapshot isolation
txn = db.begin_transaction()
txn.update_one("users", {"name": "Alice"}, {"$set": {"age": 31}})
txn.commit()
```

**Features**: CRUD &middot; Query operators ($eq, $gt, $lt, $in, $and, $or,
etc.) &middot; Dot-notation for nested fields &middot; Secondary indexes &middot;
Aggregation ($match, $group, $sort, $project, $unwind, etc.) &middot;
Transactions with snapshot isolation

See [docs/challenges.md](docs/challenges.md) for the full write-up including
API reference, complexity analysis, and usage examples.

### LRU Cache

An O(1) Least Recently Used cache backed by a hash map and doubly-linked list
with sentinel nodes. Supports `get` and `put` operations with automatic
eviction of the least recently used entry when capacity is exceeded.

```python
from challenges.lru_cache import LRUCache

cache = LRUCache(2)
cache.put(1, 10)
cache.put(2, 20)
cache.get(1)       # => 10
cache.put(3, 30)   # evicts key 2 (LRU)
cache.get(2)       # => -1 (evicted)
```

| Metric | Value | Reason |
|--------|-------|--------|
| Time (`get`/`put`) | O(1) | Hash map + linked list |
| Space | O(capacity) | Stores up to `capacity` entries |

See [docs/challenges.md](docs/challenges.md) for the full write-up including
architecture overview, API reference, and edge cases.

### MicroGPT

A refactored version of Andrej Karpathy's
[microgpt](https://karpathy.github.io/2026/02/12/microgpt/) — a complete GPT
implementation in pure Python with no external dependencies (beyond Pydantic
for configuration). Includes a scalar autograd engine, character-level
tokenizer, single-layer transformer, Adam optimizer, and temperature-controlled
text generation.

```python
from challenges.microgpt import (
    GPTConfig, AdamConfig, SampleConfig,
    load_dataset, train, sample,
)

docs = load_dataset("input.txt")
config = GPTConfig(n_layer=1, n_embd=16, n_head=4, vocab_size=27)
state_dict, vocab, bos = train(docs, config, AdamConfig(), num_steps=500)
names = sample(state_dict, vocab, bos, config, SampleConfig(temperature=0.5))
```

**Components**: Scalar autograd &middot; NN primitives (linear, softmax,
rmsnorm) &middot; Character tokenizer &middot; Transformer forward pass &middot;
Adam optimizer &middot; Text generation

See [docs/challenges.md](docs/challenges.md) for the full write-up including
architecture overview, API reference, autograd details, and test coverage.

### In-Memory Key-Value Store

An in-memory key-value store with progressive feature levels: basic CRUD with
LRU eviction, prefix scan, TTL (time-to-live) support with logical timestamps,
and compact/restore for state serialization.

```python
from challenges.kv_store import KVStore

store = KVStore(capacity=3)
store.put("name", "Alice")
store.put("age", "30")
store.put("role", "admin")

store.get("name")                # => "Alice"
store.put("city", "Seattle")     # evicts LRU entry
store.scan("n")                  # => [("name", "Alice")]

# TTL support with logical timestamps
store.put("session", "tok123", ttl=60, timestamp=100)
store.get("session", timestamp=150)  # => "tok123"
store.get("session", timestamp=160)  # => None (expired at t=160)

# Compact and restore (classmethod returns new KVStore)
snapshot = store.compact()
restored = KVStore.restore(snapshot)
```

**Features**: CRUD &middot; LRU eviction &middot; Prefix scan &middot;
TTL with logical timestamps &middot; Compact/restore serialization

| Metric | Value | Reason |
|--------|-------|--------|
| Time (`get`/`put`/`delete`) | O(1) | Hash map + linked list |
| Time (`scan`) | O(n) | Iterates all entries |
| Space | O(capacity) | Stores up to `capacity` entries |

See [docs/challenges.md](docs/challenges.md) for the full write-up including
architecture overview, API reference, and edge cases.

## CI

The GitHub Actions workflow (`.github/workflows/ci.yml`) runs on every push to
`main` and on pull requests targeting `main`:

1. **Lint** &mdash; `ruff check .` and `ruff format --check .`
2. **Test** &mdash; `pytest` against the full test suite

## Contributing

1. Create a branch from `main`
2. Add your solution under `src/challenges/`
3. Add tests under `tests/`
4. Ensure `uv run ruff check .` and `uv run pytest` pass
5. Submit a pull request
