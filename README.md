# Testing

A Python project for coding challenges and systems programming, with modern tooling and CI.

## Overview

This repository contains solutions to classic coding challenges and systems
programming projects, including **Merge Intervals**, an **In-Memory NoSQL
Database**, and a **Distributed Task Queue**. The project uses modern Python tooling for a
consistent development experience:

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
│       └── nosql_db.py              # In-Memory NoSQL Database
├── task_queue/                      # Distributed Task Queue system
│   ├── __init__.py                  # Public API exports
│   ├── models.py                    # Task, Priority, RetryPolicy
│   ├── scheduler.py                 # DAG dependency manager
│   ├── queue.py                     # Thread-safe priority queue
│   ├── worker.py                    # Worker thread pool
│   ├── retry.py                     # Retry manager + dead-letter queue
│   ├── wal.py                       # Write-ahead log
│   ├── snapshot.py                  # Periodic state snapshots
│   └── broker.py                    # Central coordinator
├── tests/
│   ├── __init__.py
│   ├── test_merge_intervals.py      # Core test suite
│   ├── test_merge_intervals_extended.py  # Extended edge-case tests
│   ├── test_nosql_db.py             # NoSQL database test suite
│   └── test_task_queue/             # Task queue test suite (200+ tests)
│       ├── test_models.py
│       ├── test_scheduler.py
│       ├── test_queue.py
│       ├── test_worker.py
│       ├── test_retry.py
│       ├── test_wal.py
│       ├── test_snapshot.py
│       ├── test_broker.py
│       └── test_integration.py
├── docs/
│   ├── index.md                     # Documentation hub
│   ├── challenges.md                # Challenge write-ups
│   └── task_queue.md                # Task queue architecture & API docs
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
| `task_queue/` | Distributed task queue system (importable as `from task_queue import Broker`) |
| `tests/` | Pytest test suites; mirrors source module naming |
| `docs/` | Written documentation, challenge explanations, and system docs |
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

### Distributed Task Queue

A production-quality distributed task queue with real Python threading, priority
scheduling, DAG-based task dependencies, retry logic with dead-letter queues,
and crash recovery via write-ahead logging and snapshots.

```python
from task_queue import Broker, Priority, RetryPolicy

broker = Broker(num_workers=4, data_dir="./queue_data")
broker.start()

# Submit a task with high priority
task = broker.submit(
    name="process_order",
    handler=lambda p: f"Processed order {p['id']}",
    payload={"id": 123},
    priority=Priority.HIGH,
)

# Wait for result
result = broker.wait_for(task.id)

# Build dependency chains (DAGs)
t1 = broker.submit(name="extract", handler=extract, payload={})
t2 = broker.submit(name="transform", handler=transform, payload={},
                   dependencies=[t1.id])
t3 = broker.submit(name="load", handler=load, payload={},
                   dependencies=[t2.id])

broker.shutdown(wait=True)
```

**Features**: Thread-safe priority queue &middot; DAG dependency scheduling
with cycle detection &middot; Configurable retry with fixed/exponential backoff
&middot; Dead-letter queue (inspect, replay, purge) &middot; Write-ahead log
for durability &middot; Periodic snapshots for fast recovery &middot; Graceful
and forceful shutdown

See [docs/task_queue.md](docs/task_queue.md) for architecture overview, full
API reference, and detailed usage examples.

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
