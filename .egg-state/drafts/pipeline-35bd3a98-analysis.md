### Task Analysis

**Problem statement**: Build a distributed task queue system in Python with real threading, priority scheduling, task dependency DAGs, retry logic with dead-letter queues, and crash recovery via WAL + snapshots. This should be a significant step up from the ~900 LOC NoSQL DB — targeting ~1500-2500 LOC with comparable test coverage.

**Quality bar**:
- Tests: Target 200+ test methods covering happy paths, edge cases, error conditions, concurrency races, and crash recovery scenarios. Tests should use deterministic helpers (latches, barriers) to exercise thread safety without flaky timing dependencies.
- Documentation: Full API reference with usage examples, architecture overview explaining how components interact, and a getting-started guide.

**System context**: New module in the testing repo alongside nosql_db. Same patterns: pure Python stdlib, pytest, ruff, uv.

**Core feature set**:
1. Task model — IDs, payloads, priorities, status lifecycle (pending → queued → running → completed/failed/dead_lettered), timestamps, dependency edges
2. Priority queue — Thread-safe, dependency-aware dequeuing
3. Worker pool — Configurable thread pool, graceful shutdown with drain/timeout
4. DAG scheduler — Dependency resolution, cycle detection (Kahn's algorithm), cascading failure
5. Retry & dead-letter — Configurable retry count, backoff strategies (fixed, exponential), DLQ with inspect/replay/purge
6. WAL — Append-only state transition log, partial-write truncation on recovery
7. Snapshots — Periodic full-state serialization, recovery = snapshot + WAL replay
8. Broker — Coordinator tying all components together

**Files affected**:
- task_queue/__init__.py — public API exports
- task_queue/models.py — Task, TaskStatus, Priority, RetryPolicy dataclasses
- task_queue/queue.py — Thread-safe priority queue with dependency awareness
- task_queue/worker.py — Worker pool management
- task_queue/scheduler.py — DAG dependency resolution and cycle detection
- task_queue/retry.py — Retry logic and dead-letter queue
- task_queue/wal.py — Write-ahead log implementation
- task_queue/snapshot.py — Snapshot serialization/deserialization
- task_queue/broker.py — Main coordinator class
- tests/test_task_queue/ — Test suite split across multiple files by component
- docs/task_queue.md — API reference + architecture docs

**Risks / edge cases**:
- Thread synchronization: race conditions between workers completing tasks and the scheduler releasing dependent tasks
- WAL replay ordering: must handle incomplete entries (crash mid-write) by truncating partial records
- Snapshot consistency: must be taken atomically relative to WAL sequence numbers
- DAG cycles: detect at submission time
- Graceful shutdown: workers must finish current tasks before exiting, with configurable timeout
- Test determinism: threading tests need synchronization primitives (Events, Barriers), not sleep-based timing