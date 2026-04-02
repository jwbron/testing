# Plan: Distributed Task Queue with Threading, DAGs, and Persistence

## Summary

Build a distributed task queue from the ground up, starting with core data models, then layering on the thread-safe queue, DAG scheduler, worker pool, retry/DLQ logic, persistence (WAL + snapshots), and finally the broker that ties everything together. Each component gets its own module and dedicated test file. Documentation covers architecture, full API reference, and usage examples. The approach prioritizes correctness and thread safety — using proper synchronization primitives throughout — over raw performance.

**Risks / edge cases**: Thread safety is the primary risk — the worker pool, priority queue, and scheduler all share mutable state under concurrent access. WAL recovery must handle partial writes from crashes. Snapshot + WAL coordination requires careful sequence numbering. All threading tests must use synchronization primitives (Events, Barriers, Conditions) rather than sleeps to avoid flakiness.

## Implementation

### Phase 1: Implement

Build all components of the distributed task queue system with comprehensive tests and documentation.

**Tasks**:
1. **[task-1-1]** Create `task_queue/models.py` — Define TaskStatus enum, Priority enum, RetryPolicy dataclass, and Task dataclass with validation and dict serialization. Acceptance: All model classes importable, serialization round-trips correctly, validation rejects invalid inputs.
2. **[task-1-2]** Create `task_queue/scheduler.py` — DAG dependency manager with cycle detection (Kahn's algorithm), readiness checks, and cascading failure propagation. Thread-safe with locking. Acceptance: Cycle detection rejects circular dependencies, tasks only ready when all deps complete, cascading failure propagates.
3. **[task-1-3]** Create `task_queue/queue.py` — Thread-safe priority queue using threading.Condition, dependency-aware dequeuing, blocking get with timeout, priority ordering (critical > high > normal > low, FIFO within same priority). Acceptance: Correct priority ordering, blocking get works with timeout, dependency-aware dequeuing verified.
4. **[task-1-4]** Create `task_queue/worker.py` — Worker pool with configurable thread count, task execution with handler callbacks, graceful and forceful shutdown support. Acceptance: Workers execute tasks concurrently, graceful shutdown completes current tasks, forceful shutdown respects timeout.
5. **[task-1-5]** Create `task_queue/retry.py` — Retry manager with fixed and exponential backoff, retry count tracking, dead-letter queue with inspect/replay/purge. Acceptance: Failed tasks retry with correct backoff, dead-letter after max retries, DLQ operations work.
6. **[task-1-6]** Create `task_queue/wal.py` — Append-only write-ahead log with JSON entries, sequence numbers, replay support, and partial-write recovery via truncation. Acceptance: WAL entries persist and replay in order, partial writes detected and truncated, truncate_after works.
7. **[task-1-7]** Create `task_queue/snapshot.py` — Periodic state snapshots as JSON with sequence number coordination, atomic writes via temp-file + rename, recovery loading. Acceptance: Snapshot captures full state, recovery from snapshot + WAL replay correct, atomic writes prevent corruption.
8. **[task-1-8]** Create `task_queue/broker.py` — Central coordinator wiring queue, scheduler, workers, retry, WAL, and snapshots. Full public API: submit, get_task, wait_for, cancel, start, shutdown, recover. Acceptance: Full lifecycle works end-to-end, state transitions emit WAL entries, recovery restores state.
9. **[task-1-9]** Create `task_queue/__init__.py` — Export public API: Broker, Task, TaskStatus, Priority, RetryPolicy, and key exceptions. Acceptance: All public classes importable from task_queue.
10. **[task-1-10]** Create comprehensive test suite in tests/test_task_queue/ with 200+ test methods across test_models.py, test_scheduler.py, test_queue.py, test_worker.py, test_retry.py, test_wal.py, test_snapshot.py, test_broker.py, test_integration.py. Acceptance: All tests pass with pytest, 200+ test methods, no flaky failures.
11. **[task-1-11]** Create docs/task_queue.md with architecture overview, full API reference, usage examples, and getting-started guide. Update root README. Acceptance: Documentation covers all public APIs, includes working code examples, explains component interactions.

```yaml
# yaml-tasks
pr:
  title: "Add distributed task queue with threading, DAGs, and persistence"
  description: |
    Adds a new task_queue module implementing a distributed task queue system with real Python threading,
    priority scheduling, DAG-based task dependencies, retry logic with dead-letter queues, and crash
    recovery via write-ahead logging and snapshots. Includes 200+ tests and comprehensive documentation.
  test_plan: |
    - Automated: pytest tests/test_task_queue/ -v runs 200+ tests covering all components
    - Automated: Tests use threading synchronization primitives (Events, Barriers) for deterministic concurrency testing
    - Manual: Run the usage examples from docs/task_queue.md to verify they work as documented
    - Manual: Simulate crash recovery by killing a running broker and verifying state restores from WAL + snapshot
  manual_steps: |
    Pre-merge: Run full test suite and verify no flaky failures across 3 consecutive runs
    Post-merge: None required
phases:
  - id: 1
    name: Implement
    goal: "Build the complete distributed task queue system with all components, tests, and documentation"
    tasks:
      - id: task-1-1
        description: "Create `task_queue/models.py` — Define TaskStatus enum, Priority enum, RetryPolicy dataclass, and Task dataclass with validation and dict serialization"
        acceptance: "All model classes importable, serialization round-trips correctly, validation rejects invalid inputs"
        files:
          - task_queue/models.py
      - id: task-1-2
        description: "Create `task_queue/scheduler.py` — DAG dependency manager with cycle detection (Kahn's algorithm), readiness checks, and cascading failure propagation. Thread-safe with locking"
        acceptance: "Cycle detection rejects circular dependencies, tasks only ready when all deps complete, cascading failure propagates"
        files:
          - task_queue/scheduler.py
      - id: task-1-3
        description: "Create `task_queue/queue.py` — Thread-safe priority queue using threading.Condition, dependency-aware dequeuing, blocking get with timeout, priority ordering (critical > high > normal > low, FIFO within same priority)"
        acceptance: "Correct priority ordering, blocking get works with timeout, dependency-aware dequeuing verified"
        files:
          - task_queue/queue.py
      - id: task-1-4
        description: "Create `task_queue/worker.py` — Worker pool with configurable thread count, task execution with handler callbacks, graceful and forceful shutdown support"
        acceptance: "Workers execute tasks concurrently, graceful shutdown completes current tasks, forceful shutdown respects timeout"
        files:
          - task_queue/worker.py
      - id: task-1-5
        description: "Create `task_queue/retry.py` — Retry manager with fixed and exponential backoff, retry count tracking, dead-letter queue with inspect/replay/purge"
        acceptance: "Failed tasks retry with correct backoff, dead-letter after max retries, DLQ operations work"
        files:
          - task_queue/retry.py
      - id: task-1-6
        description: "Create `task_queue/wal.py` — Append-only write-ahead log with JSON entries, sequence numbers, replay support, and partial-write recovery via truncation"
        acceptance: "WAL entries persist and replay in order, partial writes detected and truncated, truncate_after works"
        files:
          - task_queue/wal.py
      - id: task-1-7
        description: "Create `task_queue/snapshot.py` — Periodic state snapshots as JSON with sequence number coordination, atomic writes via temp-file + rename, recovery loading"
        acceptance: "Snapshot captures full state, recovery from snapshot + WAL replay correct, atomic writes prevent corruption"
        files:
          - task_queue/snapshot.py
      - id: task-1-8
        description: "Create `task_queue/broker.py` — Central coordinator wiring queue, scheduler, workers, retry, WAL, and snapshots. Full public API: submit, get_task, wait_for, cancel, start, shutdown, recover"
        acceptance: "Full lifecycle works end-to-end, state transitions emit WAL entries, recovery restores state"
        files:
          - task_queue/broker.py
      - id: task-1-9
        description: "Create `task_queue/__init__.py` — Export public API: Broker, Task, TaskStatus, Priority, RetryPolicy, and key exceptions"
        acceptance: "All public classes importable from task_queue"
        files:
          - task_queue/__init__.py
      - id: task-1-10
        description: "Create comprehensive test suite in tests/test_task_queue/ with 200+ test methods across test_models.py, test_scheduler.py, test_queue.py, test_worker.py, test_retry.py, test_wal.py, test_snapshot.py, test_broker.py, test_integration.py"
        acceptance: "All tests pass with pytest, 200+ test methods, no flaky failures, uses Events/Barriers for concurrency tests"
        files:
          - tests/test_task_queue/test_models.py
          - tests/test_task_queue/test_scheduler.py
          - tests/test_task_queue/test_queue.py
          - tests/test_task_queue/test_worker.py
          - tests/test_task_queue/test_retry.py
          - tests/test_task_queue/test_wal.py
          - tests/test_task_queue/test_snapshot.py
          - tests/test_task_queue/test_broker.py
          - tests/test_task_queue/test_integration.py
      - id: task-1-11
        description: "Create docs/task_queue.md with architecture overview, full API reference, usage examples, and getting-started guide. Update root README"
        acceptance: "Documentation covers all public APIs, includes working code examples, explains component interactions"
        files:
          - docs/task_queue.md
          - README.md
```