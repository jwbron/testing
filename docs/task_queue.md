# Distributed Task Queue

A production-quality distributed task queue system built with Python's native
threading primitives. Supports priority scheduling, DAG-based task dependencies,
retry logic with dead-letter queues, and crash recovery via write-ahead logging
(WAL) and periodic snapshots.

## Table of Contents

- [Architecture](#architecture)
- [Getting Started](#getting-started)
- [Core Concepts](#core-concepts)
- [API Reference](#api-reference)
- [Usage Examples](#usage-examples)
- [Persistence and Recovery](#persistence-and-recovery)
- [Thread Safety](#thread-safety)
- [Configuration](#configuration)

---

## Architecture

The task queue is composed of seven cooperating components, orchestrated by a
central **Broker**:

```
                    ┌──────────────────────────────────────────┐
                    │                 Broker                    │
                    │  (central coordinator, public API)        │
                    └──┬───┬───┬───┬───┬───┬───────────────────┘
                       │   │   │   │   │   │
          ┌────────────┘   │   │   │   │   └──────────────┐
          ▼                ▼   │   ▼   ▼                  ▼
    ┌───────────┐  ┌──────────┐│┌──────────┐  ┌────────────────┐
    │ Scheduler │  │  Queue   │││  Worker  │  │ Retry Manager  │
    │  (DAG)    │  │(priority)│││  Pool    │  │   + DLQ        │
    └───────────┘  └──────────┘│└──────────┘  └────────────────┘
                               │
                    ┌──────────┴──────────┐
                    ▼                     ▼
              ┌──────────┐        ┌────────────┐
              │   WAL    │        │  Snapshot   │
              │(append-  │        │ (periodic   │
              │  only)   │        │  state)     │
              └──────────┘        └────────────┘
```

### Component Responsibilities

| Component | Module | Purpose |
|-----------|--------|---------|
| **Models** | `task_queue/models.py` | Data types: `Task`, `TaskStatus`, `Priority`, `RetryPolicy` |
| **Scheduler** | `task_queue/scheduler.py` | DAG dependency tracking, cycle detection, readiness checks, cascading failure |
| **Queue** | `task_queue/queue.py` | Thread-safe priority queue with dependency-aware dequeuing |
| **Worker Pool** | `task_queue/worker.py` | Configurable thread pool for concurrent task execution |
| **Retry Manager** | `task_queue/retry.py` | Retry policies, backoff strategies, dead-letter queue |
| **WAL** | `task_queue/wal.py` | Append-only write-ahead log for durability |
| **Snapshot** | `task_queue/snapshot.py` | Periodic full-state snapshots for fast recovery |
| **Broker** | `task_queue/broker.py` | Central coordinator wiring all components together |

### Data Flow

1. **Submit**: Client calls `broker.submit()` &rarr; task created &rarr; WAL entry written &rarr; task registered with scheduler &rarr; task enqueued if dependencies satisfied
2. **Execute**: Worker dequeues highest-priority ready task &rarr; executes handler &rarr; reports result
3. **Success**: Task marked completed &rarr; WAL entry &rarr; downstream dependents become ready &rarr; enqueued
4. **Failure**: Retry manager checks policy &rarr; re-enqueue with backoff **or** move to dead-letter queue
5. **Recovery**: Load latest snapshot &rarr; replay WAL entries after snapshot sequence &rarr; resume processing

---

## Getting Started

### Installation

The task queue module is included in this project. Install dependencies with:

```bash
uv sync --dev
```

### Quick Example

```python
from task_queue import Broker, Priority, RetryPolicy

# Create and start a broker
broker = Broker(num_workers=4, data_dir="./queue_data")
broker.start()

# Define a task handler
def process_order(payload):
    order_id = payload["order_id"]
    print(f"Processing order {order_id}")
    return {"status": "processed", "order_id": order_id}

# Submit a task
task = broker.submit(
    name="process_order",
    handler=process_order,
    payload={"order_id": 12345},
    priority=Priority.HIGH,
)

# Wait for completion
result = broker.wait_for(task.id)
print(f"Result: {result}")

# Shut down gracefully
broker.shutdown(wait=True)
```

### Running Tests

```bash
# Run the full task queue test suite
uv run pytest tests/test_task_queue/ -v

# Run tests for a specific component
uv run pytest tests/test_task_queue/test_broker.py -v
```

---

## Core Concepts

### Task States

Tasks move through a well-defined lifecycle:

```
                    ┌──────────┐
                    │ pending  │  (created, dependencies not met)
                    └────┬─────┘
                         │  dependencies satisfied
                         ▼
                    ┌──────────┐
                    │  queued  │  (in priority queue, waiting for worker)
                    └────┬─────┘
                         │  worker picks up task
                         ▼
                    ┌──────────┐
              ┌─────│ running  │─────┐
              │     └──────────┘     │
              │ success              │ failure
              ▼                      ▼
        ┌───────────┐         ┌──────────┐
        │ completed │         │  failed  │──── retries remaining?
        └───────────┘         └────┬─────┘         │
                                   │ no             │ yes: re-enqueue
                                   ▼                │ with backoff
                            ┌──────────────┐        │
                            │dead_lettered │◄───────┘
                            └──────────────┘
```

### Priority Levels

Tasks are dequeued in priority order. Within the same priority level, tasks
follow FIFO (first-in, first-out) ordering.

| Priority | Value | Use Case |
|----------|-------|----------|
| `Priority.CRITICAL` | 0 | System-critical tasks that must run immediately |
| `Priority.HIGH` | 1 | User-facing operations, time-sensitive work |
| `Priority.NORMAL` | 2 | Standard background processing (default) |
| `Priority.LOW` | 3 | Batch jobs, non-urgent maintenance |

### Task Dependencies (DAGs)

Tasks can declare dependencies on other tasks, forming a directed acyclic graph
(DAG). A task is only queued for execution after **all** of its dependencies
have completed successfully.

```python
# Task B depends on Task A; Task C depends on both A and B
task_a = broker.submit(name="extract", handler=extract_fn, payload={})
task_b = broker.submit(name="transform", handler=transform_fn, payload={},
                       dependencies=[task_a.id])
task_c = broker.submit(name="load", handler=load_fn, payload={},
                       dependencies=[task_a.id, task_b.id])
```

**Cycle detection**: The scheduler uses Kahn's algorithm at submission time to
detect circular dependencies. Submitting a task that would create a cycle raises
a `CycleDetectedError`.

**Cascading failure**: If a task fails permanently (exhausts retries and moves
to the dead-letter queue), all downstream dependents are also marked as failed.

### Retry Policies

Configure how failed tasks are retried:

```python
from task_queue import RetryPolicy

# Fixed delay: retry up to 3 times, 5 seconds apart
policy = RetryPolicy(max_retries=3, backoff_strategy="fixed", base_delay=5.0)

# Exponential backoff: retry up to 5 times, starting at 1s (1s, 2s, 4s, 8s, 16s)
policy = RetryPolicy(max_retries=5, backoff_strategy="exponential", base_delay=1.0)
```

After exhausting all retries, a task is moved to the **dead-letter queue (DLQ)**
for manual inspection and potential replay.

---

## API Reference

### `Broker`

The central coordinator that provides the public API for the task queue system.

```python
from task_queue import Broker

broker = Broker(
    num_workers=4,        # Number of worker threads
    data_dir="./data",    # Directory for WAL and snapshots (optional)
    snapshot_interval=100, # WAL entries between snapshots (optional)
)
```

#### Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `start()` | `start() -> None` | Start the worker pool and begin processing tasks |
| `shutdown()` | `shutdown(wait: bool = True, timeout: float | None = None) -> None` | Stop the broker. If `wait=True`, finish current tasks; if `timeout` set, force-stop after N seconds |
| `submit()` | `submit(name, handler, payload, priority?, dependencies?, retry_policy?) -> Task` | Submit a new task for execution |
| `submit_many()` | `submit_many(tasks: list[dict]) -> list[Task]` | Submit multiple tasks in a batch |
| `get_task()` | `get_task(task_id: str) -> Task | None` | Retrieve a task by ID |
| `get_status()` | `get_status(task_id: str) -> TaskStatus` | Get the current status of a task |
| `wait_for()` | `wait_for(task_id: str, timeout: float | None = None) -> Any` | Block until a task completes, return its result |
| `cancel()` | `cancel(task_id: str) -> bool` | Cancel a pending or queued task |
| `recover()` | `recover() -> None` | Recover state from WAL and snapshots after a crash |

#### `submit()` Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str` | required | Human-readable task name |
| `handler` | `Callable` | required | Function to execute (receives `payload` as argument) |
| `payload` | `dict` | required | Data passed to the handler function |
| `priority` | `Priority` | `Priority.NORMAL` | Task priority level |
| `dependencies` | `list[str]` | `[]` | List of task IDs that must complete first |
| `retry_policy` | `RetryPolicy | None` | `None` | Retry configuration; `None` means no retries |

### `Task`

Represents a unit of work in the queue.

| Attribute | Type | Description |
|-----------|------|-------------|
| `id` | `str` | Unique task identifier (UUID) |
| `name` | `str` | Human-readable task name |
| `payload` | `dict` | Data for the handler |
| `priority` | `Priority` | Task priority level |
| `status` | `TaskStatus` | Current lifecycle state |
| `dependencies` | `list[str]` | IDs of upstream tasks |
| `retry_count` | `int` | Number of retries attempted |
| `result` | `Any` | Handler return value (after completion) |
| `error` | `str | None` | Error message (if failed) |
| `created_at` | `float` | Creation timestamp |
| `started_at` | `float | None` | Execution start timestamp |
| `completed_at` | `float | None` | Completion timestamp |

#### Serialization

```python
# Convert to dict (for persistence)
task_dict = task.to_dict()

# Reconstruct from dict
task = Task.from_dict(task_dict)
```

### `TaskStatus`

Enum representing possible task states:

| Value | Description |
|-------|-------------|
| `TaskStatus.PENDING` | Created, waiting for dependencies |
| `TaskStatus.QUEUED` | Dependencies met, in priority queue |
| `TaskStatus.RUNNING` | Currently being executed by a worker |
| `TaskStatus.COMPLETED` | Finished successfully |
| `TaskStatus.FAILED` | Failed (may be retried or dead-lettered) |
| `TaskStatus.DEAD_LETTERED` | Exhausted retries, moved to DLQ |

### `Priority`

Enum for task priority levels (lower numeric value = higher priority):

| Value | Numeric | Description |
|-------|---------|-------------|
| `Priority.CRITICAL` | 0 | Highest priority |
| `Priority.HIGH` | 1 | High priority |
| `Priority.NORMAL` | 2 | Default priority |
| `Priority.LOW` | 3 | Lowest priority |

### `RetryPolicy`

Configuration for task retry behavior:

| Attribute | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_retries` | `int` | required | Maximum number of retry attempts |
| `backoff_strategy` | `str` | `"fixed"` | `"fixed"` or `"exponential"` |
| `base_delay` | `float` | `1.0` | Base delay in seconds between retries |

**Backoff calculation**:
- **Fixed**: delay = `base_delay` (constant)
- **Exponential**: delay = `base_delay * 2^retry_count`

### Dead-Letter Queue (DLQ)

Tasks that exhaust their retry policy are moved to the dead-letter queue.
Access the DLQ through the broker's retry manager:

```python
# Inspect dead-lettered tasks
dead_tasks = broker.retry_manager.dlq_inspect()

# Replay a specific task (re-submit to the queue)
broker.retry_manager.dlq_replay(task_id)

# Purge all dead-lettered tasks
broker.retry_manager.dlq_purge()
```

### Exceptions

| Exception | Raised When |
|-----------|-------------|
| `CycleDetectedError` | Submitting a task would create a circular dependency |
| `TaskNotFoundError` | Referencing a task ID that does not exist |
| `QueueShutdownError` | Attempting operations on a stopped broker |

---

## Usage Examples

### Basic Task Submission

```python
from task_queue import Broker, Priority

broker = Broker(num_workers=2)
broker.start()

def greet(payload):
    return f"Hello, {payload['name']}!"

task = broker.submit(
    name="greet",
    handler=greet,
    payload={"name": "World"},
    priority=Priority.NORMAL,
)

result = broker.wait_for(task.id)
print(result)  # "Hello, World!"
broker.shutdown()
```

### Task Dependencies (ETL Pipeline)

```python
from task_queue import Broker

broker = Broker(num_workers=4)
broker.start()

def extract(payload):
    return {"data": [1, 2, 3, 4, 5]}

def transform(payload):
    data = payload.get("data", [])
    return {"data": [x * 2 for x in data]}

def load(payload):
    print(f"Loading {payload}")
    return {"loaded": True}

# Build the DAG: extract -> transform -> load
t1 = broker.submit(name="extract", handler=extract, payload={})
t2 = broker.submit(name="transform", handler=transform, payload={},
                   dependencies=[t1.id])
t3 = broker.submit(name="load", handler=load, payload={},
                   dependencies=[t2.id])

# Wait for the final task
result = broker.wait_for(t3.id)
print(result)  # {"loaded": True}
broker.shutdown()
```

### Retry with Exponential Backoff

```python
from task_queue import Broker, RetryPolicy

broker = Broker(num_workers=2)
broker.start()

attempt_count = 0

def flaky_operation(payload):
    global attempt_count
    attempt_count += 1
    if attempt_count < 3:
        raise RuntimeError("Temporary failure")
    return "Success on attempt 3"

policy = RetryPolicy(max_retries=5, backoff_strategy="exponential", base_delay=0.5)

task = broker.submit(
    name="flaky_op",
    handler=flaky_operation,
    payload={},
    retry_policy=policy,
)

result = broker.wait_for(task.id)
print(result)  # "Success on attempt 3"
broker.shutdown()
```

### Crash Recovery

```python
from task_queue import Broker

# First run: process some tasks, then crash
broker = Broker(num_workers=2, data_dir="./queue_data")
broker.start()

broker.submit(name="task_1", handler=my_handler, payload={"key": "value"})
broker.submit(name="task_2", handler=my_handler, payload={"key": "value2"})

# Simulate crash (e.g., process killed)
# ...

# Second run: recover state and resume
broker = Broker(num_workers=2, data_dir="./queue_data")
broker.recover()  # Loads snapshot + replays WAL
broker.start()    # Resumes processing incomplete tasks
```

### Priority Scheduling

```python
from task_queue import Broker, Priority

broker = Broker(num_workers=1)  # Single worker to demonstrate ordering
broker.start()

results = []

def record(payload):
    results.append(payload["label"])
    return payload["label"]

# Submit in reverse priority order
broker.submit(name="low", handler=record, payload={"label": "low"},
              priority=Priority.LOW)
broker.submit(name="critical", handler=record, payload={"label": "critical"},
              priority=Priority.CRITICAL)
broker.submit(name="normal", handler=record, payload={"label": "normal"},
              priority=Priority.NORMAL)
broker.submit(name="high", handler=record, payload={"label": "high"},
              priority=Priority.HIGH)

# Wait for all tasks, then check order
broker.shutdown(wait=True)
print(results)  # ["critical", "high", "normal", "low"]
```

### Dead-Letter Queue Management

```python
from task_queue import Broker, RetryPolicy

broker = Broker(num_workers=2)
broker.start()

def always_fails(payload):
    raise RuntimeError("Permanent failure")

policy = RetryPolicy(max_retries=2, backoff_strategy="fixed", base_delay=0.1)

task = broker.submit(
    name="doomed",
    handler=always_fails,
    payload={},
    retry_policy=policy,
)

# Wait for task to exhaust retries
import time
time.sleep(2)

# Inspect the DLQ
dead_tasks = broker.retry_manager.dlq_inspect()
print(f"Dead-lettered tasks: {len(dead_tasks)}")

# Optionally replay or purge
# broker.retry_manager.dlq_replay(task.id)
# broker.retry_manager.dlq_purge()

broker.shutdown()
```

---

## Persistence and Recovery

The task queue provides crash recovery through two complementary mechanisms:

### Write-Ahead Log (WAL)

Every state transition (task created, started, completed, failed, etc.) is
appended to a durable, append-only log **before** the in-memory state is updated.
This guarantees that no acknowledged state change is lost, even on crash.

WAL entries contain:
- **Sequence number**: Monotonically increasing, used for ordering and snapshot coordination
- **Event type**: The state transition that occurred (e.g., `task_created`, `task_completed`)
- **Task state**: Full serialized task at the time of the event
- **Delimiter**: Newline-delimited JSON for simple parsing

**Partial write recovery**: If a crash occurs mid-write, the WAL detects
incomplete final entries during replay and truncates them safely.

### Snapshots

Periodic snapshots capture the full broker state (all tasks, queue contents,
DLQ) as a single JSON file. Snapshots enable fast recovery by avoiding full
WAL replay from the beginning.

Each snapshot records the WAL sequence number of the last entry it includes.
On recovery:

1. Load the latest snapshot
2. Replay only WAL entries with sequence numbers **after** the snapshot
3. Resume processing

**Atomic writes**: Snapshots are written to a temporary file first, then
atomically renamed to prevent corruption from crashes during the write.

### Recovery Workflow

```python
broker = Broker(num_workers=4, data_dir="./queue_data")

# Recover state from persistent storage
broker.recover()

# Resume normal operation
broker.start()
```

---

## Thread Safety

All components are designed for concurrent access:

- **Queue**: Uses `threading.Condition` for blocking dequeue operations and
  mutual exclusion on queue modifications
- **Scheduler**: Uses `threading.Lock` to protect the dependency graph
- **Worker Pool**: Each worker runs in its own thread; task completion callbacks
  are synchronized
- **WAL**: Append operations are serialized with a lock to prevent interleaved
  writes
- **Broker**: Coordinates all components with appropriate synchronization

### Concurrency Testing

The test suite uses deterministic synchronization primitives (`threading.Event`,
`threading.Barrier`, `threading.Condition`) rather than `time.sleep()` to avoid
flaky tests while thoroughly exercising concurrent code paths.

---

## Configuration

### Broker Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `num_workers` | `int` | `4` | Number of worker threads in the pool |
| `data_dir` | `str | None` | `None` | Directory for WAL and snapshot files; `None` disables persistence |
| `snapshot_interval` | `int` | `100` | Number of WAL entries between automatic snapshots |

### Environment

The task queue has no external dependencies beyond the Python standard library.
It uses only:

- `threading` for concurrency
- `json` for serialization
- `uuid` for task IDs
- `os` / `tempfile` for file operations
- `time` for timestamps and delays
- `enum` / `dataclasses` for data modeling
- `collections` for internal data structures

---

## Module Structure

```
task_queue/
├── __init__.py       # Public API exports
├── models.py         # Task, TaskStatus, Priority, RetryPolicy
├── scheduler.py      # DAG dependency manager
├── queue.py          # Thread-safe priority queue
├── worker.py         # Worker thread pool
├── retry.py          # Retry manager + dead-letter queue
├── wal.py            # Write-ahead log
├── snapshot.py       # Periodic state snapshots
└── broker.py         # Central coordinator
```
