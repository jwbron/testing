# Plan: In-Memory Key-Value Store with Progressive Extensions

## Summary

Implement a new `KVStore` class in `src/challenges/kv_store.py` that progressively adds features across 4 levels. The store uses a hash map for O(1) key lookup and a doubly-linked list for LRU tracking. All operations accept an optional `timestamp` parameter (default 0) to support Level 3's TTL without breaking earlier levels. Expired entries are lazily evicted on access. `compact()`/`restore()` use JSON + zlib for a compressed string representation.

**Risks / edge cases**: `scan(prefix)` is O(n) over all keys — acceptable for a challenge. Lazy TTL expiration means capacity checks could count expired-but-not-yet-evicted entries; the implementation should eagerly check expiry during eviction decisions. `compact()`/`restore()` must preserve LRU order and TTL metadata.

## Implementation

### Phase 1: Implement

Create a new `KVStore` class with all 4 levels of functionality plus comprehensive tests.

**Tasks**:
1. **[task-1-1]** Create `src/challenges/kv_store.py` with `KVStore(capacity)` class — `get(key, timestamp=0)`, `put(key, value, ttl=None, timestamp=0)`, `delete(key, timestamp=0)` using a dict + doubly-linked list for O(1) LRU eviction. Keys and values are strings. Acceptance: basic get/put/delete work correctly with LRU eviction at capacity
2. **[task-1-2]** Add `scan(prefix, timestamp=0)` that iterates all entries, skips expired ones, and returns a list of `(key, value)` tuples where key starts with `prefix`. Acceptance: scan returns correct key-value pairs matching the prefix
3. **[task-1-3]** Implement TTL logic — store absolute expiry (`timestamp + ttl`) per entry. `get`, `scan`, and capacity checks treat expired entries as absent. Expired entries are lazily cleaned up on access. Acceptance: expired entries are not returned by get or scan and are evicted properly
4. **[task-1-4]** Add `compact(timestamp=0)` returning a base64-encoded zlib-compressed JSON string of all non-expired entries (with LRU order and TTL metadata), and `restore(data)` that reconstructs the store from that string. Acceptance: compact/restore round-trip preserves all store state including LRU order and TTL metadata
5. **[task-1-5]** Create `tests/test_kv_store.py` with comprehensive tests for all 4 levels — basic CRUD, eviction order, prefix scan, TTL expiry with logical timestamps, and compact/restore round-trips. Acceptance: all tests pass, covering happy paths and edge cases for each level

```yaml
# yaml-tasks
pr:
  title: "Add in-memory key-value store challenge with 4 levels"
  description: |
    Implements a new coding challenge: an in-memory key-value store with
    progressive extensions (LRU eviction, prefix scan, TTL, compact/restore).
  test_plan: |
    - Automated: pytest tests in tests/test_kv_store.py cover all 4 levels
    - Manual: verify ruff and mypy pass with strict mode
  manual_steps: |
    Pre-merge: run `uv run pytest tests/test_kv_store.py` and `uv run mypy src/challenges/kv_store.py`
    Post-merge: none
phases:
  - id: 1
    name: Implement
    goal: "Create KVStore class with all 4 levels of functionality plus tests"
    tasks:
      - id: task-1-1
        description: "Create `src/challenges/kv_store.py` with `KVStore(capacity)` — `get(key, timestamp=0)`, `put(key, value, ttl=None, timestamp=0)`, `delete(key, timestamp=0)` using dict + doubly-linked list for O(1) LRU eviction. Keys and values are strings."
        acceptance: "Basic get/put/delete work correctly with LRU eviction at capacity"
        files:
          - src/challenges/kv_store.py
      - id: task-1-2
        description: "Add `scan(prefix, timestamp=0)` that iterates all entries, skips expired ones, and returns a list of `(key, value)` tuples where key starts with `prefix`."
        acceptance: "scan returns correct key-value pairs matching the prefix, skipping expired entries"
        files:
          - src/challenges/kv_store.py
      - id: task-1-3
        description: "Implement TTL logic — store absolute expiry (`timestamp + ttl`) per entry. `get`, `scan`, and capacity checks treat expired entries as absent. Expired entries are lazily cleaned up on access."
        acceptance: "Expired entries (based on logical timestamp) are not returned by get or scan and are evicted properly"
        files:
          - src/challenges/kv_store.py
      - id: task-1-4
        description: "Add `compact(timestamp=0)` returning a base64-encoded zlib-compressed JSON string of all non-expired entries (with LRU order and TTL metadata), and `restore(data)` that reconstructs the store from that string."
        acceptance: "compact/restore round-trip preserves all store state including LRU order and TTL metadata"
        files:
          - src/challenges/kv_store.py
      - id: task-1-5
        description: "Create `tests/test_kv_store.py` with comprehensive tests for all 4 levels — basic CRUD, eviction order, prefix scan, TTL expiry with logical timestamps, and compact/restore round-trips."
        acceptance: "All tests pass covering happy paths and edge cases for each level"
        files:
          - tests/test_kv_store.py
```