# Plan: LRU Cache Coding Challenge

## Summary

Implement an LRU (Least Recently Used) Cache as a new coding challenge. The cache supports O(1) `get` and `put` operations using a doubly-linked list for recency tracking and a hash map for key lookup. When capacity is exceeded, the least recently used entry is evicted. This is a classic data structure challenge that complements the existing merge_intervals (algorithm) and nosql_db (complex system) challenges.

**Risks / edge cases**: None — standalone new module with no impact on existing code.

## Implementation

### Phase 1: Implement

Add the LRU Cache module and comprehensive tests.

**Tasks**:
1. **[task-1-1]** Create `src/challenges/lru_cache.py` with an `LRUCache` class that uses a doubly-linked list + dict for O(1) `get`/`put` with capacity-based eviction. The class should have `__init__(capacity)`, `get(key) -> int`, and `put(key, value) -> None` methods. Acceptance: get returns value and marks as recently used, or -1 if not found; put inserts/updates and evicts LRU when at capacity.
2. **[task-1-2]** Create `tests/test_lru_cache.py` with tests covering: basic get/put, capacity eviction order, access-order updates after get, edge cases (capacity 1, missing keys, overwrite existing key value). Acceptance: all tests pass and cover the core behaviors.

```yaml
# yaml-tasks
pr:
  title: "Add LRU Cache coding challenge"
  description: |
    Adds a new coding challenge implementing an LRU Cache with O(1) get/put
    operations using a doubly-linked list and hash map, with comprehensive tests.
  test_plan: |
    - Automated: pytest tests/test_lru_cache.py covers get, put, eviction, access order, edge cases
    - Manual: Review that implementation uses O(1) data structures (linked list + dict)
  manual_steps: |
    Pre-merge: None
    Post-merge: None
phases:
  - id: 1
    name: Implement
    goal: "Add LRU Cache implementation and tests"
    tasks:
      - id: task-1-1
        description: "Create `src/challenges/lru_cache.py` with an `LRUCache` class using a doubly-linked list + dict. Methods: `__init__(capacity: int)`, `get(key: int) -> int` (returns value or -1), `put(key: int, value: int) -> None` (evicts LRU entry when at capacity). Use sentinel head/tail nodes for the doubly-linked list to simplify edge cases."
        acceptance: "get() returns correct value and updates recency; put() inserts, updates, and evicts LRU when at capacity; all operations are O(1)"
        files:
          - src/challenges/lru_cache.py
      - id: task-1-2
        description: "Create `tests/test_lru_cache.py` with tests for: basic get/put, capacity eviction (verify LRU item is evicted), access-order update after get, capacity-1 cache, get on missing key returns -1, overwrite existing key updates value without eviction"
        acceptance: "All tests pass and cover the documented behaviors"
        files:
          - tests/test_lru_cache.py
```