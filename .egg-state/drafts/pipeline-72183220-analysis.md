### Task Analysis

**Problem statement**: Add a new coding challenge to the repo — an LRU Cache implementation with `get`, `put`, capacity eviction, and O(1) operations using a doubly-linked list + hash map.

**System context**: Challenges live in `src/challenges/` with corresponding tests in `tests/`. The existing challenges range from a simple algorithm (`merge_intervals.py`) to a complex data structure (`nosql_db.py`). An LRU Cache sits nicely in the middle.

**Files affected**:
- `src/challenges/lru_cache.py` — new module with the LRU cache implementation
- `tests/test_lru_cache.py` — comprehensive tests

**Risks / edge cases**: None — this is a new standalone module with no impact on existing code.