### Task Analysis

**Problem statement**: Implement a new standalone coding challenge — an in-memory key-value store with 4 progressive levels: basic CRUD with LRU eviction, prefix scanning, TTL with logical timestamps, and serialization/deserialization.

**System context**: This is a coding challenges repo. Each challenge lives as a module in `src/challenges/` with corresponding tests in `tests/`. The project uses Python 3.12+, pytest, ruff, and mypy (strict mode). The new module should follow the same conventions as existing challenges (e.g., `lru_cache.py`, `nosql_db.py`).

**Files affected**:
- `src/challenges/kv_store.py` — new module with the full KV store implementation (all 4 levels)
- `tests/test_kv_store.py` — comprehensive tests covering all levels

**Scope (4 levels)**:
1. `get(key)`, `put(key, value)`, `delete(key)` with fixed capacity N and LRU eviction
2. `scan(prefix)` returning all KV pairs where key starts with prefix
3. `put(key, value, ttl)` with logical timestamps passed to each operation; expired keys not returned
4. `compact()` serializes store to compressed string; `restore(data)` reconstructs it

**Risks / edge cases**: TTL with logical timestamps (Level 3) requires all operations to accept a timestamp parameter. Lazy vs eager expiration — lazy expiration on access, but capacity checks should not count expired entries. `scan(prefix)` is O(n). `compact()`/`restore()` must preserve LRU order and TTL metadata.

**Important**: This is a NEW standalone implementation. Do NOT reuse or extend the existing `lru_cache.py` or `nosql_db.py` modules.