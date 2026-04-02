"""Comprehensive tests for LRU Cache implementation.

Tests cover:
- Basic get/put operations
- Capacity enforcement and LRU eviction
- Key update behavior (existing key overwrite)
- Access recency tracking (get promotes to MRU)
- Boundary conditions (capacity=1, single element)
- Edge cases (missing keys, repeated operations)
- Ordering verification after mixed operations
- Input validation gaps (capacity <= 0)
"""

import pytest

from challenges.lru_cache import LRUCache, _Node

# ============================================================
# Fixtures
# ============================================================


@pytest.fixture
def cache2() -> LRUCache:
    """Create an LRU cache with capacity 2."""
    return LRUCache(2)


@pytest.fixture
def cache1() -> LRUCache:
    """Create an LRU cache with capacity 1."""
    return LRUCache(1)


@pytest.fixture
def cache3() -> LRUCache:
    """Create an LRU cache with capacity 3."""
    return LRUCache(3)


# ============================================================
# _Node Tests
# ============================================================


class TestNode:
    """Tests for the _Node internal class."""

    def test_default_values(self) -> None:
        node = _Node()
        assert node.key == 0
        assert node.value == 0
        assert node.prev is None
        assert node.next is None

    def test_custom_values(self) -> None:
        node = _Node(42, 99)
        assert node.key == 42
        assert node.value == 99

    def test_slots(self) -> None:
        """_Node uses __slots__ — no __dict__ allowed."""
        node = _Node()
        with pytest.raises(AttributeError):
            node.extra = "bad"  # type: ignore[attr-defined]


# ============================================================
# Basic Operations
# ============================================================


class TestBasicOperations:
    """Tests for fundamental get/put behavior."""

    def test_put_and_get_single(self, cache2: LRUCache) -> None:
        cache2.put(1, 10)
        assert cache2.get(1) == 10

    def test_get_missing_key_returns_negative_one(self, cache2: LRUCache) -> None:
        assert cache2.get(999) == -1

    def test_get_missing_key_empty_cache(self, cache2: LRUCache) -> None:
        assert cache2.get(0) == -1

    def test_put_multiple_keys(self, cache2: LRUCache) -> None:
        cache2.put(1, 10)
        cache2.put(2, 20)
        assert cache2.get(1) == 10
        assert cache2.get(2) == 20

    def test_put_updates_existing_key(self, cache2: LRUCache) -> None:
        cache2.put(1, 10)
        cache2.put(1, 100)
        assert cache2.get(1) == 100

    def test_put_update_does_not_change_size(self, cache2: LRUCache) -> None:
        """Updating an existing key should not increase the cache size."""
        cache2.put(1, 10)
        cache2.put(2, 20)
        cache2.put(1, 100)  # update, not insert
        # Key 2 should still be present (no eviction)
        assert cache2.get(2) == 20
        assert cache2.get(1) == 100


# ============================================================
# Eviction Behavior
# ============================================================


class TestEviction:
    """Tests for LRU eviction when cache is at capacity."""

    def test_evicts_lru_on_capacity(self, cache2: LRUCache) -> None:
        cache2.put(1, 1)
        cache2.put(2, 2)
        cache2.put(3, 3)  # evicts key 1 (LRU)
        assert cache2.get(1) == -1
        assert cache2.get(2) == 2
        assert cache2.get(3) == 3

    def test_get_promotes_to_mru(self, cache2: LRUCache) -> None:
        """Accessing a key via get() should make it most recently used."""
        cache2.put(1, 1)
        cache2.put(2, 2)
        cache2.get(1)  # promotes key 1 to MRU
        cache2.put(3, 3)  # evicts key 2 (now LRU)
        assert cache2.get(1) == 1
        assert cache2.get(2) == -1
        assert cache2.get(3) == 3

    def test_put_update_promotes_to_mru(self, cache2: LRUCache) -> None:
        """Updating an existing key via put() should make it MRU."""
        cache2.put(1, 1)
        cache2.put(2, 2)
        cache2.put(1, 10)  # updates key 1, promotes to MRU
        cache2.put(3, 3)  # evicts key 2 (now LRU)
        assert cache2.get(1) == 10
        assert cache2.get(2) == -1
        assert cache2.get(3) == 3

    def test_sequential_evictions(self, cache2: LRUCache) -> None:
        """Multiple sequential evictions should always remove the LRU item."""
        cache2.put(1, 1)
        cache2.put(2, 2)
        cache2.put(3, 3)  # evicts 1
        cache2.put(4, 4)  # evicts 2
        assert cache2.get(1) == -1
        assert cache2.get(2) == -1
        assert cache2.get(3) == 3
        assert cache2.get(4) == 4

    def test_eviction_after_get_reorder(self, cache3: LRUCache) -> None:
        """Complex reordering: get changes eviction order."""
        cache3.put(1, 1)
        cache3.put(2, 2)
        cache3.put(3, 3)
        cache3.get(1)  # order now: 2, 3, 1 (2 is LRU)
        cache3.put(4, 4)  # evicts 2
        assert cache3.get(2) == -1
        assert cache3.get(1) == 1
        assert cache3.get(3) == 3
        assert cache3.get(4) == 4

    def test_eviction_with_repeated_access(self, cache2: LRUCache) -> None:
        """Repeatedly accessing the same key keeps it from being evicted."""
        cache2.put(1, 1)
        cache2.put(2, 2)
        cache2.get(1)
        cache2.get(1)
        cache2.get(1)
        cache2.put(3, 3)  # evicts key 2
        assert cache2.get(1) == 1
        assert cache2.get(2) == -1


# ============================================================
# Capacity = 1 Edge Cases
# ============================================================


class TestCapacityOne:
    """Tests for edge cases with capacity=1."""

    def test_single_put_get(self, cache1: LRUCache) -> None:
        cache1.put(1, 42)
        assert cache1.get(1) == 42

    def test_evicts_on_second_put(self, cache1: LRUCache) -> None:
        cache1.put(1, 1)
        cache1.put(2, 2)
        assert cache1.get(1) == -1
        assert cache1.get(2) == 2

    def test_update_same_key_no_eviction(self, cache1: LRUCache) -> None:
        cache1.put(1, 1)
        cache1.put(1, 2)
        assert cache1.get(1) == 2

    def test_rapid_replacement(self, cache1: LRUCache) -> None:
        """Rapid successive inserts with capacity=1."""
        for i in range(100):
            cache1.put(i, i * 10)
        # Only the last key should remain
        assert cache1.get(99) == 990
        assert cache1.get(98) == -1
        assert cache1.get(0) == -1


# ============================================================
# Larger Capacity
# ============================================================


class TestLargerCapacity:
    """Tests for caches with larger capacities."""

    def test_fill_to_capacity(self) -> None:
        cache = LRUCache(5)
        for i in range(5):
            cache.put(i, i * 10)
        for i in range(5):
            assert cache.get(i) == i * 10

    def test_overfill_evicts_oldest(self) -> None:
        cache = LRUCache(5)
        for i in range(6):
            cache.put(i, i * 10)
        # Key 0 should be evicted
        assert cache.get(0) == -1
        for i in range(1, 6):
            assert cache.get(i) == i * 10

    def test_complex_access_pattern(self) -> None:
        """Complex mixed access pattern with larger cache."""
        cache = LRUCache(3)
        cache.put(1, 1)
        cache.put(2, 2)
        cache.put(3, 3)
        cache.get(2)  # order: 1, 3, 2
        cache.put(4, 4)  # evicts 1; order: 3, 2, 4
        assert cache.get(1) == -1
        cache.put(2, 22)  # update 2; order: 3, 4, 2
        cache.put(5, 5)  # evicts 3; order: 4, 2, 5
        assert cache.get(3) == -1
        assert cache.get(2) == 22
        assert cache.get(4) == 4
        assert cache.get(5) == 5


# ============================================================
# Key/Value Edge Cases
# ============================================================


class TestKeyValueEdgeCases:
    """Tests for unusual key and value inputs."""

    def test_zero_key(self, cache2: LRUCache) -> None:
        cache2.put(0, 100)
        assert cache2.get(0) == 100

    def test_negative_key(self, cache2: LRUCache) -> None:
        cache2.put(-1, 50)
        assert cache2.get(-1) == 50

    def test_negative_value(self, cache2: LRUCache) -> None:
        cache2.put(1, -99)
        assert cache2.get(1) == -99

    def test_zero_value(self, cache2: LRUCache) -> None:
        cache2.put(1, 0)
        assert cache2.get(1) == 0

    def test_large_key_value(self, cache2: LRUCache) -> None:
        cache2.put(10**9, 10**9)
        assert cache2.get(10**9) == 10**9

    def test_value_negative_one_distinguishable(self, cache2: LRUCache) -> None:
        """Value of -1 should be distinguishable from 'not found' return."""
        cache2.put(1, -1)
        # get(1) returns -1 (the stored value)
        # get(2) also returns -1 (not found)
        # Both return -1 — this is a known API ambiguity
        assert cache2.get(1) == -1
        assert cache2.get(2) == -1


# ============================================================
# Input Validation Gaps
# ============================================================


class TestInputValidation:
    """Tests exposing input validation gaps in the implementation.

    The current implementation does NOT validate capacity.
    These tests document the behavior with invalid inputs.
    """

    def test_capacity_zero_put_raises_or_fails(self) -> None:
        """Capacity=0 should ideally raise ValueError.

        Current implementation allows capacity=0 which causes an
        AssertionError on put() because _evict_lru asserts the list
        is non-empty.
        """
        cache = LRUCache(0)
        with pytest.raises(AssertionError):
            cache.put(1, 1)

    def test_capacity_negative(self) -> None:
        """Negative capacity should ideally raise ValueError.

        Current implementation allows negative capacity. The cache
        will try to evict from an empty list on first put, raising
        AssertionError.
        """
        cache = LRUCache(-1)
        with pytest.raises(AssertionError):
            cache.put(1, 1)


# ============================================================
# Internal State Consistency
# ============================================================


class TestInternalConsistency:
    """Tests verifying internal data structure consistency."""

    def test_cache_dict_size_matches_capacity(self, cache2: LRUCache) -> None:
        """Internal dict should never exceed capacity."""
        cache2.put(1, 1)
        assert len(cache2._cache) == 1
        cache2.put(2, 2)
        assert len(cache2._cache) == 2
        cache2.put(3, 3)
        assert len(cache2._cache) == 2  # still 2 after eviction

    def test_linked_list_integrity_after_operations(self, cache2: LRUCache) -> None:
        """Verify the doubly-linked list is well-formed after operations."""
        cache2.put(1, 1)
        cache2.put(2, 2)
        cache2.get(1)
        cache2.put(3, 3)  # evicts 2

        # Walk forward from head
        nodes_forward = []
        current = cache2._head.next
        while current is not cache2._tail:
            nodes_forward.append(current.key)
            current = current.next

        # Walk backward from tail
        nodes_backward = []
        current = cache2._tail.prev
        while current is not cache2._head:
            nodes_backward.append(current.key)
            current = current.prev

        assert nodes_forward == list(reversed(nodes_backward))
        assert len(nodes_forward) == len(cache2._cache)

    def test_sentinel_nodes_never_removed(self, cache2: LRUCache) -> None:
        """Head and tail sentinels should always be connected."""
        cache2.put(1, 1)
        cache2.put(2, 2)
        cache2.put(3, 3)
        cache2.put(4, 4)

        # Sentinels are still there
        assert cache2._head.next is not None
        assert cache2._tail.prev is not None
        assert cache2._head.next is not cache2._tail or len(cache2._cache) == 0


# ============================================================
# LeetCode-style Sequence Tests
# ============================================================


class TestLeetCodeSequences:
    """Test sequences matching LeetCode problem #146 examples."""

    def test_leetcode_example(self) -> None:
        """Standard LeetCode example from the problem description."""
        cache = LRUCache(2)
        cache.put(1, 1)
        cache.put(2, 2)
        assert cache.get(1) == 1
        cache.put(3, 3)  # evicts key 2
        assert cache.get(2) == -1
        cache.put(4, 4)  # evicts key 1
        assert cache.get(1) == -1
        assert cache.get(3) == 3
        assert cache.get(4) == 4

    def test_put_get_interleaved(self) -> None:
        """Interleaved put/get operations."""
        cache = LRUCache(2)
        cache.put(2, 1)
        cache.put(1, 1)
        cache.put(2, 3)
        cache.put(4, 1)  # evicts key 1
        assert cache.get(1) == -1
        assert cache.get(2) == 3

    def test_overwrite_then_evict(self) -> None:
        """Overwriting a key should not cause premature eviction."""
        cache = LRUCache(2)
        cache.put(2, 1)
        cache.put(2, 2)
        assert cache.get(2) == 2
        cache.put(1, 1)
        cache.put(4, 1)  # evicts key 2 (LRU)
        assert cache.get(2) == -1

    def test_stress_sequential(self) -> None:
        """Stress test: many sequential inserts."""
        cache = LRUCache(100)
        for i in range(1000):
            cache.put(i, i)
        # Only last 100 keys should remain
        for i in range(900):
            assert cache.get(i) == -1
        for i in range(900, 1000):
            assert cache.get(i) == i
