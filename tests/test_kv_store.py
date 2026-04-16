"""Comprehensive tests for the In-Memory Key-Value Store.

Tests cover all 4 levels of the KV store challenge:
- Level 1: Basic CRUD (get, put, delete) with LRU eviction
- Level 2: Prefix scan
- Level 3: TTL (time-to-live) with logical timestamps
- Level 4: Compact/restore serialization round-trips

Edge cases tested:
- Capacity boundaries (capacity=1, fill-to-capacity, overfill)
- Empty store operations
- LRU eviction ordering after get/put/delete
- Expired entry handling in get, scan, and eviction
- Compact/restore preserves LRU order and TTL metadata
- Input validation and error handling
"""

import base64
import json
import zlib

import pytest

from challenges.kv_store import KVStore

# ============================================================
# Fixtures
# ============================================================


@pytest.fixture
def store1() -> KVStore:
    """KV store with capacity 1."""
    return KVStore(1)


@pytest.fixture
def store2() -> KVStore:
    """KV store with capacity 2."""
    return KVStore(2)


@pytest.fixture
def store3() -> KVStore:
    """KV store with capacity 3."""
    return KVStore(3)


@pytest.fixture
def store5() -> KVStore:
    """KV store with capacity 5."""
    return KVStore(5)


# ============================================================
# Input Validation
# ============================================================


class TestInputValidation:
    """Tests for input validation on construction."""

    def test_capacity_zero_raises(self) -> None:
        with pytest.raises(ValueError, match="positive"):
            KVStore(0)

    def test_capacity_negative_raises(self) -> None:
        with pytest.raises(ValueError, match="positive"):
            KVStore(-1)

    def test_capacity_negative_large_raises(self) -> None:
        with pytest.raises(ValueError, match="positive"):
            KVStore(-100)


# ============================================================
# Level 1: Basic CRUD Operations
# ============================================================


class TestBasicPutGet:
    """Tests for basic put and get operations."""

    def test_put_and_get_single(self, store2: KVStore) -> None:
        store2.put("a", "1")
        assert store2.get("a") == "1"

    def test_get_missing_key_returns_none(self, store2: KVStore) -> None:
        assert store2.get("nonexistent") is None

    def test_get_on_empty_store(self, store2: KVStore) -> None:
        assert store2.get("anything") is None

    def test_put_multiple_keys(self, store3: KVStore) -> None:
        store3.put("a", "1")
        store3.put("b", "2")
        store3.put("c", "3")
        assert store3.get("a") == "1"
        assert store3.get("b") == "2"
        assert store3.get("c") == "3"

    def test_put_overwrites_existing_key(self, store2: KVStore) -> None:
        store2.put("a", "old")
        store2.put("a", "new")
        assert store2.get("a") == "new"

    def test_put_overwrite_does_not_increase_size(self, store2: KVStore) -> None:
        """Updating an existing key should not trigger eviction."""
        store2.put("a", "1")
        store2.put("b", "2")
        store2.put("a", "updated")  # update, not new insert
        assert store2.get("b") == "2"  # b should still be present
        assert store2.get("a") == "updated"

    def test_put_empty_string_key(self, store2: KVStore) -> None:
        store2.put("", "empty_key")
        assert store2.get("") == "empty_key"

    def test_put_empty_string_value(self, store2: KVStore) -> None:
        store2.put("a", "")
        assert store2.get("a") == ""

    def test_put_overwrite_with_empty_value(self, store2: KVStore) -> None:
        store2.put("a", "nonempty")
        store2.put("a", "")
        assert store2.get("a") == ""


class TestDelete:
    """Tests for delete operation."""

    def test_delete_existing_key(self, store2: KVStore) -> None:
        store2.put("a", "1")
        store2.delete("a")
        assert store2.get("a") is None

    def test_delete_missing_key_no_error(self, store2: KVStore) -> None:
        """Deleting a non-existent key should not raise."""
        store2.delete("nonexistent")  # should not raise

    def test_delete_on_empty_store(self, store2: KVStore) -> None:
        """Deleting from empty store should not raise."""
        store2.delete("anything")  # should not raise

    def test_delete_frees_capacity(self, store2: KVStore) -> None:
        """Deleting should free a slot, preventing eviction on next put."""
        store2.put("a", "1")
        store2.put("b", "2")
        store2.delete("a")
        store2.put("c", "3")
        assert store2.get("a") is None
        assert store2.get("b") == "2"
        assert store2.get("c") == "3"

    def test_delete_then_reinsert(self, store2: KVStore) -> None:
        store2.put("a", "1")
        store2.delete("a")
        store2.put("a", "2")
        assert store2.get("a") == "2"

    def test_delete_returns_true_for_existing(self, store2: KVStore) -> None:
        """Delete should return True when key exists and is not expired."""
        store2.put("a", "1")
        assert store2.delete("a") is True

    def test_delete_returns_false_for_missing(self, store2: KVStore) -> None:
        """Delete should return False when key does not exist."""
        assert store2.delete("nonexistent") is False

    def test_delete_returns_false_for_expired(self, store2: KVStore) -> None:
        """Delete should return False when key is expired."""
        store2.put("a", "1", ttl=5, timestamp=0)
        assert store2.delete("a", timestamp=10) is False

    def test_delete_middle_key_preserves_lru_order(self, store3: KVStore) -> None:
        """Deleting a key should not affect eviction order of remaining keys."""
        store3.put("a", "1")
        store3.put("b", "2")
        store3.put("c", "3")
        store3.delete("b")
        # Now: capacity 3, has a and c (2 entries). a is LRU.
        store3.put("d", "4")
        store3.put("e", "5")
        # After adding d and e: a(LRU), c, d, e — a should be evicted first
        assert store3.get("a") is None
        assert store3.get("c") == "3"
        assert store3.get("d") == "4"
        assert store3.get("e") == "5"


# ============================================================
# Level 1: LRU Eviction
# ============================================================


class TestLRUEviction:
    """Tests for LRU eviction behavior."""

    def test_evicts_lru_on_capacity(self, store2: KVStore) -> None:
        store2.put("a", "1")
        store2.put("b", "2")
        store2.put("c", "3")  # evicts "a" (LRU)
        assert store2.get("a") is None
        assert store2.get("b") == "2"
        assert store2.get("c") == "3"

    def test_get_promotes_to_mru(self, store2: KVStore) -> None:
        """Accessing a key via get() makes it most recently used."""
        store2.put("a", "1")
        store2.put("b", "2")
        store2.get("a")  # promotes "a" to MRU
        store2.put("c", "3")  # evicts "b" (now LRU)
        assert store2.get("a") == "1"
        assert store2.get("b") is None

    def test_put_update_promotes_to_mru(self, store2: KVStore) -> None:
        """Updating a key via put() makes it most recently used."""
        store2.put("a", "1")
        store2.put("b", "2")
        store2.put("a", "updated")  # promotes "a" to MRU
        store2.put("c", "3")  # evicts "b" (now LRU)
        assert store2.get("a") == "updated"
        assert store2.get("b") is None

    def test_sequential_evictions(self, store2: KVStore) -> None:
        store2.put("a", "1")
        store2.put("b", "2")
        store2.put("c", "3")  # evicts a
        store2.put("d", "4")  # evicts b
        assert store2.get("a") is None
        assert store2.get("b") is None
        assert store2.get("c") == "3"
        assert store2.get("d") == "4"

    def test_get_missing_key_does_not_affect_lru(self, store2: KVStore) -> None:
        """Getting a non-existent key should not change eviction order."""
        store2.put("a", "1")
        store2.put("b", "2")
        store2.get("nonexistent")  # should not affect order
        store2.put("c", "3")  # evicts "a" (still LRU)
        assert store2.get("a") is None
        assert store2.get("b") == "2"

    def test_eviction_complex_reorder(self, store3: KVStore) -> None:
        """Complex reordering via mixed get/put."""
        store3.put("a", "1")
        store3.put("b", "2")
        store3.put("c", "3")
        store3.get("a")  # order: b(LRU), c, a(MRU)
        store3.put("d", "4")  # evicts "b"
        assert store3.get("b") is None
        assert store3.get("a") == "1"
        assert store3.get("c") == "3"
        assert store3.get("d") == "4"

    def test_repeated_access_prevents_eviction(self, store2: KVStore) -> None:
        store2.put("a", "1")
        store2.put("b", "2")
        store2.get("a")
        store2.get("a")
        store2.get("a")
        store2.put("c", "3")  # evicts "b"
        assert store2.get("a") == "1"
        assert store2.get("b") is None


class TestCapacityOne:
    """Edge cases with capacity=1."""

    def test_single_put_get(self, store1: KVStore) -> None:
        store1.put("a", "1")
        assert store1.get("a") == "1"

    def test_evicts_on_second_put(self, store1: KVStore) -> None:
        store1.put("a", "1")
        store1.put("b", "2")
        assert store1.get("a") is None
        assert store1.get("b") == "2"

    def test_update_same_key_no_eviction(self, store1: KVStore) -> None:
        store1.put("a", "1")
        store1.put("a", "2")
        assert store1.get("a") == "2"

    def test_rapid_replacement(self, store1: KVStore) -> None:
        for i in range(100):
            store1.put(str(i), str(i * 10))
        assert store1.get("99") == "990"
        assert store1.get("98") is None
        assert store1.get("0") is None

    def test_delete_then_put(self, store1: KVStore) -> None:
        store1.put("a", "1")
        store1.delete("a")
        assert store1.get("a") is None
        store1.put("b", "2")
        assert store1.get("b") == "2"


class TestLargerCapacity:
    """Tests with larger capacity stores."""

    def test_fill_to_capacity(self) -> None:
        store = KVStore(5)
        for i in range(5):
            store.put(str(i), str(i * 10))
        for i in range(5):
            assert store.get(str(i)) == str(i * 10)

    def test_overfill_evicts_oldest(self) -> None:
        store = KVStore(5)
        for i in range(6):
            store.put(str(i), str(i * 10))
        assert store.get("0") is None
        for i in range(1, 6):
            assert store.get(str(i)) == str(i * 10)

    def test_stress_sequential(self) -> None:
        store = KVStore(100)
        for i in range(1000):
            store.put(str(i), str(i))
        for i in range(900):
            assert store.get(str(i)) is None
        for i in range(900, 1000):
            assert store.get(str(i)) == str(i)


# ============================================================
# Level 2: Prefix Scan
# ============================================================


class TestScan:
    """Tests for scan(prefix) operation."""

    def test_scan_matching_prefix(self, store5: KVStore) -> None:
        store5.put("user:1", "alice")
        store5.put("user:2", "bob")
        store5.put("post:1", "hello")
        result = store5.scan("user:")
        assert sorted(result) == [("user:1", "alice"), ("user:2", "bob")]

    def test_scan_no_match(self, store5: KVStore) -> None:
        store5.put("user:1", "alice")
        store5.put("user:2", "bob")
        result = store5.scan("post:")
        assert result == []

    def test_scan_empty_prefix_returns_all(self, store3: KVStore) -> None:
        """Empty prefix should match all keys."""
        store3.put("a", "1")
        store3.put("b", "2")
        store3.put("c", "3")
        result = store3.scan("")
        assert sorted(result) == [("a", "1"), ("b", "2"), ("c", "3")]

    def test_scan_empty_store(self, store2: KVStore) -> None:
        result = store2.scan("anything")
        assert result == []

    def test_scan_exact_key_match(self, store3: KVStore) -> None:
        """Prefix that exactly matches a key should include it."""
        store3.put("abc", "1")
        store3.put("abcdef", "2")
        store3.put("ab", "3")
        result = store3.scan("abc")
        assert sorted(result) == [("abc", "1"), ("abcdef", "2")]

    def test_scan_single_char_prefix(self, store5: KVStore) -> None:
        store5.put("apple", "1")
        store5.put("apricot", "2")
        store5.put("banana", "3")
        store5.put("avocado", "4")
        result = store5.scan("a")
        assert sorted(result) == [
            ("apple", "1"),
            ("apricot", "2"),
            ("avocado", "4"),
        ]

    def test_scan_after_eviction(self, store2: KVStore) -> None:
        """Scan should not return evicted entries."""
        store2.put("a:1", "v1")
        store2.put("a:2", "v2")
        store2.put("a:3", "v3")  # evicts "a:1"
        result = store2.scan("a:")
        assert sorted(result) == [("a:2", "v2"), ("a:3", "v3")]

    def test_scan_after_delete(self, store3: KVStore) -> None:
        """Scan should not return deleted entries."""
        store3.put("key:1", "v1")
        store3.put("key:2", "v2")
        store3.put("key:3", "v3")
        store3.delete("key:2")
        result = store3.scan("key:")
        assert sorted(result) == [("key:1", "v1"), ("key:3", "v3")]

    def test_scan_prefix_no_partial_key_match(self, store3: KVStore) -> None:
        """Scan should only match prefix, not substring."""
        store3.put("abc", "1")
        store3.put("xabc", "2")
        store3.put("ab", "3")
        result = store3.scan("abc")
        # Only "abc" matches, not "xabc" (prefix match, not substring)
        assert ("abc", "1") in result
        assert ("xabc", "2") not in result


# ============================================================
# Level 3: TTL (Time-to-Live)
# ============================================================


class TestTTL:
    """Tests for TTL with logical timestamps."""

    def test_put_with_ttl_get_before_expiry(self, store2: KVStore) -> None:
        store2.put("a", "1", ttl=10, timestamp=0)
        assert store2.get("a", timestamp=5) == "1"

    def test_put_with_ttl_get_at_expiry(self, store2: KVStore) -> None:
        """Entry should be expired at exactly timestamp + ttl."""
        store2.put("a", "1", ttl=10, timestamp=0)
        assert store2.get("a", timestamp=10) is None

    def test_put_with_ttl_get_after_expiry(self, store2: KVStore) -> None:
        store2.put("a", "1", ttl=10, timestamp=0)
        assert store2.get("a", timestamp=15) is None

    def test_put_without_ttl_never_expires(self, store2: KVStore) -> None:
        """Entries without TTL should never expire."""
        store2.put("a", "1")
        assert store2.get("a", timestamp=999999) == "1"

    def test_ttl_with_nonzero_timestamp(self, store2: KVStore) -> None:
        """TTL should be relative to the put timestamp."""
        store2.put("a", "1", ttl=10, timestamp=100)
        assert store2.get("a", timestamp=105) == "1"
        assert store2.get("a", timestamp=110) is None

    def test_delete_expired_on_get(self, store2: KVStore) -> None:
        """Expired entry should be lazily cleaned on get access."""
        store2.put("a", "1", ttl=5, timestamp=0)
        # After expiry, get should return None
        assert store2.get("a", timestamp=10) is None

    def test_expired_entry_does_not_block_capacity(self, store2: KVStore) -> None:
        """Expired entries should not count toward capacity."""
        store2.put("a", "1", ttl=5, timestamp=0)
        store2.put("b", "2", ttl=5, timestamp=0)
        # Both entries are now expired at timestamp=10
        # Putting two new entries should work without evicting
        store2.put("c", "3", timestamp=10)
        store2.put("d", "4", timestamp=10)
        assert store2.get("c", timestamp=10) == "3"
        assert store2.get("d", timestamp=10) == "4"

    def test_scan_skips_expired_entries(self, store5: KVStore) -> None:
        """Scan should not return expired entries."""
        store5.put("user:1", "alice", ttl=10, timestamp=0)
        store5.put("user:2", "bob", ttl=20, timestamp=0)
        store5.put("user:3", "carol", timestamp=0)  # no TTL
        result = store5.scan("user:", timestamp=15)
        assert ("user:1", "alice") not in result
        assert ("user:2", "bob") in result
        assert ("user:3", "carol") in result

    def test_ttl_zero_expires_immediately(self, store2: KVStore) -> None:
        """TTL of 0 means the entry expires at the put timestamp."""
        store2.put("a", "1", ttl=0, timestamp=5)
        assert store2.get("a", timestamp=5) is None

    def test_overwrite_with_new_ttl(self, store2: KVStore) -> None:
        """Overwriting a key should update its TTL."""
        store2.put("a", "1", ttl=5, timestamp=0)
        store2.put("a", "2", ttl=20, timestamp=0)
        assert store2.get("a", timestamp=8) == "2"

    def test_overwrite_removes_ttl(self, store2: KVStore) -> None:
        """Overwriting with no TTL should make entry permanent."""
        store2.put("a", "1", ttl=5, timestamp=0)
        store2.put("a", "2", timestamp=0)  # no TTL
        assert store2.get("a", timestamp=100) == "2"

    def test_delete_with_timestamp(self, store2: KVStore) -> None:
        """Delete should work regardless of TTL status."""
        store2.put("a", "1", ttl=100, timestamp=0)
        store2.delete("a", timestamp=5)
        assert store2.get("a", timestamp=5) is None

    def test_expired_entry_eviction_order(self, store2: KVStore) -> None:
        """When capacity is full, expired entries should be considered absent."""
        store2.put("a", "1", ttl=5, timestamp=0)
        store2.put("b", "2", timestamp=0)
        # At timestamp=10, "a" is expired. Adding "c" should not evict "b".
        store2.put("c", "3", timestamp=10)
        assert store2.get("b", timestamp=10) == "2"
        assert store2.get("c", timestamp=10) == "3"

    def test_multiple_entries_different_ttls(self, store5: KVStore) -> None:
        """Entries with different TTLs should expire independently."""
        store5.put("a", "1", ttl=5, timestamp=0)
        store5.put("b", "2", ttl=10, timestamp=0)
        store5.put("c", "3", ttl=15, timestamp=0)
        store5.put("d", "4", ttl=20, timestamp=0)
        store5.put("e", "5", timestamp=0)  # no TTL

        # At timestamp=12: a and b expired, c/d/e alive
        result = store5.scan("", timestamp=12)
        keys = sorted([k for k, _ in result])
        assert keys == ["c", "d", "e"]


class TestTTLEdgeCases:
    """Edge cases for TTL behavior."""

    def test_ttl_with_capacity_one(self) -> None:
        store = KVStore(1)
        store.put("a", "1", ttl=10, timestamp=0)
        assert store.get("a", timestamp=5) == "1"
        assert store.get("a", timestamp=10) is None

    def test_ttl_default_timestamp_zero(self, store2: KVStore) -> None:
        """Default timestamp=0 should work correctly."""
        store2.put("a", "1", ttl=10)
        assert store2.get("a") == "1"  # timestamp=0, before expiry at 10

    def test_large_ttl(self, store2: KVStore) -> None:
        store2.put("a", "1", ttl=10**9, timestamp=0)
        assert store2.get("a", timestamp=10**8) == "1"

    def test_scan_all_expired(self, store3: KVStore) -> None:
        """Scan when all entries are expired should return empty."""
        store3.put("a", "1", ttl=5, timestamp=0)
        store3.put("b", "2", ttl=5, timestamp=0)
        store3.put("c", "3", ttl=5, timestamp=0)
        result = store3.scan("", timestamp=10)
        assert result == []

    def test_put_after_all_expired(self, store2: KVStore) -> None:
        """Should be able to put new entries after all existing ones expired."""
        store2.put("a", "1", ttl=5, timestamp=0)
        store2.put("b", "2", ttl=5, timestamp=0)
        store2.put("c", "3", timestamp=10)
        store2.put("d", "4", timestamp=10)
        assert store2.get("c", timestamp=10) == "3"
        assert store2.get("d", timestamp=10) == "4"


# ============================================================
# Level 4: Compact / Restore
# ============================================================


class TestCompactRestore:
    """Tests for compact() and restore() serialization."""

    def test_compact_restore_basic(self, store3: KVStore) -> None:
        """Basic round-trip: compact then restore should preserve data."""
        store3.put("a", "1")
        store3.put("b", "2")
        store3.put("c", "3")
        data = store3.compact()

        new_store = KVStore.restore(data)
        assert new_store.get("a") == "1"
        assert new_store.get("b") == "2"
        assert new_store.get("c") == "3"

    def test_compact_returns_string(self, store2: KVStore) -> None:
        store2.put("a", "1")
        data = store2.compact()
        assert isinstance(data, str)

    def test_compact_is_base64_zlib_json(self, store2: KVStore) -> None:
        """Compact output should be base64(zlib(json))."""
        store2.put("a", "1")
        data = store2.compact()
        # Should be valid base64
        decoded = base64.b64decode(data)
        # Should be valid zlib
        decompressed = zlib.decompress(decoded)
        # Should be valid JSON
        parsed = json.loads(decompressed)
        assert isinstance(parsed, (dict, list))

    def test_compact_restore_preserves_lru_order(self, store3: KVStore) -> None:
        """After restore, LRU order should be the same as before compact."""
        store3.put("a", "1")
        store3.put("b", "2")
        store3.put("c", "3")
        store3.get("a")  # order: b(LRU), c, a(MRU)
        data = store3.compact()

        new_store = KVStore.restore(data)
        # Now add a new entry — should evict "b" (LRU)
        new_store.put("d", "4")
        assert new_store.get("b") is None
        assert new_store.get("a") == "1"
        assert new_store.get("c") == "3"
        assert new_store.get("d") == "4"

    def test_compact_restore_preserves_ttl(self, store3: KVStore) -> None:
        """Restored entries should retain their TTL metadata."""
        store3.put("a", "1", ttl=10, timestamp=0)
        store3.put("b", "2", ttl=20, timestamp=0)
        store3.put("c", "3")  # no TTL
        data = store3.compact(timestamp=0)

        new_store = KVStore.restore(data)
        # "a" should be expired at timestamp=15
        assert new_store.get("a", timestamp=15) is None
        # "b" should still be alive at timestamp=15
        assert new_store.get("b", timestamp=15) == "2"
        # "c" never expires
        assert new_store.get("c", timestamp=100) == "3"

    def test_compact_skips_expired_entries(self, store3: KVStore) -> None:
        """Compact should not include expired entries."""
        store3.put("a", "1", ttl=5, timestamp=0)
        store3.put("b", "2", ttl=20, timestamp=0)
        store3.put("c", "3")
        data = store3.compact(timestamp=10)

        new_store = KVStore.restore(data)
        assert new_store.get("a") is None  # was expired before compact
        assert new_store.get("b", timestamp=10) == "2"
        assert new_store.get("c") == "3"

    def test_compact_empty_store(self, store2: KVStore) -> None:
        """Compact on empty store should produce valid restorable data."""
        data = store2.compact()
        new_store = KVStore.restore(data)
        assert new_store.get("anything") is None

    def test_restore_returns_new_kvstore(self, store2: KVStore) -> None:
        """Restore is a classmethod that returns a new KVStore instance."""
        store2.put("a", "1")
        store2.put("b", "2")
        data = store2.compact()

        restored = KVStore.restore(data)
        assert isinstance(restored, KVStore)
        assert restored.get("a") == "1"
        assert restored.get("b") == "2"

    def test_compact_restore_capacity_one(self) -> None:
        store = KVStore(1)
        store.put("only", "entry")
        data = store.compact()

        new_store = KVStore.restore(data)
        assert new_store.get("only") == "entry"

    def test_compact_restore_after_delete(self, store3: KVStore) -> None:
        """Deleted entries should not appear in compact output."""
        store3.put("a", "1")
        store3.put("b", "2")
        store3.put("c", "3")
        store3.delete("b")
        data = store3.compact()

        new_store = KVStore.restore(data)
        assert new_store.get("a") == "1"
        assert new_store.get("b") is None
        assert new_store.get("c") == "3"

    def test_compact_restore_after_eviction(self, store2: KVStore) -> None:
        """Evicted entries should not appear in compact output."""
        store2.put("a", "1")
        store2.put("b", "2")
        store2.put("c", "3")  # evicts "a"
        data = store2.compact()

        new_store = KVStore.restore(data)
        assert new_store.get("a") is None
        assert new_store.get("b") == "2"
        assert new_store.get("c") == "3"


class TestCompactRestoreEdgeCases:
    """Edge cases for compact/restore."""

    def test_double_compact_restore(self, store3: KVStore) -> None:
        """Double round-trip should preserve data."""
        store3.put("a", "1")
        store3.put("b", "2")
        data1 = store3.compact()

        mid = KVStore.restore(data1)
        data2 = mid.compact()

        final = KVStore.restore(data2)
        assert final.get("a") == "1"
        assert final.get("b") == "2"

    def test_restore_then_operations(self, store3: KVStore) -> None:
        """Store should be fully operational after restore."""
        store3.put("a", "1")
        store3.put("b", "2")
        data = store3.compact()

        new_store = KVStore.restore(data)
        new_store.put("c", "3")
        new_store.delete("a")
        assert new_store.get("a") is None
        assert new_store.get("b") == "2"
        assert new_store.get("c") == "3"

    def test_compact_restore_with_special_characters(self, store3: KVStore) -> None:
        """Keys/values with special characters should round-trip correctly."""
        store3.put("key with spaces", "value\nwith\nnewlines")
        store3.put('key"quotes"', "value\ttabs")
        store3.put("emoji🎉", "unicode✅")
        data = store3.compact()

        new_store = KVStore.restore(data)
        assert new_store.get("key with spaces") == "value\nwith\nnewlines"
        assert new_store.get('key"quotes"') == "value\ttabs"
        assert new_store.get("emoji🎉") == "unicode✅"

    def test_compact_restore_preserves_capacity(self) -> None:
        """Restored store should have the same capacity as the original."""
        store = KVStore(3)
        store.put("a", "1")
        store.put("b", "2")
        store.put("c", "3")
        data = store.compact()

        new_store = KVStore.restore(data)
        # Adding a 4th entry should evict the LRU
        new_store.put("d", "4")
        # "a" was LRU, should be evicted
        assert new_store.get("a") is None
        assert new_store.get("b") == "2"
        assert new_store.get("c") == "3"
        assert new_store.get("d") == "4"

    def test_compact_restore_preserves_scan(self, store5: KVStore) -> None:
        """Scan should work correctly on a restored store."""
        store5.put("user:1", "alice")
        store5.put("user:2", "bob")
        store5.put("post:1", "hello")
        data = store5.compact()

        new_store = KVStore.restore(data)
        result = new_store.scan("user:")
        assert sorted(result) == [("user:1", "alice"), ("user:2", "bob")]


# ============================================================
# Integration: Cross-Level Interactions
# ============================================================


class TestCrossLevelIntegration:
    """Tests for interactions between different levels."""

    def test_ttl_and_lru_eviction(self, store2: KVStore) -> None:
        """LRU eviction should prefer expired entries."""
        store2.put("a", "1", ttl=5, timestamp=0)
        store2.put("b", "2", timestamp=0)
        # "a" is expired at timestamp=10. Adding "c" should evict expired "a"
        store2.put("c", "3", timestamp=10)
        assert store2.get("b", timestamp=10) == "2"
        assert store2.get("c", timestamp=10) == "3"

    def test_scan_ttl_and_eviction(self, store3: KVStore) -> None:
        """Scan should handle mix of TTL, evicted, and live entries."""
        store3.put("a:1", "v1", ttl=5, timestamp=0)
        store3.put("a:2", "v2", timestamp=0)
        store3.put("a:3", "v3", timestamp=0)
        # At timestamp=10, "a:1" is expired
        result = store3.scan("a:", timestamp=10)
        assert ("a:1", "v1") not in result
        assert ("a:2", "v2") in result
        assert ("a:3", "v3") in result

    def test_full_workflow(self) -> None:
        """End-to-end workflow using all levels."""
        store = KVStore(3)

        # Level 1: CRUD
        store.put("k1", "v1", timestamp=0)
        store.put("k2", "v2", timestamp=0)
        store.put("k3", "v3", timestamp=0)
        assert store.get("k1", timestamp=0) == "v1"
        # Note: get("k1") promoted k1 to MRU. Order: k2(LRU), k3, k1(MRU)

        # Level 2: Scan
        result = store.scan("k", timestamp=0)
        assert len(result) == 3

        # Level 3: TTL — put evicts k2 (LRU after get promoted k1)
        store.put("k4", "v4", ttl=10, timestamp=0)
        assert store.get("k2", timestamp=0) is None  # k2 was evicted (LRU)
        assert store.get("k1", timestamp=0) == "v1"  # k1 still alive
        assert store.get("k4", timestamp=5) == "v4"

        # Level 4: Compact/Restore (before k4 expires)
        data = store.compact(timestamp=5)
        new_store = KVStore.restore(data)
        assert new_store.get("k1", timestamp=5) == "v1"
        assert new_store.get("k3", timestamp=5) == "v3"
        assert new_store.get("k4", timestamp=5) == "v4"
        # k4 should expire at timestamp=10 in the restored store too
        assert new_store.get("k4", timestamp=10) is None

    def test_delete_expired_then_compact(self, store3: KVStore) -> None:
        """Compact after mix of deletes and TTL expiry."""
        store3.put("a", "1", ttl=5, timestamp=0)
        store3.put("b", "2", timestamp=0)
        store3.put("c", "3", timestamp=0)
        store3.delete("b", timestamp=3)
        # At timestamp=10: "a" expired, "b" deleted, "c" alive
        data = store3.compact(timestamp=10)

        new_store = KVStore.restore(data)
        assert new_store.get("a") is None
        assert new_store.get("b") is None
        assert new_store.get("c") == "3"


# ============================================================
# Stress Tests
# ============================================================


class TestStress:
    """Stress and performance tests."""

    def test_many_entries_with_eviction(self) -> None:
        """Large number of entries with continuous eviction."""
        store = KVStore(50)
        for i in range(500):
            store.put(f"key:{i}", f"val:{i}")
        # Only last 50 should remain
        for i in range(450):
            assert store.get(f"key:{i}") is None
        for i in range(450, 500):
            assert store.get(f"key:{i}") == f"val:{i}"

    def test_scan_large_store(self) -> None:
        """Scan on a store with many entries."""
        store = KVStore(100)
        for i in range(50):
            store.put(f"a:{i}", str(i))
        for i in range(50):
            store.put(f"b:{i}", str(i))
        result = store.scan("a:")
        assert len(result) == 50

    def test_compact_restore_large(self) -> None:
        """Compact/restore with many entries."""
        store = KVStore(100)
        for i in range(100):
            store.put(f"key:{i}", f"val:{i}")
        data = store.compact()
        new_store = KVStore.restore(data)
        for i in range(100):
            assert new_store.get(f"key:{i}") == f"val:{i}"

    def test_mixed_ttl_scan_stress(self) -> None:
        """Mixed TTL entries with scan under stress."""
        store = KVStore(100)
        for i in range(100):
            ttl = 10 if i % 2 == 0 else None
            store.put(f"item:{i}", str(i), ttl=ttl, timestamp=0)
        # At timestamp=15: even-indexed entries are expired
        result = store.scan("item:", timestamp=15)
        # Only odd-indexed entries should remain
        assert len(result) == 50
        for k, v in result:
            idx = int(k.split(":")[1])
            assert idx % 2 == 1
