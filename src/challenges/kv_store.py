"""In-memory key-value store with progressive extensions.

Level 1: get/put/delete with fixed capacity N and LRU eviction.
Level 2: scan(prefix) — return all key-value pairs matching a prefix.
Level 3: TTL support via logical timestamps; expired entries are lazily evicted.
Level 4: compact/restore — serialize and reconstruct the store.
"""

from __future__ import annotations

import base64
import json
import zlib


class _Node:
    """Doubly-linked list node for LRU tracking."""

    __slots__ = ("key", "value", "expires_at", "prev", "next")

    def __init__(
        self,
        key: str = "",
        value: str = "",
        expires_at: float | None = None,
    ) -> None:
        self.key = key
        self.value = value
        self.expires_at = expires_at  # None means no expiry
        self.prev: _Node | None = None
        self.next: _Node | None = None


class KVStore:
    """In-memory key-value store with LRU eviction, prefix scan, TTL, and
    compact/restore.

    Uses a hash map for O(1) key lookup and a doubly-linked list to track
    access recency.  Sentinel head/tail nodes simplify edge-case handling.

    Args:
        capacity: Maximum number of live (non-expired) key-value pairs the
            store can hold.  Must be a positive integer.

    Examples:
        >>> store = KVStore(2)
        >>> store.put("a", "1")
        >>> store.put("b", "2")
        >>> store.get("a")
        '1'
        >>> store.put("c", "3")  # evicts "b" (LRU)
        >>> store.get("b") is None
        True
    """

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def __init__(self, capacity: int) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be a positive integer")
        self._capacity = capacity
        self._store: dict[str, _Node] = {}

        # Sentinel nodes — never removed, simplify add/remove logic.
        self._head = _Node()
        self._tail = _Node()
        self._head.next = self._tail
        self._tail.prev = self._head

    # ------------------------------------------------------------------
    # Level 1 — basic CRUD + LRU eviction
    # ------------------------------------------------------------------

    def get(self, key: str, *, timestamp: float = 0) -> str | None:
        """Return the value for *key*, or ``None`` if not present or expired.

        Accessing a key marks it as most recently used.
        """
        node = self._store.get(key)
        if node is None:
            return None
        if self._is_expired(node, timestamp):
            self._remove_node(node)
            return None
        self._move_to_front(node)
        return node.value

    def put(
        self,
        key: str,
        value: str,
        ttl: float | None = None,
        *,
        timestamp: float = 0,
    ) -> None:
        """Insert or update *key* with *value*.

        If the key already exists its value is updated and it becomes the
        most recently used entry.  If the store is at capacity the least
        recently used *live* entry is evicted first.

        Args:
            key: The key string.
            value: The value string.
            ttl: Optional time-to-live in seconds.  The entry expires when
                the logical timestamp reaches ``timestamp + ttl``.
            timestamp: The current logical timestamp.
        """
        expires_at = (timestamp + ttl) if ttl is not None else None

        node = self._store.get(key)
        if node is not None:
            if self._is_expired(node, timestamp):
                # Existing entry is expired — remove and treat as new insert.
                self._remove_node(node)
            else:
                # Update in place.
                node.value = value
                node.expires_at = expires_at
                self._move_to_front(node)
                return

        # Make room: first evict expired entries, then LRU if needed.
        self._evict_expired(timestamp)
        while len(self._store) >= self._capacity:
            self._evict_lru()

        new_node = _Node(key, value, expires_at)
        self._store[key] = new_node
        self._add_to_front(new_node)

    def delete(self, key: str, *, timestamp: float = 0) -> bool:
        """Remove *key* from the store.

        Returns ``True`` if the key was present and non-expired, ``False``
        otherwise.
        """
        node = self._store.get(key)
        if node is None:
            return False
        if self._is_expired(node, timestamp):
            self._remove_node(node)
            return False
        self._remove_node(node)
        return True

    # ------------------------------------------------------------------
    # Level 2 — prefix scan
    # ------------------------------------------------------------------

    def scan(self, prefix: str, *, timestamp: float = 0) -> list[tuple[str, str]]:
        """Return all ``(key, value)`` pairs whose key starts with *prefix*.

        Expired entries are skipped (and lazily removed).  Results are
        returned in most-recently-used–first order.
        """
        result: list[tuple[str, str]] = []
        expired_nodes: list[_Node] = []

        # Walk the linked list from MRU to LRU for deterministic order.
        node = self._head.next
        while node is not None and node is not self._tail:
            next_node = node.next  # capture before potential removal
            if self._is_expired(node, timestamp):
                expired_nodes.append(node)
            elif node.key.startswith(prefix):
                result.append((node.key, node.value))
            node = next_node

        for n in expired_nodes:
            self._remove_node(n)

        return result

    # ------------------------------------------------------------------
    # Level 4 — compact / restore
    # ------------------------------------------------------------------

    def compact(self, *, timestamp: float = 0) -> str:
        """Serialize all non-expired entries to a compressed string.

        The serialized form preserves LRU order (MRU first) and TTL
        metadata so that :meth:`restore` can faithfully reconstruct the
        store.

        Returns:
            A base64-encoded, zlib-compressed JSON string.
        """
        entries: list[dict[str, str | float | None]] = []

        # Walk MRU → LRU.
        node = self._head.next
        while node is not None and node is not self._tail:
            next_node = node.next
            if not self._is_expired(node, timestamp):
                entry: dict[str, str | float | None] = {
                    "k": node.key,
                    "v": node.value,
                }
                if node.expires_at is not None:
                    entry["e"] = node.expires_at
                entries.append(entry)
            node = next_node

        payload: dict[str, object] = {
            "cap": self._capacity,
            "entries": entries,
        }
        json_bytes = json.dumps(payload, separators=(",", ":")).encode()
        compressed = zlib.compress(json_bytes)
        return base64.b64encode(compressed).decode("ascii")

    @classmethod
    def restore(cls, data: str) -> "KVStore":
        """Reconstruct a :class:`KVStore` from a :meth:`compact` string.

        Args:
            data: The base64-encoded, zlib-compressed JSON string produced
                by :meth:`compact`.

        Returns:
            A new ``KVStore`` with the same capacity, entries, LRU order,
            and TTL metadata as the original.
        """
        compressed = base64.b64decode(data)
        json_bytes = zlib.decompress(compressed)
        payload: dict[str, object] = json.loads(json_bytes)

        raw_cap = payload["cap"]
        assert isinstance(raw_cap, int)
        capacity: int = raw_cap
        raw_entries: object = payload["entries"]
        assert isinstance(raw_entries, list)

        store = cls(capacity)

        # Entries are stored MRU-first.  Insert in reverse so that the
        # first entry ends up at the front (most recently used).
        for entry in reversed(raw_entries):
            assert isinstance(entry, dict)
            key = str(entry["k"])
            value = str(entry["v"])
            raw_exp = entry.get("e")
            expires_at: float | None = float(raw_exp) if raw_exp is not None else None
            node = _Node(key, value, expires_at)
            store._store[key] = node
            store._add_to_front(node)

        return store

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _is_expired(node: _Node, timestamp: float) -> bool:
        """Return ``True`` if *node* has a TTL and has expired."""
        return node.expires_at is not None and timestamp >= node.expires_at

    def _add_to_front(self, node: _Node) -> None:
        """Insert *node* right after the head sentinel (most recent)."""
        node.prev = self._head
        node.next = self._head.next
        assert self._head.next is not None
        self._head.next.prev = node
        self._head.next = node

    def _remove(self, node: _Node) -> None:
        """Unlink *node* from the doubly-linked list."""
        assert node.prev is not None
        assert node.next is not None
        node.prev.next = node.next
        node.next.prev = node.prev

    def _move_to_front(self, node: _Node) -> None:
        """Move an existing *node* to the front (most recent)."""
        self._remove(node)
        self._add_to_front(node)

    def _remove_node(self, node: _Node) -> None:
        """Unlink *node* from the list and remove from the hash map."""
        self._remove(node)
        del self._store[node.key]

    def _evict_lru(self) -> None:
        """Remove the least recently used node (just before tail sentinel)."""
        lru = self._tail.prev
        assert lru is not None and lru is not self._head
        self._remove_node(lru)

    def _evict_expired(self, timestamp: float) -> None:
        """Remove all expired entries from the store."""
        expired: list[_Node] = []
        node = self._head.next
        while node is not None and node is not self._tail:
            if self._is_expired(node, timestamp):
                expired.append(node)
            node = node.next
        for n in expired:
            self._remove_node(n)
