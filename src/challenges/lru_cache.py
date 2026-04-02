"""LRU Cache coding challenge.

Implement a Least Recently Used (LRU) Cache that supports O(1) get and put
operations. When the cache reaches capacity, the least recently used item
is evicted before inserting a new one.
"""


class _Node:
    """Doubly-linked list node for LRU tracking."""

    __slots__ = ("key", "value", "prev", "next")

    def __init__(self, key: int = 0, value: int = 0) -> None:
        self.key = key
        self.value = value
        self.prev: _Node | None = None
        self.next: _Node | None = None


class LRUCache:
    """Least Recently Used (LRU) Cache with O(1) get and put.

    Uses a hash map for O(1) key lookup and a doubly-linked list to track
    access recency. Sentinel head/tail nodes simplify edge-case handling.

    Args:
        capacity: Maximum number of key-value pairs the cache can hold.
            Must be a positive integer.

    Examples:
        >>> cache = LRUCache(2)
        >>> cache.put(1, 1)
        >>> cache.put(2, 2)
        >>> cache.get(1)
        1
        >>> cache.put(3, 3)  # evicts key 2
        >>> cache.get(2)
        -1
    """

    def __init__(self, capacity: int) -> None:
        self._capacity = capacity
        self._cache: dict[int, _Node] = {}

        # Sentinel nodes — never removed, simplify add/remove logic.
        self._head = _Node()
        self._tail = _Node()
        self._head.next = self._tail
        self._tail.prev = self._head

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get(self, key: int) -> int:
        """Return the value for *key*, or -1 if not present.

        Accessing a key marks it as most recently used.
        """
        node = self._cache.get(key)
        if node is None:
            return -1
        self._move_to_front(node)
        return node.value

    def put(self, key: int, value: int) -> None:
        """Insert or update *key* with *value*.

        If the key already exists its value is updated and it becomes the
        most recently used entry. If the cache is at capacity the least
        recently used entry is evicted first.
        """
        node = self._cache.get(key)
        if node is not None:
            node.value = value
            self._move_to_front(node)
            return

        # New entry — evict if at capacity.
        if len(self._cache) >= self._capacity:
            self._evict_lru()

        new_node = _Node(key, value)
        self._cache[key] = new_node
        self._add_to_front(new_node)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _add_to_front(self, node: _Node) -> None:
        """Insert *node* right after the head sentinel (most recent)."""
        node.prev = self._head
        node.next = self._head.next
        self._head.next.prev = node  # type: ignore[union-attr]
        self._head.next = node

    def _remove(self, node: _Node) -> None:
        """Unlink *node* from the doubly-linked list."""
        node.prev.next = node.next  # type: ignore[union-attr]
        node.next.prev = node.prev  # type: ignore[union-attr]

    def _move_to_front(self, node: _Node) -> None:
        """Move an existing *node* to the front (most recent)."""
        self._remove(node)
        self._add_to_front(node)

    def _evict_lru(self) -> None:
        """Remove the least recently used node (just before tail sentinel)."""
        lru = self._tail.prev
        assert lru is not self._head  # capacity ≥ 1
        self._remove(lru)  # type: ignore[arg-type]
        del self._cache[lru.key]  # type: ignore[union-attr]
