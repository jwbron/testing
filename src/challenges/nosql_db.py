"""In-memory NoSQL database with MongoDB-style API.

A document-oriented database supporting CRUD operations, query operators,
secondary indexes, aggregation pipelines, and transactions with snapshot
isolation. Documents are Python dicts stored in named collections.
"""

from __future__ import annotations

import bisect
import copy
import uuid
from collections import defaultdict
from typing import Any

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _generate_id() -> str:
    """Generate a unique document ID."""
    return str(uuid.uuid4())


def _get_nested(doc: dict[str, Any], path: str) -> Any:
    """Retrieve a value from a nested dict using dot-notation.

    Args:
        doc: The document to traverse.
        path: Dot-separated field path (e.g. ``"user.address.city"``).

    Returns:
        The value at *path*, or a sentinel :data:`_MISSING` if any segment
        is absent.
    """
    parts = path.split(".")
    current: Any = doc
    for part in parts:
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return _MISSING
    return current


def _set_nested(doc: dict[str, Any], path: str, value: Any) -> None:
    """Set a value in a nested dict using dot-notation, creating parents."""
    parts = path.split(".")
    current = doc
    for part in parts[:-1]:
        if part not in current or not isinstance(current[part], dict):
            current[part] = {}
        current = current[part]
    current[parts[-1]] = value


def _delete_nested(doc: dict[str, Any], path: str) -> bool:
    """Delete a field from a nested dict using dot-notation.

    Returns:
        ``True`` if the field existed and was removed.
    """
    parts = path.split(".")
    current = doc
    for part in parts[:-1]:
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return False
    if isinstance(current, dict) and parts[-1] in current:
        del current[parts[-1]]
        return True
    return False


class _MissingSentinel:
    """Sentinel for missing fields (distinct from ``None``)."""

    def __repr__(self) -> str:
        return "<MISSING>"


_MISSING = _MissingSentinel()


# ---------------------------------------------------------------------------
# Query Engine
# ---------------------------------------------------------------------------

class QueryEngine:
    """Evaluate MongoDB-style query filters against documents.

    Supports comparison (``$eq``, ``$gt``, ``$lt``, ``$gte``, ``$lte``,
    ``$ne``), set (``$in``, ``$nin``), logical (``$and``, ``$or``, ``$not``,
    ``$nor``), and ``$exists`` operators.  Dot-notation is used for nested
    field access.
    """

    def match(self, doc: dict[str, Any], query: dict[str, Any]) -> bool:
        """Return ``True`` if *doc* satisfies *query*."""
        for key, condition in query.items():
            if key == "$and":
                if not all(self.match(doc, sub) for sub in condition):
                    return False
            elif key == "$or":
                if not any(self.match(doc, sub) for sub in condition):
                    return False
            elif key == "$nor":
                if any(self.match(doc, sub) for sub in condition):
                    return False
            elif key == "$not":
                if self.match(doc, condition):
                    return False
            else:
                value = _get_nested(doc, key)
                if not self._match_condition(value, condition):
                    return False
        return True

    def _match_condition(self, value: Any, condition: Any) -> bool:
        """Match a single field value against its condition."""
        if isinstance(condition, dict) and condition:
            first_key = next(iter(condition))
            if isinstance(first_key, str) and first_key.startswith("$"):
                return all(
                    self._apply_operator(value, op, operand)
                    for op, operand in condition.items()
                )
        # Implicit $eq
        if isinstance(value, _MissingSentinel):
            return condition is _MISSING
        return value == condition

    def _apply_operator(self, value: Any, op: str, operand: Any) -> bool:
        """Apply a single query operator."""
        if op == "$exists":
            exists = not isinstance(value, _MissingSentinel)
            return exists == bool(operand)
        if op == "$not":
            return not self._match_condition(value, operand)

        # All remaining operators require a present value
        if isinstance(value, _MissingSentinel):
            return False

        if op == "$eq":
            return value == operand
        if op == "$ne":
            return value != operand
        if op == "$gt":
            return value > operand
        if op == "$gte":
            return value >= operand
        if op == "$lt":
            return value < operand
        if op == "$lte":
            return value <= operand
        if op == "$in":
            return value in operand
        if op == "$nin":
            return value not in operand

        raise ValueError(f"Unknown operator: {op}")


# ---------------------------------------------------------------------------
# Index Manager
# ---------------------------------------------------------------------------

class IndexManager:
    """Maintain secondary indexes on a collection.

    Indexes map field values to sets of document ``_id`` values and are kept
    in sync on every insert / update / delete.
    """

    def __init__(self) -> None:
        # field_name -> {value: set[doc_id]}
        self._indexes: dict[str, dict[Any, set[str]]] = {}
        # field_name -> sorted list of unique values (for range queries)
        self._sorted_keys: dict[str, list] = {}

    @property
    def indexed_fields(self) -> set[str]:
        return set(self._indexes)

    # --- public API --------------------------------------------------------

    def create_index(self, field: str, documents: dict[str, dict]) -> None:
        """Create an index on *field* and populate it from existing docs."""
        idx: dict[Any, set[str]] = defaultdict(set)
        for doc_id, doc in documents.items():
            val = _get_nested(doc, field)
            if not isinstance(val, _MissingSentinel):
                idx[val].add(doc_id)
        self._indexes[field] = dict(idx)
        self._sorted_keys[field] = sorted(idx.keys(), key=_sort_key)

    def drop_index(self, field: str) -> bool:
        """Drop the index on *field*. Returns ``True`` if it existed."""
        if field in self._indexes:
            del self._indexes[field]
            del self._sorted_keys[field]
            return True
        return False

    def has_index(self, field: str) -> bool:
        return field in self._indexes

    # --- maintenance -------------------------------------------------------

    def on_insert(self, doc_id: str, doc: dict[str, Any]) -> None:
        for field in self._indexes:
            val = _get_nested(doc, field)
            if not isinstance(val, _MissingSentinel):
                self._add_entry(field, val, doc_id)

    def on_delete(self, doc_id: str, doc: dict[str, Any]) -> None:
        for field in self._indexes:
            val = _get_nested(doc, field)
            if not isinstance(val, _MissingSentinel):
                self._remove_entry(field, val, doc_id)

    def on_update(
        self, doc_id: str, old_doc: dict[str, Any], new_doc: dict[str, Any]
    ) -> None:
        for field in self._indexes:
            old_val = _get_nested(old_doc, field)
            new_val = _get_nested(new_doc, field)
            old_present = not isinstance(old_val, _MissingSentinel)
            new_present = not isinstance(new_val, _MissingSentinel)
            if old_present and new_present and old_val == new_val:
                continue
            if old_present:
                self._remove_entry(field, old_val, doc_id)
            if new_present:
                self._add_entry(field, new_val, doc_id)

    # --- query helpers -----------------------------------------------------

    def lookup_eq(self, field: str, value: Any) -> set[str]:
        """Return doc IDs where *field* == *value*."""
        return set(self._indexes.get(field, {}).get(value, set()))

    def lookup_range(
        self,
        field: str,
        *,
        gt: Any = _MISSING,
        gte: Any = _MISSING,
        lt: Any = _MISSING,
        lte: Any = _MISSING,
    ) -> set[str]:
        """Return doc IDs matching a range query on *field*."""
        if field not in self._indexes:
            return set()
        keys = self._sorted_keys[field]
        idx = self._indexes[field]

        lo = 0
        hi = len(keys)
        if not isinstance(gte, _MissingSentinel):
            lo = bisect.bisect_left(keys, _sort_key(gte), key=_sort_key)
        elif not isinstance(gt, _MissingSentinel):
            lo = bisect.bisect_right(keys, _sort_key(gt), key=_sort_key)
        if not isinstance(lte, _MissingSentinel):
            hi = bisect.bisect_right(keys, _sort_key(lte), key=_sort_key)
        elif not isinstance(lt, _MissingSentinel):
            hi = bisect.bisect_left(keys, _sort_key(lt), key=_sort_key)

        result: set[str] = set()
        for k in keys[lo:hi]:
            result |= idx[k]
        return result

    # --- internal ----------------------------------------------------------

    def _add_entry(self, field: str, value: Any, doc_id: str) -> None:
        idx = self._indexes[field]
        if value not in idx:
            idx[value] = set()
            bisect.insort(self._sorted_keys[field], value, key=_sort_key)
        idx[value].add(doc_id)

    def _remove_entry(self, field: str, value: Any, doc_id: str) -> None:
        idx = self._indexes[field]
        if value in idx:
            idx[value].discard(doc_id)
            if not idx[value]:
                del idx[value]
                keys = self._sorted_keys[field]
                pos = bisect.bisect_left(keys, _sort_key(value), key=_sort_key)
                if pos < len(keys) and keys[pos] == value:
                    keys.pop(pos)


def _sort_key(val: Any) -> tuple:
    """Produce a sortable key that handles mixed types."""
    if isinstance(val, bool):
        return (1, int(val))
    if isinstance(val, (int, float)):
        return (1, val)
    if isinstance(val, str):
        return (2, val)
    return (3, str(val))


# ---------------------------------------------------------------------------
# Aggregation Pipeline
# ---------------------------------------------------------------------------

class AggregationPipeline:
    """Execute MongoDB-style aggregation pipelines.

    Supported stages: ``$match``, ``$group``, ``$sort``, ``$limit``,
    ``$skip``, ``$project``, ``$unwind``, ``$count``.
    """

    def __init__(self, query_engine: QueryEngine) -> None:
        self._qe = query_engine

    def execute(
        self, documents: list[dict[str, Any]], pipeline: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Run *pipeline* over *documents* and return results."""
        docs = [copy.deepcopy(d) for d in documents]
        for stage in pipeline:
            if len(stage) != 1:
                raise ValueError(f"Each stage must have exactly one key: {stage}")
            stage_name = next(iter(stage))
            handler = getattr(self, f"_stage_{stage_name.lstrip('$')}", None)
            if handler is None:
                raise ValueError(f"Unknown aggregation stage: {stage_name}")
            docs = handler(docs, stage[stage_name])
        return docs

    # --- stages ------------------------------------------------------------

    def _stage_match(
        self, docs: list[dict], expr: dict
    ) -> list[dict]:
        return [d for d in docs if self._qe.match(d, expr)]

    def _stage_group(
        self, docs: list[dict], expr: dict
    ) -> list[dict]:
        group_id_expr = expr["_id"]
        accumulators = {k: v for k, v in expr.items() if k != "_id"}

        groups: dict[Any, list[dict]] = defaultdict(list)
        for doc in docs:
            gid = self._eval_group_id(doc, group_id_expr)
            groups[gid].append(doc)

        results: list[dict] = []
        for gid, group_docs in groups.items():
            row: dict[str, Any] = {"_id": gid}
            for alias, acc_expr in accumulators.items():
                row[alias] = self._eval_accumulator(group_docs, acc_expr)
            results.append(row)
        return results

    def _stage_sort(
        self, docs: list[dict], spec: dict
    ) -> list[dict]:
        for field, direction in reversed(list(spec.items())):
            docs = sorted(
                docs,
                key=lambda d, f=field: _sort_key(
                    _get_nested(d, f)
                    if not isinstance(_get_nested(d, f), _MissingSentinel)
                    else None
                ),
                reverse=(direction == -1),
            )
        return docs

    def _stage_limit(
        self, docs: list[dict], n: int
    ) -> list[dict]:
        return docs[:n]

    def _stage_skip(
        self, docs: list[dict], n: int
    ) -> list[dict]:
        return docs[n:]

    def _stage_project(
        self, docs: list[dict], spec: dict
    ) -> list[dict]:
        results = []
        for doc in docs:
            projected: dict[str, Any] = {}
            # Always include _id unless explicitly excluded
            if spec.get("_id", 1) != 0:
                if "_id" in doc:
                    projected["_id"] = doc["_id"]
            for field, include in spec.items():
                if field == "_id":
                    continue
                if include:
                    val = _get_nested(doc, field)
                    if not isinstance(val, _MissingSentinel):
                        _set_nested(projected, field, val)
            results.append(projected)
        return results

    def _stage_unwind(
        self, docs: list[dict], path: str
    ) -> list[dict]:
        if isinstance(path, str) and path.startswith("$"):
            path = path[1:]
        results = []
        for doc in docs:
            val = _get_nested(doc, path)
            if isinstance(val, list):
                for item in val:
                    new_doc = copy.deepcopy(doc)
                    _set_nested(new_doc, path, item)
                    results.append(new_doc)
            else:
                results.append(copy.deepcopy(doc))
        return results

    def _stage_count(
        self, docs: list[dict], field_name: str
    ) -> list[dict]:
        return [{field_name: len(docs)}]

    # --- helpers -----------------------------------------------------------

    def _eval_group_id(self, doc: dict, expr: Any) -> Any:
        if expr is None:
            return None
        if isinstance(expr, str) and expr.startswith("$"):
            val = _get_nested(doc, expr[1:])
            return None if isinstance(val, _MissingSentinel) else val
        if isinstance(expr, dict):
            return tuple(
                sorted(
                    (k, self._eval_group_id(doc, v)) for k, v in expr.items()
                )
            )
        return expr

    def _eval_accumulator(
        self, docs: list[dict], expr: dict
    ) -> Any:
        op = next(iter(expr))
        field_expr = expr[op]

        if op == "$count":
            return len(docs)

        if isinstance(field_expr, str) and field_expr.startswith("$"):
            field = field_expr[1:]
        else:
            # Literal value
            field = None

        values = []
        for doc in docs:
            if field is not None:
                val = _get_nested(doc, field)
                if not isinstance(val, _MissingSentinel):
                    values.append(val)
            else:
                values.append(field_expr)

        if op == "$sum":
            return sum(v for v in values if isinstance(v, (int, float)))
        if op == "$avg":
            nums = [v for v in values if isinstance(v, (int, float))]
            return (sum(nums) / len(nums)) if nums else 0
        if op == "$min":
            return min(values) if values else None
        if op == "$max":
            return max(values) if values else None
        if op == "$push":
            return values

        raise ValueError(f"Unknown accumulator: {op}")


# ---------------------------------------------------------------------------
# Collection
# ---------------------------------------------------------------------------

class Collection:
    """A named collection of documents.

    Documents are dicts with an auto-generated ``_id`` field.
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self._docs: dict[str, dict[str, Any]] = {}  # _id -> document
        self._versions: dict[str, int] = {}  # _id -> version counter
        self._query_engine = QueryEngine()
        self._index_manager = IndexManager()
        self._agg_pipeline = AggregationPipeline(self._query_engine)

    # --- CRUD --------------------------------------------------------------

    def insert_one(self, document: dict[str, Any]) -> str:
        """Insert a single document. Returns its ``_id``."""
        doc = copy.deepcopy(document)
        if "_id" not in doc:
            doc["_id"] = _generate_id()
        doc_id = doc["_id"]
        if doc_id in self._docs:
            raise ValueError(f"Duplicate _id: {doc_id}")
        self._docs[doc_id] = doc
        self._versions[doc_id] = 0
        self._index_manager.on_insert(doc_id, doc)
        return doc_id

    def insert_many(self, documents: list[dict[str, Any]]) -> list[str]:
        """Insert multiple documents. Returns a list of ``_id`` values."""
        return [self.insert_one(d) for d in documents]

    def find_one(
        self, filter: dict[str, Any] | None = None
    ) -> dict[str, Any] | None:
        """Return the first document matching *filter*, or ``None``."""
        for doc in self._iter_match(filter or {}):
            return copy.deepcopy(doc)
        return None

    def find(
        self, filter: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Return all documents matching *filter*."""
        return [copy.deepcopy(d) for d in self._iter_match(filter or {})]

    def update_one(
        self, filter: dict[str, Any], update: dict[str, Any]
    ) -> int:
        """Update the first matching document. Returns count of modified."""
        for doc in self._iter_match(filter):
            old = copy.deepcopy(doc)
            self._apply_update(doc, update)
            self._index_manager.on_update(doc["_id"], old, doc)
            self._versions[doc["_id"]] = self._versions.get(doc["_id"], 0) + 1
            return 1
        return 0

    def update_many(
        self, filter: dict[str, Any], update: dict[str, Any]
    ) -> int:
        """Update all matching documents. Returns count of modified."""
        count = 0
        for doc in list(self._iter_match(filter)):
            old = copy.deepcopy(doc)
            self._apply_update(doc, update)
            self._index_manager.on_update(doc["_id"], old, doc)
            self._versions[doc["_id"]] = self._versions.get(doc["_id"], 0) + 1
            count += 1
        return count

    def delete_one(self, filter: dict[str, Any]) -> int:
        """Delete the first matching document. Returns count of deleted."""
        for doc in self._iter_match(filter):
            self._index_manager.on_delete(doc["_id"], doc)
            self._versions.pop(doc["_id"], None)
            del self._docs[doc["_id"]]
            return 1
        return 0

    def delete_many(self, filter: dict[str, Any]) -> int:
        """Delete all matching documents. Returns count of deleted."""
        to_delete = [d["_id"] for d in self._iter_match(filter)]
        for doc_id in to_delete:
            doc = self._docs[doc_id]
            self._index_manager.on_delete(doc_id, doc)
            self._versions.pop(doc_id, None)
            del self._docs[doc_id]
        return len(to_delete)

    def count(self, filter: dict[str, Any] | None = None) -> int:
        """Return the number of documents matching *filter*."""
        if not filter:
            return len(self._docs)
        return sum(1 for _ in self._iter_match(filter))

    # --- Indexing -----------------------------------------------------------

    def create_index(self, field: str) -> None:
        """Create a secondary index on *field*."""
        self._index_manager.create_index(field, self._docs)

    def drop_index(self, field: str) -> bool:
        """Drop the index on *field*."""
        return self._index_manager.drop_index(field)

    # --- Aggregation --------------------------------------------------------

    def aggregate(self, pipeline: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Execute an aggregation pipeline over this collection."""
        docs = list(self._docs.values())
        return self._agg_pipeline.execute(docs, pipeline)

    # --- internal -----------------------------------------------------------

    def _iter_match(self, filter: dict[str, Any]):
        """Yield documents matching *filter*, using indexes when possible."""
        candidate_ids = self._try_index_lookup(filter)
        if candidate_ids is not None:
            for doc_id in candidate_ids:
                if doc_id in self._docs:
                    doc = self._docs[doc_id]
                    if self._query_engine.match(doc, filter):
                        yield doc
        else:
            for doc in self._docs.values():
                if self._query_engine.match(doc, filter):
                    yield doc

    def _try_index_lookup(self, filter: dict[str, Any]) -> set[str] | None:
        """Attempt to use indexes to narrow the candidate set."""
        for field, condition in filter.items():
            if field.startswith("$"):
                continue
            if not self._index_manager.has_index(field):
                continue

            if not isinstance(condition, dict):
                # Implicit $eq
                return self._index_manager.lookup_eq(field, condition)

            # Check for operator-based conditions
            ops = set(condition.keys()) if isinstance(condition, dict) else set()
            if "$eq" in ops:
                return self._index_manager.lookup_eq(field, condition["$eq"])
            range_ops = ops & {"$gt", "$gte", "$lt", "$lte"}
            if range_ops:
                kwargs = {}
                for op in range_ops:
                    kwargs[op.lstrip("$")] = condition[op]
                return self._index_manager.lookup_range(field, **kwargs)

        return None

    def _apply_update(self, doc: dict[str, Any], update: dict[str, Any]) -> None:
        """Apply update operators to *doc* in place."""
        for op, fields in update.items():
            if op == "$set":
                for field, value in fields.items():
                    _set_nested(doc, field, value)
            elif op == "$unset":
                for field in fields:
                    _delete_nested(doc, field)
            elif op == "$inc":
                for field, amount in fields.items():
                    current = _get_nested(doc, field)
                    if isinstance(current, _MissingSentinel):
                        current = 0
                    _set_nested(doc, field, current + amount)
            elif op == "$push":
                for field, value in fields.items():
                    current = _get_nested(doc, field)
                    if isinstance(current, _MissingSentinel):
                        _set_nested(doc, field, [value])
                    elif isinstance(current, list):
                        current.append(value)
                    else:
                        raise ValueError(
                            f"Cannot $push to non-array field: {field}"
                        )
            elif op == "$pull":
                for field, value in fields.items():
                    current = _get_nested(doc, field)
                    if isinstance(current, list):
                        while value in current:
                            current.remove(value)
            else:
                raise ValueError(f"Unknown update operator: {op}")


# ---------------------------------------------------------------------------
# Transaction
# ---------------------------------------------------------------------------

class Transaction:
    """Snapshot-isolation transaction over a :class:`Database`.

    Reads see a snapshot taken at ``begin()``. Writes are buffered and applied
    atomically on ``commit()``. Conflict detection prevents lost updates.
    """

    def __init__(self, database: "Database") -> None:
        self._db = database
        self._active = False
        self._snapshot: dict[str, dict[str, dict[str, Any]]] = {}
        # Buffered writes: collection_name -> {doc_id -> doc | None (deleted)}
        self._writes: dict[str, dict[str, dict[str, Any] | None]] = defaultdict(dict)
        # Track which doc_ids were read to detect write-read conflicts
        self._read_ids: dict[str, set[str]] = defaultdict(set)
        # Snapshot version per collection-doc for conflict detection (version counter)
        self._snapshot_versions: dict[str, dict[str, int]] = defaultdict(dict)

    def begin(self) -> None:
        """Take a snapshot of the current database state."""
        if self._active:
            raise RuntimeError("Transaction already active")
        self._active = True
        self._snapshot = {}
        self._writes = defaultdict(dict)
        self._read_ids = defaultdict(set)
        self._snapshot_versions = defaultdict(dict)

        for coll_name, coll in self._db._collections.items():
            self._snapshot[coll_name] = copy.deepcopy(coll._docs)
            for doc_id in coll._docs:
                ver = coll._versions.get(doc_id, 0)
                self._snapshot_versions[coll_name][doc_id] = ver

    def collection(self, name: str) -> "_TransactionCollection":
        """Get a transactional view of a collection."""
        if not self._active:
            raise RuntimeError("Transaction not active")
        return _TransactionCollection(self, name)

    def commit(self) -> None:
        """Apply all buffered writes atomically.

        Raises :class:`RuntimeError` on conflict (a document read during
        the transaction was modified outside it).
        """
        if not self._active:
            raise RuntimeError("Transaction not active")

        # Conflict detection: check if any document we read or wrote has
        # been modified since our snapshot
        for coll_name, doc_versions in self._snapshot_versions.items():
            coll = self._db._collections.get(coll_name)
            if coll is None:
                continue
            touched_ids = self._read_ids.get(coll_name, set()) | set(
                self._writes.get(coll_name, {}).keys()
            )
            for doc_id in touched_ids:
                snap_ver = doc_versions.get(doc_id)
                if snap_ver is None:
                    continue  # New doc inserted in txn, no conflict possible
                current_ver = coll._versions.get(doc_id)

                if current_ver is not None:
                    if snap_ver != current_ver:
                        self._active = False
                        raise RuntimeError(
                            f"Conflict on {coll_name}/{doc_id}: "
                            "document modified outside transaction"
                        )
                else:
                    # Doc was deleted outside transaction
                    self._active = False
                    raise RuntimeError(
                        f"Conflict on {coll_name}/{doc_id}: "
                        "document deleted outside transaction"
                    )

        # Apply buffered writes
        for coll_name, writes in self._writes.items():
            coll = self._db.collection(coll_name)
            for doc_id, doc in writes.items():
                if doc is None:
                    # Delete
                    if doc_id in coll._docs:
                        coll._index_manager.on_delete(doc_id, coll._docs[doc_id])
                        coll._versions.pop(doc_id, None)
                        del coll._docs[doc_id]
                else:
                    # Insert or update
                    if doc_id in coll._docs:
                        old = coll._docs[doc_id]
                        coll._docs[doc_id] = copy.deepcopy(doc)
                        coll._index_manager.on_update(doc_id, old, coll._docs[doc_id])
                        coll._versions[doc_id] = coll._versions.get(doc_id, 0) + 1
                    else:
                        coll._docs[doc_id] = copy.deepcopy(doc)
                        coll._versions[doc_id] = 0
                        coll._index_manager.on_insert(doc_id, coll._docs[doc_id])

        self._active = False

    def rollback(self) -> None:
        """Discard all buffered writes."""
        if not self._active:
            raise RuntimeError("Transaction not active")
        self._writes.clear()
        self._read_ids.clear()
        self._active = False

    @property
    def is_active(self) -> bool:
        return self._active

    def _get_effective_docs(
        self, coll_name: str
    ) -> dict[str, dict[str, Any]]:
        """Return the effective document set (snapshot + buffered writes)."""
        base = copy.deepcopy(self._snapshot.get(coll_name, {}))
        for doc_id, doc in self._writes.get(coll_name, {}).items():
            if doc is None:
                base.pop(doc_id, None)
            else:
                base[doc_id] = copy.deepcopy(doc)
        return base


class _TransactionCollection:
    """A transactional proxy for a collection.

    Reads come from the snapshot (plus buffered writes); writes are buffered.
    """

    def __init__(self, txn: Transaction, name: str) -> None:
        self._txn = txn
        self._name = name
        self._qe = QueryEngine()

    def insert_one(self, document: dict[str, Any]) -> str:
        doc = copy.deepcopy(document)
        if "_id" not in doc:
            doc["_id"] = _generate_id()
        doc_id = doc["_id"]
        self._txn._writes[self._name][doc_id] = doc
        return doc_id

    def find_one(
        self, filter: dict[str, Any] | None = None
    ) -> dict[str, Any] | None:
        docs = self._txn._get_effective_docs(self._name)
        for doc_id, doc in docs.items():
            self._txn._read_ids[self._name].add(doc_id)
            if self._qe.match(doc, filter or {}):
                return copy.deepcopy(doc)
        return None

    def find(
        self, filter: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        docs = self._txn._get_effective_docs(self._name)
        results = []
        for doc_id, doc in docs.items():
            self._txn._read_ids[self._name].add(doc_id)
            if self._qe.match(doc, filter or {}):
                results.append(copy.deepcopy(doc))
        return results

    def update_one(
        self, filter: dict[str, Any], update: dict[str, Any]
    ) -> int:
        docs = self._txn._get_effective_docs(self._name)
        for doc_id, doc in docs.items():
            self._txn._read_ids[self._name].add(doc_id)
            if self._qe.match(doc, filter):
                # Apply update operators
                for op, fields in update.items():
                    if op == "$set":
                        for f, v in fields.items():
                            _set_nested(doc, f, v)
                    elif op == "$inc":
                        for f, amount in fields.items():
                            cur = _get_nested(doc, f)
                            if isinstance(cur, _MissingSentinel):
                                cur = 0
                            _set_nested(doc, f, cur + amount)
                self._txn._writes[self._name][doc_id] = doc
                return 1
        return 0

    def delete_one(self, filter: dict[str, Any]) -> int:
        docs = self._txn._get_effective_docs(self._name)
        for doc_id, doc in docs.items():
            self._txn._read_ids[self._name].add(doc_id)
            if self._qe.match(doc, filter):
                self._txn._writes[self._name][doc_id] = None
                return 1
        return 0

    def count(self, filter: dict[str, Any] | None = None) -> int:
        return len(self.find(filter))


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

class Database:
    """Top-level database containing named collections.

    Usage::

        db = Database()
        users = db.collection("users")
        users.insert_one({"name": "Alice", "age": 30})
        result = users.find({"age": {"$gte": 25}})
    """

    def __init__(self) -> None:
        self._collections: dict[str, Collection] = {}

    def collection(self, name: str) -> Collection:
        """Get or create a collection by name."""
        if name not in self._collections:
            self._collections[name] = Collection(name)
        return self._collections[name]

    def drop_collection(self, name: str) -> bool:
        """Drop a collection. Returns ``True`` if it existed."""
        if name in self._collections:
            del self._collections[name]
            return True
        return False

    def list_collections(self) -> list[str]:
        """Return names of all collections."""
        return list(self._collections.keys())

    def begin_transaction(self) -> Transaction:
        """Start a new transaction with snapshot isolation."""
        txn = Transaction(self)
        txn.begin()
        return txn
