"""In-memory NoSQL database with MongoDB-style API.

A document-oriented database supporting CRUD operations, query engine with
operators, secondary indexes, aggregation pipelines, and transactions with
snapshot isolation.
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
        path: Dot-separated key path (e.g. ``"user.address.city"``).

    Returns:
        The value at *path*, or a sentinel ``_MISSING`` if any key is absent.
    """
    keys = path.split(".")
    current: Any = doc
    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return _MISSING
    return current


def _strict_eq(a: Any, b: Any) -> bool:
    """Strict equality that distinguishes bool from int."""
    if type(a) is not type(b):
        return False
    return a == b


class _MissingSentinel:
    """Sentinel indicating a field does not exist in a document."""

    def __repr__(self) -> str:
        return "<MISSING>"


_MISSING = _MissingSentinel()


def _set_nested(doc: dict[str, Any], path: str, value: Any) -> None:
    """Set a value in a nested dict using dot-notation."""
    keys = path.split(".")
    current = doc
    for key in keys[:-1]:
        if key not in current or not isinstance(current[key], dict):
            current[key] = {}
        current = current[key]
    current[keys[-1]] = value


def _unset_nested(doc: dict[str, Any], path: str) -> None:
    """Remove a field from a nested dict using dot-notation."""
    keys = path.split(".")
    current = doc
    for key in keys[:-1]:
        if not isinstance(current, dict) or key not in current:
            return
        current = current[key]
    if isinstance(current, dict):
        current.pop(keys[-1], None)


# ---------------------------------------------------------------------------
# Query Engine
# ---------------------------------------------------------------------------

class QueryEngine:
    """Evaluates MongoDB-style query filters against documents.

    Supported operators:
        Comparison: ``$eq``, ``$gt``, ``$lt``, ``$gte``, ``$lte``, ``$ne``
        Set: ``$in``, ``$nin``
        Logical: ``$and``, ``$or``, ``$not``, ``$nor``
        Element: ``$exists``

    Dot-notation is supported for nested field access.
    """

    def match(self, doc: dict[str, Any], query: dict[str, Any]) -> bool:
        """Return True if *doc* satisfies *query*."""
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
        """Evaluate a single field condition."""
        if isinstance(condition, dict) and condition:
            # Operator expression
            return all(
                self._eval_operator(value, op, operand)
                for op, operand in condition.items()
            )
        # Implicit $eq
        if isinstance(value, _MissingSentinel):
            return condition is _MISSING
        return _strict_eq(value, condition)

    def _eval_operator(self, value: Any, op: str, operand: Any) -> bool:
        """Evaluate a single operator."""
        if op == "$exists":
            exists = not isinstance(value, _MissingSentinel)
            return exists == bool(operand)

        if op == "$not":
            return not self._match_condition(value, operand)

        # For remaining operators, missing values never match (except $ne)
        is_missing = isinstance(value, _MissingSentinel)

        if op == "$eq":
            return not is_missing and _strict_eq(value, operand)
        if op == "$ne":
            if is_missing:
                return True
            return not _strict_eq(value, operand)
        if op == "$gt":
            return not is_missing and value > operand
        if op == "$gte":
            return not is_missing and value >= operand
        if op == "$lt":
            return not is_missing and value < operand
        if op == "$lte":
            return not is_missing and value <= operand
        if op == "$in":
            return not is_missing and value in operand
        if op == "$nin":
            if is_missing:
                return True
            return value not in operand

        msg = f"Unknown operator: {op}"
        raise ValueError(msg)


# ---------------------------------------------------------------------------
# Index Manager
# ---------------------------------------------------------------------------

def _sort_key(entry: tuple[Any, str]) -> tuple[str, Any, str]:
    """Generate a sort key that handles mixed types gracefully."""
    value, doc_id = entry
    type_name = type(value).__name__
    return (type_name, value, doc_id)


class IndexManager:
    """Maintains secondary indexes for a collection.

    Each index maps field values to sets of document ``_id`` values, with a
    parallel sorted list of keys for range queries.
    """

    def __init__(self) -> None:
        # field_name -> {value -> set of _id}
        self._indexes: dict[str, dict[Any, set[str]]] = {}
        # field_name -> sorted list of (value, _id) for range queries
        self._sorted_keys: dict[str, list[tuple[Any, str]]] = {}

    @property
    def indexed_fields(self) -> set[str]:
        """Return set of currently indexed field names."""
        return set(self._indexes.keys())

    def create_index(self, field: str, documents: dict[str, dict[str, Any]]) -> None:
        """Create an index on *field*, populating it from existing documents."""
        index: dict[Any, set[str]] = defaultdict(set)
        sorted_keys: list[tuple[Any, str]] = []
        for doc_id, doc in documents.items():
            value = _get_nested(doc, field)
            if not isinstance(value, _MissingSentinel):
                index[value].add(doc_id)
                bisect.insort(
                    sorted_keys, (value, doc_id), key=_sort_key
                )
        self._indexes[field] = dict(index)
        self._sorted_keys[field] = sorted_keys

    def drop_index(self, field: str) -> bool:
        """Drop the index on *field*. Returns True if it existed."""
        if field in self._indexes:
            del self._indexes[field]
            del self._sorted_keys[field]
            return True
        return False

    def has_index(self, field: str) -> bool:
        """Return True if *field* is indexed."""
        return field in self._indexes

    def add_document(self, doc_id: str, doc: dict[str, Any]) -> None:
        """Update all indexes for a newly inserted document."""
        for field in self._indexes:
            value = _get_nested(doc, field)
            if not isinstance(value, _MissingSentinel):
                self._indexes[field].setdefault(value, set()).add(doc_id)
                bisect.insort(
                    self._sorted_keys[field],
                    (value, doc_id),
                    key=_sort_key,
                )

    def remove_document(self, doc_id: str, doc: dict[str, Any]) -> None:
        """Remove a document from all indexes."""
        for field in self._indexes:
            value = _get_nested(doc, field)
            if not isinstance(value, _MissingSentinel):
                ids = self._indexes[field].get(value)
                if ids:
                    ids.discard(doc_id)
                    if not ids:
                        del self._indexes[field][value]
                try:
                    self._sorted_keys[field].remove((value, doc_id))
                except ValueError:
                    pass

    def update_document(
        self,
        doc_id: str,
        old_doc: dict[str, Any],
        new_doc: dict[str, Any],
    ) -> None:
        """Update indexes when a document changes."""
        self.remove_document(doc_id, old_doc)
        self.add_document(doc_id, new_doc)

    def lookup_eq(self, field: str, value: Any) -> set[str] | None:
        """Return document IDs where *field* == *value*, or None if not indexed."""
        if field not in self._indexes:
            return None
        return set(self._indexes[field].get(value, set()))

    def lookup_range(
        self,
        field: str,
        *,
        gt: Any = None,
        gte: Any = None,
        lt: Any = None,
        lte: Any = None,
    ) -> set[str] | None:
        """Return document IDs matching a range query on an indexed field."""
        if field not in self._sorted_keys:
            return None
        keys = self._sorted_keys[field]
        if not keys:
            return set()

        # For mixed-type safety, do a linear scan
        result: set[str] = set()
        for value, doc_id in keys:
            try:
                if gt is not None and not (value > gt):
                    continue
                if gte is not None and not (value >= gte):
                    continue
                if lt is not None and not (value < lt):
                    continue
                if lte is not None and not (value <= lte):
                    continue
                result.add(doc_id)
            except TypeError:
                continue
        return result


# ---------------------------------------------------------------------------
# Aggregation Pipeline
# ---------------------------------------------------------------------------

class AggregationPipeline:
    """Executes MongoDB-style aggregation pipelines.

    Supported stages: ``$match``, ``$group``, ``$sort``, ``$limit``,
    ``$skip``, ``$project``, ``$unwind``, ``$count``.

    Supported ``$group`` accumulators: ``$sum``, ``$avg``, ``$min``,
    ``$max``, ``$push``, ``$count``.
    """

    def __init__(self, query_engine: QueryEngine) -> None:
        self._qe = query_engine

    def execute(
        self,
        documents: list[dict[str, Any]],
        pipeline: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Run *pipeline* stages sequentially over *documents*."""
        result = [copy.deepcopy(d) for d in documents]
        for stage in pipeline:
            if len(stage) != 1:
                msg = f"Each stage must have exactly one key, got {list(stage.keys())}"
                raise ValueError(msg)
            stage_name = next(iter(stage))
            stage_spec = stage[stage_name]
            handler = getattr(self, f"_stage{stage_name.replace('$', '_')}", None)
            if handler is None:
                msg = f"Unknown aggregation stage: {stage_name}"
                raise ValueError(msg)
            result = handler(result, stage_spec)
        return result

    # -- stages -------------------------------------------------------------

    def _stage_match(
        self, docs: list[dict[str, Any]], spec: dict[str, Any]
    ) -> list[dict[str, Any]]:
        return [d for d in docs if self._qe.match(d, spec)]

    def _stage_sort(
        self, docs: list[dict[str, Any]], spec: dict[str, Any]
    ) -> list[dict[str, Any]]:
        for field, direction in reversed(list(spec.items())):
            docs = sorted(
                docs,
                key=lambda d, f=field: (
                    _get_nested(d, f)
                    if not isinstance(_get_nested(d, f), _MissingSentinel)
                    else None
                ),
                reverse=(direction == -1),
            )
        return docs

    def _stage_limit(
        self, docs: list[dict[str, Any]], spec: int
    ) -> list[dict[str, Any]]:
        return docs[:spec]

    def _stage_skip(
        self, docs: list[dict[str, Any]], spec: int
    ) -> list[dict[str, Any]]:
        return docs[spec:]

    def _stage_project(
        self, docs: list[dict[str, Any]], spec: dict[str, Any]
    ) -> list[dict[str, Any]]:
        # Determine if this is inclusion or exclusion mode
        non_id = {k: v for k, v in spec.items() if k != "_id"}
        is_exclusion = all(not v for v in non_id.values()) if non_id else False

        result: list[dict[str, Any]] = []
        for doc in docs:
            if is_exclusion:
                # Copy doc, remove excluded fields
                projected = copy.deepcopy(doc)
                for field, include in spec.items():
                    if not include:
                        _unset_nested(projected, field)
            else:
                # Include only specified fields
                projected = {}
                for field, include in spec.items():
                    if include and field != "_id":
                        value = _get_nested(doc, field)
                        if not isinstance(value, _MissingSentinel):
                            _set_nested(projected, field, value)
                # Always include _id unless explicitly excluded
                if spec.get("_id", 1):
                    if "_id" in doc:
                        projected["_id"] = doc["_id"]
            result.append(projected)
        return result

    def _stage_unwind(
        self, docs: list[dict[str, Any]], spec: str
    ) -> list[dict[str, Any]]:
        field = spec.lstrip("$")
        result: list[dict[str, Any]] = []
        for doc in docs:
            value = _get_nested(doc, field)
            if isinstance(value, list):
                for item in value:
                    new_doc = copy.deepcopy(doc)
                    _set_nested(new_doc, field, item)
                    result.append(new_doc)
            elif not isinstance(value, _MissingSentinel):
                result.append(copy.deepcopy(doc))
        return result

    def _stage_count(
        self, docs: list[dict[str, Any]], spec: str
    ) -> list[dict[str, Any]]:
        return [{spec: len(docs)}]

    def _stage_group(
        self, docs: list[dict[str, Any]], spec: dict[str, Any]
    ) -> list[dict[str, Any]]:
        group_key = spec["_id"]
        accumulators = {k: v for k, v in spec.items() if k != "_id"}

        groups: dict[Any, list[dict[str, Any]]] = defaultdict(list)
        for doc in docs:
            if group_key is None:
                key: Any = None
            elif isinstance(group_key, str) and group_key.startswith("$"):
                key = _get_nested(doc, group_key[1:])
                if isinstance(key, _MissingSentinel):
                    key = None
            elif isinstance(group_key, dict):
                key_parts = {}
                for k, v in group_key.items():
                    if isinstance(v, str) and v.startswith("$"):
                        val = _get_nested(doc, v[1:])
                        is_missing = isinstance(val, _MissingSentinel)
                        key_parts[k] = None if is_missing else val
                    else:
                        key_parts[k] = v
                key = tuple(sorted(key_parts.items()))
            else:
                key = group_key
            groups[key].append(doc)

        result: list[dict[str, Any]] = []
        for key, group_docs in groups.items():
            output: dict[str, Any] = {}
            if isinstance(key, tuple):
                output["_id"] = dict(key)
            else:
                output["_id"] = key
            for acc_field, acc_spec in accumulators.items():
                output[acc_field] = self._apply_accumulator(acc_spec, group_docs)
            result.append(output)
        return result

    def _apply_accumulator(
        self, acc_spec: dict[str, Any], docs: list[dict[str, Any]]
    ) -> Any:
        """Evaluate a single group accumulator."""
        acc_op = next(iter(acc_spec))
        acc_expr = acc_spec[acc_op]

        if acc_op == "$count":
            return len(docs)

        values = self._extract_values(acc_expr, docs)

        if acc_op == "$sum":
            if isinstance(acc_expr, (int, float)):
                return acc_expr * len(docs)
            return sum(v for v in values if isinstance(v, (int, float)))
        if acc_op == "$avg":
            nums = [v for v in values if isinstance(v, (int, float))]
            return sum(nums) / len(nums) if nums else 0
        if acc_op == "$min":
            nums = [v for v in values if v is not None]
            return min(nums) if nums else None
        if acc_op == "$max":
            nums = [v for v in values if v is not None]
            return max(nums) if nums else None
        if acc_op == "$push":
            return values

        msg = f"Unknown accumulator: {acc_op}"
        raise ValueError(msg)

    def _extract_values(
        self, expr: Any, docs: list[dict[str, Any]]
    ) -> list[Any]:
        """Extract field values from docs for an accumulator expression."""
        if isinstance(expr, str) and expr.startswith("$"):
            field = expr[1:]
            result = []
            for d in docs:
                v = _get_nested(d, field)
                result.append(None if isinstance(v, _MissingSentinel) else v)
            return result
        return [expr] * len(docs)


# ---------------------------------------------------------------------------
# Collection
# ---------------------------------------------------------------------------

class Collection:
    """A named collection of documents.

    Supports CRUD operations, query filtering, secondary indexes, and
    aggregation pipelines.
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self._documents: dict[str, dict[str, Any]] = {}
        self._query_engine = QueryEngine()
        self._index_manager = IndexManager()
        self._agg_pipeline = AggregationPipeline(self._query_engine)

    # -- CRUD ---------------------------------------------------------------

    def insert_one(self, document: dict[str, Any]) -> str:
        """Insert a single document. Returns the ``_id``."""
        doc = copy.deepcopy(document)
        if "_id" not in doc:
            doc["_id"] = _generate_id()
        doc_id = str(doc["_id"])
        if doc_id in self._documents:
            msg = f"Duplicate _id: {doc_id}"
            raise ValueError(msg)
        self._documents[doc_id] = doc
        self._index_manager.add_document(doc_id, doc)
        return doc_id

    def insert(self, document: dict[str, Any]) -> dict[str, Any]:
        """Insert a document and return the stored copy (with ``_id``)."""
        doc_id = self.insert_one(document)
        return copy.deepcopy(self._documents[doc_id])

    def insert_many(self, documents: list[dict[str, Any]]) -> list[str]:
        """Insert multiple documents. Returns list of ``_id`` values."""
        return [self.insert_one(d) for d in documents]

    def find_one(
        self, query: dict[str, Any] | None = None
    ) -> dict[str, Any] | None:
        """Return the first document matching *query*, or None."""
        query = query or {}
        for doc in self._documents.values():
            if self._query_engine.match(doc, query):
                return copy.deepcopy(doc)
        return None

    def find(self, query: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Return all documents matching *query*."""
        query = query or {}

        # Attempt index-accelerated lookup for simple equality queries
        candidate_ids = self._try_index_lookup(query)
        if candidate_ids is not None:
            docs = [
                self._documents[did]
                for did in candidate_ids
                if did in self._documents
            ]
        else:
            docs = list(self._documents.values())

        return [copy.deepcopy(d) for d in docs if self._query_engine.match(d, query)]

    def _try_index_lookup(self, query: dict[str, Any]) -> set[str] | None:
        """Attempt to use an index for a simple query. Returns candidate IDs or None."""
        for field, condition in query.items():
            if field.startswith("$"):
                continue
            if self._index_manager.has_index(field):
                if not isinstance(condition, dict):
                    # Implicit $eq
                    return self._index_manager.lookup_eq(field, condition)
                # Check for range operators
                range_ops = {"$gt", "$gte", "$lt", "$lte"}
                if condition.keys() & range_ops:
                    return self._index_manager.lookup_range(
                        field,
                        gt=condition.get("$gt"),
                        gte=condition.get("$gte"),
                        lt=condition.get("$lt"),
                        lte=condition.get("$lte"),
                    )
                if "$eq" in condition:
                    return self._index_manager.lookup_eq(field, condition["$eq"])
        return None

    def update_one(
        self, query: dict[str, Any], update: dict[str, Any]
    ) -> int:
        """Update the first matching document. Returns modified count."""
        for doc_id, doc in self._documents.items():
            if self._query_engine.match(doc, query):
                old_doc = copy.deepcopy(doc)
                self._apply_update(doc, update)
                self._index_manager.update_document(doc_id, old_doc, doc)
                return 1
        return 0

    def update_many(
        self, query: dict[str, Any], update: dict[str, Any]
    ) -> int:
        """Update all documents matching *query*. Returns count of modified docs."""
        count = 0
        for doc_id, doc in self._documents.items():
            if self._query_engine.match(doc, query):
                old_doc = copy.deepcopy(doc)
                self._apply_update(doc, update)
                self._index_manager.update_document(doc_id, old_doc, doc)
                count += 1
        return count

    def delete_one(self, query: dict[str, Any]) -> int:
        """Delete the first document matching *query*. Returns count of deleted docs."""
        for doc_id, doc in list(self._documents.items()):
            if self._query_engine.match(doc, query):
                self._index_manager.remove_document(doc_id, doc)
                del self._documents[doc_id]
                return 1
        return 0

    def update(
        self, query: dict[str, Any], update: dict[str, Any]
    ) -> int:
        """Update one matching document. Alias for ``update_one``."""
        return self.update_one(query, update)

    def delete(self, query: dict[str, Any]) -> int:
        """Delete one matching document. Alias for ``delete_one``."""
        return self.delete_one(query)

    def delete_many(self, query: dict[str, Any]) -> int:
        """Delete all documents matching *query*. Returns count of deleted docs."""
        to_delete = [
            (doc_id, doc)
            for doc_id, doc in self._documents.items()
            if self._query_engine.match(doc, query)
        ]
        for doc_id, doc in to_delete:
            self._index_manager.remove_document(doc_id, doc)
            del self._documents[doc_id]
        return len(to_delete)

    def count(self, query: dict[str, Any] | None = None) -> int:
        """Return the number of documents matching *query*."""
        if not query:
            return len(self._documents)
        return len(self.find(query))

    # -- Update operators ---------------------------------------------------

    def _apply_update(self, doc: dict[str, Any], update: dict[str, Any]) -> None:
        """Apply update operators to a document in place."""
        for op, fields in update.items():
            if op == "$set":
                for field, value in fields.items():
                    _set_nested(doc, field, value)
            elif op == "$unset":
                for field in fields:
                    _unset_nested(doc, field)
            elif op == "$inc":
                for field, value in fields.items():
                    current = _get_nested(doc, field)
                    if isinstance(current, _MissingSentinel):
                        current = 0
                    _set_nested(doc, field, current + value)
            elif op == "$push":
                for field, value in fields.items():
                    current = _get_nested(doc, field)
                    if isinstance(current, _MissingSentinel):
                        _set_nested(doc, field, [value])
                    elif isinstance(current, list):
                        current.append(value)
                    else:
                        msg = f"Cannot $push to non-array field: {field}"
                        raise TypeError(msg)
            elif op == "$pull":
                for field, value in fields.items():
                    current = _get_nested(doc, field)
                    if isinstance(current, list):
                        while value in current:
                            current.remove(value)
            elif op == "$addToSet":
                for field, value in fields.items():
                    current = _get_nested(doc, field)
                    if isinstance(current, _MissingSentinel):
                        _set_nested(doc, field, [value])
                    elif isinstance(current, list):
                        if value not in current:
                            current.append(value)
            else:
                msg = f"Unknown update operator: {op}"
                raise ValueError(msg)

    # -- Indexes ------------------------------------------------------------

    def create_index(self, field: str) -> None:
        """Create a secondary index on *field*."""
        self._index_manager.create_index(field, self._documents)

    def drop_index(self, field: str) -> bool:
        """Drop the index on *field*."""
        return self._index_manager.drop_index(field)

    # -- Aggregation --------------------------------------------------------

    def aggregate(self, pipeline: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Execute an aggregation pipeline over the collection."""
        docs = list(self._documents.values())
        return self._agg_pipeline.execute(docs, pipeline)


# ---------------------------------------------------------------------------
# Transaction
# ---------------------------------------------------------------------------

class Transaction:
    """Provides snapshot isolation over a :class:`Database`.

    Reads see the database state at the time of ``begin()``. Writes are
    buffered until ``commit()`` which applies them atomically.  If any
    document modified by the transaction was also modified outside the
    transaction after the snapshot was taken, ``commit()`` raises a
    ``ConflictError``.
    """

    def __init__(self, database: Database) -> None:
        self._db = database
        self._active = False
        # Snapshot: collection_name -> {doc_id -> doc}
        self._snapshot: dict[str, dict[str, dict[str, Any]]] = {}
        # Buffered writes: collection_name -> {doc_id -> doc | None}
        self._writes: dict[str, dict[str, dict[str, Any] | None]] = {}
        # Track original versions for conflict detection
        self._read_versions: dict[str, dict[str, dict[str, Any] | None]] = {}

    def begin(self) -> Transaction:
        """Take a snapshot and begin the transaction."""
        if self._active:
            msg = "Transaction already active"
            raise RuntimeError(msg)
        self._active = True
        self._snapshot = {}
        self._writes = {}
        self._read_versions = {}
        for coll_name, coll in self._db._collections.items():
            self._snapshot[coll_name] = copy.deepcopy(coll._documents)
            self._read_versions[coll_name] = copy.deepcopy(coll._documents)
        return self

    def collection(self, name: str) -> _TransactionCollectionProxy:
        """Return a proxy for the named collection within this transaction."""
        self._ensure_active()
        if name not in self._snapshot:
            self._snapshot[name] = {}
            self._read_versions[name] = {}
        return _TransactionCollectionProxy(self, name)

    # -- Convenience shorthand methods ------------------------------------

    def insert(
        self, collection: str, document: dict[str, Any]
    ) -> str:
        """Insert a document into the named collection."""
        return self.collection(collection).insert_one(document)

    def find(
        self,
        collection: str,
        query: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Find documents in the named collection."""
        return self.collection(collection).find(query)

    def update(
        self,
        collection: str,
        query: dict[str, Any],
        update: dict[str, Any],
    ) -> int:
        """Update one document in the named collection."""
        return self.collection(collection).update_one(query, update)

    def delete(self, collection: str, query: dict[str, Any]) -> int:
        """Delete one document from the named collection."""
        return self.collection(collection).delete_one(query)

    def commit(self) -> None:
        """Apply all buffered writes atomically, with conflict detection."""
        self._ensure_active()
        try:
            # Check for conflicts: if any document we wrote to was changed
            # outside the transaction since our snapshot, raise ConflictError
            for coll_name, writes in self._writes.items():
                coll = self._db._collections.get(coll_name)
                if coll is None:
                    # Collection didn't exist at snapshot time either —
                    # this is a new collection created inside the txn.
                    snap = self._read_versions.get(coll_name, {})
                    if snap:
                        # Collection existed at snapshot but was dropped
                        msg = (
                            f"Collection '{coll_name}' no longer exists"
                        )
                        raise ConflictError(msg)
                    # New collection — no conflict possible, skip checks
                    continue
                for doc_id in writes:
                    snap_doc = self._read_versions.get(coll_name, {}).get(doc_id)
                    current_doc = coll._documents.get(doc_id)
                    if snap_doc != current_doc:
                        msg = (
                            f"Conflict on {coll_name}/{doc_id}: "
                            "document was modified outside transaction"
                        )
                        raise ConflictError(msg)

            # Apply writes
            for coll_name, writes in self._writes.items():
                coll = self._db.collection(coll_name)
                for doc_id, doc in writes.items():
                    if doc is None:
                        # Delete
                        old = coll._documents.pop(doc_id, None)
                        if old is not None:
                            coll._index_manager.remove_document(doc_id, old)
                    else:
                        old = coll._documents.get(doc_id)
                        coll._documents[doc_id] = copy.deepcopy(doc)
                        if old is not None:
                            coll._index_manager.update_document(doc_id, old, doc)
                        else:
                            coll._index_manager.add_document(doc_id, doc)
        finally:
            self._active = False
            self._snapshot = {}
            self._writes = {}
            self._read_versions = {}

    def rollback(self) -> None:
        """Discard all buffered writes."""
        self._active = False
        self._snapshot = {}
        self._writes = {}
        self._read_versions = {}

    def _ensure_active(self) -> None:
        if not self._active:
            msg = "Transaction is not active"
            raise RuntimeError(msg)

    def _get_snapshot_docs(self, coll_name: str) -> dict[str, dict[str, Any]]:
        """Get the snapshot documents for a collection, overlaid with writes."""
        base = dict(self._snapshot.get(coll_name, {}))
        for doc_id, doc in self._writes.get(coll_name, {}).items():
            if doc is None:
                base.pop(doc_id, None)
            else:
                base[doc_id] = doc
        return base

    def _buffer_write(
        self, coll_name: str, doc_id: str, doc: dict[str, Any] | None
    ) -> None:
        """Buffer a write (insert/update/delete) in the transaction."""
        self._writes.setdefault(coll_name, {})[doc_id] = doc


class ConflictError(Exception):
    """Raised when a transaction commit detects a write conflict."""


class _TransactionCollectionProxy:
    """Provides collection-level operations within a transaction."""

    def __init__(self, txn: Transaction, coll_name: str) -> None:
        self._txn = txn
        self._coll_name = coll_name
        self._qe = QueryEngine()

    def insert_one(self, document: dict[str, Any]) -> str:
        """Insert a document within the transaction."""
        doc = copy.deepcopy(document)
        if "_id" not in doc:
            doc["_id"] = _generate_id()
        doc_id = str(doc["_id"])
        # Check for duplicates in snapshot + buffered writes
        current_docs = self._txn._get_snapshot_docs(self._coll_name)
        if doc_id in current_docs:
            msg = f"Duplicate _id: {doc_id}"
            raise ValueError(msg)
        self._txn._buffer_write(self._coll_name, doc_id, doc)
        return doc_id

    def find(self, query: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Find documents within the transaction snapshot."""
        query = query or {}
        docs = self._txn._get_snapshot_docs(self._coll_name)
        return [
            copy.deepcopy(d)
            for d in docs.values()
            if self._qe.match(d, query)
        ]

    def find_one(
        self, query: dict[str, Any] | None = None
    ) -> dict[str, Any] | None:
        """Find one document within the transaction snapshot."""
        results = self.find(query)
        return results[0] if results else None

    def update_one(self, query: dict[str, Any], update: dict[str, Any]) -> int:
        """Update one document within the transaction."""
        docs = self._txn._get_snapshot_docs(self._coll_name)
        for doc_id, doc in docs.items():
            if self._qe.match(doc, query):
                new_doc = copy.deepcopy(doc)
                # Re-use Collection's update logic
                coll = Collection("_tmp")
                coll._apply_update(new_doc, update)
                self._txn._buffer_write(self._coll_name, doc_id, new_doc)
                return 1
        return 0

    def delete_one(self, query: dict[str, Any]) -> int:
        """Delete one document within the transaction."""
        docs = self._txn._get_snapshot_docs(self._coll_name)
        for doc_id, doc in docs.items():
            if self._qe.match(doc, query):
                self._txn._buffer_write(self._coll_name, doc_id, None)
                return 1
        return 0

    def count(self, query: dict[str, Any] | None = None) -> int:
        """Count documents matching *query* in the transaction snapshot."""
        return len(self.find(query))


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

class Database:
    """Top-level in-memory NoSQL database.

    Manages named collections and provides transaction support.

    Usage::

        db = Database()
        users = db.collection("users")
        users.insert_one({"name": "Alice", "age": 30})
        docs = users.find({"age": {"$gte": 25}})
    """

    def __init__(self) -> None:
        self._collections: dict[str, Collection] = {}

    def collection(self, name: str) -> Collection:
        """Return the named collection, creating it if necessary."""
        if name not in self._collections:
            self._collections[name] = Collection(name)
        return self._collections[name]

    def drop_collection(self, name: str) -> bool:
        """Drop a collection. Returns True if it existed."""
        if name in self._collections:
            del self._collections[name]
            return True
        return False

    def list_collections(self) -> list[str]:
        """Return names of all collections."""
        return sorted(self._collections.keys())

    def create_collection(self, name: str) -> Collection:
        """Explicitly create a named collection."""
        return self.collection(name)

    def get_collection(self, name: str) -> Collection:
        """Return an existing collection by name."""
        return self.collection(name)

    # -- Convenience shorthand methods ------------------------------------

    def insert(
        self, collection: str, document: dict[str, Any]
    ) -> str:
        """Insert a document into the named collection."""
        return self.collection(collection).insert_one(document)

    def find(
        self, collection: str, query: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Find documents in the named collection."""
        return self.collection(collection).find(query)

    def update(
        self,
        collection: str,
        query: dict[str, Any],
        update: dict[str, Any],
    ) -> int:
        """Update one document in the named collection."""
        return self.collection(collection).update_one(query, update)

    def delete(
        self, collection: str, query: dict[str, Any]
    ) -> int:
        """Delete one document from the named collection."""
        return self.collection(collection).delete_one(query)

    def begin_transaction(self) -> Transaction:
        """Create and begin a new transaction."""
        return Transaction(self).begin()
