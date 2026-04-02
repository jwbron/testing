"""Tests for the IndexManager in the NoSQL database."""

import pytest
from challenges.nosql_db import Database


@pytest.fixture()
def db() -> Database:
    """Return a fresh Database instance."""
    return Database()


class TestIndexCreation:
    """Tests for creating and dropping indexes."""

    def test_create_index_on_field(self, db: Database) -> None:
        """Create an index on a field and verify queries still work."""
        col = db.create_collection("users")
        col.insert({"name": "Alice", "age": 30})
        col.insert({"name": "Bob", "age": 25})
        col.create_index("age")
        results = col.find({"age": 30})
        assert len(results) == 1
        assert results[0]["name"] == "Alice"

    def test_create_index_then_insert(self, db: Database) -> None:
        """Create index first, insert docs, verify find returns correct results."""
        col = db.create_collection("users")
        col.create_index("name")
        col.insert({"name": "Alice", "age": 30})
        col.insert({"name": "Bob", "age": 25})
        col.insert({"name": "Alice", "age": 40})
        results = col.find({"name": "Alice"})
        assert len(results) == 2
        assert all(r["name"] == "Alice" for r in results)

    def test_drop_index_queries_still_work(self, db: Database) -> None:
        """After dropping an index, queries should still return correct results."""
        col = db.create_collection("items")
        col.insert({"category": "books", "price": 10})
        col.insert({"category": "toys", "price": 20})
        col.create_index("category")
        col.drop_index("category")
        results = col.find({"category": "books"})
        assert len(results) == 1
        assert results[0]["price"] == 10

    def test_create_index_on_nested_field(self, db: Database) -> None:
        """Index on a nested field using dot notation."""
        col = db.create_collection("users")
        col.insert({"name": "Alice", "address": {"city": "NYC"}})
        col.insert({"name": "Bob", "address": {"city": "LA"}})
        col.create_index("address.city")
        results = col.find({"address.city": "NYC"})
        assert len(results) == 1
        assert results[0]["name"] == "Alice"

    def test_create_index_on_field_missing_from_some_docs(self, db: Database) -> None:
        """Index a field that not all documents have."""
        col = db.create_collection("items")
        col.insert({"name": "Alice", "score": 100})
        col.insert({"name": "Bob"})  # no score field
        col.create_index("score")
        results = col.find({"score": 100})
        assert len(results) == 1
        assert results[0]["name"] == "Alice"

    def test_multiple_indexes_on_same_collection(self, db: Database) -> None:
        """Multiple indexes can coexist on the same collection."""
        col = db.create_collection("users")
        col.insert({"name": "Alice", "age": 30, "city": "NYC"})
        col.insert({"name": "Bob", "age": 25, "city": "LA"})
        col.create_index("name")
        col.create_index("age")
        col.create_index("city")
        assert len(col.find({"name": "Bob"})) == 1
        assert len(col.find({"age": 30})) == 1
        assert len(col.find({"city": "LA"})) == 1

    def test_index_on_id_field(self, db: Database) -> None:
        """Creating an index on _id should work."""
        col = db.create_collection("items")
        doc = col.insert({"value": 42})
        doc_id = doc["_id"]
        col.create_index("_id")
        results = col.find({"_id": doc_id})
        assert len(results) == 1
        assert results[0]["value"] == 42

    def test_duplicate_index_is_idempotent(self, db: Database) -> None:
        """Creating the same index twice should not raise or corrupt data."""
        col = db.create_collection("items")
        col.insert({"x": 1})
        col.create_index("x")
        col.create_index("x")  # duplicate — should be idempotent
        results = col.find({"x": 1})
        assert len(results) == 1

    def test_create_index_on_empty_collection_then_add_docs(self, db: Database) -> None:
        """Index created on empty collection should work after docs are added."""
        col = db.create_collection("items")
        col.create_index("tag")
        col.insert({"tag": "a"})
        col.insert({"tag": "b"})
        col.insert({"tag": "a"})
        results = col.find({"tag": "a"})
        assert len(results) == 2


class TestIndexConsistency:
    """Tests that indexes stay consistent after mutations."""

    def test_update_doc_index_consistency(self, db: Database) -> None:
        """After updating a doc, find on the indexed field returns correct results."""
        col = db.create_collection("users")
        col.create_index("status")
        col.insert({"name": "Alice", "status": "active"})
        col.insert({"name": "Bob", "status": "active"})
        col.update({"name": "Alice"}, {"$set": {"status": "inactive"}})
        active = col.find({"status": "active"})
        inactive = col.find({"status": "inactive"})
        assert len(active) == 1
        assert active[0]["name"] == "Bob"
        assert len(inactive) == 1
        assert inactive[0]["name"] == "Alice"

    def test_delete_doc_index_consistency(self, db: Database) -> None:
        """After deleting a doc, the index no longer returns it."""
        col = db.create_collection("items")
        col.create_index("color")
        col.insert({"color": "red", "size": 1})
        col.insert({"color": "blue", "size": 2})
        col.insert({"color": "red", "size": 3})
        col.delete({"color": "red", "size": 1})
        results = col.find({"color": "red"})
        assert len(results) == 1
        assert results[0]["size"] == 3

    def test_insert_many_with_index(self, db: Database) -> None:
        """insert_many should update the index for all inserted docs."""
        col = db.create_collection("items")
        col.create_index("type")
        col.insert_many(
            [
                {"type": "a", "val": 1},
                {"type": "b", "val": 2},
                {"type": "a", "val": 3},
                {"type": "c", "val": 4},
            ]
        )
        assert len(col.find({"type": "a"})) == 2
        assert len(col.find({"type": "b"})) == 1
        assert len(col.find({"type": "c"})) == 1

    def test_multiple_updates_index_consistency(self, db: Database) -> None:
        """Multiple updates to the same doc keep the index accurate."""
        col = db.create_collection("items")
        col.create_index("level")
        col.insert({"name": "x", "level": 1})
        col.update({"name": "x"}, {"$set": {"level": 2}})
        col.update({"name": "x"}, {"$set": {"level": 3}})
        assert len(col.find({"level": 1})) == 0
        assert len(col.find({"level": 2})) == 0
        assert len(col.find({"level": 3})) == 1

    def test_delete_all_docs_index_consistency(self, db: Database) -> None:
        """Deleting all docs that match an indexed value yields empty results."""
        col = db.create_collection("items")
        col.create_index("k")
        col.insert({"k": "v"})
        col.insert({"k": "v"})
        col.delete({"k": "v"})
        col.delete({"k": "v"})
        assert len(col.find({"k": "v"})) == 0


class TestIndexEdgeCases:
    """Edge-case tests for index behavior."""

    def test_drop_nonexistent_index(self, db: Database) -> None:
        """Dropping an index that doesn't exist should not crash."""
        col = db.create_collection("items")
        # Should either be a no-op or raise a specific error
        try:
            col.drop_index("nonexistent")
        except (KeyError, ValueError):
            pass  # acceptable to raise

    def test_index_with_none_values(self, db: Database) -> None:
        """Docs where the indexed field is None should be findable."""
        col = db.create_collection("items")
        col.create_index("tag")
        col.insert({"tag": None})
        col.insert({"tag": "a"})
        results = col.find({"tag": None})
        assert len(results) == 1

    def test_index_with_various_value_types(self, db: Database) -> None:
        """Index should handle different value types (int, str, bool)."""
        col = db.create_collection("items")
        col.create_index("val")
        col.insert({"val": 1})
        col.insert({"val": "hello"})
        col.insert({"val": True})
        col.insert({"val": 3.14})
        assert len(col.find({"val": 1})) == 1
        assert len(col.find({"val": "hello"})) == 1

    def test_index_after_bulk_delete(self, db: Database) -> None:
        """Index stays consistent after deleting many docs."""
        col = db.create_collection("items")
        col.create_index("group")
        for i in range(20):
            col.insert({"group": i % 3, "seq": i})
        # Delete all group=0 docs
        for _ in range(7):  # ceil(20/3)
            remaining = col.find({"group": 0})
            if not remaining:
                break
            col.delete({"_id": remaining[0]["_id"]})
        assert len(col.find({"group": 0})) == 0
        assert len(col.find({"group": 1})) == 7
        assert len(col.find({"group": 2})) == 6
