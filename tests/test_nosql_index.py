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
        col = db.collection("users")
        col.insert_one({"name": "Alice", "age": 30})
        col.insert_one({"name": "Bob", "age": 25})
        col.create_index("age")
        results = col.find({"age": 30})
        assert len(results) == 1
        assert results[0]["name"] == "Alice"

    def test_create_index_then_insert(self, db: Database) -> None:
        """Create index first, insert docs, verify find returns correct results."""
        col = db.collection("users")
        col.create_index("name")
        col.insert_one({"name": "Alice", "age": 30})
        col.insert_one({"name": "Bob", "age": 25})
        col.insert_one({"name": "Alice", "age": 40})
        results = col.find({"name": "Alice"})
        assert len(results) == 2
        assert all(r["name"] == "Alice" for r in results)

    def test_drop_index_queries_still_work(self, db: Database) -> None:
        """After dropping an index, queries should still return correct results."""
        col = db.collection("items")
        col.insert_one({"category": "books", "price": 10})
        col.insert_one({"category": "toys", "price": 20})
        col.create_index("category")
        col.drop_index("category")
        results = col.find({"category": "books"})
        assert len(results) == 1
        assert results[0]["price"] == 10

    def test_create_index_on_nested_field(self, db: Database) -> None:
        """Index on a nested field using dot notation."""
        col = db.collection("users")
        col.insert_one({"name": "Alice", "address": {"city": "NYC"}})
        col.insert_one({"name": "Bob", "address": {"city": "LA"}})
        col.create_index("address.city")
        results = col.find({"address.city": "NYC"})
        assert len(results) == 1
        assert results[0]["name"] == "Alice"

    def test_create_index_on_field_missing_from_some_docs(self, db: Database) -> None:
        """Index a field that not all documents have."""
        col = db.collection("items")
        col.insert_one({"name": "Alice", "score": 100})
        col.insert_one({"name": "Bob"})  # no score field
        col.create_index("score")
        results = col.find({"score": 100})
        assert len(results) == 1
        assert results[0]["name"] == "Alice"

    def test_multiple_indexes_on_same_collection(self, db: Database) -> None:
        """Multiple indexes can coexist on the same collection."""
        col = db.collection("users")
        col.insert_one({"name": "Alice", "age": 30, "city": "NYC"})
        col.insert_one({"name": "Bob", "age": 25, "city": "LA"})
        col.create_index("name")
        col.create_index("age")
        col.create_index("city")
        assert len(col.find({"name": "Bob"})) == 1
        assert len(col.find({"age": 30})) == 1
        assert len(col.find({"city": "LA"})) == 1

    def test_index_on_id_field(self, db: Database) -> None:
        """Creating an index on _id should work."""
        col = db.collection("items")
        doc_id = col.insert_one({"value": 42})
        col.create_index("_id")
        results = col.find({"_id": doc_id})
        assert len(results) == 1
        assert results[0]["value"] == 42

    def test_duplicate_index_is_idempotent(self, db: Database) -> None:
        """Creating the same index twice should not raise or corrupt data."""
        col = db.collection("items")
        col.insert_one({"x": 1})
        col.create_index("x")
        col.create_index("x")  # duplicate — should be idempotent
        results = col.find({"x": 1})
        assert len(results) == 1

    def test_create_index_on_empty_collection_then_add_docs(self, db: Database) -> None:
        """Index created on empty collection should work after docs are added."""
        col = db.collection("items")
        col.create_index("tag")
        col.insert_one({"tag": "a"})
        col.insert_one({"tag": "b"})
        col.insert_one({"tag": "a"})
        results = col.find({"tag": "a"})
        assert len(results) == 2


class TestIndexConsistency:
    """Tests that indexes stay consistent after mutations."""

    def test_update_doc_index_consistency(self, db: Database) -> None:
        """After updating a doc, find on the indexed field returns correct results."""
        col = db.collection("users")
        col.create_index("status")
        col.insert_one({"name": "Alice", "status": "active"})
        col.insert_one({"name": "Bob", "status": "active"})
        col.update({"name": "Alice"}, {"$set": {"status": "inactive"}})
        active = col.find({"status": "active"})
        inactive = col.find({"status": "inactive"})
        assert len(active) == 1
        assert active[0]["name"] == "Bob"
        assert len(inactive) == 1
        assert inactive[0]["name"] == "Alice"

    def test_delete_doc_index_consistency(self, db: Database) -> None:
        """After deleting a doc, the index no longer returns it."""
        col = db.collection("items")
        col.create_index("color")
        col.insert_one({"color": "red", "size": 1})
        col.insert_one({"color": "blue", "size": 2})
        col.insert_one({"color": "red", "size": 3})
        col.delete({"color": "red", "size": 1})
        results = col.find({"color": "red"})
        assert len(results) == 1
        assert results[0]["size"] == 3

    def test_insert_many_with_index(self, db: Database) -> None:
        """insert_many should update the index for all inserted docs."""
        col = db.collection("items")
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
        col = db.collection("items")
        col.create_index("level")
        col.insert_one({"name": "x", "level": 1})
        col.update({"name": "x"}, {"$set": {"level": 2}})
        col.update({"name": "x"}, {"$set": {"level": 3}})
        assert len(col.find({"level": 1})) == 0
        assert len(col.find({"level": 2})) == 0
        assert len(col.find({"level": 3})) == 1

    def test_delete_all_docs_index_consistency(self, db: Database) -> None:
        """Deleting all docs that match an indexed value yields empty results."""
        col = db.collection("items")
        col.create_index("k")
        col.insert_one({"k": "v"})
        col.insert_one({"k": "v"})
        col.delete({"k": "v"})
        col.delete({"k": "v"})
        assert len(col.find({"k": "v"})) == 0


class TestIndexEdgeCases:
    """Edge-case tests for index behavior."""

    def test_drop_nonexistent_index(self, db: Database) -> None:
        """Dropping an index that doesn't exist should not crash."""
        col = db.collection("items")
        # Should either be a no-op or raise a specific error
        try:
            col.drop_index("nonexistent")
        except (KeyError, ValueError):
            pass  # acceptable to raise

    def test_index_with_none_values(self, db: Database) -> None:
        """Docs where the indexed field is None should be findable."""
        col = db.collection("items")
        col.create_index("tag")
        col.insert_one({"tag": None})
        col.insert_one({"tag": "a"})
        results = col.find({"tag": None})
        assert len(results) == 1

    def test_index_with_various_value_types(self, db: Database) -> None:
        """Index should handle different value types (int, str, bool)."""
        col = db.collection("items")
        col.create_index("val")
        col.insert_one({"val": 1})
        col.insert_one({"val": "hello"})
        col.insert_one({"val": True})
        col.insert_one({"val": 3.14})
        assert len(col.find({"val": 1})) == 1
        assert len(col.find({"val": "hello"})) == 1

    def test_index_after_bulk_delete(self, db: Database) -> None:
        """Index stays consistent after deleting many docs."""
        col = db.collection("items")
        col.create_index("group")
        for i in range(20):
            col.insert_one({"group": i % 3, "seq": i})
        # Delete all group=0 docs
        for _ in range(7):  # ceil(20/3)
            remaining = col.find({"group": 0})
            if not remaining:
                break
            col.delete({"_id": remaining[0]["_id"]})
        assert len(col.find({"group": 0})) == 0
        assert len(col.find({"group": 1})) == 7
        assert len(col.find({"group": 2})) == 6
