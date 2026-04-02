"""Tests for Database and Collection CRUD operations."""

from challenges.nosql_db import Database


class TestInsertOperations:
    """Tests for insert_one and insert_many."""

    def test_insert_one_returns_id(self) -> None:
        db = Database()
        col = db.collection("users")
        doc_id = col.insert_one({"name": "Alice", "age": 30})
        assert doc_id is not None

    def test_insert_one_document_retrievable_by_id(self) -> None:
        db = Database()
        col = db.collection("users")
        doc_id = col.insert_one({"name": "Alice", "age": 30})
        result = col.find_one({"_id": doc_id})
        assert result is not None
        assert result["name"] == "Alice"
        assert result["age"] == 30

    def test_insert_one_auto_generates_id(self) -> None:
        db = Database()
        col = db.collection("users")
        doc_id = col.insert_one({"name": "Bob"})
        result = col.find_one({"_id": doc_id})
        assert "_id" in result
        assert result["_id"] == doc_id

    def test_insert_one_preserves_existing_id(self) -> None:
        db = Database()
        col = db.collection("users")
        custom_id = "my-custom-id-123"
        returned_id = col.insert_one({"_id": custom_id, "name": "Carol"})
        assert returned_id == custom_id
        result = col.find_one({"_id": custom_id})
        assert result is not None
        assert result["name"] == "Carol"

    def test_insert_one_does_not_mutate_original_doc(self) -> None:
        db = Database()
        col = db.collection("users")
        doc = {"name": "Dave"}
        col.insert_one(doc)
        # Original doc should not have _id added
        assert "_id" not in doc

    def test_insert_many_returns_list_of_ids(self) -> None:
        db = Database()
        col = db.collection("users")
        docs = [{"name": "Alice"}, {"name": "Bob"}, {"name": "Carol"}]
        ids = col.insert_many(docs)
        assert isinstance(ids, list)
        assert len(ids) == 3
        assert len(set(ids)) == 3  # All unique

    def test_insert_many_documents_retrievable(self) -> None:
        db = Database()
        col = db.collection("users")
        docs = [{"name": "Alice"}, {"name": "Bob"}]
        ids = col.insert_many(docs)
        for doc_id, name in zip(ids, ["Alice", "Bob"]):
            result = col.find_one({"_id": doc_id})
            assert result is not None
            assert result["name"] == name


class TestFindOperations:
    """Tests for find_one and find."""

    def test_find_one_returns_none_when_no_match(self) -> None:
        db = Database()
        col = db.collection("users")
        col.insert_one({"name": "Alice"})
        result = col.find_one({"name": "NonExistent"})
        assert result is None

    def test_find_one_returns_matching_document(self) -> None:
        db = Database()
        col = db.collection("users")
        col.insert_one({"name": "Alice", "age": 30})
        col.insert_one({"name": "Bob", "age": 25})
        result = col.find_one({"name": "Bob"})
        assert result is not None
        assert result["name"] == "Bob"
        assert result["age"] == 25

    def test_find_empty_filter_returns_all_docs(self) -> None:
        db = Database()
        col = db.collection("users")
        col.insert_many([{"name": "Alice"}, {"name": "Bob"}, {"name": "Carol"}])
        results = col.find({})
        assert len(results) == 3

    def test_find_with_filter_returns_matching_only(self) -> None:
        db = Database()
        col = db.collection("users")
        col.insert_many(
            [
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 25},
                {"name": "Carol", "age": 30},
            ]
        )
        results = col.find({"age": 30})
        assert len(results) == 2
        names = {r["name"] for r in results}
        assert names == {"Alice", "Carol"}

    def test_find_on_empty_collection(self) -> None:
        db = Database()
        col = db.collection("users")
        results = col.find({})
        assert results == []

    def test_find_one_on_empty_collection(self) -> None:
        db = Database()
        col = db.collection("users")
        result = col.find_one({"name": "Alice"})
        assert result is None

    def test_find_no_matches_returns_empty_list(self) -> None:
        db = Database()
        col = db.collection("users")
        col.insert_one({"name": "Alice"})
        results = col.find({"name": "NonExistent"})
        assert results == []

    def test_find_with_multiple_filter_fields(self) -> None:
        db = Database()
        col = db.collection("users")
        col.insert_many(
            [
                {"name": "Alice", "age": 30, "city": "NYC"},
                {"name": "Bob", "age": 30, "city": "LA"},
                {"name": "Carol", "age": 25, "city": "NYC"},
            ]
        )
        results = col.find({"age": 30, "city": "NYC"})
        assert len(results) == 1
        assert results[0]["name"] == "Alice"


class TestUpdateOperations:
    """Tests for update_one and update_many."""

    def test_update_one_modifies_matching_doc(self) -> None:
        db = Database()
        col = db.collection("users")
        col.insert_one({"name": "Alice", "age": 30})
        modified = col.update_one({"name": "Alice"}, {"$set": {"age": 31}})
        assert modified == 1
        result = col.find_one({"name": "Alice"})
        assert result["age"] == 31

    def test_update_one_no_match_returns_zero(self) -> None:
        db = Database()
        col = db.collection("users")
        col.insert_one({"name": "Alice", "age": 30})
        modified = col.update_one({"name": "NonExistent"}, {"$set": {"age": 99}})
        assert modified == 0

    def test_update_one_modifies_only_first_match(self) -> None:
        db = Database()
        col = db.collection("users")
        col.insert_many(
            [
                {"name": "Alice", "age": 30},
                {"name": "Alice", "age": 25},
            ]
        )
        modified = col.update_one({"name": "Alice"}, {"$set": {"age": 99}})
        assert modified == 1
        results = col.find({"name": "Alice"})
        ages = sorted([r["age"] for r in results])
        assert 99 in ages
        # One should be unchanged
        assert len([a for a in ages if a != 99]) == 1

    def test_update_many_modifies_all_matching(self) -> None:
        db = Database()
        col = db.collection("users")
        col.insert_many(
            [
                {"name": "Alice", "status": "active"},
                {"name": "Bob", "status": "active"},
                {"name": "Carol", "status": "inactive"},
            ]
        )
        modified = col.update_many(
            {"status": "active"}, {"$set": {"status": "archived"}}
        )
        assert modified == 2
        results = col.find({"status": "archived"})
        assert len(results) == 2

    def test_update_many_no_matches_returns_zero(self) -> None:
        db = Database()
        col = db.collection("users")
        col.insert_one({"name": "Alice"})
        modified = col.update_many({"name": "NonExistent"}, {"$set": {"age": 99}})
        assert modified == 0

    def test_update_on_empty_collection(self) -> None:
        db = Database()
        col = db.collection("users")
        modified = col.update_one({"name": "Alice"}, {"$set": {"age": 30}})
        assert modified == 0


class TestDeleteOperations:
    """Tests for delete_one and delete_many."""

    def test_delete_one_removes_matching_doc(self) -> None:
        db = Database()
        col = db.collection("users")
        col.insert_one({"name": "Alice"})
        deleted = col.delete_one({"name": "Alice"})
        assert deleted == 1
        assert col.find_one({"name": "Alice"}) is None

    def test_delete_one_removes_only_first_match(self) -> None:
        db = Database()
        col = db.collection("users")
        col.insert_many([{"name": "Alice"}, {"name": "Alice"}])
        deleted = col.delete_one({"name": "Alice"})
        assert deleted == 1
        assert col.count({"name": "Alice"}) == 1

    def test_delete_one_no_match_returns_zero(self) -> None:
        db = Database()
        col = db.collection("users")
        col.insert_one({"name": "Alice"})
        deleted = col.delete_one({"name": "NonExistent"})
        assert deleted == 0
        assert col.count() == 1

    def test_delete_many_removes_all_matching(self) -> None:
        db = Database()
        col = db.collection("users")
        col.insert_many(
            [
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 30},
                {"name": "Carol", "age": 25},
            ]
        )
        deleted = col.delete_many({"age": 30})
        assert deleted == 2
        assert col.count() == 1
        assert col.find_one({"name": "Carol"}) is not None

    def test_delete_many_no_matches_returns_zero(self) -> None:
        db = Database()
        col = db.collection("users")
        col.insert_one({"name": "Alice"})
        deleted = col.delete_many({"name": "NonExistent"})
        assert deleted == 0
        assert col.count() == 1

    def test_delete_on_empty_collection(self) -> None:
        db = Database()
        col = db.collection("users")
        assert col.delete_one({"name": "Alice"}) == 0
        assert col.delete_many({"name": "Alice"}) == 0


class TestCountOperations:
    """Tests for count method."""

    def test_count_no_filter_returns_total(self) -> None:
        db = Database()
        col = db.collection("users")
        col.insert_many([{"name": "Alice"}, {"name": "Bob"}, {"name": "Carol"}])
        assert col.count() == 3

    def test_count_with_filter_returns_matching(self) -> None:
        db = Database()
        col = db.collection("users")
        col.insert_many(
            [
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 25},
                {"name": "Carol", "age": 30},
            ]
        )
        assert col.count({"age": 30}) == 2

    def test_count_empty_collection(self) -> None:
        db = Database()
        col = db.collection("users")
        assert col.count() == 0

    def test_count_no_matches(self) -> None:
        db = Database()
        col = db.collection("users")
        col.insert_one({"name": "Alice"})
        assert col.count({"name": "NonExistent"}) == 0


class TestDatabaseManagement:
    """Tests for database-level collection management."""

    def test_collection_returns_same_instance(self) -> None:
        db = Database()
        col1 = db.collection("users")
        col2 = db.collection("users")
        assert col1 is col2

    def test_multiple_collections_are_independent(self) -> None:
        db = Database()
        users = db.collection("users")
        orders = db.collection("orders")
        users.insert_one({"name": "Alice"})
        orders.insert_one({"item": "Widget"})
        assert users.count() == 1
        assert orders.count() == 1
        assert users.find_one({"item": "Widget"}) is None
        assert orders.find_one({"name": "Alice"}) is None

    def test_collection_creation(self) -> None:
        db = Database()
        col = db.collection("users")
        assert col is not None
        col.insert_one({"name": "Alice"})
        assert col.count() == 1

    def test_list_collections(self) -> None:
        db = Database()
        db.collection("users")
        db.collection("orders")
        db.collection("products")
        collections = db.list_collections()
        assert set(collections) == {"users", "orders", "products"}

    def test_drop_collection(self) -> None:
        db = Database()
        col = db.collection("users")
        col.insert_one({"name": "Alice"})
        db.drop_collection("users")
        # After dropping, a new collection with same name should be empty
        new_col = db.collection("users")
        assert new_col.count() == 0

    def test_drop_nonexistent_collection(self) -> None:
        db = Database()
        # Should not raise
        db.drop_collection("nonexistent")

    def test_list_collections_empty_db(self) -> None:
        db = Database()
        assert db.list_collections() == []
