"""Tests for Transaction class with snapshot isolation."""

import pytest
from challenges.nosql_db import Database


@pytest.fixture
def db() -> Database:
    """Create a fresh database instance."""
    return Database()


@pytest.fixture
def populated_db(db: Database) -> Database:
    """Create a database with some initial data."""
    db.insert("users", {"_id": "u1", "name": "Alice", "age": 30})
    db.insert("users", {"_id": "u2", "name": "Bob", "age": 25})
    db.insert("users", {"_id": "u3", "name": "Charlie", "age": 35})
    db.insert("orders", {"_id": "o1", "user": "u1", "total": 100})
    return db


class TestBasicTransaction:
    """Test basic transaction commit and rollback behavior."""

    def test_insert_then_commit(self, db: Database) -> None:
        """Inserted doc is visible after commit."""
        txn = db.begin_transaction()
        txn.insert("users", {"_id": "u1", "name": "Alice", "age": 30})
        txn.commit()

        result = db.find("users", {"_id": "u1"})
        assert len(result) == 1
        assert result[0]["name"] == "Alice"

    def test_insert_then_rollback(self, db: Database) -> None:
        """Inserted doc is NOT visible after rollback."""
        txn = db.begin_transaction()
        txn.insert("users", {"_id": "u1", "name": "Alice", "age": 30})
        txn.rollback()

        result = db.find("users", {"_id": "u1"})
        assert len(result) == 0

    def test_update_then_commit(self, populated_db: Database) -> None:
        """Updated doc persists after commit."""
        txn = populated_db.begin_transaction()
        txn.update("users", {"_id": "u1"}, {"$set": {"age": 31}})
        txn.commit()

        result = populated_db.find("users", {"_id": "u1"})
        assert result[0]["age"] == 31

    def test_delete_then_commit(self, populated_db: Database) -> None:
        """Deleted doc is gone after commit."""
        txn = populated_db.begin_transaction()
        txn.delete("users", {"_id": "u1"})
        txn.commit()

        result = populated_db.find("users", {"_id": "u1"})
        assert len(result) == 0

    def test_update_then_rollback(self, populated_db: Database) -> None:
        """Doc remains unchanged after rollback of an update."""
        txn = populated_db.begin_transaction()
        txn.update("users", {"_id": "u1"}, {"$set": {"age": 99}})
        txn.rollback()

        result = populated_db.find("users", {"_id": "u1"})
        assert result[0]["age"] == 30


class TestSnapshotIsolation:
    """Test that transactions see a consistent snapshot."""

    def test_reads_see_snapshot_at_start(self, populated_db: Database) -> None:
        """Transaction reads reflect data as of transaction start."""
        txn = populated_db.begin_transaction()

        result = txn.find("users", {"_id": "u1"})
        assert len(result) == 1
        assert result[0]["name"] == "Alice"

        txn.rollback()

    def test_external_changes_not_visible_inside(self, populated_db: Database) -> None:
        """Changes made outside after txn start are invisible inside."""
        txn = populated_db.begin_transaction()

        # External modification after transaction started
        populated_db.update("users", {"_id": "u1"}, {"$set": {"name": "Alicia"}})

        # Transaction still sees the old value
        result = txn.find("users", {"_id": "u1"})
        assert result[0]["name"] == "Alice"

        txn.rollback()

    def test_own_writes_visible_within_transaction(
        self, populated_db: Database
    ) -> None:
        """Writes within the transaction are visible to subsequent reads."""
        txn = populated_db.begin_transaction()
        txn.update("users", {"_id": "u1"}, {"$set": {"age": 50}})

        result = txn.find("users", {"_id": "u1"})
        assert result[0]["age"] == 50

        txn.rollback()

    def test_committed_changes_visible_outside(self, populated_db: Database) -> None:
        """After commit, changes are visible to new reads outside txn."""
        txn = populated_db.begin_transaction()
        txn.update("users", {"_id": "u2"}, {"$set": {"name": "Bobby"}})
        txn.commit()

        result = populated_db.find("users", {"_id": "u2"})
        assert result[0]["name"] == "Bobby"

    def test_external_insert_not_visible_inside(self, populated_db: Database) -> None:
        """Docs inserted outside after txn start are invisible inside."""
        txn = populated_db.begin_transaction()

        populated_db.insert("users", {"_id": "u4", "name": "Diana", "age": 28})

        result = txn.find("users", {"_id": "u4"})
        assert len(result) == 0

        txn.rollback()

    def test_external_delete_not_visible_inside(self, populated_db: Database) -> None:
        """Docs deleted outside after txn start are still visible inside."""
        txn = populated_db.begin_transaction()

        populated_db.delete("users", {"_id": "u2"})

        result = txn.find("users", {"_id": "u2"})
        assert len(result) == 1
        assert result[0]["name"] == "Bob"

        txn.rollback()


class TestAtomicity:
    """Test that all operations in a transaction are atomic."""

    def test_multiple_ops_all_commit(self, db: Database) -> None:
        """All operations in a committed transaction take effect."""
        txn = db.begin_transaction()
        txn.insert("users", {"_id": "u1", "name": "Alice", "age": 30})
        txn.insert("users", {"_id": "u2", "name": "Bob", "age": 25})
        txn.insert("orders", {"_id": "o1", "user": "u1", "total": 50})
        txn.commit()

        assert len(db.find("users", {})) == 2
        assert len(db.find("orders", {})) == 1

    def test_rollback_undoes_all_operations(self, db: Database) -> None:
        """Rollback discards ALL operations, not just the last one."""
        txn = db.begin_transaction()
        txn.insert("users", {"_id": "u1", "name": "Alice", "age": 30})
        txn.insert("users", {"_id": "u2", "name": "Bob", "age": 25})
        txn.insert("users", {"_id": "u3", "name": "Charlie", "age": 35})
        txn.rollback()

        assert len(db.find("users", {})) == 0

    def test_mixed_insert_update_delete(self, populated_db: Database) -> None:
        """Mixed insert/update/delete all apply atomically on commit."""
        txn = populated_db.begin_transaction()
        txn.insert("users", {"_id": "u4", "name": "Diana", "age": 28})
        txn.update("users", {"_id": "u1"}, {"$set": {"age": 31}})
        txn.delete("users", {"_id": "u3"})
        txn.commit()

        assert len(populated_db.find("users", {"_id": "u4"})) == 1
        assert populated_db.find("users", {"_id": "u1"})[0]["age"] == 31
        assert len(populated_db.find("users", {"_id": "u3"})) == 0
        # u2 unchanged
        assert populated_db.find("users", {"_id": "u2"})[0]["name"] == "Bob"

    def test_mixed_ops_rollback(self, populated_db: Database) -> None:
        """All mixed operations are undone on rollback."""
        txn = populated_db.begin_transaction()
        txn.insert("users", {"_id": "u4", "name": "Diana", "age": 28})
        txn.update("users", {"_id": "u1"}, {"$set": {"age": 99}})
        txn.delete("users", {"_id": "u2"})
        txn.rollback()

        assert len(populated_db.find("users", {"_id": "u4"})) == 0
        assert populated_db.find("users", {"_id": "u1"})[0]["age"] == 30
        assert len(populated_db.find("users", {"_id": "u2"})) == 1


class TestConflictDetection:
    """Test write-write conflict detection between transactions."""

    def test_concurrent_modify_same_doc_conflict(self, populated_db: Database) -> None:
        """Second commit fails when both txns modify the same doc."""
        txn1 = populated_db.begin_transaction()
        txn2 = populated_db.begin_transaction()

        txn1.update("users", {"_id": "u1"}, {"$set": {"age": 31}})
        txn2.update("users", {"_id": "u1"}, {"$set": {"age": 32}})

        txn1.commit()

        with pytest.raises(Exception):
            txn2.commit()

    def test_concurrent_modify_different_docs_no_conflict(
        self, populated_db: Database
    ) -> None:
        """Both txns succeed when they modify different documents."""
        txn1 = populated_db.begin_transaction()
        txn2 = populated_db.begin_transaction()

        txn1.update("users", {"_id": "u1"}, {"$set": {"age": 31}})
        txn2.update("users", {"_id": "u2"}, {"$set": {"age": 26}})

        txn1.commit()
        txn2.commit()

        assert populated_db.find("users", {"_id": "u1"})[0]["age"] == 31
        assert populated_db.find("users", {"_id": "u2"})[0]["age"] == 26

    def test_modify_doc_deleted_by_another_txn(self, populated_db: Database) -> None:
        """Conflict when modifying a doc deleted by another txn."""
        txn1 = populated_db.begin_transaction()
        txn2 = populated_db.begin_transaction()

        txn1.delete("users", {"_id": "u1"})
        txn2.update("users", {"_id": "u1"}, {"$set": {"age": 50}})

        txn1.commit()

        with pytest.raises(Exception):
            txn2.commit()

    def test_both_insert_same_id_conflict(self, db: Database) -> None:
        """Conflict when two transactions insert docs with the same id."""
        txn1 = db.begin_transaction()
        txn2 = db.begin_transaction()

        txn1.insert("users", {"_id": "u1", "name": "Alice"})
        txn2.insert("users", {"_id": "u1", "name": "Bob"})

        txn1.commit()

        with pytest.raises(Exception):
            txn2.commit()

    def test_concurrent_modify_different_collections(
        self, populated_db: Database
    ) -> None:
        """No conflict when txns touch different collections."""
        txn1 = populated_db.begin_transaction()
        txn2 = populated_db.begin_transaction()

        txn1.update("users", {"_id": "u1"}, {"$set": {"age": 31}})
        txn2.update("orders", {"_id": "o1"}, {"$set": {"total": 200}})

        txn1.commit()
        txn2.commit()

        assert populated_db.find("users", {"_id": "u1"})[0]["age"] == 31
        assert populated_db.find("orders", {"_id": "o1"})[0]["total"] == 200


class TestTransactionEdgeCases:
    """Test edge cases and unusual transaction scenarios."""

    def test_empty_transaction_commit(self, db: Database) -> None:
        """Empty transaction commits without error."""
        txn = db.begin_transaction()
        txn.commit()

    def test_empty_transaction_rollback(self, db: Database) -> None:
        """Empty transaction rolls back without error."""
        txn = db.begin_transaction()
        txn.rollback()

    def test_transaction_on_empty_collection(self, db: Database) -> None:
        """Transaction operations work on non-existent collections."""
        txn = db.begin_transaction()
        result = txn.find("nonexistent", {})
        assert len(result) == 0
        txn.rollback()

    def test_multiple_sequential_transactions(self, db: Database) -> None:
        """Sequential transactions each see prior committed state."""
        txn1 = db.begin_transaction()
        txn1.insert("users", {"_id": "u1", "name": "Alice"})
        txn1.commit()

        txn2 = db.begin_transaction()
        txn2.insert("users", {"_id": "u2", "name": "Bob"})
        txn2.commit()

        txn3 = db.begin_transaction()
        result = txn3.find("users", {})
        assert len(result) == 2
        txn3.rollback()

    def test_nested_collection_ops_in_transaction(self, db: Database) -> None:
        """Transaction spans operations across multiple collections."""
        txn = db.begin_transaction()
        txn.insert("users", {"_id": "u1", "name": "Alice"})
        txn.insert("orders", {"_id": "o1", "user": "u1", "total": 50})
        txn.insert("products", {"_id": "p1", "name": "Widget", "price": 10})
        txn.commit()

        assert len(db.find("users", {})) == 1
        assert len(db.find("orders", {})) == 1
        assert len(db.find("products", {})) == 1

    def test_large_number_of_operations(self, db: Database) -> None:
        """Transaction handles many operations without issue."""
        txn = db.begin_transaction()
        for i in range(200):
            txn.insert("items", {"_id": f"item_{i}", "value": i})
        txn.commit()

        result = db.find("items", {})
        assert len(result) == 200

    def test_large_transaction_rollback(self, db: Database) -> None:
        """Rolling back many operations leaves db empty."""
        txn = db.begin_transaction()
        for i in range(200):
            txn.insert("items", {"_id": f"item_{i}", "value": i})
        txn.rollback()

        result = db.find("items", {})
        assert len(result) == 0

    def test_commit_then_new_transaction_sees_changes(self, db: Database) -> None:
        """A new transaction after commit sees the committed data."""
        txn1 = db.begin_transaction()
        txn1.insert("users", {"_id": "u1", "name": "Alice", "age": 30})
        txn1.commit()

        txn2 = db.begin_transaction()
        txn2.update("users", {"_id": "u1"}, {"$set": {"age": 31}})
        txn2.commit()

        result = db.find("users", {"_id": "u1"})
        assert result[0]["age"] == 31

    def test_transaction_insert_visible_in_same_txn_find(self, db: Database) -> None:
        """A doc inserted in a txn is findable within that same txn."""
        txn = db.begin_transaction()
        txn.insert("users", {"_id": "u1", "name": "Alice"})

        result = txn.find("users", {"_id": "u1"})
        assert len(result) == 1
        assert result[0]["name"] == "Alice"

        txn.commit()

    def test_transaction_delete_hides_in_same_txn(self, populated_db: Database) -> None:
        """A doc deleted in a txn is not findable within that txn."""
        txn = populated_db.begin_transaction()
        txn.delete("users", {"_id": "u1"})

        result = txn.find("users", {"_id": "u1"})
        assert len(result) == 0

        txn.rollback()
