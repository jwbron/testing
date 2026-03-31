"""Gap-filling tests for InMemoryDB edge cases and boundary conditions.

These tests target gaps in the initial test suite: type system edge cases,
primary key lifecycle, update PK semantics, empty tables, large datasets,
predicate error handling, and mutation isolation.
"""

import pytest

from challenges.inmemory_db import (
    ColumnError,
    DatabaseError,
    InMemoryDB,
    PrimaryKeyViolationError,
    TableExistsError,
    TableNotFoundError,
    TypeValidationError,
)


@pytest.fixture
def db() -> InMemoryDB:
    """Provide a fresh InMemoryDB instance."""
    return InMemoryDB()


@pytest.fixture
def users_db(db: InMemoryDB) -> InMemoryDB:
    """Provide an InMemoryDB with a 'users' table pre-created."""
    db.create_table("users", {"id": int, "name": str, "age": int}, primary_key="id")
    return db


# ---------------------------------------------------------------------------
# Type validation edge cases
# ---------------------------------------------------------------------------


class TestTypeValidationEdgeCases:
    """Edge cases around Python's type system and isinstance checks."""

    def test_bool_is_subclass_of_int(self, db: InMemoryDB) -> None:
        """In Python, bool is a subclass of int.

        isinstance(True, int) returns True, so inserting a bool into an int
        column will be accepted by the current implementation. This documents
        that behavior — it is a known edge case, not necessarily a bug.
        """
        db.create_table("t", {"id": int, "val": int}, primary_key="id")
        # This will NOT raise because bool is a subclass of int
        db.insert("t", {"id": 1, "val": True})
        result = db.select("t")
        assert result[0]["val"] is True

    def test_int_rejected_for_bool_column(self, db: InMemoryDB) -> None:
        """An int should be rejected for a bool column (int is not subclass of bool)."""
        db.create_table("t", {"id": int, "active": bool}, primary_key="id")
        with pytest.raises(TypeValidationError):
            db.insert("t", {"id": 1, "active": 1})

    def test_float_rejected_for_int_column(self, db: InMemoryDB) -> None:
        db.create_table("t", {"id": int, "val": int}, primary_key="id")
        with pytest.raises(TypeValidationError):
            db.insert("t", {"id": 1, "val": 3.14})

    def test_int_rejected_for_float_column(self, db: InMemoryDB) -> None:
        """int is not a subclass of float in Python (unlike in math)."""
        db.create_table("t", {"id": int, "val": float}, primary_key="id")
        with pytest.raises(TypeValidationError):
            db.insert("t", {"id": 1, "val": 42})

    def test_none_rejected_for_any_typed_column(self, db: InMemoryDB) -> None:
        db.create_table("t", {"id": int, "name": str}, primary_key="id")
        with pytest.raises(TypeValidationError):
            db.insert("t", {"id": 1, "name": None})

    def test_empty_string_accepted_for_str_column(self, db: InMemoryDB) -> None:
        db.create_table("t", {"id": int, "name": str}, primary_key="id")
        db.insert("t", {"id": 1, "name": ""})
        assert db.select("t")[0]["name"] == ""

    def test_negative_int_accepted(self, db: InMemoryDB) -> None:
        db.create_table("t", {"id": int, "val": int}, primary_key="id")
        db.insert("t", {"id": -1, "val": -999})
        result = db.select("t", where=lambda r: r["id"] == -1)
        assert result[0]["val"] == -999

    def test_type_validation_on_update(self, users_db: InMemoryDB) -> None:
        """Type validation should also apply during updates."""
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        with pytest.raises(TypeValidationError):
            users_db.update("users", {"age": "thirty"})


# ---------------------------------------------------------------------------
# Primary key lifecycle
# ---------------------------------------------------------------------------


class TestPrimaryKeyLifecycle:
    """Tests for primary key constraint through insert/delete cycles."""

    def test_pk_reuse_after_delete(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        users_db.delete("users", where=lambda r: r["id"] == 1)
        # PK 1 should be free again
        users_db.insert("users", {"id": 1, "name": "Bob", "age": 25})
        result = users_db.select("users")
        assert len(result) == 1
        assert result[0]["name"] == "Bob"

    def test_pk_reuse_after_delete_all(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        users_db.insert("users", {"id": 2, "name": "Bob", "age": 25})
        users_db.delete("users")
        users_db.insert("users", {"id": 1, "name": "Charlie", "age": 20})
        assert len(users_db.select("users")) == 1

    def test_pk_zero_is_valid(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 0, "name": "Zero", "age": 0})
        result = users_db.select("users", where=lambda r: r["id"] == 0)
        assert len(result) == 1

    def test_pk_negative_is_valid(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": -1, "name": "Neg", "age": 1})
        result = users_db.select("users", where=lambda r: r["id"] == -1)
        assert len(result) == 1

    def test_pk_uniqueness_many_inserts(self, users_db: InMemoryDB) -> None:
        for i in range(100):
            users_db.insert("users", {"id": i, "name": f"User{i}", "age": i})
        with pytest.raises(PrimaryKeyViolationError):
            users_db.insert("users", {"id": 50, "name": "Dup", "age": 0})

    def test_update_pk_to_same_value_succeeds(self, users_db: InMemoryDB) -> None:
        """Updating a row's PK to its current value should not raise."""
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        # Update PK to the same value — should succeed
        count = users_db.update(
            "users", {"id": 1}, where=lambda r: r["name"] == "Alice"
        )
        assert count == 1

    def test_update_pk_to_free_value_succeeds(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        users_db.insert("users", {"id": 2, "name": "Bob", "age": 25})
        count = users_db.update("users", {"id": 99}, where=lambda r: r["name"] == "Bob")
        assert count == 1
        result = users_db.select("users", where=lambda r: r["id"] == 99)
        assert result[0]["name"] == "Bob"


# ---------------------------------------------------------------------------
# Operations on empty tables
# ---------------------------------------------------------------------------


class TestEmptyTableOperations:
    """CRUD operations on tables with no rows."""

    def test_select_empty_table(self, users_db: InMemoryDB) -> None:
        assert users_db.select("users") == []

    def test_select_with_filter_empty_table(self, users_db: InMemoryDB) -> None:
        result = users_db.select("users", where=lambda r: r["id"] == 1)
        assert result == []

    def test_update_empty_table(self, users_db: InMemoryDB) -> None:
        count = users_db.update("users", {"age": 99})
        assert count == 0

    def test_delete_empty_table(self, users_db: InMemoryDB) -> None:
        count = users_db.delete("users")
        assert count == 0


# ---------------------------------------------------------------------------
# Multiple tables / table independence
# ---------------------------------------------------------------------------


class TestTableIndependence:
    """Ensure operations on one table don't affect others."""

    def test_independent_schemas(self, db: InMemoryDB) -> None:
        db.create_table("a", {"id": int, "x": str}, primary_key="id")
        db.create_table("b", {"id": int, "y": float}, primary_key="id")
        db.insert("a", {"id": 1, "x": "hello"})
        # Table b has different schema
        with pytest.raises(ColumnError):
            db.insert("b", {"id": 1, "x": "wrong schema"})

    def test_drop_does_not_affect_other(self, db: InMemoryDB) -> None:
        db.create_table("a", {"id": int}, primary_key="id")
        db.create_table("b", {"id": int}, primary_key="id")
        db.insert("a", {"id": 1})
        db.insert("b", {"id": 1})
        db.drop_table("a")
        assert len(db.select("b")) == 1

    def test_same_pk_different_tables(self, db: InMemoryDB) -> None:
        db.create_table("a", {"id": int, "val": str}, primary_key="id")
        db.create_table("b", {"id": int, "val": str}, primary_key="id")
        db.insert("a", {"id": 1, "val": "a"})
        db.insert("b", {"id": 1, "val": "b"})
        assert db.select("a")[0]["val"] == "a"
        assert db.select("b")[0]["val"] == "b"


# ---------------------------------------------------------------------------
# Column validation edge cases
# ---------------------------------------------------------------------------


class TestColumnValidation:
    """Edge cases for column checks."""

    def test_insert_empty_row_raises(self, users_db: InMemoryDB) -> None:
        """Inserting an empty dict when table has columns should raise."""
        with pytest.raises(ColumnError):
            users_db.insert("users", {})

    def test_insert_subset_of_columns_raises(self, users_db: InMemoryDB) -> None:
        with pytest.raises(ColumnError, match="Missing"):
            users_db.insert("users", {"id": 1})

    def test_insert_superset_of_columns_raises(self, users_db: InMemoryDB) -> None:
        with pytest.raises(ColumnError, match="Unknown"):
            users_db.insert(
                "users",
                {"id": 1, "name": "A", "age": 1, "extra": "x"},
            )

    def test_update_unknown_column_raises(self, users_db: InMemoryDB) -> None:
        with pytest.raises(ColumnError, match="Unknown"):
            users_db.update("users", {"nonexistent": 1})


# ---------------------------------------------------------------------------
# Return values from update/delete
# ---------------------------------------------------------------------------


class TestReturnValues:
    """Verify update and delete return correct counts."""

    def test_update_returns_count(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        users_db.insert("users", {"id": 2, "name": "Bob", "age": 30})
        users_db.insert("users", {"id": 3, "name": "Charlie", "age": 25})
        count = users_db.update("users", {"age": 99}, where=lambda r: r["age"] == 30)
        assert count == 2

    def test_delete_returns_count(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        users_db.insert("users", {"id": 2, "name": "Bob", "age": 30})
        users_db.insert("users", {"id": 3, "name": "Charlie", "age": 25})
        count = users_db.delete("users", where=lambda r: r["age"] == 30)
        assert count == 2

    def test_delete_all_returns_full_count(self, users_db: InMemoryDB) -> None:
        for i in range(5):
            users_db.insert("users", {"id": i, "name": f"U{i}", "age": i})
        count = users_db.delete("users")
        assert count == 5


# ---------------------------------------------------------------------------
# Data isolation / mutation safety
# ---------------------------------------------------------------------------


class TestDataIsolation:
    """Ensure internal data cannot be mutated through external references."""

    def test_insert_dict_not_shared(self, users_db: InMemoryDB) -> None:
        """Mutating the row dict after insert should not change DB."""
        row = {"id": 1, "name": "Alice", "age": 30}
        users_db.insert("users", row)
        row["name"] = "MUTATED"
        assert users_db.select("users")[0]["name"] == "Alice"

    def test_select_returns_copies(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        rows = users_db.select("users")
        rows[0]["name"] = "MUTATED"
        assert users_db.select("users")[0]["name"] == "Alice"

    def test_select_returns_new_list(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        result1 = users_db.select("users")
        result2 = users_db.select("users")
        assert result1 is not result2
        assert result1[0] is not result2[0]


# ---------------------------------------------------------------------------
# Exception hierarchy
# ---------------------------------------------------------------------------


class TestExceptionHierarchy:
    """Verify exception classes inherit from DatabaseError."""

    def test_table_exists_is_database_error(self) -> None:
        assert issubclass(TableExistsError, DatabaseError)

    def test_table_not_found_is_database_error(self) -> None:
        assert issubclass(TableNotFoundError, DatabaseError)

    def test_pk_violation_is_database_error(self) -> None:
        assert issubclass(PrimaryKeyViolationError, DatabaseError)

    def test_type_validation_is_database_error(self) -> None:
        assert issubclass(TypeValidationError, DatabaseError)

    def test_column_error_is_database_error(self) -> None:
        assert issubclass(ColumnError, DatabaseError)


# ---------------------------------------------------------------------------
# Update atomicity edge case
# ---------------------------------------------------------------------------


class TestUpdateAtomicity:
    """Test update behavior with PK conflicts across matched rows."""

    def test_update_all_to_same_pk_raises(self, users_db: InMemoryDB) -> None:
        """Updating multiple rows' PK to the same value should raise."""
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        users_db.insert("users", {"id": 2, "name": "Bob", "age": 25})
        with pytest.raises(PrimaryKeyViolationError):
            users_db.update("users", {"id": 99})

    def test_update_pk_partial_raises_leaves_partial_state(
        self, users_db: InMemoryDB
    ) -> None:
        """When update raises mid-way, some rows may already be updated.

        This documents the current non-transactional behavior — the first
        matching row gets updated before the second triggers a PK conflict.
        """
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        users_db.insert("users", {"id": 2, "name": "Bob", "age": 25})
        with pytest.raises(PrimaryKeyViolationError):
            users_db.update("users", {"id": 99})
        # After the exception, the DB is in a partial state — document it
        rows = users_db.select("users")
        ids = sorted(r["id"] for r in rows)
        # First row got updated to 99, second tried 99 and raised
        assert 99 in ids


# ---------------------------------------------------------------------------
# Large dataset
# ---------------------------------------------------------------------------


class TestLargeDataset:
    """Verify behavior with larger numbers of rows."""

    def test_insert_and_select_1000_rows(self, users_db: InMemoryDB) -> None:
        for i in range(1000):
            users_db.insert(
                "users", {"id": i, "name": f"User{i}", "age": 20 + (i % 50)}
            )
        assert len(users_db.select("users")) == 1000

    def test_filter_large_dataset(self, users_db: InMemoryDB) -> None:
        for i in range(1000):
            users_db.insert(
                "users", {"id": i, "name": f"User{i}", "age": 20 + (i % 50)}
            )
        result = users_db.select("users", where=lambda r: r["age"] == 20)
        # age=20 for i%50==0, i.e., i=0,50,100,...,950 → 20 rows
        assert len(result) == 20

    def test_delete_half_large_dataset(self, users_db: InMemoryDB) -> None:
        for i in range(100):
            users_db.insert("users", {"id": i, "name": f"User{i}", "age": i})
        count = users_db.delete("users", where=lambda r: r["id"] < 50)
        assert count == 50
        assert len(users_db.select("users")) == 50
