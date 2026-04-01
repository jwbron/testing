"""Gap-filling tests for the in-memory database module.

These tests target edge cases, boundary conditions, and code paths
not covered by the coder's primary test suite.
"""

import pytest

from challenges.inmemory_db import (
    DatabaseError,
    InMemoryDB,
    PrimaryKeyViolationError,
    TableExistsError,
    TableNotFoundError,
    TypeValidationError,
)


@pytest.fixture()
def db() -> InMemoryDB:
    """Return a fresh InMemoryDB instance."""
    return InMemoryDB()


@pytest.fixture()
def users_db(db: InMemoryDB) -> InMemoryDB:
    """Return a db with a pre-created users table (id, name, age)."""
    db.create_table("users", {"id": int, "name": str, "age": int}, primary_key="id")
    return db


# ======================================================================
# Exception hierarchy
# ======================================================================


class TestExceptionHierarchy:
    """All custom exceptions inherit from DatabaseError."""

    def test_table_exists_error_is_database_error(self) -> None:
        assert issubclass(TableExistsError, DatabaseError)

    def test_table_not_found_error_is_database_error(self) -> None:
        assert issubclass(TableNotFoundError, DatabaseError)

    def test_primary_key_violation_is_database_error(self) -> None:
        assert issubclass(PrimaryKeyViolationError, DatabaseError)

    def test_type_validation_error_is_database_error(self) -> None:
        assert issubclass(TypeValidationError, DatabaseError)


# ======================================================================
# Create table edge cases
# ======================================================================


class TestCreateTableEdgeCases:
    """Edge cases for create_table not covered by primary tests."""

    def test_create_multiple_independent_tables(self, db: InMemoryDB) -> None:
        db.create_table("a", {"x": int})
        db.create_table("b", {"y": str})
        db.insert("a", {"x": 1})
        db.insert("b", {"y": "hello"})
        assert len(db.select("a")) == 1
        assert len(db.select("b")) == 1

    def test_create_table_empty_string_name(self, db: InMemoryDB) -> None:
        """Empty string is accepted as a table name."""
        db.create_table("", {"x": int})
        db.insert("", {"x": 1})
        assert db.select("") == [{"x": 1}]

    def test_create_table_single_column(self, db: InMemoryDB) -> None:
        db.create_table("t", {"val": int}, primary_key="val")
        db.insert("t", {"val": 42})
        assert db.select("t") == [{"val": 42}]

    def test_create_table_with_float_column(self, db: InMemoryDB) -> None:
        db.create_table("m", {"id": int, "value": float}, primary_key="id")
        db.insert("m", {"id": 1, "value": 3.14})
        assert db.select("m")[0]["value"] == pytest.approx(3.14)


# ======================================================================
# Drop table edge cases
# ======================================================================


class TestDropTableEdgeCases:
    """Edge cases for drop_table."""

    def test_drop_clears_all_metadata(self, db: InMemoryDB) -> None:
        """After drop, recreating with a different schema should work."""
        db.create_table("t", {"id": int, "name": str}, primary_key="id")
        db.insert("t", {"id": 1, "name": "Alice"})
        db.drop_table("t")
        db.create_table("t", {"id": int, "extra": str}, primary_key="id")
        db.insert("t", {"id": 1, "extra": "val"})
        assert db.select("t") == [{"id": 1, "extra": "val"}]

    def test_drop_one_table_does_not_affect_other(self, db: InMemoryDB) -> None:
        db.create_table("a", {"x": int})
        db.create_table("b", {"y": int})
        db.insert("a", {"x": 1})
        db.insert("b", {"y": 2})
        db.drop_table("a")
        assert db.select("b") == [{"y": 2}]


# ======================================================================
# Insert edge cases
# ======================================================================


class TestInsertEdgeCases:
    """Edge cases for insert."""

    def test_insert_stores_copy_of_row(self, users_db: InMemoryDB) -> None:
        """Modifying the dict after insert must not change stored data."""
        row = {"id": 1, "name": "Alice", "age": 30}
        users_db.insert("users", row)
        row["name"] = "MUTATED"
        assert users_db.select("users")[0]["name"] == "Alice"

    def test_insert_bool_passes_as_int(self, db: InMemoryDB) -> None:
        """bool is a subclass of int in Python, so True passes int check."""
        db.create_table("t", {"val": int})
        db.insert("t", {"val": True})
        assert db.select("t")[0]["val"] is True

    def test_insert_none_fails_type_check(self, db: InMemoryDB) -> None:
        """None is not an instance of int or str."""
        db.create_table("t", {"id": int, "name": str})
        with pytest.raises(TypeValidationError):
            db.insert("t", {"id": None, "name": "test"})

    def test_insert_with_string_primary_key(self, db: InMemoryDB) -> None:
        db.create_table("items", {"code": str, "qty": int}, primary_key="code")
        db.insert("items", {"code": "ABC", "qty": 10})
        with pytest.raises(PrimaryKeyViolationError):
            db.insert("items", {"code": "ABC", "qty": 20})

    def test_insert_type_error_message_contains_details(
        self, users_db: InMemoryDB
    ) -> None:
        """Error message should mention column name, expected and actual types."""
        with pytest.raises(TypeValidationError, match="name.*str.*int"):
            users_db.insert("users", {"id": 1, "name": 123, "age": 30})

    def test_insert_pk_error_message_contains_key_and_value(
        self, users_db: InMemoryDB
    ) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        with pytest.raises(PrimaryKeyViolationError, match="id"):
            users_db.insert("users", {"id": 1, "name": "Bob", "age": 25})


# ======================================================================
# Select edge cases
# ======================================================================


class TestSelectEdgeCases:
    """Edge cases for select."""

    def test_select_with_empty_dict_where_returns_all(
        self, users_db: InMemoryDB
    ) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        users_db.insert("users", {"id": 2, "name": "Bob", "age": 25})
        result = users_db.select("users", where={})
        assert len(result) == 2

    def test_select_where_none_returns_all(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        users_db.insert("users", {"id": 2, "name": "Bob", "age": 25})
        assert len(users_db.select("users", where=None)) == 2

    def test_select_filter_on_nonexistent_column(
        self, users_db: InMemoryDB
    ) -> None:
        """Filtering on a non-schema column returns no matches (no crash)."""
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        result = users_db.select("users", where={"email": "a@b.c"})
        assert result == []

    def test_select_where_dict_multiple_nonexistent_keys(
        self, users_db: InMemoryDB
    ) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        result = users_db.select("users", where={"foo": "bar", "baz": 42})
        assert result == []

    def test_select_callable_with_complex_logic(
        self, users_db: InMemoryDB
    ) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        users_db.insert("users", {"id": 2, "name": "Bob", "age": 25})
        users_db.insert("users", {"id": 3, "name": "Charlie", "age": 35})
        result = users_db.select(
            "users",
            where=lambda r: r["age"] > 20 and r["name"].startswith("A"),
        )
        assert len(result) == 1
        assert result[0]["name"] == "Alice"


# ======================================================================
# Update edge cases
# ======================================================================


class TestUpdateEdgeCases:
    """Edge cases for update."""

    def test_update_with_callable_where(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        users_db.insert("users", {"id": 2, "name": "Bob", "age": 25})
        count = users_db.update(
            "users", {"name": "Senior"}, where=lambda r: r["age"] >= 30
        )
        assert count == 1
        assert users_db.select("users", where={"id": 1})[0]["name"] == "Senior"

    def test_update_multiple_columns(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        count = users_db.update(
            "users", {"name": "Alicia", "age": 31}, where={"id": 1}
        )
        assert count == 1
        row = users_db.select("users")[0]
        assert row["name"] == "Alicia"
        assert row["age"] == 31

    def test_update_empty_table_returns_zero(self, users_db: InMemoryDB) -> None:
        count = users_db.update("users", {"age": 99})
        assert count == 0

    def test_update_returns_count_for_multiple_matches(
        self, users_db: InMemoryDB
    ) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        users_db.insert("users", {"id": 2, "name": "Alice", "age": 25})
        count = users_db.update("users", {"name": "Alicia"}, where={"name": "Alice"})
        assert count == 2

    def test_update_pk_to_duplicate_is_not_prevented(
        self, users_db: InMemoryDB
    ) -> None:
        """Known gap: update does not enforce PK uniqueness.

        Updating a primary key column to a duplicate value is not rejected.
        This documents the behavior as a gap for future improvement.
        """
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        users_db.insert("users", {"id": 2, "name": "Bob", "age": 25})
        count = users_db.update("users", {"id": 1}, where={"name": "Bob"})
        assert count == 1
        dupes = users_db.select("users", where={"id": 1})
        assert len(dupes) == 2

    def test_update_type_error_message(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        with pytest.raises(TypeValidationError, match="age.*int.*str"):
            users_db.update("users", {"age": "old"}, where={"id": 1})


# ======================================================================
# Delete edge cases
# ======================================================================


class TestDeleteEdgeCases:
    """Edge cases for delete."""

    def test_delete_from_empty_table(self, users_db: InMemoryDB) -> None:
        count = users_db.delete("users")
        assert count == 0

    def test_delete_returns_correct_count(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        users_db.insert("users", {"id": 2, "name": "Alice", "age": 25})
        users_db.insert("users", {"id": 3, "name": "Bob", "age": 30})
        count = users_db.delete("users", where={"name": "Alice"})
        assert count == 2

    def test_delete_with_empty_dict_where_deletes_all(
        self, users_db: InMemoryDB
    ) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        users_db.insert("users", {"id": 2, "name": "Bob", "age": 25})
        count = users_db.delete("users", where={})
        assert count == 2
        assert users_db.select("users") == []


# ======================================================================
# Integration / cross-operation tests
# ======================================================================


class TestIntegration:
    """Integration tests verifying multi-operation workflows."""

    def test_full_crud_lifecycle(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        assert len(users_db.select("users")) == 1

        users_db.update("users", {"age": 31}, where={"id": 1})
        assert users_db.select("users")[0]["age"] == 31

        users_db.delete("users", where={"id": 1})
        assert users_db.select("users") == []

    def test_reinsert_after_delete_same_pk(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        users_db.delete("users", where={"id": 1})
        users_db.insert("users", {"id": 1, "name": "Bob", "age": 25})
        result = users_db.select("users")
        assert len(result) == 1
        assert result[0]["name"] == "Bob"

    def test_multiple_tables_isolated(self, db: InMemoryDB) -> None:
        db.create_table("a", {"id": int}, primary_key="id")
        db.create_table("b", {"id": int}, primary_key="id")
        db.insert("a", {"id": 1})
        db.insert("b", {"id": 1})
        db.delete("a", where={"id": 1})
        assert db.select("a") == []
        assert len(db.select("b")) == 1

    def test_update_visible_in_subsequent_select(
        self, users_db: InMemoryDB
    ) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        users_db.update("users", {"name": "Alicia"}, where={"id": 1})
        result = users_db.select("users", where={"name": "Alicia"})
        assert len(result) == 1
        assert result[0]["id"] == 1

    def test_delete_then_insert_preserves_schema(
        self, users_db: InMemoryDB
    ) -> None:
        """Deleting all rows does not remove the table schema."""
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        users_db.delete("users")
        users_db.insert("users", {"id": 2, "name": "Bob", "age": 25})
        assert len(users_db.select("users")) == 1

    def test_large_table_operations(self, db: InMemoryDB) -> None:
        """Sanity check with many rows."""
        db.create_table("big", {"id": int, "val": str}, primary_key="id")
        for i in range(100):
            db.insert("big", {"id": i, "val": f"item-{i}"})
        assert len(db.select("big")) == 100
        count = db.delete("big", where=lambda r: r["id"] >= 50)
        assert count == 50
        assert len(db.select("big")) == 50

    def test_update_does_not_affect_non_matching_rows(
        self, users_db: InMemoryDB
    ) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        users_db.insert("users", {"id": 2, "name": "Bob", "age": 25})
        users_db.update("users", {"age": 99}, where={"name": "Alice"})
        bob = users_db.select("users", where={"name": "Bob"})
        assert bob[0]["age"] == 25

    def test_select_after_multiple_deletes(self, users_db: InMemoryDB) -> None:
        for i in range(5):
            users_db.insert("users", {"id": i, "name": f"user{i}", "age": 20 + i})
        users_db.delete("users", where=lambda r: r["id"] % 2 == 0)
        remaining = users_db.select("users")
        assert len(remaining) == 2
        ids = {r["id"] for r in remaining}
        assert ids == {1, 3}
