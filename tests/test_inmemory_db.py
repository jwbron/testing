"""Comprehensive tests for the in-memory database module."""

from __future__ import annotations

import pytest

from challenges.inmemory_db import (
    ColumnError,
    DatabaseError,
    InMemoryDB,
    PrimaryKeyError,
    Table,
    TableExistsError,
    TableNotFoundError,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def db() -> InMemoryDB:
    """Return a fresh InMemoryDB instance."""
    return InMemoryDB()


@pytest.fixture()
def users_db(db: InMemoryDB) -> InMemoryDB:
    """DB with a 'users' table already created (id PK, name, active)."""
    db.create_table(
        "users",
        {"id": "int", "name": "str", "active": "bool"},
        primary_key="id",
    )
    return db


@pytest.fixture()
def populated_db(users_db: InMemoryDB) -> InMemoryDB:
    """DB with a users table containing sample data."""
    users_db.insert("users", {"id": 1, "name": "Alice", "active": True})
    users_db.insert("users", {"id": 2, "name": "Bob", "active": False})
    users_db.insert("users", {"id": 3, "name": "Charlie", "active": True})
    return users_db


# ---------------------------------------------------------------------------
# Exception hierarchy
# ---------------------------------------------------------------------------


class TestExceptionHierarchy:
    """Verify exception hierarchy so callers can catch broadly."""

    def test_table_exists_is_database_error(self) -> None:
        assert issubclass(TableExistsError, DatabaseError)

    def test_table_not_found_is_database_error(self) -> None:
        assert issubclass(TableNotFoundError, DatabaseError)

    def test_column_error_is_database_error(self) -> None:
        assert issubclass(ColumnError, DatabaseError)

    def test_primary_key_error_is_database_error(self) -> None:
        assert issubclass(PrimaryKeyError, DatabaseError)


# ---------------------------------------------------------------------------
# Table creation & dropping
# ---------------------------------------------------------------------------


class TestCreateTable:
    """Tests for create_table."""

    def test_create_basic_table(self, db: InMemoryDB) -> None:
        db.create_table("t", {"a": "int", "b": "str"})
        assert db.select("t") == []

    def test_create_with_all_types(self, db: InMemoryDB) -> None:
        db.create_table("t", {"i": "int", "f": "float", "s": "str", "b": "bool"})
        db.insert("t", {"i": 1, "f": 2.5, "s": "x", "b": True})
        rows = db.select("t")
        assert len(rows) == 1

    def test_create_with_primary_key(self, db: InMemoryDB) -> None:
        db.create_table("t", {"id": "int"}, primary_key="id")
        db.insert("t", {"id": 42})
        assert db.select("t") == [{"id": 42}]

    def test_create_duplicate_table_raises(self, db: InMemoryDB) -> None:
        db.create_table("t", {"a": "int"})
        with pytest.raises(TableExistsError, match="already exists"):
            db.create_table("t", {"b": "str"})

    def test_unsupported_column_type_raises(self, db: InMemoryDB) -> None:
        with pytest.raises(ColumnError, match="Unsupported column type"):
            db.create_table("t", {"a": "date"})

    def test_primary_key_not_in_columns_raises(self, db: InMemoryDB) -> None:
        with pytest.raises(ColumnError, match="not in column list"):
            db.create_table("t", {"a": "int"}, primary_key="missing")


class TestDropTable:
    """Tests for drop_table."""

    def test_drop_existing_table(self, users_db: InMemoryDB) -> None:
        users_db.drop_table("users")
        with pytest.raises(TableNotFoundError):
            users_db.select("users")

    def test_drop_nonexistent_table_raises(self, db: InMemoryDB) -> None:
        with pytest.raises(TableNotFoundError, match="does not exist"):
            db.drop_table("nope")

    def test_recreate_after_drop(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "A", "active": True})
        users_db.drop_table("users")
        users_db.create_table("users", {"id": "int"}, primary_key="id")
        # Old data should be gone
        assert len(users_db.select("users")) == 0

    def test_drop_does_not_affect_other_tables(self, db: InMemoryDB) -> None:
        db.create_table("a", {"x": "int"})
        db.create_table("b", {"y": "int"})
        db.insert("b", {"y": 10})
        db.drop_table("a")
        assert db.select("b") == [{"y": 10}]


# ---------------------------------------------------------------------------
# Insert
# ---------------------------------------------------------------------------


class TestInsert:
    """Tests for inserting rows."""

    def test_insert_valid_row(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "active": True})
        rows = users_db.select("users")
        assert len(rows) == 1
        assert rows[0]["name"] == "Alice"

    def test_insert_missing_columns_filled_with_none(self, db: InMemoryDB) -> None:
        db.create_table("t", {"a": "int", "b": "str"})
        db.insert("t", {"a": 1})
        rows = db.select("t")
        assert rows == [{"a": 1, "b": None}]

    def test_insert_extra_column_raises(self, users_db: InMemoryDB) -> None:
        with pytest.raises(ColumnError, match="Unknown columns"):
            users_db.insert("users", {"id": 1, "name": "A", "extra": "bad"})

    def test_insert_wrong_type_raises(self, users_db: InMemoryDB) -> None:
        with pytest.raises(ColumnError, match="expects type"):
            users_db.insert("users", {"id": "not_an_int", "name": "A"})

    def test_insert_into_nonexistent_table_raises(self, db: InMemoryDB) -> None:
        with pytest.raises(TableNotFoundError):
            db.insert("nope", {"a": 1})

    def test_insert_duplicate_pk_raises(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "active": True})
        with pytest.raises(PrimaryKeyError, match="Duplicate"):
            users_db.insert("users", {"id": 1, "name": "Bob", "active": False})

    def test_insert_null_pk_raises(self, users_db: InMemoryDB) -> None:
        with pytest.raises(PrimaryKeyError, match="must not be null"):
            users_db.insert("users", {"name": "NoId", "active": True})

    def test_insert_no_pk_table_allows_duplicates(self, db: InMemoryDB) -> None:
        db.create_table("t", {"val": "int"})
        db.insert("t", {"val": 1})
        db.insert("t", {"val": 1})
        assert len(db.select("t")) == 2

    def test_insert_int_value_for_float_column(self, db: InMemoryDB) -> None:
        """int values should be accepted and coerced for float columns."""
        db.create_table("t", {"val": "float"})
        db.insert("t", {"val": 5})
        rows = db.select("t")
        assert rows[0]["val"] == 5.0
        assert isinstance(rows[0]["val"], float)

    def test_insert_bool_behavior_for_int_column(self, db: InMemoryDB) -> None:
        """In Python bool is subclass of int. Document actual behavior.

        isinstance(True, int) is True, so the current implementation
        accepts booleans as int values. This test documents that behavior.
        """
        db.create_table("t", {"val": "int"})
        db.insert("t", {"val": True})
        rows = db.select("t")
        assert rows[0]["val"] is True

    def test_insert_empty_row(self, db: InMemoryDB) -> None:
        """Insert with empty dict fills all columns with None."""
        db.create_table("t", {"a": "int", "b": "str"})
        db.insert("t", {})
        rows = db.select("t")
        assert rows == [{"a": None, "b": None}]


# ---------------------------------------------------------------------------
# Select
# ---------------------------------------------------------------------------


class TestSelect:
    """Tests for selecting rows."""

    def test_select_all_from_empty(self, users_db: InMemoryDB) -> None:
        assert users_db.select("users") == []

    def test_select_all_columns(self, populated_db: InMemoryDB) -> None:
        rows = populated_db.select("users")
        assert len(rows) == 3
        assert all(set(r.keys()) == {"id", "name", "active"} for r in rows)

    def test_select_specific_columns(self, populated_db: InMemoryDB) -> None:
        rows = populated_db.select("users", columns=["name"])
        assert rows == [
            {"name": "Alice"},
            {"name": "Bob"},
            {"name": "Charlie"},
        ]

    def test_select_with_where(self, populated_db: InMemoryDB) -> None:
        rows = populated_db.select("users", where=lambda r: r["active"] is True)
        assert len(rows) == 2
        assert {r["name"] for r in rows} == {"Alice", "Charlie"}

    def test_select_with_columns_and_where(self, populated_db: InMemoryDB) -> None:
        rows = populated_db.select(
            "users",
            columns=["name"],
            where=lambda r: r["id"] > 1,
        )
        assert rows == [{"name": "Bob"}, {"name": "Charlie"}]

    def test_select_unknown_column_raises(self, users_db: InMemoryDB) -> None:
        with pytest.raises(ColumnError, match="Unknown columns"):
            users_db.select("users", columns=["nonexistent"])

    def test_select_from_nonexistent_table_raises(self, db: InMemoryDB) -> None:
        with pytest.raises(TableNotFoundError):
            db.select("nope")

    def test_select_returns_copies_not_references(
        self, populated_db: InMemoryDB
    ) -> None:
        """Modifying returned rows should not affect the table."""
        rows = populated_db.select("users")
        rows[0]["name"] = "MODIFIED"
        original = populated_db.select("users", where=lambda r: r["id"] == 1)
        assert original[0]["name"] == "Alice"

    def test_select_where_matches_none(self, populated_db: InMemoryDB) -> None:
        rows = populated_db.select("users", where=lambda r: r["id"] > 100)
        assert rows == []

    def test_select_multiple_columns(self, populated_db: InMemoryDB) -> None:
        rows = populated_db.select("users", columns=["id", "name"])
        assert all(set(r.keys()) == {"id", "name"} for r in rows)


# ---------------------------------------------------------------------------
# Update
# ---------------------------------------------------------------------------


class TestUpdate:
    """Tests for updating rows."""

    def test_update_all_rows(self, populated_db: InMemoryDB) -> None:
        count = populated_db.update("users", {"active": False})
        assert count == 3
        rows = populated_db.select("users")
        assert all(r["active"] is False for r in rows)

    def test_update_with_where(self, populated_db: InMemoryDB) -> None:
        count = populated_db.update(
            "users",
            {"active": True},
            where=lambda r: r["name"] == "Bob",
        )
        assert count == 1
        bob = populated_db.select("users", where=lambda r: r["name"] == "Bob")
        assert bob[0]["active"] is True

    def test_update_returns_zero_when_no_match(self, populated_db: InMemoryDB) -> None:
        count = populated_db.update(
            "users",
            {"active": False},
            where=lambda r: r["id"] > 100,
        )
        assert count == 0

    def test_update_unknown_column_raises(self, populated_db: InMemoryDB) -> None:
        with pytest.raises(ColumnError, match="Unknown column"):
            populated_db.update("users", {"bad_col": "value"})

    def test_update_wrong_type_raises(self, populated_db: InMemoryDB) -> None:
        with pytest.raises(ColumnError, match="expects type"):
            populated_db.update("users", {"id": "not_int"})

    def test_update_pk_single_row(self, populated_db: InMemoryDB) -> None:
        """Updating PK on a single matching row should succeed."""
        count = populated_db.update(
            "users",
            {"id": 99},
            where=lambda r: r["id"] == 1,
        )
        assert count == 1
        rows = populated_db.select("users", where=lambda r: r["id"] == 99)
        assert len(rows) == 1
        assert rows[0]["name"] == "Alice"

    def test_update_pk_collision_raises(self, populated_db: InMemoryDB) -> None:
        """Updating PK to an already-existing value should raise."""
        with pytest.raises(PrimaryKeyError, match="Duplicate"):
            populated_db.update(
                "users",
                {"id": 2},
                where=lambda r: r["id"] == 1,
            )

    def test_update_pk_multiple_rows_raises(self, populated_db: InMemoryDB) -> None:
        """Setting same PK on multiple rows should raise."""
        with pytest.raises(PrimaryKeyError, match="multiple rows"):
            populated_db.update(
                "users",
                {"id": 99},
                where=lambda r: r["active"] is True,
            )

    def test_update_nonexistent_table_raises(self, db: InMemoryDB) -> None:
        with pytest.raises(TableNotFoundError):
            db.update("nope", {"a": 1})

    def test_update_pk_to_same_value(self, populated_db: InMemoryDB) -> None:
        """Updating PK to its current value should succeed."""
        count = populated_db.update(
            "users",
            {"id": 1},
            where=lambda r: r["id"] == 1,
        )
        assert count == 1

    def test_update_int_for_float_column(self, db: InMemoryDB) -> None:
        """int values should be coerced to float in updates."""
        db.create_table("t", {"val": "float"})
        db.insert("t", {"val": 1.5})
        db.update("t", {"val": 3})
        rows = db.select("t")
        assert rows[0]["val"] == 3.0
        assert isinstance(rows[0]["val"], float)

    def test_update_atomicity_on_pk_collision(self, db: InMemoryDB) -> None:
        """If PK update collides, no rows should have been modified."""
        db.create_table("t", {"id": "int", "val": "str"}, primary_key="id")
        db.insert("t", {"id": 1, "val": "a"})
        db.insert("t", {"id": 2, "val": "b"})

        with pytest.raises(PrimaryKeyError):
            db.update("t", {"id": 2}, where=lambda r: r["id"] == 1)

        # Verify row 1 was NOT mutated
        row1 = db.select("t", where=lambda r: r["id"] == 1)
        assert len(row1) == 1
        assert row1[0]["val"] == "a"

    def test_update_multiple_columns_at_once(self, populated_db: InMemoryDB) -> None:
        """Update multiple non-PK columns simultaneously."""
        count = populated_db.update(
            "users",
            {"name": "Updated", "active": False},
            where=lambda r: r["id"] == 1,
        )
        assert count == 1
        row = populated_db.select("users", where=lambda r: r["id"] == 1)
        assert row[0]["name"] == "Updated"
        assert row[0]["active"] is False

    def test_update_to_none(self, db: InMemoryDB) -> None:
        """Setting a non-PK column to None should work."""
        db.create_table("t", {"id": "int", "val": "str"}, primary_key="id")
        db.insert("t", {"id": 1, "val": "hello"})
        db.update("t", {"val": None}, where=lambda r: r["id"] == 1)
        rows = db.select("t")
        assert rows[0]["val"] is None


# ---------------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------------


class TestDelete:
    """Tests for deleting rows."""

    def test_delete_all_rows(self, populated_db: InMemoryDB) -> None:
        count = populated_db.delete("users")
        assert count == 3
        assert populated_db.select("users") == []

    def test_delete_with_where(self, populated_db: InMemoryDB) -> None:
        count = populated_db.delete("users", where=lambda r: r["name"] == "Bob")
        assert count == 1
        assert len(populated_db.select("users")) == 2

    def test_delete_no_match(self, populated_db: InMemoryDB) -> None:
        count = populated_db.delete("users", where=lambda r: r["id"] > 100)
        assert count == 0
        assert len(populated_db.select("users")) == 3

    def test_delete_nonexistent_table_raises(self, db: InMemoryDB) -> None:
        with pytest.raises(TableNotFoundError):
            db.delete("nope")

    def test_delete_clears_pk_index(self, users_db: InMemoryDB) -> None:
        """After delete, the PK value should be reusable."""
        users_db.insert("users", {"id": 1, "name": "A", "active": True})
        users_db.delete("users", where=lambda r: r["id"] == 1)
        # Should not raise duplicate PK
        users_db.insert("users", {"id": 1, "name": "B", "active": False})
        rows = users_db.select("users")
        assert len(rows) == 1
        assert rows[0]["name"] == "B"

    def test_delete_all_clears_pk_index(self, populated_db: InMemoryDB) -> None:
        """Delete without where should also clear PK index."""
        populated_db.delete("users")
        populated_db.insert("users", {"id": 1, "name": "Reuse", "active": True})
        assert len(populated_db.select("users")) == 1

    def test_delete_preserves_unmatched_rows(self, populated_db: InMemoryDB) -> None:
        populated_db.delete("users", where=lambda r: r["active"] is False)
        remaining = populated_db.select("users")
        assert len(remaining) == 2
        assert all(r["active"] is True for r in remaining)


# ---------------------------------------------------------------------------
# Table class directly
# ---------------------------------------------------------------------------


class TestTableDirect:
    """Test Table class directly for lower-level edge cases."""

    def test_table_empty_columns(self) -> None:
        """Table with no columns is technically valid."""
        t = Table("empty", {})
        t.insert({})
        assert t.select() == [{}]

    def test_validate_row_none_value_skips_type_check(self) -> None:
        """None values should be accepted for any column type."""
        t = Table("t", {"a": "int", "b": "str"})
        t.insert({"a": None, "b": None})
        rows = t.select()
        assert rows[0] == {"a": None, "b": None}

    def test_select_projected_does_not_include_extra(self) -> None:
        t = Table("t", {"a": "int", "b": "str", "c": "bool"})
        t.insert({"a": 1, "b": "x", "c": True})
        rows = t.select(columns=["a", "c"])
        assert rows == [{"a": 1, "c": True}]

    def test_multiple_inserts_ordering(self) -> None:
        """Rows should be returned in insertion order."""
        t = Table("t", {"val": "int"})
        for i in range(5):
            t.insert({"val": i})
        rows = t.select()
        assert [r["val"] for r in rows] == [0, 1, 2, 3, 4]


# ---------------------------------------------------------------------------
# Integration / cross-operation tests
# ---------------------------------------------------------------------------


class TestIntegration:
    """Integration tests across multiple operations."""

    def test_insert_update_select_delete_lifecycle(self, db: InMemoryDB) -> None:
        db.create_table("items", {"id": "int", "name": "str"}, primary_key="id")
        db.insert("items", {"id": 1, "name": "Widget"})
        db.update(
            "items",
            {"name": "Gadget"},
            where=lambda r: r["id"] == 1,
        )
        rows = db.select("items")
        assert rows[0]["name"] == "Gadget"
        db.delete("items", where=lambda r: r["id"] == 1)
        assert db.select("items") == []

    def test_multiple_tables_independent(self, db: InMemoryDB) -> None:
        db.create_table("a", {"x": "int"})
        db.create_table("b", {"y": "str"})
        db.insert("a", {"x": 1})
        db.insert("b", {"y": "hello"})
        assert len(db.select("a")) == 1
        assert len(db.select("b")) == 1
        db.delete("a")
        assert db.select("a") == []
        assert len(db.select("b")) == 1

    def test_update_then_delete_pk_consistency(self, db: InMemoryDB) -> None:
        """Update PK, then delete, then re-insert with old PK."""
        db.create_table("t", {"id": "int", "v": "str"}, primary_key="id")
        db.insert("t", {"id": 1, "v": "a"})
        db.update("t", {"id": 10}, where=lambda r: r["id"] == 1)
        db.delete("t", where=lambda r: r["id"] == 10)
        # Old PK 1 and updated PK 10 should both be available
        db.insert("t", {"id": 1, "v": "b"})
        db.insert("t", {"id": 10, "v": "c"})
        assert len(db.select("t")) == 2

    def test_where_lambda_complex_filter(self, populated_db: InMemoryDB) -> None:
        """Complex where clause combining multiple conditions."""
        rows = populated_db.select(
            "users",
            where=lambda r: r["id"] >= 2 and r["active"] is True,
        )
        assert len(rows) == 1
        assert rows[0]["name"] == "Charlie"

    def test_bulk_insert_and_select(self, db: InMemoryDB) -> None:
        db.create_table("t", {"id": "int", "val": "str"}, primary_key="id")
        for i in range(100):
            db.insert("t", {"id": i, "val": f"item_{i}"})
        assert len(db.select("t")) == 100
        filtered = db.select("t", where=lambda r: r["id"] % 10 == 0)
        assert len(filtered) == 10

    def test_float_column_operations(self, db: InMemoryDB) -> None:
        """Float columns with insert, update, select."""
        db.create_table(
            "measures",
            {"id": "int", "value": "float"},
            primary_key="id",
        )
        db.insert("measures", {"id": 1, "value": 3.14})
        db.insert("measures", {"id": 2, "value": 2})  # int coerced
        db.update(
            "measures",
            {"value": 0.0},
            where=lambda r: r["id"] == 1,
        )
        rows = db.select("measures", where=lambda r: r["value"] == 0.0)
        assert len(rows) == 1
        assert rows[0]["id"] == 1

    def test_drop_and_recreate_preserves_nothing(self, db: InMemoryDB) -> None:
        """Dropping and recreating a table starts completely fresh."""
        db.create_table("t", {"id": "int", "val": "str"}, primary_key="id")
        db.insert("t", {"id": 1, "val": "a"})
        db.drop_table("t")
        db.create_table("t", {"id": "int", "val": "str"}, primary_key="id")
        assert db.select("t") == []
        # PK index should also be fresh
        db.insert("t", {"id": 1, "val": "b"})
        assert len(db.select("t")) == 1
