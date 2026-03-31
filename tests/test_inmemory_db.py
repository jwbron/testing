"""Tests for the in-memory database module."""

import pytest

from challenges.inmemory_db import (
    ColumnError,
    InMemoryDB,
    PrimaryKeyViolationError,
    TableExistsError,
    TableNotFoundError,
    TypeValidationError,
)


@pytest.fixture
def db() -> InMemoryDB:
    """Return a fresh InMemoryDB instance."""
    return InMemoryDB()


@pytest.fixture
def users_db(db: InMemoryDB) -> InMemoryDB:
    """Return an InMemoryDB with a pre-created 'users' table."""
    db.create_table("users", {"id": int, "name": str, "age": int}, primary_key="id")
    return db


# ---------------------------------------------------------------------------
# Table creation / dropping
# ---------------------------------------------------------------------------


class TestCreateTable:
    def test_create_table(self, db: InMemoryDB) -> None:
        db.create_table("t", {"id": int}, primary_key="id")
        assert db.select("t") == []

    def test_create_duplicate_table_raises(self, db: InMemoryDB) -> None:
        db.create_table("t", {"id": int}, primary_key="id")
        with pytest.raises(TableExistsError):
            db.create_table("t", {"id": int}, primary_key="id")

    def test_create_table_bad_primary_key(self, db: InMemoryDB) -> None:
        with pytest.raises(ColumnError):
            db.create_table("t", {"id": int}, primary_key="missing")


class TestDropTable:
    def test_drop_existing_table(self, db: InMemoryDB) -> None:
        db.create_table("t", {"id": int}, primary_key="id")
        db.drop_table("t")
        with pytest.raises(TableNotFoundError):
            db.select("t")

    def test_drop_nonexistent_table_raises(self, db: InMemoryDB) -> None:
        with pytest.raises(TableNotFoundError):
            db.drop_table("ghost")


# ---------------------------------------------------------------------------
# Insert
# ---------------------------------------------------------------------------


class TestInsert:
    def test_insert_and_select(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        rows = users_db.select("users")
        assert rows == [{"id": 1, "name": "Alice", "age": 30}]

    def test_insert_multiple_rows(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        users_db.insert("users", {"id": 2, "name": "Bob", "age": 25})
        assert len(users_db.select("users")) == 2

    def test_insert_duplicate_pk_raises(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        with pytest.raises(PrimaryKeyViolationError):
            users_db.insert("users", {"id": 1, "name": "Bob", "age": 25})

    def test_insert_wrong_type_raises(self, users_db: InMemoryDB) -> None:
        with pytest.raises(TypeValidationError):
            users_db.insert("users", {"id": "not_an_int", "name": "Alice", "age": 30})

    def test_insert_missing_column_raises(self, users_db: InMemoryDB) -> None:
        with pytest.raises(ColumnError):
            users_db.insert("users", {"id": 1, "name": "Alice"})

    def test_insert_unknown_column_raises(self, users_db: InMemoryDB) -> None:
        with pytest.raises(ColumnError):
            users_db.insert(
                "users", {"id": 1, "name": "Alice", "age": 30, "extra": True}
            )

    def test_insert_into_missing_table_raises(self, db: InMemoryDB) -> None:
        with pytest.raises(TableNotFoundError):
            db.insert("ghost", {"id": 1})

    def test_insert_returns_copy(self, users_db: InMemoryDB) -> None:
        """Mutating the original dict should not affect the stored row."""
        row = {"id": 1, "name": "Alice", "age": 30}
        users_db.insert("users", row)
        row["name"] = "MUTATED"
        assert users_db.select("users")[0]["name"] == "Alice"


# ---------------------------------------------------------------------------
# Select
# ---------------------------------------------------------------------------


class TestSelect:
    def test_select_all(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        users_db.insert("users", {"id": 2, "name": "Bob", "age": 25})
        assert len(users_db.select("users")) == 2

    def test_select_with_dict_filter(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        users_db.insert("users", {"id": 2, "name": "Bob", "age": 25})
        result = users_db.select("users", where={"name": "Alice"})
        assert result == [{"id": 1, "name": "Alice", "age": 30}]

    def test_select_with_callable_filter(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        users_db.insert("users", {"id": 2, "name": "Bob", "age": 25})
        result = users_db.select("users", where=lambda row: row["age"] > 27)
        assert result == [{"id": 1, "name": "Alice", "age": 30}]

    def test_select_no_match(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        assert users_db.select("users", where={"name": "Nobody"}) == []

    def test_select_from_missing_table_raises(self, db: InMemoryDB) -> None:
        with pytest.raises(TableNotFoundError):
            db.select("ghost")

    def test_select_returns_copies(self, users_db: InMemoryDB) -> None:
        """Mutating selected rows should not affect stored data."""
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        rows = users_db.select("users")
        rows[0]["name"] = "MUTATED"
        assert users_db.select("users")[0]["name"] == "Alice"

    def test_select_multi_column_filter(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        users_db.insert("users", {"id": 2, "name": "Alice", "age": 25})
        result = users_db.select("users", where={"name": "Alice", "age": 30})
        assert len(result) == 1
        assert result[0]["id"] == 1


# ---------------------------------------------------------------------------
# Update
# ---------------------------------------------------------------------------


class TestUpdate:
    def test_update_single_row(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        count = users_db.update("users", {"age": 31}, where={"id": 1})
        assert count == 1
        assert users_db.select("users")[0]["age"] == 31

    def test_update_multiple_rows(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        users_db.insert("users", {"id": 2, "name": "Bob", "age": 30})
        count = users_db.update("users", {"age": 99}, where={"age": 30})
        assert count == 2

    def test_update_no_match(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        count = users_db.update("users", {"age": 99}, where={"id": 999})
        assert count == 0

    def test_update_all_rows(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        users_db.insert("users", {"id": 2, "name": "Bob", "age": 25})
        count = users_db.update("users", {"age": 0})
        assert count == 2
        assert all(r["age"] == 0 for r in users_db.select("users"))

    def test_update_wrong_type_raises(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        with pytest.raises(TypeValidationError):
            users_db.update("users", {"age": "old"}, where={"id": 1})

    def test_update_unknown_column_raises(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        with pytest.raises(ColumnError):
            users_db.update("users", {"nonexistent": 1}, where={"id": 1})

    def test_update_pk_duplicate_raises(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        users_db.insert("users", {"id": 2, "name": "Bob", "age": 25})
        with pytest.raises(PrimaryKeyViolationError):
            users_db.update("users", {"id": 2}, where={"id": 1})

    def test_update_pk_same_value_ok(self, users_db: InMemoryDB) -> None:
        """Updating PK to the same value should not raise."""
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        count = users_db.update("users", {"id": 1, "name": "Alicia"}, where={"id": 1})
        assert count == 1

    def test_update_missing_table_raises(self, db: InMemoryDB) -> None:
        with pytest.raises(TableNotFoundError):
            db.update("ghost", {"x": 1})


# ---------------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------------


class TestDelete:
    def test_delete_single_row(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        users_db.insert("users", {"id": 2, "name": "Bob", "age": 25})
        count = users_db.delete("users", where={"id": 1})
        assert count == 1
        assert len(users_db.select("users")) == 1

    def test_delete_all_rows(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        users_db.insert("users", {"id": 2, "name": "Bob", "age": 25})
        count = users_db.delete("users")
        assert count == 2
        assert users_db.select("users") == []

    def test_delete_no_match(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        count = users_db.delete("users", where={"id": 999})
        assert count == 0

    def test_delete_frees_pk(self, users_db: InMemoryDB) -> None:
        """After deleting a row, its PK should be reusable."""
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        users_db.delete("users", where={"id": 1})
        users_db.insert("users", {"id": 1, "name": "Alice2", "age": 31})
        assert users_db.select("users")[0]["name"] == "Alice2"

    def test_delete_with_callable_filter(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        users_db.insert("users", {"id": 2, "name": "Bob", "age": 25})
        count = users_db.delete("users", where=lambda row: row["age"] < 28)
        assert count == 1
        assert users_db.select("users")[0]["name"] == "Alice"

    def test_delete_missing_table_raises(self, db: InMemoryDB) -> None:
        with pytest.raises(TableNotFoundError):
            db.delete("ghost")


# ---------------------------------------------------------------------------
# Integration / multi-table
# ---------------------------------------------------------------------------


class TestMultiTable:
    def test_independent_tables(self, db: InMemoryDB) -> None:
        db.create_table("a", {"id": int}, primary_key="id")
        db.create_table("b", {"id": int}, primary_key="id")
        db.insert("a", {"id": 1})
        db.insert("b", {"id": 1})
        assert len(db.select("a")) == 1
        assert len(db.select("b")) == 1

    def test_drop_and_recreate(self, db: InMemoryDB) -> None:
        db.create_table("t", {"id": int}, primary_key="id")
        db.insert("t", {"id": 1})
        db.drop_table("t")
        db.create_table("t", {"id": int, "val": str}, primary_key="id")
        db.insert("t", {"id": 1, "val": "new"})
        assert db.select("t") == [{"id": 1, "val": "new"}]
