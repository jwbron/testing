"""Tests for the in-memory database challenge."""

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
    """Provide a fresh InMemoryDB instance."""
    return InMemoryDB()


@pytest.fixture
def users_db(db: InMemoryDB) -> InMemoryDB:
    """Provide an InMemoryDB with a 'users' table pre-created."""
    db.create_table("users", {"id": int, "name": str, "age": int}, primary_key="id")
    return db


class TestCreateTable:
    """Tests for table creation."""

    def test_create_table(self, db: InMemoryDB) -> None:
        db.create_table("items", {"id": int, "label": str})
        assert db.select("items") == []

    def test_create_table_with_primary_key(self, db: InMemoryDB) -> None:
        db.create_table("items", {"id": int, "label": str}, primary_key="id")
        assert db.select("items") == []

    def test_duplicate_table_raises(self, db: InMemoryDB) -> None:
        db.create_table("items", {"id": int})
        with pytest.raises(TableExistsError, match="items"):
            db.create_table("items", {"id": int})

    def test_primary_key_not_in_columns_raises(self, db: InMemoryDB) -> None:
        with pytest.raises(ColumnError, match="missing_col"):
            db.create_table(
                "items", {"id": int}, primary_key="missing_col"
            )


class TestDropTable:
    """Tests for table dropping."""

    def test_drop_existing_table(self, db: InMemoryDB) -> None:
        db.create_table("temp", {"id": int})
        db.drop_table("temp")
        with pytest.raises(TableNotFoundError):
            db.select("temp")

    def test_drop_nonexistent_table_raises(self, db: InMemoryDB) -> None:
        with pytest.raises(TableNotFoundError, match="ghost"):
            db.drop_table("ghost")

    def test_recreate_after_drop(self, db: InMemoryDB) -> None:
        db.create_table("temp", {"id": int})
        db.drop_table("temp")
        db.create_table("temp", {"id": int, "extra": str})
        assert db.select("temp") == []


class TestInsert:
    """Tests for row insertion."""

    def test_insert_and_select(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        rows = users_db.select("users")
        assert rows == [{"id": 1, "name": "Alice", "age": 30}]

    def test_insert_multiple_rows(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        users_db.insert("users", {"id": 2, "name": "Bob", "age": 25})
        assert len(users_db.select("users")) == 2

    def test_insert_missing_table_raises(self, db: InMemoryDB) -> None:
        with pytest.raises(TableNotFoundError):
            db.insert("nope", {"id": 1})

    def test_insert_missing_column_raises(self, users_db: InMemoryDB) -> None:
        with pytest.raises(ColumnError, match="Missing"):
            users_db.insert("users", {"id": 1, "name": "Alice"})

    def test_insert_extra_column_raises(self, users_db: InMemoryDB) -> None:
        with pytest.raises(ColumnError, match="Unknown"):
            users_db.insert(
                "users", {"id": 1, "name": "Alice", "age": 30, "email": "a@b.c"}
            )

    def test_insert_wrong_type_raises(self, users_db: InMemoryDB) -> None:
        with pytest.raises(TypeValidationError, match="age"):
            users_db.insert("users", {"id": 1, "name": "Alice", "age": "thirty"})

    def test_insert_duplicate_primary_key_raises(
        self, users_db: InMemoryDB
    ) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        with pytest.raises(PrimaryKeyViolationError, match="id"):
            users_db.insert("users", {"id": 1, "name": "Bob", "age": 25})

    def test_insert_returns_copy(self, users_db: InMemoryDB) -> None:
        """Modifying the inserted dict should not affect stored data."""
        row = {"id": 1, "name": "Alice", "age": 30}
        users_db.insert("users", row)
        row["name"] = "CHANGED"
        assert users_db.select("users")[0]["name"] == "Alice"


class TestSelect:
    """Tests for row selection."""

    def test_select_all(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        users_db.insert("users", {"id": 2, "name": "Bob", "age": 25})
        assert len(users_db.select("users")) == 2

    def test_select_with_filter(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        users_db.insert("users", {"id": 2, "name": "Bob", "age": 25})
        result = users_db.select("users", where=lambda r: r["age"] > 26)
        assert result == [{"id": 1, "name": "Alice", "age": 30}]

    def test_select_no_match(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        result = users_db.select("users", where=lambda r: r["age"] > 100)
        assert result == []

    def test_select_returns_copies(self, users_db: InMemoryDB) -> None:
        """Modifying selected rows should not affect stored data."""
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        rows = users_db.select("users")
        rows[0]["name"] = "CHANGED"
        assert users_db.select("users")[0]["name"] == "Alice"

    def test_select_missing_table_raises(self, db: InMemoryDB) -> None:
        with pytest.raises(TableNotFoundError):
            db.select("nope")


class TestUpdate:
    """Tests for row updates."""

    def test_update_all_rows(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        users_db.insert("users", {"id": 2, "name": "Bob", "age": 25})
        count = users_db.update("users", {"age": 99})
        assert count == 2
        for row in users_db.select("users"):
            assert row["age"] == 99

    def test_update_with_filter(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        users_db.insert("users", {"id": 2, "name": "Bob", "age": 25})
        count = users_db.update(
            "users", {"age": 31}, where=lambda r: r["name"] == "Alice"
        )
        assert count == 1
        alice = users_db.select("users", where=lambda r: r["id"] == 1)[0]
        assert alice["age"] == 31

    def test_update_no_match(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        count = users_db.update(
            "users", {"age": 99}, where=lambda r: r["id"] == 999
        )
        assert count == 0

    def test_update_unknown_column_raises(self, users_db: InMemoryDB) -> None:
        with pytest.raises(ColumnError, match="Unknown"):
            users_db.update("users", {"email": "a@b.c"})

    def test_update_wrong_type_raises(self, users_db: InMemoryDB) -> None:
        with pytest.raises(TypeValidationError, match="age"):
            users_db.update("users", {"age": "old"})

    def test_update_pk_duplicate_raises(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        users_db.insert("users", {"id": 2, "name": "Bob", "age": 25})
        with pytest.raises(PrimaryKeyViolationError):
            users_db.update(
                "users", {"id": 2}, where=lambda r: r["name"] == "Alice"
            )

    def test_update_missing_table_raises(self, db: InMemoryDB) -> None:
        with pytest.raises(TableNotFoundError):
            db.update("nope", {"x": 1})


class TestDelete:
    """Tests for row deletion."""

    def test_delete_all(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        users_db.insert("users", {"id": 2, "name": "Bob", "age": 25})
        count = users_db.delete("users")
        assert count == 2
        assert users_db.select("users") == []

    def test_delete_with_filter(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        users_db.insert("users", {"id": 2, "name": "Bob", "age": 25})
        count = users_db.delete("users", where=lambda r: r["name"] == "Alice")
        assert count == 1
        remaining = users_db.select("users")
        assert len(remaining) == 1
        assert remaining[0]["name"] == "Bob"

    def test_delete_no_match(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        count = users_db.delete("users", where=lambda r: r["id"] == 999)
        assert count == 0
        assert len(users_db.select("users")) == 1

    def test_delete_missing_table_raises(self, db: InMemoryDB) -> None:
        with pytest.raises(TableNotFoundError):
            db.delete("nope")


class TestTableWithoutPrimaryKey:
    """Tests for tables that have no primary key."""

    def test_insert_duplicate_values_allowed(self, db: InMemoryDB) -> None:
        db.create_table("logs", {"level": str, "msg": str})
        db.insert("logs", {"level": "INFO", "msg": "hello"})
        db.insert("logs", {"level": "INFO", "msg": "hello"})
        assert len(db.select("logs")) == 2
