"""Tests for the inmemory_db challenge."""

import pytest

from challenges.inmemory_db import InMemoryDB


class TestCreateTable:
    """Tests for table creation."""

    def test_create_table(self) -> None:
        db = InMemoryDB()
        db.create_table("users", {"id": int, "name": str}, primary_key="id")

    def test_create_multiple_tables(self) -> None:
        db = InMemoryDB()
        db.create_table("users", {"id": int, "name": str}, primary_key="id")
        db.create_table("orders", {"id": int, "total": float}, primary_key="id")

    def test_create_duplicate_table_raises(self) -> None:
        db = InMemoryDB()
        db.create_table("users", {"id": int, "name": str}, primary_key="id")
        with pytest.raises(ValueError):
            db.create_table("users", {"id": int, "name": str}, primary_key="id")

    def test_create_table_pk_not_in_columns_raises(self) -> None:
        db = InMemoryDB()
        with pytest.raises(ValueError):
            db.create_table("users", {"id": int, "name": str}, primary_key="missing")


class TestDropTable:
    """Tests for table dropping."""

    def test_drop_table(self) -> None:
        db = InMemoryDB()
        db.create_table("users", {"id": int, "name": str}, primary_key="id")
        db.drop_table("users")

    def test_drop_table_allows_recreation(self) -> None:
        db = InMemoryDB()
        db.create_table("users", {"id": int, "name": str}, primary_key="id")
        db.drop_table("users")
        db.create_table("users", {"id": int, "name": str}, primary_key="id")

    def test_drop_nonexistent_table_raises(self) -> None:
        db = InMemoryDB()
        with pytest.raises(ValueError):
            db.drop_table("nonexistent")


class TestInsert:
    """Tests for row insertion."""

    def test_insert_single_row(self) -> None:
        db = InMemoryDB()
        db.create_table("users", {"id": int, "name": str}, primary_key="id")
        db.insert("users", {"id": 1, "name": "Alice"})
        rows = db.select("users")
        assert rows == [{"id": 1, "name": "Alice"}]

    def test_insert_multiple_rows(self) -> None:
        db = InMemoryDB()
        db.create_table("users", {"id": int, "name": str}, primary_key="id")
        db.insert("users", {"id": 1, "name": "Alice"})
        db.insert("users", {"id": 2, "name": "Bob"})
        rows = db.select("users")
        assert len(rows) == 2

    def test_insert_into_nonexistent_table_raises(self) -> None:
        db = InMemoryDB()
        with pytest.raises(ValueError):
            db.insert("missing", {"id": 1, "name": "Alice"})

    def test_insert_duplicate_primary_key_raises(self) -> None:
        db = InMemoryDB()
        db.create_table("users", {"id": int, "name": str}, primary_key="id")
        db.insert("users", {"id": 1, "name": "Alice"})
        with pytest.raises(ValueError):
            db.insert("users", {"id": 1, "name": "Bob"})

    def test_insert_wrong_column_type_raises(self) -> None:
        db = InMemoryDB()
        db.create_table("users", {"id": int, "name": str}, primary_key="id")
        with pytest.raises(TypeError):
            db.insert("users", {"id": "not_an_int", "name": "Alice"})

    def test_insert_wrong_type_on_non_pk_column_raises(self) -> None:
        db = InMemoryDB()
        db.create_table("users", {"id": int, "name": str}, primary_key="id")
        with pytest.raises(TypeError):
            db.insert("users", {"id": 1, "name": 999})


class TestSelect:
    """Tests for row selection."""

    def test_select_empty_table(self) -> None:
        db = InMemoryDB()
        db.create_table("users", {"id": int, "name": str}, primary_key="id")
        assert db.select("users") == []

    def test_select_all_rows(self) -> None:
        db = InMemoryDB()
        db.create_table("users", {"id": int, "name": str}, primary_key="id")
        db.insert("users", {"id": 1, "name": "Alice"})
        db.insert("users", {"id": 2, "name": "Bob"})
        rows = db.select("users")
        assert len(rows) == 2

    def test_select_with_where_filter(self) -> None:
        db = InMemoryDB()
        db.create_table("users", {"id": int, "name": str}, primary_key="id")
        db.insert("users", {"id": 1, "name": "Alice"})
        db.insert("users", {"id": 2, "name": "Bob"})
        rows = db.select("users", where={"name": "Alice"})
        assert rows == [{"id": 1, "name": "Alice"}]

    def test_select_with_where_no_match(self) -> None:
        db = InMemoryDB()
        db.create_table("users", {"id": int, "name": str}, primary_key="id")
        db.insert("users", {"id": 1, "name": "Alice"})
        rows = db.select("users", where={"name": "Nobody"})
        assert rows == []

    def test_select_with_multiple_where_conditions(self) -> None:
        db = InMemoryDB()
        db.create_table("users", {"id": int, "name": str, "age": int}, primary_key="id")
        db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        db.insert("users", {"id": 2, "name": "Bob", "age": 30})
        db.insert("users", {"id": 3, "name": "Alice", "age": 25})
        rows = db.select("users", where={"name": "Alice", "age": 30})
        assert rows == [{"id": 1, "name": "Alice", "age": 30}]

    def test_select_returns_copies(self) -> None:
        """Modifying returned rows should not affect stored data."""
        db = InMemoryDB()
        db.create_table("users", {"id": int, "name": str}, primary_key="id")
        db.insert("users", {"id": 1, "name": "Alice"})
        rows = db.select("users")
        rows[0]["name"] = "Mutated"
        original = db.select("users")
        assert original[0]["name"] == "Alice"


class TestUpdate:
    """Tests for row updates."""

    def test_update_all_rows(self) -> None:
        db = InMemoryDB()
        db.create_table("users", {"id": int, "name": str}, primary_key="id")
        db.insert("users", {"id": 1, "name": "Alice"})
        db.insert("users", {"id": 2, "name": "Bob"})
        count = db.update("users", values={"name": "Updated"})
        assert count == 2
        rows = db.select("users")
        assert all(r["name"] == "Updated" for r in rows)

    def test_update_with_where_filter(self) -> None:
        db = InMemoryDB()
        db.create_table("users", {"id": int, "name": str}, primary_key="id")
        db.insert("users", {"id": 1, "name": "Alice"})
        db.insert("users", {"id": 2, "name": "Bob"})
        count = db.update("users", values={"name": "Updated"}, where={"id": 1})
        assert count == 1
        rows = db.select("users", where={"id": 1})
        assert rows[0]["name"] == "Updated"

    def test_update_no_matches_returns_zero(self) -> None:
        db = InMemoryDB()
        db.create_table("users", {"id": int, "name": str}, primary_key="id")
        db.insert("users", {"id": 1, "name": "Alice"})
        count = db.update("users", values={"name": "Updated"}, where={"id": 999})
        assert count == 0

    def test_update_wrong_type_raises(self) -> None:
        db = InMemoryDB()
        db.create_table("users", {"id": int, "name": str}, primary_key="id")
        db.insert("users", {"id": 1, "name": "Alice"})
        with pytest.raises(TypeError):
            db.update("users", values={"name": 123})

    def test_update_does_not_affect_unmatched_rows(self) -> None:
        db = InMemoryDB()
        db.create_table("users", {"id": int, "name": str}, primary_key="id")
        db.insert("users", {"id": 1, "name": "Alice"})
        db.insert("users", {"id": 2, "name": "Bob"})
        db.update("users", values={"name": "Updated"}, where={"id": 1})
        bob = db.select("users", where={"id": 2})
        assert bob[0]["name"] == "Bob"


class TestDelete:
    """Tests for row deletion."""

    def test_delete_all_rows(self) -> None:
        db = InMemoryDB()
        db.create_table("users", {"id": int, "name": str}, primary_key="id")
        db.insert("users", {"id": 1, "name": "Alice"})
        db.insert("users", {"id": 2, "name": "Bob"})
        count = db.delete("users")
        assert count == 2
        assert db.select("users") == []

    def test_delete_with_where_filter(self) -> None:
        db = InMemoryDB()
        db.create_table("users", {"id": int, "name": str}, primary_key="id")
        db.insert("users", {"id": 1, "name": "Alice"})
        db.insert("users", {"id": 2, "name": "Bob"})
        count = db.delete("users", where={"id": 1})
        assert count == 1
        rows = db.select("users")
        assert len(rows) == 1
        assert rows[0]["name"] == "Bob"

    def test_delete_no_matches_returns_zero(self) -> None:
        db = InMemoryDB()
        db.create_table("users", {"id": int, "name": str}, primary_key="id")
        db.insert("users", {"id": 1, "name": "Alice"})
        count = db.delete("users", where={"id": 999})
        assert count == 0

    def test_delete_from_empty_table_returns_zero(self) -> None:
        db = InMemoryDB()
        db.create_table("users", {"id": int, "name": str}, primary_key="id")
        count = db.delete("users")
        assert count == 0

    def test_delete_does_not_affect_unmatched_rows(self) -> None:
        db = InMemoryDB()
        db.create_table("users", {"id": int, "name": str}, primary_key="id")
        db.insert("users", {"id": 1, "name": "Alice"})
        db.insert("users", {"id": 2, "name": "Bob"})
        db.delete("users", where={"id": 1})
        remaining = db.select("users")
        assert len(remaining) == 1
        assert remaining[0]["id"] == 2
