"""Gap-filling tests for the inmemory_db challenge.

These tests target edge cases, boundary conditions, and uncovered code paths
not addressed by the coder's original test suite.
"""

import pytest

from challenges.inmemory_db import InMemoryDB


class TestCreateTableEdgeCases:
    """Edge cases for table creation."""

    def test_create_table_empty_columns_with_pk_raises(self) -> None:
        """Primary key must be in columns — empty columns means always invalid."""
        db = InMemoryDB()
        with pytest.raises(ValueError):
            db.create_table("t", {}, primary_key="id")

    def test_create_table_single_column_as_pk(self) -> None:
        """A table with only a primary key column should work."""
        db = InMemoryDB()
        db.create_table("t", {"id": int}, primary_key="id")
        db.insert("t", {"id": 1})
        assert db.select("t") == [{"id": 1}]

    def test_create_table_many_columns(self) -> None:
        """Table with many columns should work."""
        db = InMemoryDB()
        cols = {f"col_{i}": int for i in range(20)}
        cols["id"] = int
        db.create_table("big", cols, primary_key="id")
        row = {f"col_{i}": i for i in range(20)}
        row["id"] = 1
        db.insert("big", row)
        assert db.select("big") == [row]

    def test_table_names_are_case_sensitive(self) -> None:
        """'Users' and 'users' should be different tables."""
        db = InMemoryDB()
        db.create_table("users", {"id": int}, primary_key="id")
        db.create_table("Users", {"id": int}, primary_key="id")
        db.insert("users", {"id": 1})
        db.insert("Users", {"id": 2})
        assert db.select("users") == [{"id": 1}]
        assert db.select("Users") == [{"id": 2}]


class TestDropTableEdgeCases:
    """Edge cases for table dropping."""

    def test_drop_table_clears_data(self) -> None:
        """After drop+recreate, old data should be gone."""
        db = InMemoryDB()
        db.create_table("t", {"id": int}, primary_key="id")
        db.insert("t", {"id": 1})
        db.drop_table("t")
        db.create_table("t", {"id": int}, primary_key="id")
        assert db.select("t") == []

    def test_drop_one_table_preserves_others(self) -> None:
        """Dropping one table should not affect other tables."""
        db = InMemoryDB()
        db.create_table("a", {"id": int}, primary_key="id")
        db.create_table("b", {"id": int}, primary_key="id")
        db.insert("a", {"id": 1})
        db.insert("b", {"id": 2})
        db.drop_table("a")
        assert db.select("b") == [{"id": 2}]


class TestInsertEdgeCases:
    """Edge cases for row insertion."""

    def test_insert_extra_columns_not_in_schema(self) -> None:
        """Inserting a row with extra columns not in schema.

        The current implementation does not reject extra columns —
        this test documents that behavior.
        """
        db = InMemoryDB()
        db.create_table("t", {"id": int, "name": str}, primary_key="id")
        # Extra column 'age' not in schema — implementation allows this
        db.insert("t", {"id": 1, "name": "Alice", "age": 30})
        rows = db.select("t")
        assert rows[0]["age"] == 30

    def test_insert_missing_non_pk_column(self) -> None:
        """Inserting a row with missing non-PK columns.

        The current implementation allows partial rows — this documents
        the behavior.
        """
        db = InMemoryDB()
        db.create_table("t", {"id": int, "name": str}, primary_key="id")
        db.insert("t", {"id": 1})
        rows = db.select("t")
        assert rows[0]["id"] == 1
        assert "name" not in rows[0]

    def test_insert_missing_pk_column(self) -> None:
        """Inserting without primary key column — pk_value is None.

        Two rows without PK both have pk_value=None, second should
        fail due to duplicate PK.
        """
        db = InMemoryDB()
        db.create_table("t", {"id": int, "name": str}, primary_key="id")
        db.insert("t", {"name": "Alice"})
        with pytest.raises(ValueError, match="Duplicate primary key"):
            db.insert("t", {"name": "Bob"})

    def test_insert_stores_copy_of_row(self) -> None:
        """Mutating the original dict after insert should not affect DB."""
        db = InMemoryDB()
        db.create_table("t", {"id": int, "name": str}, primary_key="id")
        row = {"id": 1, "name": "Alice"}
        db.insert("t", row)
        row["name"] = "Mutated"
        assert db.select("t")[0]["name"] == "Alice"

    def test_insert_none_as_primary_key_value_raises(self) -> None:
        """Using None as a typed primary key value should fail type validation."""
        db = InMemoryDB()
        db.create_table("t", {"id": int, "name": str}, primary_key="id")
        with pytest.raises(TypeError):
            db.insert("t", {"id": None, "name": "Ghost"})

    def test_insert_preserves_order(self) -> None:
        """Rows should be returned in insertion order."""
        db = InMemoryDB()
        db.create_table("t", {"id": int}, primary_key="id")
        for i in range(5):
            db.insert("t", {"id": i})
        rows = db.select("t")
        assert [r["id"] for r in rows] == [0, 1, 2, 3, 4]

    def test_insert_with_float_column(self) -> None:
        """Float column type validation."""
        db = InMemoryDB()
        db.create_table("t", {"id": int, "price": float}, primary_key="id")
        db.insert("t", {"id": 1, "price": 9.99})
        assert db.select("t")[0]["price"] == 9.99

    def test_insert_float_column_rejects_int(self) -> None:
        """Int value for a float column — isinstance(1, float) is False."""
        db = InMemoryDB()
        db.create_table("t", {"id": int, "price": float}, primary_key="id")
        with pytest.raises(TypeError):
            db.insert("t", {"id": 1, "price": 10})

    def test_insert_bool_passes_int_check(self) -> None:
        """bool is subclass of int in Python, so True passes int type check.

        This documents a potential type confusion issue — bool values are
        accepted for int columns because isinstance(True, int) is True.
        """
        db = InMemoryDB()
        db.create_table("t", {"id": int, "count": int}, primary_key="id")
        # This should arguably fail but passes due to Python's type hierarchy
        db.insert("t", {"id": 1, "count": True})
        rows = db.select("t")
        assert rows[0]["count"] is True

    def test_insert_after_delete_reuses_pk(self) -> None:
        """After deleting a row, its PK value should be reusable."""
        db = InMemoryDB()
        db.create_table("t", {"id": int, "name": str}, primary_key="id")
        db.insert("t", {"id": 1, "name": "Alice"})
        db.delete("t", where={"id": 1})
        db.insert("t", {"id": 1, "name": "Bob"})
        assert db.select("t") == [{"id": 1, "name": "Bob"}]

    def test_insert_string_primary_key(self) -> None:
        """Primary key can be a string column."""
        db = InMemoryDB()
        db.create_table("t", {"code": str, "value": int}, primary_key="code")
        db.insert("t", {"code": "ABC", "value": 1})
        db.insert("t", {"code": "DEF", "value": 2})
        with pytest.raises(ValueError, match="Duplicate primary key"):
            db.insert("t", {"code": "ABC", "value": 3})


class TestSelectEdgeCases:
    """Edge cases for select."""

    def test_select_from_nonexistent_table_raises(self) -> None:
        """Select from a table that doesn't exist."""
        db = InMemoryDB()
        with pytest.raises(ValueError):
            db.select("nonexistent")

    def test_select_with_empty_where(self) -> None:
        """Empty where dict — should match all rows (all() on empty is True)."""
        db = InMemoryDB()
        db.create_table("t", {"id": int, "name": str}, primary_key="id")
        db.insert("t", {"id": 1, "name": "Alice"})
        db.insert("t", {"id": 2, "name": "Bob"})
        rows = db.select("t", where={})
        assert len(rows) == 2

    def test_select_where_column_not_in_row(self) -> None:
        """Filtering on a column that doesn't exist in the row.

        row.get(k) returns None, so filtering on non-existent column
        with a non-None value returns no matches.
        """
        db = InMemoryDB()
        db.create_table("t", {"id": int}, primary_key="id")
        db.insert("t", {"id": 1})
        rows = db.select("t", where={"missing_col": "value"})
        assert rows == []

    def test_select_where_column_not_in_row_with_none(self) -> None:
        """Filtering on non-existent column with None value.

        row.get('missing') returns None, None == None is True, so it matches.
        """
        db = InMemoryDB()
        db.create_table("t", {"id": int}, primary_key="id")
        db.insert("t", {"id": 1})
        rows = db.select("t", where={"missing_col": None})
        assert len(rows) == 1  # Matches because get returns None

    def test_select_returns_independent_copies(self) -> None:
        """Two select calls return independent copies."""
        db = InMemoryDB()
        db.create_table("t", {"id": int, "name": str}, primary_key="id")
        db.insert("t", {"id": 1, "name": "Alice"})
        rows1 = db.select("t")
        rows2 = db.select("t")
        rows1[0]["name"] = "Mutated"
        assert rows2[0]["name"] == "Alice"


class TestUpdateEdgeCases:
    """Edge cases for update."""

    def test_update_nonexistent_table_raises(self) -> None:
        db = InMemoryDB()
        with pytest.raises(ValueError):
            db.update("nonexistent", values={"x": 1})

    def test_update_with_empty_where(self) -> None:
        """Empty where dict matches all rows."""
        db = InMemoryDB()
        db.create_table("t", {"id": int, "name": str}, primary_key="id")
        db.insert("t", {"id": 1, "name": "Alice"})
        db.insert("t", {"id": 2, "name": "Bob"})
        count = db.update("t", values={"name": "Updated"}, where={})
        assert count == 2

    def test_update_empty_table_returns_zero(self) -> None:
        db = InMemoryDB()
        db.create_table("t", {"id": int, "name": str}, primary_key="id")
        count = db.update("t", values={"name": "Updated"})
        assert count == 0

    def test_update_primary_key_to_duplicate_raises(self) -> None:
        """Updating the primary key to an existing value raises ValueError."""
        db = InMemoryDB()
        db.create_table("t", {"id": int, "name": str}, primary_key="id")
        db.insert("t", {"id": 1, "name": "Alice"})
        db.insert("t", {"id": 2, "name": "Bob"})
        with pytest.raises(ValueError, match="Duplicate primary key"):
            db.update("t", values={"id": 2}, where={"id": 1})

    def test_update_primary_key_to_new_value_succeeds(self) -> None:
        """Updating the primary key to a non-existing value should work."""
        db = InMemoryDB()
        db.create_table("t", {"id": int, "name": str}, primary_key="id")
        db.insert("t", {"id": 1, "name": "Alice"})
        db.insert("t", {"id": 2, "name": "Bob"})
        count = db.update("t", values={"id": 99}, where={"id": 1})
        assert count == 1
        rows = db.select("t", where={"id": 99})
        assert rows == [{"id": 99, "name": "Alice"}]

    def test_update_extra_column_not_in_schema(self) -> None:
        """Updating with a column not in the schema.

        The type validation only checks columns that ARE in the schema,
        so extra columns are silently added.
        """
        db = InMemoryDB()
        db.create_table("t", {"id": int, "name": str}, primary_key="id")
        db.insert("t", {"id": 1, "name": "Alice"})
        db.update("t", values={"extra": "data"}, where={"id": 1})
        rows = db.select("t", where={"id": 1})
        assert rows[0]["extra"] == "data"

    def test_update_returns_correct_count_for_multiple_matches(self) -> None:
        db = InMemoryDB()
        db.create_table("t", {"id": int, "status": str}, primary_key="id")
        db.insert("t", {"id": 1, "status": "active"})
        db.insert("t", {"id": 2, "status": "active"})
        db.insert("t", {"id": 3, "status": "inactive"})
        count = db.update(
            "t", values={"status": "archived"}, where={"status": "active"}
        )
        assert count == 2


class TestDeleteEdgeCases:
    """Edge cases for delete."""

    def test_delete_from_nonexistent_table_raises(self) -> None:
        db = InMemoryDB()
        with pytest.raises(ValueError):
            db.delete("nonexistent")

    def test_delete_with_empty_where(self) -> None:
        """Empty where dict matches all rows."""
        db = InMemoryDB()
        db.create_table("t", {"id": int, "name": str}, primary_key="id")
        db.insert("t", {"id": 1, "name": "Alice"})
        db.insert("t", {"id": 2, "name": "Bob"})
        count = db.delete("t", where={})
        assert count == 2
        assert db.select("t") == []

    def test_delete_with_multiple_where_conditions(self) -> None:
        """Delete should match ALL conditions in where dict."""
        db = InMemoryDB()
        db.create_table("t", {"id": int, "name": str, "age": int}, primary_key="id")
        db.insert("t", {"id": 1, "name": "Alice", "age": 30})
        db.insert("t", {"id": 2, "name": "Alice", "age": 25})
        db.insert("t", {"id": 3, "name": "Bob", "age": 30})
        count = db.delete("t", where={"name": "Alice", "age": 30})
        assert count == 1
        remaining = db.select("t")
        assert len(remaining) == 2


class TestCRUDIntegration:
    """Integration tests combining multiple operations."""

    def test_full_crud_lifecycle(self) -> None:
        """Create -> Insert -> Select -> Update -> Select -> Delete -> Select."""
        db = InMemoryDB()
        db.create_table("items", {"id": int, "name": str, "qty": int}, primary_key="id")

        # Insert
        db.insert("items", {"id": 1, "name": "Widget", "qty": 10})
        db.insert("items", {"id": 2, "name": "Gadget", "qty": 5})

        # Select
        assert len(db.select("items")) == 2

        # Update
        db.update("items", values={"qty": 0}, where={"id": 1})
        row = db.select("items", where={"id": 1})
        assert row[0]["qty"] == 0

        # Delete
        db.delete("items", where={"qty": 0})
        remaining = db.select("items")
        assert len(remaining) == 1
        assert remaining[0]["name"] == "Gadget"

        # Drop
        db.drop_table("items")
        with pytest.raises(ValueError):
            db.select("items")

    def test_multiple_tables_independent(self) -> None:
        """Operations on one table should not affect another."""
        db = InMemoryDB()
        db.create_table("a", {"id": int, "val": str}, primary_key="id")
        db.create_table("b", {"id": int, "val": str}, primary_key="id")

        db.insert("a", {"id": 1, "val": "a1"})
        db.insert("b", {"id": 1, "val": "b1"})

        db.update("a", values={"val": "a1_updated"})
        assert db.select("b")[0]["val"] == "b1"

        db.delete("a")
        assert len(db.select("b")) == 1

    def test_insert_after_update_all(self) -> None:
        """Insert after bulk update should work correctly."""
        db = InMemoryDB()
        db.create_table("t", {"id": int, "name": str}, primary_key="id")
        db.insert("t", {"id": 1, "name": "Alice"})
        db.update("t", values={"name": "Updated"})
        db.insert("t", {"id": 2, "name": "Bob"})
        rows = db.select("t")
        assert len(rows) == 2
        assert rows[0]["name"] == "Updated"
        assert rows[1]["name"] == "Bob"

    def test_repeated_delete_and_insert(self) -> None:
        """Stress test: repeated delete-all and re-insert cycles."""
        db = InMemoryDB()
        db.create_table("t", {"id": int}, primary_key="id")
        for cycle in range(5):
            for i in range(3):
                db.insert("t", {"id": i + cycle * 3})
            db.delete("t")
            assert db.select("t") == []

    def test_select_after_partial_delete(self) -> None:
        """Verify select works correctly after partial deletes."""
        db = InMemoryDB()
        db.create_table("t", {"id": int, "group": str}, primary_key="id")
        db.insert("t", {"id": 1, "group": "A"})
        db.insert("t", {"id": 2, "group": "B"})
        db.insert("t", {"id": 3, "group": "A"})
        db.insert("t", {"id": 4, "group": "B"})
        db.delete("t", where={"group": "A"})
        rows = db.select("t")
        assert len(rows) == 2
        assert all(r["group"] == "B" for r in rows)
