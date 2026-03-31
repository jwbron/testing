"""Gap-filling tests for InMemoryDB.

Covers edge cases, boundary conditions, and uncovered branches not
addressed by the primary test suite: filter edge cases, atomicity
concerns, bool/int subclass gotcha, empty-name tables, and more.
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
    _build_filter,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def db() -> InMemoryDB:
    return InMemoryDB()


@pytest.fixture
def users_db(db: InMemoryDB) -> InMemoryDB:
    db.create_table("users", {"id": int, "name": str, "age": int}, primary_key="id")
    return db


# ---------------------------------------------------------------------------
# _build_filter edge cases
# ---------------------------------------------------------------------------


class TestBuildFilter:
    """Tests for the internal _build_filter helper."""

    def test_none_matches_all(self) -> None:
        pred = _build_filter(None)
        assert pred({"a": 1}) is True

    def test_dict_filter(self) -> None:
        pred = _build_filter({"x": 1})
        assert pred({"x": 1, "y": 2}) is True
        assert pred({"x": 2, "y": 2}) is False

    def test_callable_filter(self) -> None:
        pred = _build_filter(lambda r: r["x"] > 0)
        assert pred({"x": 5}) is True
        assert pred({"x": -1}) is False

    def test_invalid_where_type_raises(self) -> None:
        with pytest.raises(DatabaseError):
            _build_filter(42)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Exception hierarchy
# ---------------------------------------------------------------------------


class TestExceptionHierarchy:
    """Verify custom exceptions inherit from DatabaseError."""

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
# Bool/int subclass gotcha
# ---------------------------------------------------------------------------


class TestBoolIntSubclass:
    """Python's bool is a subclass of int.

    isinstance(True, int) is True, so a column typed as int may accept
    bools. These tests document the actual behaviour.
    """

    def test_bool_passes_int_check(self, db: InMemoryDB) -> None:
        """If the implementation uses isinstance, True will pass an int column."""
        db.create_table("t", {"id": int, "flag": int}, primary_key="id")
        # This is a known Python quirk — we document current behaviour.
        # If the impl rejects bools for int columns, that's also valid.
        try:
            db.insert("t", {"id": 1, "flag": True})
            rows = db.select("t")
            assert rows[0]["flag"] is True
        except TypeValidationError:
            pass  # stricter implementation


# ---------------------------------------------------------------------------
# Insert edge cases
# ---------------------------------------------------------------------------


class TestInsertEdgeCases:
    def test_insert_after_failed_insert_is_clean(self, users_db: InMemoryDB) -> None:
        """A failed insert (PK violation) should not leave partial state."""
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        with pytest.raises(PrimaryKeyViolationError):
            users_db.insert("users", {"id": 1, "name": "Dup", "age": 0})
        # Original row unchanged
        rows = users_db.select("users")
        assert len(rows) == 1
        assert rows[0]["name"] == "Alice"

    def test_insert_empty_string_name(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "", "age": 0})
        assert users_db.select("users")[0]["name"] == ""

    def test_insert_negative_pk(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": -1, "name": "Neg", "age": 0})
        rows = users_db.select("users", where={"id": -1})
        assert len(rows) == 1

    def test_insert_zero_pk(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 0, "name": "Zero", "age": 0})
        rows = users_db.select("users", where={"id": 0})
        assert len(rows) == 1


# ---------------------------------------------------------------------------
# Select edge cases
# ---------------------------------------------------------------------------


class TestSelectEdgeCases:
    def test_select_empty_dict_where_returns_all(self, users_db: InMemoryDB) -> None:
        """An empty dict {} should match every row (all conditions vacuously true)."""
        users_db.insert("users", {"id": 1, "name": "A", "age": 1})
        users_db.insert("users", {"id": 2, "name": "B", "age": 2})
        rows = users_db.select("users", where={})
        assert len(rows) == 2

    def test_select_filter_on_nonexistent_column(self, users_db: InMemoryDB) -> None:
        """Filtering on a column that doesn't exist should return no matches
        (the row won't have the key, so .get returns None != value).
        """
        users_db.insert("users", {"id": 1, "name": "A", "age": 1})
        rows = users_db.select("users", where={"nonexistent": "val"})
        assert rows == []

    def test_select_callable_raising_exception(self, users_db: InMemoryDB) -> None:
        """A callable filter that raises should propagate."""
        users_db.insert("users", {"id": 1, "name": "A", "age": 1})
        with pytest.raises(ZeroDivisionError):
            users_db.select("users", where=lambda r: 1 / 0)  # noqa: ARG005


# ---------------------------------------------------------------------------
# Update edge cases
# ---------------------------------------------------------------------------


class TestUpdateEdgeCases:
    def test_update_pk_across_multiple_rows_raises(self, users_db: InMemoryDB) -> None:
        """Updating PK of multiple matching rows to the same new value should
        raise because the second row would duplicate the PK.
        """
        users_db.insert("users", {"id": 1, "name": "A", "age": 30})
        users_db.insert("users", {"id": 2, "name": "B", "age": 30})
        with pytest.raises(PrimaryKeyViolationError):
            users_db.update("users", {"id": 99}, where={"age": 30})

    def test_update_returns_zero_on_empty_table(self, users_db: InMemoryDB) -> None:
        count = users_db.update("users", {"age": 1})
        assert count == 0

    def test_update_with_callable_where(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "A", "age": 20})
        users_db.insert("users", {"id": 2, "name": "B", "age": 40})
        count = users_db.update("users", {"age": 99}, where=lambda r: r["age"] > 30)
        assert count == 1
        rows = users_db.select("users", where={"id": 2})
        assert rows[0]["age"] == 99

    def test_update_preserves_unmodified_fields(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        users_db.update("users", {"age": 31}, where={"id": 1})
        row = users_db.select("users", where={"id": 1})[0]
        assert row["name"] == "Alice"
        assert row["id"] == 1


# ---------------------------------------------------------------------------
# Delete edge cases
# ---------------------------------------------------------------------------


class TestDeleteEdgeCases:
    def test_delete_on_empty_table(self, users_db: InMemoryDB) -> None:
        count = users_db.delete("users")
        assert count == 0

    def test_delete_then_reinsert_same_pk(self, users_db: InMemoryDB) -> None:
        users_db.insert("users", {"id": 1, "name": "A", "age": 1})
        users_db.delete("users", where={"id": 1})
        # PK 1 should be freed
        users_db.insert("users", {"id": 1, "name": "B", "age": 2})
        rows = users_db.select("users")
        assert len(rows) == 1
        assert rows[0]["name"] == "B"

    def test_delete_partial_with_dict_where(self, users_db: InMemoryDB) -> None:
        for i in range(5):
            users_db.insert("users", {"id": i, "name": f"u{i}", "age": 20 + i})
        count = users_db.delete("users", where={"age": 22})
        assert count == 1
        assert len(users_db.select("users")) == 4


# ---------------------------------------------------------------------------
# Full CRUD lifecycle
# ---------------------------------------------------------------------------


class TestCRUDLifecycle:
    def test_full_lifecycle(self, db: InMemoryDB) -> None:
        """Create → Insert → Select → Update → Delete → Drop."""
        db.create_table("items", {"id": int, "label": str}, primary_key="id")
        db.insert("items", {"id": 1, "label": "widget"})
        assert db.select("items") == [{"id": 1, "label": "widget"}]

        db.update("items", {"label": "gadget"}, where={"id": 1})
        assert db.select("items")[0]["label"] == "gadget"

        db.delete("items", where={"id": 1})
        assert db.select("items") == []

        db.drop_table("items")
        with pytest.raises(TableNotFoundError):
            db.select("items")

    def test_large_table(self, users_db: InMemoryDB) -> None:
        """Insert, filter, and delete across 200 rows."""
        for i in range(200):
            users_db.insert(
                "users",
                {"id": i, "name": f"user_{i}", "age": i % 50},
            )
        assert len(users_db.select("users")) == 200
        # age==0 occurs for i=0,50,100,150 → 4 rows
        assert len(users_db.select("users", where={"age": 0})) == 4

        count = users_db.delete("users", where={"age": 0})
        assert count == 4
        assert len(users_db.select("users")) == 196


# ---------------------------------------------------------------------------
# Table creation edge cases
# ---------------------------------------------------------------------------


class TestCreateTableEdgeCases:
    def test_single_column_table(self, db: InMemoryDB) -> None:
        db.create_table("ids", {"id": int}, primary_key="id")
        db.insert("ids", {"id": 42})
        assert db.select("ids") == [{"id": 42}]

    def test_many_columns(self, db: InMemoryDB) -> None:
        cols = {f"col{i}": int for i in range(20)}
        cols["id"] = int
        db.create_table("wide", cols, primary_key="id")
        row = {f"col{i}": i for i in range(20)}
        row["id"] = 1
        db.insert("wide", row)
        assert len(db.select("wide")) == 1
