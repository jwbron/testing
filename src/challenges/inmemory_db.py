"""Simple in-memory database.

A single-module Python implementation supporting: create/drop tables with typed
columns, insert/select/update/delete rows, filtering (where clauses), and
primary key constraint enforcement.
"""

from __future__ import annotations

from typing import Any, Callable


# Supported column types and their Python type validators
COLUMN_TYPES: dict[str, type] = {
    "int": int,
    "float": float,
    "str": str,
    "bool": bool,
}


class DatabaseError(Exception):
    """Base exception for database operations."""


class TableExistsError(DatabaseError):
    """Raised when creating a table that already exists."""


class TableNotFoundError(DatabaseError):
    """Raised when referencing a table that does not exist."""


class ColumnError(DatabaseError):
    """Raised for column-related errors (missing, unknown, type mismatch)."""


class PrimaryKeyError(DatabaseError):
    """Raised when a primary key constraint is violated."""


class Table:
    """Represents a single database table with typed columns and rows."""

    def __init__(
        self,
        name: str,
        columns: dict[str, str],
        primary_key: str | None = None,
    ) -> None:
        """Initialize a table.

        Args:
            name: Table name.
            columns: Mapping of column name to type name (e.g. {"id": "int"}).
            primary_key: Optional column name to enforce as a unique primary key.

        Raises:
            ColumnError: If any column type is unsupported or primary key is
                not in the column list.
        """
        for col, col_type in columns.items():
            if col_type not in COLUMN_TYPES:
                raise ColumnError(
                    f"Unsupported column type '{col_type}' for column '{col}'. "
                    f"Supported types: {', '.join(sorted(COLUMN_TYPES))}"
                )

        if primary_key is not None and primary_key not in columns:
            raise ColumnError(
                f"Primary key column '{primary_key}' not in column list"
            )

        self.name = name
        self.columns = dict(columns)
        self.primary_key = primary_key
        self.rows: list[dict[str, Any]] = []
        self._pk_index: set[Any] = set()

    def _validate_row(self, row: dict[str, Any]) -> dict[str, Any]:
        """Validate and return a row dict with all columns present.

        Missing columns are filled with ``None``. Extra columns raise an error.
        Type mismatches raise an error.
        """
        extra = set(row) - set(self.columns)
        if extra:
            raise ColumnError(f"Unknown columns: {', '.join(sorted(extra))}")

        validated: dict[str, Any] = {}
        for col, col_type_name in self.columns.items():
            value = row.get(col)
            if value is not None:
                expected_type = COLUMN_TYPES[col_type_name]
                # Allow int values for float columns
                if col_type_name == "float" and isinstance(value, int):
                    value = float(value)
                if not isinstance(value, expected_type):
                    raise ColumnError(
                        f"Column '{col}' expects type '{col_type_name}', "
                        f"got {type(value).__name__}"
                    )
            validated[col] = value
        return validated

    def insert(self, row: dict[str, Any]) -> None:
        """Insert a row into the table.

        Args:
            row: Mapping of column names to values.

        Raises:
            ColumnError: If the row has unknown columns or type mismatches.
            PrimaryKeyError: If the primary key value is duplicate or missing.
        """
        validated = self._validate_row(row)

        if self.primary_key is not None:
            pk_val = validated.get(self.primary_key)
            if pk_val is None:
                raise PrimaryKeyError(
                    f"Primary key column '{self.primary_key}' must not be null"
                )
            if pk_val in self._pk_index:
                raise PrimaryKeyError(
                    f"Duplicate primary key value: {pk_val!r}"
                )
            self._pk_index.add(pk_val)

        self.rows.append(validated)

    def select(
        self,
        columns: list[str] | None = None,
        where: Callable[[dict[str, Any]], bool] | None = None,
    ) -> list[dict[str, Any]]:
        """Select rows from the table.

        Args:
            columns: List of column names to return. ``None`` returns all.
            where: Optional predicate function to filter rows.

        Returns:
            List of row dicts matching the filter, projected to the requested
            columns.

        Raises:
            ColumnError: If a requested column does not exist.
        """
        if columns is not None:
            unknown = set(columns) - set(self.columns)
            if unknown:
                raise ColumnError(
                    f"Unknown columns: {', '.join(sorted(unknown))}"
                )

        matching = self.rows if where is None else [r for r in self.rows if where(r)]

        if columns is None:
            return [dict(r) for r in matching]
        return [{c: r[c] for c in columns} for r in matching]

    def update(
        self,
        values: dict[str, Any],
        where: Callable[[dict[str, Any]], bool] | None = None,
    ) -> int:
        """Update rows matching the filter.

        Args:
            values: Column-value pairs to set.
            where: Optional predicate. If ``None``, all rows are updated.

        Returns:
            Number of rows updated.

        Raises:
            ColumnError: If values reference unknown columns or have type
                mismatches.
            PrimaryKeyError: If the update would create duplicate primary keys.
        """
        # Validate types of incoming values
        for col, val in values.items():
            if col not in self.columns:
                raise ColumnError(f"Unknown column: '{col}'")
            if val is not None:
                expected_type = COLUMN_TYPES[self.columns[col]]
                check_val = val
                if self.columns[col] == "float" and isinstance(val, int):
                    check_val = float(val)
                    values[col] = check_val
                if not isinstance(check_val, expected_type):
                    raise ColumnError(
                        f"Column '{col}' expects type '{self.columns[col]}', "
                        f"got {type(val).__name__}"
                    )

        # If updating the primary key, validate no duplicates
        if self.primary_key and self.primary_key in values:
            new_pk = values[self.primary_key]
            targets = [r for r in self.rows if (where is None or where(r))]
            # All targeted rows will get the same pk — only valid if 0 or 1 row
            if len(targets) > 1:
                raise PrimaryKeyError(
                    "Cannot set the same primary key value on multiple rows"
                )
            if targets and new_pk != targets[0].get(self.primary_key):
                if new_pk in self._pk_index:
                    raise PrimaryKeyError(
                        f"Duplicate primary key value: {new_pk!r}"
                    )

        count = 0
        for row in self.rows:
            if where is not None and not where(row):
                continue
            # Update pk index if needed
            if self.primary_key and self.primary_key in values:
                old_pk = row[self.primary_key]
                new_pk = values[self.primary_key]
                if old_pk != new_pk:
                    self._pk_index.discard(old_pk)
                    self._pk_index.add(new_pk)
            row.update(values)
            count += 1

        return count

    def delete(
        self,
        where: Callable[[dict[str, Any]], bool] | None = None,
    ) -> int:
        """Delete rows matching the filter.

        Args:
            where: Optional predicate. If ``None``, all rows are deleted.

        Returns:
            Number of rows deleted.
        """
        if where is None:
            count = len(self.rows)
            self.rows.clear()
            self._pk_index.clear()
            return count

        original_len = len(self.rows)
        kept: list[dict[str, Any]] = []
        for row in self.rows:
            if where(row):
                if self.primary_key:
                    self._pk_index.discard(row.get(self.primary_key))
            else:
                kept.append(row)
        self.rows = kept
        return original_len - len(kept)


class InMemoryDB:
    """Simple in-memory relational database.

    Supports creating and dropping tables with typed columns,
    CRUD operations, where-clause filtering, and primary key enforcement.

    Examples:
        >>> db = InMemoryDB()
        >>> db.create_table("users", {"id": "int", "name": "str"}, primary_key="id")
        >>> db.insert("users", {"id": 1, "name": "Alice"})
        >>> db.select("users", where=lambda r: r["name"] == "Alice")
        [{'id': 1, 'name': 'Alice'}]
    """

    def __init__(self) -> None:
        self._tables: dict[str, Table] = {}

    def create_table(
        self,
        name: str,
        columns: dict[str, str],
        primary_key: str | None = None,
    ) -> None:
        """Create a new table.

        Args:
            name: Table name.
            columns: Mapping of column name to type name.
            primary_key: Optional primary key column.

        Raises:
            TableExistsError: If a table with this name already exists.
        """
        if name in self._tables:
            raise TableExistsError(f"Table '{name}' already exists")
        self._tables[name] = Table(name, columns, primary_key)

    def drop_table(self, name: str) -> None:
        """Drop an existing table.

        Args:
            name: Table name.

        Raises:
            TableNotFoundError: If the table does not exist.
        """
        if name not in self._tables:
            raise TableNotFoundError(f"Table '{name}' does not exist")
        del self._tables[name]

    def _get_table(self, name: str) -> Table:
        """Get a table by name, raising if it does not exist."""
        if name not in self._tables:
            raise TableNotFoundError(f"Table '{name}' does not exist")
        return self._tables[name]

    def insert(self, table: str, row: dict[str, Any]) -> None:
        """Insert a row into a table.

        Args:
            table: Table name.
            row: Column-value mapping.
        """
        self._get_table(table).insert(row)

    def select(
        self,
        table: str,
        columns: list[str] | None = None,
        where: Callable[[dict[str, Any]], bool] | None = None,
    ) -> list[dict[str, Any]]:
        """Select rows from a table.

        Args:
            table: Table name.
            columns: Columns to project (None = all).
            where: Optional filter predicate.

        Returns:
            List of matching row dicts.
        """
        return self._get_table(table).select(columns, where)

    def update(
        self,
        table: str,
        values: dict[str, Any],
        where: Callable[[dict[str, Any]], bool] | None = None,
    ) -> int:
        """Update rows in a table.

        Args:
            table: Table name.
            values: Column-value pairs to set.
            where: Optional filter predicate.

        Returns:
            Number of rows updated.
        """
        return self._get_table(table).update(values, where)

    def delete(
        self,
        table: str,
        where: Callable[[dict[str, Any]], bool] | None = None,
    ) -> int:
        """Delete rows from a table.

        Args:
            table: Table name.
            where: Optional filter predicate.

        Returns:
            Number of rows deleted.
        """
        return self._get_table(table).delete(where)
