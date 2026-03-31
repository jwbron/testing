"""Simple in-memory database coding challenge.

A single-module Python implementation supporting: create/drop tables with typed
columns, insert/select/update/delete rows, filtering (where clauses), and
primary key constraint enforcement.
"""

from __future__ import annotations

from typing import Any, Callable


class DatabaseError(Exception):
    """Base exception for database operations."""


class TableExistsError(DatabaseError):
    """Raised when creating a table that already exists."""


class TableNotFoundError(DatabaseError):
    """Raised when referencing a table that does not exist."""


class PrimaryKeyViolationError(DatabaseError):
    """Raised when inserting a row with a duplicate primary key."""


class TypeValidationError(DatabaseError):
    """Raised when a column value does not match the declared type."""


class ColumnError(DatabaseError):
    """Raised for missing or unknown column issues."""


class _Table:
    """Internal representation of a database table."""

    def __init__(
        self,
        name: str,
        columns: dict[str, type],
        primary_key: str,
    ) -> None:
        if primary_key not in columns:
            raise ColumnError(
                f"Primary key '{primary_key}' is not a defined column"
            )
        self.name = name
        self.columns = columns
        self.primary_key = primary_key
        self.rows: list[dict[str, Any]] = []
        self._pk_index: set[Any] = set()

    def validate_row(self, row: dict[str, Any]) -> None:
        """Validate that a row matches the table schema."""
        for col_name, col_type in self.columns.items():
            if col_name not in row:
                raise ColumnError(
                    f"Missing required column '{col_name}' in table '{self.name}'"
                )
            if not isinstance(row[col_name], col_type):
                raise TypeValidationError(
                    f"Column '{col_name}' expects {col_type.__name__}, "
                    f"got {type(row[col_name]).__name__}"
                )
        for col_name in row:
            if col_name not in self.columns:
                raise ColumnError(
                    f"Unknown column '{col_name}' in table '{self.name}'"
                )


WhereClause = Callable[[dict[str, Any]], bool] | dict[str, Any] | None


def _build_filter(where: WhereClause) -> Callable[[dict[str, Any]], bool]:
    """Convert a where clause into a callable predicate.

    Accepts:
        - None  → matches all rows
        - dict  → matches rows where every key==value pair is satisfied
        - callable → used directly as the predicate
    """
    if where is None:
        return lambda _row: True
    if callable(where):
        return where
    if isinstance(where, dict):
        items = list(where.items())
        return lambda row: all(row.get(k) == v for k, v in items)
    raise DatabaseError(f"Invalid where clause: {where!r}")


class InMemoryDB:
    """A simple in-memory relational database.

    Supports creating and dropping tables with typed columns, inserting rows
    with primary key uniqueness enforcement, and selecting/updating/deleting
    rows with optional where-clause filtering.

    Examples:
        >>> db = InMemoryDB()
        >>> db.create_table("users", {"id": int, "name": str}, primary_key="id")
        >>> db.insert("users", {"id": 1, "name": "Alice"})
        >>> db.select("users")
        [{'id': 1, 'name': 'Alice'}]
    """

    def __init__(self) -> None:
        self._tables: dict[str, _Table] = {}

    def create_table(
        self,
        name: str,
        columns: dict[str, type],
        primary_key: str,
    ) -> None:
        """Create a new table with the given schema.

        Args:
            name: Table name.
            columns: Mapping of column name to Python type (e.g. ``int``, ``str``).
            primary_key: Name of the column to use as the primary key.

        Raises:
            TableExistsError: If a table with *name* already exists.
            ColumnError: If *primary_key* is not in *columns*.
        """
        if name in self._tables:
            raise TableExistsError(f"Table '{name}' already exists")
        self._tables[name] = _Table(name, columns, primary_key)

    def drop_table(self, name: str) -> None:
        """Drop an existing table.

        Args:
            name: Table name to drop.

        Raises:
            TableNotFoundError: If the table does not exist.
        """
        if name not in self._tables:
            raise TableNotFoundError(f"Table '{name}' does not exist")
        del self._tables[name]

    def _get_table(self, name: str) -> _Table:
        try:
            return self._tables[name]
        except KeyError:
            raise TableNotFoundError(f"Table '{name}' does not exist") from None

    def insert(self, table_name: str, row: dict[str, Any]) -> None:
        """Insert a row into a table.

        Args:
            table_name: Target table.
            row: Column-name → value mapping.

        Raises:
            TableNotFoundError: If the table does not exist.
            ColumnError: If the row has missing or unknown columns.
            TypeValidationError: If a value has the wrong type.
            PrimaryKeyViolationError: If the primary key value already exists.
        """
        table = self._get_table(table_name)
        table.validate_row(row)

        pk_value = row[table.primary_key]
        if pk_value in table._pk_index:
            raise PrimaryKeyViolationError(
                f"Duplicate primary key '{table.primary_key}' = {pk_value!r} "
                f"in table '{table_name}'"
            )
        table._pk_index.add(pk_value)
        table.rows.append(dict(row))  # store a copy

    def select(
        self,
        table_name: str,
        where: WhereClause = None,
    ) -> list[dict[str, Any]]:
        """Select rows from a table, optionally filtered.

        Args:
            table_name: Source table.
            where: Optional filter — a dict of {column: value} for equality
                matching, a callable predicate, or ``None`` for all rows.

        Returns:
            A list of row dicts (copies) matching the filter.

        Raises:
            TableNotFoundError: If the table does not exist.
        """
        table = self._get_table(table_name)
        predicate = _build_filter(where)
        return [dict(row) for row in table.rows if predicate(row)]

    def update(
        self,
        table_name: str,
        values: dict[str, Any],
        where: WhereClause = None,
    ) -> int:
        """Update rows matching the filter with new values.

        Args:
            table_name: Target table.
            values: Column-name → new-value mapping to apply.
            where: Optional filter (same semantics as :meth:`select`).

        Returns:
            The number of rows updated.

        Raises:
            TableNotFoundError: If the table does not exist.
            TypeValidationError: If a new value has the wrong type.
            ColumnError: If *values* references an unknown column.
            PrimaryKeyViolationError: If updating the primary key would cause
                a duplicate.
        """
        table = self._get_table(table_name)

        # Validate value types and column names up-front.
        for col, val in values.items():
            if col not in table.columns:
                raise ColumnError(
                    f"Unknown column '{col}' in table '{table_name}'"
                )
            if not isinstance(val, table.columns[col]):
                raise TypeValidationError(
                    f"Column '{col}' expects {table.columns[col].__name__}, "
                    f"got {type(val).__name__}"
                )

        predicate = _build_filter(where)
        updated = 0
        pk_col = table.primary_key
        updating_pk = pk_col in values

        for row in table.rows:
            if not predicate(row):
                continue

            if updating_pk:
                new_pk = values[pk_col]
                old_pk = row[pk_col]
                if new_pk != old_pk and new_pk in table._pk_index:
                    raise PrimaryKeyViolationError(
                        f"Duplicate primary key '{pk_col}' = {new_pk!r} "
                        f"in table '{table_name}'"
                    )
                table._pk_index.discard(old_pk)
                table._pk_index.add(new_pk)

            row.update(values)
            updated += 1

        return updated

    def delete(
        self,
        table_name: str,
        where: WhereClause = None,
    ) -> int:
        """Delete rows matching the filter.

        Args:
            table_name: Target table.
            where: Optional filter (same semantics as :meth:`select`).

        Returns:
            The number of rows deleted.

        Raises:
            TableNotFoundError: If the table does not exist.
        """
        table = self._get_table(table_name)
        predicate = _build_filter(where)

        remaining: list[dict[str, Any]] = []
        deleted = 0
        for row in table.rows:
            if predicate(row):
                table._pk_index.discard(row[table.primary_key])
                deleted += 1
            else:
                remaining.append(row)

        table.rows = remaining
        return deleted
