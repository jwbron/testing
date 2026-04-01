"""Simple in-memory database.

A single-module implementation supporting typed tables with primary keys,
and CRUD operations (insert, select, update, delete) with filtering.
"""

from typing import Any, Callable


class DatabaseError(Exception):
    """Base exception for database operations."""


class TableExistsError(DatabaseError):
    """Raised when creating a table that already exists."""


class TableNotFoundError(DatabaseError):
    """Raised when referencing a table that does not exist."""


class PrimaryKeyViolationError(DatabaseError):
    """Raised when inserting a duplicate primary key value."""


class TypeValidationError(DatabaseError):
    """Raised when a column value does not match its declared type."""


WhereClause = dict[str, Any] | Callable[[dict[str, Any]], bool] | None


class InMemoryDB:
    """A simple in-memory relational database.

    Supports creating/dropping tables with typed columns, primary key
    constraints, and basic CRUD operations with optional filtering.

    Examples:
        >>> db = InMemoryDB()
        >>> db.create_table("users", {"id": int, "name": str}, primary_key="id")
        >>> db.insert("users", {"id": 1, "name": "Alice"})
        >>> db.select("users", where={"name": "Alice"})
        [{'id': 1, 'name': 'Alice'}]
    """

    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {}
        self._schemas: dict[str, dict[str, type]] = {}
        self._primary_keys: dict[str, str | None] = {}

    def create_table(
        self,
        name: str,
        columns: dict[str, type],
        primary_key: str | None = None,
    ) -> None:
        """Create a new table with typed columns and an optional primary key.

        Args:
            name: Table name.
            columns: Mapping of column name to Python type (e.g. ``{"id": int}``).
            primary_key: Column name to use as the primary key, or ``None``.

        Raises:
            TableExistsError: If a table with this name already exists.
            ValueError: If the primary key column is not in the column definitions.
        """
        if name in self._tables:
            raise TableExistsError(f"Table '{name}' already exists")
        if primary_key is not None and primary_key not in columns:
            raise ValueError(
                f"Primary key '{primary_key}' is not a defined column"
            )
        self._tables[name] = []
        self._schemas[name] = dict(columns)
        self._primary_keys[name] = primary_key

    def drop_table(self, name: str) -> None:
        """Drop an existing table.

        Args:
            name: Table name to drop.

        Raises:
            TableNotFoundError: If the table does not exist.
        """
        self._ensure_table(name)
        del self._tables[name]
        del self._schemas[name]
        del self._primary_keys[name]

    def insert(self, table: str, row: dict[str, Any]) -> None:
        """Insert a row into a table.

        The row must contain exactly the columns defined in the schema, and
        each value must match the declared type.

        Args:
            table: Table name.
            row: Column-name-to-value mapping.

        Raises:
            TableNotFoundError: If the table does not exist.
            TypeValidationError: If a value does not match its column type.
            PrimaryKeyViolationError: If a duplicate primary key is inserted.
            ValueError: If the row contains unknown columns or is missing columns.
        """
        self._ensure_table(table)
        schema = self._schemas[table]

        # Validate columns match schema
        extra = set(row.keys()) - set(schema.keys())
        if extra:
            raise ValueError(f"Unknown columns: {extra}")
        missing = set(schema.keys()) - set(row.keys())
        if missing:
            raise ValueError(f"Missing columns: {missing}")

        # Validate types
        for col, expected_type in schema.items():
            value = row[col]
            if not isinstance(value, expected_type):
                raise TypeValidationError(
                    f"Column '{col}' expected {expected_type.__name__}, "
                    f"got {type(value).__name__}"
                )

        # Validate primary key uniqueness
        pk = self._primary_keys[table]
        if pk is not None:
            pk_value = row[pk]
            for existing in self._tables[table]:
                if existing[pk] == pk_value:
                    raise PrimaryKeyViolationError(
                        f"Duplicate primary key '{pk}'={pk_value!r}"
                    )

        self._tables[table].append(dict(row))

    def select(
        self,
        table: str,
        where: WhereClause = None,
    ) -> list[dict[str, Any]]:
        """Select rows from a table with optional filtering.

        Args:
            table: Table name.
            where: Optional filter — either a dict of column-value equality
                conditions, a callable predicate, or ``None`` for all rows.

        Returns:
            List of matching rows (copies).

        Raises:
            TableNotFoundError: If the table does not exist.
        """
        self._ensure_table(table)
        predicate = self._build_predicate(where)
        return [dict(row) for row in self._tables[table] if predicate(row)]

    def update(
        self,
        table: str,
        values: dict[str, Any],
        where: WhereClause = None,
    ) -> int:
        """Update rows matching a filter.

        Args:
            table: Table name.
            values: Column-value pairs to set on matching rows.
            where: Optional filter (same semantics as ``select``).

        Returns:
            Number of rows updated.

        Raises:
            TableNotFoundError: If the table does not exist.
            TypeValidationError: If a new value does not match its column type.
            ValueError: If values references unknown columns.
        """
        self._ensure_table(table)
        schema = self._schemas[table]

        # Validate update values
        extra = set(values.keys()) - set(schema.keys())
        if extra:
            raise ValueError(f"Unknown columns: {extra}")
        for col, val in values.items():
            if not isinstance(val, schema[col]):
                raise TypeValidationError(
                    f"Column '{col}' expected {schema[col].__name__}, "
                    f"got {type(val).__name__}"
                )

        predicate = self._build_predicate(where)
        count = 0
        for row in self._tables[table]:
            if predicate(row):
                row.update(values)
                count += 1
        return count

    def delete(
        self,
        table: str,
        where: WhereClause = None,
    ) -> int:
        """Delete rows matching a filter.

        Args:
            table: Table name.
            where: Optional filter (same semantics as ``select``).

        Returns:
            Number of rows deleted.

        Raises:
            TableNotFoundError: If the table does not exist.
        """
        self._ensure_table(table)
        predicate = self._build_predicate(where)
        original_len = len(self._tables[table])
        self._tables[table] = [
            row for row in self._tables[table] if not predicate(row)
        ]
        return original_len - len(self._tables[table])

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ensure_table(self, name: str) -> None:
        """Raise ``TableNotFoundError`` if the table does not exist."""
        if name not in self._tables:
            raise TableNotFoundError(f"Table '{name}' does not exist")

    @staticmethod
    def _build_predicate(
        where: WhereClause,
    ) -> Callable[[dict[str, Any]], bool]:
        """Convert a where clause to a row-predicate function."""
        if where is None:
            return lambda _row: True
        if callable(where):
            return where
        # dict of equality conditions
        conditions = where

        def _match(row: dict[str, Any]) -> bool:
            return all(row.get(k) == v for k, v in conditions.items())

        return _match
