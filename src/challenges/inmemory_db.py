"""Simple in-memory database implementation.

Provides an InMemoryDB class supporting table creation with typed columns,
primary key constraints, and CRUD operations with filtering.
"""

from typing import Any, Callable


class DatabaseError(Exception):
    """Base exception for database errors."""


class TableExistsError(DatabaseError):
    """Raised when creating a table that already exists."""


class TableNotFoundError(DatabaseError):
    """Raised when referencing a table that does not exist."""


class PrimaryKeyViolationError(DatabaseError):
    """Raised when inserting a row with a duplicate primary key."""


class TypeValidationError(DatabaseError):
    """Raised when a column value does not match the expected type."""


class ColumnError(DatabaseError):
    """Raised for missing or unknown columns in a row."""


class InMemoryDB:
    """A simple in-memory relational database.

    Supports creating/dropping tables with typed columns, primary key
    constraints, and basic CRUD operations with optional filtering.

    Examples:
        >>> db = InMemoryDB()
        >>> db.create_table("users", {"id": int, "name": str}, primary_key="id")
        >>> db.insert("users", {"id": 1, "name": "Alice"})
        >>> db.select("users")
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
        """Create a new table with the given schema.

        Args:
            name: Table name.
            columns: Mapping of column name to Python type.
            primary_key: Optional column name to use as primary key.

        Raises:
            TableExistsError: If a table with this name already exists.
            ColumnError: If the primary key column is not in the schema.
        """
        if name in self._tables:
            raise TableExistsError(f"Table '{name}' already exists")
        if primary_key is not None and primary_key not in columns:
            raise ColumnError(
                f"Primary key '{primary_key}' is not a defined column"
            )
        self._tables[name] = []
        self._schemas[name] = dict(columns)
        self._primary_keys[name] = primary_key

    def drop_table(self, name: str) -> None:
        """Drop a table.

        Args:
            name: Table name.

        Raises:
            TableNotFoundError: If the table does not exist.
        """
        if name not in self._tables:
            raise TableNotFoundError(f"Table '{name}' does not exist")
        del self._tables[name]
        del self._schemas[name]
        del self._primary_keys[name]

    def insert(self, table: str, row: dict[str, Any]) -> None:
        """Insert a row into a table.

        Args:
            table: Table name.
            row: Mapping of column name to value.

        Raises:
            TableNotFoundError: If the table does not exist.
            ColumnError: If the row has missing or extra columns.
            TypeValidationError: If a value does not match the column type.
            PrimaryKeyViolationError: If the primary key value already exists.
        """
        self._ensure_table(table)
        schema = self._schemas[table]

        missing = set(schema) - set(row)
        if missing:
            raise ColumnError(f"Missing columns: {sorted(missing)}")
        extra = set(row) - set(schema)
        if extra:
            raise ColumnError(f"Unknown columns: {sorted(extra)}")

        for col, expected_type in schema.items():
            value = row[col]
            if not isinstance(value, expected_type):
                raise TypeValidationError(
                    f"Column '{col}' expected {expected_type.__name__}, "
                    f"got {type(value).__name__}"
                )

        pk = self._primary_keys[table]
        if pk is not None:
            pk_value = row[pk]
            for existing in self._tables[table]:
                if existing[pk] == pk_value:
                    raise PrimaryKeyViolationError(
                        f"Duplicate primary key '{pk}' = {pk_value!r}"
                    )

        self._tables[table].append(dict(row))

    def select(
        self,
        table: str,
        where: Callable[[dict[str, Any]], bool] | None = None,
    ) -> list[dict[str, Any]]:
        """Select rows from a table, optionally filtered.

        Args:
            table: Table name.
            where: Optional predicate to filter rows.

        Returns:
            A list of matching rows (copies).

        Raises:
            TableNotFoundError: If the table does not exist.
        """
        self._ensure_table(table)
        rows = self._tables[table]
        if where is not None:
            rows = [r for r in rows if where(r)]
        return [dict(r) for r in rows]

    def update(
        self,
        table: str,
        values: dict[str, Any],
        where: Callable[[dict[str, Any]], bool] | None = None,
    ) -> int:
        """Update rows in a table.

        Args:
            table: Table name.
            values: Mapping of column name to new value.
            where: Optional predicate to select rows to update.

        Returns:
            The number of rows updated.

        Raises:
            TableNotFoundError: If the table does not exist.
            ColumnError: If values reference unknown columns.
            TypeValidationError: If a value does not match the column type.
            PrimaryKeyViolationError: If the update would create duplicate PKs.
        """
        self._ensure_table(table)
        schema = self._schemas[table]

        unknown = set(values) - set(schema)
        if unknown:
            raise ColumnError(f"Unknown columns: {sorted(unknown)}")

        for col, val in values.items():
            if not isinstance(val, schema[col]):
                raise TypeValidationError(
                    f"Column '{col}' expected {schema[col].__name__}, "
                    f"got {type(val).__name__}"
                )

        rows = self._tables[table]
        pk = self._primary_keys[table]
        count = 0

        for row in rows:
            if where is None or where(row):
                # Check PK uniqueness if updating the PK column
                if pk is not None and pk in values:
                    new_pk = values[pk]
                    for other in rows:
                        if other is not row and other[pk] == new_pk:
                            raise PrimaryKeyViolationError(
                                f"Duplicate primary key '{pk}' = {new_pk!r}"
                            )
                row.update(values)
                count += 1

        return count

    def delete(
        self,
        table: str,
        where: Callable[[dict[str, Any]], bool] | None = None,
    ) -> int:
        """Delete rows from a table.

        Args:
            table: Table name.
            where: Optional predicate to select rows to delete.

        Returns:
            The number of rows deleted.

        Raises:
            TableNotFoundError: If the table does not exist.
        """
        self._ensure_table(table)
        rows = self._tables[table]
        if where is None:
            count = len(rows)
            rows.clear()
            return count

        to_keep = [r for r in rows if not where(r)]
        count = len(rows) - len(to_keep)
        self._tables[table] = to_keep
        return count

    def _ensure_table(self, name: str) -> None:
        """Raise TableNotFoundError if the table does not exist."""
        if name not in self._tables:
            raise TableNotFoundError(f"Table '{name}' does not exist")
