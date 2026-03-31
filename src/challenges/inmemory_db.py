"""In-Memory Database coding challenge.

Implement a simple in-memory relational database supporting table creation,
insertion, selection, update, and deletion with type validation and
primary key enforcement.
"""


class InMemoryDB:
    """A simple in-memory relational database.

    Stores tables as dictionaries, each containing column definitions,
    a primary key designation, and a list of row dictionaries.

    Examples:
        >>> db = InMemoryDB()
        >>> db.create_table("users", {"id": int, "name": str}, primary_key="id")
        >>> db.insert("users", {"id": 1, "name": "Alice"})
        >>> db.select("users")
        [{'id': 1, 'name': 'Alice'}]
    """

    def __init__(self) -> None:
        """Initialize an empty database."""
        self._tables: dict[str, dict] = {}

    def create_table(self, name: str, columns: dict[str, type], primary_key: str) -> None:
        """Create a new table with the given schema.

        Args:
            name: The table name.
            columns: A mapping of column names to their expected types.
            primary_key: The column to use as the primary key.

        Raises:
            ValueError: If the table already exists or primary_key is not in columns.
        """
        if name in self._tables:
            raise ValueError(f"Table '{name}' already exists")
        if primary_key not in columns:
            raise ValueError(
                f"Primary key '{primary_key}' is not in columns"
            )
        self._tables[name] = {
            "columns": dict(columns),
            "primary_key": primary_key,
            "rows": [],
        }

    def drop_table(self, name: str) -> None:
        """Drop an existing table.

        Args:
            name: The table name to drop.

        Raises:
            ValueError: If the table does not exist.
        """
        if name not in self._tables:
            raise ValueError(f"Table '{name}' does not exist")
        del self._tables[name]

    def _get_table(self, name: str) -> dict:
        """Return the table dict, raising ValueError if it does not exist."""
        if name not in self._tables:
            raise ValueError(f"Table '{name}' does not exist")
        return self._tables[name]

    def _validate_types(self, columns: dict[str, type], data: dict) -> None:
        """Validate that values in data match the expected column types.

        Args:
            columns: The column schema mapping names to types.
            data: The data to validate.

        Raises:
            TypeError: If any value does not match the expected column type.
        """
        for col, value in data.items():
            if col in columns and not isinstance(value, columns[col]):
                raise TypeError(
                    f"Column '{col}' expects {columns[col].__name__}, "
                    f"got {type(value).__name__}"
                )

    def insert(self, table: str, row: dict) -> None:
        """Insert a row into a table.

        Args:
            table: The table name.
            row: A dictionary mapping column names to values.

        Raises:
            ValueError: If the table does not exist or the primary key
                value is a duplicate.
            TypeError: If any value has the wrong type for its column.
        """
        tbl = self._get_table(table)
        columns = tbl["columns"]
        pk = tbl["primary_key"]

        self._validate_types(columns, row)

        pk_value = row.get(pk)
        for existing in tbl["rows"]:
            if existing.get(pk) == pk_value:
                raise ValueError(
                    f"Duplicate primary key value: {pk_value!r}"
                )

        tbl["rows"].append(dict(row))

    def _matches(self, row: dict, where: dict) -> bool:
        """Return True if the row matches all conditions in where."""
        return all(row.get(k) == v for k, v in where.items())

    def select(self, table: str, where: dict | None = None) -> list[dict]:
        """Select rows from a table.

        Args:
            table: The table name.
            where: An optional dict of column-value pairs to filter by.
                If None, all rows are returned.

        Returns:
            A list of copies of the matching rows.
        """
        tbl = self._get_table(table)
        rows = tbl["rows"]
        if where is None:
            return [dict(r) for r in rows]
        return [dict(r) for r in rows if self._matches(r, where)]

    def update(self, table: str, values: dict, where: dict | None = None) -> int:
        """Update matching rows in a table.

        Args:
            table: The table name.
            values: A dict of column-value pairs to set on matching rows.
            where: An optional dict of column-value pairs to filter by.
                If None, all rows are updated.

        Returns:
            The number of rows updated.
        """
        tbl = self._get_table(table)
        self._validate_types(tbl["columns"], values)

        pk = tbl["primary_key"]
        count = 0
        for row in tbl["rows"]:
            if where is None or self._matches(row, where):
                # If updating the primary key, check uniqueness against
                # all rows that are NOT being updated.
                if pk in values:
                    new_pk = values[pk]
                    for other in tbl["rows"]:
                        if other is not row and other.get(pk) == new_pk:
                            raise ValueError(
                                f"Duplicate primary key value: {new_pk!r}"
                            )
                row.update(values)
                count += 1
        return count

    def delete(self, table: str, where: dict | None = None) -> int:
        """Delete matching rows from a table.

        Args:
            table: The table name.
            where: An optional dict of column-value pairs to filter by.
                If None, all rows are deleted.

        Returns:
            The number of rows deleted.
        """
        tbl = self._get_table(table)
        original_count = len(tbl["rows"])
        if where is None:
            tbl["rows"] = []
        else:
            tbl["rows"] = [r for r in tbl["rows"] if not self._matches(r, where)]
        return original_count - len(tbl["rows"])
