import dataclasses
import sqlite3
import textwrap
from typing import Generator, Optional


class ChronicleError(Exception):
    pass


@dataclasses.dataclass
class Change:
    pks: tuple
    added_ms: int
    updated_ms: int
    version: int
    row: dict
    deleted: bool


def enable_chronicle(conn: sqlite3.Connection, table_name: str):
    c = conn.cursor()

    # Check if the _chronicle_ table exists
    c.execute(
        f"SELECT name FROM sqlite_master WHERE type='table' AND name='_chronicle_{table_name}';"
    )
    if c.fetchone():
        return

    # Determine primary key columns and their types
    c.execute(f'PRAGMA table_info("{table_name}");')
    primary_key_columns = [(row[1], row[2]) for row in c.fetchall() if row[5]]
    if not primary_key_columns:
        raise ChronicleError(f"Table {table_name} has no primary keys")

    # Create the _chronicle_ table
    pk_def = ", ".join(
        [f'"{col_name}" {col_type}' for col_name, col_type in primary_key_columns]
    )

    current_time_expr = "CAST((julianday('now') - 2440587.5) * 86400 * 1000 AS INTEGER)"
    next_version_expr = (
        f'COALESCE((SELECT MAX(version) FROM "_chronicle_{table_name}"), 0) + 1'
    )

    with conn:
        c.execute(
            f"""
            CREATE TABLE "_chronicle_{table_name}" (
                {pk_def},
                added_ms INTEGER,
                updated_ms INTEGER,
                version INTEGER DEFAULT 0,
                deleted INTEGER DEFAULT 0,
                PRIMARY KEY ({', '.join([f'"{col[0]}"' for col in primary_key_columns])})
            );
            """
        )

        # Index on version column
        c.execute(
            f"CREATE INDEX '_chronicle_{table_name}_version' ON '_chronicle_{table_name}' (version);"
        )

        # Populate the _chronicle_ table with existing rows from the original table
        pks = ", ".join([f'"{col[0]}"' for col in primary_key_columns])
        c.execute(
            f"""
            INSERT INTO "_chronicle_{table_name}" (
                {', '.join([col[0] for col in primary_key_columns])},
                added_ms,
                updated_ms,
                version
            )
            SELECT
                {pks},
                {current_time_expr},
                {current_time_expr},
                ROW_NUMBER() OVER (ORDER BY {pks})
            FROM "{table_name}";
            """
        )

        # Create the after insert trigger
        after_insert_sql = textwrap.dedent(
            f"""
        CREATE TRIGGER "_chronicle_{table_name}_ai"
        AFTER INSERT ON "{table_name}"
        FOR EACH ROW
        BEGIN
            INSERT INTO "_chronicle_{table_name}" ({', '.join([f'"{col[0]}"' for col in primary_key_columns])}, added_ms, updated_ms, version)
            VALUES ({', '.join(['NEW.' + f'"{col[0]}"' for col in primary_key_columns])}, {current_time_expr}, {current_time_expr}, {next_version_expr});
        END;
        """
        )
        c.execute(after_insert_sql)

        # Create the after update trigger
        c.execute(
            f"""
            CREATE TRIGGER "_chronicle_{table_name}_au"
            AFTER UPDATE ON "{table_name}"
            FOR EACH ROW
            BEGIN
                UPDATE "_chronicle_{table_name}"
                SET updated_ms = {current_time_expr},
                    version = {next_version_expr},
                    {', '.join([f'"{col[0]}" = NEW."{col[0]}"' for col in primary_key_columns])}
                WHERE { ' AND '.join([f'"{col[0]}" = OLD."{col[0]}"' for col in primary_key_columns]) };
            END;
            """
        )

        # Create the after delete trigger
        c.execute(
            f"""
            CREATE TRIGGER "_chronicle_{table_name}_ad"
            AFTER DELETE ON "{table_name}"
            FOR EACH ROW
            BEGIN
                UPDATE "_chronicle_{table_name}"
                SET updated_ms = {current_time_expr},
                    version = {next_version_expr},
                    deleted = 1
                WHERE { ' AND '.join([f'"{col[0]}" = OLD."{col[0]}"' for col in primary_key_columns]) };
            END;
            """
        )


def updates_since(
    conn: sqlite3.Connection,
    table_name: str,
    since: Optional[int] = None,
    batch_size: int = 1000,
) -> Generator[Change, None, None]:
    cursor = conn.cursor()
    cursor.row_factory = sqlite3.Row

    if since is None:
        since = 0

    # Find primary keys
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = cursor.fetchall()
    primary_keys = [col["name"] for col in columns if col["pk"]]

    # Create the join_on clause based on primary keys
    join_conditions = [f'chronicle."{pk}" = t."{pk}"' for pk in primary_keys]
    join_on = " AND ".join(join_conditions)

    # Select clause is primary keys from chronicle table, then columns from original table
    select_clause = ", ".join(
        [f"chronicle.{pk}" for pk in primary_keys]
        + [f"t.{col['name']}" for col in columns if col not in primary_keys]
        + [
            "chronicle.added_ms as __chronicle_added_ms",
            "chronicle.updated_ms as __chronicle_updated_ms",
            "chronicle.version as __chronicle_version",
            "CASE WHEN t.id IS NULL THEN 1 ELSE 0 END AS __chronicle_deleted",
        ]
    )

    # Paginate through ordered by version batch_size at a time
    while True:
        sql = textwrap.dedent(
            f"""
            SELECT {select_clause}
            FROM "_chronicle_{table_name}" chronicle
            LEFT JOIN "{table_name}" t ON {join_on}
            WHERE chronicle.version > ?
            ORDER BY chronicle.version
            LIMIT {batch_size}
            """
        )
        rows = cursor.execute(sql, (since,)).fetchall()

        if not rows:
            break

        for row in rows:
            # Need row without __chronicle_ columns
            row_without = dict(
                (k, row[k]) for k in row.keys() if not k.startswith("__chronicle_")
            )
            added_ms = row["__chronicle_added_ms"]
            updated_ms = row["__chronicle_updated_ms"]
            since = row["__chronicle_version"]
            yield Change(
                pks=tuple(row[pk] for pk in primary_keys),
                added_ms=added_ms,
                updated_ms=updated_ms,
                version=since,
                row=row_without,
                deleted=row["__chronicle_deleted"],
            )
