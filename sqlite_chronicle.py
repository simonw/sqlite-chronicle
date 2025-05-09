import dataclasses
import sqlite3
import textwrap
from typing import Generator, Optional, List


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


def enable_chronicle(conn: sqlite3.Connection, table_name: str) -> None:
    """
    Turn on chronicle tracking for `table_name`.

    - Creates _chronicle_<table> (PK cols + __added_ms, __updated_ms, __version, __deleted)
    - Populates that table with one row per existing row in the original
    - AFTER INSERT trigger
    - AFTER UPDATE trigger (WHEN any OLD<>NEW)
    - AFTER DELETE trigger

    Requires client code to use UPSERT (INSERT…ON CONFLICT DO UPDATE) for upserts rather
    than INSERT OR REPLACE, since INSERT OR REPLACE will be incorrectly tracked.
    """
    cursor = conn.cursor()

    # If chronicle table exists already, do nothing
    chronicle_table = f"_chronicle_{table_name}"
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (chronicle_table,),
    )
    if cursor.fetchone():
        return

    # Gather table schema info
    cursor.execute(f'PRAGMA table_info("{table_name}")')
    table_info = cursor.fetchall()

    # Error if no such table
    if not table_info:
        raise ChronicleError(f"Table {table_name!r} does not exist")

    # Identify primary key columns and non-PK columns
    primary_key_columns = [(row[1], row[2]) for row in table_info if row[5]]
    if not primary_key_columns:
        raise ChronicleError(f"{table_name!r} has no PRIMARY KEY")
    non_pk_columns = [row[1] for row in table_info if not row[5]]
    primary_key_names = [col for col, _ in primary_key_columns]

    # SQL expressions for timestamps and versioning
    current_timestamp_expr = (
        "CAST((julianday('now') - 2440587.5) * 86400 * 1000 AS INTEGER)"
    )
    next_version_expr = (
        f'COALESCE((SELECT MAX(__version) FROM "{chronicle_table}"), 0) + 1'
    )

    # Build trigger WHEN condition: any non-PK column changed
    if non_pk_columns:
        update_condition = " OR ".join(
            f'OLD."{col}" IS NOT NEW."{col}"' for col in non_pk_columns
        )
    else:
        # no non-PK columns → treat any update as a change
        update_condition = "1"

    # Build PK matching clause for WHERE conditions
    primary_key_match_clause = " AND ".join(
        f'"{col}" = NEW."{col}"' for col in primary_key_names
    )

    # Collect all SQL statements to execute
    sql_statements: List[str] = []

    # 1) Create chronicle table
    pk_definitions = ", ".join(
        f'"{col}" {col_type}' for col, col_type in primary_key_columns
    )
    pk_constraint = ", ".join(f'"{col}"' for col in primary_key_names)
    sql_statements.append(
        textwrap.dedent(
            f"""
            CREATE TABLE "{chronicle_table}" (
              {pk_definitions},
              __added_ms   INTEGER,
              __updated_ms INTEGER,
              __version    INTEGER,
              __deleted    INTEGER DEFAULT 0,
              PRIMARY KEY({pk_constraint})
            )
        """
        ).strip()
    )
    sql_statements.append(
        textwrap.dedent(
            f"""
            CREATE INDEX "{chronicle_table}__version_idx"
              ON "{chronicle_table}"(__version);
        """
        ).strip()
    )

    # 2) Seed chronicle table with existing rows
    version_expr = (
        f"ROW_NUMBER() OVER (ORDER BY "
        + ", ".join(f'"{col}"' for col in primary_key_names)
        + ")"
    )

    cols_insert = (
        ", ".join(f'"{col}"' for col in primary_key_names)
        + ", __added_ms, __updated_ms, __version, __deleted"
    )
    cols_select = (
        ", ".join(f'"{col}"' for col in primary_key_names)
        + f", {current_timestamp_expr} AS __added_ms"
        + f", {current_timestamp_expr} AS __updated_ms"
        + f", {version_expr} AS __version"
        + ", 0 AS __deleted"
    )

    sql_statements.append(
        f'INSERT INTO "{chronicle_table}" ({cols_insert})\n'
        f" SELECT {cols_select}\n"
        f'   FROM "{table_name}";'
    )

    # 3) AFTER INSERT trigger
    sql_statements.append(
        textwrap.dedent(
            f"""
            CREATE TRIGGER "chronicle_{table_name}_ai"
            AFTER INSERT ON "{table_name}"
            FOR EACH ROW
            BEGIN
              INSERT OR IGNORE INTO "{chronicle_table}" (
                {', '.join(f'"{col}"' for col in primary_key_names)},
                __added_ms, __updated_ms, __version, __deleted
              )
              VALUES (
                {', '.join(f'NEW."{col}"' for col in primary_key_names)},
                {current_timestamp_expr}, {current_timestamp_expr},
                {next_version_expr}, 0
              );
            END;
        """
        ).strip()
    )

    # 4) AFTER UPDATE trigger (only if real change)
    sql_statements.append(
        textwrap.dedent(
            f"""
            CREATE TRIGGER "chronicle_{table_name}_au"
            AFTER UPDATE ON "{table_name}"
            FOR EACH ROW
            WHEN {update_condition}
            BEGIN
              UPDATE "{chronicle_table}"
                SET __updated_ms = {current_timestamp_expr},
                    __version    = {next_version_expr}
              WHERE {primary_key_match_clause};
            END;
        """
        ).strip()
    )

    # 5) AFTER DELETE trigger
    pk_old_match = " AND ".join(f'"{col}" = OLD."{col}"' for col in primary_key_names)
    sql_statements.append(
        textwrap.dedent(
            f"""
            CREATE TRIGGER "chronicle_{table_name}_ad"
            AFTER DELETE ON "{table_name}"
            FOR EACH ROW
            BEGIN
              UPDATE "{chronicle_table}"
                SET __updated_ms = {current_timestamp_expr},
                    __version    = {next_version_expr},
                    __deleted    = 1
              WHERE {pk_old_match};
            END;
        """
        ).strip()
    )

    # Execute all statements within a transaction
    with conn:
        for stmt in sql_statements:
            cursor.execute(stmt)


def updates_since(
    conn: sqlite3.Connection,
    table_name: str,
    since: Optional[int] = None,
    batch_size: int = 1000,
) -> Generator[Change, None, None]:
    """
    Yields Change(pks, added_ms, updated_ms, version, row, deleted)
    for every chronicle.version > since, in ascending order.
    """
    cur = conn.cursor()
    cur.row_factory = sqlite3.Row
    if since is None:
        since = 0

    # find PK columns
    cur.execute(f'PRAGMA table_info("{table_name}")')
    cols = cur.fetchall()
    pk_names = [c["name"] for c in cols if c["pk"]]
    non_pk = [c["name"] for c in cols if not c["pk"]]

    # build select
    chron = f"_chronicle_{table_name}"
    pks = ", ".join(f'c."{c}"' for c in pk_names)
    originals = ", ".join(f't."{c}"' for c in non_pk)
    select = ", ".join(
        [pks, originals, "c.__added_ms", "c.__updated_ms", "c.__version", "c.__deleted"]
    )
    join = " AND ".join(f'c."{c}" = t."{c}"' for c in pk_names)

    sql = textwrap.dedent(
        f"""
        SELECT {select}
          FROM {chron} AS c
          LEFT JOIN "{table_name}" AS t
            ON {join}
         WHERE c.__version > ?
         ORDER BY c.__version
         LIMIT {batch_size}
        """
    ).strip()

    while True:
        rows = cur.execute(sql, (since,)).fetchall()
        if not rows:
            break
        for r in rows:
            since = r["__version"]
            # build row dict of original columns
            row = {
                c: r[c]
                for c in r.keys()
                if c not in ("__added_ms", "__updated_ms", "__version", "__deleted")
            }
            yield Change(
                pks=tuple(r[c] for c in pk_names),
                added_ms=r["__added_ms"],
                updated_ms=r["__updated_ms"],
                version=r["__version"],
                row=row,
                deleted=bool(r["__deleted"]),
            )


def cli_main(argv=None) -> int:
    import argparse
    import sys

    parser = argparse.ArgumentParser(
        prog="python -m sqlite_chronicle",
        description="Enable chronicle tracking on one or more tables in an SQLite DB.",
    )
    parser.add_argument("db_path", help="Path to the SQLite database file")
    parser.add_argument(
        "tables",
        nargs="+",
        help="One or more table names to enable chronicle tracking on",
    )

    args = parser.parse_args(argv)

    try:
        conn = sqlite3.connect(args.db_path)
    except sqlite3.Error as e:
        print(f"ERROR: cannot open database {args.db_path!r}: {e}", file=sys.stderr)
        return 1

    any_error = False
    for tbl in args.tables:
        try:
            enable_chronicle(conn, tbl)
            print(f"- chronicle enabled on table {tbl!r}")
        except ChronicleError as ce:
            print(f"ERROR: {ce}", file=sys.stderr)
            any_error = True
        except sqlite3.Error as se:
            print(f"SQL ERROR on table {tbl!r}: {se}", file=sys.stderr)
            any_error = True

    conn.close()
    return 1 if any_error else 0


if __name__ == "__main__":
    import sys

    sys.exit(cli_main())
