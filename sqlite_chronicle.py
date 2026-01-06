import dataclasses
import sqlite3
import textwrap
from typing import Generator, Optional, List


class ChronicleError(Exception):
    pass


# SQL expression for current timestamp in milliseconds since Unix epoch
CURRENT_TIMESTAMP_MS_EXPR = (
    "CAST((julianday('now') - 2440587.5) * 86400 * 1000 AS INTEGER)"
)

# Chronicle table column names
ADDED_MS_COL = "__added_ms"
UPDATED_MS_COL = "__updated_ms"
VERSION_COL = "__version"
DELETED_COL = "__deleted"


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

    # SQL expression for next version number
    next_version_expr = (
        f'COALESCE((SELECT MAX({VERSION_COL}) FROM "{chronicle_table}"), 0) + 1'
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
              {ADDED_MS_COL} INTEGER,
              {UPDATED_MS_COL} INTEGER,
              {VERSION_COL} INTEGER,
              {DELETED_COL} INTEGER DEFAULT 0,
              PRIMARY KEY({pk_constraint})
            )
        """
        ).strip()
    )
    sql_statements.append(
        textwrap.dedent(
            f"""
            CREATE INDEX "{chronicle_table}__version_idx"
              ON "{chronicle_table}"({VERSION_COL});
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
        + f", {ADDED_MS_COL}, {UPDATED_MS_COL}, {VERSION_COL}, {DELETED_COL}"
    )
    cols_select = (
        ", ".join(f'"{col}"' for col in primary_key_names)
        + f", {CURRENT_TIMESTAMP_MS_EXPR} AS {ADDED_MS_COL}"
        + f", {CURRENT_TIMESTAMP_MS_EXPR} AS {UPDATED_MS_COL}"
        + f", {version_expr} AS {VERSION_COL}"
        + f", 0 AS {DELETED_COL}"
    )

    sql_statements.append(
        f'INSERT INTO "{chronicle_table}" ({cols_insert})\n'
        f" SELECT {cols_select}\n"
        f'   FROM "{table_name}";'
    )

    sql_statements.extend(_chronicle_triggers(conn, table_name))

    # Execute all statements within a transaction
    with conn:
        for stmt in sql_statements:
            cursor.execute(stmt)


def _chronicle_triggers(conn: sqlite3.Connection, table_name: str) -> List[str]:
    """
    Return SQL statements to create chronicle triggers for the given table.
    """
    chron = f"_chronicle_{table_name}"
    cur = conn.cursor()

    # get pk / non‐pk column lists from the primary table
    cur.execute(f'PRAGMA table_info("{table_name}")')
    info = cur.fetchall()
    pks = [r[1] for r in info if r[5]]
    nonpks = [r[1] for r in info if not r[5]]
    if not pks:
        raise ChronicleError(
            f"{table_name!r} has no PRIMARY KEY, cannot create triggers"
        )

    # SQL expression for next version number
    nextv = f'COALESCE((SELECT MAX({VERSION_COL}) FROM "{chron}"),0) + 1'

    pk_list = ", ".join(f'"{c}"' for c in pks)
    new_pk_list = ", ".join(f'NEW."{c}"' for c in pks)
    match_new = " AND ".join(f'"{c}"=NEW."{c}"' for c in pks)
    match_old = " AND ".join(f'"{c}"=OLD."{c}"' for c in pks)

    # build an UPDATE‐only‐if‐really‐changed clause
    if nonpks:
        cond = " OR ".join(f'OLD."{c}" IS NOT NEW."{c}"' for c in nonpks)
        when = f"WHEN {cond}"
    else:
        when = ""

    stmts: List[str] = []

    # AFTER INSERT
    stmts.append(
        textwrap.dedent(
            f"""
    CREATE TRIGGER "chronicle_{table_name}_ai"
    AFTER INSERT ON "{table_name}"
    FOR EACH ROW
    BEGIN
      INSERT OR IGNORE INTO "{chron}" (
        {pk_list}, {ADDED_MS_COL}, {UPDATED_MS_COL}, {VERSION_COL}, {DELETED_COL}
      ) VALUES (
        {new_pk_list},
        {CURRENT_TIMESTAMP_MS_EXPR},
        {CURRENT_TIMESTAMP_MS_EXPR},
        {nextv},
        0
      );
    END;
    """
        ).strip()
    )

    # AFTER UPDATE
    stmts.append(
        textwrap.dedent(
            f"""
    CREATE TRIGGER "chronicle_{table_name}_au"
    AFTER UPDATE ON "{table_name}"
    FOR EACH ROW
    {when}
    BEGIN
      UPDATE "{chron}"
      SET {UPDATED_MS_COL} = {CURRENT_TIMESTAMP_MS_EXPR},
        {VERSION_COL} = {nextv}
      WHERE {match_new};
    END;
    """
        ).strip()
    )

    # AFTER DELETE
    stmts.append(
        textwrap.dedent(
            f"""
    CREATE TRIGGER "chronicle_{table_name}_ad"
    AFTER DELETE ON "{table_name}"
    FOR EACH ROW
    BEGIN
      UPDATE "{chron}"
        SET {UPDATED_MS_COL} = {CURRENT_TIMESTAMP_MS_EXPR},
          {VERSION_COL} = {nextv},
          {DELETED_COL} = 1
      WHERE {match_old};
    END;
    """
        ).strip()
    )
    return stmts


def upgrade_chronicle(conn: sqlite3.Connection, table_name: str) -> None:
    """
    Migrate a *legacy* chronicle table:

    - If _chronicle_<table_name> does not exist → no-op
    - If it *does* exist and still has columns named
      added_ms, updated_ms, version, deleted then migrate to new table
    """
    chron = f"_chronicle_{table_name}"
    cur = conn.cursor()

    # Does the chronicle table even exist?
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (chron,),
    )
    if not cur.fetchone():
        return

    # Inspect its columns, bail if we're already on the new schema
    cur.execute(f'PRAGMA table_info("{chron}")')
    cols = [r[1] for r in cur.fetchall()]
    if "added_ms" not in cols:
        return  # already migrated

    # Build an ALTER + DROP + CREATE script
    script = f"""
    DROP INDEX IF EXISTS "{chron}_version";

    DROP TRIGGER IF EXISTS "_chronicle_{table_name}_ai";
    DROP TRIGGER IF EXISTS "_chronicle_{table_name}_au";
    DROP TRIGGER IF EXISTS "_chronicle_{table_name}_ad";

    ALTER TABLE "{chron}" RENAME COLUMN added_ms TO __added_ms;
    ALTER TABLE "{chron}" RENAME COLUMN updated_ms TO __updated_ms;
    ALTER TABLE "{chron}" RENAME COLUMN version TO __version;
    ALTER TABLE "{chron}" RENAME COLUMN deleted TO __deleted;

    CREATE INDEX IF NOT EXISTS "{chron}__version_idx"
        ON "{chron}"(__version);
    """
    with conn:
        conn.executescript(script)
        # 4) re‐create the new triggers
        for stmt in _chronicle_triggers(conn, table_name):
            conn.execute(stmt)


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
        [pks, originals, f"c.{ADDED_MS_COL}", f"c.{UPDATED_MS_COL}", f"c.{VERSION_COL}", f"c.{DELETED_COL}"]
    )
    join = " AND ".join(f'c."{c}" = t."{c}"' for c in pk_names)

    sql = textwrap.dedent(
        f"""
        SELECT {select}
          FROM {chron} AS c
          LEFT JOIN "{table_name}" AS t
            ON {join}
         WHERE c.{VERSION_COL} > ?
         ORDER BY c.{VERSION_COL}
         LIMIT {batch_size}
        """
    ).strip()

    while True:
        rows = cur.execute(sql, (since,)).fetchall()
        if not rows:
            break
        for r in rows:
            since = r[VERSION_COL]
            # build row dict of original columns
            row = {
                c: r[c]
                for c in r.keys()
                if c not in (ADDED_MS_COL, UPDATED_MS_COL, VERSION_COL, DELETED_COL)
            }
            yield Change(
                pks=tuple(r[c] for c in pk_names),
                added_ms=r[ADDED_MS_COL],
                updated_ms=r[UPDATED_MS_COL],
                version=r[VERSION_COL],
                row=row,
                deleted=bool(r[DELETED_COL]),
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
