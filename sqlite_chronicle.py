import sqlite3
import textwrap


class ChronicleError(Exception):
    pass


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
    primary_key_columns = [
        # cid, name, type, notnull, dflt_value, pk
        (row[1], row[2])
        for row in c.fetchall()
        if row[5]
    ]
    if not primary_key_columns:
        raise ChronicleError(f"Table {table_name} has no primary keys")

    # Create the _chronicle_ table
    pk_def = ", ".join(
        [f'"{col_name}" {col_type}' for col_name, col_type in primary_key_columns]
    )

    with conn:
        c.execute(
            textwrap.dedent(
                f"""
            CREATE TABLE "_chronicle_{table_name}" (
                {pk_def},
                updated_ms INTEGER,
                deleted INTEGER DEFAULT 0,
                PRIMARY KEY ({', '.join([f'"{col[0]}"' for col in primary_key_columns])})
            );
        """
            )
        )
        # Add an index on the updated_ms column
        c.execute(
            f"""
            CREATE INDEX "_chronicle_{table_name}_updated_ms" ON "_chronicle_{table_name}" (updated_ms);
        """.strip()
        )

        # Populate the _chronicle_ table with existing rows from the original table
        current_time_expr = (
            "CAST((julianday('now') - 2440587.5) * 86400 * 1000 AS INTEGER)"
        )
        c.execute(
            f"""
            INSERT INTO "_chronicle_{table_name}" ({', '.join([col[0] for col in primary_key_columns])}, updated_ms)
            SELECT {', '.join([f'"{col[0]}"' for col in primary_key_columns])}, {current_time_expr} 
            FROM "{table_name}";
        """
        )

        # Create the after insert trigger
        c.execute(
            f"""
            CREATE TRIGGER "_chronicle_{table_name}_ai"
            AFTER INSERT ON "{table_name}"
            FOR EACH ROW
            BEGIN
                INSERT INTO "_chronicle_{table_name}" ({', '.join([f'"{col[0]}"' for col in primary_key_columns])}, updated_ms)
                VALUES ({', '.join(['NEW.' + f'"{col[0]}"' for col in primary_key_columns])}, {current_time_expr});
            END;
        """
        )

        # Create the after update trigger
        c.execute(
            f"""
            CREATE TRIGGER "_chronicle_{table_name}_au"
            AFTER UPDATE ON "{table_name}"
            FOR EACH ROW
            BEGIN
                UPDATE "_chronicle_{table_name}"
                SET updated_ms = {current_time_expr},
                -- Also update primary key columns if they have changed:
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
                SET updated_ms = {current_time_expr}, deleted = 1
                WHERE { ' AND '.join([f'"{col[0]}" = OLD."{col[0]}"' for col in primary_key_columns]) };
            END;
        """
        )
