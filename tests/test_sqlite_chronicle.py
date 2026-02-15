import pytest
import sqlite_utils
from sqlite_chronicle import enable_chronicle
import time
from unittest.mock import ANY


@pytest.mark.parametrize("table_name", ("dogs", "dogs and stuff", "weird.table.name"))
@pytest.mark.parametrize("pks", (["id"], ["id", "name"]))
def test_enable_chronicle(table_name, pks):
    chronicle_table = f"_chronicle_{table_name}"
    db = sqlite_utils.Database(memory=True)
    db[table_name].insert_all(
        [
            {"id": 1, "name": "Cleo", "color": "black"},
        ],
        pk=pks[0] if len(pks) == 1 else pks,
    )
    enable_chronicle(db.conn, table_name)
    db[table_name].insert({"id": 2, "name": "Pancakes", "color": "corgi"})
    # It should have the same primary keys
    assert db[chronicle_table].pks == pks
    # Should also have updated_ms and deleted columns
    assert set(db[chronicle_table].columns_dict.keys()) == set(
        pks + ["__added_ms", "__updated_ms", "__version", "__deleted"]
    )
    # With an index
    assert db[chronicle_table].indexes[0].columns == ["__version"]
    if pks == ["id"]:
        expected = [
            {
                "id": 1,
                "__added_ms": ANY,
                "__updated_ms": ANY,
                "__version": 1,
                "__deleted": 0,
            },
            {
                "id": 2,
                "__added_ms": ANY,
                "__updated_ms": ANY,
                "__version": 2,
                "__deleted": 0,
            },
        ]
    else:
        expected = [
            {
                "id": 1,
                "name": "Cleo",
                "__added_ms": ANY,
                "__updated_ms": ANY,
                "__version": 1,
                "__deleted": 0,
            },
            {
                "id": 2,
                "name": "Pancakes",
                "__added_ms": ANY,
                "__updated_ms": ANY,
                "__version": 2,
                "__deleted": 0,
            },
        ]
    rows = list(db[chronicle_table].rows)
    assert rows == expected
    for row in rows:
        assert row["__added_ms"] != 0
        assert row["__updated_ms"] != 0
        assert row["__added_ms"] == row["__updated_ms"]
    # Running it again should do nothing because table exists
    enable_chronicle(db.conn, table_name)
    # Insert a row
    db[table_name].insert({"id": 3, "name": "Mango", "color": "orange"})
    get_by = 3 if pks == ["id"] else (3, "Mango")
    row = db[chronicle_table].get(get_by)
    if pks == ["id"]:
        assert row == {
            "id": 3,
            "__added_ms": ANY,
            "__updated_ms": ANY,
            "__version": 3,
            "__deleted": 0,
        }

    else:
        assert row == {
            "id": 3,
            "name": "Mango",
            "__added_ms": ANY,
            "__updated_ms": ANY,
            "__version": 3,
            "__deleted": 0,
        }

    version = db[chronicle_table].get(get_by)["__version"]
    updated_ms = db[chronicle_table].get(get_by)["__updated_ms"]
    time.sleep(0.01)
    # Update a row
    db[table_name].update(get_by, {"color": "mango"})
    assert db[chronicle_table].get(get_by)["__version"] > version
    assert db[chronicle_table].get(get_by)["__updated_ms"] > updated_ms
    # Delete a row
    assert db[table_name].count == 3
    time.sleep(0.01)
    db[table_name].delete(get_by)
    assert db[table_name].count == 2
    assert db[chronicle_table].get(get_by)["__deleted"] == 1
    new_version = db[chronicle_table].get(get_by)["__version"]
    assert new_version > version


@pytest.mark.parametrize("pks", (["foo"], ["foo", "bar"]))
def test_enable_chronicle_alternative_primary_keys(pks):
    db = sqlite_utils.Database(memory=True)
    db["dogs"].insert({"foo": 1, "bar": 2, "name": "Cleo", "color": "black"}, pk=pks)
    enable_chronicle(db.conn, "dogs")
    assert db["_chronicle_dogs"].pks == pks


def test_upsert():
    db = sqlite_utils.Database(memory=True)
    dogs = db.table("dogs", pk="id").create(
        {"id": int, "name": str, "color": str}, pk="id"
    )
    enable_chronicle(db.conn, "dogs")
    dogs.insert({"id": 1, "name": "Cleo", "color": "black"})
    dogs.upsert({"id": 2, "name": "Pancakes", "color": "corgi"})

    def chronicle_rows():
        return list(
            db.query("select id, __version as version from _chronicle_dogs order by id")
        )

    assert chronicle_rows() == [{"id": 1, "version": 1}, {"id": 2, "version": 2}]

    # Upsert that should update the row
    dogs.upsert({"id": 1, "name": "Cleo", "color": "brown"})
    assert chronicle_rows() == [{"id": 1, "version": 3}, {"id": 2, "version": 2}]

    # Upsert that should be a no-op
    dogs.upsert({"id": 1, "name": "Cleo", "color": "brown"})
    assert chronicle_rows() == [{"id": 1, "version": 3}, {"id": 2, "version": 2}]


def test_insert_or_replace():
    db = sqlite_utils.Database(memory=True)
    dogs = db.table("dogs", pk="id").create(
        {"id": int, "name": str, "color": str}, pk="id"
    )
    enable_chronicle(db.conn, "dogs")
    dogs.insert({"id": 1, "name": "Cleo", "color": "black"})

    def chronicle_rows():
        return list(
            db.query(
                "select id, __version as version, __deleted as deleted"
                " from _chronicle_dogs order by id"
            )
        )

    assert chronicle_rows() == [
        {"id": 1, "version": 1, "deleted": 0},
    ]

    # Record the original added_ms
    original_added_ms = db.execute(
        "select __added_ms from _chronicle_dogs where id = 1"
    ).fetchone()[0]

    time.sleep(0.01)

    # INSERT OR REPLACE that changes data — should bump version, preserve added_ms
    db.execute(
        "INSERT OR REPLACE INTO dogs (id, name, color) VALUES (1, 'Cleo', 'brown')"
    )
    rows = chronicle_rows()
    assert rows == [{"id": 1, "version": 2, "deleted": 0}]
    added_after_replace = db.execute(
        "select __added_ms from _chronicle_dogs where id = 1"
    ).fetchone()[0]
    assert added_after_replace == original_added_ms, "added_ms should be preserved"

    time.sleep(0.01)

    # INSERT OR REPLACE with identical data — should be a no-op (no version bump)
    db.execute(
        "INSERT OR REPLACE INTO dogs (id, name, color) VALUES (1, 'Cleo', 'brown')"
    )
    rows = chronicle_rows()
    assert rows == [{"id": 1, "version": 2, "deleted": 0}]

    # INSERT OR REPLACE on a new row — should work like a regular insert
    db.execute(
        "INSERT OR REPLACE INTO dogs (id, name, color) VALUES (2, 'Pancakes', 'corgi')"
    )
    rows = chronicle_rows()
    assert rows == [
        {"id": 1, "version": 2, "deleted": 0},
        {"id": 2, "version": 3, "deleted": 0},
    ]


def test_insert_or_replace_compound_pk():
    db = sqlite_utils.Database(memory=True)
    db.execute("CREATE TABLE pairs (a TEXT, b INTEGER, val TEXT, PRIMARY KEY(a, b))")
    enable_chronicle(db.conn, "pairs")
    db.execute("INSERT INTO pairs VALUES ('x', 1, 'hello')")

    def chronicle_rows():
        return list(
            db.query(
                "select a, b, __version as version, __deleted as deleted"
                " from _chronicle_pairs order by a, b"
            )
        )

    assert chronicle_rows() == [{"a": "x", "b": 1, "version": 1, "deleted": 0}]

    # INSERT OR REPLACE with changed data
    db.execute("INSERT OR REPLACE INTO pairs VALUES ('x', 1, 'world')")
    assert chronicle_rows() == [{"a": "x", "b": 1, "version": 2, "deleted": 0}]

    # INSERT OR REPLACE with identical data — no version bump
    db.execute("INSERT OR REPLACE INTO pairs VALUES ('x', 1, 'world')")
    assert chronicle_rows() == [{"a": "x", "b": 1, "version": 2, "deleted": 0}]


def test_insert_or_replace_after_delete():
    """Re-inserting via INSERT OR REPLACE after a delete should un-delete the chronicle row."""
    db = sqlite_utils.Database(memory=True)
    dogs = db.table("dogs", pk="id").create(
        {"id": int, "name": str, "color": str}, pk="id"
    )
    enable_chronicle(db.conn, "dogs")
    dogs.insert({"id": 1, "name": "Cleo", "color": "black"})
    db.execute("DELETE FROM dogs WHERE id = 1")

    def chronicle_rows():
        return list(
            db.query(
                "select id, __version as version, __deleted as deleted"
                " from _chronicle_dogs order by id"
            )
        )

    assert chronicle_rows() == [{"id": 1, "version": 2, "deleted": 1}]

    # Re-insert via INSERT OR REPLACE
    db.execute(
        "INSERT OR REPLACE INTO dogs (id, name, color) VALUES (1, 'Cleo', 'brown')"
    )
    rows = chronicle_rows()
    assert rows == [{"id": 1, "version": 3, "deleted": 0}]


def test_insert_or_replace_blob_column():
    """INSERT OR REPLACE should work correctly with BLOB columns."""
    db = sqlite_utils.Database(memory=True)
    db.execute("CREATE TABLE files (id INTEGER PRIMARY KEY, name TEXT, data BLOB)")
    enable_chronicle(db.conn, "files")

    def chronicle_rows():
        return list(
            db.query(
                "select id, __version as version, __deleted as deleted"
                " from _chronicle_files order by id"
            )
        )

    # Insert a row with a blob
    db.execute("INSERT INTO files VALUES (1, 'a.bin', X'DEADBEEF')")
    assert chronicle_rows() == [{"id": 1, "version": 1, "deleted": 0}]

    # INSERT OR REPLACE with changed blob — should bump version
    db.execute("INSERT OR REPLACE INTO files VALUES (1, 'a.bin', X'CAFEBABE')")
    assert chronicle_rows() == [{"id": 1, "version": 2, "deleted": 0}]

    # INSERT OR REPLACE with identical blob — no-op
    db.execute("INSERT OR REPLACE INTO files VALUES (1, 'a.bin', X'CAFEBABE')")
    assert chronicle_rows() == [{"id": 1, "version": 2, "deleted": 0}]

    # Change the text column but keep the blob — should bump
    db.execute("INSERT OR REPLACE INTO files VALUES (1, 'b.bin', X'CAFEBABE')")
    assert chronicle_rows() == [{"id": 1, "version": 3, "deleted": 0}]

    # Verify the actual data in the main table
    row = db.execute("SELECT name, data FROM files WHERE id = 1").fetchone()
    assert row[0] == "b.bin"
    assert row[1] == b"\xca\xfe\xba\xbe"


def test_insert_or_replace_null_values():
    """INSERT OR REPLACE should handle NULL values correctly in change detection."""
    db = sqlite_utils.Database(memory=True)
    db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, a TEXT, b INTEGER)")
    enable_chronicle(db.conn, "items")

    def version_of(pk):
        return db.execute(
            "SELECT __version FROM _chronicle_items WHERE id = ?", [pk]
        ).fetchone()[0]

    # Insert with NULLs
    db.execute("INSERT INTO items VALUES (1, NULL, NULL)")
    assert version_of(1) == 1

    # Replace with identical NULLs — no-op
    db.execute("INSERT OR REPLACE INTO items VALUES (1, NULL, NULL)")
    assert version_of(1) == 1

    # Replace NULL with a value — should bump
    db.execute("INSERT OR REPLACE INTO items VALUES (1, 'hello', NULL)")
    assert version_of(1) == 2

    # Replace value with NULL — should bump
    db.execute("INSERT OR REPLACE INTO items VALUES (1, NULL, NULL)")
    assert version_of(1) == 3

    # Identical NULLs again — no-op
    db.execute("INSERT OR REPLACE INTO items VALUES (1, NULL, NULL)")
    assert version_of(1) == 3


def test_insert_or_replace_mixed_types():
    """INSERT OR REPLACE change detection across TEXT, INTEGER, REAL, BLOB, and NULL."""
    db = sqlite_utils.Database(memory=True)
    db.execute(
        "CREATE TABLE mix (id INTEGER PRIMARY KEY, t TEXT, i INTEGER, r REAL, b BLOB)"
    )
    enable_chronicle(db.conn, "mix")

    def version_of(pk):
        return db.execute(
            "SELECT __version FROM _chronicle_mix WHERE id = ?", [pk]
        ).fetchone()[0]

    db.execute("INSERT INTO mix VALUES (1, 'hi', 42, 3.14, X'FF')")
    assert version_of(1) == 1

    # Identical — no-op
    db.execute("INSERT OR REPLACE INTO mix VALUES (1, 'hi', 42, 3.14, X'FF')")
    assert version_of(1) == 1

    # Change just the text
    db.execute("INSERT OR REPLACE INTO mix VALUES (1, 'bye', 42, 3.14, X'FF')")
    assert version_of(1) == 2

    # Change just the integer
    db.execute("INSERT OR REPLACE INTO mix VALUES (1, 'bye', 99, 3.14, X'FF')")
    assert version_of(1) == 3

    # Change just the real
    db.execute("INSERT OR REPLACE INTO mix VALUES (1, 'bye', 99, 2.72, X'FF')")
    assert version_of(1) == 4

    # Change just the blob
    db.execute("INSERT OR REPLACE INTO mix VALUES (1, 'bye', 99, 2.72, X'00')")
    assert version_of(1) == 5

    # Set everything to NULL
    db.execute("INSERT OR REPLACE INTO mix VALUES (1, NULL, NULL, NULL, NULL)")
    assert version_of(1) == 6

    # All NULLs again — no-op
    db.execute("INSERT OR REPLACE INTO mix VALUES (1, NULL, NULL, NULL, NULL)")
    assert version_of(1) == 6
