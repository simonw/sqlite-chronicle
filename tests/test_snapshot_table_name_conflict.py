import sqlite_utils
from sqlite_chronicle import enable_chronicle, list_chronicled_tables


def test_dogs_and_snapshot_dogs_conflict_dogs_first():
    """
    Tables 'dogs' and 'snapshot_dogs' both have chronicle tables under
    _chronicle_{name}. With the old per-table snapshot scheme, both would
    compete for '_chronicle_snapshot_dogs'. The shared _chroniclesnapshots
    table avoids this — both tables can be chronicled independently.
    """
    db = sqlite_utils.Database(memory=True)
    db["dogs"].insert({"id": 1, "name": "Cleo"}, pk="id")
    db["snapshot_dogs"].insert({"id": 1, "name": "Pancakes"}, pk="id")

    enable_chronicle(db.conn, "dogs")
    enable_chronicle(db.conn, "snapshot_dogs")

    # Verify dogs chronicle works
    dogs_chronicle = list(
        db.execute("SELECT id, __version FROM _chronicle_dogs ORDER BY id").fetchall()
    )
    assert dogs_chronicle == [(1, 1)]

    # Insert into snapshot_dogs — should be tracked in its own chronicle table
    db["snapshot_dogs"].insert({"id": 2, "name": "Mango"})

    # Both chronicle tables work independently
    snapshot_chronicle = list(
        db.execute(
            "SELECT id, __version FROM _chronicle_snapshot_dogs ORDER BY id"
        ).fetchall()
    )
    assert snapshot_chronicle == [(1, 1), (2, 2)]


def test_dogs_and_snapshot_dogs_conflict_snapshot_dogs_first():
    """
    Same conflict scenario as above but with the opposite enable order.
    Enabling 'snapshot_dogs' first, then 'dogs' should work because the
    shared _chroniclesnapshots table keeps snapshot data separate per table.
    """
    db = sqlite_utils.Database(memory=True)
    db["dogs"].insert({"id": 1, "name": "Cleo"}, pk="id")
    db["snapshot_dogs"].insert({"id": 1, "name": "Pancakes"}, pk="id")

    enable_chronicle(db.conn, "snapshot_dogs")
    enable_chronicle(db.conn, "dogs")

    # Verify snapshot_dogs chronicle works
    snapshot_chronicle = list(
        db.execute(
            "SELECT id, __version FROM _chronicle_snapshot_dogs ORDER BY id"
        ).fetchall()
    )
    assert snapshot_chronicle == [(1, 1)]

    # INSERT OR REPLACE on dogs should be tracked correctly
    db.execute("INSERT OR REPLACE INTO dogs (id, name) VALUES (1, 'Cleo Updated')")

    dogs_chronicle = list(
        db.execute("SELECT id, __version FROM _chronicle_dogs ORDER BY id").fetchall()
    )
    assert dogs_chronicle == [(1, 2)]


def test_table_named_snapshots():
    """
    A table literally called 'snapshots' should not conflict with the shared
    _chroniclesnapshots helper table. With the old per-table naming scheme
    (_chronicle_snapshot_{name}), a table named 'snapshots' would produce
    _chronicle_snapshots — which could collide. The new _chroniclesnapshots
    name avoids this.
    """
    db = sqlite_utils.Database(memory=True)
    db["snapshots"].insert({"id": 1, "name": "snap1"}, pk="id")
    db["other"].insert({"id": 1, "name": "other1"}, pk="id")

    enable_chronicle(db.conn, "snapshots")
    enable_chronicle(db.conn, "other")

    assert sorted(list_chronicled_tables(db.conn)) == ["other", "snapshots"]

    # Both tables should track inserts independently
    db["snapshots"].insert({"id": 2, "name": "snap2"})
    db["other"].insert({"id": 2, "name": "other2"})

    snap_chronicle = list(
        db.execute(
            "SELECT id, __version FROM _chronicle_snapshots ORDER BY id"
        ).fetchall()
    )
    assert snap_chronicle == [(1, 1), (2, 2)]

    other_chronicle = list(
        db.execute("SELECT id, __version FROM _chronicle_other ORDER BY id").fetchall()
    )
    assert other_chronicle == [(1, 1), (2, 2)]

    # INSERT OR REPLACE should work on both tables
    db.execute(
        "INSERT OR REPLACE INTO snapshots (id, name) VALUES (1, 'snap1_updated')"
    )
    db.execute("INSERT OR REPLACE INTO other (id, name) VALUES (1, 'other1_updated')")

    snap_chronicle = list(
        db.execute(
            "SELECT id, __version FROM _chronicle_snapshots ORDER BY id"
        ).fetchall()
    )
    assert snap_chronicle == [(1, 3), (2, 2)]

    other_chronicle = list(
        db.execute("SELECT id, __version FROM _chronicle_other ORDER BY id").fetchall()
    )
    assert other_chronicle == [(1, 3), (2, 2)]
