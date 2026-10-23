"""
INSERT OR IGNORE, ON CONFLICT DO NOTHING and ON CONFLICT DO UPDATE all fire the
BEFORE INSERT trigger (which writes a snapshot) but never the AFTER INSERT
trigger (which cleans it up). These tests check that a leftover snapshot
cannot cause a later change to be missed.
"""

import sqlite3
import pytest
from sqlite_chronicle import enable_chronicle

NO_OP_INSERTS = (
    "INSERT OR IGNORE INTO dogs VALUES (1, 'Other')",
    "INSERT INTO dogs VALUES (1, 'Other') ON CONFLICT DO NOTHING",
    "INSERT INTO dogs VALUES (1, 'Cleo') ON CONFLICT(id) DO UPDATE SET name = excluded.name",
    "INSERT INTO dogs VALUES (1, 'Other') ON CONFLICT(id) DO UPDATE SET name = excluded.name",
)


@pytest.fixture(params=[False, True], ids=["recursive_off", "recursive_on"])
def db(request):
    conn = sqlite3.connect(":memory:", isolation_level=None)
    conn.execute(f"PRAGMA recursive_triggers = {'on' if request.param else 'off'}")
    conn.execute("CREATE TABLE dogs (id INTEGER PRIMARY KEY, name TEXT)")
    enable_chronicle(conn, "dogs")
    conn.execute("INSERT INTO dogs VALUES (1, 'Cleo')")
    return conn


def chronicle(conn, id=1):
    return conn.execute(
        "SELECT __added_ms, __updated_ms, __version, __deleted"
        " FROM _chronicle_dogs WHERE id = ?",
        (id,),
    ).fetchone()


@pytest.mark.parametrize("no_op_insert", NO_OP_INSERTS)
def test_delete_after_ignored_insert_is_recorded(db, no_op_insert):
    db.execute(no_op_insert)
    version_before = chronicle(db)[2]
    db.execute("DELETE FROM dogs WHERE id = 1")
    _, _, version, deleted = chronicle(db)
    assert deleted == 1
    assert version > version_before


@pytest.mark.parametrize("no_op_insert", NO_OP_INSERTS)
def test_reinsert_after_ignored_insert_and_delete(db, no_op_insert):
    db.execute(no_op_insert)
    db.execute("DELETE FROM dogs WHERE id = 1")
    deleted_state = chronicle(db)
    db.execute("INSERT INTO dogs VALUES (1, 'Cleo')")
    added_ms, updated_ms, version, deleted = chronicle(db)
    assert deleted == 0
    assert version > deleted_state[2]
    # Treated as a fresh addition, not a REPLACE of the stale snapshot
    assert added_ms == updated_ms >= deleted_state[1]


def test_upsert_does_not_leave_snapshot(db):
    db.execute(
        "INSERT INTO dogs VALUES (1, 'Pancakes')"
        " ON CONFLICT(id) DO UPDATE SET name = excluded.name"
    )
    assert db.execute("SELECT count(*) FROM _chroniclesnapshots").fetchone()[0] == 0


def test_back_to_back_statements(db):
    # Statements run within the same millisecond share julianday('now'), so
    # nothing here may rely on timestamps to tell statements apart
    for i in range(200):
        db.execute("INSERT INTO dogs VALUES (1, 'Cleo') ON CONFLICT DO NOTHING")
        db.execute("INSERT OR IGNORE INTO dogs VALUES (1, 'Other')")
        db.execute("DELETE FROM dogs WHERE id = 1")
        assert chronicle(db)[3] == 1, f"delete lost on iteration {i}"
        db.execute("INSERT INTO dogs VALUES (1, 'Cleo')")
        assert chronicle(db)[3] == 0


def test_multi_row_replace_same_key(db):
    version = chronicle(db)[2]
    # Identical data twice in one statement: no change
    db.execute("INSERT OR REPLACE INTO dogs VALUES (1, 'Cleo'), (1, 'Cleo')")
    assert chronicle(db)[2] == version
    # Changed then changed back within one statement: still counts as changed
    db.execute("INSERT OR REPLACE INTO dogs VALUES (1, 'Other'), (1, 'Cleo')")
    assert chronicle(db)[2] > version
    assert chronicle(db)[3] == 0
