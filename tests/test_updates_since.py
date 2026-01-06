from sqlite_chronicle import enable_chronicle, updates_since, Change
from sqlite_utils import Database
import pytest
from unittest.mock import ANY


@pytest.fixture
def db():
    db = Database(memory=True)
    db["mysteries"].insert_all(
        [
            {
                "id": 1,
                "name": "The fate of the crew on the Mary Celeste",
                "year": "1872",
            },
            {
                "id": 2,
                "name": "The disappearance of the Amber Room",
                "year": "1941",
            },
        ],
        pk="id",
    )
    enable_chronicle(db.conn, "mysteries")
    return db


def _add_row(db):
    db["mysteries"].insert(
        {
            "id": 3,
            "name": "The lost city of Atlantis",
            "year": "360 BC",
        }
    )


def _update_rows(db):
    with db.conn:
        db.execute("update mysteries set year = 'unknown' where id in (1, 3)")


def _delete_row(db):
    with db.conn:
        db.execute("delete from mysteries where id = 2")


def test_updates_since(db):
    changes = list(updates_since(db.conn, "mysteries"))
    assert changes == [
        Change(
            pks=(1,),
            added_ms=ANY,
            updated_ms=ANY,
            version=1,
            row={
                "id": 1,
                "name": "The fate of the crew on the Mary Celeste",
                "year": "1872",
            },
            deleted=False,
        ),
        Change(
            pks=(2,),
            added_ms=ANY,
            updated_ms=ANY,
            version=2,
            row={
                "id": 2,
                "name": "The disappearance of the Amber Room",
                "year": "1941",
            },
            deleted=False,
        ),
    ]
    last = changes[-1].version
    _add_row(db)
    new_changes = list(updates_since(db.conn, "mysteries", since=last))
    assert new_changes == [
        Change(
            pks=(3,),
            added_ms=ANY,
            updated_ms=ANY,
            version=3,
            row={"id": 3, "name": "The lost city of Atlantis", "year": "360 BC"},
            deleted=False,
        )
    ]
    last2 = new_changes[-1].version
    _update_rows(db)
    new_changes2 = list(updates_since(db.conn, "mysteries", since=last2))
    assert new_changes2 == [
        Change(
            pks=(1,),
            added_ms=ANY,
            updated_ms=ANY,
            version=4,
            row={
                "id": 1,
                "name": "The fate of the crew on the Mary Celeste",
                "year": "unknown",
            },
            deleted=False,
        ),
        Change(
            pks=(3,),
            added_ms=ANY,
            updated_ms=ANY,
            version=5,
            row={"id": 3, "name": "The lost city of Atlantis", "year": "unknown"},
            deleted=False,
        ),
    ]
    last3 = new_changes2[-1].version
    _delete_row(db)
    new_changes3 = list(updates_since(db.conn, "mysteries", since=last3))
    assert new_changes3 == [
        Change(
            pks=(2,),
            added_ms=ANY,
            updated_ms=ANY,
            version=6,
            row={"id": 2, "name": None, "year": None},
            deleted=True,
        )
    ]


def test_updates_since_more_rows_than_batch_size_when_enabled():
    db = Database(memory=True)
    db["mysteries"].insert_all(
        ({"id": i, "name": "Name {}".format(i)} for i in range(201)), pk="id"
    )
    enable_chronicle(db.conn, "mysteries")
    changes = list(updates_since(db.conn, "mysteries", batch_size=100))
    assert len(changes) == 201


def test_updates_since_more_rows_than_batch_size_in_an_update():
    # https://github.com/simonw/sqlite-chronicle/issues/4#issuecomment-1842059727
    db = Database(memory=True)
    db["mysteries"].insert_all(
        ({"id": i, "name": "Name {}".format(i)} for i in range(201)), pk="id"
    )
    enable_chronicle(db.conn, "mysteries")
    max_v = db.execute("select max(__version) from _chronicle_mysteries").fetchone()[0]
    # Update them all in one go
    with db.conn:
        db.execute("update mysteries set name = 'Updated'")

    changes = list(updates_since(db.conn, "mysteries", batch_size=100, since=max_v))
    assert len(changes) == 201
    # Each change should have a different version
    assert len(set(c.version for c in changes)) == 201


def test_updates_since_batch_size_equals_rows():
    """Test batch_size exactly matching the number of rows."""
    db = Database(memory=True)
    db["items"].insert_all(
        [{"id": i, "name": f"Item {i}"} for i in range(10)], pk="id"
    )
    enable_chronicle(db.conn, "items")

    # batch_size equals row count
    changes = list(updates_since(db.conn, "items", batch_size=10))
    assert len(changes) == 10
    assert [c.version for c in changes] == list(range(1, 11))


def test_updates_since_batch_size_one():
    """Test with batch_size=1 to ensure pagination works correctly."""
    db = Database(memory=True)
    db["items"].insert_all(
        [{"id": i, "name": f"Item {i}"} for i in range(5)], pk="id"
    )
    enable_chronicle(db.conn, "items")

    # batch_size of 1 should still return all rows
    changes = list(updates_since(db.conn, "items", batch_size=1))
    assert len(changes) == 5
    # Verify ordering is correct
    assert [c.version for c in changes] == [1, 2, 3, 4, 5]
    # Verify each row has correct data
    for i, change in enumerate(changes):
        assert change.pks == (i,)
        assert change.row["name"] == f"Item {i}"


def test_updates_since_batch_size_larger_than_rows():
    """Test with batch_size larger than total rows."""
    db = Database(memory=True)
    db["items"].insert_all(
        [{"id": i, "name": f"Item {i}"} for i in range(3)], pk="id"
    )
    enable_chronicle(db.conn, "items")

    # batch_size much larger than row count
    changes = list(updates_since(db.conn, "items", batch_size=1000))
    assert len(changes) == 3


def test_updates_since_empty_table():
    """Test updates_since on a table with no changes."""
    db = Database(memory=True)
    db.execute("CREATE TABLE empty_items (id INTEGER PRIMARY KEY, name TEXT)")
    enable_chronicle(db.conn, "empty_items")

    # Should return empty list for empty chronicle
    changes = list(updates_since(db.conn, "empty_items"))
    assert changes == []


def test_updates_since_with_since_beyond_max_version():
    """Test updates_since with since parameter beyond the max version."""
    db = Database(memory=True)
    db["items"].insert_all(
        [{"id": i, "name": f"Item {i}"} for i in range(5)], pk="id"
    )
    enable_chronicle(db.conn, "items")

    # since=1000 is way beyond max version (5)
    changes = list(updates_since(db.conn, "items", since=1000))
    assert changes == []
