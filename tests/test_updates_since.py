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
            deleted=0,
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
            deleted=0,
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
            deleted=0,
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
            deleted=0,
        ),
        Change(
            pks=(3,),
            added_ms=ANY,
            updated_ms=ANY,
            version=5,
            row={"id": 3, "name": "The lost city of Atlantis", "year": "unknown"},
            deleted=0,
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
            deleted=1,
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
    since_id = db.execute("select max(version) from _chronicle_mysteries").fetchone()[0]
    # Update them all in one go
    with db.conn:
        db.execute("update mysteries set name = 'Updated'")

    changes = list(updates_since(db.conn, "mysteries", batch_size=100, since=since_id))
    assert len(changes) == 201
    # Each change should have a different version
    assert len(set(c.version for c in changes)) == 201
