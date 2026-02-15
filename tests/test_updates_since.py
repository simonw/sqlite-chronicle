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


@pytest.mark.parametrize("table_name", ("dogs and stuff", "weird.table.name"))
def test_updates_since_special_table_names(table_name):
    """Test that updates_since works with table names containing special characters."""
    db = Database(memory=True)
    db[table_name].insert_all(
        [{"id": 1, "name": "Cleo"}, {"id": 2, "name": "Pancakes"}],
        pk="id",
    )
    enable_chronicle(db.conn, table_name)

    # Should not raise an error and should return correct results
    changes = list(updates_since(db.conn, table_name))
    assert len(changes) == 2
    assert changes[0].pks == (1,)
    assert changes[1].pks == (2,)
