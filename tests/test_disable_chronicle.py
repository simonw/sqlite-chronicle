import pytest
import sqlite_utils
from sqlite_chronicle import enable_chronicle, disable_chronicle


def test_disable_chronicle_removes_table_and_triggers():
    db = sqlite_utils.Database(memory=True)
    db["dogs"].insert({"id": 1, "name": "Cleo"}, pk="id")
    enable_chronicle(db.conn, "dogs")

    # Verify chronicle is enabled
    assert "_chronicle_dogs" in db.table_names()
    triggers = [
        r[0]
        for r in db.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger'"
        ).fetchall()
    ]
    assert "chronicle_dogs_ai" in triggers
    assert "chronicle_dogs_au" in triggers
    assert "chronicle_dogs_ad" in triggers

    # Disable chronicle
    result = disable_chronicle(db.conn, "dogs")
    assert result is True

    # Verify table is gone
    assert "_chronicle_dogs" not in db.table_names()

    # Verify triggers are gone
    triggers = [
        r[0]
        for r in db.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger'"
        ).fetchall()
    ]
    assert "chronicle_dogs_ai" not in triggers
    assert "chronicle_dogs_au" not in triggers
    assert "chronicle_dogs_ad" not in triggers

    # Verify indexes are gone
    indexes = [
        r[0]
        for r in db.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE '%chronicle%'"
        ).fetchall()
    ]
    assert indexes == []


def test_disable_chronicle_returns_false_if_not_enabled():
    db = sqlite_utils.Database(memory=True)
    db["dogs"].insert({"id": 1, "name": "Cleo"}, pk="id")

    # Should return False when no chronicle exists
    result = disable_chronicle(db.conn, "dogs")
    assert result is False


def test_disable_chronicle_idempotent():
    db = sqlite_utils.Database(memory=True)
    db["dogs"].insert({"id": 1, "name": "Cleo"}, pk="id")
    enable_chronicle(db.conn, "dogs")

    # First disable
    assert disable_chronicle(db.conn, "dogs") is True

    # Second disable should return False
    assert disable_chronicle(db.conn, "dogs") is False


def test_disable_chronicle_original_table_still_works():
    db = sqlite_utils.Database(memory=True)
    db["dogs"].insert({"id": 1, "name": "Cleo"}, pk="id")
    enable_chronicle(db.conn, "dogs")

    # Insert while chronicle is enabled
    db["dogs"].insert({"id": 2, "name": "Pancakes"})

    # Disable chronicle
    disable_chronicle(db.conn, "dogs")

    # Original table should still work normally
    db["dogs"].insert({"id": 3, "name": "Mango"})
    db["dogs"].update(1, {"name": "Cleo Updated"})
    db["dogs"].delete(2)

    rows = list(db["dogs"].rows)
    assert len(rows) == 2
    assert rows[0]["name"] == "Cleo Updated"
    assert rows[1]["name"] == "Mango"


def test_disable_chronicle_can_reenable():
    db = sqlite_utils.Database(memory=True)
    db["dogs"].insert({"id": 1, "name": "Cleo"}, pk="id")

    # Enable, disable, re-enable
    enable_chronicle(db.conn, "dogs")
    disable_chronicle(db.conn, "dogs")
    enable_chronicle(db.conn, "dogs")

    # Should work normally
    assert "_chronicle_dogs" in db.table_names()
    db["dogs"].insert({"id": 2, "name": "Pancakes"})

    chronicle_rows = list(db["_chronicle_dogs"].rows)
    assert len(chronicle_rows) == 2


@pytest.mark.parametrize("table_name", ("dogs", "dogs and stuff", "weird.table.name"))
def test_disable_chronicle_special_table_names(table_name):
    db = sqlite_utils.Database(memory=True)
    db[table_name].insert({"id": 1, "name": "Cleo"}, pk="id")
    enable_chronicle(db.conn, table_name)

    chronicle_table = f"_chronicle_{table_name}"
    assert chronicle_table in db.table_names()

    result = disable_chronicle(db.conn, table_name)
    assert result is True
    assert chronicle_table not in db.table_names()
