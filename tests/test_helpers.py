import sqlite_utils
from sqlite_chronicle import (
    enable_chronicle,
    disable_chronicle,
    is_chronicle_enabled,
    list_chronicled_tables,
)


def test_is_chronicle_enabled_true():
    db = sqlite_utils.Database(memory=True)
    db["dogs"].insert({"id": 1, "name": "Cleo"}, pk="id")
    enable_chronicle(db.conn, "dogs")

    assert is_chronicle_enabled(db.conn, "dogs") is True


def test_is_chronicle_enabled_false():
    db = sqlite_utils.Database(memory=True)
    db["dogs"].insert({"id": 1, "name": "Cleo"}, pk="id")

    assert is_chronicle_enabled(db.conn, "dogs") is False


def test_is_chronicle_enabled_after_disable():
    db = sqlite_utils.Database(memory=True)
    db["dogs"].insert({"id": 1, "name": "Cleo"}, pk="id")
    enable_chronicle(db.conn, "dogs")

    assert is_chronicle_enabled(db.conn, "dogs") is True
    disable_chronicle(db.conn, "dogs")
    assert is_chronicle_enabled(db.conn, "dogs") is False


def test_is_chronicle_enabled_nonexistent_table():
    db = sqlite_utils.Database(memory=True)

    # Table doesn't exist at all
    assert is_chronicle_enabled(db.conn, "nonexistent") is False


def test_list_chronicled_tables_empty():
    db = sqlite_utils.Database(memory=True)
    db["dogs"].insert({"id": 1, "name": "Cleo"}, pk="id")

    assert list_chronicled_tables(db.conn) == []


def test_list_chronicled_tables_single():
    db = sqlite_utils.Database(memory=True)
    db["dogs"].insert({"id": 1, "name": "Cleo"}, pk="id")
    enable_chronicle(db.conn, "dogs")

    result = list_chronicled_tables(db.conn)
    assert result == ["dogs"]


def test_list_chronicled_tables_multiple():
    db = sqlite_utils.Database(memory=True)
    db["dogs"].insert({"id": 1, "name": "Cleo"}, pk="id")
    db["cats"].insert({"id": 1, "name": "Whiskers"}, pk="id")
    db["birds"].insert({"id": 1, "name": "Tweety"}, pk="id")

    enable_chronicle(db.conn, "dogs")
    enable_chronicle(db.conn, "cats")
    # birds not enabled

    result = list_chronicled_tables(db.conn)
    assert sorted(result) == ["cats", "dogs"]


def test_list_chronicled_tables_after_disable():
    db = sqlite_utils.Database(memory=True)
    db["dogs"].insert({"id": 1, "name": "Cleo"}, pk="id")
    db["cats"].insert({"id": 1, "name": "Whiskers"}, pk="id")

    enable_chronicle(db.conn, "dogs")
    enable_chronicle(db.conn, "cats")

    assert sorted(list_chronicled_tables(db.conn)) == ["cats", "dogs"]

    disable_chronicle(db.conn, "dogs")

    assert list_chronicled_tables(db.conn) == ["cats"]


def test_list_chronicled_tables_special_names():
    db = sqlite_utils.Database(memory=True)
    db["dogs and stuff"].insert({"id": 1, "name": "Cleo"}, pk="id")
    db["weird.table.name"].insert({"id": 1, "name": "Whiskers"}, pk="id")

    enable_chronicle(db.conn, "dogs and stuff")
    enable_chronicle(db.conn, "weird.table.name")

    result = list_chronicled_tables(db.conn)
    assert sorted(result) == ["dogs and stuff", "weird.table.name"]
