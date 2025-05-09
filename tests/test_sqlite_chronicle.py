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
