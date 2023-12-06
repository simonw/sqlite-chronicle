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
            {"id": 2, "name": "Pancakes", "color": "corgi"},
        ],
        pk=pks[0] if len(pks) == 1 else pks,
    )
    enable_chronicle(db.conn, table_name)
    # It should have the same primary keys
    assert db[chronicle_table].pks == pks
    # Should also have updated_ms and deleted columns
    assert set(db[chronicle_table].columns_dict.keys()) == set(
        pks + ["added_ms", "updated_ms", "version", "deleted"]
    )
    # With an index
    assert db[chronicle_table].indexes[0].columns == ["version"]
    if pks == ["id"]:
        expected = [
            {
                "id": 1,
                "added_ms": ANY,
                "updated_ms": ANY,
                "version": 1,
                "deleted": 0,
            },
            {
                "id": 2,
                "added_ms": ANY,
                "updated_ms": ANY,
                "version": 2,
                "deleted": 0,
            },
        ]
    else:
        expected = [
            {
                "id": 1,
                "name": "Cleo",
                "added_ms": ANY,
                "updated_ms": ANY,
                "version": 1,
                "deleted": 0,
            },
            {
                "id": 2,
                "name": "Pancakes",
                "added_ms": ANY,
                "updated_ms": ANY,
                "version": 2,
                "deleted": 0,
            },
        ]
    rows = list(db[chronicle_table].rows)
    assert rows == expected
    # Running it again should do nothing because table exists
    enable_chronicle(db.conn, table_name)
    # Insert a row
    db[table_name].insert({"id": 3, "name": "Mango", "color": "orange"})
    get_by = 3 if pks == ["id"] else (3, "Mango")
    row = db[chronicle_table].get(get_by)
    if pks == ["id"]:
        assert row == {
            "id": 3,
            "added_ms": ANY,
            "updated_ms": ANY,
            "version": 3,
            "deleted": 0,
        }

    else:
        assert row == {
            "id": 3,
            "name": "Mango",
            "added_ms": ANY,
            "updated_ms": ANY,
            "version": 3,
            "deleted": 0,
        }

    version = db[chronicle_table].get(get_by)["updated_ms"]
    time.sleep(0.01)
    # Update a row
    db[table_name].update(get_by, {"color": "mango"})
    assert db[chronicle_table].get(get_by)["updated_ms"] > version
    # Delete a row
    assert db[table_name].count == 3
    time.sleep(0.01)
    db[table_name].delete(get_by)
    assert db[table_name].count == 2
    assert db[chronicle_table].get(get_by)["deleted"] == 1
    new_version = db[chronicle_table].get(get_by)["updated_ms"]
    assert new_version > version
    # Now update a column that's part of the compound primary key
    time.sleep(0.1)
    if pks == ["id", "name"]:
        db[table_name].update((2, "Pancakes"), {"name": "Pancakes the corgi"})
        # This should have renamed the row in the chronicle table as well
        renamed_row = db[chronicle_table].get((2, "Pancakes the corgi"))
        assert renamed_row["updated_ms"] > version
    else:
        # Update single primary key
        db[table_name].update(2, {"id": 4})
        # This should have renamed the row in the chronicle table as well
        renamed_row = db[chronicle_table].get(4)
        assert renamed_row["updated_ms"] > version


@pytest.mark.parametrize("pks", (["foo"], ["foo", "bar"]))
def test_enable_chronicle_alternative_primary_keys(pks):
    db = sqlite_utils.Database(memory=True)
    db["dogs"].insert({"foo": 1, "bar": 2, "name": "Cleo", "color": "black"}, pk=pks)
    enable_chronicle(db.conn, "dogs")
    assert db["_chronicle_dogs"].pks == pks
