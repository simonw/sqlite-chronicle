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
        pks + ["updated_ms", "deleted"]
    )
    # With an index
    assert db[chronicle_table].indexes[0].columns == ["updated_ms"]
    if pks == ["id"]:
        expected = [
            {"id": 1, "updated_ms": ANY, "deleted": 0},
            {"id": 2, "updated_ms": ANY, "deleted": 0},
        ]
    else:
        expected = [
            {"id": 1, "name": "Cleo", "updated_ms": ANY, "deleted": 0},
            {"id": 2, "name": "Pancakes", "updated_ms": ANY, "deleted": 0},
        ]
    assert list(db[chronicle_table].rows) == expected
    # Running it again should do nothing because table exists
    enable_chronicle(db.conn, table_name)
    # Insert a row
    db[table_name].insert({"id": 3, "name": "Mango", "color": "orange"})
    get_by = 3 if pks == ["id"] else (3, "Mango")
    row = db[chronicle_table].get(get_by)
    if pks == ["id"]:
        assert row == {"id": 3, "updated_ms": ANY, "deleted": 0}
    else:
        assert row == {"id": 3, "name": "Mango", "updated_ms": ANY, "deleted": 0}
    record_timestamp = db[chronicle_table].get(get_by)["updated_ms"]
    time.sleep(0.01)
    # Update a row
    db[table_name].update(get_by, {"color": "mango"})
    assert db[chronicle_table].get(get_by)["updated_ms"] > record_timestamp
    # Delete a row
    assert db[table_name].count == 3
    time.sleep(0.01)
    db[table_name].delete(get_by)
    assert db[table_name].count == 2
    assert db[chronicle_table].get(get_by)["deleted"] == 1
    new_record_timestamp = db[chronicle_table].get(get_by)["updated_ms"]
    assert new_record_timestamp > record_timestamp
    # Now update a column that's part of the compound primary key
    time.sleep(0.1)
    if pks == ["id", "name"]:
        db[table_name].update((2, "Pancakes"), {"name": "Pancakes the corgi"})
        # This should have renamed the row in the chronicle table as well
        renamed_row = db[chronicle_table].get((2, "Pancakes the corgi"))
        assert renamed_row["updated_ms"] > record_timestamp
    else:
        # Update single primary key
        db[table_name].update(2, {"id": 4})
        # This should have renamed the row in the chronicle table as well
        renamed_row = db[chronicle_table].get(4)
        assert renamed_row["updated_ms"] > record_timestamp
