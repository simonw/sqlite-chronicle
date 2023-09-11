import pytest
import sqlite_utils
import sqlite_chronicle
import time
from unittest.mock import ANY


@pytest.mark.parametrize("table_name", ("dogs", "dogs and stuff", "weird.table.name"))
def test_enable_chronicle(table_name):
    db = sqlite_utils.Database(memory=True)
    db[table_name].insert_all(
        [
            {"id": 1, "name": "Cleo"},
            {"id": 2, "name": "Pancakes"},
        ],
        pk="id",
    )
    sqlite_chronicle.enable_chronicle(db.conn, table_name)
    assert list(db[f"_chronicle_{table_name}"].rows) == [
        {"id": 1, "updated_ms": ANY, "deleted": 0},
        {"id": 2, "updated_ms": ANY, "deleted": 0},
    ]
    # Running it again should do nothing because table exists
    sqlite_chronicle.enable_chronicle(db.conn, table_name)
    # Insert a row
    db[table_name].insert({"id": 3, "name": "Mango"})
    assert list(db[f"_chronicle_{table_name}"].rows) == [
        {"id": 1, "updated_ms": ANY, "deleted": 0},
        {"id": 2, "updated_ms": ANY, "deleted": 0},
        {"id": 3, "updated_ms": ANY, "deleted": 0},
    ]
    pancakes_timestamp = db[f"_chronicle_{table_name}"].get(2)["updated_ms"]
    time.sleep(0.01)
    # Update a row
    db[table_name].update(2, {"name": "Pancakes the dog"})
    assert db[f"_chronicle_{table_name}"].get(2)["updated_ms"] > pancakes_timestamp
    # Delete a row
    db[table_name].delete(2)
    assert db[f"_chronicle_{table_name}"].get(2)["deleted"] == 1
    assert db[f"_chronicle_{table_name}"].get(2)["updated_ms"] > pancakes_timestamp
