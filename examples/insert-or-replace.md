# sqlite-chronicle trigger demos

*2026-02-15T05:25:53Z by Showboat 0.5.0*

This document demonstrates every important trigger behavior in sqlite-chronicle and provides executable proof that each works correctly. Every code block below is reproducible — run `showboat verify examples/trigger-demos.md` to confirm.

## 1. Basic chronicle tracking

enable_chronicle creates a `_chronicle_<table>` table plus four triggers (BEFORE INSERT, AFTER INSERT, AFTER UPDATE, AFTER DELETE). The chronicle table mirrors the primary key columns and adds metadata: `__added_ms`, `__updated_ms`, `__version`, and `__deleted`.

```python3

import sqlite3
from sqlite_chronicle import enable_chronicle

conn = sqlite3.connect(":memory:")
conn.execute("CREATE TABLE dogs(id INTEGER PRIMARY KEY, name TEXT, color TEXT)")
conn.execute("INSERT INTO dogs VALUES(1, 'Cleo', 'black')")
enable_chronicle(conn, "dogs")

# Show the triggers that were created
for r in conn.execute("SELECT name FROM sqlite_master WHERE type='trigger' ORDER BY name"):
    print(r[0])

print()

# Show the chronicle table schema
for r in conn.execute("PRAGMA table_info(_chronicle_dogs)"):
    print(f"{r[1]:15s} {r[2]}")

```

```output
chronicle_dogs_ad
chronicle_dogs_ai
chronicle_dogs_au
chronicle_dogs_bi

id              INTEGER
__added_ms      INTEGER
__updated_ms    INTEGER
__version       INTEGER
__deleted       INTEGER
```

The seeded row gets version 1. Subsequent inserts, updates, and deletes each bump the version monotonically.

```python3

import sqlite3
from sqlite_chronicle import enable_chronicle

conn = sqlite3.connect(":memory:")
conn.execute("CREATE TABLE dogs(id INTEGER PRIMARY KEY, name TEXT, color TEXT)")
conn.execute("INSERT INTO dogs VALUES(1, 'Cleo', 'black')")
enable_chronicle(conn, "dogs")

def show(label):
    rows = conn.execute(
        "SELECT id, __version, __deleted FROM _chronicle_dogs ORDER BY id"
    ).fetchall()
    print(f"{label}:")
    for r in rows:
        print(f"  id={r[0]}  version={r[1]}  deleted={r[2]}")

show("After seed")

conn.execute("INSERT INTO dogs VALUES(2, 'Pancakes', 'corgi')")
show("After INSERT id=2")

conn.execute("UPDATE dogs SET color='brown' WHERE id=1")
show("After UPDATE id=1")

conn.execute("DELETE FROM dogs WHERE id=2")
show("After DELETE id=2")

```

```output
After seed:
  id=1  version=1  deleted=0
After INSERT id=2:
  id=1  version=1  deleted=0
  id=2  version=2  deleted=0
After UPDATE id=1:
  id=1  version=3  deleted=0
  id=2  version=2  deleted=0
After DELETE id=2:
  id=1  version=3  deleted=0
  id=2  version=4  deleted=1
```

## 2. The conflict resolution propagation bug (background)

SQLite has a subtle documented behavior: when you run `INSERT OR REPLACE INTO t(...)`, the `OR REPLACE` conflict resolution strategy propagates into **all DML statements inside any triggers** that fire — even triggers on unrelated tables. This means `INSERT OR IGNORE` inside a trigger body silently becomes `INSERT OR REPLACE`.

This standalone demo proves the propagation happens, independent of sqlite-chronicle:

```python3

import sqlite3

conn = sqlite3.connect(":memory:")
conn.executescript("""
CREATE TABLE main_t(id INTEGER PRIMARY KEY, val TEXT);
CREATE TABLE side_t(id INTEGER PRIMARY KEY, info TEXT);
INSERT INTO side_t VALUES(1, 'original');

CREATE TRIGGER trg AFTER INSERT ON main_t FOR EACH ROW
BEGIN
    -- This says OR IGNORE, but gets silently overridden
    INSERT OR IGNORE INTO side_t(id, info) VALUES(1, 'from_trigger');
END;
""")

# Plain INSERT: OR IGNORE is respected
conn.execute("INSERT INTO main_t VALUES(1, 'hello')")
result = conn.execute("SELECT info FROM side_t WHERE id=1").fetchone()[0]
print(f"After plain INSERT  -> side_t.info = '{ result }'  (OR IGNORE respected)")

# Reset
conn.execute("UPDATE side_t SET info = 'original' WHERE id=1")

# INSERT OR REPLACE: OR IGNORE is overridden to OR REPLACE!
conn.execute("INSERT OR REPLACE INTO main_t VALUES(2, 'world')")
result = conn.execute("SELECT info FROM side_t WHERE id=1").fetchone()[0]
print(f"After INSERT OR REPLACE -> side_t.info = '{ result }'  (OR IGNORE overridden!)")

```

```output
After plain INSERT  -> side_t.info = 'original'  (OR IGNORE respected)
After INSERT OR REPLACE -> side_t.info = 'from_trigger'  (OR IGNORE overridden!)
```

The fix in sqlite-chronicle avoids this by using `INSERT INTO ... SELECT ... WHERE NOT EXISTS(...)` instead of `INSERT OR IGNORE`. Since there is no conflict clause to propagate, the trigger body behaves identically regardless of how the outer statement was invoked.

## 3. INSERT OR REPLACE with changed data

When `INSERT OR REPLACE` replaces an existing row with different values, the chronicle should bump the version exactly once, keep `__deleted = 0`, and preserve the original `__added_ms`.

The library achieves this with a BEFORE INSERT trigger that snapshots the old non-PK column values into a helper table, and an AFTER INSERT trigger that compares the snapshot against the new values to decide whether a version bump is needed.

```python3

import sqlite3, time
from sqlite_chronicle import enable_chronicle

conn = sqlite3.connect(":memory:")
conn.execute("PRAGMA recursive_triggers = ON")  # sqlite-utils enables this
conn.execute("CREATE TABLE dogs(id INTEGER PRIMARY KEY, name TEXT, color TEXT)")
conn.execute("INSERT INTO dogs VALUES(1, 'Cleo', 'black')")
enable_chronicle(conn, "dogs")

before = conn.execute(
    "SELECT __version, __deleted, __added_ms, __updated_ms FROM _chronicle_dogs WHERE id=1"
).fetchone()
print(f"Before:  version={before[0]}  deleted={before[1]}")

time.sleep(0.01)
conn.execute("INSERT OR REPLACE INTO dogs VALUES(1, 'Cleo', 'brown')")

after = conn.execute(
    "SELECT __version, __deleted, __added_ms, __updated_ms FROM _chronicle_dogs WHERE id=1"
).fetchone()
print(f"After:   version={after[0]}  deleted={after[1]}")
print(f"added_ms preserved: {before[2] == after[2]}")
print(f"updated_ms changed: {after[3] > before[3]}")

```

```output
Before:  version=1  deleted=0
After:   version=2  deleted=0
added_ms preserved: True
updated_ms changed: True
```

## 4. INSERT OR REPLACE with identical data (no-op)

When `INSERT OR REPLACE` is called with values identical to the existing row, the chronicle should **not** bump the version. The AFTER INSERT trigger compares the snapshot of old values against the new values and skips the version bump when they match.

```python3

import sqlite3
from sqlite_chronicle import enable_chronicle

conn = sqlite3.connect(":memory:")
conn.execute("PRAGMA recursive_triggers = ON")
conn.execute("CREATE TABLE dogs(id INTEGER PRIMARY KEY, name TEXT, color TEXT)")
conn.execute("INSERT INTO dogs VALUES(1, 'Cleo', 'black')")
enable_chronicle(conn, "dogs")

v1 = conn.execute("SELECT __version FROM _chronicle_dogs WHERE id=1").fetchone()[0]
print(f"Initial version: {v1}")

# Replace with identical values — three times
for i in range(3):
    conn.execute("INSERT OR REPLACE INTO dogs VALUES(1, 'Cleo', 'black')")
    v = conn.execute("SELECT __version FROM _chronicle_dogs WHERE id=1").fetchone()[0]
    print(f"After identical REPLACE #{i+1}: version={v}  (unchanged: {v == v1})")

```

```output
Initial version: 1
After identical REPLACE #1: version=1  (unchanged: True)
After identical REPLACE #2: version=1  (unchanged: True)
After identical REPLACE #3: version=1  (unchanged: True)
```

## 5. INSERT OR REPLACE on a new primary key

When the PK does not already exist, `INSERT OR REPLACE` behaves like a plain INSERT. No snapshot is created (the BEFORE INSERT trigger's WHEN clause doesn't match), and the AFTER INSERT trigger creates a fresh chronicle row via the `INSERT ... WHERE NOT EXISTS` path.

```python3

import sqlite3
from sqlite_chronicle import enable_chronicle

conn = sqlite3.connect(":memory:")
conn.execute("PRAGMA recursive_triggers = ON")
conn.execute("CREATE TABLE dogs(id INTEGER PRIMARY KEY, name TEXT, color TEXT)")
enable_chronicle(conn, "dogs")

conn.execute("INSERT OR REPLACE INTO dogs VALUES(1, 'Cleo', 'black')")
conn.execute("INSERT OR REPLACE INTO dogs VALUES(2, 'Pancakes', 'corgi')")

for r in conn.execute("SELECT id, __version, __deleted FROM _chronicle_dogs ORDER BY id"):
    print(f"id={r[0]}  version={r[1]}  deleted={r[2]}")

# Confirm no snapshot residue
snaps = conn.execute("SELECT count(*) FROM _chroniclesnapshots WHERE table_name = 'dogs'").fetchone()[0]
print(f"Snapshot rows remaining: {snaps}")

```

```output
id=1  version=1  deleted=0
id=2  version=2  deleted=0
Snapshot rows remaining: 0
```

## 6. Re-insert after DELETE via INSERT OR REPLACE

When a row is deleted and then re-inserted (with either INSERT or INSERT OR REPLACE), the chronicle row should flip from `__deleted = 1` back to `__deleted = 0`, bump the version, and reset `__added_ms` to the time of re-insertion. This treats an "undelete" as a fresh addition rather than a continuation of the original row's history.

```python3

import sqlite3, time
from sqlite_chronicle import enable_chronicle

conn = sqlite3.connect(":memory:")
conn.execute("PRAGMA recursive_triggers = ON")
conn.execute("CREATE TABLE dogs(id INTEGER PRIMARY KEY, name TEXT, color TEXT)")
conn.execute("INSERT INTO dogs VALUES(1, 'Cleo', 'black')")
enable_chronicle(conn, "dogs")

def show():
    r = conn.execute(
        "SELECT __version, __deleted, __added_ms, __updated_ms FROM _chronicle_dogs WHERE id=1"
    ).fetchone()
    return r

r = show()
print(f"After insert:                  version={r[0]}  deleted={r[1]}")
original_added_ms = r[2]

conn.execute("DELETE FROM dogs WHERE id=1")
r = show()
print(f"After delete:                  version={r[0]}  deleted={r[1]}")

time.sleep(0.01)
conn.execute("INSERT OR REPLACE INTO dogs VALUES(1, 'Cleo', 'brown')")
r = show()
print(f"After re-insert via REPLACE:   version={r[0]}  deleted={r[1]}")
print(f"added_ms was reset:            {r[2] > original_added_ms}")
print(f"added_ms matches updated_ms:   {r[2] == r[3]}")

# Verify the row is really back in the main table
actual = conn.execute("SELECT name, color FROM dogs WHERE id=1").fetchone()
print(f"Main table row:                name={actual[0]!r}, color={actual[1]!r}")

```

```output
After insert:                  version=1  deleted=0
After delete:                  version=2  deleted=1
After re-insert via REPLACE:   version=3  deleted=0
added_ms was reset:            True
added_ms matches updated_ms:   True
Main table row:                name='Cleo', color='brown'
```

## 7. Compound primary keys

All trigger logic generalizes to tables with multi-column primary keys. The snapshot key uses `json_array()` to combine the PK columns into a single TEXT key for the snapshot table.

```python3

import sqlite3
from sqlite_chronicle import enable_chronicle

conn = sqlite3.connect(":memory:")
conn.execute("PRAGMA recursive_triggers = ON")
conn.execute("CREATE TABLE pairs(a TEXT, b INTEGER, val TEXT, PRIMARY KEY(a, b))")
conn.execute("INSERT INTO pairs VALUES('x', 1, 'hello')")
enable_chronicle(conn, "pairs")

def show():
    rows = conn.execute(
        "SELECT a, b, __version, __deleted FROM _chronicle_pairs ORDER BY a, b"
    ).fetchall()
    for r in rows:
        print(f"  a={r[0]!r}  b={r[1]}  version={r[2]}  deleted={r[3]}")

print("After seed:")
show()

conn.execute("INSERT OR REPLACE INTO pairs VALUES('x', 1, 'world')")
print("After REPLACE (changed):")
show()

conn.execute("INSERT OR REPLACE INTO pairs VALUES('x', 1, 'world')")
print("After REPLACE (identical):")
show()

conn.execute("INSERT OR REPLACE INTO pairs VALUES('y', 2, 'new')")
print("After REPLACE (new PK):")
show()

```

```output
After seed:
  a='x'  b=1  version=1  deleted=0
After REPLACE (changed):
  a='x'  b=1  version=2  deleted=0
After REPLACE (identical):
  a='x'  b=1  version=2  deleted=0
After REPLACE (new PK):
  a='x'  b=1  version=2  deleted=0
  a='y'  b=2  version=3  deleted=0
```

## 8. UPSERT (INSERT ... ON CONFLICT DO UPDATE)

The standard UPSERT pattern continues to work correctly. An UPSERT that changes data bumps the version via the AFTER UPDATE trigger, while one that writes identical values is a no-op thanks to the WHEN clause.

```python3

import sqlite3
from sqlite_chronicle import enable_chronicle

conn = sqlite3.connect(":memory:")
conn.execute("PRAGMA recursive_triggers = ON")
conn.execute("CREATE TABLE dogs(id INTEGER PRIMARY KEY, name TEXT, color TEXT)")
enable_chronicle(conn, "dogs")

# UPSERT as insert (new row)
conn.execute("""
    INSERT INTO dogs(id, name, color) VALUES(1, 'Cleo', 'black')
    ON CONFLICT(id) DO UPDATE SET name=excluded.name, color=excluded.color
""")
v = conn.execute("SELECT __version FROM _chronicle_dogs WHERE id=1").fetchone()[0]
print(f"UPSERT (new row):       version={v}")

# UPSERT that changes data
conn.execute("""
    INSERT INTO dogs(id, name, color) VALUES(1, 'Cleo', 'brown')
    ON CONFLICT(id) DO UPDATE SET name=excluded.name, color=excluded.color
""")
v = conn.execute("SELECT __version FROM _chronicle_dogs WHERE id=1").fetchone()[0]
print(f"UPSERT (changed):       version={v}")

# UPSERT with identical data — no-op
conn.execute("""
    INSERT INTO dogs(id, name, color) VALUES(1, 'Cleo', 'brown')
    ON CONFLICT(id) DO UPDATE SET name=excluded.name, color=excluded.color
""")
v = conn.execute("SELECT __version FROM _chronicle_dogs WHERE id=1").fetchone()[0]
print(f"UPSERT (identical):     version={v}  (unchanged)")

```

```output
UPSERT (new row):       version=1
UPSERT (changed):       version=2
UPSERT (identical):     version=2  (unchanged)
```

## 9. Works with recursive_triggers ON and OFF

`sqlite-utils` enables `PRAGMA recursive_triggers = ON`, which changes SQLite's behavior during INSERT OR REPLACE: the implicit DELETE fires DELETE triggers. The AFTER DELETE trigger's `WHEN NOT EXISTS(... snapshot ...)` guard prevents it from incorrectly marking a row as deleted during a REPLACE operation.

This demo proves identical results under both settings:

```python3

import sqlite3, time
from sqlite_chronicle import enable_chronicle

def run_scenario(label, recursive):
    conn = sqlite3.connect(":memory:")
    conn.execute(f"PRAGMA recursive_triggers = {'ON' if recursive else 'OFF'}")
    conn.execute("CREATE TABLE dogs(id INTEGER PRIMARY KEY, name TEXT, color TEXT)")
    conn.execute("INSERT INTO dogs VALUES(1, 'Cleo', 'black')")
    enable_chronicle(conn, "dogs")

    # REPLACE with change
    time.sleep(0.005)
    conn.execute("INSERT OR REPLACE INTO dogs VALUES(1, 'Cleo', 'brown')")
    r1 = conn.execute("SELECT __version, __deleted FROM _chronicle_dogs WHERE id=1").fetchone()

    # REPLACE with no change
    conn.execute("INSERT OR REPLACE INTO dogs VALUES(1, 'Cleo', 'brown')")
    r2 = conn.execute("SELECT __version, __deleted FROM _chronicle_dogs WHERE id=1").fetchone()

    # DELETE then re-insert
    conn.execute("DELETE FROM dogs WHERE id=1")
    conn.execute("INSERT OR REPLACE INTO dogs VALUES(1, 'Cleo', 'white')")
    r3 = conn.execute("SELECT __version, __deleted FROM _chronicle_dogs WHERE id=1").fetchone()

    print(f"{label}:")
    print(f"  REPLACE changed:   version={r1[0]}  deleted={r1[1]}")
    print(f"  REPLACE identical: version={r2[0]}  deleted={r2[1]}")
    print(f"  Delete+re-insert:  version={r3[0]}  deleted={r3[1]}")

run_scenario("recursive_triggers=OFF", False)
run_scenario("recursive_triggers=ON ", True)

```

```output
recursive_triggers=OFF:
  REPLACE changed:   version=2  deleted=0
  REPLACE identical: version=2  deleted=0
  Delete+re-insert:  version=4  deleted=0
recursive_triggers=ON :
  REPLACE changed:   version=2  deleted=0
  REPLACE identical: version=2  deleted=0
  Delete+re-insert:  version=4  deleted=0
```

## 10. UPDATE no-op detection

The AFTER UPDATE trigger uses a `WHEN` clause that compares every non-PK column with `OLD.col IS NOT NEW.col`. If nothing changed, the trigger doesn't fire and the version stays put.

```python3

import sqlite3
from sqlite_chronicle import enable_chronicle

conn = sqlite3.connect(":memory:")
conn.execute("PRAGMA recursive_triggers = ON")
conn.execute("CREATE TABLE dogs(id INTEGER PRIMARY KEY, name TEXT, color TEXT)")
conn.execute("INSERT INTO dogs VALUES(1, 'Cleo', 'black')")
enable_chronicle(conn, "dogs")

v = conn.execute("SELECT __version FROM _chronicle_dogs WHERE id=1").fetchone()[0]
print(f"Initial:                version={v}")

conn.execute("UPDATE dogs SET color='brown' WHERE id=1")
v = conn.execute("SELECT __version FROM _chronicle_dogs WHERE id=1").fetchone()[0]
print(f"After real change:      version={v}")

conn.execute("UPDATE dogs SET color='brown' WHERE id=1")
v = conn.execute("SELECT __version FROM _chronicle_dogs WHERE id=1").fetchone()[0]
print(f"After identical UPDATE: version={v}  (unchanged)")

# Also handles NULL values correctly
conn.execute("UPDATE dogs SET color=NULL WHERE id=1")
v = conn.execute("SELECT __version FROM _chronicle_dogs WHERE id=1").fetchone()[0]
print(f"After SET color=NULL:   version={v}")

conn.execute("UPDATE dogs SET color=NULL WHERE id=1")
v = conn.execute("SELECT __version FROM _chronicle_dogs WHERE id=1").fetchone()[0]
print(f"After same NULL:        version={v}  (unchanged)")

```

```output
Initial:                version=1
After real change:      version=2
After identical UPDATE: version=2  (unchanged)
After SET color=NULL:   version=3
After same NULL:        version=3  (unchanged)
```

## 11. updates_since() generator

The `updates_since()` function yields `Change` objects for all chronicle entries with a version greater than the supplied cursor. This allows consumers to poll for incremental changes.

```python3

import sqlite3
from sqlite_chronicle import enable_chronicle, updates_since

conn = sqlite3.connect(":memory:")
conn.execute("PRAGMA recursive_triggers = ON")
conn.execute("CREATE TABLE dogs(id INTEGER PRIMARY KEY, name TEXT, color TEXT)")
conn.execute("INSERT INTO dogs VALUES(1, 'Cleo', 'black')")
conn.execute("INSERT INTO dogs VALUES(2, 'Pancakes', 'corgi')")
enable_chronicle(conn, "dogs")

# Get all changes (since version 0)
print("All changes:")
cursor = 0
for change in updates_since(conn, "dogs", since=cursor):
    print(f"  pks={change.pks}  version={change.version}  deleted={change.deleted}  row={change.row}")
    cursor = change.version

# Make some modifications
conn.execute("UPDATE dogs SET color='brown' WHERE id=1")
conn.execute("DELETE FROM dogs WHERE id=2")

# Get only the new changes
print(f"Changes since version {cursor}:")
for change in updates_since(conn, "dogs", since=cursor):
    print(f"  pks={change.pks}  version={change.version}  deleted={change.deleted}  row={change.row}")

```

```output
All changes:
  pks=(1,)  version=1  deleted=False  row={'id': 1, 'name': 'Cleo', 'color': 'black'}
  pks=(2,)  version=2  deleted=False  row={'id': 2, 'name': 'Pancakes', 'color': 'corgi'}
Changes since version 2:
  pks=(1,)  version=3  deleted=False  row={'id': 1, 'name': 'Cleo', 'color': 'brown'}
  pks=(2,)  version=4  deleted=True  row={'id': 2, 'name': None, 'color': None}
```

## 12. Snapshot table is always clean

The `_chroniclesnapshots` table is a shared transient helper. It is populated by the BEFORE INSERT trigger and cleaned up within the same AFTER INSERT trigger. After any operation the snapshot rows for each table should always be empty.

```python3

import sqlite3
from sqlite_chronicle import enable_chronicle

conn = sqlite3.connect(":memory:")
conn.execute("PRAGMA recursive_triggers = ON")
conn.execute("CREATE TABLE dogs(id INTEGER PRIMARY KEY, name TEXT, color TEXT)")
conn.execute("INSERT INTO dogs VALUES(1, 'Cleo', 'black')")
enable_chronicle(conn, "dogs")

def snap_count():
    return conn.execute("SELECT count(*) FROM _chroniclesnapshots WHERE table_name = 'dogs'").fetchone()[0]

ops = [
    ("INSERT",          "INSERT INTO dogs VALUES(2, 'Rex', 'brown')"),
    ("INSERT OR REPLACE (new)",     "INSERT OR REPLACE INTO dogs VALUES(3, 'Luna', 'white')"),
    ("INSERT OR REPLACE (change)",  "INSERT OR REPLACE INTO dogs VALUES(1, 'Cleo', 'red')"),
    ("INSERT OR REPLACE (no-op)",   "INSERT OR REPLACE INTO dogs VALUES(1, 'Cleo', 'red')"),
    ("UPDATE",          "UPDATE dogs SET color='blue' WHERE id=1"),
    ("DELETE",          "DELETE FROM dogs WHERE id=2"),
    ("Re-insert",       "INSERT INTO dogs VALUES(2, 'Rex', 'brown')"),
]

for label, sql in ops:
    conn.execute(sql)
    print(f"After {label:35s} -> snapshot rows: {snap_count()}")

```

```output
After INSERT                              -> snapshot rows: 0
After INSERT OR REPLACE (new)             -> snapshot rows: 0
After INSERT OR REPLACE (change)          -> snapshot rows: 0
After INSERT OR REPLACE (no-op)           -> snapshot rows: 0
After UPDATE                              -> snapshot rows: 0
After DELETE                              -> snapshot rows: 0
After Re-insert                           -> snapshot rows: 0
```

## 13. Full test suite

All tests pass, including the INSERT OR REPLACE tests for blob columns, NULL values, mixed types, and undelete timestamp reset:

```bash
uv run pytest tests/ -v 2>&1 | sed "s/in [0-9.]*s/in Xs/"
```

```output
============================= test session starts ==============================
platform linux -- Python 3.11.14, pytest-9.0.2, pluggy-1.6.0 -- /home/user/sqlite-chronicle/.venv/bin/python
cachedir: .pytest_cache
rootdir: /home/user/sqlite-chronicle
configfile: pyproject.toml
collecting ... collected 51 items

tests/test_cli.py::test_cli_version PASSED                               [  1%]
tests/test_cli.py::test_cli_main_success PASSED                          [  3%]
tests/test_cli.py::test_cli_main_error_invalid_db PASSED                 [  5%]
tests/test_cli.py::test_cli_main_bad_table PASSED                        [  7%]
tests/test_cli.py::test_cli_disable_success PASSED                       [  9%]
tests/test_cli.py::test_cli_disable_no_chronicle PASSED                  [ 11%]
tests/test_cli.py::test_cli_disable_multiple_tables PASSED               [ 13%]
tests/test_disable_chronicle.py::test_disable_chronicle_removes_table_and_triggers PASSED [ 15%]
tests/test_disable_chronicle.py::test_disable_chronicle_returns_false_if_not_enabled PASSED [ 17%]
tests/test_disable_chronicle.py::test_disable_chronicle_idempotent PASSED [ 19%]
tests/test_disable_chronicle.py::test_disable_chronicle_original_table_still_works PASSED [ 21%]
tests/test_disable_chronicle.py::test_disable_chronicle_can_reenable PASSED [ 23%]
tests/test_disable_chronicle.py::test_disable_chronicle_special_table_names[dogs] PASSED [ 25%]
tests/test_disable_chronicle.py::test_disable_chronicle_special_table_names[dogs and stuff] PASSED [ 27%]
tests/test_disable_chronicle.py::test_disable_chronicle_special_table_names[weird.table.name] PASSED [ 29%]
tests/test_helpers.py::test_is_chronicle_enabled_true PASSED             [ 31%]
tests/test_helpers.py::test_is_chronicle_enabled_false PASSED            [ 33%]
tests/test_helpers.py::test_is_chronicle_enabled_after_disable PASSED    [ 35%]
tests/test_helpers.py::test_is_chronicle_enabled_nonexistent_table PASSED [ 37%]
tests/test_helpers.py::test_list_chronicled_tables_empty PASSED          [ 39%]
tests/test_helpers.py::test_list_chronicled_tables_single PASSED         [ 41%]
tests/test_helpers.py::test_list_chronicled_tables_multiple PASSED       [ 43%]
tests/test_helpers.py::test_list_chronicled_tables_after_disable PASSED  [ 45%]
tests/test_helpers.py::test_list_chronicled_tables_special_names PASSED  [ 47%]
tests/test_snapshot_table_name_conflict.py::test_dogs_and_snapshot_dogs_conflict_dogs_first PASSED [ 49%]
tests/test_snapshot_table_name_conflict.py::test_dogs_and_snapshot_dogs_conflict_snapshot_dogs_first PASSED [ 50%]
tests/test_snapshot_table_name_conflict.py::test_table_named_snapshots PASSED [ 52%]
tests/test_sqlite_chronicle.py::test_enable_chronicle[pks0-dogs] PASSED  [ 54%]
tests/test_sqlite_chronicle.py::test_enable_chronicle[pks0-dogs and stuff] PASSED [ 56%]
tests/test_sqlite_chronicle.py::test_enable_chronicle[pks0-weird.table.name] PASSED [ 58%]
tests/test_sqlite_chronicle.py::test_enable_chronicle[pks1-dogs] PASSED  [ 60%]
tests/test_sqlite_chronicle.py::test_enable_chronicle[pks1-dogs and stuff] PASSED [ 62%]
tests/test_sqlite_chronicle.py::test_enable_chronicle[pks1-weird.table.name] PASSED [ 64%]
tests/test_sqlite_chronicle.py::test_enable_chronicle_alternative_primary_keys[pks0] PASSED [ 66%]
tests/test_sqlite_chronicle.py::test_enable_chronicle_alternative_primary_keys[pks1] PASSED [ 68%]
tests/test_sqlite_chronicle.py::test_upsert PASSED                       [ 70%]
tests/test_sqlite_chronicle.py::test_insert_or_replace PASSED            [ 72%]
tests/test_sqlite_chronicle.py::test_insert_or_replace_compound_pk PASSED [ 74%]
tests/test_sqlite_chronicle.py::test_insert_or_replace_after_delete PASSED [ 76%]
tests/test_sqlite_chronicle.py::test_undelete_resets_added_ms PASSED     [ 78%]
tests/test_sqlite_chronicle.py::test_insert_or_replace_blob_column PASSED [ 80%]
tests/test_sqlite_chronicle.py::test_insert_or_replace_null_values PASSED [ 82%]
tests/test_sqlite_chronicle.py::test_insert_or_replace_mixed_types PASSED [ 84%]
tests/test_updates_since.py::test_updates_since PASSED                   [ 86%]
tests/test_updates_since.py::test_updates_since_more_rows_than_batch_size_when_enabled PASSED [ 88%]
tests/test_updates_since.py::test_updates_since_more_rows_than_batch_size_in_an_update PASSED [ 90%]
tests/test_updates_since.py::test_updates_since_special_table_names[dogs and stuff] PASSED [ 92%]
tests/test_updates_since.py::test_updates_since_special_table_names[weird.table.name] PASSED [ 94%]
tests/test_upgrade.py::test_upgrade_drops_old_and_installs_new PASSED    [ 96%]
tests/test_upgrade.py::test_idempotent_and_noop_on_nonexistent PASSED    [ 98%]
tests/test_upgrade.py::test_noop_if_already_new_schema PASSED            [100%]

============================== 51 passed in Xs ==============================
```
