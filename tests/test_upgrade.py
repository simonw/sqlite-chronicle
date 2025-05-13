import sqlite3
import pytest
from sqlite_chronicle import upgrade_chronicle


@pytest.fixture
def legacy_db():
    # Build a tiny in-memory DB with the *old* chronicle schema
    conn = sqlite3.connect(":memory:")
    c = conn.cursor()

    # main table
    c.execute("CREATE TABLE dogs (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")

    # legacy chronicle table + index + old triggers
    c.executescript(
        """
    CREATE TABLE "_chronicle_dogs" (
      id         INTEGER,
      added_ms   INTEGER,
      updated_ms INTEGER,
      version    INTEGER DEFAULT 0,
      deleted    INTEGER DEFAULT 0,
      PRIMARY KEY(id)
    );
    CREATE INDEX "_chronicle_dogs_version"
      ON "_chronicle_dogs"(version);

    CREATE TRIGGER "_chronicle_dogs_ai"
      AFTER INSERT ON "dogs"
    FOR EACH ROW BEGIN
      INSERT INTO "_chronicle_dogs"(id,added_ms,updated_ms,version,deleted)
      VALUES(NEW.id,111,111,1,0);
    END;

    CREATE TRIGGER "_chronicle_dogs_au"
      AFTER UPDATE ON "dogs"
    FOR EACH ROW BEGIN
      UPDATE "_chronicle_dogs"
        SET updated_ms=222, version=2
      WHERE id=OLD.id;
    END;

    CREATE TRIGGER "_chronicle_dogs_ad"
      AFTER DELETE ON "dogs"
    FOR EACH ROW BEGIN
      UPDATE "_chronicle_dogs"
        SET updated_ms=333, version=3, deleted=1
      WHERE id=OLD.id;
    END;

    INSERT INTO dogs(id,name,age) VALUES(1,'Fido',5);
    """
    )
    conn.commit()
    return conn


def all_triggers(conn):
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='trigger'")
    return {r[0] for r in cur.fetchall()}


def test_upgrade_drops_old_and_installs_new(legacy_db):
    before = all_triggers(legacy_db)
    # old trigger names must be present
    assert "_chronicle_dogs_ai" in before
    assert "_chronicle_dogs_au" in before
    assert "_chronicle_dogs_ad" in before

    # run the migration
    upgrade_chronicle(legacy_db, "dogs")

    after = all_triggers(legacy_db)
    # old ones gone
    for t in ("_chronicle_dogs_ai", "_chronicle_dogs_au", "_chronicle_dogs_ad"):
        assert t not in after
    # new ones present
    for t in ("chronicle_dogs_ai", "chronicle_dogs_au", "chronicle_dogs_ad"):
        assert t in after

    # also verify the table columns were renamed
    cur = legacy_db.execute('PRAGMA table_info("_chronicle_dogs")')
    cols = {r[1] for r in cur.fetchall()}
    assert "added_ms" not in cols
    assert "__added_ms" in cols

    # and that inserting into dogs still fires the new AI trigger:
    cur = legacy_db.cursor()
    cur.execute("INSERT INTO dogs(id,name,age) VALUES(2,'Rex',3)")
    legacy_db.commit()
    row = legacy_db.execute(
        'SELECT id, __version, __deleted FROM "_chronicle_dogs" WHERE id=2'
    ).fetchone()
    # __version should have advanced to 2, deleted still 0
    assert row == (2, 2, 0)


def test_idempotent_and_noop_on_nonexistent():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE foo(id INTEGER PRIMARY KEY)")
    # no _chronicle_foo → no errors, nothing added
    upgrade_chronicle(conn, "foo")
    names = {
        r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    assert names == {"foo"}


def test_noop_if_already_new_schema():
    conn = sqlite3.connect(":memory:")
    cur = conn.cursor()
    cur.execute("CREATE TABLE bar(id INTEGER PRIMARY KEY)")
    cur.executescript(
        """
    CREATE TABLE "_chronicle_bar"(
      id INTEGER,
      __added_ms INTEGER,
      __updated_ms INTEGER,
      __version INTEGER,
      __deleted INTEGER DEFAULT 0,
      PRIMARY KEY(id)
    );
    CREATE INDEX "_chronicle_bar__version_idx"
      ON "_chronicle_bar"(__version);
    """
    )
    conn.commit()

    # Should not error or change anything
    upgrade_chronicle(conn, "bar")
    trig = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='trigger'"
    ).fetchone()[0]
    assert trig == 0
    cols = {r[1] for r in conn.execute('PRAGMA table_info("_chronicle_bar")')}
    assert "__added_ms" in cols and "added_ms" not in cols
