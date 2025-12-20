import sqlite3
from sqlite_chronicle import cli_main, enable_chronicle


def test_cli_main_success(tmp_path, capsys):
    # Setup a simple DB file with a table with PRIMARY KEY
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
    conn.execute("INSERT INTO t1 (val) VALUES (?)", ("foo",))
    conn.commit()
    conn.close()

    # Enable chronicle on the table
    exit_code = cli_main([str(db_path), "t1"])
    captured = capsys.readouterr()
    assert exit_code == 0
    assert "chronicle enabled on table 't1'" in captured.out

    # Enabling chronicle again on the same table should be no-op (success)
    exit_code = cli_main([str(db_path), "t1"])
    captured = capsys.readouterr()
    assert exit_code == 0


def test_cli_main_error_invalid_db(capsys):
    # Test passing a non-existent DB path
    fake_path = "/nonexistent/path/to.db"
    exit_code = cli_main([fake_path, "t1"])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert f"ERROR: cannot open database '{fake_path}'" in captured.err


def test_cli_main_bad_table(tmp_path, capsys):
    # Create a table and then drop it to cause SQL error during processing
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
    conn.commit()
    # Drop t1 so enable_chronicle fails with SQL error
    conn.execute("DROP TABLE t1")
    conn.commit()
    conn.close()

    exit_code = cli_main([str(db_path), "t1"])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "ERROR: Table 't1' does not exist" in captured.err


def test_cli_disable_success(tmp_path, capsys):
    # Setup a DB with chronicle enabled
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
    conn.execute("INSERT INTO t1 (val) VALUES (?)", ("foo",))
    conn.commit()
    enable_chronicle(conn, "t1")
    conn.close()

    # Disable chronicle on the table
    exit_code = cli_main([str(db_path), "t1", "--disable"])
    captured = capsys.readouterr()
    assert exit_code == 0
    assert "chronicle disabled on table 't1'" in captured.out

    # Verify chronicle table is gone
    conn = sqlite3.connect(str(db_path))
    tables = [
        r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    ]
    assert "_chronicle_t1" not in tables
    conn.close()


def test_cli_disable_no_chronicle(tmp_path, capsys):
    # Setup a DB without chronicle
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
    conn.commit()
    conn.close()

    # Try to disable non-existent chronicle
    exit_code = cli_main([str(db_path), "t1", "--disable"])
    captured = capsys.readouterr()
    assert exit_code == 0
    assert "no chronicle found for table 't1'" in captured.out


def test_cli_disable_multiple_tables(tmp_path, capsys):
    # Setup a DB with multiple tables
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
    conn.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY, val TEXT)")
    conn.commit()
    enable_chronicle(conn, "t1")
    enable_chronicle(conn, "t2")
    conn.close()

    # Disable both
    exit_code = cli_main([str(db_path), "t1", "t2", "--disable"])
    captured = capsys.readouterr()
    assert exit_code == 0
    assert "chronicle disabled on table 't1'" in captured.out
    assert "chronicle disabled on table 't2'" in captured.out
