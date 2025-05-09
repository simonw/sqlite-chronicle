import sqlite3
from sqlite_chronicle import cli_main


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
