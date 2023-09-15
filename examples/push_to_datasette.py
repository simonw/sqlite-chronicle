import argparse
import httpx
import pathlib
from pprint import pprint
import sqlite_utils
from sqlite_chronicle import updates_since


def push_database(db_path, url, key=None, since=None):
    headers = {}
    if key:
        headers["Authorization"] = f"Bearer {key}"

    db = sqlite_utils.Database(db_path)
    # Check for tables with chronicle enabled
    chronicle_tables = []
    for table in db.table_names():
        # Does a _chronicle_{tablename} table exist?
        if db[f"_chronicle_{table}"].exists():
            chronicle_tables.append(table)
    if not chronicle_tables:
        raise ValueError(f"No tables with chronicle enabled found in {db_path}")

    # Did we record our last sync?
    last_sync_table = db["__datasette_last_sync"]
    if not last_sync_table.exists():
        # Create it
        with db.conn:
            last_sync_table.insert({"url": url, "last_sync": 0}, pk="url")

    try:
        last_sync = last_sync_table.get(url)["last_sync"]
    except sqlite_utils.db.NotFoundError:
        last_sync = 0

    max_version = last_sync

    for table in chronicle_tables:
        changes = list(
            updates_since(db.conn, table, since=last_sync if since is None else since)
        )
        print(table)
        pprint(changes)
        for change in changes:
            if change.version >= max_version:
                max_version = change.version

        if changes:
            create_url = f"{url}/-/create"
            json_body = {
                "table": "creatures",
                "rows": [change.row for change in changes],
                "pk": "id",
                "replace": True,
            }
            print(json_body)

            response = httpx.post(create_url, json=json_body, headers=headers)
            response.raise_for_status()

            with db.conn:
                last_sync_table.upsert({"url": url, "last_sync": max_version}, pk="url")

        else:
            print("No changes found for table", table)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Push changes to a SQLite database to a Datasette instance"
    )
    parser.add_argument("db_path", type=str, help="The path to the SQLite database")
    parser.add_argument("url", type=str, help="The URL of the Datasette database")
    parser.add_argument(
        "--key",
        dest="key",
        type=str,
        default=None,
        help="Optional API token for Datasette",
    )
    parser.add_argument(
        "--since",
        dest="since",
        type=int,
        default=None,
        help="Since version to start from",
    )

    args = parser.parse_args()

    if not pathlib.Path(args.db_path).exists():
        raise ValueError(f"Database file {args.db_path} does not exist")

    push_database(args.db_path, args.url, args.key, args.since)
