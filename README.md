# sqlite-chronicle

[![PyPI](https://img.shields.io/pypi/v/sqlite-chronicle.svg)](https://pypi.org/project/sqlite-chronicle/)
[![Changelog](https://img.shields.io/github/v/release/simonw/sqlite-chronicle?include_prereleases&label=changelog)](https://github.com/simonw/sqlite-chronicle/releases)
[![Tests](https://github.com/simonw/sqlite-chronicle/workflows/Test/badge.svg)](https://github.com/simonw/sqlite-chronicle/actions?query=workflow%3ATest)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/simonw/sqlite-chronicle/blob/main/LICENSE)

Use triggers to track when rows in a SQLite table were updated or deleted

## Installation

```bash
pip install sqlite-chronicle
```

## Command-line interface

You can enable chronicle for specific tables in a SQLite database using the command-line interface, passing in one or more table names:

```bash
python -m sqlite_chronicle database.db table_1 table_2
```

## Python API

This package exposes two Python functions for configuring and using chronicle tables:

### enable_chronicle(conn, table_name)

This module provides a function: `sqlite_chronicle.enable_chronicle(conn, table_name)`, which does the following:

1. Checks if a `_chronicle_{table_name}` table exists already. If so, it does nothing. Otherwise...
2. Creates that table, with the same primary key columns as the original table plus integer columns `__added_ms`, `__updated_ms`, `__version` and `__deleted`
3. Creates a new row in the chronicle table corresponding to every row in the original table, setting `__added_ms` and `__updated_ms` to the current timestamp in milliseconds, and `__version` column that starts at 1 and increments for each subsequent row
4. Sets up three triggers on the table:

  - An AFTER INSERT trigger, which creates a new row in the chronicle table, sets `__added_ms` and `__updated_ms` to the current time and sets the `__version` to one higher than the current maximum version for that table
  - An AFTER UPDATE trigger, which updates the `__updated_ms` timestamp and increments the `__version` - but only if at least one column in the row has changed
  - An AFTER DELETE trigger, which updates the `__updated_ms`, increments the `__version` and places a `1` in the `deleted` column

The function will raise a `sqlite_chronicle.ChronicleError` exception if the table does not exist or if it does not have a single or compound primary key, 

Note that the `__version` for a table is a globally incrementing number, so every time it is set it will be set to the current `max(__version)` + 1 for that entire table.

The end result is a chronicle table that looks something like this:

|  id |    __added_ms  | __updated_ms | __version | __deleted |
|-----|---------------|---------|--------|---------|
|  47 | 1694408890954 | 1694408890954 | 2 |      0 |
|  48 | 1694408874863 | 1694408874863 | 3 |      1 |
|   1 | 1694408825192 | 1694408825192 | 4 |      0 |
|   2 | 1694408825192 | 1694408825192 | 5 |      0 |
|   3 | 1694408825192 | 1694408825192 | 6 |      0 |

### updates_since(conn, table_name, since=None, batch_size=1000)

The `sqlite_chronicle.updates_since()` function returns a generator over a list of `Change` objects.

These objects represent changes that have occurred to rows in the table since the `since` version number, or since the beginning of time if `since` is not provided.

- `conn` is a SQLite connection object
- `table_name` is a string containing the name of the table to get changes for
- `since` is an optional integer version number - if not provided, all changes will be returned
- `batch_size` is an internal detail, controlling the number of rows that are returned from the database at a time. You should not need to change this as the function implements its own internal pagination.

Each `Change` returned from the generator looks something like this:

```python
Change(
    pks=(5,),
    added_ms=1701836971223,
    updated_ms=1701836971223,
    version=5,
    row={'id': 5, 'name': 'Simon'},
    deleted=False
)
```
A `Change` is a dataclass with the following properties:

- `pks` is a tuple of the primary key values for the row - this will be a tuple with a single item for normal primary keys, or multiple items for compound primary keys
- `added_ms` is the timestamp in milliseconds when the row was added
- `updated_ms` is the timestamp in milliseconds when the row was last updated
- `version` is the version number for the row - you can use this as a `since` value to get changes since that point
- `row` is a dictionary containing the current values for the row - these will be `None` if the row has been deleted (except for the primary keys)
- `deleted` is `0` if the row has not been deleted, or `1` if it has been deleted

Any time you call this you should track the last `version` number that you see, so you can pass it as the `since` value in future calls to get changes that occurred since that point.

Note that if a row had multiple updates in between calls to this function you will still only see one `Change` object for that row - the `updated_ms` and `version` will reflect the most recent update.

## Implementation notes

- If you run `INSERT OR REPLACE INTO ...` and update an existing record in a way that does not change any of the fields, this system will still treat that record as if it has been updated. Use `INSERT ... ON CONFLICT SET` upserts instead to avoid this problem.
- Updates to columns that are part of a primary key for the record is not currently supported.

## Potential applications

Chronicle tables can be used to efficiently answer the question "what rows have been inserted, updated or deleted since I last checked" - by looking at the `version` column which has an index to make it fast to answer that question.

This has numerous potential applications, including:

- Synchronization and replication: other databases can "subscribe" to tables, keeping track of when they last refreshed their copy and requesting just rows that changed since the last time - and deleting rows that have been marked as deleted.
- Indexing: if you need to update an Elasticsearch index or a vector database embeddings index or similar you can run against just the records that changed since your last run - see also [The denormalized query engine design pattern](https://2017.djangocon.us/talks/the-denormalized-query-engine-design-pattern/)
- Enrichments: [datasette-enrichments](https://github.com/datasette/datasette-enrichments) needs to to persist something that says "every address column should be geocoded" - then have an enrichment that runs every X seconds and looks for newly inserted or updated rows and enriches just those.
- Showing people what has changed since their last visit - "52 rows have been updated and 16 deleted since yesterday" kind of thing.
