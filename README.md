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

## enable_chronicle(conn, table_name)

This module provides a single function: `sqlite_chronicle.enable_chronicle(conn, table_name)`, which does the following:

1. Checks if a `_chronicle_{table_name}` table exists alreday. If so, it does nothing. Otherwise...
2. Creates that table, with the same primary key columns as the original table plus integer columns `updated_ms` and `deleted`
3. Creates a new row in the chronicle table corresponding to every row in the original table, setting `updated_ms` to the current timestamp in milliseconds
4. Sets up three triggers on the table:
  - An after insert trigger, which creates a new row in the chronicle table and sets `updated_ms` to the current time
  - An after update trigger, which updates that timestamp and also updates any primary keys if they have changed (likely extremely rare)
  - An after delete trigger, which updates that timestamp and places a `1` in the `deleted` column

The function will raise a `sqlite_chronicle.ChronicleError` exception if the table does not have a single or compound primary key.

The end result is a chronicle table that looks something like this:

|  id |    updated_ms | deleted |
|-----|---------------|---------|
|  47 | 1694408890954 |       0 |
|  48 | 1694408874863 |       1 |
|   1 | 1694408825192 |       0 |
|   2 | 1694408825192 |       0 |
|   3 | 1694408825192 |       0 |

## Applications

Chronicle tables can be used to efficiently answer the question "what rows have been inserted, updated or deleted since I last checked".

This has numerous potential applications, including:

- Synchronization and replication: other databases can "subscribe" to tables, keeping track of when they last refreshed their copy and requesting just rows that changed since the last time - and deleting rows that have been marked as deleted.
- Indexing: if you need to update an Elasticsearch index or a vector database embeddings index or similar you can run against just the records that changed since your last run - see also [The denormalized query engine design pattern](https://2017.djangocon.us/talks/the-denormalized-query-engine-design-pattern/)
- Enrichments: [datasette-enrichments](https://github.com/datasette/datasette-enrichments) needs to to persist something that says "every address column should be geocoded" - then have an enrichment that runs every X seconds and looks for newly inserted or updated rows and enriches just those.
- Showing people what has changed since their last visit - "52 rows have been updated and 16 deleted since yesterday" kind of thing.
