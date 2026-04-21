# JavaDatabase

A relational database engine built from scratch in Java, implementing storage, indexing, and query execution without any database libraries.

## Why I built this

<!-- TODO: Write this section in your own voice. 2-3 sentences about why you built this.
     What made you curious about database internals?
     What did you want to understand that you couldn't learn from just using PostgreSQL/MySQL?
     Delete this comment and replace with your own words. -->

I wanted to understand what actually happens between typing a SQL query and bytes hitting disk. Every backend developer uses databases daily, but the internals — how pages are laid out, how a B+ tree splits, how a query plan turns into disk reads — are a black box unless you build one yourself.

## Architecture

```
                         SQL string
                             |
                         [ Parser ]
                             |
                      ParsedStatement
                             |
                      [ QueryPlan ]
                             |
                        [ Executor ]
                        /    |    \
                       /     |     \
               [ Catalog ]   |   [ BPlusTree ]
                    |        |        |
               [ Schema ]   |    index lookup
                    |        |        |
                    +--------+--------+
                             |
                        [ HeapFile ]
                             |
                       [ DiskManager ]
                             |
                     [ Page (4096 B) ]
                             |
                        RandomAccessFile
                             |
                           disk
```

## What each layer does

**Storage** (`storage/`) — The foundation. `Page` is a 4096-byte slotted page where slots grow left-to-right from byte 4 and row data grows right-to-left from the end. Each row is stored with a 4-byte length prefix. Deletes write a `-1` tombstone into the slot rather than compacting — this keeps RecordIDs stable. `DiskManager` handles page-aligned I/O against a `RandomAccessFile`, and `HeapFile` sits on top as an unordered collection of pages that scans for free space on insert.

**Buffer Pool** (`buffer/`) — An LRU cache between the query layer and disk. Tracks pin counts per page so in-use pages are never evicted, and a dirty bit so modified pages get flushed on eviction. Built on `LinkedHashMap` with access-order iteration — the JDK basically gives you LRU for free.

**Record** (`record/`) — `Schema` defines a table's columns with types (`INT`, `VARCHAR`, `BOOLEAN`) and fixed byte sizes. `Record` serializes column values into a flat byte array — ints as 4-byte big-endian, booleans as a single byte, and VARCHARs null-padded to the column's declared size. Deserialization trims trailing null bytes from strings.

**Index** (`index/`) — A B+ tree implementation with configurable order. Leaf nodes store `(key, RecordID)` pairs and are linked left-to-right for range scans. When a leaf overflows, it splits at the midpoint and pushes a key up to the parent. Internal node splits propagate upward the same way, growing the tree from the bottom up.

**Catalog** (`catalog/`) — An in-memory registry that maps table names to their schema, heap file, and optional B+ tree index. This is the glue between the query layer ("give me the `users` table") and the storage layer ("here's the HeapFile at `/tmp/users.db`").

**Query** (`query/`) — A minimal SQL pipeline. `Parser` tokenizes SQL strings into a `ParsedStatement`, `QueryPlan.fromStatement()` maps that to an execution plan (sequential scan, insert, or delete), and `Executor` runs the plan against the catalog. The executor is index-aware — if a table has a B+ tree and the query has a WHERE clause on the indexed column, it does a tree lookup instead of a full scan.

## How to run it

**Prerequisites:** Java 21+, Maven 3.8+

```bash
# Build
mvn compile

# Run the CLI demo (inserts, selects, deletes against a temp database)
mvn exec:java -Dexec.mainClass="com.yourname.db.Main"

# Run all tests (76 tests including performance benchmarks)
mvn test

# Run a specific test class
mvn -Dtest=EndToEndTest test

# Run a specific test method
mvn -Dtest=EndToEndTest#bulkInsert10000Records test
```

## Test suite

76 tests across 11 test classes:

| Layer | Tests | Highlights |
|-------|-------|------------|
| Page | 2 | Space checks on empty and full pages |
| DiskManager | 6 | Write/read roundtrip, persistence across reopen |
| HeapFile | 7 | Multi-page spanning, insert/delete/scan |
| BufferPool | 6 | LRU eviction, dirty page flush, pin semantics |
| Schema | 5 | Offset calculation, column lookup |
| Record | 3 | Serialize/deserialize roundtrip, VARCHAR overflow |
| BPlusTree | 9 | Splits, out-of-order insertion, linked list order, 1K-key perf |
| Catalog | 11 | Table/index CRUD, error cases |
| Parser | 7 | INSERT/SELECT/DELETE parsing, WHERE clauses |
| Executor | 10 | Index vs. seq scan, delete with/without WHERE |
| **End-to-End** | **10** | **Full SQL pipeline, 10K bulk insert, bulk delete, multi-page stress** |

Performance on 10,000 records (single-threaded, no buffer pool):
- Bulk insert: ~420ms
- Full table scan: ~13ms
- Index lookup: ~2 microseconds average

## Known limitations

<!-- TODO: Rewrite these in your own voice. The technical facts are correct,
     but the phrasing should sound like you explaining trade-offs you consciously made.
     Delete this comment when done. -->

- **No concurrency.** Single-threaded only — no locking, no MVCC, no transaction isolation. The buffer pool tracks pin counts but nothing prevents two threads from modifying the same page.
- **B+ tree doesn't support delete.** When you `DELETE` a row, the heap file tombstones it but the index entry remains. This means the tree grows monotonically and stale entries can cause phantom lookups.
- **WHERE is limited to single-column equality.** `WHERE id = 5` works. `WHERE id > 5`, `WHERE id = 5 AND name = 'Alice'`, and anything with `OR` / `LIKE` / `IN` do not.
- **No UPDATE.** Only INSERT, SELECT, and DELETE are implemented.
- **Catalog is in-memory only.** Table definitions and index state are lost when the process exits. The demo uses temp files that are cleaned up by the OS.
- **No joins, subqueries, or aggregations.** `SELECT * FROM users` is the extent of it — no `JOIN`, `GROUP BY`, `COUNT(*)`, or nested queries.
- **Fixed-size rows only.** VARCHAR columns are padded to their declared size (e.g., `VARCHAR(20)` always uses 20 bytes). No overflow pages, no variable-length storage.
- **Buffer pool isn't wired in.** The `BufferPool` class exists and is tested, but `HeapFile` talks directly to `DiskManager`. Integrating the buffer pool would reduce disk I/O significantly.

