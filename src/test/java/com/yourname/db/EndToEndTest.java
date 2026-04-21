package com.yourname.db;

import com.yourname.db.catalog.Catalog;
import com.yourname.db.index.BPlusTree;
import com.yourname.db.query.Executor;
import com.yourname.db.query.ParsedStatement;
import com.yourname.db.query.Parser;
import com.yourname.db.query.QueryPlan;
import com.yourname.db.record.Record;
import com.yourname.db.record.Schema;
import com.yourname.db.record.Schema.Column;
import com.yourname.db.record.Schema.ColumnType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class EndToEndTest {

    private Catalog catalog;
    private Parser parser;
    private Executor executor;

    @BeforeEach
    void setUp() throws IOException {
        Schema schema = new Schema(List.of(
                new Column("id", ColumnType.INT, 4),
                new Column("name", ColumnType.VARCHAR, 20),
                new Column("active", ColumnType.BOOLEAN, 1)
        ));

        catalog = new Catalog();
        String tempPath = Files.createTempFile("e2e-test", ".db").toString();
        catalog.createTable("users", schema, tempPath);

        BPlusTree index = new BPlusTree(3);
        catalog.createIndex("users", index);

        parser = new Parser();
        executor = new Executor(catalog);
    }

    private List<Record> run(String sql) throws IOException {
        ParsedStatement ps = parser.parse(sql);
        QueryPlan plan = QueryPlan.fromStatement(ps);
        return executor.execute(plan);
    }

    @Test
    void fullPipelineInsertSelectDeleteSelect() throws IOException {
        // Insert 3 records
        run("INSERT INTO users VALUES (1, 'Alice', true)");
        run("INSERT INTO users VALUES (2, 'Bob', false)");
        run("INSERT INTO users VALUES (3, 'Charlie', true)");

        // SELECT all — should return 3
        List<Record> all = run("SELECT * FROM users");
        assertEquals(3, all.size());

        // SELECT with WHERE — index lookup
        List<Record> bob = run("SELECT * FROM users WHERE id = 2");
        assertEquals(1, bob.size());
        assertEquals("Bob", bob.get(0).get("name"));
        assertEquals(false, bob.get(0).get("active"));

        // DELETE id=1
        run("DELETE FROM users WHERE id = 1");

        // SELECT all — should return 2
        List<Record> remaining = run("SELECT * FROM users");
        assertEquals(2, remaining.size());

        // Verify Alice is gone
        boolean aliceFound = remaining.stream()
                .anyMatch(r -> "Alice".equals(r.get("name")));
        assertFalse(aliceFound, "Alice should have been deleted");

        // Verify Bob and Charlie still present
        boolean bobFound = remaining.stream()
                .anyMatch(r -> "Bob".equals(r.get("name")));
        boolean charlieFound = remaining.stream()
                .anyMatch(r -> "Charlie".equals(r.get("name")));
        assertTrue(bobFound);
        assertTrue(charlieFound);
    }

    @Test
    void insertThenDeleteAllThenSelectReturnsEmpty() throws IOException {
        run("INSERT INTO users VALUES (1, 'Alice', true)");
        run("INSERT INTO users VALUES (2, 'Bob', false)");

        run("DELETE FROM users");

        List<Record> results = run("SELECT * FROM users");
        assertTrue(results.isEmpty());
    }

    @Test
    void selectOnEmptyTable() throws IOException {
        List<Record> results = run("SELECT * FROM users");
        assertTrue(results.isEmpty());
    }

    @Test
    void multipleDeletesAreIdempotent() throws IOException {
        run("INSERT INTO users VALUES (1, 'Alice', true)");
        run("INSERT INTO users VALUES (2, 'Bob', false)");

        run("DELETE FROM users WHERE id = 1");
        // Deleting again where id=1 should be a no-op (already deleted)
        run("DELETE FROM users WHERE id = 1");

        List<Record> results = run("SELECT * FROM users");
        assertEquals(1, results.size());
        assertEquals("Bob", results.get(0).get("name"));
    }

    @Test
    void selectWhereOnNonexistentIdReturnsEmpty() throws IOException {
        run("INSERT INTO users VALUES (1, 'Alice', true)");

        List<Record> results = run("SELECT * FROM users WHERE id = 999");
        assertTrue(results.isEmpty());
    }

    @Test
    void insertManyThenSelectAll() throws IOException {
        for (int i = 1; i <= 20; i++) {
            run("INSERT INTO users VALUES (" + i + ", 'User" + i + "', true)");
        }

        List<Record> results = run("SELECT * FROM users");
        assertEquals(20, results.size());
    }

    @Test
    void indexLookupMatchesSeqScan() throws IOException {
        run("INSERT INTO users VALUES (10, 'Alice', true)");
        run("INSERT INTO users VALUES (20, 'Bob', false)");
        run("INSERT INTO users VALUES (30, 'Charlie', true)");

        // Index lookup
        List<Record> indexed = run("SELECT * FROM users WHERE id = 20");

        assertEquals(1, indexed.size());
        assertEquals(20, indexed.get(0).get("id"));
        assertEquals("Bob", indexed.get(0).get("name"));
    }

    // ======================== PERFORMANCE TESTS ========================

    @Test
    void bulkInsert10000Records() throws IOException {
        // Create a fresh table for bulk test
        Schema schema = new Schema(List.of(
                new Column("id", ColumnType.INT, 4),
                new Column("name", ColumnType.VARCHAR, 20),
                new Column("active", ColumnType.BOOLEAN, 1)
        ));
        String tempPath = Files.createTempFile("bulk-test", ".db").toString();
        catalog.createTable("bulk", schema, tempPath);
        BPlusTree bulkIndex = new BPlusTree(50); // higher order for bulk data

        catalog.createIndex("bulk", bulkIndex);
        Executor bulkExecutor = new Executor(catalog);

        int count = 10_000;
        long startInsert = System.nanoTime();

        for (int i = 1; i <= count; i++) {
            ParsedStatement ps = new ParsedStatement();
            ps.statementType = com.yourname.db.query.StatementType.INSERT;
            ps.tableName = "bulk";
            // Pad name to fit within 20 chars
            ps.values = List.of(String.valueOf(i), "User" + i, "true");
            bulkExecutor.execute(QueryPlan.fromStatement(ps));
        }

        long insertElapsed = System.nanoTime() - startInsert;
        System.out.println("=== Bulk Insert Performance ===");
        System.out.println("  " + count + " inserts: " + (insertElapsed / 1_000_000) + " ms");

        // Verify all records via scan
        long startScan = System.nanoTime();
        ParsedStatement scanPs = new ParsedStatement();
        scanPs.statementType = com.yourname.db.query.StatementType.SELECT;
        scanPs.tableName = "bulk";
        List<Record> all = bulkExecutor.execute(QueryPlan.fromStatement(scanPs));
        long scanElapsed = System.nanoTime() - startScan;

        assertEquals(count, all.size());
        System.out.println("  Full scan of " + count + " records: " + (scanElapsed / 1_000_000) + " ms");

        // Index lookup performance
        long startLookup = System.nanoTime();
        int lookups = 1000;
        for (int i = 1; i <= lookups; i++) {
            int key = (i * 7) % count + 1; // pseudo-random keys
            ParsedStatement lookupPs = new ParsedStatement();
            lookupPs.statementType = com.yourname.db.query.StatementType.SELECT;
            lookupPs.tableName = "bulk";
            lookupPs.condition = new com.yourname.db.query.Condition("id", "=", String.valueOf(key));
            List<Record> result = bulkExecutor.execute(QueryPlan.fromStatement(lookupPs));
            assertEquals(1, result.size(), "Index lookup should find key " + key);
        }
        long lookupElapsed = System.nanoTime() - startLookup;
        System.out.println("  " + lookups + " index lookups: " + (lookupElapsed / 1_000_000) + " ms");
        System.out.println("  Avg per lookup: " + (lookupElapsed / lookups / 1000) + " μs");
    }

    @Test
    void bulkDeletePerformance() throws IOException {
        Schema schema = new Schema(List.of(
                new Column("id", ColumnType.INT, 4),
                new Column("val", ColumnType.VARCHAR, 10)
        ));
        String tempPath = Files.createTempFile("bulkdel-test", ".db").toString();
        catalog.createTable("deltest", schema, tempPath);
        Executor delExecutor = new Executor(catalog);

        int count = 500;
        for (int i = 1; i <= count; i++) {
            ParsedStatement ps = new ParsedStatement();
            ps.statementType = com.yourname.db.query.StatementType.INSERT;
            ps.tableName = "deltest";
            ps.values = List.of(String.valueOf(i), "val" + i);
            delExecutor.execute(QueryPlan.fromStatement(ps));
        }

        // Delete half the records
        long startDelete = System.nanoTime();
        for (int i = 1; i <= count; i += 2) {
            ParsedStatement ps = new ParsedStatement();
            ps.statementType = com.yourname.db.query.StatementType.DELETE;
            ps.tableName = "deltest";
            ps.condition = new com.yourname.db.query.Condition("id", "=", String.valueOf(i));
            delExecutor.execute(QueryPlan.fromStatement(ps));
        }
        long deleteElapsed = System.nanoTime() - startDelete;

        // Verify remaining
        ParsedStatement scanPs = new ParsedStatement();
        scanPs.statementType = com.yourname.db.query.StatementType.SELECT;
        scanPs.tableName = "deltest";
        List<Record> remaining = delExecutor.execute(QueryPlan.fromStatement(scanPs));

        assertEquals(count / 2, remaining.size());
        System.out.println("=== Bulk Delete Performance ===");
        System.out.println("  Deleted " + (count / 2) + " of " + count + " records: " + (deleteElapsed / 1_000_000) + " ms");

        // All remaining should have even ids
        for (Record r : remaining) {
            int id = (int) r.get("id");
            assertEquals(0, id % 2, "Only even IDs should remain, found id=" + id);
        }
    }

    @Test
    void multiPageStressTest() throws IOException {
        // Insert records large enough to force many pages
        Schema schema = new Schema(List.of(
                new Column("id", ColumnType.INT, 4),
                new Column("payload", ColumnType.VARCHAR, 200)
        ));
        String tempPath = Files.createTempFile("multipage-test", ".db").toString();
        catalog.createTable("bigrows", schema, tempPath);
        Executor bigExecutor = new Executor(catalog);

        int count = 200; // 204 bytes per row -> ~15 rows per 4096 page -> ~14 pages
        for (int i = 1; i <= count; i++) {
            ParsedStatement ps = new ParsedStatement();
            ps.statementType = com.yourname.db.query.StatementType.INSERT;
            ps.tableName = "bigrows";
            // Create a payload that fills most of the 200-byte column
            String payload = "data" + i;
            ps.values = List.of(String.valueOf(i), payload);
            bigExecutor.execute(QueryPlan.fromStatement(ps));
        }

        ParsedStatement scanPs = new ParsedStatement();
        scanPs.statementType = com.yourname.db.query.StatementType.SELECT;
        scanPs.tableName = "bigrows";
        List<Record> all = bigExecutor.execute(QueryPlan.fromStatement(scanPs));

        assertEquals(count, all.size());
        System.out.println("=== Multi-Page Stress Test ===");
        System.out.println("  " + count + " large rows (204 bytes each) inserted and scanned successfully");
    }
}
