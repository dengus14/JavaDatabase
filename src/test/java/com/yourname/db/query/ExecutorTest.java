package com.yourname.db.query;

import com.yourname.db.catalog.Catalog;
import com.yourname.db.index.BPlusTree;
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

class ExecutorTest {

    private Catalog catalog;
    private Executor executor;
    private Parser parser;

    @BeforeEach
    void setUp() throws IOException {
        Schema schema = new Schema(List.of(
                new Column("id", ColumnType.INT, 4),
                new Column("name", ColumnType.VARCHAR, 20),
                new Column("active", ColumnType.BOOLEAN, 1)
        ));

        catalog = new Catalog();
        String tempPath = Files.createTempFile("executor-test", ".db").toString();
        catalog.createTable("users", schema, tempPath);

        BPlusTree index = new BPlusTree(3);
        catalog.createIndex("users", index);

        executor = new Executor(catalog);
        parser = new Parser();
    }

    private List<Record> run(String sql) throws IOException {
        ParsedStatement ps = parser.parse(sql);
        QueryPlan plan = QueryPlan.fromStatement(ps);
        return executor.execute(plan);
    }

    @Test
    void insertReturnsEmptyList() throws IOException {
        List<Record> result = run("INSERT INTO users VALUES (1, 'Alice', true)");
        assertTrue(result.isEmpty());
    }

    @Test
    void selectAfterInsertReturnsRecords() throws IOException {
        run("INSERT INTO users VALUES (1, 'Alice', true)");
        run("INSERT INTO users VALUES (2, 'Bob', false)");

        List<Record> results = run("SELECT * FROM users");
        assertEquals(2, results.size());
    }



    @Test
    void selectWithWhereUsingIndex() throws IOException {
        run("INSERT INTO users VALUES (1, 'Alice', true)");
        run("INSERT INTO users VALUES (2, 'Bob', false)");
        run("INSERT INTO users VALUES (3, 'Charlie', true)");

        List<Record> results = run("SELECT * FROM users WHERE id = 2");
        assertEquals(1, results.size());
        assertEquals(2, results.get(0).get("id"));
        assertEquals("Bob", results.get(0).get("name"));
        assertEquals(false, results.get(0).get("active"));
    }

    @Test
    void selectWithWhereReturnsEmptyForMissingKey() throws IOException {
        run("INSERT INTO users VALUES (1, 'Alice', true)");

        List<Record> results = run("SELECT * FROM users WHERE id = 99");
        assertTrue(results.isEmpty());
    }

    @Test
    void deleteRemovesRecord() throws IOException {
        run("INSERT INTO users VALUES (1, 'Alice', true)");
        run("INSERT INTO users VALUES (2, 'Bob', false)");

        run("DELETE FROM users WHERE id = 1");

        List<Record> results = run("SELECT * FROM users");
        assertEquals(1, results.size());
        assertEquals("Bob", results.get(0).get("name"));
    }

    @Test
    void deleteAllRecords() throws IOException {
        run("INSERT INTO users VALUES (1, 'Alice', true)");
        run("INSERT INTO users VALUES (2, 'Bob', false)");

        run("DELETE FROM users");

        List<Record> results = run("SELECT * FROM users");
        assertTrue(results.isEmpty());
    }

    @Test
    void selectOnEmptyTableReturnsEmpty() throws IOException {
        List<Record> results = run("SELECT * FROM users");
        assertTrue(results.isEmpty());
    }

    @Test
    void insertAndVerifyAllFields() throws IOException {
        run("INSERT INTO users VALUES (42, 'Zara', true)");

        List<Record> results = run("SELECT * FROM users WHERE id = 42");
        assertEquals(1, results.size());
        Record r = results.get(0);
        assertEquals(42, r.get("id"));
        assertEquals("Zara", r.get("name"));
        assertEquals(true, r.get("active"));
    }

    @Test
    void deleteNonexistentKeyIsNoOp() throws IOException {
        run("INSERT INTO users VALUES (1, 'Alice', true)");

        run("DELETE FROM users WHERE id = 99");

        List<Record> results = run("SELECT * FROM users");
        assertEquals(1, results.size());
    }

    @Test
    void selectWithoutIndexFallsBackToSeqScan() throws IOException {
        // Create a table without an index
        Schema schema = new Schema(List.of(
                new Column("id", ColumnType.INT, 4),
                new Column("val", ColumnType.VARCHAR, 10)
        ));
        String tempPath = Files.createTempFile("noindex-test", ".db").toString();
        catalog.createTable("items", schema, tempPath);

        // Insert via direct QueryPlan construction since parser targets "users"
        ParsedStatement ps = new ParsedStatement();
        ps.statementType = StatementType.INSERT;
        ps.tableName = "items";
        ps.values = List.of("1", "apple");
        executor.execute(QueryPlan.fromStatement(ps));

        ps = new ParsedStatement();
        ps.statementType = StatementType.INSERT;
        ps.tableName = "items";
        ps.values = List.of("2", "banana");
        executor.execute(QueryPlan.fromStatement(ps));

        // SELECT with WHERE but no index — should use seq scan fallback
        ParsedStatement selectPs = new ParsedStatement();
        selectPs.statementType = StatementType.SELECT;
        selectPs.tableName = "items";
        selectPs.condition = new Condition("id", "=", "2");
        List<Record> results = executor.execute(QueryPlan.fromStatement(selectPs));

        assertEquals(1, results.size());
        assertEquals("banana", results.get(0).get("val"));
    }
}
