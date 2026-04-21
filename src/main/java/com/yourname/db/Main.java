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

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

public class Main {

    public static void main(String[] args) throws IOException {
        Schema schema = new Schema(List.of(
                new Column("id", ColumnType.INT, 4),
                new Column("name", ColumnType.VARCHAR, 20),
                new Column("active", ColumnType.BOOLEAN, 1)
        ));

        Catalog catalog = new Catalog();
        String tempPath = Files.createTempFile("users", ".db").toString();
        catalog.createTable("users", schema, tempPath);

        // Create B+ tree index (order 3) on users table
        BPlusTree index = new BPlusTree(3);
        catalog.createIndex("users", index);

        // Create parser and executor
        Parser parser = new Parser();
        Executor executor = new Executor(catalog);

        String[] statements = {
                "INSERT INTO users VALUES (1, 'Alice', true)",
                "INSERT INTO users VALUES (2, 'Bob', false)",
                "INSERT INTO users VALUES (3, 'Charlie', true)",
                "SELECT * FROM users",
                "SELECT * FROM users WHERE id = 2",
                "DELETE FROM users WHERE id = 1",
                "SELECT * FROM users"
        };

        for (String sql : statements) {
            System.out.println("\n> " + sql);

            ParsedStatement ps = parser.parse(sql);
            QueryPlan plan = QueryPlan.fromStatement(ps);
            List<Record> results = executor.execute(plan);

            for (Record record : results) {
                System.out.println("  id=" + record.get("id")
                        + ", name=" + record.get("name")
                        + ", active=" + record.get("active"));
            }
        }
    }
}
