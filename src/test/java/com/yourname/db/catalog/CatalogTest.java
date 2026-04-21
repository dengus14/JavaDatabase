package com.yourname.db.catalog;

import com.yourname.db.index.BPlusTree;
import com.yourname.db.record.Schema;
import com.yourname.db.record.Schema.Column;
import com.yourname.db.record.Schema.ColumnType;
import com.yourname.db.storage.HeapFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CatalogTest {

    private Catalog catalog;
    private Schema schema;

    @BeforeEach
    void setUp() {
        catalog = new Catalog();
        schema = new Schema(List.of(
                new Column("id", ColumnType.INT, 4),
                new Column("name", ColumnType.VARCHAR, 20)
        ));
    }

    private String tempPath() throws IOException {
        return Files.createTempFile("catalog-test", ".db").toString();
    }

    @Test
    void createTableAndHasTable() throws IOException {
        catalog.createTable("users", schema, tempPath());
        assertTrue(catalog.hasTable("users"));
        assertFalse(catalog.hasTable("orders"));
    }

    @Test
    void getSchemaReturnsCorrectSchema() throws IOException {
        catalog.createTable("users", schema, tempPath());
        Schema retrieved = catalog.getSchema("users");
        assertEquals(schema, retrieved);
    }

    @Test
    void getHeapFileReturnsNonNull() throws IOException {
        catalog.createTable("users", schema, tempPath());
        HeapFile hf = catalog.getHeapFile("users");
        assertNotNull(hf);
    }

    @Test
    void duplicateCreateTableThrows() throws IOException {
        catalog.createTable("users", schema, tempPath());
        assertThrows(IllegalStateException.class,
                () -> catalog.createTable("users", schema, tempPath()));
    }

    @Test
    void getSchemaForMissingTableThrows() {
        assertThrows(IllegalStateException.class,
                () -> catalog.getSchema("nonexistent"));
    }

    @Test
    void getHeapFileForMissingTableThrows() {
        assertThrows(IllegalStateException.class,
                () -> catalog.getHeapFile("nonexistent"));
    }

    @Test
    void dropTableRemovesTable() throws IOException {
        catalog.createTable("users", schema, tempPath());
        catalog.dropTable("users");
        assertFalse(catalog.hasTable("users"));
    }

    @Test
    void dropMissingTableThrows() {
        assertThrows(IllegalStateException.class,
                () -> catalog.dropTable("nonexistent"));
    }

    @Test
    void createIndexAndHasIndex() throws IOException {
        catalog.createTable("users", schema, tempPath());
        BPlusTree tree = new BPlusTree(3);
        catalog.createIndex("users", tree);

        assertTrue(catalog.hasIndex("users"));
        assertEquals(tree, catalog.getIndex("users"));
    }

    @Test
    void createIndexOnMissingTableThrows() {
        BPlusTree tree = new BPlusTree(3);
        assertThrows(IllegalStateException.class,
                () -> catalog.createIndex("nonexistent", tree));
    }

    @Test
    void hasIndexReturnsFalseWhenNoIndex() throws IOException {
        catalog.createTable("users", schema, tempPath());
        assertFalse(catalog.hasIndex("users"));
    }
}
