package com.yourname.db.catalog;

import com.yourname.db.record.Schema;
import com.yourname.db.storage.HeapFile;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Catalog {
    private Map<String, TableMetaData> tables;


    private class TableMetaData {
        String tableName;
        Schema schema;
        HeapFile heapFile;
    }


    public void createTable(String tableName, Schema schema, String filePath) throws IllegalStateException, IOException {
        if (tables.containsKey(tableName)) {
            throw new IllegalStateException("Table " + tableName + " already exists");
        }

        TableMetaData tableMetaData = new TableMetaData();
        tableMetaData.tableName = tableName;
        tableMetaData.schema = schema;
        tableMetaData.heapFile = new HeapFile(filePath);

        tables.put(tableName, tableMetaData);

    }
}
