package com.yourname.db.catalog;

import com.yourname.db.record.Schema;
import com.yourname.db.storage.HeapFile;

import java.util.HashMap;
import java.util.Map;

public class Catalog {
    private Map<String, TableMetaData> schemas;


    private class TableMetaData {
        String tableName;
        Schema schema;
        HeapFile heapFile;
    }
}
