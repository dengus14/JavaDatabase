package com.yourname.db.query;

import com.yourname.db.catalog.Catalog;
import com.yourname.db.index.BPlusTree;
import com.yourname.db.record.Schema;
import com.yourname.db.storage.HeapFile;
import com.yourname.db.storage.RecordID;
import com.yourname.db.record.Record;

import javax.management.Query;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Executor {

    private Catalog catalog;
    public Executor(Catalog catalog) {
        this.catalog = catalog;
    }


    public List<Record> execute(QueryPlan queryPlan) throws IOException {
        switch (queryPlan.planType) {
            case INSERT:
                return executeInsert(queryPlan);
            case DELETE:
                return executeDelete(queryPlan);
            case SEQ_SCAN:
                return executeSeqScan(queryPlan);
            default:
                throw new IllegalArgumentException("Unknown planType: " + queryPlan.planType);
        }
    }

    public List<Record> executeSeqScan(QueryPlan queryPlan) throws IOException {
        List<Record> records = new ArrayList<>();

        HeapFile heapFile = catalog.getHeapFile(queryPlan.tableName);
        Schema schema = catalog.getSchema(queryPlan.tableName);

        if (catalog.hasIndex(queryPlan.tableName) && queryPlan.condition != null) {
            BPlusTree tree = catalog.getIndex(queryPlan.tableName);
            int conditionKey = Integer.parseInt(queryPlan.condition.value);
            RecordID rid = tree.search(conditionKey);
            if (rid != null) {
                byte[] rawData = heapFile.get(rid);
                Record record = Record.deserialize(rawData, schema);
                records.add(record);
            }
            return records;
        }

        List<RecordID> rids = heapFile.scan();
        boolean hasCondition = queryPlan.condition != null;
        Condition condition = queryPlan.condition;
        Object conditionValue = null;

        if (hasCondition) {
            Schema.Column col = schema.getColumn(condition.column);
            switch (col.type()) {
                case INT:
                    conditionValue = Integer.parseInt(condition.value);
                    break;
                case BOOLEAN:
                    conditionValue = Boolean.parseBoolean(condition.value);
                    break;
                default:
                    conditionValue = condition.value;
            }
        }

        for (RecordID rid: rids) {
            byte[] rawData = heapFile.get(rid);
            Record newRecord = Record.deserialize(rawData, schema);
            if (hasCondition) {
                Object recordValue = newRecord.get(condition.column);
                if (!recordValue.equals(conditionValue)) {
                    continue;
                }
            }
            records.add(newRecord);
        }
        return records;
    }

    public List<Record> executeInsert(QueryPlan queryPlan) throws IOException {
        List<Record> records = new ArrayList<>();

        HeapFile heapFile = catalog.getHeapFile(queryPlan.tableName);
        Schema schema = catalog.getSchema(queryPlan.tableName);

        Map<String, Object> values = new HashMap<>();

        for (int i = 0; i < schema.getColumns().size(); i++) {
            Schema.Column col = schema.getColumns().get(i);
            String raw = queryPlan.values.get(i);
            switch (col.type()) {
                case INT:
                    values.put(col.name(), Integer.parseInt(raw));
                    break;
                case BOOLEAN:
                    values.put(col.name(), Boolean.parseBoolean(raw));
                    break;
                case VARCHAR:
                    values.put(col.name(), raw);
                    break;
            }
        }

        Record record = new Record(schema, values);
        byte[] bytes = record.serialize();
        RecordID rid = heapFile.insert(bytes);

        if (catalog.hasIndex(queryPlan.tableName)) {
            BPlusTree tree = catalog.getIndex(queryPlan.tableName);
            String firstColumnName = schema.getColumns().get(0).name();
            int key = (int) values.get(firstColumnName);
            tree.insert(key, rid);
        }

        return records;
    }

    public List<Record> executeDelete(QueryPlan queryPlan) throws IOException {
        List<Record> records = new ArrayList<>();

        HeapFile heapFile = catalog.getHeapFile(queryPlan.tableName);
        Schema schema = catalog.getSchema(queryPlan.tableName);

        List<RecordID> rids = heapFile.scan();

        boolean hasCondition = queryPlan.condition != null;
        Object conditionValue = null;

        if (hasCondition) {
            Schema.Column col = schema.getColumn(queryPlan.condition.column);
            switch (col.type()) {
                case INT:
                    conditionValue = Integer.parseInt(queryPlan.condition.value);
                    break;
                case BOOLEAN:
                    conditionValue = Boolean.parseBoolean(queryPlan.condition.value);
                    break;
                case VARCHAR:
                    conditionValue = queryPlan.condition.value;
                    break;
            }
        }

        for (RecordID rid: rids) {
            byte[] rawData = heapFile.get(rid);
            Record record = Record.deserialize(rawData, schema);
            if (hasCondition) {
                Object recordValue = record.get(queryPlan.condition.column);
                if (recordValue.equals(conditionValue)) {
                    heapFile.delete(rid);
                }
            }else {
                heapFile.delete(rid);
            }
        }
        return records;

    }


}
