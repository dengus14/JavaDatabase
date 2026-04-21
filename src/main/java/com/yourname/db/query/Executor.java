package com.yourname.db.query;

import com.yourname.db.catalog.Catalog;
import com.yourname.db.record.Schema;
import com.yourname.db.storage.HeapFile;
import com.yourname.db.storage.RecordID;
import com.yourname.db.record.Record;

import javax.management.Query;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Executor {

    private Catalog catalog;
    public Executor(Catalog catalog) {
        this.catalog = catalog;
    }


    public List<Record> execute(QueryPlan queryPlan) {

    }

    public List<Record> executeSeqScan(QueryPlan queryPlan) throws IOException {
        List<Record> records = new ArrayList<>();

        HeapFile heapFile = catalog.getHeapFile(queryPlan.tableName);
        Schema schema = catalog.getSchema(queryPlan.tableName);
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


}
