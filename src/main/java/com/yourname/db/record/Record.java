package com.yourname.db.record;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static com.yourname.db.record.Schema.ColumnType.BOOLEAN;

public class Record {
    private Schema schema;
    private Map<String, Object> columns;

    public Record(Schema schema, Map<String, Object> columns) {
        this.schema = schema;
        this.columns = columns;
    }

    public static Record deserialize(byte[] data, Schema schema) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        Map<String,Object> columns = new HashMap<>();

        for(Schema.Column column :  schema.getColumns()){
            int offset = schema.getOffset(column.name());
            switch (column.type()) {
                case INT:
                    int value = buffer.getInt(offset);
                    columns.put(column.name(), value);
                    break;
                case BOOLEAN:
                    byte b =  buffer.get(offset);
                    if (b == 1){
                        columns.put(column.name(), true);
                    }
                    else{
                        columns.put(column.name(), false);
                    }
                    break;
                case VARCHAR:
                    byte[] bytes = new byte[column.size()];
                    buffer.get(offset,bytes);
                    columns.put(column.name(), new String(bytes).replace("\0", ""));
                    break;
            }
        }

        return new Record(schema, columns);
    }
}
