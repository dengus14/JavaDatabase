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

    public byte[] serialize() {
        byte[] bytes = new byte[schema.getTotalSize()];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        for(Schema.Column column :  schema.getColumns()){
            int offset = schema.getOffset(column.name());
            switch (column.type()) {
                case INT:
                    int value = (int) columns.get(column.name());
                    buffer.putInt(offset, value);
                    break;
                case BOOLEAN:
                    boolean bool =  (boolean)columns.get(column.name());
                    if(bool){
                        buffer.put(offset,(byte)1);
                    }
                    else{
                        buffer.put(offset,(byte)0);
                    }
                    break;
                case VARCHAR:
                    String v = (String) columns.get(column.name());
                    byte[] data = v.getBytes();
                    if (data.length > column.size()) {
                        throw new IllegalArgumentException("VARCHAR value too long for column: " + column.name());
                    }
                    buffer.put(offset,data);

            }

        }

        return buffer.array();
    }

    public Object get(String columnName) {
        return columns.get(columnName);
    }
}
