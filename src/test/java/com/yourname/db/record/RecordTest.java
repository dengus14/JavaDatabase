package com.yourname.db.record;

import com.yourname.db.record.Schema.Column;
import com.yourname.db.record.Schema.ColumnType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class RecordTest {

    private final Schema schema = new Schema(List.of(
            new Column("id", ColumnType.INT, 4),
            new Column("name", ColumnType.VARCHAR, 20),
            new Column("active", ColumnType.BOOLEAN, 1)
    ));

    @Test
    void serializeDeserializeRoundtrip() {
        Map<String, Object> values = Map.of(
                "id", 42,
                "name", "Alice",
                "active", true
        );
        Record original = new Record(schema, values);

        byte[] bytes = original.serialize();
        Record restored = Record.deserialize(bytes, schema);
        byte[] reserialized = restored.serialize();

        assertArrayEquals(bytes, reserialized);
    }

    @Test
    void deserializeTrimsTrailingNullBytesForVarchar() {
        // "Bob" is 3 bytes but the VARCHAR column is 20 bytes wide,
        // so the remaining 17 bytes are zero-filled after serialize.
        // Deserialize should trim those nulls back to "Bob".
        Map<String, Object> values = Map.of(
                "id", 1,
                "name", "Bob",
                "active", false
        );
        Record record = new Record(schema, values);
        byte[] bytes = record.serialize();

        Record restored = Record.deserialize(bytes, schema);
        byte[] reserialized = restored.serialize();

        // If nulls weren't trimmed, re-serializing would produce different bytes
        // because the name would contain trailing nulls as characters.
        assertArrayEquals(bytes, reserialized);
    }

    @Test
    void serializeThrowsWhenVarcharExceedsColumnSize() {
        String tooLong = "a".repeat(21); // column size is 20
        Map<String, Object> values = Map.of(
                "id", 1,
                "name", tooLong,
                "active", false
        );
        Record record = new Record(schema, values);

        assertThrows(IllegalArgumentException.class, record::serialize);
    }
}
