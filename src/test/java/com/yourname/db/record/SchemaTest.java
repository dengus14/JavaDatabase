package com.yourname.db.record;

import com.yourname.db.record.Schema.Column;
import com.yourname.db.record.Schema.ColumnType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SchemaTest {

    private final Schema schema = new Schema(List.of(
            new Column("id", ColumnType.INT, 4),
            new Column("name", ColumnType.VARCHAR, 20),
            new Column("active", ColumnType.BOOLEAN, 1)
    ));

    @Test
    void getTotalSizeReturnsSumOfAllColumnSizes() {
        assertEquals(25, schema.getTotalSize());
    }

    @Test
    void getOffsetReturnsCorrectOffsetForFirstColumn() {
        assertEquals(0, schema.getOffset("id"));
    }

    @Test
    void getOffsetReturnsCorrectOffsetForMiddleColumn() {
        assertEquals(4, schema.getOffset("name"));
    }

    @Test
    void getOffsetReturnsCorrectOffsetForLastColumn() {
        assertEquals(24, schema.getOffset("active"));
    }

    @Test
    void getOffsetThrowsForNonexistentColumn() {
        assertThrows(IllegalArgumentException.class, () -> schema.getOffset("missing"));
    }
}
