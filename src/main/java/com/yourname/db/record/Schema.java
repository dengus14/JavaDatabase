package com.yourname.db.record;

import java.sql.SQLException;
import java.util.List;

public class Schema {

    private List<Column> columns;


    public enum ColumnType {INT, VARCHAR, BOOLEAN}
    public record Column(String name, ColumnType type, int size) {}

    public Schema(List<Column> columns) {
        this.columns = columns;
    }

    public int getTotalSize() {
        int totalSize = 0;
        for (Column column : columns) {
            totalSize += column.size();
        }
        return totalSize;
    }

    public int getOffset(String columnName) {
        int runningByte = 0;
        for (Column column : columns) {
            if (column.name().equals(columnName)) {
                return runningByte;
            }
            runningByte += column.size();
        }

        throw new IllegalArgumentException("Column name not found");
    }

    public List<Column> getColumns() {
        return columns;
    }

}
