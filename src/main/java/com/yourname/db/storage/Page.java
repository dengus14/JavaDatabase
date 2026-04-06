package com.yourname.db.storage;

import java.nio.ByteBuffer;

public class Page {
    private int pageNumber;
    public static final int PAGE_SIZE = 4096;           // fixed size of every page in bytes
    private static final int NUM_ROWS_OFFSET = 0;       // byte 0 stores the number of rows
    private static final int SLOT_ARRAY_START = 4;      // slots start at byte 4, right after num_rows
    private static final int SLOT_SIZE = 4;             // each slot is one int (4 bytes) storing a row's byte offset
    private ByteBuffer buffer;

    public Page(int pageNumber) {
        this.pageNumber = pageNumber;
        this.buffer = ByteBuffer.allocate(PAGE_SIZE);
        buffer.putInt(NUM_ROWS_OFFSET, 0);              // initialize num_rows to 0 on empty page
    }

    // reads num_rows from byte 0 of the buffer
    public int getNumRows() {
        return buffer.getInt(NUM_ROWS_OFFSET);
    }

    // finds where the last inserted row starts — PAGE_SIZE if no rows exist yet
    // rows grow from the right, so the last row's offset is stored in the most recent slot
    private int getLastRowStart() {
        if (getNumRows() == 0) {
            return PAGE_SIZE;
        }
        int lastSlotOffset = SLOT_ARRAY_START + ((getNumRows() - 1) * SLOT_SIZE);
        return buffer.getInt(lastSlotOffset);
    }

    // checks if there is enough free space for a new row and its slot
    // free space is the gap between where the header ends and where the rows begin
    public boolean hasSpace(int rowSize) {
        int headerEnd = SLOT_ARRAY_START + (getNumRows() * SLOT_SIZE);
        int freeSpace = getLastRowStart() - headerEnd;
        return freeSpace >= rowSize + SLOT_SIZE + 4;
    }

    // inserts a row into the page and returns a RecordID to locate it later
    // rows are written from the right, slots are written from the left
    public RecordID insert(byte[] row) {
        if (!hasSpace(row.length)) {
            return null;
        }

        int newSlotIndex = getNumRows();                        // save slot index before incrementing num_rows
        int newRowPosition = getLastRowStart() - row.length - 4; // place row before previous one, accounting for length prefix

        buffer.putInt(newRowPosition, row.length);              // write row length first (4 bytes)
        buffer.put(newRowPosition + 4, row);                    // write row bytes after the length
        int newSlotOffset = SLOT_ARRAY_START + (newSlotIndex * SLOT_SIZE);
        buffer.putInt(newSlotOffset, newRowPosition);           // write row's byte offset into the new slot
        buffer.putInt(NUM_ROWS_OFFSET, getNumRows() + 1);       // increment num_rows

        // return a RecordID so the caller can locate this row later
        RecordID recordID = new RecordID();
        recordID.setPageNumber(pageNumber);
        recordID.setSlotNumber(newSlotIndex);
        return recordID;
    }
}