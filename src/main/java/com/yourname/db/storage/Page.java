package com.yourname.db.storage;

import java.nio.ByteBuffer;
import java.util.Optional;

public class Page {
    public static final int PAGE_SIZE = 4096;           // fixed size of every page in bytes
    private static final int NUM_ROWS_OFFSET = 0;       // byte 0 stores the number of rows
    private static final int SLOT_ARRAY_START = 4;      // slots start at byte 4, right after num_rows
    private static final int SLOT_SIZE = 4;             // each slot is one int (4 bytes) storing a row's byte offset
    private static final int DELETED_SLOT = -1;         // tombstone marker written into a slot on delete
    private static final int ROW_LENGTH_PREFIX_SIZE = 4; // each row is prefixed with a 4-byte int length

    private final int pageNumber;
    private final ByteBuffer buffer;

    public int getPageNumber() {
        return pageNumber;
    }

    public Page(int pageNumber) {
        this.pageNumber = pageNumber;
        this.buffer = ByteBuffer.allocate(PAGE_SIZE);
        buffer.putInt(NUM_ROWS_OFFSET, 0);              // initialize num_rows to 0 on empty page
    }

    public Page(int pageNumber, ByteBuffer buffer) {
        this.pageNumber = pageNumber;
        this.buffer = buffer;
    }

    // exposes the raw backing array so DiskManager can write the page to disk
    public byte[] getData() {
        return buffer.array();
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
        return freeSpace >= rowSize + SLOT_SIZE + ROW_LENGTH_PREFIX_SIZE;
    }

    // inserts a row into the page and returns a RecordID to locate it later
    // rows are written from the right, slots are written from the left
    // returns Optional.empty() if the page is full
    public Optional<RecordID> insert(byte[] row) {
        if (!hasSpace(row.length)) {
            return Optional.empty();
        }

        int newSlotIndex = getNumRows();                                           // save slot index before incrementing num_rows
        int newRowPosition = getLastRowStart() - row.length - ROW_LENGTH_PREFIX_SIZE; // place row before previous one, accounting for length prefix

        buffer.putInt(newRowPosition, row.length);                                 // write row length first (4 bytes)
        buffer.put(newRowPosition + ROW_LENGTH_PREFIX_SIZE, row);                  // write row bytes after the length
        int newSlotOffset = SLOT_ARRAY_START + (newSlotIndex * SLOT_SIZE);
        buffer.putInt(newSlotOffset, newRowPosition);                              // write row's byte offset into the new slot
        buffer.putInt(NUM_ROWS_OFFSET, getNumRows() + 1);                          // increment num_rows

        return Optional.of(new RecordID(pageNumber, newSlotIndex));
    }

    public byte[] get(RecordID recordID) {
        int slotIndex = recordID.slotNumber();
        validateSlotIndex(slotIndex);

        int slotOffset = SLOT_ARRAY_START + (slotIndex * SLOT_SIZE);               // the slot in the header
        int rowToRead = buffer.getInt(slotOffset);                                 // byte offset of where the row lives in the buffer
        if (rowToRead == DELETED_SLOT) {
            throw new IllegalStateException("Record has been deleted: " + recordID);
        }

        int lengthToRead = buffer.getInt(rowToRead);                               // how many bytes to read to get the full row
        byte[] row = new byte[lengthToRead];
        buffer.get(rowToRead + ROW_LENGTH_PREFIX_SIZE, row);                       // copy row bytes out of the buffer
        return row;
    }

    public void delete(RecordID recordID) {
        int slotIndex = recordID.slotNumber();
        validateSlotIndex(slotIndex);

        int slotOffset = SLOT_ARRAY_START + (slotIndex * SLOT_SIZE);
        if (buffer.getInt(slotOffset) == DELETED_SLOT) {
            throw new IllegalStateException("Record already deleted: " + recordID);
        }
        buffer.putInt(slotOffset, DELETED_SLOT);                                   // tombstone the slot instead of compacting
    }

    private void validateSlotIndex(int slotIndex) {
        if (slotIndex < 0 || slotIndex >= getNumRows()) {
            throw new IndexOutOfBoundsException(
                "Slot index " + slotIndex + " is out of bounds for page with " + getNumRows() + " rows");
        }
    }

    //helper method for HeapFile for scan() method
    public boolean isDeleted(int slotIndex) {
        int slotOffset = SLOT_ARRAY_START + (slotIndex * SLOT_SIZE);
        return buffer.getInt(slotOffset) == DELETED_SLOT;
    }
}
