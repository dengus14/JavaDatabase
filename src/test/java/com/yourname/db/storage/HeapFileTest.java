package com.yourname.db.storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class HeapFileTest {

    private Path tempFile;
    private HeapFile heapFile;

    @BeforeEach
    void setUp() throws IOException {
        tempFile = Files.createTempFile("heapfile-test", ".db");
        heapFile = new HeapFile(tempFile.toString());
    }

    @AfterEach
    void tearDown() throws IOException {
        Files.deleteIfExists(tempFile);
    }

    @Test
    void scanOnEmptyFileReturnsEmptyList() throws IOException {
        List<RecordID> rids = heapFile.scan();
        assertTrue(rids.isEmpty());
    }

    @Test
    void insertAndGetRoundtrip() throws IOException {
        byte[] data = "test-row".getBytes();
        RecordID rid = heapFile.insert(data);

        assertNotNull(rid);
        byte[] retrieved = heapFile.get(rid);
        assertArrayEquals(data, retrieved);
    }

    @Test
    void insertMultipleAndScanReturnsAll() throws IOException {
        for (int i = 0; i < 10; i++) {
            heapFile.insert(("row-" + i).getBytes());
        }

        List<RecordID> rids = heapFile.scan();
        assertEquals(10, rids.size());
    }

    @Test
    void deleteRemovesRecordFromScan() throws IOException {
        RecordID rid1 = heapFile.insert("first".getBytes());
        RecordID rid2 = heapFile.insert("second".getBytes());
        RecordID rid3 = heapFile.insert("third".getBytes());

        heapFile.delete(rid2);

        List<RecordID> rids = heapFile.scan();
        assertEquals(2, rids.size());
        assertFalse(rids.contains(rid2));
        assertTrue(rids.contains(rid1));
        assertTrue(rids.contains(rid3));
    }

    @Test
    void getAfterDeleteThrows() throws IOException {
        RecordID rid = heapFile.insert("deleteme".getBytes());
        heapFile.delete(rid);

        assertThrows(IllegalStateException.class, () -> heapFile.get(rid));
    }

    @Test
    void insertSpansMultiplePages() throws IOException {
        // Each row is ~200 bytes. Page is 4096 bytes.
        // With slot overhead, roughly 15-16 rows per page.
        // 50 rows should span at least 3 pages.
        byte[] largeRow = new byte[200];
        for (int i = 0; i < 50; i++) {
            largeRow[0] = (byte) i;
            heapFile.insert(largeRow.clone());
        }

        List<RecordID> rids = heapFile.scan();
        assertEquals(50, rids.size());

        // Verify records span multiple pages
        long distinctPages = rids.stream()
                .map(RecordID::pageNumber)
                .distinct()
                .count();
        assertTrue(distinctPages > 1, "Records should span multiple pages, got " + distinctPages);
    }

    @Test
    void deleteAllThenScanReturnsEmpty() throws IOException {
        RecordID rid1 = heapFile.insert("a".getBytes());
        RecordID rid2 = heapFile.insert("b".getBytes());

        heapFile.delete(rid1);
        heapFile.delete(rid2);

        List<RecordID> rids = heapFile.scan();
        assertTrue(rids.isEmpty());
    }
}
