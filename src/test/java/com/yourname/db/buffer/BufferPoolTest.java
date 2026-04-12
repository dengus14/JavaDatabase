package com.yourname.db.buffer;

import com.yourname.db.storage.DiskManager;
import com.yourname.db.storage.Page;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class BufferPoolTest {

    @TempDir
    Path tempDir;

    private DiskManager diskManager;

    @BeforeEach
    void setUp() throws IOException {
        String dbFile = tempDir.resolve("test.db").toString();
        diskManager = new DiskManager(dbFile);
    }

    @AfterEach
    void tearDown() {
        diskManager.close();
    }

    @Test
    void fetchPageLoadsFromDisk() throws IOException {
        diskManager.allocatePage();
        BufferPool pool = new BufferPool(10, diskManager);

        Page page = pool.fetchPage(0);

        assertNotNull(page);
        assertEquals(0, page.getPageNumber());
    }

    @Test
    void cacheHitReturnsSameObject() throws IOException {
        diskManager.allocatePage();
        BufferPool pool = new BufferPool(10, diskManager);

        Page first = pool.fetchPage(0);
        pool.unpinPage(0, false);
        Page second = pool.fetchPage(0);

        assertSame(first, second, "Second fetch should return the cached Page object");
    }

    @Test
    void evictionSucceedsWhenUnpinnedPageExists() throws IOException {
        diskManager.allocatePage(); // page 0
        diskManager.allocatePage(); // page 1
        diskManager.allocatePage(); // page 2
        BufferPool pool = new BufferPool(2, diskManager);

        pool.fetchPage(0);
        pool.unpinPage(0, false);
        pool.fetchPage(1);
        pool.unpinPage(1, false);

        assertDoesNotThrow(() -> pool.fetchPage(2));
    }

    @Test
    void dirtyPageFlushedOnEviction() throws IOException {
        diskManager.allocatePage(); // page 0
        diskManager.allocatePage(); // page 1
        diskManager.allocatePage(); // page 2
        BufferPool pool = new BufferPool(2, diskManager);

        // Fetch page 0 and insert data into it
        Page page0 = pool.fetchPage(0);
        byte[] row = "hello".getBytes();
        page0.insert(row);
        pool.unpinPage(0, true); // mark dirty

        // Fetch pages 1 and 2 to force eviction of page 0
        pool.fetchPage(1);
        pool.unpinPage(1, false);
        pool.fetchPage(2);

        // Read page 0 directly from disk — it should have the inserted row
        Page fromDisk = diskManager.readPage(0);
        assertArrayEquals(row, fromDisk.get(new com.yourname.db.storage.RecordID(0, 0)));
    }

    @Test
    void allPinnedThrowsIOException() throws IOException {
        diskManager.allocatePage(); // page 0
        diskManager.allocatePage(); // page 1
        BufferPool pool = new BufferPool(1, diskManager);

        pool.fetchPage(0); // pinned, don't unpin

        assertThrows(IOException.class, () -> pool.fetchPage(1));
    }

    @Test
    void flushAllCompletesWithoutException() throws IOException {
        diskManager.allocatePage(); // page 0
        diskManager.allocatePage(); // page 1
        BufferPool pool = new BufferPool(10, diskManager);

        Page page0 = pool.fetchPage(0);
        page0.insert("data0".getBytes());
        pool.unpinPage(0, true);

        Page page1 = pool.fetchPage(1);
        page1.insert("data1".getBytes());
        pool.unpinPage(1, true);

        assertDoesNotThrow(() -> pool.flushAll());

        // Verify data was actually persisted to disk
        Page disk0 = diskManager.readPage(0);
        assertArrayEquals("data0".getBytes(),
                disk0.get(new com.yourname.db.storage.RecordID(0, 0)));
        Page disk1 = diskManager.readPage(1);
        assertArrayEquals("data1".getBytes(),
                disk1.get(new com.yourname.db.storage.RecordID(1, 0)));
    }
}
