package com.yourname.db.storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class DiskManagerTest {

    private Path tempFile;
    private DiskManager dm;

    @BeforeEach
    void setUp() throws IOException {
        tempFile = Files.createTempFile("diskmanager-test", ".db");
        dm = new DiskManager(tempFile.toString());
    }

    @AfterEach
    void tearDown() throws IOException {
        dm.close();
        Files.deleteIfExists(tempFile);
    }

    @Test
    void newFileHasZeroPages() {
        assertEquals(0, dm.getNumPages());
    }

    @Test
    void allocatePageIncrementsPageCount() throws IOException {
        dm.allocatePage();
        assertEquals(1, dm.getNumPages());
        dm.allocatePage();
        assertEquals(2, dm.getNumPages());
    }

    @Test
    void writeAndReadPageRoundtrip() throws IOException {
        Page page = dm.allocatePage();
        byte[] row = "hello".getBytes();
        page.insert(row);
        dm.writePage(page);

        Page loaded = dm.readPage(0);
        byte[] retrieved = loaded.get(new RecordID(0, 0));
        assertArrayEquals(row, retrieved);
    }

    @Test
    void readOutOfBoundsThrows() {
        assertThrows(IndexOutOfBoundsException.class, () -> dm.readPage(0));
        assertThrows(IndexOutOfBoundsException.class, () -> dm.readPage(-1));
    }

    @Test
    void dataPersistedAcrossReopen() throws IOException {
        Page page = dm.allocatePage();
        byte[] row = "persist".getBytes();
        page.insert(row);
        dm.writePage(page);
        dm.close();

        DiskManager dm2 = new DiskManager(tempFile.toString());
        assertEquals(1, dm2.getNumPages());
        Page loaded = dm2.readPage(0);
        assertArrayEquals(row, loaded.get(new RecordID(0, 0)));
        dm2.close();
    }

    @Test
    void allocateMultiplePagesAndReadEach() throws IOException {
        for (int i = 0; i < 5; i++) {
            Page page = dm.allocatePage();
            byte[] row = ("page-" + i).getBytes();
            page.insert(row);
            dm.writePage(page);
        }
        assertEquals(5, dm.getNumPages());

        for (int i = 0; i < 5; i++) {
            Page page = dm.readPage(i);
            byte[] row = page.get(new RecordID(i, 0));
            assertEquals("page-" + i, new String(row));
        }
    }
}
