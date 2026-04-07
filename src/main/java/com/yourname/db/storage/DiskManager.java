package com.yourname.db.storage;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DiskManager {

    private static final Logger log = Logger.getLogger(DiskManager.class.getName());

    private final RandomAccessFile file;
    private final String fileName;
    private int numPages;

    public DiskManager(String fileName) throws IOException {
        this.fileName = fileName;
        this.file = new RandomAccessFile(fileName, "rw");
        this.numPages = (int) (file.length() / Page.PAGE_SIZE);
    }

    public int getNumPages() {
        return numPages;
    }

    // writes a page's bytes to its page-aligned position on disk
    public void writePage(Page page) throws IOException {
        int positionToWrite = page.getPageNumber() * Page.PAGE_SIZE;
        file.seek(positionToWrite);
        file.write(page.getData());
    }

    // reads a full page from disk at its page-aligned position
    public Page readPage(int pageNumber) throws IOException {
        if (pageNumber < 0 || pageNumber >= numPages) {
            throw new IndexOutOfBoundsException(
                "Page number " + pageNumber + " is out of bounds (numPages=" + numPages + ")");
        }
        int positionToRead = pageNumber * Page.PAGE_SIZE;
        file.seek(positionToRead);
        byte[] array = new byte[Page.PAGE_SIZE];
        file.readFully(array); // fail fast on short reads instead of returning a partial page
        return new Page(pageNumber, ByteBuffer.wrap(array));
    }

    // allocates a fresh zero-initialized page at the end of the file
    public Page allocatePage() throws IOException {
        Page allocatedPage = new Page(numPages);
        numPages++;
        writePage(allocatedPage);
        return allocatedPage;
    }

    public void close() {
        try {
            file.close();
        } catch (IOException e) {
            log.log(Level.WARNING, "Failed to close disk file: " + fileName, e);
        }
    }
}
