package com.yourname.db.storage;

import java.io.IOException;
import java.util.Optional;

public class HeapFile {

    private DiskManager diskManager;
    private int numPages;


    public HeapFile(String fileName) throws IOException {
        this.diskManager = new DiskManager(fileName);
        this.numPages = diskManager.getNumPages();
    }

    public RecordID insert(byte[] data)throws IOException {

        for (int i = 0; i < numPages; i++) {
            Page page = diskManager.readPage(i);
            if (page.hasSpace(data.length)) {
                RecordID id = page.insert(data).get();
                diskManager.writePage(page);
                return id;
            }
        }
        Page allocatedPage = diskManager.allocatePage();
        RecordID id = allocatedPage.insert(data).get();
        diskManager.writePage(allocatedPage);
        this.numPages++;
        return id;
    }
}
