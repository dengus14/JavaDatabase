package com.yourname.db.storage;

import java.io.IOException;

public class HeapFile {

    private DiskManager diskManager;
    private int numPages;


    public HeapFile(String fileName) throws IOException {
        this.diskManager = new DiskManager(fileName);
        this.numPages = diskManager.getNumPages();
    }
}
