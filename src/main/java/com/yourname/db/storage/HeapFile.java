package com.yourname.db.storage;

public class HeapFile {

    private DiskManager diskManager;
    private int numPages;
    public HeapFile(DiskManager diskManager) {
        this.diskManager = diskManager;
    }
}
