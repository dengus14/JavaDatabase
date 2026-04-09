package com.yourname.db.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

// manages an unordered collection of pages on disk
// inserts go to the first page with space; if none, a new page is allocated
public class HeapFile {

    private DiskManager diskManager;
    private int numPages;

    public HeapFile(String fileName) throws IOException {
        this.diskManager = new DiskManager(fileName);
        this.numPages = diskManager.getNumPages();
    }

    // scans existing pages for free space, allocates a new page if all are full
    public RecordID insert(byte[] data) throws IOException {
        for (int i = 0; i < numPages; i++) {
            Page page = diskManager.readPage(i);
            if (page.hasSpace(data.length)) {
                RecordID id = page.insert(data).get();
                diskManager.writePage(page);                // flush updated page to disk
                return id;
            }
        }
        Page allocatedPage = diskManager.allocatePage();     // append a fresh page at end of file
        RecordID id = allocatedPage.insert(data).get();
        diskManager.writePage(allocatedPage);
        this.numPages++;
        return id;
    }

    // reads the page containing the record and extracts it by pageNumber
    public byte[] get(RecordID rid) throws IOException {
        int pageNumber = rid.pageNumber();
        Page page = diskManager.readPage(pageNumber);
        return page.get(rid);
    }

    public void delete(RecordID rid) throws IOException {
        int pageNumber = rid.pageNumber();
        Page page = diskManager.readPage(pageNumber);
        page.delete(rid);
        diskManager.writePage(page);
    }

    public List<RecordID> scan() throws IOException {
        List<RecordID> ids = new ArrayList<>();

        for (int i = 0; i < numPages; i++) {
            Page page = diskManager.readPage(i);
            for(int j = 0; j < page.getNumRows(); j++){
                if(!page.isDeleted(j)){
                    ids.add(new RecordID(i,j));
                }
            }
        }
        return ids;
    }
}
